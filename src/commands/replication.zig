const std = @import("std");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");
const repl_mod = @import("../storage/replication.zig");
const persistence_mod = @import("../storage/persistence.zig");

const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;
const ReplicationState = repl_mod.ReplicationState;
const Role = repl_mod.Role;
const Persistence = persistence_mod.Persistence;

// ── REPLICAOF ─────────────────────────────────────────────────────────────────

/// REPLICAOF host port   — become a replica of the given primary.
/// REPLICAOF NO ONE      — stop replication and become a primary.
///
/// For Iteration 10, the handshake is synchronous (blocks until RDB is loaded).
/// Returns +OK on success or -ERR on failure.
pub fn cmdReplicaof(
    allocator: std.mem.Allocator,
    storage: *Storage,
    repl: *ReplicationState,
    args: []const []const u8,
    my_port: u16,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'replicaof' command");
    }

    const host_arg = args[1];
    const port_arg = args[2];

    // REPLICAOF NO ONE
    const host_upper = try std.ascii.allocUpperString(allocator, host_arg);
    defer allocator.free(host_upper);
    const port_upper = try std.ascii.allocUpperString(allocator, port_arg);
    defer allocator.free(port_upper);

    if (std.mem.eql(u8, host_upper, "NO") and std.mem.eql(u8, port_upper, "ONE")) {
        return cmdReplicaofNoOne(allocator, repl);
    }

    // REPLICAOF host port
    const port = std.fmt.parseInt(u16, port_arg, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    // Tear down existing primary connection if any
    if (repl.primary_stream) |s| {
        s.close();
        repl.primary_stream = null;
    }
    if (repl.primary_host) |h| {
        repl.allocator.free(h);
        repl.primary_host = null;
    }

    repl.role = .replica;
    repl.primary_host = try repl.allocator.dupe(u8, host_arg);
    repl.primary_port = port;
    repl.primary_link_up = false;

    // Perform synchronous handshake + RDB load
    repl.connectToPrimary(storage, my_port) catch |err| {
        var buf: [256]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "ERR replication handshake failed: {any}", .{err}) catch "ERR replication handshake failed";
        return w.writeError(msg);
    };

    return w.writeSimpleString("OK");
}

/// Promote this instance to primary and disconnect from the old primary.
fn cmdReplicaofNoOne(allocator: std.mem.Allocator, repl: *ReplicationState) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (repl.primary_stream) |s| {
        s.close();
        repl.primary_stream = null;
    }
    if (repl.primary_host) |h| {
        repl.allocator.free(h);
        repl.primary_host = null;
    }
    repl.primary_port = 0;
    repl.primary_link_up = false;
    repl.role = .primary;
    // Keep existing replid and offset so former replicas can do a partial
    // resync (future work). For now we just flip the role.

    return w.writeSimpleString("OK");
}

// ── REPLCONF ─────────────────────────────────────────────────────────────────

/// REPLCONF subcommand [args...]
///
/// Replica->Primary: REPLCONF listening-port <port>
///                   REPLCONF capa eof capa psync2
///                   REPLCONF GETACK *
/// Primary->Replica: REPLCONF ACK <offset>
///
/// Returns +OK for most sub-commands, or :offset for GETACK.
pub fn cmdReplconf(
    allocator: std.mem.Allocator,
    repl: *ReplicationState,
    args: []const []const u8,
    /// If this call came from a replica that has already been registered,
    /// we pass a mutable pointer so we can update its port or state.
    replica_idx: ?usize,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'replconf' command");
    }

    const sub = args[1];
    const sub_upper = try std.ascii.allocUpperString(allocator, sub);
    defer allocator.free(sub_upper);

    if (std.mem.eql(u8, sub_upper, "LISTENING-PORT")) {
        // Replica is telling us its client port during handshake
        if (args.len >= 3) {
            const port = std.fmt.parseInt(u16, args[2], 10) catch 0;
            if (replica_idx) |idx| {
                if (idx < repl.replicas.items.len) {
                    repl.replicas.items[idx].port = port;
                }
            }
        }
        return w.writeSimpleString("OK");
    } else if (std.mem.eql(u8, sub_upper, "CAPA")) {
        // Acknowledge capability negotiation
        return w.writeSimpleString("OK");
    } else if (std.mem.eql(u8, sub_upper, "GETACK")) {
        // Primary asks replica for its current offset.
        // As a replica we respond with our current offset.
        const offset_str = try std.fmt.allocPrint(allocator, "{d}", .{repl.repl_offset});
        defer allocator.free(offset_str);

        // RESP: *3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$<n>\r\n<offset>\r\n
        var out = std.ArrayList(u8){};
        defer out.deinit(allocator);
        const hdr = try std.fmt.allocPrint(
            allocator,
            "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${d}\r\n{s}\r\n",
            .{ offset_str.len, offset_str },
        );
        defer allocator.free(hdr);
        try out.appendSlice(allocator, hdr);
        return out.toOwnedSlice(allocator);
    } else if (std.mem.eql(u8, sub_upper, "ACK")) {
        // Replica is confirming it received bytes up to <offset>
        if (args.len >= 3 and replica_idx != null) {
            const offset = std.fmt.parseInt(i64, args[2], 10) catch repl.repl_offset;
            const idx = replica_idx.?;
            if (idx < repl.replicas.items.len) {
                repl.replicas.items[idx].repl_offset = offset;
            }
        }
        return w.writeSimpleString("OK");
    } else {
        return w.writeSimpleString("OK");
    }
}

// ── PSYNC ─────────────────────────────────────────────────────────────────────

/// PSYNC replid offset
///
/// Primary responds with `+FULLRESYNC <replid> <offset>\r\n` then sends the RDB.
/// For Iteration 10, we always do a full resync regardless of replid/offset.
///
/// This function:
///   1. Sends the FULLRESYNC line.
///   2. Sends the RDB snapshot.
///   3. Marks the replica as online.
///
/// `stream` is the raw TCP stream of the replica connection.
/// The replica must already be added to `repl.replicas` at index `replica_idx`.
pub fn cmdPsync(
    allocator: std.mem.Allocator,
    storage: *Storage,
    repl: *ReplicationState,
    replica_idx: usize,
    stream: std.net.Stream,
) ![]const u8 {
    // Send FULLRESYNC response inline (not returning it as a string to avoid
    // the server writing it while we also write the RDB on the same stream).
    var header_buf: [64]u8 = undefined;
    const header = try std.fmt.bufPrint(
        &header_buf,
        "+FULLRESYNC {s} {d}\r\n",
        .{ repl.replid, repl.repl_offset },
    );
    stream.writeAll(header) catch |err| {
        std.debug.print("Replication: PSYNC header write error: {any}\n", .{err});
        return error.ReplicationError;
    };

    // Send the RDB snapshot
    repl.sendRdb(stream, storage) catch |err| {
        std.debug.print("Replication: RDB send error: {any}\n", .{err});
        return error.ReplicationError;
    };

    // Transition replica to online state
    if (replica_idx < repl.replicas.items.len) {
        repl.replicas.items[replica_idx].state = .online;
    }

    std.debug.print("Replication: replica fully synced (index={d})\n", .{replica_idx});

    // Return empty string since we already wrote directly to the stream
    var w = Writer.init(allocator);
    defer w.deinit();
    _ = &w;
    return try allocator.dupe(u8, "");
}

// ── WAIT ─────────────────────────────────────────────────────────────────────

/// WAIT numreplicas timeout_ms
///
/// Returns the number of replicas that have acknowledged the current offset.
/// Simplified: polls with 10ms sleep intervals up to timeout_ms.
pub fn cmdWait(
    allocator: std.mem.Allocator,
    repl: *ReplicationState,
    args: []const []const u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'wait' command");
    }

    const num_replicas = std.fmt.parseInt(u32, args[1], 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };
    const timeout_ms = std.fmt.parseInt(u64, args[2], 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    if (repl.role != .primary or num_replicas == 0 or repl.replicas.items.len == 0) {
        const count: i64 = @intCast(repl.replicas.items.len);
        return w.writeInteger(count);
    }

    // Poll until enough replicas are caught up or timeout expires
    const deadline_ns: u64 = if (timeout_ms > 0)
        @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000 + timeout_ms * 1_000_000
    else
        0;

    while (true) {
        var caught_up: u32 = 0;
        for (repl.replicas.items) |r| {
            if (r.state == .online and r.repl_offset >= repl.repl_offset) {
                caught_up += 1;
            }
        }

        if (caught_up >= num_replicas) {
            return w.writeInteger(@intCast(caught_up));
        }

        if (deadline_ns > 0) {
            const now_ns = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;
            if (now_ns >= deadline_ns) {
                return w.writeInteger(@intCast(caught_up));
            }
        } else {
            // timeout == 0: return immediately
            return w.writeInteger(@intCast(caught_up));
        }

        std.Thread.sleep(10 * std.time.ns_per_ms);
    }
}

// ── WAITAOF ──────────────────────────────────────────────────────────────────

/// WAITAOF numlocal numreplicas timeout
///
/// Blocks the client until all previous write commands are acknowledged as fsynced to AOF
/// of the local Redis and/or at least the specified number of replicas.
///
/// Returns an array of two integers:
/// [0] Number of local Redises (0 or 1) that fsynced all writes
/// [1] Number of replicas that fsynced all writes
///
/// Available since Redis 7.2.0
/// Time complexity: O(1)
/// ACL categories: @slow, @blocking, @connection
pub fn cmdWaitaof(
    allocator: std.mem.Allocator,
    repl: *ReplicationState,
    aof: ?*@import("../storage/aof.zig").Aof,
    args: []const []const u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'waitaof' command");
    }

    const numlocal = std.fmt.parseInt(u32, args[1], 10) catch {
        return w.writeError("ERR numlocal is not an integer or out of range");
    };

    const numreplicas = std.fmt.parseInt(u32, args[2], 10) catch {
        return w.writeError("ERR numreplicas is not an integer or out of range");
    };

    const timeout_ms = std.fmt.parseInt(u64, args[3], 10) catch {
        return w.writeError("ERR timeout is not an integer or out of range");
    };

    // Validate numlocal (must be 0 or 1)
    if (numlocal > 1) {
        return w.writeError("ERR numlocal should be 0 or 1");
    }

    // Cannot execute WAITAOF on replicas
    if (repl.role != .primary) {
        return w.writeError("ERR WAITAOF cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.");
    }

    // Check AOF availability
    const local_fsynced: u32 = if (numlocal == 1) blk: {
        if (aof == null) {
            return w.writeError("ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled");
        }
        // Stub: In a full implementation, we would track AOF fsync offset and wait for it
        // For now, if AOF is enabled and numlocal=1, assume immediate fsync (optimistic)
        break :blk 1;
    } else 0;

    // Replica fsync acknowledgment
    // Stub: In a full implementation, replicas would send AOF fsync ACKs via replication stream
    // For now, treat this like WAIT — check if replicas are caught up with replication offset
    var replicas_fsynced: u32 = 0;

    if (numreplicas > 0) {
        // Poll until enough replicas are caught up or timeout expires
        const deadline_ns: u64 = if (timeout_ms > 0)
            @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000 + timeout_ms * 1_000_000
        else
            0;

        while (true) {
            replicas_fsynced = 0;
            for (repl.replicas.items) |r| {
                if (r.state == .online and r.repl_offset >= repl.repl_offset) {
                    replicas_fsynced += 1;
                }
            }

            if (replicas_fsynced >= numreplicas) {
                break;
            }

            if (deadline_ns > 0) {
                const now_ns = @as(u64, @intCast(std.time.milliTimestamp())) * 1_000_000;
                if (now_ns >= deadline_ns) {
                    break; // Timeout reached
                }
            } else {
                // timeout == 0: block forever (but with replica checking)
                // For stub implementation, check once and return
                break;
            }

            std.Thread.sleep(10 * std.time.ns_per_ms);
        }
    }

    // Return [local_fsynced, replicas_fsynced] as array
    // RESP array format: *2\r\n:N\r\n:M\r\n
    var buf = std.ArrayList(u8){};
    defer buf.deinit(allocator);
    const buf_writer = buf.writer(allocator);

    try buf_writer.print("*2\r\n:{d}\r\n:{d}\r\n", .{ local_fsynced, replicas_fsynced });

    return buf.toOwnedSlice(allocator);
}

// ── FAILOVER ─────────────────────────────────────────────────────────────────

/// FAILOVER [TO host port [FORCE]] [ABORT] [TIMEOUT milliseconds]
///
/// Initiates a coordinated failover from a master to one of its replicas.
/// Returns +OK if the command was accepted and failover is in progress.
///
/// Options:
/// - TO host port [FORCE]: Designate a specific replica to failover to
/// - TIMEOUT ms: Maximum time to wait in waiting-for-sync state before aborting
/// - ABORT: Abort an ongoing failover
///
/// Stub implementation for Iteration 97:
/// - Validates arguments and sets failover state
/// - Real failover execution (CLIENT PAUSE, sync wait, demote, PSYNC FAILOVER) not implemented
/// - Requires full event-loop integration for production use
pub fn cmdFailover(
    allocator: std.mem.Allocator,
    repl: *ReplicationState,
    args: []const []const u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Only masters can initiate failover
    if (repl.role != .primary) {
        return w.writeError("ERR FAILOVER is not supported for replica instances");
    }

    // Parse options
    var abort = false;
    var timeout_ms: u64 = 0;
    var target_host: ?[]const u8 = null;
    var target_port: u16 = 0;
    var force = false;

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        const arg_upper = try std.ascii.allocUpperString(allocator, arg);
        defer allocator.free(arg_upper);

        if (std.mem.eql(u8, arg_upper, "ABORT")) {
            abort = true;
        } else if (std.mem.eql(u8, arg_upper, "TIMEOUT")) {
            if (i + 1 >= args.len) {
                return w.writeError("ERR syntax error");
            }
            i += 1;
            timeout_ms = std.fmt.parseInt(u64, args[i], 10) catch {
                return w.writeError("ERR timeout is not an integer or out of range");
            };
        } else if (std.mem.eql(u8, arg_upper, "TO")) {
            if (i + 2 >= args.len) {
                return w.writeError("ERR syntax error");
            }
            i += 1;
            target_host = args[i];
            i += 1;
            target_port = std.fmt.parseInt(u16, args[i], 10) catch {
                return w.writeError("ERR invalid port");
            };
        } else if (std.mem.eql(u8, arg_upper, "FORCE")) {
            force = true;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // ABORT: cancel ongoing failover
    if (abort) {
        if (repl.failover_state == .no_failover) {
            return w.writeError("ERR No failover in progress");
        }

        // Reset failover state
        repl.failover_state = .no_failover;
        if (repl.failover_target_host) |h| {
            repl.allocator.free(h);
            repl.failover_target_host = null;
        }
        repl.failover_target_port = 0;
        repl.failover_timeout_ms = 0;
        repl.failover_start_ms = 0;

        return w.writeSimpleString("OK");
    }

    // Check if failover already in progress
    if (repl.failover_state != .no_failover) {
        return w.writeError("ERR Failover already in progress");
    }

    // Validate FORCE: only valid with TO + TIMEOUT
    if (force and (target_host == null or timeout_ms == 0)) {
        return w.writeError("ERR FORCE requires both TO and TIMEOUT");
    }

    // Validate target replica (if specified)
    if (target_host != null) {
        // Check if target replica exists and is connected
        var found = false;
        for (repl.replicas.items) |r| {
            if (r.port == target_port) {
                found = true;
                break;
            }
        }
        if (!found) {
            return w.writeError("ERR Target replica not found or not connected");
        }
    }

    // Initiate failover
    // Stub implementation: set state to waiting_for_sync
    // Real implementation would:
    // 1. CLIENT PAUSE WRITE
    // 2. Monitor replicas for sync
    // 3. Demote master to replica
    // 4. Send PSYNC FAILOVER to target replica
    // 5. CLIENT UNPAUSE after acknowledgment

    repl.failover_state = .waiting_for_sync;
    repl.failover_timeout_ms = timeout_ms;
    repl.failover_start_ms = std.time.milliTimestamp();

    if (target_host) |host| {
        // Store target replica info
        if (repl.failover_target_host) |h| {
            repl.allocator.free(h);
        }
        repl.failover_target_host = try repl.allocator.dupe(u8, host);
        repl.failover_target_port = target_port;
    }

    return w.writeSimpleString("OK");
}

// ── ROLE ──────────────────────────────────────────────────────────────────────

/// ROLE
///
/// Returns information about the role of a Redis instance in the replication stack.
///
/// For a primary, returns:
///   *3\r\n
///   $6\r\nmaster\r\n
///   :<offset>\r\n
///   *<num_replicas>\r\n
///     *3\r\n$<ip_len>\r\n<ip>\r\n$<port_len>\r\n<port>\r\n$<offset_len>\r\n<offset>\r\n
///     ...
///
/// For a replica, returns:
///   *5\r\n
///   $5\r\nslave\r\n
///   $<ip_len>\r\n<ip>\r\n
///   :<port>\r\n
///   $<state_len>\r\n<state>\r\n
///   :<offset>\r\n
///
/// State inference for replicas:
///   - primary_stream == null → "connect" (not yet connected)
///   - primary_link_up == false → "connecting" (connection exists but not fully established)
///   - otherwise → "connected"
pub fn cmdRole(
    allocator: std.mem.Allocator,
    repl: *ReplicationState,
    args: []const []const u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // ROLE takes no arguments
    if (args.len != 1) {
        return w.writeError("ERR wrong number of arguments for 'role' command");
    }

    var buffer = std.ArrayList(u8){};
    errdefer buffer.deinit(allocator);

    if (repl.role == .primary) {
        // Master response: ["master", offset, [[ip, port, offset], ...]]
        try buffer.appendSlice(allocator, "*3\r\n");
        try buffer.appendSlice(allocator, "$6\r\nmaster\r\n");

        // Write offset as integer
        try std.fmt.format(buffer.writer(allocator), ":{d}\r\n", .{repl.repl_offset});

        // Write replica list array
        try std.fmt.format(buffer.writer(allocator), "*{d}\r\n", .{repl.replicas.items.len});

        // Write each replica as [ip, port, offset]
        for (repl.replicas.items) |replica| {
            // Each replica is a 3-element array
            try buffer.appendSlice(allocator, "*3\r\n");

            // IP address (hardcoded to 127.0.0.1 for now)
            const ip = "127.0.0.1";
            try std.fmt.format(buffer.writer(allocator), "${d}\r\n{s}\r\n", .{ ip.len, ip });

            // Port as bulk string
            const port_str = try std.fmt.allocPrint(allocator, "{d}", .{replica.port});
            defer allocator.free(port_str);
            try std.fmt.format(buffer.writer(allocator), "${d}\r\n{s}\r\n", .{ port_str.len, port_str });

            // Offset as bulk string
            const offset_str = try std.fmt.allocPrint(allocator, "{d}", .{replica.repl_offset});
            defer allocator.free(offset_str);
            try std.fmt.format(buffer.writer(allocator), "${d}\r\n{s}\r\n", .{ offset_str.len, offset_str });
        }
    } else {
        // Replica response: ["slave", master_ip, master_port, state, offset]
        try buffer.appendSlice(allocator, "*5\r\n");
        try buffer.appendSlice(allocator, "$5\r\nslave\r\n");

        // Master IP (use primary_host if available, else use placeholder)
        const master_ip = repl.primary_host orelse "127.0.0.1";
        try std.fmt.format(buffer.writer(allocator), "${d}\r\n{s}\r\n", .{ master_ip.len, master_ip });

        // Master port as integer
        try std.fmt.format(buffer.writer(allocator), ":{d}\r\n", .{repl.primary_port});

        // Connection state as bulk string
        const state: []const u8 = if (repl.primary_stream == null)
            "connect"
        else if (!repl.primary_link_up)
            "connecting"
        else
            "connected";

        try std.fmt.format(buffer.writer(allocator), "${d}\r\n{s}\r\n", .{ state.len, state });

        // Replication offset as integer (or -1 if not connected)
        const offset: i64 = if (repl.primary_stream == null) -1 else repl.repl_offset;
        try std.fmt.format(buffer.writer(allocator), ":{d}\r\n", .{offset});
    }

    return buffer.toOwnedSlice(allocator);
}

// ── INFO ─────────────────────────────────────────────────────────────────────

/// INFO [section]
///
/// Returns a bulk string with server information.
/// For Iteration 10, only the replication section is implemented.
pub fn cmdInfo(
    allocator: std.mem.Allocator,
    repl: *ReplicationState,
    args: []const []const u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Determine which section was requested
    const section: []const u8 = if (args.len >= 2) args[1] else "all";
    const section_upper = try std.ascii.allocUpperString(allocator, section);
    defer allocator.free(section_upper);

    const show_replication = std.mem.eql(u8, section_upper, "ALL") or
        std.mem.eql(u8, section_upper, "REPLICATION") or
        std.mem.eql(u8, section_upper, "DEFAULT");

    if (!show_replication) {
        // Unknown section: return empty bulk string
        return w.writeBulkString("");
    }

    return buildReplicationInfo(allocator, repl);
}

/// Build the "# Replication" INFO section.
pub fn buildReplicationInfo(
    allocator: std.mem.Allocator,
    repl: *ReplicationState,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    var buf = std.ArrayList(u8){};
    defer buf.deinit(allocator);

    const bw = buf.writer(allocator);

    try bw.writeAll("# Replication\r\n");

    const role_str: []const u8 = switch (repl.role) {
        .primary => "master",
        .replica => "slave",
    };
    try bw.print("role:{s}\r\n", .{role_str});
    try bw.print("master_replid:{s}\r\n", .{repl.replid});
    try bw.print("master_repl_offset:{d}\r\n", .{repl.repl_offset});

    // Add master_failover_state for primaries
    if (repl.role == .primary) {
        try bw.print("master_failover_state:{s}\r\n", .{repl.failover_state.toString()});
    }

    if (repl.role == .primary) {
        try bw.print("connected_slaves:{d}\r\n", .{repl.replicas.items.len});
        for (repl.replicas.items, 0..) |r, i| {
            const state_str: []const u8 = switch (r.state) {
                .handshake => "wait_bgsave",
                .rdb_transfer => "send_bulk",
                .online => "online",
            };
            try bw.print("slave{d}:ip=unknown,port={d},state={s},offset={d},lag=0\r\n", .{
                i, r.port, state_str, r.repl_offset,
            });
        }
    } else {
        const host = repl.primary_host orelse "unknown";
        try bw.print("master_host:{s}\r\n", .{host});
        try bw.print("master_port:{d}\r\n", .{repl.primary_port});
        const link_status: []const u8 = if (repl.primary_link_up) "up" else "down";
        try bw.print("master_link_status:{s}\r\n", .{link_status});
        try bw.print("slave_repl_offset:{d}\r\n", .{repl.repl_offset});
    }

    return w.writeBulkString(buf.items);
}

// ── Embedded unit tests ───────────────────────────────────────────────────────

test "replication commands - INFO primary role" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const args = [_][]const u8{ "INFO", "replication" };
    const result = try cmdInfo(allocator, &repl, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "role:master") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "master_replid:") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "connected_slaves:0") != null);
}

test "replication commands - INFO replica role" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initReplica(allocator, "127.0.0.1", 6379);
    defer repl.deinit();

    const args = [_][]const u8{ "INFO", "replication" };
    const result = try cmdInfo(allocator, &repl, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "role:slave") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "master_host:127.0.0.1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "master_port:6379") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "master_link_status:down") != null);
}

test "replication commands - INFO all section" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const args = [_][]const u8{"INFO"};
    const result = try cmdInfo(allocator, &repl, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "# Replication") != null);
}

test "replication commands - INFO unknown section returns empty" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const args = [_][]const u8{ "INFO", "server" };
    const result = try cmdInfo(allocator, &repl, &args);
    defer allocator.free(result);

    // Unknown section: returns empty bulk string $0\r\n\r\n
    try std.testing.expectEqualStrings("$0\r\n\r\n", result);
}

test "replication commands - WAIT returns 0 with no replicas" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const args = [_][]const u8{ "WAIT", "1", "0" };
    const result = try cmdWait(allocator, &repl, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "replication commands - WAIT wrong args" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const args = [_][]const u8{"WAIT"};
    const result = try cmdWait(allocator, &repl, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

test "replication commands - REPLCONF listening-port" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const args = [_][]const u8{ "REPLCONF", "listening-port", "6380" };
    const result = try cmdReplconf(allocator, &repl, &args, null);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "replication commands - REPLCONF capa" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const args = [_][]const u8{ "REPLCONF", "capa", "eof", "capa", "psync2" };
    const result = try cmdReplconf(allocator, &repl, &args, null);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "replication commands - REPLICAOF NO ONE" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var repl = try ReplicationState.initReplica(allocator, "127.0.0.1", 6379);
    defer repl.deinit();

    const args = [_][]const u8{ "REPLICAOF", "NO", "ONE" };
    const result = try cmdReplicaof(allocator, storage, &repl, &args, 6380);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqual(Role.primary, repl.role);
}

test "replication commands - WAITAOF wrong number of args" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const args = [_][]const u8{ "WAITAOF", "1", "0" };
    const result = try cmdWaitaof(allocator, &repl, null, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

test "replication commands - WAITAOF invalid numlocal" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const args = [_][]const u8{ "WAITAOF", "2", "0", "1000" };
    const result = try cmdWaitaof(allocator, &repl, null, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

test "replication commands - WAITAOF without AOF enabled" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const args = [_][]const u8{ "WAITAOF", "1", "0", "1000" };
    const result = try cmdWaitaof(allocator, &repl, null, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, result, "appendonly") != null);
}

test "replication commands - WAITAOF on replica" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initReplica(allocator, "127.0.0.1", 6379);
    defer repl.deinit();

    const args = [_][]const u8{ "WAITAOF", "1", "0", "1000" };
    const result = try cmdWaitaof(allocator, &repl, null, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, result, "replica") != null);
}

test "replication commands - WAITAOF success with no AOF required" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const args = [_][]const u8{ "WAITAOF", "0", "0", "0" };
    const result = try cmdWaitaof(allocator, &repl, null, &args);
    defer allocator.free(result);

    // Should return array [0, 0]
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
}

test "replication commands - FAILOVER on replica fails" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initReplica(allocator, "127.0.0.1", 6379);
    defer repl.deinit();

    const args = [_][]const u8{"FAILOVER"};
    const result = try cmdFailover(allocator, &repl, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, result, "replica") != null);
}

test "replication commands - FAILOVER basic (no options)" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const args = [_][]const u8{"FAILOVER"};
    const result = try cmdFailover(allocator, &repl, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqual(repl_mod.FailoverState.waiting_for_sync, repl.failover_state);
}

test "replication commands - FAILOVER with TIMEOUT" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const args = [_][]const u8{ "FAILOVER", "TIMEOUT", "5000" };
    const result = try cmdFailover(allocator, &repl, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqual(repl_mod.FailoverState.waiting_for_sync, repl.failover_state);
    try std.testing.expectEqual(@as(u64, 5000), repl.failover_timeout_ms);
}

test "replication commands - FAILOVER ABORT without ongoing failover" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const args = [_][]const u8{ "FAILOVER", "ABORT" };
    const result = try cmdFailover(allocator, &repl, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, result, "No failover") != null);
}

test "replication commands - FAILOVER ABORT after starting failover" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Start failover
    const args1 = [_][]const u8{"FAILOVER"};
    const result1 = try cmdFailover(allocator, &repl, &args1);
    defer allocator.free(result1);
    try std.testing.expectEqualStrings("+OK\r\n", result1);

    // Abort it
    const args2 = [_][]const u8{ "FAILOVER", "ABORT" };
    const result2 = try cmdFailover(allocator, &repl, &args2);
    defer allocator.free(result2);

    try std.testing.expectEqualStrings("+OK\r\n", result2);
    try std.testing.expectEqual(repl_mod.FailoverState.no_failover, repl.failover_state);
}

test "replication commands - FAILOVER already in progress" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Start failover
    const args1 = [_][]const u8{"FAILOVER"};
    const result1 = try cmdFailover(allocator, &repl, &args1);
    defer allocator.free(result1);

    // Try to start another
    const args2 = [_][]const u8{"FAILOVER"};
    const result2 = try cmdFailover(allocator, &repl, &args2);
    defer allocator.free(result2);

    try std.testing.expect(std.mem.startsWith(u8, result2, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, result2, "already in progress") != null);
}

test "replication commands - FAILOVER FORCE requires TO and TIMEOUT" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const args = [_][]const u8{ "FAILOVER", "FORCE" };
    const result = try cmdFailover(allocator, &repl, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, result, "FORCE requires") != null);
}

test "replication commands - FAILOVER invalid timeout" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const args = [_][]const u8{ "FAILOVER", "TIMEOUT", "notanumber" };
    const result = try cmdFailover(allocator, &repl, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

test "replication commands - INFO replication includes master_failover_state" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const info = try buildReplicationInfo(allocator, &repl);
    defer allocator.free(info);

    try std.testing.expect(std.mem.indexOf(u8, info, "master_failover_state:no-failover") != null);
}

test "replication commands - ROLE returns master with no replicas" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // ROLE command takes no arguments
    const args = [_][]const u8{"ROLE"};
    const result = try cmdRole(allocator, &repl, &args);
    defer allocator.free(result);

    // Expected: *3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n
    // Array of 3 elements: "master", offset (0), empty array of replicas
    try std.testing.expect(std.mem.startsWith(u8, result, "*3\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "$6\r\nmaster\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":0\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "*0\r\n") != null);
}

test "replication commands - ROLE returns master with 2 replicas" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Manually add mock replica connections
    const mock_stream1 = std.net.Stream{ .handle = 100 };
    const mock_stream2 = std.net.Stream{ .handle = 101 };

    try repl.replicas.append(allocator, .{
        .stream = mock_stream1,
        .port = 6380,
        .repl_offset = 1024,
        .state = .online,
    });
    try repl.replicas.append(allocator, .{
        .stream = mock_stream2,
        .port = 6381,
        .repl_offset = 2048,
        .state = .online,
    });

    const args = [_][]const u8{"ROLE"};
    const result = try cmdRole(allocator, &repl, &args);
    defer allocator.free(result);

    // Expected: *3\r\n$6\r\nmaster\r\n:0\r\n*2\r\n*3\r\n$9\r\n127.0.0.1\r\n$4\r\n6380\r\n$4\r\n1024\r\n*3\r\n$9\r\n127.0.0.1\r\n$4\r\n6381\r\n$4\r\n2048\r\n
    // Array of 3 elements: "master", offset, array of 2 replica info arrays
    try std.testing.expect(std.mem.startsWith(u8, result, "*3\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "$6\r\nmaster\r\n") != null);
    // Should contain array of 2 replicas
    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
    // Should contain replica info (IP, port, offset)
    try std.testing.expect(std.mem.indexOf(u8, result, "127.0.0.1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "6380") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "1024") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "6381") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "2048") != null);
}

test "replication commands - ROLE returns replica not connected" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initReplica(allocator, "192.168.1.100", 6379);
    defer repl.deinit();

    // primary_stream is null → state is "connect"
    try std.testing.expect(repl.primary_stream == null);

    const args = [_][]const u8{"ROLE"};
    const result = try cmdRole(allocator, &repl, &args);
    defer allocator.free(result);

    // Expected: *5\r\n$5\r\nslave\r\n$13\r\n192.168.1.100\r\n:6379\r\n$7\r\nconnect\r\n:-1\r\n
    // Array of 5 elements: "slave", master_ip, master_port, state ("connect"), offset (-1)
    try std.testing.expect(std.mem.startsWith(u8, result, "*5\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "$5\r\nslave\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "192.168.1.100") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":6379\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$7\r\nconnect\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":-1\r\n") != null);
}

test "replication commands - ROLE returns replica connected" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initReplica(allocator, "10.0.0.5", 6379);
    defer repl.deinit();

    // Simulate successful connection
    const mock_stream = std.net.Stream{ .handle = 200 };
    repl.primary_stream = mock_stream;
    repl.primary_link_up = true;
    repl.repl_offset = 4096;

    const args = [_][]const u8{"ROLE"};
    const result = try cmdRole(allocator, &repl, &args);
    defer allocator.free(result);

    // Expected: *5\r\n$5\r\nslave\r\n$8\r\n10.0.0.5\r\n:6379\r\n$9\r\nconnected\r\n:4096\r\n
    // Array of 5 elements: "slave", master_ip, master_port, state ("connected"), offset
    try std.testing.expect(std.mem.startsWith(u8, result, "*5\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "$5\r\nslave\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "10.0.0.5") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":6379\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$9\r\nconnected\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":4096\r\n") != null);
}

test "replication commands - ROLE returns replica connecting" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initReplica(allocator, "172.16.0.10", 6379);
    defer repl.deinit();

    // Simulate connection exists but link not fully up
    const mock_stream = std.net.Stream{ .handle = 300 };
    repl.primary_stream = mock_stream;
    repl.primary_link_up = false; // Link not fully established
    repl.repl_offset = 512;

    const args = [_][]const u8{"ROLE"};
    const result = try cmdRole(allocator, &repl, &args);
    defer allocator.free(result);

    // Expected: *5\r\n$5\r\nslave\r\n$11\r\n172.16.0.10\r\n:6379\r\n$10\r\nconnecting\r\n:512\r\n
    // Array of 5 elements: "slave", master_ip, master_port, state ("connecting"), offset
    try std.testing.expect(std.mem.startsWith(u8, result, "*5\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "$5\r\nslave\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "172.16.0.10") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":6379\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$10\r\nconnecting\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":512\r\n") != null);
}

test "replication commands - ROLE rejects arguments" {
    const allocator = std.testing.allocator;
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // ROLE command accepts no arguments
    const args = [_][]const u8{ "ROLE", "extra" };
    const result = try cmdRole(allocator, &repl, &args);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}
