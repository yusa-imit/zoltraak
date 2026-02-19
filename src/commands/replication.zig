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
