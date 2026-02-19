const std = @import("std");
const persistence_mod = @import("persistence.zig");
const memory_mod = @import("memory.zig");

const Persistence = persistence_mod.Persistence;
const Storage = memory_mod.Storage;

// ── Constants ────────────────────────────────────────────────────────────────

/// Length of a Redis replication ID (40 hex characters)
pub const REPLID_LEN: usize = 40;

/// Bytes used for initial "empty" RDB sent to replicas (Zoltraak custom magic)
const RDB_MAGIC = "ZOLTRAAK";
const RDB_VERSION: u8 = 1;
const RDB_TYPE_EOF: u8 = 0xFF;

// ── Types ────────────────────────────────────────────────────────────────────

/// Server role in a replication topology.
pub const Role = enum {
    primary,
    replica,
};

/// Connection state of a connected replica.
const ReplicaState = enum {
    /// Performing PING / REPLCONF / PSYNC handshake
    handshake,
    /// Receiving full RDB data
    rdb_transfer,
    /// Fully synchronised; receiving command stream
    online,
};

/// Information about a connected replica (primary-side view).
pub const ReplicaInfo = struct {
    /// TCP stream to the replica
    stream: std.net.Stream,
    /// Port the replica told us via REPLCONF listening-port
    port: u16,
    /// Bytes the replica has acknowledged
    repl_offset: i64,
    /// Current handshake / sync state
    state: ReplicaState,
};

/// Central replication state shared across all connections.
///
/// On the primary: tracks connected replicas and the current offset.
/// On the replica: holds the connection back to the primary.
pub const ReplicationState = struct {
    allocator: std.mem.Allocator,

    /// This instance's role (primary or replica)
    role: Role,

    /// 40-character hex replication ID (primary generates it; replica stores primary's)
    replid: [REPLID_LEN]u8,

    /// Bytes propagated to replicas so far (primary) or received (replica)
    repl_offset: i64,

    /// List of connected replica connections (primary-side only)
    replicas: std.ArrayList(ReplicaInfo),

    /// Primary host string (owned slice, null when role == .primary)
    primary_host: ?[]u8,

    /// Primary port (0 when role == .primary)
    primary_port: u16,

    /// TCP stream to primary (replica-side only)
    primary_stream: ?std.net.Stream,

    /// Whether the replica-to-primary link is healthy
    primary_link_up: bool,

    // ── Initialisation ────────────────────────────────────────────────────

    /// Create a new ReplicationState as a primary (generates a random replid).
    pub fn initPrimary(allocator: std.mem.Allocator) !ReplicationState {
        var state = ReplicationState{
            .allocator = allocator,
            .role = .primary,
            .replid = undefined,
            .repl_offset = 0,
            .replicas = std.ArrayList(ReplicaInfo){},
            .primary_host = null,
            .primary_port = 0,
            .primary_stream = null,
            .primary_link_up = false,
        };
        generateReplid(&state.replid);
        return state;
    }

    /// Create a new ReplicationState as a replica targeting the given primary.
    /// `host` is duplicated into the allocator; caller need not keep the original alive.
    pub fn initReplica(
        allocator: std.mem.Allocator,
        host: []const u8,
        port: u16,
    ) !ReplicationState {
        var state = ReplicationState{
            .allocator = allocator,
            .role = .replica,
            .replid = undefined,
            .repl_offset = -1,
            .replicas = std.ArrayList(ReplicaInfo){},
            .primary_host = try allocator.dupe(u8, host),
            .primary_port = port,
            .primary_stream = null,
            .primary_link_up = false,
        };
        // replid will be set from the primary's FULLRESYNC response
        @memset(&state.replid, '0');
        return state;
    }

    /// Free all owned memory.
    pub fn deinit(self: *ReplicationState) void {
        // Close replica streams
        for (self.replicas.items) |*r| {
            r.stream.close();
        }
        self.replicas.deinit(self.allocator);

        // Close primary stream
        if (self.primary_stream) |s| {
            s.close();
        }

        // Free heap strings
        if (self.primary_host) |h| {
            self.allocator.free(h);
        }
    }

    // ── Primary helpers ───────────────────────────────────────────────────

    /// Register a newly connected replica.
    /// Takes ownership of `stream`; it will be closed in deinit().
    pub fn addReplica(self: *ReplicationState, stream: std.net.Stream, port: u16) !void {
        try self.replicas.append(self.allocator, ReplicaInfo{
            .stream = stream,
            .port = port,
            .repl_offset = 0,
            .state = .handshake,
        });
    }

    /// Propagate raw RESP bytes to all online replicas.
    /// Failed writes log a warning but do not abort propagation to other replicas.
    /// Also increments `repl_offset` by `data.len`.
    pub fn propagate(self: *ReplicationState, data: []const u8) void {
        if (self.role != .primary) return;

        self.repl_offset += @intCast(data.len);

        var i: usize = 0;
        while (i < self.replicas.items.len) {
            const replica = &self.replicas.items[i];
            if (replica.state != .online) {
                i += 1;
                continue;
            }
            replica.stream.writeAll(data) catch |err| {
                std.debug.print("Replication: write to replica failed (port={d}): {any}\n", .{ replica.port, err });
                // Remove dead replica
                replica.stream.close();
                _ = self.replicas.swapRemove(i);
                continue;
            };
            i += 1;
        }
    }

    /// Generate and send an RDB snapshot over `stream`.
    ///
    /// Protocol: `$<len>\r\n<rdb_bytes>` (no trailing \r\n after RDB body).
    pub fn sendRdb(
        self: *ReplicationState,
        stream: std.net.Stream,
        storage: *Storage,
    ) !void {
        _ = self;

        // Build RDB in memory
        var buf = std.ArrayList(u8){};
        defer buf.deinit(std.heap.page_allocator);

        // Temporarily save storage to a buffer via Persistence
        try buildRdbInMemory(storage, &buf);

        // Send RESP bulk-string header: $<len>\r\n
        var header_buf: [32]u8 = undefined;
        const header = try std.fmt.bufPrint(&header_buf, "${d}\r\n", .{buf.items.len});
        try stream.writeAll(header);

        // Send RDB body (NO trailing \r\n per Redis replication protocol)
        try stream.writeAll(buf.items);
    }

    // ── Replica helpers ───────────────────────────────────────────────────

    /// Perform the full replica handshake with the primary and load the
    /// initial RDB snapshot into `storage`.
    ///
    /// Steps:
    ///   1. TCP connect
    ///   2. PING -> +PONG
    ///   3. REPLCONF listening-port <my_port> -> +OK
    ///   4. REPLCONF capa eof capa psync2 -> +OK
    ///   5. PSYNC ? -1 -> +FULLRESYNC <replid> <offset>
    ///   6. Receive $<len>\r\n<rdb_body>
    ///   7. Load RDB into storage
    pub fn connectToPrimary(
        self: *ReplicationState,
        storage: *Storage,
        my_port: u16,
    ) !void {
        const host = self.primary_host orelse return error.NoPrimaryConfigured;

        // 1. TCP connect
        const address = try std.net.Address.parseIp4(host, self.primary_port);
        const stream = try std.net.tcpConnectToAddress(address);
        errdefer stream.close();

        var read_buf: [4096]u8 = undefined;

        // 2. PING
        try stream.writeAll("*1\r\n$4\r\nPING\r\n");
        const pong_len = try stream.read(&read_buf);
        if (pong_len == 0) return error.PrimaryClosedConnection;
        const pong = read_buf[0..pong_len];
        if (!std.mem.startsWith(u8, pong, "+PONG")) {
            std.debug.print("Replication: expected +PONG from primary, got: {s}\n", .{pong});
            return error.HandshakeFailed;
        }

        // 3. REPLCONF listening-port
        var cmd_buf: [256]u8 = undefined;
        const replconf_port = try std.fmt.bufPrint(
            &cmd_buf,
            "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${d}\r\n{d}\r\n",
            .{ digitCount(my_port), my_port },
        );
        try stream.writeAll(replconf_port);
        const ok1_len = try stream.read(&read_buf);
        if (ok1_len == 0) return error.PrimaryClosedConnection;
        if (!std.mem.startsWith(u8, read_buf[0..ok1_len], "+OK")) {
            return error.HandshakeFailed;
        }

        // 4. REPLCONF capa eof capa psync2
        const replconf_capa =
            "*5\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        try stream.writeAll(replconf_capa);
        const ok2_len = try stream.read(&read_buf);
        if (ok2_len == 0) return error.PrimaryClosedConnection;
        if (!std.mem.startsWith(u8, read_buf[0..ok2_len], "+OK")) {
            return error.HandshakeFailed;
        }

        // 5. PSYNC ? -1  (request full sync)
        try stream.writeAll("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");

        // Read FULLRESYNC response (may be multi-line; read until we find \r\n)
        const fr_len = try readLine(stream, &read_buf);
        const fr = read_buf[0..fr_len];
        if (!std.mem.startsWith(u8, fr, "+FULLRESYNC ")) {
            std.debug.print("Replication: expected +FULLRESYNC, got: {s}\n", .{fr});
            return error.HandshakeFailed;
        }
        // Parse replid from "+FULLRESYNC <replid> <offset>\r\n"
        const after_prefix = fr["+FULLRESYNC ".len..];
        if (after_prefix.len >= REPLID_LEN) {
            @memcpy(&self.replid, after_prefix[0..REPLID_LEN]);
        }

        // 6. Receive RDB: $<len>\r\n<body>
        const rdb_header_len = try readLine(stream, &read_buf);
        const rdb_header = read_buf[0..rdb_header_len];
        if (rdb_header.len < 2 or rdb_header[0] != '$') {
            return error.HandshakeFailed;
        }
        const rdb_size_str = std.mem.trimRight(u8, rdb_header[1..], "\r\n");
        const rdb_size = try std.fmt.parseInt(usize, rdb_size_str, 10);

        // Read exactly rdb_size bytes
        const rdb_data = try std.heap.page_allocator.alloc(u8, rdb_size);
        defer std.heap.page_allocator.free(rdb_data);
        var rdb_received: usize = 0;
        while (rdb_received < rdb_size) {
            const n = try stream.read(rdb_data[rdb_received..]);
            if (n == 0) return error.PrimaryClosedConnection;
            rdb_received += n;
        }

        // 7. Load RDB into storage
        _ = try Persistence.loadFromBytes(storage, rdb_data, std.heap.page_allocator);

        self.primary_stream = stream;
        self.primary_link_up = true;
        self.repl_offset = 0;

        std.debug.print("Replication: connected to primary {s}:{d}, RDB loaded ({d} bytes)\n", .{
            host, self.primary_port, rdb_size,
        });
    }
};

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Generate a 40-character lowercase hex replication ID using random bytes.
fn generateReplid(out: *[REPLID_LEN]u8) void {
    var raw: [20]u8 = undefined;
    std.crypto.random.bytes(&raw);
    const hex_chars = "0123456789abcdef";
    for (raw, 0..) |byte, i| {
        out[i * 2] = hex_chars[byte >> 4];
        out[i * 2 + 1] = hex_chars[byte & 0xF];
    }
}

/// Return the number of decimal digits in `n` (minimum 1).
fn digitCount(n: u16) usize {
    if (n == 0) return 1;
    var count: usize = 0;
    var v = n;
    while (v > 0) : (v /= 10) count += 1;
    return count;
}

/// Read bytes from `stream` until `\n` is found or `buf` is full.
/// Returns the number of bytes read (including the newline).
fn readLine(stream: std.net.Stream, buf: []u8) !usize {
    var total: usize = 0;
    while (total < buf.len) {
        const n = try stream.read(buf[total..total + 1]);
        if (n == 0) return error.EndOfStream;
        total += n;
        if (buf[total - 1] == '\n') break;
    }
    return total;
}

/// Build an RDB snapshot of `storage` into `buf` using the Persistence format.
fn buildRdbInMemory(storage: *Storage, buf: *std.ArrayList(u8)) !void {
    const bytes = try Persistence.saveToBytes(storage, std.heap.page_allocator);
    defer std.heap.page_allocator.free(bytes);
    try buf.appendSlice(std.heap.page_allocator, bytes);
}

// ── Embedded unit tests ───────────────────────────────────────────────────────

test "replication - generateReplid produces 40 hex chars" {
    var replid: [REPLID_LEN]u8 = undefined;
    generateReplid(&replid);

    try std.testing.expectEqual(@as(usize, 40), replid.len);
    for (replid) |c| {
        const valid = (c >= '0' and c <= '9') or (c >= 'a' and c <= 'f');
        try std.testing.expect(valid);
    }
}

test "replication - two replids are different" {
    var a: [REPLID_LEN]u8 = undefined;
    var b: [REPLID_LEN]u8 = undefined;
    generateReplid(&a);
    generateReplid(&b);
    // Extremely unlikely to collide for 40 random hex chars (160 bits)
    try std.testing.expect(!std.mem.eql(u8, &a, &b));
}

test "replication - digitCount" {
    try std.testing.expectEqual(@as(usize, 1), digitCount(0));
    try std.testing.expectEqual(@as(usize, 1), digitCount(9));
    try std.testing.expectEqual(@as(usize, 2), digitCount(10));
    try std.testing.expectEqual(@as(usize, 4), digitCount(6380));
    try std.testing.expectEqual(@as(usize, 5), digitCount(65535));
}

test "replication - initPrimary sets role and valid replid" {
    const allocator = std.testing.allocator;
    var state = try ReplicationState.initPrimary(allocator);
    defer state.deinit();

    try std.testing.expectEqual(Role.primary, state.role);
    try std.testing.expectEqual(@as(i64, 0), state.repl_offset);
    for (state.replid) |c| {
        const valid = (c >= '0' and c <= '9') or (c >= 'a' and c <= 'f');
        try std.testing.expect(valid);
    }
}

test "replication - initReplica sets role and host" {
    const allocator = std.testing.allocator;
    var state = try ReplicationState.initReplica(allocator, "127.0.0.1", 6379);
    defer state.deinit();

    try std.testing.expectEqual(Role.replica, state.role);
    try std.testing.expectEqualStrings("127.0.0.1", state.primary_host.?);
    try std.testing.expectEqual(@as(u16, 6379), state.primary_port);
}
