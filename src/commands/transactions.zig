const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;

/// A queued command for transaction execution.
/// Stores the raw RESP-encoded bytes to replay during EXEC.
pub const QueuedCommand = struct {
    /// Raw RESP bytes of the command (owned, allocated with the TxState allocator)
    data: []const u8,

    pub fn deinit(self: *QueuedCommand, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
    }
};

/// Per-connection transaction state.
/// Lives for the lifetime of a single client connection.
pub const TxState = struct {
    allocator: std.mem.Allocator,
    /// Whether we are inside a MULTI block
    active: bool,
    /// Queued command RESP bytes
    queue: std.ArrayList(QueuedCommand),
    /// Keys being watched (owned copies of key strings)
    watched_keys: std.ArrayList([]const u8),
    /// Whether a watched key was modified — causes EXEC to return null array
    dirty: bool,

    pub fn init(allocator: std.mem.Allocator) TxState {
        return TxState{
            .allocator = allocator,
            .active = false,
            .queue = std.ArrayList(QueuedCommand){},
            .watched_keys = std.ArrayList([]const u8){},
            .dirty = false,
        };
    }

    pub fn deinit(self: *TxState) void {
        for (self.queue.items) |*qc| {
            qc.deinit(self.allocator);
        }
        self.queue.deinit(self.allocator);

        for (self.watched_keys.items) |key| {
            self.allocator.free(key);
        }
        self.watched_keys.deinit(self.allocator);
    }

    /// Reset the transaction state (after EXEC or DISCARD)
    pub fn reset(self: *TxState) void {
        for (self.queue.items) |*qc| {
            qc.deinit(self.allocator);
        }
        self.queue.clearRetainingCapacity();

        for (self.watched_keys.items) |key| {
            self.allocator.free(key);
        }
        self.watched_keys.clearRetainingCapacity();

        self.active = false;
        self.dirty = false;
    }

    /// Queue a raw command (takes ownership of `data`)
    pub fn enqueue(self: *TxState, data: []const u8) !void {
        try self.queue.append(self.allocator, QueuedCommand{ .data = data });
    }

    /// Watch a key; marks dirty if key is already dirty
    pub fn watch(self: *TxState, key: []const u8) !void {
        const owned = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned);
        try self.watched_keys.append(self.allocator, owned);
    }

    /// Check if a key is being watched
    pub fn isWatched(self: *const TxState, key: []const u8) bool {
        for (self.watched_keys.items) |wk| {
            if (std.mem.eql(u8, wk, key)) return true;
        }
        return false;
    }
};

/// Handle MULTI command — begin a transaction block
/// Returns +OK or error if already in transaction
pub fn cmdMulti(allocator: std.mem.Allocator, tx: *TxState, args: []const RespValue) ![]const u8 {
    _ = args;
    var w = Writer.init(allocator);
    defer w.deinit();

    if (tx.active) {
        return w.writeError("ERR MULTI calls can not be nested");
    }

    tx.active = true;
    return w.writeOK();
}

/// Handle DISCARD command — abort a transaction block
pub fn cmdDiscard(allocator: std.mem.Allocator, tx: *TxState, args: []const RespValue) ![]const u8 {
    _ = args;
    var w = Writer.init(allocator);
    defer w.deinit();

    if (!tx.active) {
        return w.writeError("ERR DISCARD without MULTI");
    }

    tx.reset();
    return w.writeOK();
}

/// Handle WATCH key [key ...] — watch keys for modification
pub fn cmdWatch(allocator: std.mem.Allocator, tx: *TxState, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (tx.active) {
        return w.writeError("ERR WATCH inside MULTI is not allowed");
    }

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'watch' command");
    }

    for (args[1..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR wrong type of argument"),
        };
        try tx.watch(key);
    }

    return w.writeOK();
}

/// Handle UNWATCH command — unwatch all keys
pub fn cmdUnwatch(allocator: std.mem.Allocator, tx: *TxState, args: []const RespValue) ![]const u8 {
    _ = args;
    var w = Writer.init(allocator);
    defer w.deinit();

    for (tx.watched_keys.items) |key| {
        tx.allocator.free(key);
    }
    tx.watched_keys.clearRetainingCapacity();
    tx.dirty = false;

    return w.writeOK();
}

/// Check if any watched keys have been modified by a write command.
/// This is called from the main command dispatcher after every write command.
/// `modified_key` is the key that was just written.
pub fn markWatchedDirty(tx: *TxState, modified_key: []const u8) void {
    if (tx.isWatched(modified_key)) {
        tx.dirty = true;
    }
}

// ─── Unit Tests ───────────────────────────────────────────────────────────────

test "TxState init and deinit" {
    const allocator = std.testing.allocator;
    var tx = TxState.init(allocator);
    defer tx.deinit();

    try std.testing.expect(!tx.active);
    try std.testing.expect(!tx.dirty);
    try std.testing.expectEqual(@as(usize, 0), tx.queue.items.len);
    try std.testing.expectEqual(@as(usize, 0), tx.watched_keys.items.len);
}

test "TxState enqueue and reset" {
    const allocator = std.testing.allocator;
    var tx = TxState.init(allocator);
    defer tx.deinit();

    const data = try allocator.dupe(u8, "*1\r\n$4\r\nPING\r\n");
    try tx.enqueue(data);
    try std.testing.expectEqual(@as(usize, 1), tx.queue.items.len);

    tx.reset();
    try std.testing.expectEqual(@as(usize, 0), tx.queue.items.len);
    try std.testing.expect(!tx.active);
}

test "TxState watch and isWatched" {
    const allocator = std.testing.allocator;
    var tx = TxState.init(allocator);
    defer tx.deinit();

    try tx.watch("mykey");
    try std.testing.expect(tx.isWatched("mykey"));
    try std.testing.expect(!tx.isWatched("otherkey"));
}

test "TxState markWatchedDirty" {
    const allocator = std.testing.allocator;
    var tx = TxState.init(allocator);
    defer tx.deinit();

    try tx.watch("key1");
    try std.testing.expect(!tx.dirty);

    markWatchedDirty(&tx, "key2");
    try std.testing.expect(!tx.dirty); // key2 not watched

    markWatchedDirty(&tx, "key1");
    try std.testing.expect(tx.dirty); // key1 is watched
}

test "cmdMulti returns OK" {
    const allocator = std.testing.allocator;
    var tx = TxState.init(allocator);
    defer tx.deinit();

    const args = [_]RespValue{RespValue{ .bulk_string = "MULTI" }};
    const result = try cmdMulti(allocator, &tx, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expect(tx.active);
}

test "cmdMulti nested returns error" {
    const allocator = std.testing.allocator;
    var tx = TxState.init(allocator);
    defer tx.deinit();

    tx.active = true;
    const args = [_]RespValue{RespValue{ .bulk_string = "MULTI" }};
    const result = try cmdMulti(allocator, &tx, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-"));
}

test "cmdDiscard without MULTI returns error" {
    const allocator = std.testing.allocator;
    var tx = TxState.init(allocator);
    defer tx.deinit();

    const args = [_]RespValue{RespValue{ .bulk_string = "DISCARD" }};
    const result = try cmdDiscard(allocator, &tx, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-"));
}

test "cmdDiscard with MULTI resets state" {
    const allocator = std.testing.allocator;
    var tx = TxState.init(allocator);
    defer tx.deinit();

    tx.active = true;
    const args = [_]RespValue{RespValue{ .bulk_string = "DISCARD" }};
    const result = try cmdDiscard(allocator, &tx, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expect(!tx.active);
}

test "cmdWatch outside MULTI" {
    const allocator = std.testing.allocator;
    var tx = TxState.init(allocator);
    defer tx.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "WATCH" },
        RespValue{ .bulk_string = "key1" },
    };
    const result = try cmdWatch(allocator, &tx, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expect(tx.isWatched("key1"));
}

test "cmdWatch inside MULTI returns error" {
    const allocator = std.testing.allocator;
    var tx = TxState.init(allocator);
    defer tx.deinit();

    tx.active = true;
    const args = [_]RespValue{
        RespValue{ .bulk_string = "WATCH" },
        RespValue{ .bulk_string = "key1" },
    };
    const result = try cmdWatch(allocator, &tx, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-"));
}
