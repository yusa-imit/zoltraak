const std = @import("std");
const memory = @import("../storage/memory.zig");
const Storage = memory.Storage;
const RespWriter = @import("../protocol/writer.zig").RespWriter;
const notifications_mod = @import("../storage/notifications.zig");
const pubsub_mod = @import("../storage/pubsub.zig");

const PubSub = pubsub_mod.PubSub;

/// Helper to publish bitmap/bitfield notifications
fn notifyBitmapEvent(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub_state: *PubSub,
    db_index: u32,
    key: []const u8,
    event_flag: notifications_mod.NotificationFlag,
    event_name: []const u8,
) void {
    const config_value = storage.config.getAsString("notify-keyspace-events") catch return;
    const config_str = config_value orelse return;
    const flags = notifications_mod.parseNotificationFlags(config_str);

    if (!notifications_mod.shouldNotify(flags, event_flag)) return;

    notifications_mod.publishNotification(allocator, pubsub_state, db_index, key, event_name, flags) catch {};
}

/// SETBIT key offset value
/// Set or clear the bit at offset in the string value stored at key
pub fn cmdSetbit(
    storage: *Storage,
    args: []const []const u8,
    writer: anytype,
    allocator: std.mem.Allocator,
    ps: *PubSub,
    db_index: u32,
) !void {
    if (args.len != 4) {
        try RespWriter.writeError(writer, "ERR wrong number of arguments for 'setbit' command");
        return;
    }

    const key = args[1];
    const offset = std.fmt.parseInt(usize, args[2], 10) catch {
        try RespWriter.writeError(writer, "ERR bit offset is not an integer or out of range");
        return;
    };

    const value_int = std.fmt.parseInt(u8, args[3], 10) catch {
        try RespWriter.writeError(writer, "ERR bit is not an integer or out of range");
        return;
    };

    if (value_int > 1) {
        try RespWriter.writeError(writer, "ERR bit is not an integer or out of range");
        return;
    }

    const value: u1 = @intCast(value_int);

    const original_bit = storage.setbit(key, offset, value) catch |err| switch (err) {
        error.WrongType => {
            try RespWriter.writeError(writer, "WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        },
        error.OutOfMemory => {
            try RespWriter.writeError(writer, "ERR out of memory");
            return;
        },
    };

    // Fire notification only if bit changed
    if (original_bit != value) {
        notifyBitmapEvent(allocator, storage, ps, db_index, key, .string, "setbit");
    }

    try RespWriter.writeInteger(writer, original_bit);
}

/// GETBIT key offset
/// Returns the bit value at offset in the string value stored at key
pub fn cmdGetbit(
    storage: *Storage,
    args: []const []const u8,
    writer: anytype,
    _: std.mem.Allocator,
) !void {
    if (args.len != 3) {
        try RespWriter.writeError(writer, "ERR wrong number of arguments for 'getbit' command");
        return;
    }

    const key = args[1];
    const offset = std.fmt.parseInt(usize, args[2], 10) catch {
        try RespWriter.writeError(writer, "ERR bit offset is not an integer or out of range");
        return;
    };

    const bit = storage.getbit(key, offset) catch |err| switch (err) {
        error.WrongType => {
            try RespWriter.writeError(writer, "WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        },
    };

    try RespWriter.writeInteger(writer, bit);
}

/// BITCOUNT key [start end]
/// Count the number of set bits (population counting) in a string
pub fn cmdBitcount(
    storage: *Storage,
    args: []const []const u8,
    writer: anytype,
    _: std.mem.Allocator,
) !void {
    if (args.len != 2 and args.len != 4) {
        try RespWriter.writeError(writer, "ERR wrong number of arguments for 'bitcount' command");
        return;
    }

    const key = args[1];

    const start: ?i64 = if (args.len == 4) blk: {
        break :blk std.fmt.parseInt(i64, args[2], 10) catch {
            try RespWriter.writeError(writer, "ERR value is not an integer or out of range");
            return;
        };
    } else null;

    const end: ?i64 = if (args.len == 4) blk: {
        break :blk std.fmt.parseInt(i64, args[3], 10) catch {
            try RespWriter.writeError(writer, "ERR value is not an integer or out of range");
            return;
        };
    } else null;

    const count = storage.bitcount(key, start, end) catch |err| switch (err) {
        error.WrongType => {
            try RespWriter.writeError(writer, "WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        },
    };

    try RespWriter.writeInteger(writer, count);
}

/// BITOP operation destkey key [key ...]
/// Perform bitwise operations between strings
pub fn cmdBitop(
    storage: *Storage,
    args: []const []const u8,
    writer: anytype,
    allocator: std.mem.Allocator,
    ps: *PubSub,
    db_index: u32,
) !void {
    if (args.len < 4) {
        try RespWriter.writeError(writer, "ERR wrong number of arguments for 'bitop' command");
        return;
    }

    // Parse operation
    const op_str = args[1];
    const operation: Storage.BitOp = if (std.ascii.eqlIgnoreCase(op_str, "AND"))
        .AND
    else if (std.ascii.eqlIgnoreCase(op_str, "OR"))
        .OR
    else if (std.ascii.eqlIgnoreCase(op_str, "XOR"))
        .XOR
    else if (std.ascii.eqlIgnoreCase(op_str, "NOT"))
        .NOT
    else if (std.ascii.eqlIgnoreCase(op_str, "DIFF"))
        .DIFF
    else if (std.ascii.eqlIgnoreCase(op_str, "DIFF1"))
        .DIFF1
    else if (std.ascii.eqlIgnoreCase(op_str, "ANDOR"))
        .ANDOR
    else if (std.ascii.eqlIgnoreCase(op_str, "ONE"))
        .ONE
    else {
        try RespWriter.writeError(writer, "ERR syntax error");
        return;
    };

    const destkey = args[2];
    const srckeys = args[3..];

    // Validate operation-specific requirements
    if (operation == .NOT and srckeys.len != 1) {
        try RespWriter.writeError(writer, "ERR BITOP NOT must be called with a single source key");
        return;
    }
    if (operation == .DIFF and srckeys.len < 2) {
        try RespWriter.writeError(writer, "ERR BITOP DIFF requires at least 2 source keys");
        return;
    }
    if (operation == .DIFF1 and srckeys.len != 2) {
        try RespWriter.writeError(writer, "ERR BITOP DIFF1 must be called with exactly 2 source keys");
        return;
    }
    if (operation == .ANDOR and srckeys.len % 2 != 0) {
        try RespWriter.writeError(writer, "ERR BITOP ANDOR requires an even number of source keys");
        return;
    }

    const result_len = storage.bitop(operation, destkey, srckeys) catch |err| switch (err) {
        error.WrongType => {
            try RespWriter.writeError(writer, "WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        },
        error.OutOfMemory => {
            try RespWriter.writeError(writer, "ERR out of memory");
            return;
        },
    };

    // Fire appropriate notification based on result
    if (result_len > 0) {
        // Result has content, fire "set" event
        notifyBitmapEvent(allocator, storage, ps, db_index, destkey, .string, "set");
    } else {
        // Result is empty, check if destkey existed and fire "del" if needed
        const destkey_existed = storage.get(destkey) != null;
        if (destkey_existed) {
            notifyBitmapEvent(allocator, storage, ps, db_index, destkey, .generic, "del");
        }
    }

    try RespWriter.writeInteger(writer, @intCast(result_len));
}

/// BITPOS key bit [start [end [BYTE|BIT]]]
/// Find first bit set to 0 or 1 in a string
pub fn cmdBitpos(
    storage: *Storage,
    args: []const []const u8,
    writer: anytype,
    _: std.mem.Allocator,
) !void {
    if (args.len < 3 or args.len > 6) {
        try RespWriter.writeError(writer, "ERR wrong number of arguments for 'bitpos' command");
        return;
    }

    const key = args[1];

    const bit_int = std.fmt.parseInt(u8, args[2], 10) catch {
        try RespWriter.writeError(writer, "ERR bit must be 0 or 1");
        return;
    };

    if (bit_int > 1) {
        try RespWriter.writeError(writer, "ERR bit must be 0 or 1");
        return;
    }

    const bit: u1 = @intCast(bit_int);

    const start: ?i64 = if (args.len >= 4) blk: {
        break :blk std.fmt.parseInt(i64, args[3], 10) catch {
            try RespWriter.writeError(writer, "ERR value is not an integer or out of range");
            return;
        };
    } else null;

    const end: ?i64 = if (args.len >= 5) blk: {
        break :blk std.fmt.parseInt(i64, args[4], 10) catch {
            try RespWriter.writeError(writer, "ERR value is not an integer or out of range");
            return;
        };
    } else null;

    const unit: memory.RangeUnit = if (args.len == 6) blk: {
        if (std.ascii.eqlIgnoreCase(args[5], "BYTE")) {
            break :blk .byte;
        } else if (std.ascii.eqlIgnoreCase(args[5], "BIT")) {
            break :blk .bit;
        } else {
            try RespWriter.writeError(writer, "ERR syntax error");
            return;
        }
    } else .byte;

    const position = storage.bitpos(key, bit, start, end, unit) catch |err| switch (err) {
        error.WrongType => {
            try RespWriter.writeError(writer, "WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        },
    };

    try RespWriter.writeInteger(writer, position);
}

test "SETBIT command" {
    const testing = std.testing;
    var storage = Storage.init(testing.allocator);
    defer storage.deinit();

    var buf = std.ArrayList(u8){};
    defer buf.deinit(testing.allocator);
    const writer = buf.writer(testing.allocator);

    // SETBIT mykey 7 1
    try cmdSetbit(&storage, &[_][]const u8{ "SETBIT", "mykey", "7", "1" }, writer, testing.allocator);
    try testing.expectEqualStrings(":0\r\n", buf.items);
    buf.clearRetainingCapacity();

    // SETBIT mykey 7 0 (should return 1)
    try cmdSetbit(&storage, &[_][]const u8{ "SETBIT", "mykey", "7", "0" }, writer, testing.allocator);
    try testing.expectEqualStrings(":1\r\n", buf.items);
}

test "GETBIT command" {
    const testing = std.testing;
    var storage = Storage.init(testing.allocator);
    defer storage.deinit();

    var buf = std.ArrayList(u8){};
    defer buf.deinit(testing.allocator);
    const writer = buf.writer(testing.allocator);

    // Set a bit first
    _ = try storage.setbit("mykey", 7, 1);

    // GETBIT mykey 7
    try cmdGetbit(&storage, &[_][]const u8{ "GETBIT", "mykey", "7" }, writer, testing.allocator);
    try testing.expectEqualStrings(":1\r\n", buf.items);
    buf.clearRetainingCapacity();

    // GETBIT mykey 0
    try cmdGetbit(&storage, &[_][]const u8{ "GETBIT", "mykey", "0" }, writer, testing.allocator);
    try testing.expectEqualStrings(":0\r\n", buf.items);
}

test "BITCOUNT command" {
    const testing = std.testing;
    var storage = Storage.init(testing.allocator);
    defer storage.deinit();

    var buf = std.ArrayList(u8){};
    defer buf.deinit(testing.allocator);
    const writer = buf.writer(testing.allocator);

    // Set some bits
    _ = try storage.setbit("mykey", 0, 1);
    _ = try storage.setbit("mykey", 1, 1);
    _ = try storage.setbit("mykey", 8, 1);

    // BITCOUNT mykey
    try cmdBitcount(&storage, &[_][]const u8{ "BITCOUNT", "mykey" }, writer, testing.allocator);
    try testing.expectEqualStrings(":3\r\n", buf.items);
}

test "BITOP command" {
    const testing = std.testing;
    var storage = Storage.init(testing.allocator);
    defer storage.deinit();

    var buf = std.ArrayList(u8){};
    defer buf.deinit(testing.allocator);
    const writer = buf.writer(testing.allocator);

    // Set up test data
    try storage.set("key1", "foo", null);
    try storage.set("key2", "bar", null);

    // BITOP AND dest key1 key2
    try cmdBitop(&storage, &[_][]const u8{ "BITOP", "AND", "dest", "key1", "key2" }, writer, testing.allocator);
    try testing.expectEqualStrings(":3\r\n", buf.items);
}

test "BITPOS command - find first 1" {
    const testing = std.testing;
    var storage = Storage.init(testing.allocator);
    defer storage.deinit();

    var buf = std.ArrayList(u8){};
    defer buf.deinit(testing.allocator);
    const writer = buf.writer(testing.allocator);

    // Set bits: byte 0 = 0b00000000, byte 1 = 0b10000000 (bit 8 is set)
    _ = try storage.setbit("mykey", 8, 1);

    // BITPOS mykey 1 - should find bit 8
    try cmdBitpos(&storage, &[_][]const u8{ "BITPOS", "mykey", "1" }, writer, testing.allocator);
    try testing.expectEqualStrings(":8\r\n", buf.items);
}

test "BITPOS command - find first 0" {
    const testing = std.testing;
    var storage = Storage.init(testing.allocator);
    defer storage.deinit();

    var buf = std.ArrayList(u8){};
    defer buf.deinit(testing.allocator);
    const writer = buf.writer(testing.allocator);

    // Set all bits in byte 0
    for (0..8) |i| {
        _ = try storage.setbit("mykey", i, 1);
    }

    // BITPOS mykey 0 - should find bit 8 (first bit of byte 1, which is 0)
    try cmdBitpos(&storage, &[_][]const u8{ "BITPOS", "mykey", "0" }, writer, testing.allocator);
    try testing.expectEqualStrings(":8\r\n", buf.items);
}

test "BITPOS command - with range" {
    const testing = std.testing;
    var storage = Storage.init(testing.allocator);
    defer storage.deinit();

    var buf = std.ArrayList(u8){};
    defer buf.deinit(testing.allocator);
    const writer = buf.writer(testing.allocator);

    // Set bit 16 (byte 2, bit 0)
    _ = try storage.setbit("mykey", 16, 1);

    // BITPOS mykey 1 0 1 - search in bytes 0-1, should not find (returns -1)
    try cmdBitpos(&storage, &[_][]const u8{ "BITPOS", "mykey", "1", "0", "1" }, writer, testing.allocator);
    try testing.expectEqualStrings(":-1\r\n", buf.items);
    buf.clearRetainingCapacity();

    // BITPOS mykey 1 2 2 - search in byte 2, should find bit 16
    try cmdBitpos(&storage, &[_][]const u8{ "BITPOS", "mykey", "1", "2", "2" }, writer, testing.allocator);
    try testing.expectEqualStrings(":16\r\n", buf.items);
}

test "BITPOS command - non-existent key" {
    const testing = std.testing;
    var storage = Storage.init(testing.allocator);
    defer storage.deinit();

    var buf = std.ArrayList(u8){};
    defer buf.deinit(testing.allocator);
    const writer = buf.writer(testing.allocator);

    // BITPOS nonexistent 1 - should return -1
    try cmdBitpos(&storage, &[_][]const u8{ "BITPOS", "nonexistent", "1" }, writer, testing.allocator);
    try testing.expectEqualStrings(":-1\r\n", buf.items);
    buf.clearRetainingCapacity();

    // BITPOS nonexistent 0 - should return 0 (first bit is conceptually 0)
    try cmdBitpos(&storage, &[_][]const u8{ "BITPOS", "nonexistent", "0" }, writer, testing.allocator);
    try testing.expectEqualStrings(":0\r\n", buf.items);
}

test "BITPOS command - BIT mode with positive indices" {
    const testing = std.testing;
    var storage = Storage.init(testing.allocator);
    defer storage.deinit();

    var buf = std.ArrayList(u8){};
    defer buf.deinit(testing.allocator);
    const writer = buf.writer(testing.allocator);

    // Set bit 10: byte 1, bit 2 (within 0b00100000)
    _ = try storage.setbit("mykey", 10, 1);

    // BITPOS mykey 1 0 15 BIT - search bits 0-15, should find bit 10
    try cmdBitpos(&storage, &[_][]const u8{ "BITPOS", "mykey", "1", "0", "15", "BIT" }, writer, testing.allocator);
    try testing.expectEqualStrings(":10\r\n", buf.items);
    buf.clearRetainingCapacity();

    // BITPOS mykey 1 0 9 BIT - search bits 0-9, should not find (returns -1)
    try cmdBitpos(&storage, &[_][]const u8{ "BITPOS", "mykey", "1", "0", "9", "BIT" }, writer, testing.allocator);
    try testing.expectEqualStrings(":-1\r\n", buf.items);
    buf.clearRetainingCapacity();

    // BITPOS mykey 1 10 10 BIT - exact bit match
    try cmdBitpos(&storage, &[_][]const u8{ "BITPOS", "mykey", "1", "10", "10", "BIT" }, writer, testing.allocator);
    try testing.expectEqualStrings(":10\r\n", buf.items);
}

test "BITPOS command - BIT mode with negative indices" {
    const testing = std.testing;
    var storage = Storage.init(testing.allocator);
    defer storage.deinit();

    var buf = std.ArrayList(u8){};
    defer buf.deinit(testing.allocator);
    const writer = buf.writer(testing.allocator);

    // Set bits to create pattern: 3 bytes = 24 bits
    // Byte 0: 0xFF (all 1s), Byte 1: 0x00, Byte 2: 0x80 (bit 16 set)
    for (0..8) |i| {
        _ = try storage.setbit("mykey", i, 1);
    }
    _ = try storage.setbit("mykey", 16, 1);

    // BITPOS mykey 0 -16 -1 BIT - last 16 bits (8-23), first 0 should be bit 8
    try cmdBitpos(&storage, &[_][]const u8{ "BITPOS", "mykey", "0", "-16", "-1", "BIT" }, writer, testing.allocator);
    try testing.expectEqualStrings(":8\r\n", buf.items);
    buf.clearRetainingCapacity();

    // BITPOS mykey 1 -8 -1 BIT - last byte (16-23), should find bit 16
    try cmdBitpos(&storage, &[_][]const u8{ "BITPOS", "mykey", "1", "-8", "-1", "BIT" }, writer, testing.allocator);
    try testing.expectEqualStrings(":16\r\n", buf.items);
}

test "BITPOS command - BIT vs BYTE mode comparison" {
    const testing = std.testing;
    var storage = Storage.init(testing.allocator);
    defer storage.deinit();

    var buf = std.ArrayList(u8){};
    defer buf.deinit(testing.allocator);
    const writer = buf.writer(testing.allocator);

    // Set bit 20: byte 2, bit 4
    _ = try storage.setbit("mykey", 20, 1);

    // BYTE mode: search bytes 2-2 (16 bits to 23 bits)
    try cmdBitpos(&storage, &[_][]const u8{ "BITPOS", "mykey", "1", "2", "2" }, writer, testing.allocator);
    try testing.expectEqualStrings(":20\r\n", buf.items);
    buf.clearRetainingCapacity();

    // BIT mode: search bits 16-23 (same range as byte 2)
    try cmdBitpos(&storage, &[_][]const u8{ "BITPOS", "mykey", "1", "16", "23", "BIT" }, writer, testing.allocator);
    try testing.expectEqualStrings(":20\r\n", buf.items);
    buf.clearRetainingCapacity();

    // BIT mode: narrow search bits 20-22 (mid-byte range)
    try cmdBitpos(&storage, &[_][]const u8{ "BITPOS", "mykey", "1", "20", "22", "BIT" }, writer, testing.allocator);
    try testing.expectEqualStrings(":20\r\n", buf.items);
    buf.clearRetainingCapacity();

    // BIT mode: search bits 21-22 (after target bit)
    try cmdBitpos(&storage, &[_][]const u8{ "BITPOS", "mykey", "1", "21", "22", "BIT" }, writer, testing.allocator);
    try testing.expectEqualStrings(":-1\r\n", buf.items);
}

test "BITPOS command - invalid BYTE|BIT modifier" {
    const testing = std.testing;
    var storage = Storage.init(testing.allocator);
    defer storage.deinit();

    var buf = std.ArrayList(u8){};
    defer buf.deinit(testing.allocator);
    const writer = buf.writer(testing.allocator);

    // Invalid modifier should return error
    try cmdBitpos(&storage, &[_][]const u8{ "BITPOS", "mykey", "1", "0", "10", "INVALID" }, writer, testing.allocator);
    try testing.expect(std.mem.startsWith(u8, buf.items, "-ERR"));
    buf.clearRetainingCapacity();

    // BYTE modifier should be accepted (case insensitive)
    _ = try storage.setbit("mykey", 10, 1);
    try cmdBitpos(&storage, &[_][]const u8{ "BITPOS", "mykey", "1", "1", "1", "BYTE" }, writer, testing.allocator);
    try testing.expectEqualStrings(":10\r\n", buf.items);
}

// ─────────────────────────────────────────────────────────────────────────────
// Keyspace Notification Support (Iteration 255)
// ─────────────────────────────────────────────────────────────────────────────
//
// TODO for zig-implementor:
// 1. Add imports:
//    const notifications_mod = @import("../storage/notifications.zig");
//    const pubsub_mod = @import("../storage/pubsub.zig");
//    const PubSub = pubsub_mod.PubSub;
//
// 2. Add helper function:
//    fn notifyBitmapEvent(allocator, storage, pubsub_state, db_index, key, event_flag, event_name) void {
//        const config_value = storage.config.getAsString("notify-keyspace-events") catch return;
//        const config_str = config_value orelse return;
//        const flags = notifications_mod.parseNotificationFlags(config_str);
//        if (!notifications_mod.shouldNotify(flags, event_flag)) return;
//        notifications_mod.publishNotification(allocator, pubsub_state, db_index, key, event_name, flags) catch {};
//    }
//
// 3. Update cmdSetbit signature: add `ps: *PubSub, db_index: u32` parameters
//    - Call notifyBitmapEvent(..., .string, "setbit") ONLY when bit changes
//    - Check: if (original_bit != value) { notifyBitmapEvent(...); }
//
// 4. Update cmdBitop signature: add `ps: *PubSub, db_index: u32` parameters
//    - After storage.bitop(), check result_len
//    - If result_len > 0: notifyBitmapEvent(..., .string, "set")
//    - Else if destkey existed: notifyBitmapEvent(..., .generic, "del")
//
// 5. Update command routing in server.zig to pass ps and db_index
//
// See tests/test_bitmap_notifications.zig for comprehensive notification tests
