const std = @import("std");
const Storage = @import("../storage/memory.zig").Storage;
const RespWriter = @import("../protocol/writer.zig").RespWriter;

/// SETBIT key offset value
/// Set or clear the bit at offset in the string value stored at key
pub fn cmdSetbit(
    storage: *Storage,
    args: []const []const u8,
    writer: anytype,
    _: std.mem.Allocator,
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
    else {
        try RespWriter.writeError(writer, "ERR syntax error");
        return;
    };

    const destkey = args[2];
    const srckeys = args[3..];

    // NOT requires exactly one source key
    if (operation == .NOT and srckeys.len != 1) {
        try RespWriter.writeError(writer, "ERR BITOP NOT must be called with a single source key");
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

    _ = allocator;
    try RespWriter.writeInteger(writer, @intCast(result_len));
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
