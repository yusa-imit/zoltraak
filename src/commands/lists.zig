const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;

/// LPUSH key element [element ...]
/// Prepends one or more elements to the list head
/// Returns integer - length of list after push
pub fn cmdLpush(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'lpush' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Extract all elements
    var elements = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 2);
    defer elements.deinit(allocator);

    for (args[2..]) |arg| {
        const elem = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid element"),
        };
        try elements.append(allocator, elem);
    }

    // Execute LPUSH
    const length = storage.lpush(key, elements.items, null) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(length));
}

/// RPUSH key element [element ...]
/// Appends one or more elements to the list tail
/// Returns integer - length of list after push
pub fn cmdRpush(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'rpush' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Extract all elements
    var elements = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 2);
    defer elements.deinit(allocator);

    for (args[2..]) |arg| {
        const elem = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid element"),
        };
        try elements.append(allocator, elem);
    }

    // Execute RPUSH
    const length = storage.rpush(key, elements.items, null) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(length));
}

/// LPOP key [count]
/// Removes and returns the first element(s) from the list head
/// Without count: returns single bulk string
/// With count: returns array of bulk strings
pub fn cmdLpop(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2 or args.len > 3) {
        return w.writeError("ERR wrong number of arguments for 'lpop' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse optional count parameter
    var count: usize = 1;
    var has_count_param = false;
    if (args.len == 3) {
        has_count_param = true;
        const count_str = switch (args[2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid count"),
        };
        const count_i64 = std.fmt.parseInt(i64, count_str, 10) catch {
            return w.writeError("ERR value is not an integer or out of range");
        };
        if (count_i64 < 0) {
            return w.writeError("ERR value is out of range, must be positive");
        }
        count = @intCast(count_i64);
    }

    // Execute LPOP
    const result = (try storage.lpop(allocator, key, count)) orelse {
        // Key doesn't exist or is not a list
        if (!has_count_param) {
            return w.writeNull();
        } else {
            return w.writeArray(&[_]RespValue{});
        }
    };
    defer {
        for (result) |elem| allocator.free(elem);
        allocator.free(result);
    }

    // Format response based on count parameter
    if (!has_count_param) {
        // No count parameter - return single bulk string
        if (result.len > 0) {
            return w.writeBulkString(result[0]);
        } else {
            return w.writeNull();
        }
    } else {
        // Count parameter provided - return array
        var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, result.len);
        defer resp_values.deinit(allocator);

        for (result) |elem| {
            try resp_values.append(allocator, RespValue{ .bulk_string = elem });
        }

        return w.writeArray(resp_values.items);
    }
}

/// RPOP key [count]
/// Removes and returns the last element(s) from the list tail
/// Without count: returns single bulk string
/// With count: returns array of bulk strings
pub fn cmdRpop(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2 or args.len > 3) {
        return w.writeError("ERR wrong number of arguments for 'rpop' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse optional count parameter
    var count: usize = 1;
    var has_count_param = false;
    if (args.len == 3) {
        has_count_param = true;
        const count_str = switch (args[2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid count"),
        };
        const count_i64 = std.fmt.parseInt(i64, count_str, 10) catch {
            return w.writeError("ERR value is not an integer or out of range");
        };
        if (count_i64 < 0) {
            return w.writeError("ERR value is out of range, must be positive");
        }
        count = @intCast(count_i64);
    }

    // Execute RPOP
    const result = (try storage.rpop(allocator, key, count)) orelse {
        // Key doesn't exist or is not a list
        if (!has_count_param) {
            return w.writeNull();
        } else {
            return w.writeArray(&[_]RespValue{});
        }
    };
    defer {
        for (result) |elem| allocator.free(elem);
        allocator.free(result);
    }

    // Format response based on count parameter
    if (!has_count_param) {
        // No count parameter - return single bulk string
        if (result.len > 0) {
            return w.writeBulkString(result[0]);
        } else {
            return w.writeNull();
        }
    } else {
        // Count parameter provided - return array
        var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, result.len);
        defer resp_values.deinit(allocator);

        for (result) |elem| {
            try resp_values.append(allocator, RespValue{ .bulk_string = elem });
        }

        return w.writeArray(resp_values.items);
    }
}

/// LRANGE key start stop
/// Returns elements in the specified range (both indices inclusive)
/// Supports negative indices (-1 = last element, -2 = penultimate, etc.)
pub fn cmdLrange(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'lrange' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const start_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid start index"),
    };

    const stop_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid stop index"),
    };

    const start = std.fmt.parseInt(i64, start_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    const stop = std.fmt.parseInt(i64, stop_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    // Execute LRANGE
    const result = (try storage.lrange(allocator, key, start, stop)) orelse {
        // Key doesn't exist or is not a list - return empty array
        return w.writeArray(&[_]RespValue{});
    };
    defer allocator.free(result);

    // Convert to RespValue array
    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, result.len);
    defer resp_values.deinit(allocator);

    for (result) |elem| {
        try resp_values.append(allocator, RespValue{ .bulk_string = elem });
    }

    return w.writeArray(resp_values.items);
}

/// LLEN key
/// Returns the length of the list stored at key
/// Returns 0 if key doesn't exist
pub fn cmdLlen(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'llen' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Check type first to provide proper WRONGTYPE error
    const value_type = storage.getType(key);
    if (value_type) |vtype| {
        if (vtype != .list) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }

    // Execute LLEN
    const length = storage.llen(key) orelse 0;
    return w.writeInteger(@intCast(length));
}

// Embedded unit tests

test "lists - LPUSH single element" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "hello" },
    };

    const result = try cmdLpush(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
}

test "lists - LPUSH multiple elements" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };

    const result = try cmdLpush(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":3\r\n", result);
}

test "lists - LPUSH on existing string returns WRONGTYPE" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "string", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LPUSH" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "value" },
    };

    const result = try cmdLpush(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "WRONGTYPE") != null);
}

test "lists - RPUSH single element" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "hello" },
    };

    const result = try cmdRpush(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
}

test "lists - RPUSH multiple elements" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };

    const result = try cmdRpush(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":3\r\n", result);
}

test "lists - RPUSH on existing string returns WRONGTYPE" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "string", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "value" },
    };

    const result = try cmdRpush(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "WRONGTYPE") != null);
}

test "lists - LPOP without count parameter" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    const setup_result = try cmdRpush(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test LPOP
    const args = [_]RespValue{
        RespValue{ .bulk_string = "LPOP" },
        RespValue{ .bulk_string = "mylist" },
    };

    const result = try cmdLpop(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$1\r\na\r\n", result);
}

test "lists - LPOP with count parameter" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    const setup_result = try cmdRpush(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test LPOP with count
    const args = [_]RespValue{
        RespValue{ .bulk_string = "LPOP" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "2" },
    };

    const result = try cmdLpop(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
}

test "lists - LPOP on non-existent key returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LPOP" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    const result = try cmdLpop(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "lists - LPOP count greater than length" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
    };
    const setup_result = try cmdRpush(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test LPOP with count > length
    const args = [_]RespValue{
        RespValue{ .bulk_string = "LPOP" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "10" },
    };

    const result = try cmdLpop(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
}

test "lists - RPOP without count parameter" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    const setup_result = try cmdRpush(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test RPOP
    const args = [_]RespValue{
        RespValue{ .bulk_string = "RPOP" },
        RespValue{ .bulk_string = "mylist" },
    };

    const result = try cmdRpop(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$1\r\nc\r\n", result);
}

test "lists - RPOP with count parameter" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    const setup_result = try cmdRpush(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test RPOP with count
    const args = [_]RespValue{
        RespValue{ .bulk_string = "RPOP" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "2" },
    };

    const result = try cmdRpop(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
}

test "lists - LRANGE all elements" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    const setup_result = try cmdRpush(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test LRANGE
    const args = [_]RespValue{
        RespValue{ .bulk_string = "LRANGE" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "-1" },
    };

    const result = try cmdLrange(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*3\r\n") != null);
}

test "lists - LRANGE with negative indices" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
        RespValue{ .bulk_string = "d" },
        RespValue{ .bulk_string = "e" },
    };
    const setup_result = try cmdRpush(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test LRANGE with negative indices
    const args = [_]RespValue{
        RespValue{ .bulk_string = "LRANGE" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "-3" },
        RespValue{ .bulk_string = "-1" },
    };

    const result = try cmdLrange(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*3\r\n") != null);
}

test "lists - LRANGE on non-existent key returns empty array" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LRANGE" },
        RespValue{ .bulk_string = "nosuchkey" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "-1" },
    };

    const result = try cmdLrange(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("*0\r\n", result);
}

test "lists - LLEN returns length" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    const setup_result = try cmdRpush(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test LLEN
    const args = [_]RespValue{
        RespValue{ .bulk_string = "LLEN" },
        RespValue{ .bulk_string = "mylist" },
    };

    const result = try cmdLlen(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":3\r\n", result);
}

test "lists - LLEN on non-existent key returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LLEN" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    const result = try cmdLlen(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "lists - LLEN on string key returns WRONGTYPE" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "string", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LLEN" },
        RespValue{ .bulk_string = "mykey" },
    };

    const result = try cmdLlen(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "WRONGTYPE") != null);
}

test "lists - integration LPUSH RPUSH LPOP RPOP" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // RPUSH a b
    const rpush1 = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
    };
    const r1 = try cmdRpush(allocator, storage, &rpush1);
    defer allocator.free(r1);

    // LPUSH x y
    const lpush1 = [_]RespValue{
        RespValue{ .bulk_string = "LPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "x" },
        RespValue{ .bulk_string = "y" },
    };
    const r2 = try cmdLpush(allocator, storage, &lpush1);
    defer allocator.free(r2);
    try std.testing.expectEqualStrings(":4\r\n", r2);

    // LRANGE 0 -1 should return [y, x, a, b]
    const lrange_args = [_]RespValue{
        RespValue{ .bulk_string = "LRANGE" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "-1" },
    };
    const r3 = try cmdLrange(allocator, storage, &lrange_args);
    defer allocator.free(r3);
    try std.testing.expect(std.mem.indexOf(u8, r3, "*4\r\n") != null);
}
