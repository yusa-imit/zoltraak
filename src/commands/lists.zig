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

// ── New list commands ─────────────────────────────────────────────────────────

/// LINDEX key index
/// Returns the element at index in the list (negative = from tail).
/// Returns null bulk string if key doesn't exist or index is out of range.
pub fn cmdLindex(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'lindex' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const index_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not an integer or out of range"),
    };

    const index = std.fmt.parseInt(i64, index_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    const result = storage.lindex(key, index) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    if (result) |elem| {
        return w.writeBulkString(elem);
    } else {
        return w.writeNull();
    }
}

/// LSET key index element
/// Sets the element at index in the list to element.
pub fn cmdLset(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'lset' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const index_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not an integer or out of range"),
    };

    const element = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid element"),
    };

    const index = std.fmt.parseInt(i64, index_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    storage.lset(key, index, element) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.NoSuchKey => return w.writeError("ERR no such key"),
        error.IndexOutOfRange => return w.writeError("ERR index out of range"),
        else => return err,
    };

    return w.writeSimpleString("OK");
}

/// LTRIM key start stop
/// Trims the list to the specified range [start, stop].
/// Returns +OK always (even if key doesn't exist).
pub fn cmdLtrim(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'ltrim' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const start_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not an integer or out of range"),
    };

    const stop_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not an integer or out of range"),
    };

    const start = std.fmt.parseInt(i64, start_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    const stop = std.fmt.parseInt(i64, stop_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    storage.ltrim(key, start, stop) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeSimpleString("OK");
}

/// LREM key count element
/// Removes `count` occurrences of `element` from the list.
/// count > 0: from head; count < 0: from tail; count == 0: all.
pub fn cmdLrem(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'lrem' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const count_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not an integer or out of range"),
    };

    const element = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid element"),
    };

    const count = std.fmt.parseInt(i64, count_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    const removed = storage.lrem(key, count, element) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(removed));
}

/// LPUSHX key element [element ...]
/// Prepends elements only if the key already exists as a list.
/// Returns 0 if key doesn't exist; returns new length otherwise.
pub fn cmdLpushx(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'lpushx' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    var elements = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 2);
    defer elements.deinit(allocator);

    for (args[2..]) |arg| {
        const elem = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid element"),
        };
        try elements.append(allocator, elem);
    }

    const length = storage.lpushx(key, elements.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(length orelse 0));
}

/// RPUSHX key element [element ...]
/// Appends elements only if the key already exists as a list.
/// Returns 0 if key doesn't exist; returns new length otherwise.
pub fn cmdRpushx(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'rpushx' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    var elements = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 2);
    defer elements.deinit(allocator);

    for (args[2..]) |arg| {
        const elem = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid element"),
        };
        try elements.append(allocator, elem);
    }

    const length = storage.rpushx(key, elements.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(length orelse 0));
}

/// LINSERT key BEFORE|AFTER pivot element
/// Inserts element before or after the first occurrence of pivot.
/// Returns new list length, -1 if pivot not found, 0 if key doesn't exist.
pub fn cmdLinsert(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 5) {
        return w.writeError("ERR wrong number of arguments for 'linsert' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const where_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    const pivot = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid pivot"),
    };

    const element = switch (args[4]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid element"),
    };

    const before = if (std.ascii.eqlIgnoreCase(where_str, "BEFORE"))
        true
    else if (std.ascii.eqlIgnoreCase(where_str, "AFTER"))
        false
    else
        return w.writeError("ERR syntax error");

    const result = storage.linsert(key, before, pivot, element) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(result);
}

/// LPOS key element [RANK rank] [COUNT num] [MAXLEN len]
/// Find positions of element in list.
/// Without COUNT: returns single integer or null bulk string.
/// With COUNT: returns array of integers.
pub fn cmdLpos(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'lpos' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const element = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid element"),
    };

    // Parse optional parameters
    var rank: i64 = 1;
    var count: ?usize = null;
    var maxlen: usize = 0;

    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        if (std.ascii.eqlIgnoreCase(opt, "RANK")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const val_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR value is not an integer or out of range"),
            };
            rank = std.fmt.parseInt(i64, val_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            if (rank == 0) return w.writeError("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list");
        } else if (std.ascii.eqlIgnoreCase(opt, "COUNT")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const val_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR value is not an integer or out of range"),
            };
            const n = std.fmt.parseInt(i64, val_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            if (n < 0) return w.writeError("ERR COUNT can't be negative");
            count = @intCast(n);
        } else if (std.ascii.eqlIgnoreCase(opt, "MAXLEN")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const val_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR value is not an integer or out of range"),
            };
            const n = std.fmt.parseInt(i64, val_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            if (n < 0) return w.writeError("ERR MAXLEN can't be negative");
            maxlen = @intCast(n);
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Storage call: count=0 means "all" in storage layer; we translate from command count
    const storage_count: usize = count orelse 1; // default to 1 if no COUNT given

    const positions = storage.lpos(allocator, key, element, rank, storage_count, maxlen) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };
    defer allocator.free(positions);

    if (count) |_| {
        // COUNT was given: return array
        var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, positions.len);
        defer resp_values.deinit(allocator);

        for (positions) |pos| {
            try resp_values.append(allocator, RespValue{ .integer = @intCast(pos) });
        }
        return w.writeArray(resp_values.items);
    } else {
        // No COUNT: return single integer or null bulk
        if (positions.len > 0) {
            return w.writeInteger(@intCast(positions[0]));
        } else {
            return w.writeNull();
        }
    }
}

/// LMOVE source dest LEFT|RIGHT LEFT|RIGHT
/// Atomically pops from source and pushes to destination.
/// Returns the moved element, or null bulk if source is empty/missing.
pub fn cmdLmove(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 5) {
        return w.writeError("ERR wrong number of arguments for 'lmove' command");
    }

    const src = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid source key"),
    };

    const dst = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid destination key"),
    };

    const src_dir = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    const dst_dir = switch (args[4]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    const src_left = if (std.ascii.eqlIgnoreCase(src_dir, "LEFT"))
        true
    else if (std.ascii.eqlIgnoreCase(src_dir, "RIGHT"))
        false
    else
        return w.writeError("ERR syntax error");

    const dst_left = if (std.ascii.eqlIgnoreCase(dst_dir, "LEFT"))
        true
    else if (std.ascii.eqlIgnoreCase(dst_dir, "RIGHT"))
        false
    else
        return w.writeError("ERR syntax error");

    const moved = storage.lmove(allocator, src, dst, src_left, dst_left) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };
    defer if (moved) |m| allocator.free(m);

    if (moved) |elem| {
        return w.writeBulkString(elem);
    } else {
        return w.writeNull();
    }
}

/// RPOPLPUSH source dest
/// Legacy command equivalent to LMOVE source dest RIGHT LEFT.
/// Pops from source tail, pushes to dest head.
pub fn cmdRpoplpush(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'rpoplpush' command");
    }

    const src = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid source key"),
    };

    const dst = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid destination key"),
    };

    // RPOPLPUSH = LMOVE source dest RIGHT LEFT
    const moved = storage.lmove(allocator, src, dst, false, true) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };
    defer if (moved) |m| allocator.free(m);

    if (moved) |elem| {
        return w.writeBulkString(elem);
    } else {
        return w.writeNull();
    }
}

// ── Unit tests for new list commands ────────────────────────────────────────

test "lists - LINDEX returns element at index" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LINDEX" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "1" },
    };
    const result = try cmdLindex(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$1\r\nb\r\n", result);
}

test "lists - LINDEX negative index" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LINDEX" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "-1" },
    };
    const result = try cmdLindex(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$1\r\nc\r\n", result);
}

test "lists - LINDEX out of range returns null bulk" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LINDEX" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "10" },
    };
    const result = try cmdLindex(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "lists - LSET updates element" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LSET" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "z" },
    };
    const result = try cmdLset(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "lists - LSET out of range returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LSET" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "5" },
        RespValue{ .bulk_string = "z" },
    };
    const result = try cmdLset(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "index out of range") != null);
}

test "lists - LTRIM trims list to range" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
        RespValue{ .bulk_string = "d" },
        RespValue{ .bulk_string = "e" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LTRIM" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "3" },
    };
    const result = try cmdLtrim(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);

    // Verify contents
    const lrange_args = [_]RespValue{
        RespValue{ .bulk_string = "LRANGE" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "-1" },
    };
    const range_result = try cmdLrange(allocator, storage, &lrange_args);
    defer allocator.free(range_result);
    try std.testing.expect(std.mem.indexOf(u8, range_result, "*3\r\n") != null);
}

test "lists - LREM removes occurrences from head" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "c" },
        RespValue{ .bulk_string = "a" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LREM" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "a" },
    };
    const result = try cmdLrem(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":2\r\n", result);
}

test "lists - LREM on non-existent key returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LREM" },
        RespValue{ .bulk_string = "nosuchkey" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "a" },
    };
    const result = try cmdLrem(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "lists - LPUSHX on existing list" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LPUSHX" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "b" },
    };
    const result = try cmdLpushx(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":2\r\n", result);
}

test "lists - LPUSHX on non-existent key returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LPUSHX" },
        RespValue{ .bulk_string = "nosuchkey" },
        RespValue{ .bulk_string = "a" },
    };
    const result = try cmdLpushx(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
    try std.testing.expect(!storage.exists("nosuchkey"));
}

test "lists - RPUSHX on existing list" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "RPUSHX" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "b" },
    };
    const result = try cmdRpushx(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":2\r\n", result);
}

test "lists - RPUSHX on non-existent key returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "RPUSHX" },
        RespValue{ .bulk_string = "nosuchkey" },
        RespValue{ .bulk_string = "a" },
    };
    const result = try cmdRpushx(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "lists - LINSERT before pivot" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LINSERT" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "BEFORE" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "x" },
    };
    const result = try cmdLinsert(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":4\r\n", result);
}

test "lists - LINSERT pivot not found returns -1" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LINSERT" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "AFTER" },
        RespValue{ .bulk_string = "z" },
        RespValue{ .bulk_string = "x" },
    };
    const result = try cmdLinsert(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":-1\r\n", result);
}

test "lists - LPOS returns position" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "c" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LPOS" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
    };
    const result = try cmdLpos(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "lists - LPOS with COUNT returns array" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "a" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LPOS" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "COUNT" },
        RespValue{ .bulk_string = "0" },
    };
    const result = try cmdLpos(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
}

test "lists - LMOVE moves element between lists" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "src" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LMOVE" },
        RespValue{ .bulk_string = "src" },
        RespValue{ .bulk_string = "dst" },
        RespValue{ .bulk_string = "LEFT" },
        RespValue{ .bulk_string = "RIGHT" },
    };
    const result = try cmdLmove(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$1\r\na\r\n", result);
}

test "lists - LMOVE on empty source returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LMOVE" },
        RespValue{ .bulk_string = "nosrc" },
        RespValue{ .bulk_string = "dst" },
        RespValue{ .bulk_string = "LEFT" },
        RespValue{ .bulk_string = "LEFT" },
    };
    const result = try cmdLmove(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "lists - RPOPLPUSH moves from tail to head" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "src" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "RPOPLPUSH" },
        RespValue{ .bulk_string = "src" },
        RespValue{ .bulk_string = "dst" },
    };
    const result = try cmdRpoplpush(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$1\r\nc\r\n", result);
}

test "lists - RPOPLPUSH on missing source returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "RPOPLPUSH" },
        RespValue{ .bulk_string = "nosrc" },
        RespValue{ .bulk_string = "dst" },
    };
    const result = try cmdRpoplpush(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

// ── Blocking list commands ───────────────────────────────────────────────────

/// BLPOP key [key ...] timeout
/// Blocking version of LPOP - blocks until an element is available.
/// In this single-threaded implementation, behaves as immediate check (timeout=0).
/// Returns array [key, element] or null if all lists are empty.
pub fn cmdBlpop(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'blpop' command");
    }

    // Last argument is timeout (we parse but ignore in this implementation)
    const timeout_str = switch (args[args.len - 1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR timeout is not a float or out of range"),
    };

    _ = std.fmt.parseFloat(f64, timeout_str) catch {
        return w.writeError("ERR timeout is not a float or out of range");
    };

    // Try to pop from each key in order
    for (args[1 .. args.len - 1]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        // Try LPOP from this key
        const result = (try storage.lpop(allocator, key, 1)) orelse continue;
        defer {
            for (result) |elem| allocator.free(elem);
            allocator.free(result);
        }

        if (result.len > 0) {
            // Return [key, element]
            var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, 2);
            defer resp_values.deinit(allocator);

            try resp_values.append(allocator, RespValue{ .bulk_string = key });
            try resp_values.append(allocator, RespValue{ .bulk_string = result[0] });

            return w.writeArray(resp_values.items);
        }
    }

    // All lists empty - return null
    return w.writeNull();
}

/// BRPOP key [key ...] timeout
/// Blocking version of RPOP - blocks until an element is available.
/// In this single-threaded implementation, behaves as immediate check (timeout=0).
/// Returns array [key, element] or null if all lists are empty.
pub fn cmdBrpop(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'brpop' command");
    }

    // Last argument is timeout (we parse but ignore in this implementation)
    const timeout_str = switch (args[args.len - 1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR timeout is not a float or out of range"),
    };

    _ = std.fmt.parseFloat(f64, timeout_str) catch {
        return w.writeError("ERR timeout is not a float or out of range");
    };

    // Try to pop from each key in order
    for (args[1 .. args.len - 1]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        // Try RPOP from this key
        const result = (try storage.rpop(allocator, key, 1)) orelse continue;
        defer {
            for (result) |elem| allocator.free(elem);
            allocator.free(result);
        }

        if (result.len > 0) {
            // Return [key, element]
            var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, 2);
            defer resp_values.deinit(allocator);

            try resp_values.append(allocator, RespValue{ .bulk_string = key });
            try resp_values.append(allocator, RespValue{ .bulk_string = result[0] });

            return w.writeArray(resp_values.items);
        }
    }

    // All lists empty - return null
    return w.writeNull();
}

/// BLMOVE source dest LEFT|RIGHT LEFT|RIGHT timeout
/// Blocking version of LMOVE - blocks until an element is available.
/// In this single-threaded implementation, behaves as immediate check (timeout=0).
/// Returns the moved element or null if source is empty.
pub fn cmdBlmove(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 6) {
        return w.writeError("ERR wrong number of arguments for 'blmove' command");
    }

    const src = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid source key"),
    };

    const dst = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid destination key"),
    };

    const src_dir = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    const dst_dir = switch (args[4]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    const timeout_str = switch (args[5]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR timeout is not a float or out of range"),
    };

    _ = std.fmt.parseFloat(f64, timeout_str) catch {
        return w.writeError("ERR timeout is not a float or out of range");
    };

    const src_left = if (std.ascii.eqlIgnoreCase(src_dir, "LEFT"))
        true
    else if (std.ascii.eqlIgnoreCase(src_dir, "RIGHT"))
        false
    else
        return w.writeError("ERR syntax error");

    const dst_left = if (std.ascii.eqlIgnoreCase(dst_dir, "LEFT"))
        true
    else if (std.ascii.eqlIgnoreCase(dst_dir, "RIGHT"))
        false
    else
        return w.writeError("ERR syntax error");

    const moved = storage.lmove(allocator, src, dst, src_left, dst_left) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };
    defer if (moved) |m| allocator.free(m);

    if (moved) |elem| {
        return w.writeBulkString(elem);
    } else {
        return w.writeNull();
    }
}

/// BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
/// Blocking version of LMPOP - pops elements from the first non-empty list.
/// In this single-threaded implementation, behaves as immediate check (timeout=0).
/// Returns array [key, [elements...]] or null if all lists are empty.
pub fn cmdBlmpop(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 5) {
        return w.writeError("ERR wrong number of arguments for 'blmpop' command");
    }

    // Parse timeout
    const timeout_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR timeout is not a float or out of range"),
    };
    _ = std.fmt.parseFloat(f64, timeout_str) catch {
        return w.writeError("ERR timeout is not a float or out of range");
    };

    // Parse numkeys
    const numkeys_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR numkeys is not an integer or out of range"),
    };
    const numkeys = std.fmt.parseInt(i64, numkeys_str, 10) catch {
        return w.writeError("ERR numkeys is not an integer or out of range");
    };
    if (numkeys <= 0) {
        return w.writeError("ERR numkeys should be greater than 0");
    }

    const numkeys_usize = @as(usize, @intCast(numkeys));
    if (args.len < 3 + numkeys_usize + 1) {
        return w.writeError("ERR syntax error");
    }

    // Parse direction (LEFT or RIGHT)
    const direction_idx = 3 + numkeys_usize;
    const direction = switch (args[direction_idx]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    const pop_left = if (std.mem.eql(u8, direction, "LEFT"))
        true
    else if (std.mem.eql(u8, direction, "RIGHT"))
        false
    else
        return w.writeError("ERR syntax error");

    // Parse optional COUNT
    var count: usize = 1;
    if (args.len > direction_idx + 1) {
        if (args.len < direction_idx + 3) {
            return w.writeError("ERR syntax error");
        }
        const count_keyword = switch (args[direction_idx + 1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        if (!std.mem.eql(u8, count_keyword, "COUNT")) {
            return w.writeError("ERR syntax error");
        }
        const count_str = switch (args[direction_idx + 2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR count is not an integer or out of range"),
        };
        const count_i64 = std.fmt.parseInt(i64, count_str, 10) catch {
            return w.writeError("ERR count is not an integer or out of range");
        };
        if (count_i64 <= 0) {
            return w.writeError("ERR count should be greater than 0");
        }
        count = @as(usize, @intCast(count_i64));
    }

    // Try to pop from each key in order
    for (args[3 .. 3 + numkeys_usize]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        // Try popping from this key
        const result = if (pop_left)
            (try storage.lpop(allocator, key, count)) orelse continue
        else
            (try storage.rpop(allocator, key, count)) orelse continue;

        defer {
            for (result) |elem| allocator.free(elem);
            allocator.free(result);
        }

        if (result.len > 0) {
            // Return [key, [elements...]]
            var elements = try std.ArrayList(RespValue).initCapacity(allocator, result.len);
            defer elements.deinit(allocator);
            for (result) |elem| {
                try elements.append(allocator, RespValue{ .bulk_string = elem });
            }

            var outer = try std.ArrayList(RespValue).initCapacity(allocator, 2);
            defer outer.deinit(allocator);
            try outer.append(allocator, RespValue{ .bulk_string = key });

            // Build array response for elements
            var inner_buf = std.ArrayList(u8){};
            defer inner_buf.deinit(allocator);
            const inner_writer = inner_buf.writer(allocator);
            try inner_writer.print("*{d}\r\n", .{elements.items.len});
            for (elements.items) |elem| {
                const bs = switch (elem) {
                    .bulk_string => |s| s,
                    else => "",
                };
                try inner_writer.print("${d}\r\n{s}\r\n", .{ bs.len, bs });
            }

            const inner_str = try inner_buf.toOwnedSlice(allocator);
            defer allocator.free(inner_str);

            // Manually build response: *2\r\n$<keylen>\r\n<key>\r\n<inner>
            var resp_buf = std.ArrayList(u8){};
            defer resp_buf.deinit(allocator);
            const resp_writer = resp_buf.writer(allocator);
            try resp_writer.print("*2\r\n${d}\r\n{s}\r\n{s}", .{ key.len, key, inner_str });
            return resp_buf.toOwnedSlice(allocator);
        }
    }

    // All lists empty - return null
    return w.writeNull();
}

// ── Unit tests for blocking list commands ────────────────────────────────────

test "lists - BLPOP returns from first non-empty list" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup: list2 has data, list1 is empty
    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "list2" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "BLPOP" },
        RespValue{ .bulk_string = "list1" },
        RespValue{ .bulk_string = "list2" },
        RespValue{ .bulk_string = "0" },
    };
    const result = try cmdBlpop(allocator, storage, &args);
    defer allocator.free(result);

    // Should return [list2, a]
    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "list2") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$1\r\na") != null);
}

test "lists - BLPOP on all empty lists returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "BLPOP" },
        RespValue{ .bulk_string = "list1" },
        RespValue{ .bulk_string = "list2" },
        RespValue{ .bulk_string = "0" },
    };
    const result = try cmdBlpop(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "lists - BRPOP returns from first non-empty list" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup: list2 has data
    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "list2" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "BRPOP" },
        RespValue{ .bulk_string = "list1" },
        RespValue{ .bulk_string = "list2" },
        RespValue{ .bulk_string = "0" },
    };
    const result = try cmdBrpop(allocator, storage, &args);
    defer allocator.free(result);

    // Should return [list2, b]
    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "list2") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$1\r\nb") != null);
}

test "lists - BRPOP on all empty lists returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "BRPOP" },
        RespValue{ .bulk_string = "list1" },
        RespValue{ .bulk_string = "list2" },
        RespValue{ .bulk_string = "0" },
    };
    const result = try cmdBrpop(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "lists - BLMOVE moves element with timeout" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "src" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "BLMOVE" },
        RespValue{ .bulk_string = "src" },
        RespValue{ .bulk_string = "dst" },
        RespValue{ .bulk_string = "LEFT" },
        RespValue{ .bulk_string = "RIGHT" },
        RespValue{ .bulk_string = "1.0" },
    };
    const result = try cmdBlmove(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$1\r\na\r\n", result);
}

test "lists - BLMOVE on empty source returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "BLMOVE" },
        RespValue{ .bulk_string = "nosrc" },
        RespValue{ .bulk_string = "dst" },
        RespValue{ .bulk_string = "LEFT" },
        RespValue{ .bulk_string = "LEFT" },
        RespValue{ .bulk_string = "0.5" },
    };
    const result = try cmdBlmove(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "lists - BLMPOP pops from first non-empty list with LEFT" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup: list2 has data
    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "list2" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "BLMPOP" },
        RespValue{ .bulk_string = "0" }, // timeout
        RespValue{ .bulk_string = "2" }, // numkeys
        RespValue{ .bulk_string = "list1" },
        RespValue{ .bulk_string = "list2" },
        RespValue{ .bulk_string = "LEFT" },
        RespValue{ .bulk_string = "COUNT" },
        RespValue{ .bulk_string = "2" },
    };
    const result = try cmdBlmpop(allocator, storage, &args);
    defer allocator.free(result);

    // Should return [list2, [a, b]]
    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "list2") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$1\r\na") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$1\r\nb") != null);
}

test "lists - BLMPOP pops from first non-empty list with RIGHT" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup: list2 has data
    const setup = [_]RespValue{
        RespValue{ .bulk_string = "RPUSH" },
        RespValue{ .bulk_string = "list2" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    const sr = try cmdRpush(allocator, storage, &setup);
    defer allocator.free(sr);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "BLMPOP" },
        RespValue{ .bulk_string = "0" }, // timeout
        RespValue{ .bulk_string = "2" }, // numkeys
        RespValue{ .bulk_string = "list1" },
        RespValue{ .bulk_string = "list2" },
        RespValue{ .bulk_string = "RIGHT" },
        RespValue{ .bulk_string = "COUNT" },
        RespValue{ .bulk_string = "2" },
    };
    const result = try cmdBlmpop(allocator, storage, &args);
    defer allocator.free(result);

    // Should return [list2, [c, b]] (from the right)
    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "list2") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$1\r\nc") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "$1\r\nb") != null);
}

test "lists - BLMPOP on all empty lists returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "BLMPOP" },
        RespValue{ .bulk_string = "0" }, // timeout
        RespValue{ .bulk_string = "2" }, // numkeys
        RespValue{ .bulk_string = "list1" },
        RespValue{ .bulk_string = "list2" },
        RespValue{ .bulk_string = "LEFT" },
    };
    const result = try cmdBlmpop(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}
