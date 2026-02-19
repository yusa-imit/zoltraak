const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;

/// HSET key field value [field value ...]
/// Sets field-value pairs in a hash
/// Returns integer - count of fields added (not updated)
pub fn cmdHset(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4 or (args.len - 2) % 2 != 0) {
        return w.writeError("ERR wrong number of arguments for 'hset' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Extract field-value pairs
    const pair_count = (args.len - 2) / 2;
    var fields = try std.ArrayList([]const u8).initCapacity(allocator, pair_count);
    defer fields.deinit(allocator);
    var values = try std.ArrayList([]const u8).initCapacity(allocator, pair_count);
    defer values.deinit(allocator);

    var i: usize = 2;
    while (i < args.len) : (i += 2) {
        const field = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid field"),
        };
        const value = switch (args[i + 1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid value"),
        };

        try fields.append(allocator, field);
        try values.append(allocator, value);
    }

    // Execute HSET
    const added_count = storage.hset(key, fields.items, values.items, null) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(added_count));
}

/// HGET key field
/// Get value of a field in a hash
/// Returns bulk string or null
pub fn cmdHget(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'hget' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const field = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid field"),
    };

    // Execute HGET
    const value = storage.hget(key, field);

    if (value) |v| {
        return w.writeBulkString(v);
    } else {
        return w.writeNull();
    }
}

/// HDEL key field [field ...]
/// Delete fields from a hash
/// Returns integer - count of fields deleted
pub fn cmdHdel(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'hdel' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Extract all fields
    var fields = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 2);
    defer fields.deinit(allocator);

    for (args[2..]) |arg| {
        const field = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid field"),
        };
        try fields.append(allocator, field);
    }

    // Execute HDEL
    const deleted_count = storage.hdel(key, fields.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(deleted_count));
}

/// HGETALL key
/// Get all fields and values in a hash
/// Returns array where even indices are fields, odd indices are values
pub fn cmdHgetall(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'hgetall' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Execute HGETALL
    const result = try storage.hgetall(allocator, key);

    if (result) |pairs| {
        defer allocator.free(pairs);

        // Convert to RespValue array
        var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, pairs.len);
        defer resp_values.deinit(allocator);

        for (pairs) |item| {
            try resp_values.append(allocator, RespValue{ .bulk_string = item });
        }

        return w.writeArray(resp_values.items);
    } else {
        // Empty array for non-existent key
        return w.writeArray(&[_]RespValue{});
    }
}

/// HKEYS key
/// Get all field names in a hash
/// Returns array of field names
pub fn cmdHkeys(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'hkeys' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Execute HKEYS
    const result = try storage.hkeys(allocator, key);

    if (result) |fields| {
        defer allocator.free(fields);

        // Convert to RespValue array
        var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, fields.len);
        defer resp_values.deinit(allocator);

        for (fields) |field| {
            try resp_values.append(allocator, RespValue{ .bulk_string = field });
        }

        return w.writeArray(resp_values.items);
    } else {
        // Empty array for non-existent key
        return w.writeArray(&[_]RespValue{});
    }
}

/// HVALS key
/// Get all values in a hash
/// Returns array of values
pub fn cmdHvals(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'hvals' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Execute HVALS
    const result = try storage.hvals(allocator, key);

    if (result) |values| {
        defer allocator.free(values);

        // Convert to RespValue array
        var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, values.len);
        defer resp_values.deinit(allocator);

        for (values) |value| {
            try resp_values.append(allocator, RespValue{ .bulk_string = value });
        }

        return w.writeArray(resp_values.items);
    } else {
        // Empty array for non-existent key
        return w.writeArray(&[_]RespValue{});
    }
}

/// HEXISTS key field
/// Check if field exists in hash
/// Returns integer: 1 if field exists, 0 otherwise
pub fn cmdHexists(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'hexists' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const field = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid field"),
    };

    // Execute HEXISTS
    const exists = storage.hexists(key, field) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(if (exists) 1 else 0);
}

/// HLEN key
/// Get number of fields in a hash
/// Returns integer - field count (0 for non-existent key)
pub fn cmdHlen(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'hlen' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Execute HLEN
    const len = storage.hlen(key);

    return w.writeInteger(@intCast(len orelse 0));
}

/// HMGET key field [field ...]
/// Get values of multiple fields in a hash
/// Returns array where each element is a bulk string or null for missing fields
pub fn cmdHmget(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'hmget' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const fields = args[2..];
    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, fields.len);
    defer resp_values.deinit(allocator);

    for (fields) |field_arg| {
        const field = switch (field_arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid field"),
        };
        const value = storage.hget(key, field);
        if (value) |v| {
            try resp_values.append(allocator, RespValue{ .bulk_string = v });
        } else {
            try resp_values.append(allocator, RespValue{ .null_bulk_string = {} });
        }
    }

    return w.writeArray(resp_values.items);
}

/// HINCRBY key field increment
/// Increment integer field in a hash by increment
/// Returns new value as integer
pub fn cmdHincrby(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'hincrby' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const field = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid field"),
    };

    const incr_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not an integer or out of range"),
    };

    const increment = std.fmt.parseInt(i64, incr_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    const new_value = storage.hincrby(allocator, key, field, increment) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.InvalidValue => return w.writeError("ERR hash value is not an integer"),
        else => return err,
    };

    return w.writeInteger(new_value);
}

/// HINCRBYFLOAT key field increment
/// Increment float field in a hash by increment
/// Returns new value as bulk string
pub fn cmdHincrbyfloat(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'hincrbyfloat' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const field = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid field"),
    };

    const incr_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not a valid float"),
    };

    const increment = std.fmt.parseFloat(f64, incr_str) catch {
        return w.writeError("ERR value is not a valid float");
    };

    const new_value = storage.hincrbyfloat(allocator, key, field, increment) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.InvalidValue => return w.writeError("ERR hash value is not a valid float"),
        else => return err,
    };

    // Format result as bulk string with no trailing zeros
    var buf: [64]u8 = undefined;
    const raw = std.fmt.bufPrint(&buf, "{d}", .{new_value}) catch "0";
    // Trim trailing zeros after decimal point
    const result_str = blk: {
        if (std.mem.indexOf(u8, raw, ".") != null) {
            var end = raw.len;
            while (end > 0 and raw[end - 1] == '0') end -= 1;
            if (end > 0 and raw[end - 1] == '.') end -= 1;
            break :blk raw[0..end];
        }
        break :blk raw;
    };
    return w.writeBulkString(result_str);
}

/// HSETNX key field value
/// Set field in hash only if field does not exist
/// Returns 1 if field was set, 0 if field already existed
pub fn cmdHsetnx(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'hsetnx' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const field = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid field"),
    };

    const value = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid value"),
    };

    const was_set = storage.hsetnx(allocator, key, field, value) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => return err,
    };

    return w.writeInteger(if (was_set) 1 else 0);
}

// Embedded unit tests

test "cmdHset - basic set" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };

    const response = try cmdHset(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":1\r\n", response);
}

test "cmdHset - multiple fields" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "value2" },
    };

    const response = try cmdHset(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":2\r\n", response);
}

test "cmdHset - update existing field" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Initial set
    const args1 = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try cmdHset(allocator, storage, &args1);

    // Update field
    const args2 = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "newvalue" },
    };
    const response = try cmdHset(allocator, storage, &args2);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":0\r\n", response);
}

test "cmdHget - get existing field" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set field
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    // Get field
    const get_args = [_]RespValue{
        .{ .bulk_string = "HGET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
    };
    const response = try cmdHget(allocator, storage, &get_args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("$6\r\nvalue1\r\n", response);
}

test "cmdHget - non-existent field" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "HGET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "nosuchfield" },
    };
    const response = try cmdHget(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("$-1\r\n", response);
}

test "cmdHdel - delete field" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set fields
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "value2" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    // Delete field
    const del_args = [_]RespValue{
        .{ .bulk_string = "HDEL" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
    };
    const response = try cmdHdel(allocator, storage, &del_args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":1\r\n", response);
}

test "cmdHgetall - get all fields and values" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set fields
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "value2" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    // Get all
    const args = [_]RespValue{
        .{ .bulk_string = "HGETALL" },
        .{ .bulk_string = "myhash" },
    };
    const response = try cmdHgetall(allocator, storage, &args);
    defer allocator.free(response);

    // Should return array with 4 elements (2 fields + 2 values)
    try std.testing.expect(std.mem.startsWith(u8, response, "*4\r\n"));
}

test "cmdHkeys - get all field names" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set fields
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "value2" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    // Get keys
    const args = [_]RespValue{
        .{ .bulk_string = "HKEYS" },
        .{ .bulk_string = "myhash" },
    };
    const response = try cmdHkeys(allocator, storage, &args);
    defer allocator.free(response);

    // Should return array with 2 elements
    try std.testing.expect(std.mem.startsWith(u8, response, "*2\r\n"));
}

test "cmdHvals - get all values" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set fields
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "value2" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    // Get values
    const args = [_]RespValue{
        .{ .bulk_string = "HVALS" },
        .{ .bulk_string = "myhash" },
    };
    const response = try cmdHvals(allocator, storage, &args);
    defer allocator.free(response);

    // Should return array with 2 elements
    try std.testing.expect(std.mem.startsWith(u8, response, "*2\r\n"));
}

test "cmdHexists - field exists" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set field
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    // Check existence
    const args = [_]RespValue{
        .{ .bulk_string = "HEXISTS" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
    };
    const response = try cmdHexists(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":1\r\n", response);
}

test "cmdHexists - field does not exist" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "HEXISTS" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "nosuchfield" },
    };
    const response = try cmdHexists(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":0\r\n", response);
}

test "cmdHlen - get hash length" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set fields
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "value2" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    // Get length
    const args = [_]RespValue{
        .{ .bulk_string = "HLEN" },
        .{ .bulk_string = "myhash" },
    };
    const response = try cmdHlen(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":2\r\n", response);
}

test "cmdHlen - non-existent key returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "HLEN" },
        .{ .bulk_string = "nosuchkey" },
    };
    const response = try cmdHlen(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":0\r\n", response);
}

test "cmdHmget - get multiple fields" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "f1" },
        .{ .bulk_string = "v1" },
        .{ .bulk_string = "f2" },
        .{ .bulk_string = "v2" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    const args = [_]RespValue{
        .{ .bulk_string = "HMGET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "f1" },
        .{ .bulk_string = "nosuch" },
        .{ .bulk_string = "f2" },
    };
    const response = try cmdHmget(allocator, storage, &args);
    defer allocator.free(response);

    // Should be *3 array with v1, nil, v2
    try std.testing.expect(std.mem.startsWith(u8, response, "*3\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, response, "$2\r\nv1\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "$-1\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "$2\r\nv2\r\n") != null);
}

test "cmdHincrby - increment new field" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "HINCRBY" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "counter" },
        .{ .bulk_string = "5" },
    };
    const response = try cmdHincrby(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":5\r\n", response);
}

test "cmdHincrby - increment existing field" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "counter" },
        .{ .bulk_string = "10" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    const args = [_]RespValue{
        .{ .bulk_string = "HINCRBY" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "counter" },
        .{ .bulk_string = "3" },
    };
    const response = try cmdHincrby(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":13\r\n", response);
}

test "cmdHincrby - negative increment" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "HINCRBY" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "counter" },
        .{ .bulk_string = "-3" },
    };
    const response = try cmdHincrby(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":-3\r\n", response);
}

test "cmdHincrbyfloat - increment float field" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "HINCRBYFLOAT" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "score" },
        .{ .bulk_string = "1.5" },
    };
    const response = try cmdHincrbyfloat(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "$"));
    try std.testing.expect(std.mem.indexOf(u8, response, "1.5") != null);
}

test "cmdHsetnx - set new field" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "HSETNX" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field" },
        .{ .bulk_string = "value" },
    };
    const response = try cmdHsetnx(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":1\r\n", response);
}

test "cmdHsetnx - field already exists returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field" },
        .{ .bulk_string = "original" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    const args = [_]RespValue{
        .{ .bulk_string = "HSETNX" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field" },
        .{ .bulk_string = "newvalue" },
    };
    const response = try cmdHsetnx(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":0\r\n", response);
}
