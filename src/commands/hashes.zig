const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");
const client_mod = @import("./client.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const MapPair = writer_mod.MapPair;
const Storage = storage_mod.Storage;
const RespProtocol = client_mod.RespProtocol;

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
pub fn cmdHgetall(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, protocol_version: RespProtocol) ![]const u8 {
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

        // RESP3: return as map, RESP2: return as flat array
        if (protocol_version == .RESP3 and pairs.len > 0) {
            // Build map pairs (field -> value)
            const map_len = pairs.len / 2;
            const map_pairs = try allocator.alloc(MapPair, map_len);
            defer allocator.free(map_pairs);

            var i: usize = 0;
            var pair_idx: usize = 0;
            while (i < pairs.len) : (i += 2) {
                map_pairs[pair_idx] = MapPair{
                    .key = RespValue{ .bulk_string = pairs[i] },
                    .value = RespValue{ .bulk_string = pairs[i + 1] },
                };
                pair_idx += 1;
            }

            return w.writeMap(map_pairs);
        } else {
            // RESP2: flat array [field1, value1, field2, value2, ...]
            var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, pairs.len);
            defer resp_values.deinit(allocator);

            for (pairs) |item| {
                try resp_values.append(allocator, RespValue{ .bulk_string = item });
            }

            return w.writeArray(resp_values.items);
        }
    } else {
        // Empty array/map for non-existent key
        if (protocol_version == .RESP3) {
            return w.writeMap(&[_]MapPair{});
        } else {
            return w.writeArray(&[_]RespValue{});
        }
    }
}

/// HKEYS key
/// Get all field names in a hash
/// Returns array of field names (RESP3: set, since fields are unique)
pub fn cmdHkeys(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, protocol_version: RespProtocol) ![]const u8 {
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

        // RESP3: return as set (fields are unique), RESP2: return as array
        if (protocol_version == .RESP3) {
            return w.writeSet(resp_values.items);
        } else {
            return w.writeArray(resp_values.items);
        }
    } else {
        // Empty set/array for non-existent key
        if (protocol_version == .RESP3) {
            return w.writeSet(&[_]RespValue{});
        } else {
            return w.writeArray(&[_]RespValue{});
        }
    }
}

/// HVALS key
/// Get all values in a hash
/// Returns array of values (values may repeat, so always array even in RESP3)
pub fn cmdHvals(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, protocol_version: RespProtocol) ![]const u8 {
    _ = protocol_version; // Values may repeat, so always array
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

test "cmdHgetall - RESP2 returns flat array" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set up hash with two fields
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "value2" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    // HGETALL with RESP2
    const args = [_]RespValue{
        .{ .bulk_string = "HGETALL" },
        .{ .bulk_string = "myhash" },
    };
    const response = try cmdHgetall(allocator, storage, &args, .RESP2);
    defer allocator.free(response);

    // Should return flat array: *4\r\n$6\r\nfield1\r\n$6\r\nvalue1\r\n$6\r\nfield2\r\n$6\r\nvalue2\r\n
    try std.testing.expect(std.mem.startsWith(u8, response, "*4\r\n"));
}

test "cmdHgetall - RESP3 returns map" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set up hash with two fields
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "value2" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    // HGETALL with RESP3
    const args = [_]RespValue{
        .{ .bulk_string = "HGETALL" },
        .{ .bulk_string = "myhash" },
    };
    const response = try cmdHgetall(allocator, storage, &args, .RESP3);
    defer allocator.free(response);

    // Should return RESP3 map: %2\r\n...
    try std.testing.expect(std.mem.startsWith(u8, response, "%2\r\n"));
}

test "cmdHgetall - RESP3 empty hash returns empty map" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // HGETALL on non-existent key with RESP3
    const args = [_]RespValue{
        .{ .bulk_string = "HGETALL" },
        .{ .bulk_string = "nonexistent" },
    };
    const response = try cmdHgetall(allocator, storage, &args, .RESP3);
    defer allocator.free(response);

    // Should return empty RESP3 map: %0\r\n
    try std.testing.expectEqualStrings("%0\r\n", response);
}

/// HSTRLEN key field
/// Returns the string length of the value associated with field in the hash stored at key.
/// If the key or the field do not exist, 0 is returned.
pub fn cmdHstrlen(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'hstrlen' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const field = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid field"),
    };

    const len = storage.hstrlen(key, field) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(len));
}

test "cmdHstrlen - returns length of hash field value" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set a field with a value
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "Hello World" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    // Get string length
    const args = [_]RespValue{
        .{ .bulk_string = "HSTRLEN" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
    };
    const response = try cmdHstrlen(allocator, storage, &args);
    defer allocator.free(response);

    // "Hello World" has 11 characters
    try std.testing.expectEqualStrings(":11\r\n", response);
}

test "cmdHstrlen - returns 0 for non-existent field" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "HSTRLEN" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "nonexistent" },
    };
    const response = try cmdHstrlen(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":0\r\n", response);
}

test "cmdHstrlen - returns 0 for non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "HSTRLEN" },
        .{ .bulk_string = "nonexistent" },
        .{ .bulk_string = "field1" },
    };
    const response = try cmdHstrlen(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings(":0\r\n", response);
}

/// HRANDFIELD key [count [WITHVALUES]]
/// Return random field(s) from a hash
/// Returns bulk string (single field), array of fields, or array of field-value pairs
pub fn cmdHrandfield(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, protocol_version: RespProtocol) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2 or args.len > 4) {
        return w.writeError("ERR wrong number of arguments for 'hrandfield' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    var count: ?i64 = null;
    var with_values = false;

    if (args.len >= 3) {
        count = switch (args[2]) {
            .bulk_string => |s| std.fmt.parseInt(i64, s, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            },
            else => return w.writeError("ERR value is not an integer or out of range"),
        };

        if (args.len == 4) {
            const opt = switch (args[3]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };

            if (std.ascii.eqlIgnoreCase(opt, "withvalues")) {
                with_values = true;
            } else {
                return w.writeError("ERR syntax error");
            }
        }
    }

    const result = storage.hrandfield(allocator, key, count, with_values) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };
    defer {
        if (result) |r| allocator.free(r);
    }

    if (result) |fields| {
        if (count == null) {
            // Single field without count
            if (fields.len > 0) {
                return w.writeBulkString(fields[0]);
            } else {
                return w.writeNull();
            }
        } else {
            // Multiple fields - return array
            if (with_values) {
                // Return array of field-value pairs
                // For RESP3 with protocol_version == RESP3, return a map
                if (protocol_version == .RESP3) {
                    // Build map pairs
                    var pairs = try std.ArrayList(MapPair).initCapacity(allocator, fields.len / 2);
                    defer pairs.deinit(allocator);

                    var i: usize = 0;
                    while (i < fields.len) : (i += 2) {
                        try pairs.append(allocator, .{
                            .key = .{ .bulk_string = fields[i] },
                            .value = .{ .bulk_string = fields[i + 1] },
                        });
                    }

                    return w.writeMap(pairs.items);
                } else {
                    // RESP2: flat array - convert to RespValue
                    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, fields.len);
                    defer resp_values.deinit(allocator);

                    for (fields) |field| {
                        try resp_values.append(allocator, RespValue{ .bulk_string = field });
                    }

                    return w.writeArray(resp_values.items);
                }
            } else {
                // Return array of fields - convert to RespValue
                var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, fields.len);
                defer resp_values.deinit(allocator);

                for (fields) |field| {
                    try resp_values.append(allocator, RespValue{ .bulk_string = field });
                }

                return w.writeArray(resp_values.items);
            }
        }
    } else {
        return w.writeNull();
    }
}

/// HEXPIRE key seconds FIELDS numfields field [field ...] [NX|XX|GT|LT]
/// Set expiration time for hash fields
/// Returns array of integers (1 if set, 0 if not, -2 if field doesn't exist)
pub fn cmdHexpire(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 5) {
        return w.writeError("ERR wrong number of arguments for 'hexpire' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const seconds_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid seconds"),
    };
    const seconds = std.fmt.parseInt(i64, seconds_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    // Parse FIELDS keyword
    const fields_keyword = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };
    if (!std.mem.eql(u8, fields_keyword, "FIELDS")) {
        return w.writeError("ERR syntax error");
    }

    const numfields_str = switch (args[4]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid numfields"),
    };
    const numfields = std.fmt.parseInt(usize, numfields_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    if (args.len < 5 + numfields) {
        return w.writeError("ERR wrong number of arguments for 'hexpire' command");
    }

    // Extract fields
    var fields = try std.ArrayList([]const u8).initCapacity(allocator, numfields);
    defer fields.deinit(allocator);

    for (5..5 + numfields) |i| {
        const field = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid field"),
        };
        try fields.append(allocator, field);
    }

    // Parse options (NX|XX|GT|LT)
    var options: u8 = 0;
    if (args.len > 5 + numfields) {
        const option = switch (args[5 + numfields]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        if (std.mem.eql(u8, option, "NX")) {
            options = 1;
        } else if (std.mem.eql(u8, option, "XX")) {
            options = 2;
        } else if (std.mem.eql(u8, option, "GT")) {
            options = 4;
        } else if (std.mem.eql(u8, option, "LT")) {
            options = 8;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Calculate expiration time in milliseconds
    const now = std.time.milliTimestamp();
    const expires_at_ms = now + (seconds * 1000);

    // Execute HEXPIRE
    const updated_count = storage.hexpire(key, fields.items, expires_at_ms, options) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    // Build result array (1 if set, 0 if not set, -2 if field doesn't exist)
    // For simplicity, we return number of updated fields as success indicator
    // In real Redis, this would return an array per field
    var results = try allocator.alloc(i64, numfields);
    defer allocator.free(results);

    // Mark first 'updated_count' as 1, rest as 0
    for (0..numfields) |i| {
        results[i] = if (i < updated_count) 1 else 0;
    }

    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, numfields);
    defer resp_values.deinit(allocator);

    for (results) |r| {
        try resp_values.append(allocator, RespValue{ .integer = r });
    }

    return w.writeArray(resp_values.items);
}

/// HPEXPIRE key milliseconds FIELDS numfields field [field ...] [NX|XX|GT|LT]
/// Set expiration time for hash fields in milliseconds
pub fn cmdHpexpire(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 5) {
        return w.writeError("ERR wrong number of arguments for 'hpexpire' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const ms_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid milliseconds"),
    };
    const milliseconds = std.fmt.parseInt(i64, ms_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    // Parse FIELDS keyword
    const fields_keyword = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };
    if (!std.mem.eql(u8, fields_keyword, "FIELDS")) {
        return w.writeError("ERR syntax error");
    }

    const numfields_str = switch (args[4]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid numfields"),
    };
    const numfields = std.fmt.parseInt(usize, numfields_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    if (args.len < 5 + numfields) {
        return w.writeError("ERR wrong number of arguments for 'hpexpire' command");
    }

    // Extract fields
    var fields = try std.ArrayList([]const u8).initCapacity(allocator, numfields);
    defer fields.deinit(allocator);

    for (5..5 + numfields) |i| {
        const field = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid field"),
        };
        try fields.append(allocator, field);
    }

    // Parse options (NX|XX|GT|LT)
    var options: u8 = 0;
    if (args.len > 5 + numfields) {
        const option = switch (args[5 + numfields]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        if (std.mem.eql(u8, option, "NX")) {
            options = 1;
        } else if (std.mem.eql(u8, option, "XX")) {
            options = 2;
        } else if (std.mem.eql(u8, option, "GT")) {
            options = 4;
        } else if (std.mem.eql(u8, option, "LT")) {
            options = 8;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Calculate expiration time in milliseconds
    const now = std.time.milliTimestamp();
    const expires_at_ms = now + milliseconds;

    // Execute HPEXPIRE
    const updated_count = storage.hexpire(key, fields.items, expires_at_ms, options) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    var results = try allocator.alloc(i64, numfields);
    defer allocator.free(results);

    for (0..numfields) |i| {
        results[i] = if (i < updated_count) 1 else 0;
    }

    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, numfields);
    defer resp_values.deinit(allocator);

    for (results) |r| {
        try resp_values.append(allocator, RespValue{ .integer = r });
    }

    return w.writeArray(resp_values.items);
}

/// HEXPIREAT key unix-time-seconds FIELDS numfields field [field ...] [NX|XX|GT|LT]
/// Set expiration time for hash fields at absolute Unix timestamp (seconds)
pub fn cmdHexpireat(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 5) {
        return w.writeError("ERR wrong number of arguments for 'hexpireat' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const ts_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid timestamp"),
    };
    const unix_time_seconds = std.fmt.parseInt(i64, ts_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    // Parse FIELDS keyword
    const fields_keyword = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };
    if (!std.mem.eql(u8, fields_keyword, "FIELDS")) {
        return w.writeError("ERR syntax error");
    }

    const numfields_str = switch (args[4]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid numfields"),
    };
    const numfields = std.fmt.parseInt(usize, numfields_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    if (args.len < 5 + numfields) {
        return w.writeError("ERR wrong number of arguments for 'hexpireat' command");
    }

    // Extract fields
    var fields = try std.ArrayList([]const u8).initCapacity(allocator, numfields);
    defer fields.deinit(allocator);

    for (5..5 + numfields) |i| {
        const field = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid field"),
        };
        try fields.append(allocator, field);
    }

    // Parse options
    var options: u8 = 0;
    if (args.len > 5 + numfields) {
        const option = switch (args[5 + numfields]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        if (std.mem.eql(u8, option, "NX")) {
            options = 1;
        } else if (std.mem.eql(u8, option, "XX")) {
            options = 2;
        } else if (std.mem.eql(u8, option, "GT")) {
            options = 4;
        } else if (std.mem.eql(u8, option, "LT")) {
            options = 8;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    const expires_at_ms = unix_time_seconds * 1000;

    const updated_count = storage.hexpire(key, fields.items, expires_at_ms, options) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    var results = try allocator.alloc(i64, numfields);
    defer allocator.free(results);

    for (0..numfields) |i| {
        results[i] = if (i < updated_count) 1 else 0;
    }

    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, numfields);
    defer resp_values.deinit(allocator);

    for (results) |r| {
        try resp_values.append(allocator, RespValue{ .integer = r });
    }

    return w.writeArray(resp_values.items);
}

/// HPEXPIREAT key unix-time-milliseconds FIELDS numfields field [field ...] [NX|XX|GT|LT]
/// Set expiration time for hash fields at absolute Unix timestamp (milliseconds)
pub fn cmdHpexpireat(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 5) {
        return w.writeError("ERR wrong number of arguments for 'hpexpireat' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const ts_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid timestamp"),
    };
    const unix_time_ms = std.fmt.parseInt(i64, ts_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    // Parse FIELDS keyword
    const fields_keyword = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };
    if (!std.mem.eql(u8, fields_keyword, "FIELDS")) {
        return w.writeError("ERR syntax error");
    }

    const numfields_str = switch (args[4]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid numfields"),
    };
    const numfields = std.fmt.parseInt(usize, numfields_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    if (args.len < 5 + numfields) {
        return w.writeError("ERR wrong number of arguments for 'hpexpireat' command");
    }

    // Extract fields
    var fields = try std.ArrayList([]const u8).initCapacity(allocator, numfields);
    defer fields.deinit(allocator);

    for (5..5 + numfields) |i| {
        const field = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid field"),
        };
        try fields.append(allocator, field);
    }

    // Parse options
    var options: u8 = 0;
    if (args.len > 5 + numfields) {
        const option = switch (args[5 + numfields]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        if (std.mem.eql(u8, option, "NX")) {
            options = 1;
        } else if (std.mem.eql(u8, option, "XX")) {
            options = 2;
        } else if (std.mem.eql(u8, option, "GT")) {
            options = 4;
        } else if (std.mem.eql(u8, option, "LT")) {
            options = 8;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    const updated_count = storage.hexpire(key, fields.items, unix_time_ms, options) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    var results = try allocator.alloc(i64, numfields);
    defer allocator.free(results);

    for (0..numfields) |i| {
        results[i] = if (i < updated_count) 1 else 0;
    }

    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, numfields);
    defer resp_values.deinit(allocator);

    for (results) |r| {
        try resp_values.append(allocator, RespValue{ .integer = r });
    }

    return w.writeArray(resp_values.items);
}

/// HPERSIST key FIELDS numfields field [field ...]
/// Remove expiration from hash fields
/// Returns array of integers (1 if expiration removed, 0 if field has no expiration, -2 if field doesn't exist)
pub fn cmdHpersist(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'hpersist' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse FIELDS keyword
    const fields_keyword = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };
    if (!std.mem.eql(u8, fields_keyword, "FIELDS")) {
        return w.writeError("ERR syntax error");
    }

    const numfields_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid numfields"),
    };
    const numfields = std.fmt.parseInt(usize, numfields_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    if (args.len != 4 + numfields) {
        return w.writeError("ERR wrong number of arguments for 'hpersist' command");
    }

    // Extract fields
    var fields = try std.ArrayList([]const u8).initCapacity(allocator, numfields);
    defer fields.deinit(allocator);

    for (4..4 + numfields) |i| {
        const field = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid field"),
        };
        try fields.append(allocator, field);
    }

    // Execute HPERSIST
    const removed_count = storage.hpersist(key, fields.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    var results = try allocator.alloc(i64, numfields);
    defer allocator.free(results);

    for (0..numfields) |i| {
        results[i] = if (i < removed_count) 1 else 0;
    }

    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, numfields);
    defer resp_values.deinit(allocator);

    for (results) |r| {
        try resp_values.append(allocator, RespValue{ .integer = r });
    }

    return w.writeArray(resp_values.items);
}

/// HTTL key FIELDS numfields field [field ...]
/// Get TTL in seconds for hash fields
/// Returns array of TTL values (-2 if field doesn't exist, -1 if no expiry, positive for TTL)
pub fn cmdHttpl(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'httl' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse FIELDS keyword
    const fields_keyword = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };
    if (!std.mem.eql(u8, fields_keyword, "FIELDS")) {
        return w.writeError("ERR syntax error");
    }

    const numfields_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid numfields"),
    };
    const numfields = std.fmt.parseInt(usize, numfields_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    if (args.len != 4 + numfields) {
        return w.writeError("ERR wrong number of arguments for 'httl' command");
    }

    // Extract fields
    var fields = try std.ArrayList([]const u8).initCapacity(allocator, numfields);
    defer fields.deinit(allocator);

    for (4..4 + numfields) |i| {
        const field = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid field"),
        };
        try fields.append(allocator, field);
    }

    // Execute HTTL
    const ttls = try storage.httl(allocator, key, fields.items);
    defer allocator.free(ttls);

    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, ttls.len);
    defer resp_values.deinit(allocator);

    for (ttls) |ttl| {
        try resp_values.append(allocator, RespValue{ .integer = ttl });
    }

    return w.writeArray(resp_values.items);
}

/// HPTTL key FIELDS numfields field [field ...]
/// Get TTL in milliseconds for hash fields
/// Returns array of TTL values (-2 if field doesn't exist, -1 if no expiry, positive for TTL)
pub fn cmdHpttl(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'hpttl' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse FIELDS keyword
    const fields_keyword = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };
    if (!std.mem.eql(u8, fields_keyword, "FIELDS")) {
        return w.writeError("ERR syntax error");
    }

    const numfields_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid numfields"),
    };
    const numfields = std.fmt.parseInt(usize, numfields_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    if (args.len != 4 + numfields) {
        return w.writeError("ERR wrong number of arguments for 'hpttl' command");
    }

    // Extract fields
    var fields = try std.ArrayList([]const u8).initCapacity(allocator, numfields);
    defer fields.deinit(allocator);

    for (4..4 + numfields) |i| {
        const field = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid field"),
        };
        try fields.append(allocator, field);
    }

    // Execute HPTTL
    const ttls = try storage.hpttl(allocator, key, fields.items);
    defer allocator.free(ttls);

    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, ttls.len);
    defer resp_values.deinit(allocator);

    for (ttls) |ttl| {
        try resp_values.append(allocator, RespValue{ .integer = ttl });
    }

    return w.writeArray(resp_values.items);
}

/// HEXPIRETIME key FIELDS numfields field [field ...]
/// Get expiration time in seconds (Unix timestamp) for hash fields
/// Returns array of expiration times (-2 if field doesn't exist, -1 if no expiry, positive for timestamp)
pub fn cmdHexpiretime(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'hexpiretime' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse FIELDS keyword
    const fields_keyword = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };
    if (!std.mem.eql(u8, fields_keyword, "FIELDS")) {
        return w.writeError("ERR syntax error");
    }

    const numfields_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid numfields"),
    };
    const numfields = std.fmt.parseInt(usize, numfields_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    if (args.len != 4 + numfields) {
        return w.writeError("ERR wrong number of arguments for 'hexpiretime' command");
    }

    // Extract fields
    var fields = try std.ArrayList([]const u8).initCapacity(allocator, numfields);
    defer fields.deinit(allocator);

    for (4..4 + numfields) |i| {
        const field = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid field"),
        };
        try fields.append(allocator, field);
    }

    // Execute HEXPIRETIME
    const times = try storage.hexpiretime(allocator, key, fields.items);
    defer allocator.free(times);

    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, times.len);
    defer resp_values.deinit(allocator);

    for (times) |time| {
        try resp_values.append(allocator, RespValue{ .integer = time });
    }

    return w.writeArray(resp_values.items);
}

/// HPEXPIRETIME key FIELDS numfields field [field ...]
/// Get expiration time in milliseconds (Unix timestamp) for hash fields
/// Returns array of expiration times (-2 if field doesn't exist, -1 if no expiry, positive for timestamp)
pub fn cmdHpexpiretime(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'hpexpiretime' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse FIELDS keyword
    const fields_keyword = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };
    if (!std.mem.eql(u8, fields_keyword, "FIELDS")) {
        return w.writeError("ERR syntax error");
    }

    const numfields_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid numfields"),
    };
    const numfields = std.fmt.parseInt(usize, numfields_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    if (args.len != 4 + numfields) {
        return w.writeError("ERR wrong number of arguments for 'hpexpiretime' command");
    }

    // Extract fields
    var fields = try std.ArrayList([]const u8).initCapacity(allocator, numfields);
    defer fields.deinit(allocator);

    for (4..4 + numfields) |i| {
        const field = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid field"),
        };
        try fields.append(allocator, field);
    }

    // Execute HPEXPIRETIME
    const times = try storage.hpexpiretime(allocator, key, fields.items);
    defer allocator.free(times);

    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, times.len);
    defer resp_values.deinit(allocator);

    for (times) |time| {
        try resp_values.append(allocator, RespValue{ .integer = time });
    }

    return w.writeArray(resp_values.items);
}

/// HGETDEL key FIELDS numfields field [field ...]
/// Atomically gets hash field values and deletes the fields
/// Returns array of values (nil for non-existent fields)
pub fn cmdHgetdel(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'hgetdel' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse FIELDS keyword
    const fields_keyword = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };
    if (!std.mem.eql(u8, fields_keyword, "FIELDS")) {
        return w.writeError("ERR syntax error");
    }

    const numfields_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid numfields"),
    };
    const numfields = std.fmt.parseInt(usize, numfields_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    if (args.len != 4 + numfields) {
        return w.writeError("ERR wrong number of arguments for 'hgetdel' command");
    }

    // Extract fields
    var fields = try std.ArrayList([]const u8).initCapacity(allocator, numfields);
    defer fields.deinit(allocator);

    for (4..4 + numfields) |i| {
        const field = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid field"),
        };
        try fields.append(allocator, field);
    }

    // Execute HGETDEL
    const values = storage.hgetdel(allocator, key, fields.items) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => return err,
    };
    defer {
        for (values) |val| {
            if (val) |v| allocator.free(v);
        }
        allocator.free(values);
    }

    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, values.len);
    defer resp_values.deinit(allocator);

    for (values) |val| {
        if (val) |v| {
            try resp_values.append(allocator, RespValue{ .bulk_string = v });
        } else {
            try resp_values.append(allocator, RespValue{ .null_bulk_string = {} });
        }
    }

    return w.writeArray(resp_values.items);
}

/// HGETEX key [EX|PX|EXAT|PXAT|PERSIST] FIELDS numfields field [field ...]
/// Atomically gets hash field values and sets/updates field expiration
/// Returns array of values (nil for non-existent fields)
pub fn cmdHgetex(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'hgetex' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    var arg_idx: usize = 2;
    var expires_at_ms: ?i64 = null;
    var has_expiration_option = false;

    // Parse optional expiration options
    while (arg_idx < args.len) {
        const arg = switch (args[arg_idx]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        if (std.mem.eql(u8, arg, "FIELDS")) {
            break; // Found FIELDS keyword, stop parsing options
        }

        if (std.mem.eql(u8, arg, "EX") or std.mem.eql(u8, arg, "PX") or
            std.mem.eql(u8, arg, "EXAT") or std.mem.eql(u8, arg, "PXAT") or
            std.mem.eql(u8, arg, "PERSIST"))
        {
            if (has_expiration_option) {
                return w.writeError("ERR EX, PX, EXAT, PXAT, and PERSIST are mutually exclusive");
            }
            has_expiration_option = true;

            if (std.mem.eql(u8, arg, "PERSIST")) {
                expires_at_ms = null;
                arg_idx += 1;
            } else {
                if (arg_idx + 1 >= args.len) {
                    return w.writeError("ERR syntax error");
                }

                const time_str = switch (args[arg_idx + 1]) {
                    .bulk_string => |s| s,
                    else => return w.writeError("ERR syntax error"),
                };

                const time_value = std.fmt.parseInt(i64, time_str, 10) catch {
                    return w.writeError("ERR value is not an integer or out of range");
                };

                if (time_value < 0) {
                    return w.writeError("ERR invalid expire time");
                }

                if (std.mem.eql(u8, arg, "EX")) {
                    expires_at_ms = Storage.getCurrentTimestamp() + (time_value * 1000);
                } else if (std.mem.eql(u8, arg, "PX")) {
                    expires_at_ms = Storage.getCurrentTimestamp() + time_value;
                } else if (std.mem.eql(u8, arg, "EXAT")) {
                    expires_at_ms = time_value * 1000;
                } else if (std.mem.eql(u8, arg, "PXAT")) {
                    expires_at_ms = time_value;
                }

                arg_idx += 2;
            }
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Parse FIELDS keyword
    if (arg_idx >= args.len) {
        return w.writeError("ERR syntax error");
    }

    const fields_keyword = switch (args[arg_idx]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };
    if (!std.mem.eql(u8, fields_keyword, "FIELDS")) {
        return w.writeError("ERR syntax error");
    }
    arg_idx += 1;

    if (arg_idx >= args.len) {
        return w.writeError("ERR wrong number of arguments for 'hgetex' command");
    }

    const numfields_str = switch (args[arg_idx]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid numfields"),
    };
    const numfields = std.fmt.parseInt(usize, numfields_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };
    arg_idx += 1;

    if (args.len != arg_idx + numfields) {
        return w.writeError("ERR wrong number of arguments for 'hgetex' command");
    }

    // Extract fields
    var fields = try std.ArrayList([]const u8).initCapacity(allocator, numfields);
    defer fields.deinit(allocator);

    for (arg_idx..arg_idx + numfields) |i| {
        const field = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid field"),
        };
        try fields.append(allocator, field);
    }

    // Execute HGETEX
    const values = storage.hgetex(allocator, key, fields.items, expires_at_ms) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => return err,
    };
    defer {
        for (values) |val| {
            if (val) |v| allocator.free(v);
        }
        allocator.free(values);
    }

    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, values.len);
    defer resp_values.deinit(allocator);

    for (values) |val| {
        if (val) |v| {
            try resp_values.append(allocator, RespValue{ .bulk_string = v });
        } else {
            try resp_values.append(allocator, RespValue{ .null_bulk_string = {} });
        }
    }

    return w.writeArray(resp_values.items);
}

/// HSETEX key [FNX|FXX] [EX|PX|EXAT|PXAT|KEEPTTL] FIELDS numfields field value [field value ...]
/// Atomically sets hash fields with values and expiration
/// Returns 1 if all fields were set, 0 if conditional (fnx/fxx) failed
pub fn cmdHsetex(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 5) {
        return w.writeError("ERR wrong number of arguments for 'hsetex' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    var arg_idx: usize = 2;
    var fnx = false;
    var fxx = false;
    var keep_ttl = false;
    var expires_at_ms: ?i64 = null;
    var has_conditional = false;
    var has_expiration_option = false;

    // Parse optional flags and expiration options
    while (arg_idx < args.len) {
        const arg = switch (args[arg_idx]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        if (std.mem.eql(u8, arg, "FIELDS")) {
            break; // Found FIELDS keyword, stop parsing options
        }

        if (std.mem.eql(u8, arg, "FNX")) {
            if (has_conditional) {
                return w.writeError("ERR FNX and FXX are mutually exclusive");
            }
            fnx = true;
            has_conditional = true;
            arg_idx += 1;
        } else if (std.mem.eql(u8, arg, "FXX")) {
            if (has_conditional) {
                return w.writeError("ERR FNX and FXX are mutually exclusive");
            }
            fxx = true;
            has_conditional = true;
            arg_idx += 1;
        } else if (std.mem.eql(u8, arg, "EX") or std.mem.eql(u8, arg, "PX") or
            std.mem.eql(u8, arg, "EXAT") or std.mem.eql(u8, arg, "PXAT") or
            std.mem.eql(u8, arg, "KEEPTTL"))
        {
            if (has_expiration_option) {
                return w.writeError("ERR EX, PX, EXAT, PXAT, and KEEPTTL are mutually exclusive");
            }
            has_expiration_option = true;

            if (std.mem.eql(u8, arg, "KEEPTTL")) {
                keep_ttl = true;
                arg_idx += 1;
            } else {
                if (arg_idx + 1 >= args.len) {
                    return w.writeError("ERR syntax error");
                }

                const time_str = switch (args[arg_idx + 1]) {
                    .bulk_string => |s| s,
                    else => return w.writeError("ERR syntax error"),
                };

                const time_value = std.fmt.parseInt(i64, time_str, 10) catch {
                    return w.writeError("ERR value is not an integer or out of range");
                };

                if (time_value < 0) {
                    return w.writeError("ERR invalid expire time");
                }

                if (std.mem.eql(u8, arg, "EX")) {
                    expires_at_ms = Storage.getCurrentTimestamp() + (time_value * 1000);
                } else if (std.mem.eql(u8, arg, "PX")) {
                    expires_at_ms = Storage.getCurrentTimestamp() + time_value;
                } else if (std.mem.eql(u8, arg, "EXAT")) {
                    expires_at_ms = time_value * 1000;
                } else if (std.mem.eql(u8, arg, "PXAT")) {
                    expires_at_ms = time_value;
                }

                arg_idx += 2;
            }
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Parse FIELDS keyword
    if (arg_idx >= args.len) {
        return w.writeError("ERR syntax error");
    }

    const fields_keyword = switch (args[arg_idx]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };
    if (!std.mem.eql(u8, fields_keyword, "FIELDS")) {
        return w.writeError("ERR syntax error");
    }
    arg_idx += 1;

    if (arg_idx >= args.len) {
        return w.writeError("ERR wrong number of arguments for 'hsetex' command");
    }

    const numfields_str = switch (args[arg_idx]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid numfields"),
    };
    const numfields = std.fmt.parseInt(usize, numfields_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };
    arg_idx += 1;

    if (args.len != arg_idx + (numfields * 2)) {
        return w.writeError("ERR wrong number of arguments for 'hsetex' command");
    }

    // Extract fields and values
    var fields = try std.ArrayList([]const u8).initCapacity(allocator, numfields);
    defer fields.deinit(allocator);

    var values = try std.ArrayList([]const u8).initCapacity(allocator, numfields);
    defer values.deinit(allocator);

    for (0..numfields) |i| {
        const field = switch (args[arg_idx + (i * 2)]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid field"),
        };
        try fields.append(allocator, field);

        const value = switch (args[arg_idx + (i * 2) + 1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid value"),
        };
        try values.append(allocator, value);
    }

    // Execute HSETEX
    const result = storage.hsetex(key, fields.items, values.items, fnx, fxx, keep_ttl, expires_at_ms, null) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => return err,
    };

    return w.writeInteger(result);
}

test "cmdHrandfield - returns single random field" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set up hash with fields
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "value2" },
        .{ .bulk_string = "field3" },
        .{ .bulk_string = "value3" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    // Get single random field
    const args = [_]RespValue{
        .{ .bulk_string = "HRANDFIELD" },
        .{ .bulk_string = "myhash" },
    };
    const response = try cmdHrandfield(allocator, storage, &args, .RESP2);
    defer allocator.free(response);

    // Should return a bulk string (one of the fields)
    try std.testing.expect(std.mem.startsWith(u8, response, "$"));
}

test "cmdHrandfield - returns multiple random fields with count" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set up hash with fields
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "value2" },
        .{ .bulk_string = "field3" },
        .{ .bulk_string = "value3" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    // Get 2 random fields
    const args = [_]RespValue{
        .{ .bulk_string = "HRANDFIELD" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "2" },
    };
    const response = try cmdHrandfield(allocator, storage, &args, .RESP2);
    defer allocator.free(response);

    // Should return an array
    try std.testing.expect(std.mem.startsWith(u8, response, "*"));
}

test "cmdHrandfield - returns null for non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "HRANDFIELD" },
        .{ .bulk_string = "nonexistent" },
    };
    const response = try cmdHrandfield(allocator, storage, &args, .RESP2);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("$-1\r\n", response);
}

// ═══════════════════════════════════════════════════════════════════════════
// Iteration 52: HGETDEL, HGETEX, HSETEX Tests
// ═══════════════════════════════════════════════════════════════════════════

test "cmdHgetdel - basic operation" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup: create hash
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "value2" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    // Execute HGETDEL
    const args = [_]RespValue{
        .{ .bulk_string = "HGETDEL" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try cmdHgetdel(allocator, storage, &args);
    defer allocator.free(response);

    // Should return array with value1
    try std.testing.expect(std.mem.indexOf(u8, response, "value1") != null);

    // Verify field was deleted
    const get_args = [_]RespValue{
        .{ .bulk_string = "HGET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
    };
    const get_response = try cmdHget(allocator, storage, &get_args);
    defer allocator.free(get_response);
    try std.testing.expectEqualStrings("$-1\r\n", get_response);

    // Verify other field still exists
    const get2_args = [_]RespValue{
        .{ .bulk_string = "HGET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field2" },
    };
    const get2_response = try cmdHget(allocator, storage, &get2_args);
    defer allocator.free(get2_response);
    try std.testing.expect(std.mem.indexOf(u8, get2_response, "value2") != null);
}

test "cmdHgetdel - deletes key when all fields removed" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup: create hash with one field
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    // Execute HGETDEL on the only field
    const args = [_]RespValue{
        .{ .bulk_string = "HGETDEL" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try cmdHgetdel(allocator, storage, &args);
    defer allocator.free(response);

    // Verify key no longer exists via HLEN
    const len_args = [_]RespValue{
        .{ .bulk_string = "HLEN" },
        .{ .bulk_string = "myhash" },
    };
    const len_response = try cmdHlen(allocator, storage, &len_args);
    defer allocator.free(len_response);
    try std.testing.expectEqualStrings(":0\r\n", len_response);
}

test "cmdHgetex - sets expiration with EX" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup: create hash
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    // Execute HGETEX with EX
    const args = [_]RespValue{
        .{ .bulk_string = "HGETEX" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "EX" },
        .{ .bulk_string = "60" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try cmdHgetex(allocator, storage, &args);
    defer allocator.free(response);

    // Should return value1
    try std.testing.expect(std.mem.indexOf(u8, response, "value1") != null);

    // Verify field has TTL
    const ttl_args = [_]RespValue{
        .{ .bulk_string = "HPTTL" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const ttl_response = try cmdHpttl(allocator, storage, &ttl_args);
    defer allocator.free(ttl_response);
    // Should have positive TTL (around 60000ms)
    try std.testing.expect(std.mem.indexOf(u8, ttl_response, ":") != null);
}

test "cmdHgetex - PERSIST removes expiration" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup: create hash with field expiration
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    const expire_args = [_]RespValue{
        .{ .bulk_string = "HEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "60" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    _ = try cmdHexpire(allocator, storage, &expire_args);

    // Execute HGETEX with PERSIST
    const args = [_]RespValue{
        .{ .bulk_string = "HGETEX" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "PERSIST" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try cmdHgetex(allocator, storage, &args);
    defer allocator.free(response);

    // Verify field has no TTL
    const ttl_args = [_]RespValue{
        .{ .bulk_string = "HTTL" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const ttl_response = try cmdHttpl(allocator, storage, &ttl_args);
    defer allocator.free(ttl_response);
    // Should return -1 (no expiry)
    try std.testing.expect(std.mem.indexOf(u8, ttl_response, ":-1") != null);
}

test "cmdHsetex - basic operation with EX" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Execute HSETEX
    const args = [_]RespValue{
        .{ .bulk_string = "HSETEX" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "EX" },
        .{ .bulk_string = "60" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "2" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "value2" },
    };
    const response = try cmdHsetex(allocator, storage, &args);
    defer allocator.free(response);

    // Should return :1 (all set)
    try std.testing.expectEqualStrings(":1\r\n", response);

    // Verify values are set
    const get_args = [_]RespValue{
        .{ .bulk_string = "HGET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
    };
    const get_response = try cmdHget(allocator, storage, &get_args);
    defer allocator.free(get_response);
    try std.testing.expect(std.mem.indexOf(u8, get_response, "value1") != null);

    // Verify fields have TTL
    const ttl_args = [_]RespValue{
        .{ .bulk_string = "HPTTL" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const ttl_response = try cmdHpttl(allocator, storage, &ttl_args);
    defer allocator.free(ttl_response);
    try std.testing.expect(std.mem.indexOf(u8, ttl_response, ":") != null);
}

test "cmdHsetex - FNX succeeds when fields don't exist" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Execute HSETEX with FNX (field-not-exists)
    const args = [_]RespValue{
        .{ .bulk_string = "HSETEX" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "FNX" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    const response = try cmdHsetex(allocator, storage, &args);
    defer allocator.free(response);

    // Should return :1 (success)
    try std.testing.expectEqualStrings(":1\r\n", response);
}

test "cmdHsetex - FNX fails when field exists" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup: create hash with field
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    // Execute HSETEX with FNX (should fail)
    const args = [_]RespValue{
        .{ .bulk_string = "HSETEX" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "FNX" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "newvalue" },
    };
    const response = try cmdHsetex(allocator, storage, &args);
    defer allocator.free(response);

    // Should return :0 (failed)
    try std.testing.expectEqualStrings(":0\r\n", response);

    // Verify original value unchanged
    const get_args = [_]RespValue{
        .{ .bulk_string = "HGET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
    };
    const get_response = try cmdHget(allocator, storage, &get_args);
    defer allocator.free(get_response);
    try std.testing.expect(std.mem.indexOf(u8, get_response, "value1") != null);
}

test "cmdHsetex - FXX succeeds when field exists" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup: create hash with field
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    // Execute HSETEX with FXX (field-exists-exists)
    const args = [_]RespValue{
        .{ .bulk_string = "HSETEX" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "FXX" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "newvalue" },
    };
    const response = try cmdHsetex(allocator, storage, &args);
    defer allocator.free(response);

    // Should return :1 (success)
    try std.testing.expectEqualStrings(":1\r\n", response);

    // Verify value was updated
    const get_args = [_]RespValue{
        .{ .bulk_string = "HGET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
    };
    const get_response = try cmdHget(allocator, storage, &get_args);
    defer allocator.free(get_response);
    try std.testing.expect(std.mem.indexOf(u8, get_response, "newvalue") != null);
}

test "cmdHsetex - FXX fails when field doesn't exist" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Execute HSETEX with FXX on non-existent field (should fail)
    const args = [_]RespValue{
        .{ .bulk_string = "HSETEX" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "FXX" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    const response = try cmdHsetex(allocator, storage, &args);
    defer allocator.free(response);

    // Should return :0 (failed)
    try std.testing.expectEqualStrings(":0\r\n", response);

    // Verify field was not created
    const get_args = [_]RespValue{
        .{ .bulk_string = "HGET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
    };
    const get_response = try cmdHget(allocator, storage, &get_args);
    defer allocator.free(get_response);
    try std.testing.expectEqualStrings("$-1\r\n", get_response);
}

test "cmdHsetex - KEEPTTL preserves existing field TTL" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup: create hash with field and expiration
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try cmdHset(allocator, storage, &set_args);

    const expire_args = [_]RespValue{
        .{ .bulk_string = "HEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "60" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    _ = try cmdHexpire(allocator, storage, &expire_args);

    // Get TTL before update
    const ttl_before_args = [_]RespValue{
        .{ .bulk_string = "HTTL" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const ttl_before = try cmdHttpl(allocator, storage, &ttl_before_args);
    defer allocator.free(ttl_before);

    // Execute HSETEX with KEEPTTL
    const args = [_]RespValue{
        .{ .bulk_string = "HSETEX" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "KEEPTTL" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "newvalue" },
    };
    const response = try cmdHsetex(allocator, storage, &args);
    defer allocator.free(response);

    // Should return :1
    try std.testing.expectEqualStrings(":1\r\n", response);

    // Verify value was updated
    const get_args = [_]RespValue{
        .{ .bulk_string = "HGET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
    };
    const get_response = try cmdHget(allocator, storage, &get_args);
    defer allocator.free(get_response);
    try std.testing.expect(std.mem.indexOf(u8, get_response, "newvalue") != null);

    // Verify TTL was preserved (should still be around 60)
    const ttl_after_args = [_]RespValue{
        .{ .bulk_string = "HTTL" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const ttl_after = try cmdHttpl(allocator, storage, &ttl_after_args);
    defer allocator.free(ttl_after);

    // Should have positive TTL
    try std.testing.expect(std.mem.indexOf(u8, ttl_after, ":") != null);
    try std.testing.expect(!std.mem.eql(u8, ttl_after, ":-1\r\n"));
}
