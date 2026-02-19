const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;

/// SADD key member [member ...]
/// Adds one or more members to a set
/// Returns integer - count of members actually added (excluding duplicates)
pub fn cmdSadd(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'sadd' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Extract all members
    var members = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 2);
    defer members.deinit(allocator);

    for (args[2..]) |arg| {
        const member = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid member"),
        };
        try members.append(allocator, member);
    }

    // Execute SADD
    const added_count = storage.sadd(key, members.items, null) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(added_count));
}

/// SREM key member [member ...]
/// Removes one or more members from a set
/// Returns integer - count of members actually removed
pub fn cmdSrem(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'srem' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Extract all members
    var members = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 2);
    defer members.deinit(allocator);

    for (args[2..]) |arg| {
        const member = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid member"),
        };
        try members.append(allocator, member);
    }

    // Execute SREM
    const removed_count = storage.srem(allocator, key, members.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(removed_count));
}

/// SISMEMBER key member
/// Check if member exists in set
/// Returns integer: 1 if member exists, 0 otherwise
pub fn cmdSismember(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'sismember' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const member = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid member"),
    };

    // Execute SISMEMBER
    const is_member = storage.sismember(key, member) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(if (is_member) @as(i64, 1) else @as(i64, 0));
}

/// SMEMBERS key
/// Returns all members of the set
/// Returns array of bulk strings
pub fn cmdSmembers(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'smembers' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Check type first to provide proper WRONGTYPE error
    const value_type = storage.getType(key);
    if (value_type) |vtype| {
        if (vtype != .set) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }

    // Execute SMEMBERS
    const members = (try storage.smembers(allocator, key)) orelse {
        // Key doesn't exist or is empty - return empty array
        return w.writeArray(&[_]RespValue{});
    };
    defer allocator.free(members);

    // Convert to RespValue array
    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, members.len);
    defer resp_values.deinit(allocator);

    for (members) |member| {
        try resp_values.append(allocator, RespValue{ .bulk_string = member });
    }

    return w.writeArray(resp_values.items);
}

/// SCARD key
/// Returns the cardinality (number of elements) of the set
/// Returns integer: cardinality or 0 if key doesn't exist
pub fn cmdScard(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'scard' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Check type first to provide proper WRONGTYPE error
    const value_type = storage.getType(key);
    if (value_type) |vtype| {
        if (vtype != .set) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }

    // Execute SCARD
    const cardinality = storage.scard(key) orelse 0;
    return w.writeInteger(@intCast(cardinality));
}

/// SUNION key [key ...]
/// Returns the members of the set resulting from the union of all the given sets
/// Non-existent keys are treated as empty sets
pub fn cmdSunion(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'sunion' command");
    }

    var keys = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 1);
    defer keys.deinit(allocator);

    for (args[1..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    const members = storage.sunion(allocator, keys.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };
    defer allocator.free(members);

    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, members.len);
    defer resp_values.deinit(allocator);

    for (members) |member| {
        try resp_values.append(allocator, RespValue{ .bulk_string = member });
    }

    return w.writeArray(resp_values.items);
}

/// SINTER key [key ...]
/// Returns the members of the set resulting from the intersection of all the given sets
/// Non-existent keys are treated as empty sets
pub fn cmdSinter(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'sinter' command");
    }

    var keys = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 1);
    defer keys.deinit(allocator);

    for (args[1..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    const members = storage.sinter(allocator, keys.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };
    defer allocator.free(members);

    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, members.len);
    defer resp_values.deinit(allocator);

    for (members) |member| {
        try resp_values.append(allocator, RespValue{ .bulk_string = member });
    }

    return w.writeArray(resp_values.items);
}

/// SDIFF key [key ...]
/// Returns the members of the set resulting from the difference between the first set and all successive sets
/// Non-existent keys are treated as empty sets
pub fn cmdSdiff(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'sdiff' command");
    }

    var keys = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 1);
    defer keys.deinit(allocator);

    for (args[1..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    const members = storage.sdiff(allocator, keys.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };
    defer allocator.free(members);

    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, members.len);
    defer resp_values.deinit(allocator);

    for (members) |member| {
        try resp_values.append(allocator, RespValue{ .bulk_string = member });
    }

    return w.writeArray(resp_values.items);
}

/// SUNIONSTORE destination key [key ...]
/// Store the union of sets in destination. Returns count of members in result.
pub fn cmdSunionstore(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'sunionstore' command");
    }

    const dest = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid destination"),
    };

    var keys = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 2);
    defer keys.deinit(allocator);

    for (args[2..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    const count = storage.sunionstore(allocator, dest, keys.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(count));
}

/// SINTERSTORE destination key [key ...]
/// Store the intersection of sets in destination. Returns count of members in result.
pub fn cmdSinterstore(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'sinterstore' command");
    }

    const dest = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid destination"),
    };

    var keys = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 2);
    defer keys.deinit(allocator);

    for (args[2..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    const count = storage.sinterstore(allocator, dest, keys.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(count));
}

/// SDIFFSTORE destination key [key ...]
/// Store the difference of sets in destination. Returns count of members in result.
pub fn cmdSdiffstore(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'sdiffstore' command");
    }

    const dest = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid destination"),
    };

    var keys = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 2);
    defer keys.deinit(allocator);

    for (args[2..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    const count = storage.sdiffstore(allocator, dest, keys.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(count));
}

/// SPOP key [count]
/// Remove and return random member(s) from set.
/// Returns bulk string (single pop) or array (with count).
pub fn cmdSpop(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2 or args.len > 3) {
        return w.writeError("ERR wrong number of arguments for 'spop' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const has_count = args.len == 3;
    const count: usize = if (has_count) blk: {
        const cs = switch (args[2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR value is not an integer or out of range"),
        };
        const n = std.fmt.parseInt(i64, cs, 10) catch {
            return w.writeError("ERR value is not an integer or out of range");
        };
        if (n < 0) return w.writeError("ERR value is not an integer or out of range");
        break :blk @as(usize, @intCast(n));
    } else 0;

    const popped = storage.spop(allocator, key, count) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    if (!has_count) {
        // Single pop: return bulk string or null
        if (popped) |members| {
            defer {
                for (members) |m| allocator.free(m);
                allocator.free(members);
            }
            if (members.len > 0) {
                return w.writeBulkString(members[0]);
            }
        }
        return w.writeBulkString(null);
    } else {
        // With count: return array
        if (popped) |members| {
            defer {
                for (members) |m| allocator.free(m);
                allocator.free(members);
            }
            var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, members.len);
            defer resp_values.deinit(allocator);
            for (members) |m| {
                try resp_values.append(allocator, RespValue{ .bulk_string = m });
            }
            return w.writeArray(resp_values.items);
        }
        return w.writeArray(&[_]RespValue{});
    }
}

/// SRANDMEMBER key [count]
/// Return random member(s) from set without removing.
/// Without count: returns one bulk string.
/// With positive count: up to count distinct members.
/// With negative count: abs(count) members (may repeat).
pub fn cmdSrandmember(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2 or args.len > 3) {
        return w.writeError("ERR wrong number of arguments for 'srandmember' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const has_count = args.len == 3;
    const count: i64 = if (has_count) blk: {
        const cs = switch (args[2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR value is not an integer or out of range"),
        };
        break :blk std.fmt.parseInt(i64, cs, 10) catch {
            return w.writeError("ERR value is not an integer or out of range");
        };
    } else 1;

    const members = storage.srandmember(allocator, key, count) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    if (!has_count) {
        if (members) |ms| {
            defer {
                for (ms) |m| allocator.free(m);
                allocator.free(ms);
            }
            if (ms.len > 0) return w.writeBulkString(ms[0]);
        }
        return w.writeBulkString(null);
    } else {
        if (members) |ms| {
            defer {
                for (ms) |m| allocator.free(m);
                allocator.free(ms);
            }
            var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, ms.len);
            defer resp_values.deinit(allocator);
            for (ms) |m| {
                try resp_values.append(allocator, RespValue{ .bulk_string = m });
            }
            return w.writeArray(resp_values.items);
        }
        return w.writeArray(&[_]RespValue{});
    }
}

/// SMOVE source destination member
/// Atomically move member between sets.
/// Returns :1 if moved, :0 if member not in source.
pub fn cmdSmove(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'smove' command");
    }

    const source = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid source"),
    };
    const destination = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid destination"),
    };
    const member = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid member"),
    };

    const moved = storage.smove(source, destination, member) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(if (moved) 1 else 0);
}

/// SMISMEMBER key member [member ...]
/// Bulk membership check. Returns array of 0/1.
pub fn cmdSmismember(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'smismember' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    var members = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 2);
    defer members.deinit(allocator);

    for (args[2..]) |arg| {
        const member = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid member"),
        };
        try members.append(allocator, member);
    }

    const results = storage.smismember(allocator, key, members.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };
    defer allocator.free(results);

    var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, results.len);
    defer resp_values.deinit(allocator);

    for (results) |is_member| {
        try resp_values.append(allocator, RespValue{ .integer = if (is_member) 1 else 0 });
    }

    return w.writeArray(resp_values.items);
}

/// SINTERCARD numkeys key [key ...] [LIMIT limit]
/// Returns cardinality of intersection without storing.
pub fn cmdSintercard(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'sintercard' command");
    }

    const numkeys_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not an integer or out of range"),
    };
    const numkeys = std.fmt.parseInt(usize, numkeys_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };
    if (numkeys == 0) {
        return w.writeError("ERR numkeys can't be non-positive");
    }
    if (args.len < 2 + numkeys) {
        return w.writeError("ERR syntax error");
    }

    var keys = try std.ArrayList([]const u8).initCapacity(allocator, numkeys);
    defer keys.deinit(allocator);

    for (args[2..2 + numkeys]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    // Parse optional LIMIT
    var limit: usize = 0;
    var i: usize = 2 + numkeys;
    while (i < args.len) : (i += 1) {
        const opt_str = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const opt_upper = try std.ascii.allocUpperString(allocator, opt_str);
        defer allocator.free(opt_upper);
        if (std.mem.eql(u8, opt_upper, "LIMIT")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const limit_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            limit = std.fmt.parseInt(usize, limit_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
        }
    }

    const card = storage.sintercard(allocator, keys.items, limit) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(card));
}

// Embedded unit tests

test "sets - SADD single member" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "hello" },
    };

    const result = try cmdSadd(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
}

test "sets - SADD multiple members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "one" },
        RespValue{ .bulk_string = "two" },
        RespValue{ .bulk_string = "three" },
    };

    const result = try cmdSadd(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":3\r\n", result);
}

test "sets - SADD duplicate members returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // First add
    const args1 = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "hello" },
    };
    const result1 = try cmdSadd(allocator, storage, &args1);
    defer allocator.free(result1);

    // Add same member again
    const args2 = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "hello" },
    };
    const result2 = try cmdSadd(allocator, storage, &args2);
    defer allocator.free(result2);

    try std.testing.expectEqualStrings(":0\r\n", result2);
}

test "sets - SADD on existing string returns WRONGTYPE" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "string", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "member" },
    };

    const result = try cmdSadd(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "WRONGTYPE") != null);
}

test "sets - SREM single member" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "one" },
        RespValue{ .bulk_string = "two" },
    };
    const setup_result = try cmdSadd(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test SREM
    const args = [_]RespValue{
        RespValue{ .bulk_string = "SREM" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "one" },
    };

    const result = try cmdSrem(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
}

test "sets - SREM multiple members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "one" },
        RespValue{ .bulk_string = "two" },
        RespValue{ .bulk_string = "three" },
    };
    const setup_result = try cmdSadd(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test SREM
    const args = [_]RespValue{
        RespValue{ .bulk_string = "SREM" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "one" },
        RespValue{ .bulk_string = "three" },
    };

    const result = try cmdSrem(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":2\r\n", result);
}

test "sets - SREM non-existent member returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "one" },
    };
    const setup_result = try cmdSadd(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test SREM
    const args = [_]RespValue{
        RespValue{ .bulk_string = "SREM" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "two" },
    };

    const result = try cmdSrem(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "sets - SREM on non-existent key returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SREM" },
        RespValue{ .bulk_string = "nosuchkey" },
        RespValue{ .bulk_string = "member" },
    };

    const result = try cmdSrem(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "sets - SISMEMBER returns 1 for member" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "one" },
    };
    const setup_result = try cmdSadd(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test SISMEMBER
    const args = [_]RespValue{
        RespValue{ .bulk_string = "SISMEMBER" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "one" },
    };

    const result = try cmdSismember(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
}

test "sets - SISMEMBER returns 0 for non-member" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "one" },
    };
    const setup_result = try cmdSadd(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test SISMEMBER
    const args = [_]RespValue{
        RespValue{ .bulk_string = "SISMEMBER" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "two" },
    };

    const result = try cmdSismember(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "sets - SISMEMBER on non-existent key returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SISMEMBER" },
        RespValue{ .bulk_string = "nosuchkey" },
        RespValue{ .bulk_string = "member" },
    };

    const result = try cmdSismember(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "sets - SMEMBERS returns all members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "one" },
        RespValue{ .bulk_string = "two" },
        RespValue{ .bulk_string = "three" },
    };
    const setup_result = try cmdSadd(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test SMEMBERS
    const args = [_]RespValue{
        RespValue{ .bulk_string = "SMEMBERS" },
        RespValue{ .bulk_string = "myset" },
    };

    const result = try cmdSmembers(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*3\r\n") != null);
}

test "sets - SMEMBERS on non-existent key returns empty array" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SMEMBERS" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    const result = try cmdSmembers(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("*0\r\n", result);
}

test "sets - SCARD returns cardinality" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Setup
    const setup_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "one" },
        RespValue{ .bulk_string = "two" },
        RespValue{ .bulk_string = "three" },
    };
    const setup_result = try cmdSadd(allocator, storage, &setup_args);
    defer allocator.free(setup_result);

    // Test SCARD
    const args = [_]RespValue{
        RespValue{ .bulk_string = "SCARD" },
        RespValue{ .bulk_string = "myset" },
    };

    const result = try cmdScard(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":3\r\n", result);
}

test "sets - SCARD on non-existent key returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SCARD" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    const result = try cmdScard(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "sets - integration test: SADD SCARD SMEMBERS SISMEMBER SREM" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // SADD
    const sadd_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "one" },
        RespValue{ .bulk_string = "two" },
        RespValue{ .bulk_string = "three" },
    };
    const sadd_result = try cmdSadd(allocator, storage, &sadd_args);
    defer allocator.free(sadd_result);
    try std.testing.expectEqualStrings(":3\r\n", sadd_result);

    // SCARD
    const scard_args = [_]RespValue{
        RespValue{ .bulk_string = "SCARD" },
        RespValue{ .bulk_string = "myset" },
    };
    const scard_result = try cmdScard(allocator, storage, &scard_args);
    defer allocator.free(scard_result);
    try std.testing.expectEqualStrings(":3\r\n", scard_result);

    // SISMEMBER
    const sismember_args = [_]RespValue{
        RespValue{ .bulk_string = "SISMEMBER" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "two" },
    };
    const sismember_result = try cmdSismember(allocator, storage, &sismember_args);
    defer allocator.free(sismember_result);
    try std.testing.expectEqualStrings(":1\r\n", sismember_result);

    // SREM
    const srem_args = [_]RespValue{
        RespValue{ .bulk_string = "SREM" },
        RespValue{ .bulk_string = "myset" },
        RespValue{ .bulk_string = "two" },
    };
    const srem_result = try cmdSrem(allocator, storage, &srem_args);
    defer allocator.free(srem_result);
    try std.testing.expectEqualStrings(":1\r\n", srem_result);

    // SCARD after SREM
    const scard2_result = try cmdScard(allocator, storage, &scard_args);
    defer allocator.free(scard2_result);
    try std.testing.expectEqualStrings(":2\r\n", scard2_result);
}

test "sets - SUNION basic" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set up two sets
    const sadd1_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "s1" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
    };
    _ = try cmdSadd(allocator, storage, &sadd1_args);

    const sadd2_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "s2" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    _ = try cmdSadd(allocator, storage, &sadd2_args);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SUNION" },
        RespValue{ .bulk_string = "s1" },
        RespValue{ .bulk_string = "s2" },
    };
    const result = try cmdSunion(allocator, storage, &args);
    defer allocator.free(result);

    // Should contain 3 unique members: a, b, c
    try std.testing.expect(std.mem.startsWith(u8, result, "*3\r\n"));
}

test "sets - SINTER basic" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const sadd1_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "s1" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    _ = try cmdSadd(allocator, storage, &sadd1_args);

    const sadd2_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "s2" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
        RespValue{ .bulk_string = "d" },
    };
    _ = try cmdSadd(allocator, storage, &sadd2_args);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SINTER" },
        RespValue{ .bulk_string = "s1" },
        RespValue{ .bulk_string = "s2" },
    };
    const result = try cmdSinter(allocator, storage, &args);
    defer allocator.free(result);

    // Should contain 2 members: b and c
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
}

test "sets - SINTER with non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const sadd1_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "s1" },
        RespValue{ .bulk_string = "a" },
    };
    _ = try cmdSadd(allocator, storage, &sadd1_args);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SINTER" },
        RespValue{ .bulk_string = "s1" },
        RespValue{ .bulk_string = "nosuchkey" },
    };
    const result = try cmdSinter(allocator, storage, &args);
    defer allocator.free(result);

    // Empty intersection with non-existent key
    try std.testing.expectEqualStrings("*0\r\n", result);
}

test "sets - SDIFF basic" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const sadd1_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "s1" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    _ = try cmdSadd(allocator, storage, &sadd1_args);

    const sadd2_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "s2" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "d" },
    };
    _ = try cmdSadd(allocator, storage, &sadd2_args);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SDIFF" },
        RespValue{ .bulk_string = "s1" },
        RespValue{ .bulk_string = "s2" },
    };
    const result = try cmdSdiff(allocator, storage, &args);
    defer allocator.free(result);

    // Should contain a and c (in s1 but not s2)
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
}

test "sets - SUNIONSTORE" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const sadd1_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "s1" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
    };
    _ = try cmdSadd(allocator, storage, &sadd1_args);

    const sadd2_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "s2" },
        RespValue{ .bulk_string = "c" },
    };
    _ = try cmdSadd(allocator, storage, &sadd2_args);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SUNIONSTORE" },
        RespValue{ .bulk_string = "dest" },
        RespValue{ .bulk_string = "s1" },
        RespValue{ .bulk_string = "s2" },
    };
    const result = try cmdSunionstore(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":3\r\n", result);
}

test "sets - SINTERSTORE" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const sadd1_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "s1" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
    };
    _ = try cmdSadd(allocator, storage, &sadd1_args);

    const sadd2_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "s2" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    _ = try cmdSadd(allocator, storage, &sadd2_args);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SINTERSTORE" },
        RespValue{ .bulk_string = "dest" },
        RespValue{ .bulk_string = "s1" },
        RespValue{ .bulk_string = "s2" },
    };
    const result = try cmdSinterstore(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
}

test "sets - SDIFFSTORE" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const sadd1_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "s1" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "c" },
    };
    _ = try cmdSadd(allocator, storage, &sadd1_args);

    const sadd2_args = [_]RespValue{
        RespValue{ .bulk_string = "SADD" },
        RespValue{ .bulk_string = "s2" },
        RespValue{ .bulk_string = "b" },
    };
    _ = try cmdSadd(allocator, storage, &sadd2_args);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SDIFFSTORE" },
        RespValue{ .bulk_string = "dest" },
        RespValue{ .bulk_string = "s1" },
        RespValue{ .bulk_string = "s2" },
    };
    const result = try cmdSdiffstore(allocator, storage, &args);
    defer allocator.free(result);

    // a and c are in s1 but not s2
    try std.testing.expectEqualStrings(":2\r\n", result);
}
