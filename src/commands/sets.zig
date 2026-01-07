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
