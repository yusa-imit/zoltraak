// Iteration 348: ACL CAT real command listing
// ACL CAT <category> previously returned empty array (stub).
// This iteration implements real command listing per category.
//
// NOTE: cmdACLCat receives array[1..] from dispatch, so array[0]="CAT",
// array[1]=category name (if present).
const std = @import("std");
const testing = std.testing;
const zoltraak = @import("zoltraak");

const RespValue = zoltraak.protocol.RespValue;
const acl = zoltraak.acl_commands;

fn containsCommand(resp: []const u8, cmd_lower: []const u8) bool {
    // Search for $N\r\ncmd\r\n pattern in RESP bulk string array
    return std.mem.indexOf(u8, resp, cmd_lower) != null;
}

test "iter348 - ACL CAT (no args) returns categories list" {
    const allocator = testing.allocator;
    const args = [_]RespValue{.{ .bulk_string = "CAT" }};
    const result = try acl.cmdACLCat(allocator, &args);
    defer allocator.free(result);

    // Must be a non-empty RESP array
    try testing.expect(std.mem.startsWith(u8, result, "*"));
    try testing.expect(std.mem.indexOf(u8, result, "string") != null);
    try testing.expect(std.mem.indexOf(u8, result, "hash") != null);
    try testing.expect(std.mem.indexOf(u8, result, "sortedset") != null);
}

test "iter348 - ACL CAT string returns string commands" {
    const allocator = testing.allocator;
    const args = [_]RespValue{
        .{ .bulk_string = "CAT" },
        .{ .bulk_string = "string" },
    };
    const result = try acl.cmdACLCat(allocator, &args);
    defer allocator.free(result);

    // Must be non-empty array (not *0\r\n)
    try testing.expect(!std.mem.eql(u8, result, "*0\r\n"));
    try testing.expect(std.mem.startsWith(u8, result, "*"));
    // GET, SET must be in the string category
    try testing.expect(containsCommand(result, "get"));
    try testing.expect(containsCommand(result, "set"));
}

test "iter348 - ACL CAT hash returns hash commands" {
    const allocator = testing.allocator;
    const args = [_]RespValue{
        .{ .bulk_string = "CAT" },
        .{ .bulk_string = "hash" },
    };
    const result = try acl.cmdACLCat(allocator, &args);
    defer allocator.free(result);

    try testing.expect(!std.mem.eql(u8, result, "*0\r\n"));
    try testing.expect(containsCommand(result, "hset"));
    try testing.expect(containsCommand(result, "hget"));
    try testing.expect(containsCommand(result, "hdel"));
}

test "iter348 - ACL CAT sortedset returns sorted set commands" {
    const allocator = testing.allocator;
    const args = [_]RespValue{
        .{ .bulk_string = "CAT" },
        .{ .bulk_string = "sortedset" },
    };
    const result = try acl.cmdACLCat(allocator, &args);
    defer allocator.free(result);

    try testing.expect(!std.mem.eql(u8, result, "*0\r\n"));
    try testing.expect(containsCommand(result, "zadd"));
    try testing.expect(containsCommand(result, "zrange"));
}

test "iter348 - ACL CAT list returns list commands" {
    const allocator = testing.allocator;
    const args = [_]RespValue{
        .{ .bulk_string = "CAT" },
        .{ .bulk_string = "list" },
    };
    const result = try acl.cmdACLCat(allocator, &args);
    defer allocator.free(result);

    try testing.expect(!std.mem.eql(u8, result, "*0\r\n"));
    try testing.expect(containsCommand(result, "lpush"));
    try testing.expect(containsCommand(result, "rpush"));
    try testing.expect(containsCommand(result, "lrange"));
}

test "iter348 - ACL CAT read returns read-only commands" {
    const allocator = testing.allocator;
    const args = [_]RespValue{
        .{ .bulk_string = "CAT" },
        .{ .bulk_string = "read" },
    };
    const result = try acl.cmdACLCat(allocator, &args);
    defer allocator.free(result);

    try testing.expect(!std.mem.eql(u8, result, "*0\r\n"));
    // GET is a read command
    try testing.expect(containsCommand(result, "get"));
    // HGET is a read command
    try testing.expect(containsCommand(result, "hget"));
}

test "iter348 - ACL CAT write returns write commands" {
    const allocator = testing.allocator;
    const args = [_]RespValue{
        .{ .bulk_string = "CAT" },
        .{ .bulk_string = "write" },
    };
    const result = try acl.cmdACLCat(allocator, &args);
    defer allocator.free(result);

    try testing.expect(!std.mem.eql(u8, result, "*0\r\n"));
    // SET is a write command
    try testing.expect(containsCommand(result, "set"));
    // DEL is a write command
    try testing.expect(containsCommand(result, "del"));
}

test "iter348 - ACL CAT fast returns fast commands" {
    const allocator = testing.allocator;
    const args = [_]RespValue{
        .{ .bulk_string = "CAT" },
        .{ .bulk_string = "fast" },
    };
    const result = try acl.cmdACLCat(allocator, &args);
    defer allocator.free(result);

    try testing.expect(!std.mem.eql(u8, result, "*0\r\n"));
    // GET is O(1) = fast
    try testing.expect(containsCommand(result, "get"));
}

test "iter348 - ACL CAT slow returns non-fast commands" {
    const allocator = testing.allocator;
    const args = [_]RespValue{
        .{ .bulk_string = "CAT" },
        .{ .bulk_string = "slow" },
    };
    const result = try acl.cmdACLCat(allocator, &args);
    defer allocator.free(result);

    // KEYS is not fast (O(N))
    try testing.expect(!std.mem.eql(u8, result, "*0\r\n"));
}

test "iter348 - ACL CAT blocking returns blocking commands" {
    const allocator = testing.allocator;
    const args = [_]RespValue{
        .{ .bulk_string = "CAT" },
        .{ .bulk_string = "blocking" },
    };
    const result = try acl.cmdACLCat(allocator, &args);
    defer allocator.free(result);

    try testing.expect(!std.mem.eql(u8, result, "*0\r\n"));
    try testing.expect(containsCommand(result, "blpop"));
}

test "iter348 - ACL CAT unknown category returns error" {
    const allocator = testing.allocator;
    const args = [_]RespValue{
        .{ .bulk_string = "CAT" },
        .{ .bulk_string = "nonexistentcategory" },
    };
    const result = try acl.cmdACLCat(allocator, &args);
    defer allocator.free(result);

    // Must return an error response
    try testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, result, "nonexistentcategory") != null);
}

test "iter348 - ACL CAT case insensitive" {
    const allocator = testing.allocator;

    // Uppercase category name should work too
    const args = [_]RespValue{
        .{ .bulk_string = "CAT" },
        .{ .bulk_string = "STRING" },
    };
    const result = try acl.cmdACLCat(allocator, &args);
    defer allocator.free(result);

    try testing.expect(!std.mem.eql(u8, result, "*0\r\n"));
    try testing.expect(containsCommand(result, "get"));
}

test "iter348 - ACL CAT pubsub returns pubsub commands" {
    const allocator = testing.allocator;
    const args = [_]RespValue{
        .{ .bulk_string = "CAT" },
        .{ .bulk_string = "pubsub" },
    };
    const result = try acl.cmdACLCat(allocator, &args);
    defer allocator.free(result);

    try testing.expect(!std.mem.eql(u8, result, "*0\r\n"));
    try testing.expect(containsCommand(result, "subscribe"));
    try testing.expect(containsCommand(result, "publish"));
}
