const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const writer_mod = @import("../src/protocol/writer.zig");
const strings = @import("../src/commands/strings.zig");
const Storage = @import("../src/storage/memory.zig").Storage;
const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;

/// Helper: execute FT.AGGREGATE command
fn ftAggregate(storage: *Storage, allocator: std.mem.Allocator, args: []const []const u8) ![]u8 {
    const result = try strings.handleCommand(
        storage,
        allocator,
        "FT.AGGREGATE",
        args,
    );
    return result;
}

test "FT.AGGREGATE: basic arity validation" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    // Missing query argument
    const resp1 = try ftAggregate(&storage, allocator, &[_][]const u8{"myidx"});
    defer allocator.free(resp1);
    try std.testing.expect(std.mem.startsWith(u8, resp1, "-ERR wrong number"));

    // Valid minimal call (index + query)
    _ = try ftAggregate(&storage, allocator, &[_][]const u8{ "myidx", "*" });
}

test "FT.AGGREGATE: nonexistent index" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    const resp = try ftAggregate(&storage, allocator, &[_][]const u8{ "nosuchindex", "*" });
    defer allocator.free(resp);
    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR Unknown index name"));
}

test "FT.AGGREGATE: LOAD clause parsing" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    // Create index first
    _ = try strings.handleCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{
        "idx",       "ON",   "HASH", "SCHEMA",
        "title",     "TEXT",
        "category",  "TAG",
    });

    // LOAD without count
    const resp1 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "LOAD" });
    defer allocator.free(resp1);
    try std.testing.expect(std.mem.startsWith(u8, resp1, "-ERR LOAD requires count"));

    // LOAD with invalid count
    const resp2 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "LOAD", "abc" });
    defer allocator.free(resp2);
    try std.testing.expect(std.mem.startsWith(u8, resp2, "-ERR LOAD count must be"));

    // LOAD with insufficient field arguments
    const resp3 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "LOAD", "2", "title" });
    defer allocator.free(resp3);
    try std.testing.expect(std.mem.startsWith(u8, resp3, "-ERR not enough LOAD"));

    // Valid LOAD clause (returns stub empty result)
    const resp4 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "LOAD", "2", "title", "category" });
    defer allocator.free(resp4);
    try std.testing.expect(std.mem.startsWith(u8, resp4, "*1\r\n:0\r\n")); // [0] = 0 results
}

test "FT.AGGREGATE: GROUPBY clause parsing" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    _ = try strings.handleCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{
        "idx",       "ON",   "HASH", "SCHEMA",
        "category",  "TAG",
    });

    // GROUPBY without count
    const resp1 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "GROUPBY" });
    defer allocator.free(resp1);
    try std.testing.expect(std.mem.startsWith(u8, resp1, "-ERR GROUPBY requires count"));

    // GROUPBY with invalid count
    const resp2 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "GROUPBY", "xyz" });
    defer allocator.free(resp2);
    try std.testing.expect(std.mem.startsWith(u8, resp2, "-ERR GROUPBY count must be"));

    // GROUPBY with insufficient fields
    const resp3 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "GROUPBY", "2", "category" });
    defer allocator.free(resp3);
    try std.testing.expect(std.mem.startsWith(u8, resp3, "-ERR not enough GROUPBY"));

    // Valid GROUPBY clause
    const resp4 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "GROUPBY", "1", "@category" });
    defer allocator.free(resp4);
    try std.testing.expect(std.mem.startsWith(u8, resp4, "*1\r\n:0\r\n"));
}

test "FT.AGGREGATE: REDUCE clause parsing" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    _ = try strings.handleCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{
        "idx",   "ON",   "HASH", "SCHEMA",
        "price", "NUMERIC",
    });

    // REDUCE without function
    const resp1 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "GROUPBY", "1", "@cat", "REDUCE" });
    defer allocator.free(resp1);
    try std.testing.expect(std.mem.startsWith(u8, resp1, "-ERR REDUCE requires"));

    // REDUCE with invalid nargs
    const resp2 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "GROUPBY", "1", "@cat", "REDUCE", "SUM", "abc" });
    defer allocator.free(resp2);
    try std.testing.expect(std.mem.startsWith(u8, resp2, "-ERR REDUCE nargs"));

    // REDUCE with insufficient args
    const resp3 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "GROUPBY", "1", "@cat", "REDUCE", "SUM", "2", "@price" });
    defer allocator.free(resp3);
    try std.testing.expect(std.mem.startsWith(u8, resp3, "-ERR not enough REDUCE"));

    // Unsupported REDUCE function
    const resp4 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "GROUPBY", "1", "@cat", "REDUCE", "STDDEV", "0" });
    defer allocator.free(resp4);
    try std.testing.expect(std.mem.startsWith(u8, resp4, "-ERR unsupported REDUCE"));

    // Valid REDUCE COUNT
    const resp5 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "GROUPBY", "1", "@cat", "REDUCE", "COUNT", "0" });
    defer allocator.free(resp5);
    try std.testing.expect(std.mem.startsWith(u8, resp5, "*1\r\n:0\r\n"));

    // Valid REDUCE with AS clause
    const resp6 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "GROUPBY", "1", "@cat", "REDUCE", "COUNT", "0", "AS", "cnt" });
    defer allocator.free(resp6);
    try std.testing.expect(std.mem.startsWith(u8, resp6, "*1\r\n:0\r\n"));

    // Valid REDUCE SUM with arg
    const resp7 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "GROUPBY", "1", "@cat", "REDUCE", "SUM", "1", "@price" });
    defer allocator.free(resp7);
    try std.testing.expect(std.mem.startsWith(u8, resp7, "*1\r\n:0\r\n"));
}

test "FT.AGGREGATE: SORTBY clause parsing" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    _ = try strings.handleCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{
        "idx",   "ON",   "HASH", "SCHEMA",
        "score", "NUMERIC",
    });

    // SORTBY without count
    const resp1 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "SORTBY" });
    defer allocator.free(resp1);
    try std.testing.expect(std.mem.startsWith(u8, resp1, "-ERR SORTBY requires count"));

    // SORTBY with invalid count
    const resp2 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "SORTBY", "abc" });
    defer allocator.free(resp2);
    try std.testing.expect(std.mem.startsWith(u8, resp2, "-ERR SORTBY count must be"));

    // SORTBY with odd count (must be even for field/order pairs)
    const resp3 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "SORTBY", "1", "@score" });
    defer allocator.free(resp3);
    try std.testing.expect(std.mem.startsWith(u8, resp3, "-ERR SORTBY count must be even"));

    // SORTBY with insufficient arguments
    const resp4 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "SORTBY", "4", "@score", "DESC" });
    defer allocator.free(resp4);
    try std.testing.expect(std.mem.startsWith(u8, resp4, "-ERR not enough SORTBY"));

    // SORTBY with invalid order
    const resp5 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "SORTBY", "2", "@score", "INVALID" });
    defer allocator.free(resp5);
    try std.testing.expect(std.mem.startsWith(u8, resp5, "-ERR SORTBY order must be"));

    // Valid SORTBY ASC
    const resp6 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "SORTBY", "2", "@score", "ASC" });
    defer allocator.free(resp6);
    try std.testing.expect(std.mem.startsWith(u8, resp6, "*1\r\n:0\r\n"));

    // Valid SORTBY DESC
    const resp7 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "SORTBY", "2", "@score", "DESC" });
    defer allocator.free(resp7);
    try std.testing.expect(std.mem.startsWith(u8, resp7, "*1\r\n:0\r\n"));

    // Multiple SORTBY fields
    const resp8 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "SORTBY", "4", "@score", "DESC", "@title", "ASC" });
    defer allocator.free(resp8);
    try std.testing.expect(std.mem.startsWith(u8, resp8, "*1\r\n:0\r\n"));
}

test "FT.AGGREGATE: LIMIT clause parsing" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    _ = try strings.handleCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{
        "idx",    "ON",   "HASH", "SCHEMA",
        "title",  "TEXT",
    });

    // LIMIT without arguments
    const resp1 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "LIMIT" });
    defer allocator.free(resp1);
    try std.testing.expect(std.mem.startsWith(u8, resp1, "-ERR LIMIT requires offset and count"));

    // LIMIT with only offset
    const resp2 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "LIMIT", "0" });
    defer allocator.free(resp2);
    try std.testing.expect(std.mem.startsWith(u8, resp2, "-ERR LIMIT requires offset and count"));

    // LIMIT with invalid offset
    const resp3 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "LIMIT", "abc", "10" });
    defer allocator.free(resp3);
    try std.testing.expect(std.mem.startsWith(u8, resp3, "-ERR LIMIT offset must be"));

    // LIMIT with invalid count
    const resp4 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "LIMIT", "0", "xyz" });
    defer allocator.free(resp4);
    try std.testing.expect(std.mem.startsWith(u8, resp4, "-ERR LIMIT count must be"));

    // Valid LIMIT clause
    const resp5 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "LIMIT", "0", "10" });
    defer allocator.free(resp5);
    try std.testing.expect(std.mem.startsWith(u8, resp5, "*1\r\n:0\r\n"));

    // LIMIT with offset
    const resp6 = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "LIMIT", "5", "20" });
    defer allocator.free(resp6);
    try std.testing.expect(std.mem.startsWith(u8, resp6, "*1\r\n:0\r\n"));
}

test "FT.AGGREGATE: unknown clause" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    _ = try strings.handleCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{
        "idx",    "ON",   "HASH", "SCHEMA",
        "field",  "TEXT",
    });

    const resp = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*", "UNKNOWN_CLAUSE" });
    defer allocator.free(resp);
    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR unknown clause"));
}

test "FT.AGGREGATE: full pipeline stub" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    _ = try strings.handleCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{
        "products", "ON",        "HASH",    "SCHEMA",
        "name",     "TEXT",
        "category", "TAG",
        "price",    "NUMERIC",
    });

    // Full aggregation pipeline (stub implementation returns empty results)
    const resp = try ftAggregate(&storage, allocator, &[_][]const u8{
        "products",
        "*",
        "LOAD",
        "2",
        "@name",
        "@price",
        "GROUPBY",
        "1",
        "@category",
        "REDUCE",
        "COUNT",
        "0",
        "AS",
        "cnt",
        "REDUCE",
        "SUM",
        "1",
        "@price",
        "AS",
        "total",
        "SORTBY",
        "2",
        "@total",
        "DESC",
        "LIMIT",
        "0",
        "10",
    });
    defer allocator.free(resp);

    // Stub returns [0] = 0 results
    try std.testing.expect(std.mem.startsWith(u8, resp, "*1\r\n:0\r\n"));
}

test "FT.AGGREGATE: response format for empty results" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    _ = try strings.handleCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{
        "idx",    "ON",   "HASH", "SCHEMA",
        "field",  "TEXT",
    });

    const resp = try ftAggregate(&storage, allocator, &[_][]const u8{ "idx", "*" });
    defer allocator.free(resp);

    // Response should be RESP array: [count, row1, row2, ...]
    // For stub: [0] (no rows)
    try std.testing.expect(std.mem.startsWith(u8, resp, "*1\r\n:0\r\n"));
}
