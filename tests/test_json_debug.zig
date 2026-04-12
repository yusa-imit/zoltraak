const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const Storage = @import("../src/storage/memory.zig").Storage;
const json_cmds = @import("../src/commands/json.zig");
const Writer = @import("../src/protocol/writer.zig").Writer;

const RespValue = protocol.RespValue;

fn deinitRespValue(value: *const RespValue, allocator: std.mem.Allocator) void {
    switch (value.*) {
        .array => |arr| {
            for (arr) |*item| {
                deinitRespValue(item, allocator);
            }
            allocator.free(arr);
        },
        .bulk_string => |s| allocator.free(s),
        else => {},
    }
}

// ============================================================================
// JSON.DEBUG MEMORY Tests
// ============================================================================

test "JSON.DEBUG MEMORY - basic string" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set a JSON string
    const args_set = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "\"hello\"" },
    };
    _ = try json_cmds.cmdJsonSet(&storage, &args_set, allocator);

    // Get memory usage
    const args_debug = [_]RespValue{
        RespValue{ .bulk_string = "JSON.DEBUG" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
    };
    const result = try json_cmds.cmdJsonDebugMemory(&storage, &args_debug, allocator);
    defer deinitRespValue(&result, allocator);

    // Write RESP and check format
    var w = Writer.init(allocator);
    defer w.deinit();
    const resp = try w.writeRespValue(result);
    defer allocator.free(resp);

    // Should return integer (e.g., ":64\r\n")
    try std.testing.expect(resp[0] == ':');
    try std.testing.expect(std.mem.indexOf(u8, resp, "\r\n") != null);
}

test "JSON.DEBUG MEMORY - object" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set a JSON object
    const args_set = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":1,\"b\":2}" },
    };
    _ = try json_cmds.cmdJsonSet(&storage, &args_set, allocator);

    // Get memory usage with default path
    const args_debug = [_]RespValue{
        RespValue{ .bulk_string = "JSON.DEBUG" },
        RespValue{ .bulk_string = "doc" },
    };
    const result = try json_cmds.cmdJsonDebugMemory(&storage, &args_debug, allocator);
    defer deinitRespValue(&result, allocator);

    var w = Writer.init(allocator);
    defer w.deinit();
    const resp = try w.writeRespValue(result);
    defer allocator.free(resp);

    try std.testing.expect(resp[0] == ':');
}

test "JSON.DEBUG MEMORY - array" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args_set = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "[1,2,3,4,5]" },
    };
    _ = try json_cmds.cmdJsonSet(&storage, &args_set, allocator);

    const args_debug = [_]RespValue{
        RespValue{ .bulk_string = "JSON.DEBUG" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
    };
    const result = try json_cmds.cmdJsonDebugMemory(&storage, &args_debug, allocator);
    defer deinitRespValue(&result, allocator);

    var w = Writer.init(allocator);
    defer w.deinit();
    const resp = try w.writeRespValue(result);
    defer allocator.free(resp);

    try std.testing.expect(resp[0] == ':');
}

test "JSON.DEBUG MEMORY - nested path" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args_set = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":{\"b\":\"value\"}}" },
    };
    _ = try json_cmds.cmdJsonSet(&storage, &args_set, allocator);

    const args_debug = [_]RespValue{
        RespValue{ .bulk_string = "JSON.DEBUG" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$.a.b" },
    };
    const result = try json_cmds.cmdJsonDebugMemory(&storage, &args_debug, allocator);
    defer deinitRespValue(&result, allocator);

    var w = Writer.init(allocator);
    defer w.deinit();
    const resp = try w.writeRespValue(result);
    defer allocator.free(resp);

    try std.testing.expect(resp[0] == ':');
}

test "JSON.DEBUG MEMORY - wildcard returns array" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args_set = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":1,\"b\":2,\"c\":3}" },
    };
    _ = try json_cmds.cmdJsonSet(&storage, &args_set, allocator);

    const args_debug = [_]RespValue{
        RespValue{ .bulk_string = "JSON.DEBUG" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$.*" },
    };
    const result = try json_cmds.cmdJsonDebugMemory(&storage, &args_debug, allocator);
    defer deinitRespValue(&result, allocator);

    var w = Writer.init(allocator);
    defer w.deinit();
    const resp = try w.writeRespValue(result);
    defer allocator.free(resp);

    // Should return array (starts with '*')
    try std.testing.expect(resp[0] == '*');
    try std.testing.expect(std.mem.indexOf(u8, resp, "*3\r\n") != null); // 3 elements
}

test "JSON.DEBUG MEMORY - non-existent key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args_debug = [_]RespValue{
        RespValue{ .bulk_string = "JSON.DEBUG" },
        RespValue{ .bulk_string = "nonexistent" },
    };
    const result = try json_cmds.cmdJsonDebugMemory(&storage, &args_debug, allocator);
    defer deinitRespValue(&result, allocator);

    var w = Writer.init(allocator);
    defer w.deinit();
    const resp = try w.writeRespValue(result);
    defer allocator.free(resp);

    // Should return null bulk string
    try std.testing.expect(std.mem.eql(u8, resp, "$-1\r\n"));
}

test "JSON.DEBUG MEMORY - wrong type" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set a non-JSON value
    const Value = @import("../src/storage/memory.zig").Value;
    try storage.data.put("key", Value{ .string = "not json" });

    const args_debug = [_]RespValue{
        RespValue{ .bulk_string = "JSON.DEBUG" },
        RespValue{ .bulk_string = "key" },
    };
    const result = try json_cmds.cmdJsonDebugMemory(&storage, &args_debug, allocator);
    defer deinitRespValue(&result, allocator);

    var w = Writer.init(allocator);
    defer w.deinit();
    const resp = try w.writeRespValue(result);
    defer allocator.free(resp);

    // Should return WRONGTYPE error
    try std.testing.expect(resp[0] == '-');
    try std.testing.expect(std.mem.indexOf(u8, resp, "WRONGTYPE") != null);
}

test "JSON.DEBUG MEMORY - wrong arity (too few)" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args_debug = [_]RespValue{
        RespValue{ .bulk_string = "JSON.DEBUG" },
    };
    const result = try json_cmds.cmdJsonDebugMemory(&storage, &args_debug, allocator);
    defer deinitRespValue(&result, allocator);

    var w = Writer.init(allocator);
    defer w.deinit();
    const resp = try w.writeRespValue(result);
    defer allocator.free(resp);

    try std.testing.expect(resp[0] == '-');
    try std.testing.expect(std.mem.indexOf(u8, resp, "wrong number of arguments") != null);
}

test "JSON.DEBUG MEMORY - invalid path" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args_set = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "\"test\"" },
    };
    _ = try json_cmds.cmdJsonSet(&storage, &args_set, allocator);

    const args_debug = [_]RespValue{
        RespValue{ .bulk_string = "JSON.DEBUG" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "invalid[[[path" },
    };
    const result = try json_cmds.cmdJsonDebugMemory(&storage, &args_debug, allocator);
    defer deinitRespValue(&result, allocator);

    var w = Writer.init(allocator);
    defer w.deinit();
    const resp = try w.writeRespValue(result);
    defer allocator.free(resp);

    try std.testing.expect(resp[0] == '-');
    try std.testing.expect(std.mem.indexOf(u8, resp, "invalid path") != null);
}

// ============================================================================
// JSON.DEBUG HELP Tests
// ============================================================================

test "JSON.DEBUG HELP - returns help array" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args_help = [_]RespValue{
        RespValue{ .bulk_string = "JSON.DEBUG" },
    };
    const result = try json_cmds.cmdJsonDebugHelp(&storage, &args_help, allocator);
    defer deinitRespValue(&result, allocator);

    var w = Writer.init(allocator);
    defer w.deinit();
    const resp = try w.writeRespValue(result);
    defer allocator.free(resp);

    // Should return array of bulk strings
    try std.testing.expect(resp[0] == '*');
    try std.testing.expect(std.mem.indexOf(u8, resp, "*2\r\n") != null); // 2 help messages
    try std.testing.expect(std.mem.indexOf(u8, resp, "HELP") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "MEMORY") != null);
}

test "JSON.DEBUG HELP - wrong arity" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args_help = [_]RespValue{
        RespValue{ .bulk_string = "JSON.DEBUG" },
        RespValue{ .bulk_string = "extra" },
    };
    const result = try json_cmds.cmdJsonDebugHelp(&storage, &args_help, allocator);
    defer deinitRespValue(&result, allocator);

    var w = Writer.init(allocator);
    defer w.deinit();
    const resp = try w.writeRespValue(result);
    defer allocator.free(resp);

    try std.testing.expect(resp[0] == '-');
    try std.testing.expect(std.mem.indexOf(u8, resp, "wrong number of arguments") != null);
}
