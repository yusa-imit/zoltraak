const std = @import("std");
const testing = std.testing;
const protocol = @import("../src/protocol/parser.zig");
const writer_mod = @import("../src/protocol/writer.zig");
const hotkeys_cmds = @import("../src/commands/hotkeys.zig");
const Storage = @import("../src/storage/memory.zig").Storage;

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;

test "HOTKEYS HELP" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &[_]RespValue{});
    defer allocator.free(result);

    // Should return array with 10 help lines
    try testing.expect(std.mem.startsWith(u8, result, "*10\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "START METRICS count") != null);
    try testing.expect(std.mem.indexOf(u8, result, "STOP") != null);
    try testing.expect(std.mem.indexOf(u8, result, "GET") != null);
    try testing.expect(std.mem.indexOf(u8, result, "RESET") != null);
}

test "HOTKEYS HELP via explicit subcommand" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "HELP" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "*10\r\n"));
}

test "HOTKEYS START with CPU metric" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "CPU" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
}

test "HOTKEYS START with NET metric" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "NET" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
}

test "HOTKEYS START with both CPU and NET metrics" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "CPU" },
        RespValue{ .bulk_string = "NET" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
}

test "HOTKEYS START with COUNT parameter" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "CPU" },
        RespValue{ .bulk_string = "COUNT" },
        RespValue{ .bulk_string = "10" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
}

test "HOTKEYS START with DURATION parameter" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "CPU" },
        RespValue{ .bulk_string = "DURATION" },
        RespValue{ .bulk_string = "60" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
}

test "HOTKEYS START with SAMPLE parameter" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "CPU" },
        RespValue{ .bulk_string = "SAMPLE" },
        RespValue{ .bulk_string = "100" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
}

test "HOTKEYS START with all parameters" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "CPU" },
        RespValue{ .bulk_string = "NET" },
        RespValue{ .bulk_string = "COUNT" },
        RespValue{ .bulk_string = "10" },
        RespValue{ .bulk_string = "DURATION" },
        RespValue{ .bulk_string = "60" },
        RespValue{ .bulk_string = "SAMPLE" },
        RespValue{ .bulk_string = "100" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
}

test "HOTKEYS START error: missing METRICS keyword" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "CPU" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "syntax error") != null);
}

test "HOTKEYS START error: no CPU or NET specified" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "2" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "at least one of CPU or NET") != null);
}

test "HOTKEYS START error: zero count" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "CPU" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "count must be positive") != null);
}

test "HOTKEYS START error: invalid count" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "not-a-number" },
        RespValue{ .bulk_string = "CPU" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "not an integer") != null);
}

test "HOTKEYS START error: SLOTS in non-cluster mode" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "CPU" },
        RespValue{ .bulk_string = "SLOTS" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "0" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "cluster support disabled") != null);
}

test "HOTKEYS START case insensitive" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "start" },
        RespValue{ .bulk_string = "metrics" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "cpu" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
}

test "HOTKEYS STOP" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "STOP" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
}

test "HOTKEYS STOP with wrong arity" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "STOP" },
        RespValue{ .bulk_string = "extra" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

test "HOTKEYS GET returns null when no tracking" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GET" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    // Should return null bulk string when no tracking data
    try testing.expect(std.mem.eql(u8, result, "$-1\r\n"));
}

test "HOTKEYS GET with wrong arity" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "extra" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

test "HOTKEYS RESET" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "RESET" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
}

test "HOTKEYS RESET with wrong arity" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "RESET" },
        RespValue{ .bulk_string = "extra" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

test "HOTKEYS unknown subcommand" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "UNKNOWN" },
    };

    const result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "unknown subcommand") != null);
}

test "HOTKEYS workflow: START -> STOP -> GET -> RESET" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // START
    const start_args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "CPU" },
        RespValue{ .bulk_string = "NET" },
    };
    const start_result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &start_args);
    defer allocator.free(start_result);
    try testing.expect(std.mem.eql(u8, start_result, "+OK\r\n"));

    // STOP
    const stop_args = [_]RespValue{
        RespValue{ .bulk_string = "STOP" },
    };
    const stop_result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &stop_args);
    defer allocator.free(stop_result);
    try testing.expect(std.mem.eql(u8, stop_result, "+OK\r\n"));

    // GET
    const get_args = [_]RespValue{
        RespValue{ .bulk_string = "GET" },
    };
    const get_result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &get_args);
    defer allocator.free(get_result);
    try testing.expect(std.mem.eql(u8, get_result, "$-1\r\n"));

    // RESET
    const reset_args = [_]RespValue{
        RespValue{ .bulk_string = "RESET" },
    };
    const reset_result = try hotkeys_cmds.cmdHotkeys(allocator, &storage, &reset_args);
    defer allocator.free(reset_result);
    try testing.expect(std.mem.eql(u8, reset_result, "+OK\r\n"));
}
