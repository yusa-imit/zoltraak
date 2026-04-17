const std = @import("std");
const Storage = @import("../src/storage/memory.zig").Storage;
const search_cmds = @import("../src/commands/search.zig");
const RespValue = @import("../src/protocol/writer.zig").RespValue;

test "FT.CONFIG GET: default timeout" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "GET", "TIMEOUT" };
    const result = try search_cmds.cmdFtConfig(&storage, allocator, &args);
    defer result.deinit(allocator);

    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 2), result.array.len);
    try std.testing.expect(result.array[0] == .bulk_string);
    try std.testing.expect(std.mem.eql(u8, result.array[0].bulk_string, "TIMEOUT"));
    try std.testing.expect(result.array[1] == .bulk_string);
    try std.testing.expect(std.mem.eql(u8, result.array[1].bulk_string, "500"));
}

test "FT.CONFIG GET: on_timeout default" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "GET", "ON_TIMEOUT" };
    const result = try search_cmds.cmdFtConfig(&storage, allocator, &args);
    defer result.deinit(allocator);

    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 2), result.array.len);
    try std.testing.expect(std.mem.eql(u8, result.array[1].bulk_string, "return"));
}

test "FT.CONFIG GET: case insensitive option" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "GET", "timeout" };
    const result = try search_cmds.cmdFtConfig(&storage, allocator, &args);
    defer result.deinit(allocator);

    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 2), result.array.len);
}

test "FT.CONFIG GET: unknown option" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "GET", "NOSUCHOPTION" };
    const result = try search_cmds.cmdFtConfig(&storage, allocator, &args);
    defer result.deinit(allocator);

    try std.testing.expect(result == .error_string);
    try std.testing.expect(std.mem.eql(u8, result.error_string, "ERR Unknown option"));
}

test "FT.CONFIG SET: timeout" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const set_args = [_][]const u8{ "SET", "TIMEOUT", "1000" };
    const set_result = try search_cmds.cmdFtConfig(&storage, allocator, &set_args);
    defer set_result.deinit(allocator);
    try std.testing.expect(set_result == .simple_string);
    try std.testing.expect(std.mem.eql(u8, set_result.simple_string, "OK"));

    const get_args = [_][]const u8{ "GET", "TIMEOUT" };
    const get_result = try search_cmds.cmdFtConfig(&storage, allocator, &get_args);
    defer get_result.deinit(allocator);
    try std.testing.expect(std.mem.eql(u8, get_result.array[1].bulk_string, "1000"));
}

test "FT.CONFIG SET: on_timeout valid values" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set to "fail"
    const set_fail_args = [_][]const u8{ "SET", "ON_TIMEOUT", "fail" };
    const set_fail_result = try search_cmds.cmdFtConfig(&storage, allocator, &set_fail_args);
    defer set_fail_result.deinit(allocator);
    try std.testing.expect(set_fail_result == .simple_string);

    const get_args = [_][]const u8{ "GET", "ON_TIMEOUT" };
    const get_result = try search_cmds.cmdFtConfig(&storage, allocator, &get_args);
    defer get_result.deinit(allocator);
    try std.testing.expect(std.mem.eql(u8, get_result.array[1].bulk_string, "fail"));

    // Set to "return"
    const set_return_args = [_][]const u8{ "SET", "ON_TIMEOUT", "RETURN" };
    const set_return_result = try search_cmds.cmdFtConfig(&storage, allocator, &set_return_args);
    defer set_return_result.deinit(allocator);
    try std.testing.expect(set_return_result == .simple_string);
}

test "FT.CONFIG SET: on_timeout invalid value" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "SET", "ON_TIMEOUT", "invalid" };
    const result = try search_cmds.cmdFtConfig(&storage, allocator, &args);
    defer result.deinit(allocator);

    try std.testing.expect(result == .error_string);
    try std.testing.expect(std.mem.eql(u8, result.error_string, "ERR Invalid value"));
}

test "FT.CONFIG SET: maxexpansions" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const set_args = [_][]const u8{ "SET", "MAXEXPANSIONS", "500" };
    const set_result = try search_cmds.cmdFtConfig(&storage, allocator, &set_args);
    defer set_result.deinit(allocator);
    try std.testing.expect(set_result == .simple_string);

    const get_args = [_][]const u8{ "GET", "MAXEXPANSIONS" };
    const get_result = try search_cmds.cmdFtConfig(&storage, allocator, &get_args);
    defer get_result.deinit(allocator);
    try std.testing.expect(std.mem.eql(u8, get_result.array[1].bulk_string, "500"));
}

test "FT.CONFIG SET: invalid number format" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "SET", "TIMEOUT", "notanumber" };
    const result = try search_cmds.cmdFtConfig(&storage, allocator, &args);
    defer result.deinit(allocator);

    try std.testing.expect(result == .error_string);
    try std.testing.expect(std.mem.eql(u8, result.error_string, "ERR Invalid value format"));
}

test "FT.CONFIG HELP: returns help text" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{"HELP"};
    const result = try search_cmds.cmdFtConfig(&storage, allocator, &args);
    defer result.deinit(allocator);

    try std.testing.expect(result == .array);
    try std.testing.expect(result.array.len == 9); // 9 help lines
    try std.testing.expect(result.array[0] == .bulk_string);
}

test "FT.CONFIG: unknown subcommand" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{"UNKNOWN"};
    const result = try search_cmds.cmdFtConfig(&storage, allocator, &args);
    defer result.deinit(allocator);

    try std.testing.expect(result == .error_string);
    try std.testing.expect(std.mem.eql(u8, result.error_string, "ERR unknown FT.CONFIG subcommand"));
}

test "FT.CONFIG GET: arity error" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "GET" };
    const result = try search_cmds.cmdFtConfig(&storage, allocator, &args);
    defer result.deinit(allocator);

    try std.testing.expect(result == .error_string);
}

test "FT.CONFIG SET: arity error" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "SET", "TIMEOUT" };
    const result = try search_cmds.cmdFtConfig(&storage, allocator, &args);
    defer result.deinit(allocator);

    try std.testing.expect(result == .error_string);
}
