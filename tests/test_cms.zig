const std = @import("std");
const Server = @import("../src/server.zig").Server;
const Parser = @import("../src/protocol/parser.zig").Parser;
const Storage = @import("../src/storage/memory.zig").Storage;
const Config = @import("../src/storage/config.zig").Config;

// ============================================================================
// CMS.INITBYDIM Integration Tests
// ============================================================================

test "CMS.INITBYDIM: creates new CMS with valid dimensions" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    const cmd = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$3\r\n100\r\n$1\r\n5\r\n";
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const result = try parser.parse(cmd);
    defer parser.reset();

    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);

    // Verify CMS was created
    const value = storage.get("mysketch").?;
    try std.testing.expect(value.* == .count_min_sketch);
    try std.testing.expectEqual(@as(u32, 100), value.count_min_sketch.width);
    try std.testing.expectEqual(@as(u32, 5), value.count_min_sketch.depth);
}

test "CMS.INITBYDIM: replaces existing CMS" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create initial CMS
    const cmd1 = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$2\r\n50\r\n$1\r\n3\r\n";
    const result1 = try parser.parse(cmd1);
    defer parser.reset();
    const response1 = try server.handleCommand(result1.array);
    defer allocator.free(response1);

    // Replace with new dimensions
    const cmd2 = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$3\r\n200\r\n$1\r\n7\r\n";
    const result2 = try parser.parse(cmd2);
    const response2 = try server.handleCommand(result2.array);
    defer allocator.free(response2);

    try std.testing.expectEqualStrings("+OK\r\n", response2);

    const value = storage.get("mysketch").?;
    try std.testing.expectEqual(@as(u32, 200), value.count_min_sketch.width);
    try std.testing.expectEqual(@as(u32, 7), value.count_min_sketch.depth);
}

test "CMS.INITBYDIM: returns WRONGTYPE for non-CMS key" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    // Create a string value
    try storage.set("mykey", .{ .string = .{ .data = try allocator.dupe(u8, "hello"), .expires_at = null } });

    var parser = Parser.init(allocator);
    defer parser.deinit();

    const cmd = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$5\r\nmykey\r\n$3\r\n100\r\n$1\r\n5\r\n";
    const result = try parser.parse(cmd);
    defer parser.reset();

    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-WRONGTYPE"));
}

test "CMS.INITBYDIM: rejects zero width" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    const cmd = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$1\r\n0\r\n$1\r\n5\r\n";
    const result = try parser.parse(cmd);
    defer parser.reset();

    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "width") != null);
}

test "CMS.INITBYDIM: rejects zero depth" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    const cmd = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$3\r\n100\r\n$1\r\n0\r\n";
    const result = try parser.parse(cmd);
    defer parser.reset();

    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "depth") != null);
}

test "CMS.INITBYDIM: rejects invalid width format" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    const cmd = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$3\r\nabc\r\n$1\r\n5\r\n";
    const result = try parser.parse(cmd);
    defer parser.reset();

    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "width") != null);
}

test "CMS.INITBYDIM: rejects wrong arity" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Too few arguments
    const cmd1 = "*3\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$3\r\n100\r\n";
    const result1 = try parser.parse(cmd1);
    defer parser.reset();

    const response1 = try server.handleCommand(result1.array);
    defer allocator.free(response1);

    try std.testing.expect(std.mem.indexOf(u8, response1, "wrong number") != null);

    // Too many arguments
    const cmd2 = "*5\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$3\r\n100\r\n$1\r\n5\r\n$5\r\nextra\r\n";
    const result2 = try parser.parse(cmd2);
    const response2 = try server.handleCommand(result2.array);
    defer allocator.free(response2);

    try std.testing.expect(std.mem.indexOf(u8, response2, "wrong number") != null);
}

// ============================================================================
// CMS.INITBYPROB Integration Tests
// ============================================================================

test "CMS.INITBYPROB: creates CMS with error bounds" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    const cmd = "*4\r\n$14\r\nCMS.INITBYPROB\r\n$8\r\nmysketch\r\n$4\r\n0.01\r\n$4\r\n0.01\r\n";
    const result = try parser.parse(cmd);
    defer parser.reset();

    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);

    const value = storage.get("mysketch").?;
    try std.testing.expect(value.* == .count_min_sketch);

    // width = ceil(e / 0.01) = 272
    try std.testing.expectEqual(@as(u32, 272), value.count_min_sketch.width);

    // depth = ceil(ln(100)) = 5
    try std.testing.expectEqual(@as(u32, 5), value.count_min_sketch.depth);
}

test "CMS.INITBYPROB: rejects invalid error_rate zero" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    const cmd = "*4\r\n$14\r\nCMS.INITBYPROB\r\n$8\r\nmysketch\r\n$3\r\n0.0\r\n$4\r\n0.01\r\n";
    const result = try parser.parse(cmd);
    defer parser.reset();

    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "error_rate") != null);
}

test "CMS.INITBYPROB: rejects invalid error_rate one" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    const cmd = "*4\r\n$14\r\nCMS.INITBYPROB\r\n$8\r\nmysketch\r\n$3\r\n1.0\r\n$4\r\n0.01\r\n";
    const result = try parser.parse(cmd);
    defer parser.reset();

    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "error_rate") != null);
}

test "CMS.INITBYPROB: rejects invalid probability zero" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    const cmd = "*4\r\n$14\r\nCMS.INITBYPROB\r\n$8\r\nmysketch\r\n$4\r\n0.01\r\n$3\r\n0.0\r\n";
    const result = try parser.parse(cmd);
    defer parser.reset();

    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "probability") != null);
}

test "CMS.INITBYPROB: rejects invalid probability one" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    const cmd = "*4\r\n$14\r\nCMS.INITBYPROB\r\n$8\r\nmysketch\r\n$4\r\n0.01\r\n$3\r\n1.0\r\n";
    const result = try parser.parse(cmd);
    defer parser.reset();

    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "probability") != null);
}

test "CMS.INITBYPROB: rejects invalid float format" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    const cmd = "*4\r\n$14\r\nCMS.INITBYPROB\r\n$8\r\nmysketch\r\n$3\r\nabc\r\n$4\r\n0.01\r\n";
    const result = try parser.parse(cmd);
    defer parser.reset();

    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "error_rate") != null);
}

test "CMS.INITBYPROB: calculates dimensions correctly for different params" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Error rate 0.001, probability 0.001 → larger sketch
    const cmd = "*4\r\n$14\r\nCMS.INITBYPROB\r\n$8\r\nmysketch\r\n$5\r\n0.001\r\n$5\r\n0.001\r\n";
    const result = try parser.parse(cmd);
    defer parser.reset();

    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);

    const value = storage.get("mysketch").?;

    // width = ceil(e / 0.001) = 2719
    try std.testing.expectEqual(@as(u32, 2719), value.count_min_sketch.width);

    // depth = ceil(ln(1000)) = 7
    try std.testing.expectEqual(@as(u32, 7), value.count_min_sketch.depth);
}
