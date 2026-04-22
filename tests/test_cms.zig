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

// ============================================================================
// CMS.INCRBY Integration Tests
// ============================================================================

test "CMS.INCRBY: increments single item" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create CMS
    const init_cmd = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$3\r\n100\r\n$1\r\n5\r\n";
    const init_result = try parser.parse(init_cmd);
    defer parser.reset();
    const init_response = try server.handleCommand(init_result.array);
    defer allocator.free(init_response);

    // Increment item
    const cmd = "*4\r\n$11\r\nCMS.INCRBY\r\n$8\r\nmysketch\r\n$5\r\napple\r\n$1\r\n5\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    // Expected: *1\r\n:5\r\n
    try std.testing.expect(std.mem.startsWith(u8, response, "*1\r\n:5\r\n"));
}

test "CMS.INCRBY: increments multiple items" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create CMS
    const init_cmd = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$3\r\n100\r\n$1\r\n5\r\n";
    const init_result = try parser.parse(init_cmd);
    defer parser.reset();
    const init_response = try server.handleCommand(init_result.array);
    defer allocator.free(init_response);

    // Increment multiple items
    const cmd = "*8\r\n$11\r\nCMS.INCRBY\r\n$8\r\nmysketch\r\n$5\r\napple\r\n$2\r\n10\r\n$6\r\nbanana\r\n$2\r\n20\r\n$6\r\ncherry\r\n$2\r\n30\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    // Expected: *3\r\n:10\r\n:20\r\n:30\r\n
    try std.testing.expect(std.mem.startsWith(u8, response, "*3\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, response, ":10\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, ":20\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, ":30\r\n") != null);
}

test "CMS.INCRBY: accumulates increments" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create CMS
    const init_cmd = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$3\r\n100\r\n$1\r\n5\r\n";
    const init_result = try parser.parse(init_cmd);
    defer parser.reset();
    const init_response = try server.handleCommand(init_result.array);
    defer allocator.free(init_response);

    // First increment
    const cmd1 = "*4\r\n$11\r\nCMS.INCRBY\r\n$8\r\nmysketch\r\n$5\r\napple\r\n$1\r\n5\r\n";
    const result1 = try parser.parse(cmd1);
    const response1 = try server.handleCommand(result1.array);
    defer allocator.free(response1);

    // Second increment
    const cmd2 = "*4\r\n$11\r\nCMS.INCRBY\r\n$8\r\nmysketch\r\n$5\r\napple\r\n$1\r\n3\r\n";
    const result2 = try parser.parse(cmd2);
    const response2 = try server.handleCommand(result2.array);
    defer allocator.free(response2);

    // Should accumulate: 5 + 3 = 8
    try std.testing.expect(std.mem.indexOf(u8, response2, ":8\r\n") != null);
}

test "CMS.INCRBY: handles negative increments" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create CMS
    const init_cmd = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$3\r\n100\r\n$1\r\n5\r\n";
    const init_result = try parser.parse(init_cmd);
    defer parser.reset();
    const init_response = try server.handleCommand(init_result.array);
    defer allocator.free(init_response);

    // Increment
    const cmd1 = "*4\r\n$11\r\nCMS.INCRBY\r\n$8\r\nmysketch\r\n$5\r\napple\r\n$2\r\n10\r\n";
    const result1 = try parser.parse(cmd1);
    const response1 = try server.handleCommand(result1.array);
    defer allocator.free(response1);

    // Decrement
    const cmd2 = "*4\r\n$11\r\nCMS.INCRBY\r\n$8\r\nmysketch\r\n$5\r\napple\r\n$2\r\n-3\r\n";
    const result2 = try parser.parse(cmd2);
    const response2 = try server.handleCommand(result2.array);
    defer allocator.free(response2);

    // Should be: 10 - 3 = 7
    try std.testing.expect(std.mem.indexOf(u8, response2, ":7\r\n") != null);
}

test "CMS.INCRBY: returns error for nonexistent key" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    const cmd = "*4\r\n$11\r\nCMS.INCRBY\r\n$11\r\nnonexistent\r\n$5\r\napple\r\n$1\r\n5\r\n";
    const result = try parser.parse(cmd);
    defer parser.reset();

    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "not found") != null);
}

test "CMS.INCRBY: returns WRONGTYPE for non-CMS key" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    // Create string value
    try storage.set("mykey", .{ .string = .{ .data = try allocator.dupe(u8, "hello"), .expires_at = null } });

    var parser = Parser.init(allocator);
    defer parser.deinit();

    const cmd = "*4\r\n$11\r\nCMS.INCRBY\r\n$5\r\nmykey\r\n$5\r\napple\r\n$1\r\n5\r\n";
    const result = try parser.parse(cmd);
    defer parser.reset();

    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-WRONGTYPE"));
}

test "CMS.INCRBY: rejects invalid increment format" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create CMS
    const init_cmd = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$3\r\n100\r\n$1\r\n5\r\n";
    const init_result = try parser.parse(init_cmd);
    defer parser.reset();
    const init_response = try server.handleCommand(init_result.array);
    defer allocator.free(init_response);

    const cmd = "*4\r\n$11\r\nCMS.INCRBY\r\n$8\r\nmysketch\r\n$5\r\napple\r\n$3\r\nabc\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "increment") != null);
}

test "CMS.INCRBY: rejects odd number of item-increment pairs" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create CMS
    const init_cmd = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$3\r\n100\r\n$1\r\n5\r\n";
    const init_result = try parser.parse(init_cmd);
    defer parser.reset();
    const init_response = try server.handleCommand(init_result.array);
    defer allocator.free(init_response);

    // Missing increment for second item
    const cmd = "*5\r\n$11\r\nCMS.INCRBY\r\n$8\r\nmysketch\r\n$5\r\napple\r\n$1\r\n5\r\n$6\r\nbanana\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "pairs") != null);
}

// ============================================================================
// CMS.QUERY Integration Tests
// ============================================================================

test "CMS.QUERY: queries single item" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create CMS
    const init_cmd = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$3\r\n100\r\n$1\r\n5\r\n";
    const init_result = try parser.parse(init_cmd);
    defer parser.reset();
    const init_response = try server.handleCommand(init_result.array);
    defer allocator.free(init_response);

    // Increment item
    const incr_cmd = "*4\r\n$11\r\nCMS.INCRBY\r\n$8\r\nmysketch\r\n$5\r\napple\r\n$2\r\n42\r\n";
    const incr_result = try parser.parse(incr_cmd);
    const incr_response = try server.handleCommand(incr_result.array);
    defer allocator.free(incr_response);

    // Query item
    const cmd = "*3\r\n$9\r\nCMS.QUERY\r\n$8\r\nmysketch\r\n$5\r\napple\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    // Expected: *1\r\n:42\r\n
    try std.testing.expect(std.mem.startsWith(u8, response, "*1\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, response, ":42\r\n") != null);
}

test "CMS.QUERY: queries multiple items" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create CMS with larger dimensions for accuracy
    const init_cmd = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$4\r\n1000\r\n$1\r\n7\r\n";
    const init_result = try parser.parse(init_cmd);
    defer parser.reset();
    const init_response = try server.handleCommand(init_result.array);
    defer allocator.free(init_response);

    // Increment multiple items
    const incr_cmd = "*8\r\n$11\r\nCMS.INCRBY\r\n$8\r\nmysketch\r\n$5\r\napple\r\n$2\r\n10\r\n$6\r\nbanana\r\n$2\r\n20\r\n$6\r\ncherry\r\n$2\r\n30\r\n";
    const incr_result = try parser.parse(incr_cmd);
    const incr_response = try server.handleCommand(incr_result.array);
    defer allocator.free(incr_response);

    // Query multiple items
    const cmd = "*5\r\n$9\r\nCMS.QUERY\r\n$8\r\nmysketch\r\n$5\r\napple\r\n$6\r\nbanana\r\n$6\r\ncherry\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    // Expected: *3\r\n:10\r\n:20\r\n:30\r\n (or higher due to collisions)
    try std.testing.expect(std.mem.startsWith(u8, response, "*3\r\n"));
}

test "CMS.QUERY: returns zero for nonexistent items" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create CMS
    const init_cmd = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$3\r\n100\r\n$1\r\n5\r\n";
    const init_result = try parser.parse(init_cmd);
    defer parser.reset();
    const init_response = try server.handleCommand(init_result.array);
    defer allocator.free(init_response);

    // Query nonexistent item
    const cmd = "*3\r\n$9\r\nCMS.QUERY\r\n$8\r\nmysketch\r\n$11\r\nnonexistent\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    // Expected: *1\r\n:0\r\n
    try std.testing.expect(std.mem.indexOf(u8, response, ":0\r\n") != null);
}

test "CMS.QUERY: returns error for nonexistent key" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    const cmd = "*3\r\n$9\r\nCMS.QUERY\r\n$11\r\nnonexistent\r\n$5\r\napple\r\n";
    const result = try parser.parse(cmd);
    defer parser.reset();

    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "not found") != null);
}

test "CMS.QUERY: returns WRONGTYPE for non-CMS key" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    // Create string value
    try storage.set("mykey", .{ .string = .{ .data = try allocator.dupe(u8, "hello"), .expires_at = null } });

    var parser = Parser.init(allocator);
    defer parser.deinit();

    const cmd = "*3\r\n$9\r\nCMS.QUERY\r\n$5\r\nmykey\r\n$5\r\napple\r\n";
    const result = try parser.parse(cmd);
    defer parser.reset();

    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-WRONGTYPE"));
}

test "CMS.QUERY: requires at least one item" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create CMS
    const init_cmd = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$3\r\n100\r\n$1\r\n5\r\n";
    const init_result = try parser.parse(init_cmd);
    defer parser.reset();
    const init_response = try server.handleCommand(init_result.array);
    defer allocator.free(init_response);

    // Query with no items
    const cmd = "*2\r\n$9\r\nCMS.QUERY\r\n$8\r\nmysketch\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "wrong number") != null);
}

// ============================================================================
// CMS.MERGE Tests
// ============================================================================

test "CMS.MERGE: merges two sketches" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create dest sketch
    const init1 = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$4\r\ndest\r\n$3\r\n100\r\n$1\r\n5\r\n";
    const r1 = try parser.parse(init1);
    defer parser.reset();
    const resp1 = try server.handleCommand(r1.array);
    defer allocator.free(resp1);

    // Create src sketch
    const init2 = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$3\r\nsrc\r\n$3\r\n100\r\n$1\r\n5\r\n";
    const r2 = try parser.parse(init2);
    const resp2 = try server.handleCommand(r2.array);
    defer allocator.free(resp2);

    // Increment dest
    const incr1 = "*4\r\n$10\r\nCMS.INCRBY\r\n$4\r\ndest\r\n$5\r\napple\r\n$2\r\n10\r\n";
    const r3 = try parser.parse(incr1);
    const resp3 = try server.handleCommand(r3.array);
    defer allocator.free(resp3);

    // Increment src
    const incr2 = "*4\r\n$10\r\nCMS.INCRBY\r\n$3\r\nsrc\r\n$5\r\napple\r\n$2\r\n20\r\n";
    const r4 = try parser.parse(incr2);
    const resp4 = try server.handleCommand(r4.array);
    defer allocator.free(resp4);

    // Merge src into dest
    const cmd = "*3\r\n$9\r\nCMS.MERGE\r\n$4\r\ndest\r\n$3\r\nsrc\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    // Expected: +OK\r\n
    try std.testing.expect(std.mem.eql(u8, response, "+OK\r\n"));

    // Query dest - should be 10 + 20 = 30
    const query = "*3\r\n$9\r\nCMS.QUERY\r\n$4\r\ndest\r\n$5\r\napple\r\n";
    const r5 = try parser.parse(query);
    const resp5 = try server.handleCommand(r5.array);
    defer allocator.free(resp5);

    try std.testing.expect(std.mem.indexOf(u8, resp5, ":30\r\n") != null);
}

test "CMS.MERGE: merges multiple sources" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create 4 sketches
    const sketches = [_][]const u8{ "dest", "src1", "src2", "src3" };
    for (sketches) |sketch| {
        var buf: [256]u8 = undefined;
        const init_cmd = try std.fmt.bufPrint(&buf, "*4\r\n$13\r\nCMS.INITBYDIM\r\n${d}\r\n{s}\r\n$3\r\n100\r\n$1\r\n5\r\n", .{ sketch.len, sketch });
        const r = try parser.parse(init_cmd);
        defer parser.reset();
        const resp = try server.handleCommand(r.array);
        defer allocator.free(resp);
    }

    // Increment each sketch
    const values = [_]u8{ 5, 10, 15, 20 };
    for (sketches, values) |sketch, val| {
        var buf: [256]u8 = undefined;
        const val_str = try std.fmt.bufPrint(&buf, "{d}", .{val});
        var cmd_buf: [512]u8 = undefined;
        const incr_cmd = try std.fmt.bufPrint(&cmd_buf, "*4\r\n$10\r\nCMS.INCRBY\r\n${d}\r\n{s}\r\n$5\r\napple\r\n${d}\r\n{s}\r\n", .{ sketch.len, sketch, val_str.len, val_str });
        const r = try parser.parse(incr_cmd);
        defer parser.reset();
        const resp = try server.handleCommand(r.array);
        defer allocator.free(resp);
    }

    // Merge src1, src2, src3 into dest
    const cmd = "*5\r\n$9\r\nCMS.MERGE\r\n$4\r\ndest\r\n$4\r\nsrc1\r\n$4\r\nsrc2\r\n$4\r\nsrc3\r\n";
    const result = try parser.parse(cmd);
    defer parser.reset();
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.eql(u8, response, "+OK\r\n"));

    // Query dest - should be 5 + 10 + 15 + 20 = 50
    const query = "*3\r\n$9\r\nCMS.QUERY\r\n$4\r\ndest\r\n$5\r\napple\r\n";
    const r = try parser.parse(query);
    const resp = try server.handleCommand(r.array);
    defer allocator.free(resp);

    try std.testing.expect(std.mem.indexOf(u8, resp, ":50\r\n") != null);
}

test "CMS.MERGE: returns error if destination not found" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    const cmd = "*3\r\n$9\r\nCMS.MERGE\r\n$11\r\nnonexistent\r\n$3\r\nsrc\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "not found") != null);
}

test "CMS.MERGE: returns error if source not found" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create dest
    const init = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$4\r\ndest\r\n$3\r\n100\r\n$1\r\n5\r\n";
    const r = try parser.parse(init);
    defer parser.reset();
    const resp = try server.handleCommand(r.array);
    defer allocator.free(resp);

    const cmd = "*3\r\n$9\r\nCMS.MERGE\r\n$4\r\ndest\r\n$11\r\nnonexistent\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "not found") != null);
}

test "CMS.MERGE: returns WRONGTYPE if dest is not CMS" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create string key
    const set = "*3\r\n$3\r\nSET\r\n$4\r\ndest\r\n$5\r\nhello\r\n";
    const r = try parser.parse(set);
    defer parser.reset();
    const resp = try server.handleCommand(r.array);
    defer allocator.free(resp);

    const cmd = "*3\r\n$9\r\nCMS.MERGE\r\n$4\r\ndest\r\n$3\r\nsrc\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-WRONGTYPE"));
}

test "CMS.MERGE: returns error for dimension mismatch" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create dest with 100x5
    const init1 = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$4\r\ndest\r\n$3\r\n100\r\n$1\r\n5\r\n";
    const r1 = try parser.parse(init1);
    defer parser.reset();
    const resp1 = try server.handleCommand(r1.array);
    defer allocator.free(resp1);

    // Create src with 200x5 (different width)
    const init2 = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$3\r\nsrc\r\n$3\r\n200\r\n$1\r\n5\r\n";
    const r2 = try parser.parse(init2);
    const resp2 = try server.handleCommand(r2.array);
    defer allocator.free(resp2);

    const cmd = "*3\r\n$9\r\nCMS.MERGE\r\n$4\r\ndest\r\n$3\r\nsrc\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "dimension mismatch") != null);
}

test "CMS.MERGE: requires at least one source" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    const cmd = "*2\r\n$9\r\nCMS.MERGE\r\n$4\r\ndest\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "wrong number") != null);
}

// ============================================================================
// CMS.INFO Tests
// ============================================================================

test "CMS.INFO: returns metadata for CMS" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create CMS
    const init = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$3\r\n100\r\n$1\r\n5\r\n";
    const r1 = try parser.parse(init);
    defer parser.reset();
    const resp1 = try server.handleCommand(r1.array);
    defer allocator.free(resp1);

    const cmd = "*2\r\n$8\r\nCMS.INFO\r\n$8\r\nmysketch\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    // Expected: *6\r\n$5\r\nwidth\r\n:100\r\n$5\r\ndepth\r\n:5\r\n$5\r\ncount\r\n:0\r\n
    try std.testing.expect(std.mem.startsWith(u8, response, "*6\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, response, "$5\r\nwidth\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, ":100\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "$5\r\ndepth\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, ":5\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "$5\r\ncount\r\n") != null);
}

test "CMS.INFO: returns count after increments" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create CMS
    const init = "*4\r\n$13\r\nCMS.INITBYDIM\r\n$8\r\nmysketch\r\n$3\r\n100\r\n$1\r\n5\r\n";
    const r1 = try parser.parse(init);
    defer parser.reset();
    const resp1 = try server.handleCommand(r1.array);
    defer allocator.free(resp1);

    // Increment item
    const incr = "*4\r\n$10\r\nCMS.INCRBY\r\n$8\r\nmysketch\r\n$5\r\napple\r\n$2\r\n10\r\n";
    const r2 = try parser.parse(incr);
    const resp2 = try server.handleCommand(r2.array);
    defer allocator.free(resp2);

    const cmd = "*2\r\n$8\r\nCMS.INFO\r\n$8\r\nmysketch\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    // Count should be 10 * 5 (depth) = 50
    try std.testing.expect(std.mem.indexOf(u8, response, ":50\r\n") != null);
}

test "CMS.INFO: returns error for nonexistent key" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    const cmd = "*2\r\n$8\r\nCMS.INFO\r\n$11\r\nnonexistent\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "not found") != null);
}

test "CMS.INFO: returns WRONGTYPE for non-CMS key" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Create string key
    const set = "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nhello\r\n";
    const r = try parser.parse(set);
    defer parser.reset();
    const resp = try server.handleCommand(r.array);
    defer allocator.free(resp);

    const cmd = "*2\r\n$8\r\nCMS.INFO\r\n$5\r\nmykey\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-WRONGTYPE"));
}

test "CMS.INFO: requires exactly one argument" {
    const allocator = std.testing.allocator;

    var config = Config.default();
    var storage = try Storage.init(allocator, &config, null);
    defer storage.deinit();

    var server = try Server.init(allocator, &config, @constCast(&[_]Storage{storage}), 1);
    defer server.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    const cmd = "*1\r\n$8\r\nCMS.INFO\r\n";
    const result = try parser.parse(cmd);
    const response = try server.handleCommand(result.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "wrong number") != null);
}
