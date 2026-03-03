const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const Server = @import("../src/server.zig").Server;
const Config = @import("../src/server.zig").Config;

const Parser = protocol.Parser;
const RespValue = protocol.RespValue;

/// Helper to parse a RESP response into a RespValue
fn parseResp(allocator: std.mem.Allocator, data: []const u8) !RespValue {
    var parser = Parser.init(allocator);
    defer parser.deinit();
    return try parser.parse(data);
}

test "PSUBSCRIBE - single pattern subscription" {
    const allocator = std.testing.allocator;
    const server = try Server.init(allocator, .{});
    defer server.deinit();

    const commands = @import("../src/commands/strings.zig");

    // PSUBSCRIBE news*
    const args1 = [_]RespValue{
        .{ .bulk_string = "PSUBSCRIBE" },
        .{ .bulk_string = "news*" },
    };
    const resp1 = try commands.executeCommand(
        allocator,
        server.storage,
        null,
        &server.pubsub,
        null,
        null,
        null,
        &server.client_registry,
        1,
        .resp2,
        &server.script_store,
        .{ .array = &args1 },
        false,
    );
    defer allocator.free(resp1);

    // Should return psubscribe confirmation frame
    try std.testing.expect(std.mem.indexOf(u8, resp1, "psubscribe") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp1, "news*") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp1, ":1\r\n") != null);
}

test "PSUBSCRIBE - multiple patterns" {
    const allocator = std.testing.allocator;
    const server = try Server.init(allocator, .{});
    defer server.deinit();

    const commands = @import("../src/commands/strings.zig");

    // PSUBSCRIBE news* sports?
    const args1 = [_]RespValue{
        .{ .bulk_string = "PSUBSCRIBE" },
        .{ .bulk_string = "news*" },
        .{ .bulk_string = "sports?" },
    };
    const resp1 = try commands.executeCommand(
        allocator,
        server.storage,
        null,
        &server.pubsub,
        null,
        null,
        null,
        &server.client_registry,
        1,
        .resp2,
        &server.script_store,
        .{ .array = &args1 },
        false,
    );
    defer allocator.free(resp1);

    // Should return two psubscribe frames (count 1 and 2)
    try std.testing.expect(std.mem.indexOf(u8, resp1, ":1\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp1, ":2\r\n") != null);
}

test "PUNSUBSCRIBE - single pattern" {
    const allocator = std.testing.allocator;
    const server = try Server.init(allocator, .{});
    defer server.deinit();

    const commands = @import("../src/commands/strings.zig");

    // First subscribe
    const args1 = [_]RespValue{
        .{ .bulk_string = "PSUBSCRIBE" },
        .{ .bulk_string = "news*" },
    };
    const resp1 = try commands.executeCommand(
        allocator,
        server.storage,
        null,
        &server.pubsub,
        null,
        null,
        null,
        &server.client_registry,
        1,
        .resp2,
        &server.script_store,
        .{ .array = &args1 },
        false,
    );
    allocator.free(resp1);

    // Now unsubscribe
    const args2 = [_]RespValue{
        .{ .bulk_string = "PUNSUBSCRIBE" },
        .{ .bulk_string = "news*" },
    };
    const resp2 = try commands.executeCommand(
        allocator,
        server.storage,
        null,
        &server.pubsub,
        null,
        null,
        null,
        &server.client_registry,
        1,
        .resp2,
        &server.script_store,
        .{ .array = &args2 },
        false,
    );
    defer allocator.free(resp2);

    try std.testing.expect(std.mem.indexOf(u8, resp2, "punsubscribe") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp2, ":0\r\n") != null);
}

test "PUNSUBSCRIBE - all patterns" {
    const allocator = std.testing.allocator;
    const server = try Server.init(allocator, .{});
    defer server.deinit();

    const commands = @import("../src/commands/strings.zig");

    // Subscribe to two patterns
    const args1 = [_]RespValue{
        .{ .bulk_string = "PSUBSCRIBE" },
        .{ .bulk_string = "news*" },
        .{ .bulk_string = "sports?" },
    };
    const resp1 = try commands.executeCommand(
        allocator,
        server.storage,
        null,
        &server.pubsub,
        null,
        null,
        null,
        &server.client_registry,
        1,
        .resp2,
        &server.script_store,
        .{ .array = &args1 },
        false,
    );
    allocator.free(resp1);

    // Unsubscribe from all (no args)
    const args2 = [_]RespValue{
        .{ .bulk_string = "PUNSUBSCRIBE" },
    };
    const resp2 = try commands.executeCommand(
        allocator,
        server.storage,
        null,
        &server.pubsub,
        null,
        null,
        null,
        &server.client_registry,
        1,
        .resp2,
        &server.script_store,
        .{ .array = &args2 },
        false,
    );
    defer allocator.free(resp2);

    // Should contain two punsubscribe frames
    try std.testing.expect(std.mem.count(u8, resp2, "punsubscribe") == 2);
}

test "PUBLISH - delivers to pattern subscribers" {
    const allocator = std.testing.allocator;
    const server = try Server.init(allocator, .{});
    defer server.deinit();

    const commands = @import("../src/commands/strings.zig");

    // Subscribe to pattern
    const args1 = [_]RespValue{
        .{ .bulk_string = "PSUBSCRIBE" },
        .{ .bulk_string = "news*" },
    };
    const resp1 = try commands.executeCommand(
        allocator,
        server.storage,
        null,
        &server.pubsub,
        null,
        null,
        null,
        &server.client_registry,
        1,
        .resp2,
        &server.script_store,
        .{ .array = &args1 },
        false,
    );
    allocator.free(resp1);

    // Publish to matching channel
    const args2 = [_]RespValue{
        .{ .bulk_string = "PUBLISH" },
        .{ .bulk_string = "news.world" },
        .{ .bulk_string = "hello" },
    };
    const resp2 = try commands.executeCommand(
        allocator,
        server.storage,
        null,
        &server.pubsub,
        null,
        null,
        null,
        &server.client_registry,
        1,
        .resp2,
        &server.script_store,
        .{ .array = &args2 },
        false,
    );
    defer allocator.free(resp2);

    // Should return :1 (1 subscriber received)
    try std.testing.expectEqualStrings(":1\r\n", resp2);

    // Check pending messages
    const pending = server.pubsub.pendingMessages(1);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    // pmessage frame is *4 elements
    try std.testing.expect(std.mem.startsWith(u8, pending[0], "*4\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, pending[0], "pmessage") != null);
}

test "PUBLISH - delivers to both exact and pattern subscribers" {
    const allocator = std.testing.allocator;
    const server = try Server.init(allocator, .{});
    defer server.deinit();

    const commands = @import("../src/commands/strings.zig");

    // Subscriber 1: exact channel
    const args1 = [_]RespValue{
        .{ .bulk_string = "SUBSCRIBE" },
        .{ .bulk_string = "news" },
    };
    const resp1 = try commands.executeCommand(
        allocator,
        server.storage,
        null,
        &server.pubsub,
        null,
        null,
        null,
        &server.client_registry,
        1,
        .resp2,
        &server.script_store,
        .{ .array = &args1 },
        false,
    );
    allocator.free(resp1);

    // Subscriber 2: pattern
    const args2 = [_]RespValue{
        .{ .bulk_string = "PSUBSCRIBE" },
        .{ .bulk_string = "news*" },
    };
    const resp2 = try commands.executeCommand(
        allocator,
        server.storage,
        null,
        &server.pubsub,
        null,
        null,
        null,
        &server.client_registry,
        2,
        .resp2,
        &server.script_store,
        .{ .array = &args2 },
        false,
    );
    allocator.free(resp2);

    // Publish to news
    const args3 = [_]RespValue{
        .{ .bulk_string = "PUBLISH" },
        .{ .bulk_string = "news" },
        .{ .bulk_string = "hello" },
    };
    const resp3 = try commands.executeCommand(
        allocator,
        server.storage,
        null,
        &server.pubsub,
        null,
        null,
        null,
        &server.client_registry,
        1,
        .resp2,
        &server.script_store,
        .{ .array = &args3 },
        false,
    );
    defer allocator.free(resp3);

    // Should deliver to both subscribers
    try std.testing.expectEqualStrings(":2\r\n", resp3);

    // Subscriber 1 gets regular message
    const pending1 = server.pubsub.pendingMessages(1);
    try std.testing.expectEqual(@as(usize, 1), pending1.len);
    try std.testing.expect(std.mem.startsWith(u8, pending1[0], "*3\r\n"));

    // Subscriber 2 gets pmessage
    const pending2 = server.pubsub.pendingMessages(2);
    try std.testing.expectEqual(@as(usize, 1), pending2.len);
    try std.testing.expect(std.mem.startsWith(u8, pending2[0], "*4\r\n"));
}

test "PUBSUB NUMPAT - returns pattern count" {
    const allocator = std.testing.allocator;
    const server = try Server.init(allocator, .{});
    defer server.deinit();

    const commands = @import("../src/commands/strings.zig");

    // Subscribe to patterns
    const args1 = [_]RespValue{
        .{ .bulk_string = "PSUBSCRIBE" },
        .{ .bulk_string = "news*" },
    };
    const resp1 = try commands.executeCommand(
        allocator,
        server.storage,
        null,
        &server.pubsub,
        null,
        null,
        null,
        &server.client_registry,
        1,
        .resp2,
        &server.script_store,
        .{ .array = &args1 },
        false,
    );
    allocator.free(resp1);

    const args2 = [_]RespValue{
        .{ .bulk_string = "PSUBSCRIBE" },
        .{ .bulk_string = "sports*" },
    };
    const resp2 = try commands.executeCommand(
        allocator,
        server.storage,
        null,
        &server.pubsub,
        null,
        null,
        null,
        &server.client_registry,
        2,
        .resp2,
        &server.script_store,
        .{ .array = &args2 },
        false,
    );
    allocator.free(resp2);

    // PUBSUB NUMPAT
    const args3 = [_]RespValue{
        .{ .bulk_string = "PUBSUB" },
        .{ .bulk_string = "NUMPAT" },
    };
    const resp3 = try commands.executeCommand(
        allocator,
        server.storage,
        null,
        &server.pubsub,
        null,
        null,
        null,
        &server.client_registry,
        1,
        .resp2,
        &server.script_store,
        .{ .array = &args3 },
        false,
    );
    defer allocator.free(resp3);

    try std.testing.expectEqualStrings(":2\r\n", resp3);
}

test "PUBSUB HELP - returns help text" {
    const allocator = std.testing.allocator;
    const server = try Server.init(allocator, .{});
    defer server.deinit();

    const commands = @import("../src/commands/strings.zig");

    const args = [_]RespValue{
        .{ .bulk_string = "PUBSUB" },
        .{ .bulk_string = "HELP" },
    };
    const resp = try commands.executeCommand(
        allocator,
        server.storage,
        null,
        &server.pubsub,
        null,
        null,
        null,
        &server.client_registry,
        1,
        .resp2,
        &server.script_store,
        .{ .array = &args },
        false,
    );
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "*"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "CHANNELS") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "NUMPAT") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "NUMSUB") != null);
}

test "Pattern matching - question mark wildcard" {
    const allocator = std.testing.allocator;
    const server = try Server.init(allocator, .{});
    defer server.deinit();

    const commands = @import("../src/commands/strings.zig");

    // Subscribe to sport? (matches sport1, sports, but not sport12)
    const args1 = [_]RespValue{
        .{ .bulk_string = "PSUBSCRIBE" },
        .{ .bulk_string = "sport?" },
    };
    const resp1 = try commands.executeCommand(
        allocator,
        server.storage,
        null,
        &server.pubsub,
        null,
        null,
        null,
        &server.client_registry,
        1,
        .resp2,
        &server.script_store,
        .{ .array = &args1 },
        false,
    );
    allocator.free(resp1);

    // Publish to sports (matches)
    const args2 = [_]RespValue{
        .{ .bulk_string = "PUBLISH" },
        .{ .bulk_string = "sports" },
        .{ .bulk_string = "hello" },
    };
    const resp2 = try commands.executeCommand(
        allocator,
        server.storage,
        null,
        &server.pubsub,
        null,
        null,
        null,
        &server.client_registry,
        1,
        .resp2,
        &server.script_store,
        .{ .array = &args2 },
        false,
    );
    defer allocator.free(resp2);

    try std.testing.expectEqualStrings(":1\r\n", resp2);

    // Publish to sport12 (does not match)
    const args3 = [_]RespValue{
        .{ .bulk_string = "PUBLISH" },
        .{ .bulk_string = "sport12" },
        .{ .bulk_string = "hello" },
    };
    const resp3 = try commands.executeCommand(
        allocator,
        server.storage,
        null,
        &server.pubsub,
        null,
        null,
        null,
        &server.client_registry,
        1,
        .resp2,
        &server.script_store,
        .{ .array = &args3 },
        false,
    );
    defer allocator.free(resp3);

    try std.testing.expectEqualStrings(":0\r\n", resp3);
}
