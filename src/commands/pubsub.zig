const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const pubsub_mod = @import("../storage/pubsub.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const PubSub = pubsub_mod.PubSub;

/// SUBSCRIBE channel [channel ...]
///
/// Subscribes `subscriber_id` to one or more channels.
/// Returns a concatenated sequence of RESP subscribe-confirmation frames,
/// one per channel:
///   *3\r\n$9\r\nsubscribe\r\n$<len>\r\n<channel>\r\n:<count>\r\n
///
/// Caller owns the returned memory.
pub fn cmdSubscribe(
    allocator: std.mem.Allocator,
    ps: *PubSub,
    args: []const RespValue,
    subscriber_id: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'subscribe' command");
    }

    // Build a concatenated response of confirmation frames (one per channel)
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    for (args[1..]) |arg| {
        const channel = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid channel name"),
        };

        const total = try ps.subscribe(subscriber_id, channel);
        const frame = try pubsub_mod.buildSubscribeFrame(allocator, channel, total);
        defer allocator.free(frame);
        try buf.appendSlice(allocator, frame);
    }

    return buf.toOwnedSlice(allocator);
}

/// UNSUBSCRIBE [channel ...]
///
/// Unsubscribes `subscriber_id` from the given channels, or from all channels
/// if no channel argument is provided.
/// Returns a concatenated sequence of RESP unsubscribe-confirmation frames.
///
/// Caller owns the returned memory.
pub fn cmdUnsubscribe(
    allocator: std.mem.Allocator,
    ps: *PubSub,
    args: []const RespValue,
    subscriber_id: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    if (args.len == 1) {
        // No channels specified: unsubscribe from all
        // Snapshot channel list before mutating
        const count_before = ps.channelCount(subscriber_id);
        if (count_before == 0) {
            // Redis sends one frame with nil channel and count 0
            const frame = try pubsub_mod.buildUnsubscribeFrame(allocator, null, 0);
            defer allocator.free(frame);
            try buf.appendSlice(allocator, frame);
        } else {
            // Collect names first because unsubscribeAll frees them
            var names = std.ArrayList([]const u8){};
            defer names.deinit(allocator);

            // We need a snapshot before we destroy state
            if (ps.subscribers.getPtr(subscriber_id)) |state| {
                var it = state.channels.keyIterator();
                while (it.next()) |key| {
                    // Duplicate so we can hold the name after unsubscribing
                    const name_copy = try allocator.dupe(u8, key.*);
                    try names.append(allocator, name_copy);
                }
            }
            defer for (names.items) |n| allocator.free(n);

            // Now unsubscribe each and emit frames
            for (names.items) |ch| {
                const remaining = try ps.unsubscribe(subscriber_id, ch);
                const frame = try pubsub_mod.buildUnsubscribeFrame(allocator, ch, remaining);
                defer allocator.free(frame);
                try buf.appendSlice(allocator, frame);
            }
        }
    } else {
        for (args[1..]) |arg| {
            const channel = switch (arg) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid channel name"),
            };

            const remaining = try ps.unsubscribe(subscriber_id, channel);
            const frame = try pubsub_mod.buildUnsubscribeFrame(allocator, channel, remaining);
            defer allocator.free(frame);
            try buf.appendSlice(allocator, frame);
        }
    }

    return buf.toOwnedSlice(allocator);
}

/// PUBLISH channel message
///
/// Publish `message` to `channel`. Returns an integer response with the number
/// of subscribers that received the message.
///
/// Caller owns the returned memory.
pub fn cmdPublish(
    allocator: std.mem.Allocator,
    ps: *PubSub,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'publish' command");
    }

    const channel = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid channel name"),
    };

    const message = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid message"),
    };

    const delivered = try ps.publish(channel, message);
    return w.writeInteger(@intCast(delivered));
}

/// PUBSUB CHANNELS [pattern]
///
/// Returns an array of active channel names. If `pattern` is provided,
/// only channels whose names match the glob pattern are included.
/// Pattern matching supports `*` (any sequence) and `?` (any single char).
///
/// Caller owns the returned memory.
pub fn cmdPubsubChannels(
    allocator: std.mem.Allocator,
    ps: *PubSub,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "PUBSUB", args[1] = "CHANNELS", args[2] = optional pattern
    const pattern: ?[]const u8 = if (args.len >= 3)
        switch (args[2]) {
            .bulk_string => |s| s,
            else => null,
        }
    else
        null;

    const all_channels = try ps.activeChannels(allocator);
    defer allocator.free(all_channels);

    // Filter by pattern if provided
    var matched = std.ArrayList(RespValue){};
    defer matched.deinit(allocator);

    for (all_channels) |ch| {
        if (pattern) |pat| {
            if (globMatch(pat, ch)) {
                try matched.append(allocator, RespValue{ .bulk_string = ch });
            }
        } else {
            try matched.append(allocator, RespValue{ .bulk_string = ch });
        }
    }

    return w.writeArray(matched.items);
}

/// PUBSUB NUMSUB [channel ...]
///
/// Returns an array of alternating channel names and subscriber counts.
/// For channels that don't exist, count is 0.
///
/// Caller owns the returned memory.
pub fn cmdPubsubNumsub(
    allocator: std.mem.Allocator,
    ps: *PubSub,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "PUBSUB", args[1] = "NUMSUB", args[2..] = channels
    var result = std.ArrayList(RespValue){};
    defer result.deinit(allocator);

    for (args[2..]) |arg| {
        const channel = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid channel name"),
        };
        const count = ps.channelSubscriberCount(channel);
        try result.append(allocator, RespValue{ .bulk_string = channel });
        try result.append(allocator, RespValue{ .integer = @intCast(count) });
    }

    return w.writeArray(result.items);
}

/// Simple glob matching supporting `*` and `?` wildcards.
/// Case-sensitive, matching against the full string.
fn globMatch(pattern: []const u8, str: []const u8) bool {
    var pi: usize = 0;
    var si: usize = 0;
    var star_pi: usize = std.math.maxInt(usize);
    var star_si: usize = 0;

    while (si < str.len) {
        if (pi < pattern.len and (pattern[pi] == '?' or pattern[pi] == str[si])) {
            pi += 1;
            si += 1;
        } else if (pi < pattern.len and pattern[pi] == '*') {
            star_pi = pi;
            star_si = si;
            pi += 1;
        } else if (star_pi != std.math.maxInt(usize)) {
            pi = star_pi + 1;
            star_si += 1;
            si = star_si;
        } else {
            return false;
        }
    }

    // Consume trailing '*' in pattern
    while (pi < pattern.len and pattern[pi] == '*') {
        pi += 1;
    }

    return pi == pattern.len;
}

// --- Embedded unit tests ---

test "cmdPublish - wrong number of args" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{.{ .bulk_string = "PUBLISH" }};
    const resp = try cmdPublish(allocator, &ps, &args);
    defer allocator.free(resp);

    try std.testing.expectEqualStrings(
        "-ERR wrong number of arguments for 'publish' command\r\n",
        resp,
    );
}

test "cmdPublish - no subscribers returns 0" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "PUBLISH" },
        .{ .bulk_string = "news" },
        .{ .bulk_string = "hello" },
    };
    const resp = try cmdPublish(allocator, &ps, &args);
    defer allocator.free(resp);

    try std.testing.expectEqualStrings(":0\r\n", resp);
}

test "cmdPublish - delivers to one subscriber" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");

    const args = [_]RespValue{
        .{ .bulk_string = "PUBLISH" },
        .{ .bulk_string = "news" },
        .{ .bulk_string = "hello" },
    };
    const resp = try cmdPublish(allocator, &ps, &args);
    defer allocator.free(resp);

    try std.testing.expectEqualStrings(":1\r\n", resp);
}

test "cmdSubscribe - wrong number of args" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{.{ .bulk_string = "SUBSCRIBE" }};
    const resp = try cmdSubscribe(allocator, &ps, &args, 1);
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "cmdSubscribe - single channel confirmation frame" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "SUBSCRIBE" },
        .{ .bulk_string = "news" },
    };
    const resp = try cmdSubscribe(allocator, &ps, &args, 1);
    defer allocator.free(resp);

    // Should be a subscribe confirmation frame
    const expected = "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n";
    try std.testing.expectEqualStrings(expected, resp);
}

test "cmdSubscribe - multiple channels returns concatenated frames" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "SUBSCRIBE" },
        .{ .bulk_string = "ch1" },
        .{ .bulk_string = "ch2" },
    };
    const resp = try cmdSubscribe(allocator, &ps, &args, 1);
    defer allocator.free(resp);

    // ch1 count=1, ch2 count=2
    const expected =
        "*3\r\n$9\r\nsubscribe\r\n$3\r\nch1\r\n:1\r\n" ++
        "*3\r\n$9\r\nsubscribe\r\n$3\r\nch2\r\n:2\r\n";
    try std.testing.expectEqualStrings(expected, resp);
}

test "cmdUnsubscribe - single channel" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");

    const args = [_]RespValue{
        .{ .bulk_string = "UNSUBSCRIBE" },
        .{ .bulk_string = "news" },
    };
    const resp = try cmdUnsubscribe(allocator, &ps, &args, 1);
    defer allocator.free(resp);

    const expected = "*3\r\n$11\r\nunsubscribe\r\n$4\r\nnews\r\n:0\r\n";
    try std.testing.expectEqualStrings(expected, resp);
}

test "cmdUnsubscribe - no args when subscribed sends frames for each" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "ch1");
    _ = try ps.subscribe(1, "ch2");

    const args = [_]RespValue{.{ .bulk_string = "UNSUBSCRIBE" }};
    const resp = try cmdUnsubscribe(allocator, &ps, &args, 1);
    defer allocator.free(resp);

    // Should contain two unsubscribe frames; count may be 1 then 0 or 0 then 0
    // depending on order (hash map order is not guaranteed)
    try std.testing.expect(std.mem.count(u8, resp, "$11\r\nunsubscribe\r\n") == 2);
}

test "cmdUnsubscribe - no args when not subscribed sends nil frame" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{.{ .bulk_string = "UNSUBSCRIBE" }};
    const resp = try cmdUnsubscribe(allocator, &ps, &args, 99);
    defer allocator.free(resp);

    const expected = "*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n";
    try std.testing.expectEqualStrings(expected, resp);
}

test "cmdPubsubChannels - no channels returns empty array" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "PUBSUB" },
        .{ .bulk_string = "CHANNELS" },
    };
    const resp = try cmdPubsubChannels(allocator, &ps, &args);
    defer allocator.free(resp);

    try std.testing.expectEqualStrings("*0\r\n", resp);
}

test "cmdPubsubChannels - with pattern filters" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news.world");
    _ = try ps.subscribe(2, "sports");

    const args = [_]RespValue{
        .{ .bulk_string = "PUBSUB" },
        .{ .bulk_string = "CHANNELS" },
        .{ .bulk_string = "news*" },
    };
    const resp = try cmdPubsubChannels(allocator, &ps, &args);
    defer allocator.free(resp);

    try std.testing.expect(std.mem.startsWith(u8, resp, "*1\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "news.world") != null);
}

test "cmdPubsubNumsub - returns counts" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");
    _ = try ps.subscribe(2, "news");

    const args = [_]RespValue{
        .{ .bulk_string = "PUBSUB" },
        .{ .bulk_string = "NUMSUB" },
        .{ .bulk_string = "news" },
        .{ .bulk_string = "sports" },
    };
    const resp = try cmdPubsubNumsub(allocator, &ps, &args);
    defer allocator.free(resp);

    // *4\r\n (2 pairs), news -> 2, sports -> 0
    try std.testing.expect(std.mem.startsWith(u8, resp, "*4\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, resp, ":2\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, ":0\r\n") != null);
}

test "globMatch - exact match" {
    try std.testing.expect(globMatch("news", "news"));
}

test "globMatch - star wildcard" {
    try std.testing.expect(globMatch("news*", "news.world"));
    try std.testing.expect(globMatch("*", "anything"));
    try std.testing.expect(!globMatch("sports*", "news.world"));
}

test "globMatch - question wildcard" {
    try std.testing.expect(globMatch("n?ws", "news"));
    try std.testing.expect(!globMatch("n?ws", "nws"));
}

test "globMatch - no match" {
    try std.testing.expect(!globMatch("abc", "xyz"));
}
