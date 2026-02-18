const std = @import("std");

/// Maximum number of pending messages per subscriber before oldest are dropped.
const MAX_PENDING_MESSAGES: usize = 1024;

/// PubSub errors
pub const PubSubError = error{
    OutOfMemory,
};

/// Per-subscriber state: which channels they are subscribed to and their
/// pending message queue. Messages are stored as fully-formatted RESP bytes.
const SubscriberState = struct {
    /// Set of channel names this subscriber is watching.
    /// Owned strings duplicated from caller input.
    channels: std.StringHashMap(void),
    /// Ring-buffer of pending RESP-formatted message frames.
    /// Each element is an owned slice allocated with the PubSub allocator.
    pending: std.ArrayList([]const u8),

    fn init(allocator: std.mem.Allocator) SubscriberState {
        return SubscriberState{
            .channels = std.StringHashMap(void).init(allocator),
            .pending = std.ArrayList([]const u8){},
        };
    }

    fn deinit(self: *SubscriberState, allocator: std.mem.Allocator) void {
        // Free all channel name strings
        var it = self.channels.keyIterator();
        while (it.next()) |key| {
            allocator.free(key.*);
        }
        self.channels.deinit();
        // Free all pending message bytes
        for (self.pending.items) |msg| {
            allocator.free(msg);
        }
        self.pending.deinit(allocator);
    }
};

/// Pub/Sub engine.
///
/// Maintains a mapping of channel -> subscriber IDs and per-subscriber
/// message queues. Since Zoltraak is single-threaded, no locking is needed
/// here; the server must not call publish/subscribe concurrently.
///
/// Ownership rules:
///   - Channel name strings stored in `channels` and `SubscriberState.channels`
///     are duplicated and owned by this struct.
///   - Message bytes in `SubscriberState.pending` are owned by this struct.
///   - Slices returned by `pendingMessages` point into internal storage;
///     callers must NOT free them and must not hold references across
///     subsequent mutation calls.
pub const PubSub = struct {
    allocator: std.mem.Allocator,
    /// channel name -> list of subscriber IDs.
    /// Channel name strings are owned (duplicated).
    channels: std.StringHashMap(std.ArrayList(u64)),
    /// subscriber_id -> SubscriberState
    subscribers: std.AutoHashMap(u64, SubscriberState),

    /// Initialise a new PubSub instance.
    pub fn init(allocator: std.mem.Allocator) PubSub {
        return PubSub{
            .allocator = allocator,
            .channels = std.StringHashMap(std.ArrayList(u64)).init(allocator),
            .subscribers = std.AutoHashMap(u64, SubscriberState).init(allocator),
        };
    }

    /// Free all resources owned by this PubSub instance.
    pub fn deinit(self: *PubSub) void {
        // Free channel subscriber lists and owned channel name keys
        var chan_it = self.channels.iterator();
        while (chan_it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.channels.deinit();

        // Free subscriber states
        var sub_it = self.subscribers.valueIterator();
        while (sub_it.next()) |state| {
            state.deinit(self.allocator);
        }
        self.subscribers.deinit();
    }

    /// Subscribe `subscriber_id` to `channel`.
    /// Returns the total number of channels this subscriber is now watching.
    /// Safe to call multiple times for the same (subscriber, channel) pair.
    pub fn subscribe(self: *PubSub, subscriber_id: u64, channel: []const u8) !usize {
        // Ensure subscriber state exists
        const state = try self.getOrCreateState(subscriber_id);

        // Check if already subscribed to avoid duplicate registration
        if (state.channels.contains(channel)) {
            return state.channels.count();
        }

        // Add channel to subscriber's set
        const channel_copy = try self.allocator.dupe(u8, channel);
        errdefer self.allocator.free(channel_copy);
        try state.channels.put(channel_copy, {});

        // Add subscriber to channel's list (create channel entry if needed)
        if (self.channels.getPtr(channel)) |sub_list| {
            try sub_list.append(self.allocator, subscriber_id);
        } else {
            const chan_key = try self.allocator.dupe(u8, channel);
            errdefer self.allocator.free(chan_key);

            var sub_list = std.ArrayList(u64){};
            errdefer sub_list.deinit(self.allocator);
            try sub_list.append(self.allocator, subscriber_id);

            try self.channels.put(chan_key, sub_list);
        }

        return state.channels.count();
    }

    /// Unsubscribe `subscriber_id` from `channel`.
    /// Returns the remaining number of channels this subscriber is watching.
    /// Returns 0 if the subscriber was not subscribed to the channel.
    pub fn unsubscribe(self: *PubSub, subscriber_id: u64, channel: []const u8) !usize {
        const state = self.subscribers.getPtr(subscriber_id) orelse return 0;

        // Remove channel from subscriber's set, freeing the owned key
        if (state.channels.fetchRemove(channel)) |kv| {
            self.allocator.free(kv.key);
        } else {
            // Not subscribed to this channel
            return state.channels.count();
        }

        // Remove subscriber from channel's list
        if (self.channels.getPtr(channel)) |sub_list| {
            for (sub_list.items, 0..) |sid, idx| {
                if (sid == subscriber_id) {
                    _ = sub_list.swapRemove(idx);
                    break;
                }
            }
            // If no subscribers remain, remove the channel entry entirely
            if (sub_list.items.len == 0) {
                sub_list.deinit(self.allocator);
                if (self.channels.fetchRemove(channel)) |kv| {
                    self.allocator.free(kv.key);
                }
            }
        }

        return state.channels.count();
    }

    /// Unsubscribe `subscriber_id` from all channels and remove its state.
    /// Safe to call even if the subscriber is not registered.
    pub fn unsubscribeAll(self: *PubSub, subscriber_id: u64) !void {
        const state = self.subscribers.getPtr(subscriber_id) orelse return;

        // Duplicate channel names into a temporary list.
        // We must copy the strings because `unsubscribe` frees the owned keys
        // from `state.channels`, which would invalidate raw pointers taken
        // from the iterator.
        var channel_names = std.ArrayList([]u8){};
        defer channel_names.deinit(self.allocator);

        var chan_it = state.channels.keyIterator();
        while (chan_it.next()) |key| {
            const copy = try self.allocator.dupe(u8, key.*);
            errdefer self.allocator.free(copy);
            try channel_names.append(self.allocator, copy);
        }

        // Unsubscribe from each (this frees the originals inside state.channels)
        for (channel_names.items) |ch| {
            _ = try self.unsubscribe(subscriber_id, ch);
            self.allocator.free(ch);
        }
        // Clear items so defer deinit doesn't try to access freed slices
        channel_names.clearRetainingCapacity();

        // Remove subscriber state entirely (pending messages freed here)
        if (self.subscribers.fetchRemove(subscriber_id)) |kv| {
            var st = kv.value;
            st.deinit(self.allocator);
        }
    }

    /// Publish `message` to `channel`.
    /// Enqueues a RESP push frame (`*3\r\n$7\r\nmessage\r\n…`) to every
    /// subscriber's pending queue.
    /// Returns the number of subscribers that received the message.
    pub fn publish(self: *PubSub, channel: []const u8, message: []const u8) !usize {
        const sub_list = self.channels.get(channel) orelse return 0;

        if (sub_list.items.len == 0) return 0;

        // Build the RESP push frame once, then clone it for each subscriber
        const frame = try buildMessageFrame(self.allocator, channel, message);
        defer self.allocator.free(frame);

        var delivered: usize = 0;
        for (sub_list.items) |sid| {
            const st = self.subscribers.getPtr(sid) orelse continue;

            // Drop oldest message if queue is full
            if (st.pending.items.len >= MAX_PENDING_MESSAGES) {
                const oldest = st.pending.orderedRemove(0);
                self.allocator.free(oldest);
            }

            const frame_copy = try self.allocator.dupe(u8, frame);
            errdefer self.allocator.free(frame_copy);
            try st.pending.append(self.allocator, frame_copy);
            delivered += 1;
        }

        return delivered;
    }

    /// Return a slice of pending RESP message frames for `subscriber_id`.
    /// The returned slice and the byte slices within it are valid only until
    /// the next mutation of this PubSub instance.
    /// Returns empty slice if subscriber has no pending messages.
    pub fn pendingMessages(self: *PubSub, subscriber_id: u64) []const []const u8 {
        const state = self.subscribers.getPtr(subscriber_id) orelse return &.{};
        return state.pending.items;
    }

    /// Clear the pending message queue for `subscriber_id`, freeing all memory.
    pub fn drainMessages(self: *PubSub, subscriber_id: u64) void {
        const state = self.subscribers.getPtr(subscriber_id) orelse return;
        for (state.pending.items) |msg| {
            self.allocator.free(msg);
        }
        state.pending.clearRetainingCapacity();
    }

    /// Return the number of channels `subscriber_id` is subscribed to.
    pub fn channelCount(self: *PubSub, subscriber_id: u64) usize {
        const state = self.subscribers.get(subscriber_id) orelse return 0;
        return state.channels.count();
    }

    /// Return an allocated slice of all active channel names (channels with
    /// at least one subscriber). Caller owns the returned slice but NOT the
    /// individual strings within it — those point into internal storage.
    pub fn activeChannels(self: *PubSub, allocator: std.mem.Allocator) ![][]const u8 {
        var result = std.ArrayList([]const u8){};
        errdefer result.deinit(allocator);

        var it = self.channels.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.items.len > 0) {
                try result.append(allocator, entry.key_ptr.*);
            }
        }

        return result.toOwnedSlice(allocator);
    }

    /// Return the number of subscribers for `channel`. Returns 0 for unknown channels.
    pub fn channelSubscriberCount(self: *PubSub, channel: []const u8) usize {
        const sub_list = self.channels.get(channel) orelse return 0;
        return sub_list.items.len;
    }

    // --- Private helpers ---

    fn getOrCreateState(self: *PubSub, subscriber_id: u64) !*SubscriberState {
        if (self.subscribers.getPtr(subscriber_id)) |state| return state;

        try self.subscribers.put(subscriber_id, SubscriberState.init(self.allocator));
        return self.subscribers.getPtr(subscriber_id).?;
    }
};

/// Build a Redis-compatible push frame for a received message:
/// ```
/// *3\r\n
/// $7\r\nmessage\r\n
/// $<channel_len>\r\n<channel>\r\n
/// $<msg_len>\r\n<msg>\r\n
/// ```
/// Caller owns the returned slice.
pub fn buildMessageFrame(allocator: std.mem.Allocator, channel: []const u8, message: []const u8) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    try buf.appendSlice(allocator, "*3\r\n");
    try buf.appendSlice(allocator, "$7\r\nmessage\r\n");
    try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ channel.len, channel });
    try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ message.len, message });

    return buf.toOwnedSlice(allocator);
}

/// Build a subscribe confirmation frame:
/// ```
/// *3\r\n
/// $9\r\nsubscribe\r\n
/// $<channel_len>\r\n<channel>\r\n
/// :<count>\r\n
/// ```
/// Caller owns the returned slice.
pub fn buildSubscribeFrame(allocator: std.mem.Allocator, channel: []const u8, count: usize) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    try buf.appendSlice(allocator, "*3\r\n");
    try buf.appendSlice(allocator, "$9\r\nsubscribe\r\n");
    try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ channel.len, channel });
    try std.fmt.format(buf.writer(allocator), ":{d}\r\n", .{count});

    return buf.toOwnedSlice(allocator);
}

/// Build an unsubscribe confirmation frame:
/// ```
/// *3\r\n
/// $11\r\nunsubscribe\r\n
/// $<channel_len>\r\n<channel>\r\n   (or $-1\r\n when channel is null)
/// :<count>\r\n
/// ```
/// Caller owns the returned slice.
pub fn buildUnsubscribeFrame(allocator: std.mem.Allocator, channel: ?[]const u8, count: usize) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    try buf.appendSlice(allocator, "*3\r\n");
    try buf.appendSlice(allocator, "$11\r\nunsubscribe\r\n");
    if (channel) |ch| {
        try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ ch.len, ch });
    } else {
        try buf.appendSlice(allocator, "$-1\r\n");
    }
    try std.fmt.format(buf.writer(allocator), ":{d}\r\n", .{count});

    return buf.toOwnedSlice(allocator);
}

// --- Embedded unit tests ---

test "pubsub - init and deinit empty" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();
    // No-op: must not leak
}

test "pubsub - subscribe once returns count 1" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const count = try ps.subscribe(1, "news");
    try std.testing.expectEqual(@as(usize, 1), count);
}

test "pubsub - subscribe twice same channel is idempotent" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");
    const count = try ps.subscribe(1, "news");
    // Still one channel, one subscriber in the channel list
    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(usize, 1), ps.channelSubscriberCount("news"));
}

test "pubsub - subscribe to multiple channels" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");
    const count = try ps.subscribe(1, "sports");
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectEqual(@as(usize, 2), ps.channelCount(1));
}

test "pubsub - unsubscribe reduces count" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");
    _ = try ps.subscribe(1, "sports");
    const count = try ps.unsubscribe(1, "news");
    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(usize, 0), ps.channelSubscriberCount("news"));
}

test "pubsub - unsubscribe non-subscribed channel returns current count" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");
    const count = try ps.unsubscribe(1, "sports");
    try std.testing.expectEqual(@as(usize, 1), count);
}

test "pubsub - unsubscribe unknown subscriber returns 0" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const count = try ps.unsubscribe(99, "news");
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "pubsub - unsubscribeAll removes subscriber" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");
    _ = try ps.subscribe(1, "sports");
    try ps.unsubscribeAll(1);
    try std.testing.expectEqual(@as(usize, 0), ps.channelCount(1));
    try std.testing.expectEqual(@as(usize, 0), ps.channelSubscriberCount("news"));
    try std.testing.expectEqual(@as(usize, 0), ps.channelSubscriberCount("sports"));
}

test "pubsub - publish to channel with no subscribers returns 0" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const delivered = try ps.publish("news", "hello");
    try std.testing.expectEqual(@as(usize, 0), delivered);
}

test "pubsub - publish delivers to subscriber" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");
    const delivered = try ps.publish("news", "hello");
    try std.testing.expectEqual(@as(usize, 1), delivered);

    const pending = ps.pendingMessages(1);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    // Check the RESP frame begins with *3\r\n
    try std.testing.expect(std.mem.startsWith(u8, pending[0], "*3\r\n"));
}

test "pubsub - publish delivers to multiple subscribers" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");
    _ = try ps.subscribe(2, "news");
    const delivered = try ps.publish("news", "hello");
    try std.testing.expectEqual(@as(usize, 2), delivered);
    try std.testing.expectEqual(@as(usize, 1), ps.pendingMessages(1).len);
    try std.testing.expectEqual(@as(usize, 1), ps.pendingMessages(2).len);
}

test "pubsub - publish not delivered to unsubscribed channel" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "sports");
    _ = try ps.publish("news", "hello");
    try std.testing.expectEqual(@as(usize, 0), ps.pendingMessages(1).len);
}

test "pubsub - drainMessages clears queue" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");
    _ = try ps.publish("news", "msg1");
    _ = try ps.publish("news", "msg2");
    try std.testing.expectEqual(@as(usize, 2), ps.pendingMessages(1).len);

    ps.drainMessages(1);
    try std.testing.expectEqual(@as(usize, 0), ps.pendingMessages(1).len);
}

test "pubsub - channelSubscriberCount" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    try std.testing.expectEqual(@as(usize, 0), ps.channelSubscriberCount("news"));
    _ = try ps.subscribe(1, "news");
    _ = try ps.subscribe(2, "news");
    try std.testing.expectEqual(@as(usize, 2), ps.channelSubscriberCount("news"));
}

test "pubsub - activeChannels returns only populated channels" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");
    _ = try ps.subscribe(2, "sports");

    const chans = try ps.activeChannels(allocator);
    defer allocator.free(chans);

    try std.testing.expectEqual(@as(usize, 2), chans.len);
}

test "pubsub - activeChannels excludes empty channels after unsubscribe" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");
    _ = try ps.unsubscribe(1, "news");

    const chans = try ps.activeChannels(allocator);
    defer allocator.free(chans);

    try std.testing.expectEqual(@as(usize, 0), chans.len);
}

test "pubsub - buildMessageFrame correct RESP format" {
    const allocator = std.testing.allocator;
    const frame = try buildMessageFrame(allocator, "news", "hello");
    defer allocator.free(frame);

    const expected = "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildSubscribeFrame correct RESP format" {
    const allocator = std.testing.allocator;
    const frame = try buildSubscribeFrame(allocator, "news", 1);
    defer allocator.free(frame);

    const expected = "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildUnsubscribeFrame with channel" {
    const allocator = std.testing.allocator;
    const frame = try buildUnsubscribeFrame(allocator, "news", 0);
    defer allocator.free(frame);

    const expected = "*3\r\n$11\r\nunsubscribe\r\n$4\r\nnews\r\n:0\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildUnsubscribeFrame with null channel" {
    const allocator = std.testing.allocator;
    const frame = try buildUnsubscribeFrame(allocator, null, 0);
    defer allocator.free(frame);

    const expected = "*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}
