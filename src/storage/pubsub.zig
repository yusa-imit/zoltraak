const std = @import("std");
const zuda = @import("zuda");

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
    /// Set of pattern subscriptions this subscriber is watching.
    /// Owned strings duplicated from caller input.
    patterns: std.StringHashMap(void),
    /// Set of sharded channel names this subscriber is watching (Redis 7.0+).
    /// Owned strings duplicated from caller input.
    sharded_channels: std.StringHashMap(void),
    /// Ring-buffer of pending RESP-formatted message frames.
    /// Each element is an owned slice allocated with the PubSub allocator.
    pending: std.ArrayList([]const u8),
    /// RESP protocol version for this subscriber (2 or 3)
    resp_version: u8,

    fn init(allocator: std.mem.Allocator) SubscriberState {
        return SubscriberState{
            .channels = std.StringHashMap(void).init(allocator),
            .patterns = std.StringHashMap(void).init(allocator),
            .sharded_channels = std.StringHashMap(void).init(allocator),
            .pending = std.ArrayList([]const u8){},
            .resp_version = 2, // Default to RESP2
        };
    }

    fn deinit(self: *SubscriberState, allocator: std.mem.Allocator) void {
        // Free all channel name strings
        var it = self.channels.keyIterator();
        while (it.next()) |key| {
            allocator.free(key.*);
        }
        self.channels.deinit();
        // Free all pattern strings
        var pat_it = self.patterns.keyIterator();
        while (pat_it.next()) |key| {
            allocator.free(key.*);
        }
        self.patterns.deinit();
        // Free all sharded channel strings
        var shard_it = self.sharded_channels.keyIterator();
        while (shard_it.next()) |key| {
            allocator.free(key.*);
        }
        self.sharded_channels.deinit();
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
    /// pattern -> list of subscriber IDs for pattern subscriptions.
    /// Pattern strings are owned (duplicated).
    patterns: std.StringHashMap(std.ArrayList(u64)),
    /// sharded channel name -> list of subscriber IDs (Redis 7.0+).
    /// Sharded channels are hash-slot routed in cluster mode.
    /// Channel name strings are owned (duplicated).
    sharded_channels: std.StringHashMap(std.ArrayList(u64)),
    /// subscriber_id -> SubscriberState
    subscribers: std.AutoHashMap(u64, SubscriberState),

    /// Initialise a new PubSub instance.
    pub fn init(allocator: std.mem.Allocator) PubSub {
        return PubSub{
            .allocator = allocator,
            .channels = std.StringHashMap(std.ArrayList(u64)).init(allocator),
            .patterns = std.StringHashMap(std.ArrayList(u64)).init(allocator),
            .sharded_channels = std.StringHashMap(std.ArrayList(u64)).init(allocator),
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

        // Free pattern subscriber lists and owned pattern keys
        var pat_it = self.patterns.iterator();
        while (pat_it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.patterns.deinit();

        // Free sharded channel subscriber lists and owned channel name keys
        var shard_it = self.sharded_channels.iterator();
        while (shard_it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.sharded_channels.deinit();

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

    /// Subscribe `subscriber_id` to `pattern`.
    /// Returns the total number of patterns this subscriber is now watching.
    /// Safe to call multiple times for the same (subscriber, pattern) pair.
    pub fn psubscribe(self: *PubSub, subscriber_id: u64, pattern: []const u8) !usize {
        // Ensure subscriber state exists
        const state = try self.getOrCreateState(subscriber_id);

        // Check if already subscribed to avoid duplicate registration
        if (state.patterns.contains(pattern)) {
            return state.patterns.count();
        }

        // Add pattern to subscriber's set
        const pattern_copy = try self.allocator.dupe(u8, pattern);
        errdefer self.allocator.free(pattern_copy);
        try state.patterns.put(pattern_copy, {});

        // Add subscriber to pattern's list (create pattern entry if needed)
        if (self.patterns.getPtr(pattern)) |sub_list| {
            try sub_list.append(self.allocator, subscriber_id);
        } else {
            const pat_key = try self.allocator.dupe(u8, pattern);
            errdefer self.allocator.free(pat_key);

            var sub_list = std.ArrayList(u64){};
            errdefer sub_list.deinit(self.allocator);
            try sub_list.append(self.allocator, subscriber_id);

            try self.patterns.put(pat_key, sub_list);
        }

        return state.patterns.count();
    }

    /// Unsubscribe `subscriber_id` from `pattern`.
    /// Returns the remaining number of patterns this subscriber is watching.
    /// Returns 0 if the subscriber was not subscribed to the pattern.
    pub fn punsubscribe(self: *PubSub, subscriber_id: u64, pattern: []const u8) !usize {
        const state = self.subscribers.getPtr(subscriber_id) orelse return 0;

        // Remove pattern from subscriber's set, freeing the owned key
        if (state.patterns.fetchRemove(pattern)) |kv| {
            self.allocator.free(kv.key);
        } else {
            // Not subscribed to this pattern
            return state.patterns.count();
        }

        // Remove subscriber from pattern's list
        if (self.patterns.getPtr(pattern)) |sub_list| {
            for (sub_list.items, 0..) |sid, idx| {
                if (sid == subscriber_id) {
                    _ = sub_list.swapRemove(idx);
                    break;
                }
            }
            // If no subscribers remain, remove the pattern entry entirely
            if (sub_list.items.len == 0) {
                sub_list.deinit(self.allocator);
                if (self.patterns.fetchRemove(pattern)) |kv| {
                    self.allocator.free(kv.key);
                }
            }
        }

        return state.patterns.count();
    }

    /// Unsubscribe `subscriber_id` from all patterns.
    /// Does not affect channel subscriptions.
    pub fn punsubscribeAll(self: *PubSub, subscriber_id: u64) !void {
        const state = self.subscribers.getPtr(subscriber_id) orelse return;

        // Duplicate pattern names into a temporary list
        var pattern_names = std.ArrayList([]u8){};
        defer pattern_names.deinit(self.allocator);

        var pat_it = state.patterns.keyIterator();
        while (pat_it.next()) |key| {
            const copy = try self.allocator.dupe(u8, key.*);
            errdefer self.allocator.free(copy);
            try pattern_names.append(self.allocator, copy);
        }

        // Unsubscribe from each
        for (pattern_names.items) |pat| {
            _ = try self.punsubscribe(subscriber_id, pat);
            self.allocator.free(pat);
        }
        pattern_names.clearRetainingCapacity();
    }

    /// Publish `message` to `channel`.
    /// Enqueues a RESP push frame to every subscriber's pending queue.
    /// Matches against both exact channel subscriptions and pattern subscriptions.
    /// Returns the number of subscribers that received the message.
    pub fn publish(self: *PubSub, channel: []const u8, message: []const u8) !usize {
        var delivered: usize = 0;

        // Track which subscribers we've already delivered to (avoid duplicates)
        var delivered_to = std.AutoHashMap(u64, void).init(self.allocator);
        defer delivered_to.deinit();

        // 1. Deliver to exact channel subscribers
        if (self.channels.get(channel)) |sub_list| {
            for (sub_list.items) |sid| {
                const st = self.subscribers.getPtr(sid) orelse continue;

                // Build frame with subscriber's protocol version
                const frame = try buildMessageFrame(self.allocator, channel, message, st.resp_version);
                defer self.allocator.free(frame);

                // Drop oldest message if queue is full
                if (st.pending.items.len >= MAX_PENDING_MESSAGES) {
                    const oldest = st.pending.orderedRemove(0);
                    self.allocator.free(oldest);
                }

                const frame_copy = try self.allocator.dupe(u8, frame);
                errdefer self.allocator.free(frame_copy);
                try st.pending.append(self.allocator, frame_copy);
                try delivered_to.put(sid, {});
                delivered += 1;
            }
        }

        // 2. Deliver to pattern subscribers (pmessage format)
        var pat_it = self.patterns.iterator();
        while (pat_it.next()) |entry| {
            const pattern = entry.key_ptr.*;
            const sub_list = entry.value_ptr;

            // Check if channel matches this pattern
            if (!globMatch(pattern, channel)) continue;

            for (sub_list.items) |sid| {
                // Skip if already delivered via exact channel match
                if (delivered_to.contains(sid)) continue;

                const st = self.subscribers.getPtr(sid) orelse continue;

                // Build frame with subscriber's protocol version
                const pframe = try buildPmessageFrame(self.allocator, pattern, channel, message, st.resp_version);
                defer self.allocator.free(pframe);

                // Drop oldest message if queue is full
                if (st.pending.items.len >= MAX_PENDING_MESSAGES) {
                    const oldest = st.pending.orderedRemove(0);
                    self.allocator.free(oldest);
                }

                const pframe_copy = try self.allocator.dupe(u8, pframe);
                errdefer self.allocator.free(pframe_copy);
                try st.pending.append(self.allocator, pframe_copy);
                try delivered_to.put(sid, {});
                delivered += 1;
            }
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

    /// Return the number of patterns `subscriber_id` is subscribed to.
    pub fn patternCount(self: *PubSub, subscriber_id: u64) usize {
        const state = self.subscribers.get(subscriber_id) orelse return 0;
        return state.patterns.count();
    }

    /// Return the total number of active channel subscriptions (distinct channels with subscribers).
    pub fn totalChannelCount(self: *PubSub) usize {
        return self.channels.count();
    }

    /// Return the total number of active pattern subscriptions across all subscribers.
    pub fn totalPatternCount(self: *PubSub) usize {
        return self.patterns.count();
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

    // ── Sharded Pub/Sub (Redis 7.0+) ───────────────────────────────────────────

    /// Subscribe `subscriber_id` to sharded `channel`.
    /// Sharded channels are routed by hash slot in cluster mode.
    /// Returns the total number of sharded channels this subscriber is now watching.
    /// Safe to call multiple times for the same (subscriber, channel) pair.
    pub fn ssubscribe(self: *PubSub, subscriber_id: u64, channel: []const u8) !usize {
        // Ensure subscriber state exists
        const state = try self.getOrCreateState(subscriber_id);

        // Check if already subscribed to avoid duplicate registration
        if (state.sharded_channels.contains(channel)) {
            return state.sharded_channels.count();
        }

        // Add channel to subscriber's set
        const channel_copy = try self.allocator.dupe(u8, channel);
        errdefer self.allocator.free(channel_copy);
        try state.sharded_channels.put(channel_copy, {});

        // Add subscriber to sharded channel's list (create channel entry if needed)
        if (self.sharded_channels.getPtr(channel)) |sub_list| {
            try sub_list.append(self.allocator, subscriber_id);
        } else {
            const chan_key = try self.allocator.dupe(u8, channel);
            errdefer self.allocator.free(chan_key);

            var sub_list = std.ArrayList(u64){};
            errdefer sub_list.deinit(self.allocator);
            try sub_list.append(self.allocator, subscriber_id);

            try self.sharded_channels.put(chan_key, sub_list);
        }

        return state.sharded_channels.count();
    }

    /// Unsubscribe `subscriber_id` from sharded `channel`.
    /// Returns the remaining number of sharded channels this subscriber is watching.
    /// Returns 0 if the subscriber was not subscribed to the channel.
    pub fn sunsubscribe(self: *PubSub, subscriber_id: u64, channel: []const u8) !usize {
        const state = self.subscribers.getPtr(subscriber_id) orelse return 0;

        // Remove channel from subscriber's set, freeing the owned key
        if (state.sharded_channels.fetchRemove(channel)) |kv| {
            self.allocator.free(kv.key);
        } else {
            // Not subscribed to this channel
            return state.sharded_channels.count();
        }

        // Remove subscriber from channel's list
        if (self.sharded_channels.getPtr(channel)) |sub_list| {
            for (sub_list.items, 0..) |sid, idx| {
                if (sid == subscriber_id) {
                    _ = sub_list.swapRemove(idx);
                    break;
                }
            }
            // If no subscribers remain, remove the channel entry entirely
            if (sub_list.items.len == 0) {
                sub_list.deinit(self.allocator);
                if (self.sharded_channels.fetchRemove(channel)) |kv| {
                    self.allocator.free(kv.key);
                }
            }
        }

        return state.sharded_channels.count();
    }

    /// Unsubscribe `subscriber_id` from all sharded channels.
    /// Does not affect regular channel or pattern subscriptions.
    pub fn sunsubscribeAll(self: *PubSub, subscriber_id: u64) !void {
        const state = self.subscribers.getPtr(subscriber_id) orelse return;

        // Duplicate channel names into a temporary list
        var channel_names = std.ArrayList([]u8){};
        defer channel_names.deinit(self.allocator);

        var shard_it = state.sharded_channels.keyIterator();
        while (shard_it.next()) |key| {
            const copy = try self.allocator.dupe(u8, key.*);
            errdefer self.allocator.free(copy);
            try channel_names.append(self.allocator, copy);
        }

        // Unsubscribe from each
        for (channel_names.items) |ch| {
            _ = try self.sunsubscribe(subscriber_id, ch);
            self.allocator.free(ch);
        }
        channel_names.clearRetainingCapacity();
    }

    /// Publish `message` to sharded `channel`.
    /// Enqueues a RESP push frame to every sharded subscriber's pending queue.
    /// In cluster mode, this would only deliver to nodes responsible for channel's hash slot.
    /// Returns the number of subscribers that received the message.
    pub fn spublish(self: *PubSub, channel: []const u8, message: []const u8) !usize {
        var delivered: usize = 0;

        // Deliver to sharded channel subscribers
        if (self.sharded_channels.get(channel)) |sub_list| {
            for (sub_list.items) |sid| {
                const st = self.subscribers.getPtr(sid) orelse continue;

                // Build frame with subscriber's protocol version
                const frame = try buildSmessageFrame(self.allocator, channel, message, st.resp_version);
                defer self.allocator.free(frame);

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
        }

        return delivered;
    }

    /// Return an allocated slice of all active sharded channel names (channels with
    /// at least one subscriber). Caller owns the returned slice but NOT the
    /// individual strings within it — those point into internal storage.
    pub fn activeShardedChannels(self: *PubSub, allocator: std.mem.Allocator) ![][]const u8 {
        var result = std.ArrayList([]const u8){};
        errdefer result.deinit(allocator);

        var it = self.sharded_channels.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.items.len > 0) {
                try result.append(allocator, entry.key_ptr.*);
            }
        }

        return result.toOwnedSlice(allocator);
    }

    /// Return the number of subscribers for sharded `channel`. Returns 0 for unknown channels.
    pub fn shardedChannelSubscriberCount(self: *PubSub, channel: []const u8) usize {
        const sub_list = self.sharded_channels.get(channel) orelse return 0;
        return sub_list.items.len;
    }

    /// Return the number of sharded channels `subscriber_id` is subscribed to.
    pub fn shardedChannelCount(self: *PubSub, subscriber_id: u64) usize {
        const state = self.subscribers.get(subscriber_id) orelse return 0;
        return state.sharded_channels.count();
    }

    /// Set the RESP protocol version for a subscriber (2 or 3)
    /// Creates subscriber state if it doesn't exist
    pub fn setSubscriberVersion(self: *PubSub, subscriber_id: u64, version: u8) !void {
        const state = try self.getOrCreateState(subscriber_id);
        state.resp_version = version;
    }

    /// Get the RESP protocol version for a subscriber
    /// Returns the subscriber's version, or 2 (RESP2) if subscriber doesn't exist
    pub fn getSubscriberVersion(self: *PubSub, subscriber_id: u64) u8 {
        const state = self.subscribers.get(subscriber_id) orelse return 2;
        return state.resp_version;
    }

    // --- Private helpers ---

    fn getOrCreateState(self: *PubSub, subscriber_id: u64) !*SubscriberState {
        if (self.subscribers.getPtr(subscriber_id)) |state| return state;

        try self.subscribers.put(subscriber_id, SubscriberState.init(self.allocator));
        return self.subscribers.getPtr(subscriber_id).?;
    }
};

/// Build a Redis-compatible push frame for a received message.
/// RESP2 format (version=2):
/// ```
/// *3\r\n
/// $7\r\nmessage\r\n
/// $<channel_len>\r\n<channel>\r\n
/// $<msg_len>\r\n<msg>\r\n
/// ```
/// RESP3 format (version=3):
/// ```
/// >3\r\n
/// +message\r\n
/// $<channel_len>\r\n<channel>\r\n
/// $<msg_len>\r\n<msg>\r\n
/// ```
/// Caller owns the returned slice.
pub fn buildMessageFrame(allocator: std.mem.Allocator, channel: []const u8, message: []const u8, version: u8) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    if (version == 3) {
        // RESP3 push format
        try buf.appendSlice(allocator, ">3\r\n");
        try buf.appendSlice(allocator, "+message\r\n");
    } else {
        // RESP2 array format
        try buf.appendSlice(allocator, "*3\r\n");
        try buf.appendSlice(allocator, "$7\r\nmessage\r\n");
    }
    try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ channel.len, channel });
    try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ message.len, message });

    return buf.toOwnedSlice(allocator);
}

/// Build a subscribe confirmation frame.
/// RESP2 format (version=2):
/// ```
/// *3\r\n
/// $9\r\nsubscribe\r\n
/// $<channel_len>\r\n<channel>\r\n
/// :<count>\r\n
/// ```
/// RESP3 format (version=3):
/// ```
/// >3\r\n
/// +subscribe\r\n
/// $<channel_len>\r\n<channel>\r\n
/// :<count>\r\n
/// ```
/// Caller owns the returned slice.
pub fn buildSubscribeFrame(allocator: std.mem.Allocator, channel: []const u8, count: usize, version: u8) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    if (version == 3) {
        // RESP3 push format
        try buf.appendSlice(allocator, ">3\r\n");
        try buf.appendSlice(allocator, "+subscribe\r\n");
    } else {
        // RESP2 array format
        try buf.appendSlice(allocator, "*3\r\n");
        try buf.appendSlice(allocator, "$9\r\nsubscribe\r\n");
    }
    try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ channel.len, channel });
    try std.fmt.format(buf.writer(allocator), ":{d}\r\n", .{count});

    return buf.toOwnedSlice(allocator);
}

/// Build an unsubscribe confirmation frame.
/// RESP2 format (version=2):
/// ```
/// *3\r\n
/// $11\r\nunsubscribe\r\n
/// $<channel_len>\r\n<channel>\r\n   (or $-1\r\n when channel is null)
/// :<count>\r\n
/// ```
/// RESP3 format (version=3):
/// ```
/// >3\r\n
/// +unsubscribe\r\n
/// $<channel_len>\r\n<channel>\r\n   (or _\r\n when channel is null)
/// :<count>\r\n
/// ```
/// Caller owns the returned slice.
pub fn buildUnsubscribeFrame(allocator: std.mem.Allocator, channel: ?[]const u8, count: usize, version: u8) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    if (version == 3) {
        // RESP3 push format
        try buf.appendSlice(allocator, ">3\r\n");
        try buf.appendSlice(allocator, "+unsubscribe\r\n");
    } else {
        // RESP2 array format
        try buf.appendSlice(allocator, "*3\r\n");
        try buf.appendSlice(allocator, "$11\r\nunsubscribe\r\n");
    }
    if (channel) |ch| {
        try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ ch.len, ch });
    } else {
        if (version == 3) {
            try buf.appendSlice(allocator, "_\r\n");
        } else {
            try buf.appendSlice(allocator, "$-1\r\n");
        }
    }
    try std.fmt.format(buf.writer(allocator), ":{d}\r\n", .{count});

    return buf.toOwnedSlice(allocator);
}

/// Build a pattern message frame (pmessage).
/// RESP2 format (version=2):
/// ```
/// *4\r\n
/// $8\r\npmessage\r\n
/// $<pattern_len>\r\n<pattern>\r\n
/// $<channel_len>\r\n<channel>\r\n
/// $<msg_len>\r\n<msg>\r\n
/// ```
/// RESP3 format (version=3):
/// ```
/// >4\r\n
/// +pmessage\r\n
/// $<pattern_len>\r\n<pattern>\r\n
/// $<channel_len>\r\n<channel>\r\n
/// $<msg_len>\r\n<msg>\r\n
/// ```
/// Caller owns the returned slice.
pub fn buildPmessageFrame(allocator: std.mem.Allocator, pattern: []const u8, channel: []const u8, message: []const u8, version: u8) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    if (version == 3) {
        // RESP3 push format
        try buf.appendSlice(allocator, ">4\r\n");
        try buf.appendSlice(allocator, "+pmessage\r\n");
    } else {
        // RESP2 array format
        try buf.appendSlice(allocator, "*4\r\n");
        try buf.appendSlice(allocator, "$8\r\npmessage\r\n");
    }
    try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ pattern.len, pattern });
    try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ channel.len, channel });
    try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ message.len, message });

    return buf.toOwnedSlice(allocator);
}

/// Build a psubscribe confirmation frame.
/// RESP2 format (version=2):
/// ```
/// *3\r\n
/// $10\r\npsubscribe\r\n
/// $<pattern_len>\r\n<pattern>\r\n
/// :<count>\r\n
/// ```
/// RESP3 format (version=3):
/// ```
/// >3\r\n
/// +psubscribe\r\n
/// $<pattern_len>\r\n<pattern>\r\n
/// :<count>\r\n
/// ```
/// Caller owns the returned slice.
pub fn buildPsubscribeFrame(allocator: std.mem.Allocator, pattern: []const u8, count: usize, version: u8) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    if (version == 3) {
        // RESP3 push format
        try buf.appendSlice(allocator, ">3\r\n");
        try buf.appendSlice(allocator, "+psubscribe\r\n");
    } else {
        // RESP2 array format
        try buf.appendSlice(allocator, "*3\r\n");
        try buf.appendSlice(allocator, "$10\r\npsubscribe\r\n");
    }
    try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ pattern.len, pattern });
    try std.fmt.format(buf.writer(allocator), ":{d}\r\n", .{count});

    return buf.toOwnedSlice(allocator);
}

/// Build a punsubscribe confirmation frame.
/// RESP2 format (version=2):
/// ```
/// *3\r\n
/// $12\r\npunsubscribe\r\n
/// $<pattern_len>\r\n<pattern>\r\n   (or $-1\r\n when pattern is null)
/// :<count>\r\n
/// ```
/// RESP3 format (version=3):
/// ```
/// >3\r\n
/// +punsubscribe\r\n
/// $<pattern_len>\r\n<pattern>\r\n   (or _\r\n when pattern is null)
/// :<count>\r\n
/// ```
/// Caller owns the returned slice.
pub fn buildPunsubscribeFrame(allocator: std.mem.Allocator, pattern: ?[]const u8, count: usize, version: u8) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    if (version == 3) {
        // RESP3 push format
        try buf.appendSlice(allocator, ">3\r\n");
        try buf.appendSlice(allocator, "+punsubscribe\r\n");
    } else {
        // RESP2 array format
        try buf.appendSlice(allocator, "*3\r\n");
        try buf.appendSlice(allocator, "$12\r\npunsubscribe\r\n");
    }
    if (pattern) |pat| {
        try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ pat.len, pat });
    } else {
        if (version == 3) {
            try buf.appendSlice(allocator, "_\r\n");
        } else {
            try buf.appendSlice(allocator, "$-1\r\n");
        }
    }
    try std.fmt.format(buf.writer(allocator), ":{d}\r\n", .{count});

    return buf.toOwnedSlice(allocator);
}

/// Build a sharded message frame (smessage).
/// RESP2 format (version=2):
/// ```
/// *3\r\n
/// $8\r\nsmessage\r\n
/// $<channel_len>\r\n<channel>\r\n
/// $<msg_len>\r\n<msg>\r\n
/// ```
/// RESP3 format (version=3):
/// ```
/// >3\r\n
/// +smessage\r\n
/// $<channel_len>\r\n<channel>\r\n
/// $<msg_len>\r\n<msg>\r\n
/// ```
/// Caller owns the returned slice.
pub fn buildSmessageFrame(allocator: std.mem.Allocator, channel: []const u8, message: []const u8, version: u8) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    if (version == 3) {
        // RESP3 push format
        try buf.appendSlice(allocator, ">3\r\n");
        try buf.appendSlice(allocator, "+smessage\r\n");
    } else {
        // RESP2 array format
        try buf.appendSlice(allocator, "*3\r\n");
        try buf.appendSlice(allocator, "$8\r\nsmessage\r\n");
    }
    try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ channel.len, channel });
    try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ message.len, message });

    return buf.toOwnedSlice(allocator);
}

/// Build a ssubscribe confirmation frame.
/// RESP2 format (version=2):
/// ```
/// *3\r\n
/// $10\r\nssubscribe\r\n
/// $<channel_len>\r\n<channel>\r\n
/// :<count>\r\n
/// ```
/// RESP3 format (version=3):
/// ```
/// >3\r\n
/// +ssubscribe\r\n
/// $<channel_len>\r\n<channel>\r\n
/// :<count>\r\n
/// ```
/// Caller owns the returned slice.
pub fn buildSsubscribeFrame(allocator: std.mem.Allocator, channel: []const u8, count: usize, version: u8) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    if (version == 3) {
        // RESP3 push format
        try buf.appendSlice(allocator, ">3\r\n");
        try buf.appendSlice(allocator, "+ssubscribe\r\n");
    } else {
        // RESP2 array format
        try buf.appendSlice(allocator, "*3\r\n");
        try buf.appendSlice(allocator, "$10\r\nssubscribe\r\n");
    }
    try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ channel.len, channel });
    try std.fmt.format(buf.writer(allocator), ":{d}\r\n", .{count});

    return buf.toOwnedSlice(allocator);
}

/// Build a sunsubscribe confirmation frame.
/// RESP2 format (version=2):
/// ```
/// *3\r\n
/// $12\r\nsunsubscribe\r\n
/// $<channel_len>\r\n<channel>\r\n   (or $-1\r\n when channel is null)
/// :<count>\r\n
/// ```
/// RESP3 format (version=3):
/// ```
/// >3\r\n
/// +sunsubscribe\r\n
/// $<channel_len>\r\n<channel>\r\n   (or _\r\n when channel is null)
/// :<count>\r\n
/// ```
/// Caller owns the returned slice.
pub fn buildSunsubscribeFrame(allocator: std.mem.Allocator, channel: ?[]const u8, count: usize, version: u8) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    if (version == 3) {
        // RESP3 push format
        try buf.appendSlice(allocator, ">3\r\n");
        try buf.appendSlice(allocator, "+sunsubscribe\r\n");
    } else {
        // RESP2 array format
        try buf.appendSlice(allocator, "*3\r\n");
        try buf.appendSlice(allocator, "$12\r\nsunsubscribe\r\n");
    }
    if (channel) |ch| {
        try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ ch.len, ch });
    } else {
        if (version == 3) {
            try buf.appendSlice(allocator, "_\r\n");
        } else {
            try buf.appendSlice(allocator, "$-1\r\n");
        }
    }
    try std.fmt.format(buf.writer(allocator), ":{d}\r\n", .{count});

    return buf.toOwnedSlice(allocator);
}

/// Simple glob matching supporting `*` and `?` wildcards.
/// Glob matching using zuda implementation.
/// Supports `*` and `?` wildcards, case-sensitive.
/// Migrated from local implementation to zuda.algorithms.string.globMatch.
fn globMatch(pattern: []const u8, str: []const u8) bool {
    return zuda.algorithms.string.globMatch(pattern, str);
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

test "pubsub - buildMessageFrame correct RESP2 format" {
    const allocator = std.testing.allocator;
    const frame = try buildMessageFrame(allocator, "news", "hello", 2);
    defer allocator.free(frame);

    const expected = "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildMessageFrame correct RESP3 format" {
    const allocator = std.testing.allocator;
    const frame = try buildMessageFrame(allocator, "news", "hello", 3);
    defer allocator.free(frame);

    const expected = ">3\r\n+message\r\n$4\r\nnews\r\n$5\r\nhello\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildSubscribeFrame correct RESP2 format" {
    const allocator = std.testing.allocator;
    const frame = try buildSubscribeFrame(allocator, "news", 1, 2);
    defer allocator.free(frame);

    const expected = "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildSubscribeFrame correct RESP3 format" {
    const allocator = std.testing.allocator;
    const frame = try buildSubscribeFrame(allocator, "news", 1, 3);
    defer allocator.free(frame);

    const expected = ">3\r\n+subscribe\r\n$4\r\nnews\r\n:1\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildUnsubscribeFrame with channel RESP2" {
    const allocator = std.testing.allocator;
    const frame = try buildUnsubscribeFrame(allocator, "news", 0, 2);
    defer allocator.free(frame);

    const expected = "*3\r\n$11\r\nunsubscribe\r\n$4\r\nnews\r\n:0\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildUnsubscribeFrame with null channel RESP2" {
    const allocator = std.testing.allocator;
    const frame = try buildUnsubscribeFrame(allocator, null, 0, 2);
    defer allocator.free(frame);

    const expected = "*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildUnsubscribeFrame with channel RESP3" {
    const allocator = std.testing.allocator;
    const frame = try buildUnsubscribeFrame(allocator, "news", 0, 3);
    defer allocator.free(frame);

    const expected = ">3\r\n+unsubscribe\r\n$4\r\nnews\r\n:0\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildUnsubscribeFrame with null channel RESP3" {
    const allocator = std.testing.allocator;
    const frame = try buildUnsubscribeFrame(allocator, null, 0, 3);
    defer allocator.free(frame);

    const expected = ">3\r\n+unsubscribe\r\n_\r\n:0\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

// --- Pattern subscription tests ---

test "pubsub - psubscribe once returns count 1" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const count = try ps.psubscribe(1, "news*");
    try std.testing.expectEqual(@as(usize, 1), count);
}

test "pubsub - psubscribe twice same pattern is idempotent" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.psubscribe(1, "news*");
    const count = try ps.psubscribe(1, "news*");
    try std.testing.expectEqual(@as(usize, 1), count);
}

test "pubsub - psubscribe to multiple patterns" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.psubscribe(1, "news*");
    const count = try ps.psubscribe(1, "sports?");
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectEqual(@as(usize, 2), ps.patternCount(1));
}

test "pubsub - punsubscribe reduces count" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.psubscribe(1, "news*");
    _ = try ps.psubscribe(1, "sports*");
    const count = try ps.punsubscribe(1, "news*");
    try std.testing.expectEqual(@as(usize, 1), count);
}

test "pubsub - punsubscribe non-subscribed pattern returns current count" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.psubscribe(1, "news*");
    const count = try ps.punsubscribe(1, "sports*");
    try std.testing.expectEqual(@as(usize, 1), count);
}

test "pubsub - punsubscribeAll removes all patterns" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.psubscribe(1, "news*");
    _ = try ps.psubscribe(1, "sports*");
    try ps.punsubscribeAll(1);
    try std.testing.expectEqual(@as(usize, 0), ps.patternCount(1));
}

test "pubsub - publish delivers to pattern subscribers with pmessage frame" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.psubscribe(1, "news*");
    const delivered = try ps.publish("news.world", "hello");
    try std.testing.expectEqual(@as(usize, 1), delivered);

    const pending = ps.pendingMessages(1);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    // Check the pmessage RESP frame begins with *4\r\n
    try std.testing.expect(std.mem.startsWith(u8, pending[0], "*4\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, pending[0], "pmessage") != null);
}

test "pubsub - publish delivers to both exact and pattern subscribers" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");
    _ = try ps.psubscribe(2, "news*");
    const delivered = try ps.publish("news", "hello");
    try std.testing.expectEqual(@as(usize, 2), delivered);

    // Subscriber 1 gets regular message
    const pending1 = ps.pendingMessages(1);
    try std.testing.expectEqual(@as(usize, 1), pending1.len);
    try std.testing.expect(std.mem.startsWith(u8, pending1[0], "*3\r\n"));

    // Subscriber 2 gets pmessage
    const pending2 = ps.pendingMessages(2);
    try std.testing.expectEqual(@as(usize, 1), pending2.len);
    try std.testing.expect(std.mem.startsWith(u8, pending2[0], "*4\r\n"));
}

test "pubsub - publish avoids duplicate delivery to same subscriber" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");
    _ = try ps.psubscribe(1, "news*");
    const delivered = try ps.publish("news", "hello");
    // Should deliver only once (exact channel takes precedence)
    try std.testing.expectEqual(@as(usize, 1), delivered);
    try std.testing.expectEqual(@as(usize, 1), ps.pendingMessages(1).len);
}

test "pubsub - totalPatternCount returns global pattern count" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.psubscribe(1, "news*");
    _ = try ps.psubscribe(2, "sports*");
    _ = try ps.psubscribe(3, "news*"); // Same pattern, different subscriber
    try std.testing.expectEqual(@as(usize, 2), ps.totalPatternCount());
}

test "pubsub - buildPmessageFrame correct RESP2 format" {
    const allocator = std.testing.allocator;
    const frame = try buildPmessageFrame(allocator, "news*", "news.world", "hello", 2);
    defer allocator.free(frame);

    const expected = "*4\r\n$8\r\npmessage\r\n$5\r\nnews*\r\n$10\r\nnews.world\r\n$5\r\nhello\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildPmessageFrame correct RESP3 format" {
    const allocator = std.testing.allocator;
    const frame = try buildPmessageFrame(allocator, "news*", "news.world", "hello", 3);
    defer allocator.free(frame);

    const expected = ">4\r\n+pmessage\r\n$5\r\nnews*\r\n$10\r\nnews.world\r\n$5\r\nhello\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildPsubscribeFrame correct RESP2 format" {
    const allocator = std.testing.allocator;
    const frame = try buildPsubscribeFrame(allocator, "news*", 1, 2);
    defer allocator.free(frame);

    const expected = "*3\r\n$10\r\npsubscribe\r\n$5\r\nnews*\r\n:1\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildPsubscribeFrame correct RESP3 format" {
    const allocator = std.testing.allocator;
    const frame = try buildPsubscribeFrame(allocator, "news*", 1, 3);
    defer allocator.free(frame);

    const expected = ">3\r\n+psubscribe\r\n$5\r\nnews*\r\n:1\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildPunsubscribeFrame with pattern RESP2" {
    const allocator = std.testing.allocator;
    const frame = try buildPunsubscribeFrame(allocator, "news*", 0, 2);
    defer allocator.free(frame);

    const expected = "*3\r\n$12\r\npunsubscribe\r\n$5\r\nnews*\r\n:0\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildPunsubscribeFrame with null pattern RESP2" {
    const allocator = std.testing.allocator;
    const frame = try buildPunsubscribeFrame(allocator, null, 0, 2);
    defer allocator.free(frame);

    const expected = "*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:0\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildPunsubscribeFrame with pattern RESP3" {
    const allocator = std.testing.allocator;
    const frame = try buildPunsubscribeFrame(allocator, "news*", 0, 3);
    defer allocator.free(frame);

    const expected = ">3\r\n+punsubscribe\r\n$5\r\nnews*\r\n:0\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildPunsubscribeFrame with null pattern RESP3" {
    const allocator = std.testing.allocator;
    const frame = try buildPunsubscribeFrame(allocator, null, 0, 3);
    defer allocator.free(frame);

    const expected = ">3\r\n+punsubscribe\r\n_\r\n:0\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - globMatch exact match" {
    try std.testing.expect(globMatch("news", "news"));
}

test "pubsub - globMatch star wildcard" {
    try std.testing.expect(globMatch("news*", "news.world"));
    try std.testing.expect(globMatch("*", "anything"));
    try std.testing.expect(globMatch("news*world", "news.amazing.world"));
    try std.testing.expect(!globMatch("sports*", "news.world"));
}

test "pubsub - globMatch question wildcard" {
    try std.testing.expect(globMatch("n?ws", "news"));
    try std.testing.expect(globMatch("new?", "news"));
    try std.testing.expect(!globMatch("n?ws", "nws"));
    try std.testing.expect(!globMatch("n?ws", "newss"));
}

test "pubsub - globMatch combined wildcards" {
    try std.testing.expect(globMatch("n*s?", "newsy"));
    try std.testing.expect(globMatch("?ews*", "news.world"));
    try std.testing.expect(!globMatch("n*s?", "news"));
}

test "pubsub - globMatch no match" {
    try std.testing.expect(!globMatch("abc", "xyz"));
    try std.testing.expect(!globMatch("news*", "sports"));
}

// --- Sharded Pub/Sub tests (Redis 7.0+) ---

test "pubsub - ssubscribe once returns count 1" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const count = try ps.ssubscribe(1, "news");
    try std.testing.expectEqual(@as(usize, 1), count);
}

test "pubsub - ssubscribe twice same channel is idempotent" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.ssubscribe(1, "news");
    const count = try ps.ssubscribe(1, "news");
    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(usize, 1), ps.shardedChannelSubscriberCount("news"));
}

test "pubsub - ssubscribe to multiple channels" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.ssubscribe(1, "news");
    const count = try ps.ssubscribe(1, "sports");
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectEqual(@as(usize, 2), ps.shardedChannelCount(1));
}

test "pubsub - sunsubscribe reduces count" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.ssubscribe(1, "news");
    _ = try ps.ssubscribe(1, "sports");
    const count = try ps.sunsubscribe(1, "news");
    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(usize, 0), ps.shardedChannelSubscriberCount("news"));
}

test "pubsub - sunsubscribe non-subscribed channel returns current count" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.ssubscribe(1, "news");
    const count = try ps.sunsubscribe(1, "sports");
    try std.testing.expectEqual(@as(usize, 1), count);
}

test "pubsub - sunsubscribe unknown subscriber returns 0" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const count = try ps.sunsubscribe(99, "news");
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "pubsub - sunsubscribeAll removes all sharded channels" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.ssubscribe(1, "news");
    _ = try ps.ssubscribe(1, "sports");
    try ps.sunsubscribeAll(1);
    try std.testing.expectEqual(@as(usize, 0), ps.shardedChannelCount(1));
    try std.testing.expectEqual(@as(usize, 0), ps.shardedChannelSubscriberCount("news"));
    try std.testing.expectEqual(@as(usize, 0), ps.shardedChannelSubscriberCount("sports"));
}

test "pubsub - spublish to channel with no subscribers returns 0" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const delivered = try ps.spublish("news", "hello");
    try std.testing.expectEqual(@as(usize, 0), delivered);
}

test "pubsub - spublish delivers to subscriber" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.ssubscribe(1, "news");
    const delivered = try ps.spublish("news", "hello");
    try std.testing.expectEqual(@as(usize, 1), delivered);

    const pending = ps.pendingMessages(1);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    // Check the smessage RESP frame begins with *3\r\n
    try std.testing.expect(std.mem.startsWith(u8, pending[0], "*3\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, pending[0], "smessage") != null);
}

test "pubsub - spublish delivers to multiple subscribers" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.ssubscribe(1, "news");
    _ = try ps.ssubscribe(2, "news");
    const delivered = try ps.spublish("news", "hello");
    try std.testing.expectEqual(@as(usize, 2), delivered);
    try std.testing.expectEqual(@as(usize, 1), ps.pendingMessages(1).len);
    try std.testing.expectEqual(@as(usize, 1), ps.pendingMessages(2).len);
}

test "pubsub - spublish not delivered to regular channel subscribers" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");
    _ = try ps.spublish("news", "hello");
    try std.testing.expectEqual(@as(usize, 0), ps.pendingMessages(1).len);
}

test "pubsub - activeShardedChannels returns only populated channels" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.ssubscribe(1, "news");
    _ = try ps.ssubscribe(2, "sports");

    const chans = try ps.activeShardedChannels(allocator);
    defer allocator.free(chans);

    try std.testing.expectEqual(@as(usize, 2), chans.len);
}

test "pubsub - activeShardedChannels excludes empty channels after unsubscribe" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.ssubscribe(1, "news");
    _ = try ps.sunsubscribe(1, "news");

    const chans = try ps.activeShardedChannels(allocator);
    defer allocator.free(chans);

    try std.testing.expectEqual(@as(usize, 0), chans.len);
}

test "pubsub - buildSmessageFrame correct RESP2 format" {
    const allocator = std.testing.allocator;
    const frame = try buildSmessageFrame(allocator, "news", "hello", 2);
    defer allocator.free(frame);

    const expected = "*3\r\n$8\r\nsmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildSmessageFrame correct RESP3 format" {
    const allocator = std.testing.allocator;
    const frame = try buildSmessageFrame(allocator, "news", "hello", 3);
    defer allocator.free(frame);

    const expected = ">3\r\n+smessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildSsubscribeFrame correct RESP2 format" {
    const allocator = std.testing.allocator;
    const frame = try buildSsubscribeFrame(allocator, "news", 1, 2);
    defer allocator.free(frame);

    const expected = "*3\r\n$10\r\nssubscribe\r\n$4\r\nnews\r\n:1\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildSsubscribeFrame correct RESP3 format" {
    const allocator = std.testing.allocator;
    const frame = try buildSsubscribeFrame(allocator, "news", 1, 3);
    defer allocator.free(frame);

    const expected = ">3\r\n+ssubscribe\r\n$4\r\nnews\r\n:1\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildSunsubscribeFrame with channel RESP2" {
    const allocator = std.testing.allocator;
    const frame = try buildSunsubscribeFrame(allocator, "news", 0, 2);
    defer allocator.free(frame);

    const expected = "*3\r\n$12\r\nsunsubscribe\r\n$4\r\nnews\r\n:0\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildSunsubscribeFrame with null channel RESP2" {
    const allocator = std.testing.allocator;
    const frame = try buildSunsubscribeFrame(allocator, null, 0, 2);
    defer allocator.free(frame);

    const expected = "*3\r\n$12\r\nsunsubscribe\r\n$-1\r\n:0\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildSunsubscribeFrame with channel RESP3" {
    const allocator = std.testing.allocator;
    const frame = try buildSunsubscribeFrame(allocator, "news", 0, 3);
    defer allocator.free(frame);

    const expected = ">3\r\n+sunsubscribe\r\n$4\r\nnews\r\n:0\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - buildSunsubscribeFrame with null channel RESP3" {
    const allocator = std.testing.allocator;
    const frame = try buildSunsubscribeFrame(allocator, null, 0, 3);
    defer allocator.free(frame);

    const expected = ">3\r\n+sunsubscribe\r\n_\r\n:0\r\n";
    try std.testing.expectEqualStrings(expected, frame);
}

test "pubsub - sharded and regular channels are independent" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");
    _ = try ps.ssubscribe(1, "news");
    try std.testing.expectEqual(@as(usize, 1), ps.channelCount(1));
    try std.testing.expectEqual(@as(usize, 1), ps.shardedChannelCount(1));

    _ = try ps.publish("news", "regular");
    _ = try ps.spublish("news", "sharded");
    try std.testing.expectEqual(@as(usize, 2), ps.pendingMessages(1).len);
}

test "pubsub - setSubscriberVersion and getSubscriberVersion RESP2" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Default should be RESP2
    try std.testing.expectEqual(@as(u8, 2), ps.getSubscriberVersion(1));

    // Can be set explicitly
    try ps.setSubscriberVersion(1, 2);
    try std.testing.expectEqual(@as(u8, 2), ps.getSubscriberVersion(1));
}

test "pubsub - setSubscriberVersion and getSubscriberVersion RESP3" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Set to RESP3
    try ps.setSubscriberVersion(1, 3);
    try std.testing.expectEqual(@as(u8, 3), ps.getSubscriberVersion(1));
}

test "pubsub - getSubscriberVersion returns default for unknown subscriber" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    // Unknown subscriber defaults to RESP2
    try std.testing.expectEqual(@as(u8, 2), ps.getSubscriberVersion(999));
}

test "pubsub - publish respects subscriber RESP3 version" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");
    try ps.setSubscriberVersion(1, 3);
    _ = try ps.publish("news", "hello");

    const pending = ps.pendingMessages(1);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    // Should be RESP3 push format
    try std.testing.expect(std.mem.startsWith(u8, pending[0], ">3\r\n"));
}

test "pubsub - publish respects subscriber RESP2 version" {
    const allocator = std.testing.allocator;
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    _ = try ps.subscribe(1, "news");
    try ps.setSubscriberVersion(1, 2);
    _ = try ps.publish("news", "hello");

    const pending = ps.pendingMessages(1);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    // Should be RESP2 array format
    try std.testing.expect(std.mem.startsWith(u8, pending[0], "*3\r\n"));
}
