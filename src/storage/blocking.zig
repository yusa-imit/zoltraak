// Blocking queue infrastructure for XREAD BLOCK, XREADGROUP BLOCK, and other blocking commands
const std = @import("std");
const memory = @import("memory.zig");
const StreamId = memory.Value.StreamId;

/// Unblock behavior mode
pub const UnblockMode = enum {
    timeout, // Unblock as if timeout expired (default)
    error_mode, // Unblock with UNBLOCKED error
};

/// Represents a client blocked on XREAD or XREADGROUP
pub const BlockedClient = struct {
    client_id: usize,
    keys: [][]const u8, // Owned copies of key names
    start_ids: []StreamId, // Parsed start IDs for each key
    count: ?usize,
    timeout_ms: i64, // 0 = infinite
    start_time: i64, // Milliseconds since epoch
    allocator: std.mem.Allocator,

    pub fn deinit(self: *BlockedClient) void {
        for (self.keys) |key| {
            self.allocator.free(key);
        }
        self.allocator.free(self.keys);
        self.allocator.free(self.start_ids);
    }
};

/// Extended blocked client for XREADGROUP with group/consumer info
pub const BlockedXreadgroupClient = struct {
    client: BlockedClient,
    group: []const u8, // Owned copy
    consumer: []const u8, // Owned copy
    noack: bool,

    pub fn deinit(self: *BlockedXreadgroupClient) void {
        self.client.deinit();
        self.client.allocator.free(self.group);
        self.client.allocator.free(self.consumer);
    }
};

/// Response prepared for a blocked client
pub const ClientResponse = struct {
    client_id: usize,
    response: []const u8, // Owned RESP-encoded response
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ClientResponse) void {
        self.allocator.free(self.response);
    }
};

/// Tracks unblock requests for clients
pub const UnblockRequest = struct {
    mode: UnblockMode,
};

/// Manages blocked clients waiting for stream data
pub const BlockingQueue = struct {
    xread_clients: std.StringHashMap(std.ArrayList(BlockedClient)),
    xreadgroup_clients: std.StringHashMap(std.ArrayList(BlockedXreadgroupClient)),
    pending_responses: std.AutoHashMap(usize, ClientResponse), // client_id -> response
    unblock_requests: std.AutoHashMap(u64, UnblockRequest), // client_id -> unblock request
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) BlockingQueue {
        return .{
            .xread_clients = std.StringHashMap(std.ArrayList(BlockedClient)).init(allocator),
            .xreadgroup_clients = std.StringHashMap(std.ArrayList(BlockedXreadgroupClient)).init(allocator),
            .pending_responses = std.AutoHashMap(usize, ClientResponse).init(allocator),
            .unblock_requests = std.AutoHashMap(u64, UnblockRequest).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BlockingQueue) void {
        var iter = self.xread_clients.iterator();
        while (iter.next()) |entry| {
            for (entry.value_ptr.items) |*client| {
                client.deinit();
            }
            entry.value_ptr.deinit(self.allocator);
        }
        self.xread_clients.deinit();

        var iter2 = self.xreadgroup_clients.iterator();
        while (iter2.next()) |entry| {
            for (entry.value_ptr.items) |*client| {
                client.deinit();
            }
            entry.value_ptr.deinit(self.allocator);
        }
        self.xreadgroup_clients.deinit();

        var iter3 = self.pending_responses.iterator();
        while (iter3.next()) |entry| {
            var resp = entry.value_ptr.*;
            resp.deinit();
        }
        self.pending_responses.deinit();

        self.unblock_requests.deinit();
    }

    /// Enqueue a client blocked on XREAD
    pub fn enqueueXreadClient(self: *BlockingQueue, key: []const u8, client: BlockedClient) !void {
        const gop = try self.xread_clients.getOrPut(key);
        if (!gop.found_existing) {
            gop.value_ptr.* = std.ArrayList(BlockedClient){};
        }
        try gop.value_ptr.append(self.allocator, client);
    }

    /// Enqueue a client blocked on XREADGROUP
    pub fn enqueueXreadgroupClient(self: *BlockingQueue, key: []const u8, client: BlockedXreadgroupClient) !void {
        const gop = try self.xreadgroup_clients.getOrPut(key);
        if (!gop.found_existing) {
            gop.value_ptr.* = std.ArrayList(BlockedXreadgroupClient){};
        }
        try gop.value_ptr.append(self.allocator, client);
    }

    /// Get all clients blocked on a specific key (XREAD)
    pub fn getXreadClients(self: *BlockingQueue, key: []const u8) ?[]BlockedClient {
        const list = self.xread_clients.get(key) orelse return null;
        return list.items;
    }

    /// Get all clients blocked on a specific key (XREADGROUP)
    pub fn getXreadgroupClients(self: *BlockingQueue, key: []const u8) ?[]BlockedXreadgroupClient {
        const list = self.xreadgroup_clients.get(key) orelse return null;
        return list.items;
    }

    /// Store a prepared response for a client
    pub fn setPendingResponse(self: *BlockingQueue, client_id: usize, response: []const u8) !void {
        const owned_response = try self.allocator.dupe(u8, response);
        errdefer self.allocator.free(owned_response);

        const gop = try self.pending_responses.getOrPut(client_id);
        if (gop.found_existing) {
            // Replace existing response
            var old = gop.value_ptr.*;
            old.deinit();
        }
        gop.value_ptr.* = ClientResponse{
            .client_id = client_id,
            .response = owned_response,
            .allocator = self.allocator,
        };
    }

    /// Get and remove a pending response for a client
    pub fn takePendingResponse(self: *BlockingQueue, client_id: usize) ?[]const u8 {
        const resp = self.pending_responses.fetchRemove(client_id) orelse return null;
        const response = resp.value.response;
        // Don't deinit - caller owns the response now
        return response;
    }

    /// Request unblocking of a client
    /// Returns true if client exists and was found in blocking queues
    pub fn requestUnblock(self: *BlockingQueue, client_id: u64, mode: UnblockMode) !bool {
        // Check if client is actually blocked
        var found = false;

        // Check XREAD clients
        var iter = self.xread_clients.iterator();
        while (iter.next()) |entry| {
            for (entry.value_ptr.items) |client| {
                if (client.client_id == client_id) {
                    found = true;
                    break;
                }
            }
            if (found) break;
        }

        if (!found) {
            // Check XREADGROUP clients
            var iter2 = self.xreadgroup_clients.iterator();
            while (iter2.next()) |entry| {
                for (entry.value_ptr.items) |client| {
                    if (client.client.client_id == client_id) {
                        found = true;
                        break;
                    }
                }
                if (found) break;
            }
        }

        if (found) {
            try self.unblock_requests.put(client_id, UnblockRequest{ .mode = mode });
        }

        return found;
    }

    /// Check if a client has a pending unblock request
    /// If yes, returns the mode and removes the request
    pub fn checkUnblockRequest(self: *BlockingQueue, client_id: u64) ?UnblockMode {
        if (self.unblock_requests.fetchRemove(client_id)) |kv| {
            return kv.value.mode;
        }
        return null;
    }

    /// Remove all entries for a specific client (on disconnect)
    pub fn removeClient(self: *BlockingQueue, client_id: usize) void {
        // Remove from XREAD queues
        var iter = self.xread_clients.iterator();
        while (iter.next()) |entry| {
            const clients = entry.value_ptr;
            var i: usize = 0;
            while (i < clients.items.len) {
                if (clients.items[i].client_id == client_id) {
                    var removed = clients.swapRemove(i);
                    removed.deinit();
                } else {
                    i += 1;
                }
            }
        }

        // Remove from XREADGROUP queues
        var iter2 = self.xreadgroup_clients.iterator();
        while (iter2.next()) |entry| {
            const clients = entry.value_ptr;
            var i: usize = 0;
            while (i < clients.items.len) {
                if (clients.items[i].client.client_id == client_id) {
                    var removed = clients.swapRemove(i);
                    removed.deinit();
                } else {
                    i += 1;
                }
            }
        }

        // Remove pending response
        if (self.pending_responses.fetchRemove(client_id)) |kv| {
            var resp = kv.value;
            resp.deinit();
        }

        // Remove unblock request if any
        _ = self.unblock_requests.remove(@intCast(client_id));
    }

    /// Check for expired blocked clients and store timeout responses
    pub fn checkTimeouts(self: *BlockingQueue, now_ms: i64, nil_response: []const u8) !usize {
        var expired_count: usize = 0;

        // Check XREAD clients
        var iter = self.xread_clients.iterator();
        while (iter.next()) |entry| {
            const clients = entry.value_ptr;
            var i: usize = 0;
            while (i < clients.items.len) {
                const client = &clients.items[i];

                if (client.timeout_ms == 0) {
                    // Infinite wait
                    i += 1;
                    continue;
                }

                const elapsed = now_ms - client.start_time;
                if (elapsed >= client.timeout_ms) {
                    // Timeout expired - prepare nil response
                    try self.setPendingResponse(client.client_id, nil_response);
                    var removed = clients.swapRemove(i);
                    removed.deinit();
                    expired_count += 1;
                    // Don't increment i (swapRemove moved last item to this position)
                } else {
                    i += 1;
                }
            }
        }

        // Check XREADGROUP clients
        var iter2 = self.xreadgroup_clients.iterator();
        while (iter2.next()) |entry| {
            const clients = entry.value_ptr;
            var i: usize = 0;
            while (i < clients.items.len) {
                const client = &clients.items[i];

                if (client.client.timeout_ms == 0) {
                    // Infinite wait
                    i += 1;
                    continue;
                }

                const elapsed = now_ms - client.client.start_time;
                if (elapsed >= client.client.timeout_ms) {
                    // Timeout expired
                    try self.setPendingResponse(client.client.client_id, nil_response);
                    var removed = clients.swapRemove(i);
                    removed.deinit();
                    expired_count += 1;
                } else {
                    i += 1;
                }
            }
        }

        return expired_count;
    }
};

test "BlockingQueue: basic operations" {
    const testing = std.testing;
    var queue = BlockingQueue.init(testing.allocator);
    defer queue.deinit();

    // Create a blocked client
    const keys = try testing.allocator.alloc([]const u8, 1);
    keys[0] = try testing.allocator.dupe(u8, "stream1");
    const start_ids = try testing.allocator.alloc(StreamId, 1);
    start_ids[0] = StreamId{ .ms = 0, .seq = 0 };

    const client = BlockedClient{
        .client_id = 1,
        .keys = keys,
        .start_ids = start_ids,
        .count = null,
        .timeout_ms = 1000,
        .start_time = std.time.milliTimestamp(),
        .allocator = testing.allocator,
    };

    try queue.enqueueXreadClient("stream1", client);

    // Check retrieval
    const clients = queue.getXreadClients("stream1");
    try testing.expect(clients != null);
    try testing.expectEqual(@as(usize, 1), clients.?.len);
    try testing.expectEqual(@as(usize, 1), clients.?[0].client_id);
}

test "BlockingQueue: timeout checking" {
    const testing = std.testing;
    var queue = BlockingQueue.init(testing.allocator);
    defer queue.deinit();

    const now = std.time.milliTimestamp();

    // Create expired client
    const keys = try testing.allocator.alloc([]const u8, 1);
    keys[0] = try testing.allocator.dupe(u8, "stream1");
    const start_ids = try testing.allocator.alloc(StreamId, 1);
    start_ids[0] = StreamId{ .ms = 0, .seq = 0 };

    const client = BlockedClient{
        .client_id = 1,
        .keys = keys,
        .start_ids = start_ids,
        .count = null,
        .timeout_ms = 100, // 100ms timeout
        .start_time = now - 200, // Started 200ms ago
        .allocator = testing.allocator,
    };

    try queue.enqueueXreadClient("stream1", client);

    // Check timeouts
    const nil_response = "$-1\r\n";
    const expired = try queue.checkTimeouts(now, nil_response);
    try testing.expectEqual(@as(usize, 1), expired);

    // Verify nil response was stored
    const response = queue.takePendingResponse(1);
    try testing.expect(response != null);
    try testing.expectEqualStrings(nil_response, response.?);
    testing.allocator.free(response.?);
}

test "BlockingQueue: remove client" {
    const testing = std.testing;
    var queue = BlockingQueue.init(testing.allocator);
    defer queue.deinit();

    // Add two clients
    const keys1 = try testing.allocator.alloc([]const u8, 1);
    keys1[0] = try testing.allocator.dupe(u8, "stream1");
    const start_ids1 = try testing.allocator.alloc(StreamId, 1);
    start_ids1[0] = StreamId{ .ms = 0, .seq = 0 };

    const client1 = BlockedClient{
        .client_id = 1,
        .keys = keys1,
        .start_ids = start_ids1,
        .count = null,
        .timeout_ms = 0,
        .start_time = std.time.milliTimestamp(),
        .allocator = testing.allocator,
    };

    const keys2 = try testing.allocator.alloc([]const u8, 1);
    keys2[0] = try testing.allocator.dupe(u8, "stream1");
    const start_ids2 = try testing.allocator.alloc(StreamId, 1);
    start_ids2[0] = StreamId{ .ms = 0, .seq = 0 };

    const client2 = BlockedClient{
        .client_id = 2,
        .keys = keys2,
        .start_ids = start_ids2,
        .count = null,
        .timeout_ms = 0,
        .start_time = std.time.milliTimestamp(),
        .allocator = testing.allocator,
    };

    try queue.enqueueXreadClient("stream1", client1);
    try queue.enqueueXreadClient("stream1", client2);

    // Remove client 1
    queue.removeClient(1);

    // Verify only client 2 remains
    const clients = queue.getXreadClients("stream1");
    try testing.expect(clients != null);
    try testing.expectEqual(@as(usize, 1), clients.?.len);
    try testing.expectEqual(@as(usize, 2), clients.?[0].client_id);
}
