const std = @import("std");

/// Redis Cluster uses CRC16-CCITT with 16,384 slots (0-16383)
pub const CLUSTER_SLOTS = 16384;

/// CRC16 lookup table for CCITT polynomial (0x1021)
const crc16_tab: [256]u16 = blk: {
    @setEvalBranchQuota(10000);
    var tab: [256]u16 = undefined;
    for (&tab, 0..) |*entry, i| {
        var crc: u16 = @intCast(i << 8);
        for (0..8) |_| {
            if (crc & 0x8000 != 0) {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc = crc << 1;
            }
        }
        entry.* = crc;
    }
    break :blk tab;
};

/// Calculate CRC16 checksum for a byte slice
/// Uses CRC16-CCITT polynomial (0x1021) compatible with Redis Cluster
fn crc16(data: []const u8) u16 {
    var crc: u16 = 0;
    for (data) |byte| {
        const idx = @as(u8, @truncate((crc >> 8) ^ byte));
        crc = (crc << 8) ^ crc16_tab[idx];
    }
    return crc;
}

/// Extract hash tag from key if present
/// Hash tags are denoted by curly braces: {tag}
/// Returns the substring inside the first matching pair of braces,
/// or the full key if no valid hash tag is found.
///
/// Redis semantics:
/// - Find first '{' then find next '}'
/// - Return substring between them if non-empty
/// - If no valid tag found, return full key
///
/// Examples:
/// - "user:{123}:profile" -> "123"
/// - "key{tag}suffix" -> "tag"
/// - "no_tag_key" -> "no_tag_key"
/// - "key{}" -> "key{}" (empty tag is invalid)
/// - "}{tag}" -> "}{tag}" (opening must come before closing)
fn extractHashTag(key: []const u8) []const u8 {
    // Find first '{'
    var start_idx: ?usize = null;
    for (key, 0..) |char, i| {
        if (char == '{') {
            start_idx = i;
            break;
        }
    }

    if (start_idx == null) {
        return key; // No opening brace
    }

    const start = start_idx.? + 1;

    // Find first '}' after '{'
    for (key[start..], 0..) |char, offset| {
        if (char == '}') {
            const end = start + offset;
            // Only return tag if it's non-empty
            if (end > start) {
                return key[start..end];
            } else {
                return key; // Empty tag {} is invalid
            }
        }
    }

    // No closing brace found
    return key;
}

/// Calculate the Redis Cluster slot for a given key
/// Uses CRC16(key) mod 16384
/// Supports hash tags: {tag} inside key name
///
/// Examples:
/// - keySlot("user:123") -> CRC16("user:123") mod 16384
/// - keySlot("user:{123}:profile") -> CRC16("123") mod 16384
pub fn keySlot(key: []const u8) u16 {
    const hash_key = extractHashTag(key);
    const crc = crc16(hash_key);
    return @intCast(crc % CLUSTER_SLOTS);
}

/// Cluster node state
pub const ClusterNode = struct {
    /// Node ID (40-char hex string in Redis)
    id: [40]u8,
    /// IP address
    addr: []const u8,
    /// Client port
    port: u16,
    /// Cluster bus port (usually port + 10000)
    cluster_port: u16,
    /// Node flags (master, slave, fail, etc.)
    flags: NodeFlags,
    /// Master node ID if this is a replica
    master_id: ?[40]u8,
    /// Ping sent timestamp
    ping_sent: i64,
    /// Pong received timestamp
    pong_recv: i64,
    /// Configuration epoch
    config_epoch: u64,
    /// Link state (connected/disconnected)
    link_state: LinkState,
    /// Assigned slot ranges
    slots: std.ArrayListUnmanaged(SlotRange),

    pub const NodeFlags = packed struct {
        myself: bool = false,
        master: bool = false,
        slave: bool = false,
        pfail: bool = false,
        fail: bool = false,
        handshake: bool = false,
        noaddr: bool = false,
        nofailover: bool = false,
        _padding: u8 = 0,
    };

    pub const LinkState = enum {
        connected,
        disconnected,
    };

    pub const SlotRange = struct {
        start: u16,
        end: u16,
    };

    pub fn init(allocator: std.mem.Allocator, id: [40]u8, addr: []const u8, port: u16) !ClusterNode {
        return ClusterNode{
            .id = id,
            .addr = try allocator.dupe(u8, addr),
            .port = port,
            .cluster_port = port + 10000,
            .flags = .{ .master = true },
            .master_id = null,
            .ping_sent = 0,
            .pong_recv = 0,
            .config_epoch = 0,
            .link_state = .connected,
            .slots = .{},
        };
    }

    pub fn deinit(self: *ClusterNode, allocator: std.mem.Allocator) void {
        allocator.free(self.addr);
        self.slots.deinit(allocator);
    }
};

/// Cluster state management
pub const ClusterState = struct {
    allocator: std.mem.Allocator,
    /// Cluster enabled
    enabled: bool,
    /// Current node
    myself: ?*ClusterNode,
    /// All known nodes
    nodes: std.StringHashMap(*ClusterNode),
    /// Slot to node mapping
    slots: [CLUSTER_SLOTS]?*ClusterNode,
    /// Current configuration epoch
    current_epoch: u64,
    /// Cluster state (ok/fail)
    state: State,

    pub const State = enum {
        ok,
        fail,
    };

    pub fn init(allocator: std.mem.Allocator) ClusterState {
        return ClusterState{
            .allocator = allocator,
            .enabled = false,
            .myself = null,
            .nodes = std.StringHashMap(*ClusterNode).init(allocator),
            .slots = [_]?*ClusterNode{null} ** CLUSTER_SLOTS,
            .current_epoch = 0,
            .state = .fail,
        };
    }

    pub fn deinit(self: *ClusterState) void {
        var it = self.nodes.valueIterator();
        while (it.next()) |node| {
            node.*.deinit(self.allocator);
            self.allocator.destroy(node.*);
        }
        self.nodes.deinit();
    }

    /// Get the node responsible for a given slot
    pub fn getNodeBySlot(self: *const ClusterState, slot: u16) ?*ClusterNode {
        if (slot >= CLUSTER_SLOTS) return null;
        return self.slots[slot];
    }

    /// Get the node responsible for a given key
    pub fn getNodeByKey(self: *const ClusterState, key: []const u8) ?*ClusterNode {
        const slot = keySlot(key);
        return self.getNodeBySlot(slot);
    }

    /// Assign a slot range to a node
    pub fn assignSlots(self: *ClusterState, node: *ClusterNode, start: u16, end: u16) !void {
        if (start >= CLUSTER_SLOTS or end >= CLUSTER_SLOTS or start > end) {
            return error.InvalidSlotRange;
        }

        for (start..end + 1) |slot| {
            self.slots[slot] = node;
        }

        try node.slots.append(self.allocator, .{ .start = start, .end = end });
    }
};

// Tests
test "CRC16 calculation" {
    // Test that CRC16 produces consistent values
    const result1 = crc16("123456789");
    const result2 = crc16("123456789");
    try std.testing.expectEqual(result1, result2);

    // Empty string should produce 0
    const empty = crc16("");
    try std.testing.expectEqual(@as(u16, 0x0000), empty);

    // Different strings should produce different CRCs (high probability)
    const crc_a = crc16("a");
    const crc_b = crc16("b");
    try std.testing.expect(crc_a != crc_b);
}

test "Hash tag extraction" {
    const cases = .{
        .{ "user:{123}:profile", "123" },
        .{ "key{tag}suffix", "tag" },
        .{ "no_tag_key", "no_tag_key" },
        .{ "key{}", "key{}" }, // Empty tag is invalid
        .{ "{tag}", "tag" },
        .{ "key{tag1}{tag2}", "tag1" }, // First valid tag wins
        .{ "}{tag}", "tag" }, // Finds first valid tag after opening brace
        .{ "foo{bar", "foo{bar" }, // No closing brace
    };

    inline for (cases) |case| {
        const result = extractHashTag(case[0]);
        try std.testing.expectEqualStrings(case[1], result);
    }
}

test "Key slot calculation" {
    const allocator = std.testing.allocator;
    _ = allocator;

    // Same keys should always produce same slot
    const slot1 = keySlot("mykey");
    const slot2 = keySlot("mykey");
    try std.testing.expectEqual(slot1, slot2);

    // Slot should be in valid range
    try std.testing.expect(slot1 < CLUSTER_SLOTS);

    // Hash tags should cause different keys to map to same slot
    const slot_tagged1 = keySlot("user:{123}:profile");
    const slot_tagged2 = keySlot("session:{123}:data");
    try std.testing.expectEqual(slot_tagged1, slot_tagged2);

    // Without hash tags, they should map to different slots (high probability)
    const slot_untagged1 = keySlot("user:123:profile");
    const slot_untagged2 = keySlot("session:456:data");
    // This might rarely fail due to hash collision, but very unlikely
    try std.testing.expect(slot_untagged1 != slot_untagged2);
}

test "ClusterState init and deinit" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    try std.testing.expectEqual(false, cluster.enabled);
    try std.testing.expectEqual(null, cluster.myself);
    try std.testing.expectEqual(ClusterState.State.fail, cluster.state);
}

test "ClusterState slot assignment" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const node = try allocator.create(ClusterNode);
    node.* = try ClusterNode.init(allocator, node_id, "127.0.0.1", 7000);
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    // Assign slots 0-5000 to this node
    try cluster.assignSlots(node, 0, 5000);

    // Verify slot assignment
    const assigned_node = cluster.getNodeBySlot(100);
    try std.testing.expect(assigned_node != null);
    try std.testing.expectEqual(node, assigned_node.?);

    // Verify unassigned slot returns null
    const unassigned_node = cluster.getNodeBySlot(10000);
    try std.testing.expectEqual(null, unassigned_node);
}

test "ClusterState key routing" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const node = try allocator.create(ClusterNode);
    node.* = try ClusterNode.init(allocator, node_id, "127.0.0.1", 7000);
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    // Assign all slots to this node for simplicity
    try cluster.assignSlots(node, 0, 16383);

    // Any key should route to this node
    const routed_node = cluster.getNodeByKey("mykey");
    try std.testing.expect(routed_node != null);
    try std.testing.expectEqual(node, routed_node.?);
}
