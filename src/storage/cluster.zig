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

/// Error types for cluster slot operations
pub const ClusterError = error{
    SlotAlreadyBusy,
    SlotNotAssignedToNode,
    InvalidSlot,
    InvalidSlotRange,
    InvalidNodeId,
    UnknownNode,
    CannotForgetMyself,
    CannotForgetMaster,
    DatabaseNotEmpty,
    OutOfMemory,
    SlotNotOwnedByNode,
    SlotAlreadyOwned,
    SlotHasKeys,
};

/// Slot migration state
pub const SlotMigrationState = struct {
    /// Slot is being migrated to this node ID
    migrating_to: ?[40]u8 = null,
    /// Slot is being imported from this node ID
    importing_from: ?[40]u8 = null,
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
    /// Slot migration states
    slot_migration_states: [CLUSTER_SLOTS]SlotMigrationState,
    /// Current configuration epoch
    current_epoch: u64,
    /// Cluster state (ok/fail)
    state: State,
    /// Banned node IDs with expiration timestamps (Unix milliseconds)
    banned_nodes: std.StringHashMap(i64),

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
            .slot_migration_states = [_]SlotMigrationState{.{}} ** CLUSTER_SLOTS,
            .current_epoch = 0,
            .state = .fail,
            .banned_nodes = std.StringHashMap(i64).init(allocator),
        };
    }

    pub fn deinit(self: *ClusterState) void {
        var it = self.nodes.valueIterator();
        while (it.next()) |node| {
            node.*.deinit(self.allocator);
            self.allocator.destroy(node.*);
        }
        self.nodes.deinit();
        self.banned_nodes.deinit();
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

    /// Add individual slots to a node (used by CLUSTER ADDSLOTS)
    /// Validates all slots are unassigned before assignment
    pub fn addSlotsToNode(self: *ClusterState, node: *ClusterNode, slots_to_add: []const u16) ClusterError!void {
        // Validate all slots unassigned first
        for (slots_to_add) |slot| {
            if (slot >= CLUSTER_SLOTS) {
                return ClusterError.InvalidSlot;
            }
            if (self.slots[slot] != null) {
                return ClusterError.SlotAlreadyBusy;
            }
        }

        // Assign all slots
        for (slots_to_add) |slot| {
            self.slots[slot] = node;
        }

        // Recompress node's slot ranges
        try self.recompressNodeSlots(node);
        self.updateClusterState();
    }

    /// Remove slots from a node (used by CLUSTER DELSLOTS)
    /// Validates all slots are assigned to this node before removal
    pub fn removeSlotsFromNode(self: *ClusterState, node: *ClusterNode, slots_to_remove: []const u16) ClusterError!void {
        // Validate all slots assigned to this node first
        for (slots_to_remove) |slot| {
            if (slot >= CLUSTER_SLOTS) {
                return ClusterError.InvalidSlot;
            }
            if (self.slots[slot] != node) {
                return ClusterError.SlotNotAssignedToNode;
            }
        }

        // Unassign all slots
        for (slots_to_remove) |slot| {
            self.slots[slot] = null;
        }

        // Recompress node's slot ranges
        try self.recompressNodeSlots(node);
        self.updateClusterState();
    }

    /// Recompress a node's slot ranges after modification
    /// Rebuilds the slot ranges array to maintain consecutive ranges
    fn recompressNodeSlots(self: *ClusterState, node: *ClusterNode) ClusterError!void {
        node.slots.clearRetainingCapacity();

        var start: ?u16 = null;
        var prev: ?u16 = null;

        for (0..CLUSTER_SLOTS) |i| {
            const slot = @as(u16, @intCast(i));
            if (self.slots[slot] == node) {
                if (start == null) {
                    start = slot;
                } else if (prev != null and slot != prev.? + 1) {
                    // Non-consecutive, save previous range
                    try node.slots.append(self.allocator, .{
                        .start = start.?,
                        .end = prev.?,
                    });
                    start = slot;
                }
                prev = slot;
            }
        }

        // Save final range if any
        if (start != null and prev != null) {
            try node.slots.append(self.allocator, .{
                .start = start.?,
                .end = prev.?,
            });
        }
    }

    /// Flush all slot assignments (used by CLUSTER FLUSHSLOTS)
    /// Clears all slots and cluster state
    pub fn flushSlots(self: *ClusterState) void {
        // Clear all slot assignments
        for (&self.slots) |*slot| {
            slot.* = null;
        }

        // Clear all nodes' slot ranges
        var it = self.nodes.valueIterator();
        while (it.next()) |node_ptr| {
            node_ptr.*.slots.clearRetainingCapacity();
        }

        self.state = .fail;
    }

    /// Update cluster state based on slot coverage
    /// Sets state to .ok if all 16384 slots are covered, .fail otherwise
    pub fn updateClusterState(self: *ClusterState) void {
        var covered: u32 = 0;
        for (self.slots) |maybe_node| {
            if (maybe_node != null) covered += 1;
        }
        self.state = if (covered == CLUSTER_SLOTS) .ok else .fail;
    }

    /// Check if database is empty (required for FLUSHSLOTS)
    pub fn isDatabaseEmpty(storage: *const @import("memory.zig").Storage) bool {
        return storage.data.count() == 0;
    }

    /// Add a node to the cluster (used by CLUSTER MEET)
    pub fn meetNode(self: *ClusterState, allocator: std.mem.Allocator, ip: []const u8, port: u16, cluster_port: u16) ClusterError!*ClusterNode {
        // Validate port
        if (port < 1 or port > 65535) {
            return ClusterError.InvalidSlot; // Reuse for invalid parameter
        }

        // Generate a new node ID (random 40-char hex)
        var node_id: [40]u8 = undefined;
        var prng = std.Random.DefaultPrng.init(@intCast(std.time.microTimestamp()));
        const random = prng.random();
        const hex_chars = "0123456789abcdef";
        for (&node_id) |*c| {
            c.* = hex_chars[random.intRangeLessThan(u8, 0, 16)];
        }

        // Create new node
        const node = try allocator.create(ClusterNode);
        errdefer allocator.destroy(node);

        node.* = try ClusterNode.init(allocator, node_id, ip, port);
        errdefer node.deinit(allocator);

        node.cluster_port = cluster_port;
        node.flags.handshake = true;
        node.link_state = .disconnected;

        // Add to nodes map
        const key = try allocator.dupe(u8, &node_id);
        errdefer allocator.free(key);

        self.nodes.put(key, node) catch |err| {
            allocator.free(key);
            return err;
        };
        return node;
    }

    /// Remove a node from the cluster (used by CLUSTER FORGET)
    pub fn forgetNode(self: *ClusterState, allocator: std.mem.Allocator, node_id: []const u8, now_ms: i64) ClusterError!void {
        // Check if node exists
        const node_entry = self.nodes.fetchRemove(node_id) orelse {
            return ClusterError.UnknownNode;
        };

        const node = node_entry.value;

        // Clear any slots owned by this node
        for (&self.slots) |*slot| {
            if (slot.* == node) {
                slot.* = null;
            }
        }

        // Add to ban-list with 60 second expiration
        const ban_key = try allocator.dupe(u8, node_id);
        try self.banned_nodes.put(ban_key, now_ms + 60000); // 60 seconds in milliseconds

        // Clean up the node
        node.deinit(allocator);
        allocator.destroy(node);
        allocator.free(node_entry.key);

        self.updateClusterState();
    }

    /// Set a slot as migrating to a destination node
    /// Validates that the current node owns the slot
    /// Returns SlotNotOwnedByNode if slot is not owned by myself
    pub fn setSlotMigrating(self: *ClusterState, slot: u16, dest_node_id: [40]u8) ClusterError!void {
        if (slot >= CLUSTER_SLOTS) {
            return ClusterError.InvalidSlot;
        }

        // Validate that myself owns this slot
        if (self.slots[slot] != self.myself) {
            return ClusterError.SlotNotOwnedByNode;
        }

        // Set the migration state
        self.slot_migration_states[slot].migrating_to = dest_node_id;
    }

    /// Set a slot as importing from a source node
    /// Validates that the current node does NOT own the slot
    /// Returns SlotAlreadyOwned if slot is already owned by myself
    pub fn setSlotImporting(self: *ClusterState, slot: u16, source_node_id: [40]u8) ClusterError!void {
        if (slot >= CLUSTER_SLOTS) {
            return ClusterError.InvalidSlot;
        }

        // Validate that myself does NOT own this slot
        if (self.slots[slot] == self.myself) {
            return ClusterError.SlotAlreadyOwned;
        }

        // Set the import state
        self.slot_migration_states[slot].importing_from = source_node_id;
    }

    /// Set a slot to stable state (clear all migration states)
    /// Idempotent operation - succeeds even if no migration state exists
    pub fn setSlotStable(self: *ClusterState, slot: u16) ClusterError!void {
        if (slot >= CLUSTER_SLOTS) {
            return ClusterError.InvalidSlot;
        }

        // Clear both migration states
        self.slot_migration_states[slot].migrating_to = null;
        self.slot_migration_states[slot].importing_from = null;
    }

    /// Finalize slot ownership assignment
    /// If transitioning from importing→owner (slot being assigned to myself after importing),
    /// increments the config epoch. Otherwise, just updates ownership.
    /// Returns SlotHasKeys if attempting to reassign a slot with keys to a different node
    /// Returns UnknownNode if target node_id doesn't exist in the cluster
    pub fn setSlotNode(self: *ClusterState, slot: u16, node_id: [40]u8, slot_has_keys: bool) ClusterError!void {
        if (slot >= CLUSTER_SLOTS) {
            return ClusterError.InvalidSlot;
        }

        // Look up the target node
        const target_node = self.nodes.get(&node_id) orelse {
            return ClusterError.UnknownNode;
        };

        // Check if slot has keys and we're reassigning to a different node
        if (slot_has_keys and self.slots[slot] != target_node) {
            return ClusterError.SlotHasKeys;
        }

        // Check for importing→owner transition (increment config epoch)
        const is_importing_to_owner = self.slot_migration_states[slot].importing_from != null and
                                      target_node == self.myself;

        if (is_importing_to_owner) {
            // Increment config epoch and update myself's epoch
            self.current_epoch += 1;
            if (self.myself) |myself| {
                myself.config_epoch = self.current_epoch;
            }
        }

        // Clear migration states
        self.slot_migration_states[slot].migrating_to = null;
        self.slot_migration_states[slot].importing_from = null;

        // Update slot assignment
        self.slots[slot] = target_node;

        // Recompress node's slot ranges
        try self.recompressNodeSlots(target_node);
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

test "addSlotsToNode - assign individual slots" {
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

    // Add slots 100, 101, 102
    const slots = [_]u16{ 100, 101, 102 };
    try cluster.addSlotsToNode(node, &slots);

    // Verify slots are assigned
    try std.testing.expectEqual(node, cluster.slots[100]);
    try std.testing.expectEqual(node, cluster.slots[101]);
    try std.testing.expectEqual(node, cluster.slots[102]);

    // Verify slot ranges compressed
    try std.testing.expectEqual(@as(usize, 1), node.slots.items.len);
    try std.testing.expectEqual(@as(u16, 100), node.slots.items[0].start);
    try std.testing.expectEqual(@as(u16, 102), node.slots.items[0].end);
}

test "addSlotsToNode - reject already busy slot" {
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

    // Pre-assign slot 100
    cluster.slots[100] = node;

    // Try to add slot 100 again - should fail
    const slots = [_]u16{100};
    const result = cluster.addSlotsToNode(node, &slots);
    try std.testing.expectError(ClusterError.SlotAlreadyBusy, result);
}

test "removeSlotsFromNode - remove assigned slots" {
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

    // Add slots first
    var slots = [_]u16{ 100, 101, 102 };
    try cluster.addSlotsToNode(node, &slots);

    // Remove slot 101
    const remove_slots = [_]u16{101};
    try cluster.removeSlotsFromNode(node, &remove_slots);

    // Verify slot 101 is unassigned
    try std.testing.expectEqual(null, cluster.slots[101]);
    try std.testing.expectEqual(node, cluster.slots[100]);
    try std.testing.expectEqual(node, cluster.slots[102]);

    // Verify ranges are split
    try std.testing.expectEqual(@as(usize, 2), node.slots.items.len);
}

test "removeSlotsFromNode - reject unassigned slot" {
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

    // Try to remove unassigned slot
    const slots = [_]u16{100};
    const result = cluster.removeSlotsFromNode(node, &slots);
    try std.testing.expectError(ClusterError.SlotNotAssignedToNode, result);
}

test "flushSlots - clear all assignments" {
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

    // Assign all slots
    try cluster.assignSlots(node, 0, 16383);
    try std.testing.expectEqual(ClusterState.State.ok, cluster.state);

    // Flush slots
    cluster.flushSlots();

    // Verify all slots unassigned
    for (0..CLUSTER_SLOTS) |i| {
        const slot = @as(u16, @intCast(i));
        try std.testing.expectEqual(null, cluster.slots[slot]);
    }

    // Verify node has no ranges
    try std.testing.expectEqual(@as(usize, 0), node.slots.items.len);

    // Verify cluster state is fail
    try std.testing.expectEqual(ClusterState.State.fail, cluster.state);
}

test "updateClusterState - ok when all slots covered" {
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

    // Assign all slots
    try cluster.assignSlots(node, 0, 16383);
    cluster.updateClusterState();

    try std.testing.expectEqual(ClusterState.State.ok, cluster.state);
}

test "updateClusterState - fail when slots missing" {
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

    // Assign only partial slots
    try cluster.assignSlots(node, 0, 5000);
    cluster.updateClusterState();

    try std.testing.expectEqual(ClusterState.State.fail, cluster.state);
}

test "meetNode - add new node to cluster" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const node = try cluster.meetNode(allocator, "127.0.0.1", 7001, 17001);

    // Verify node in nodes map
    try std.testing.expectEqual(@as(usize, 1), cluster.nodes.count());

    // Verify node properties
    try std.testing.expectEqual(@as(u16, 7001), node.port);
    try std.testing.expectEqual(@as(u16, 17001), node.cluster_port);
    try std.testing.expect(node.flags.handshake);
    try std.testing.expectEqual(ClusterNode.LinkState.disconnected, node.link_state);
}

test "forgetNode - remove node from cluster" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const node = try cluster.meetNode(allocator, "127.0.0.1", 7001, 17001);
    const node_id_str = node.id;

    // Verify node in map
    try std.testing.expectEqual(@as(usize, 1), cluster.nodes.count());

    // Forget the node
    try cluster.forgetNode(allocator, &node_id_str, 1000000);

    // Verify node removed
    try std.testing.expectEqual(@as(usize, 0), cluster.nodes.count());

    // Verify banned
    try std.testing.expectEqual(@as(usize, 1), cluster.banned_nodes.count());
}

// CLUSTER SETSLOT tests

test "setSlotMigrating - success when node owns slot" {
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

    cluster.myself = node;

    // Assign slot 100 to myself
    const slots = [_]u16{100};
    try cluster.addSlotsToNode(node, &slots);

    // Set slot 100 as migrating to another node
    const dest_node_id = "abcdefabcdefabcdefabcdefabcdefabcdefabcd".*;
    try cluster.setSlotMigrating(100, dest_node_id);

    // Verify migration state
    try std.testing.expectEqual(dest_node_id, cluster.slot_migration_states[100].migrating_to.?);
    try std.testing.expectEqual(null, cluster.slot_migration_states[100].importing_from);
}

test "setSlotMigrating - error when node doesn't own slot" {
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

    cluster.myself = node;

    // Slot 100 is NOT assigned to myself
    const dest_node_id = "abcdefabcdefabcdefabcdefabcdefabcdefabcd".*;
    const result = cluster.setSlotMigrating(100, dest_node_id);

    // Should fail with SlotNotOwnedByNode
    try std.testing.expectError(ClusterError.SlotNotOwnedByNode, result);
}

test "setSlotImporting - success when node doesn't own slot" {
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

    cluster.myself = node;

    // Slot 100 is NOT assigned to myself (correct for importing)
    const source_node_id = "abcdefabcdefabcdefabcdefabcdefabcdefabcd".*;
    try cluster.setSlotImporting(100, source_node_id);

    // Verify import state
    try std.testing.expectEqual(source_node_id, cluster.slot_migration_states[100].importing_from.?);
    try std.testing.expectEqual(null, cluster.slot_migration_states[100].migrating_to);
}

test "setSlotImporting - error when node owns slot" {
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

    cluster.myself = node;

    // Assign slot 100 to myself
    const slots = [_]u16{100};
    try cluster.addSlotsToNode(node, &slots);

    // Try to import - should fail because we already own it
    const source_node_id = "abcdefabcdefabcdefabcdefabcdefabcdefabcd".*;
    const result = cluster.setSlotImporting(100, source_node_id);

    // Should fail with SlotAlreadyOwned
    try std.testing.expectError(ClusterError.SlotAlreadyOwned, result);
}

test "setSlotStable - clears migration states" {
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

    cluster.myself = node;

    // Assign slot 100
    const slots = [_]u16{100};
    try cluster.addSlotsToNode(node, &slots);

    // Set as migrating
    const dest_node_id = "abcdefabcdefabcdefabcdefabcdefabcdefabcd".*;
    try cluster.setSlotMigrating(100, dest_node_id);

    // Verify migrating state is set
    try std.testing.expect(cluster.slot_migration_states[100].migrating_to != null);

    // Set to stable
    try cluster.setSlotStable(100);

    // Verify states are cleared
    try std.testing.expectEqual(null, cluster.slot_migration_states[100].migrating_to);
    try std.testing.expectEqual(null, cluster.slot_migration_states[100].importing_from);
}

test "setSlotStable - idempotent when no migration state" {
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

    cluster.myself = node;

    // Slot 100 has no migration state
    try std.testing.expectEqual(null, cluster.slot_migration_states[100].migrating_to);
    try std.testing.expectEqual(null, cluster.slot_migration_states[100].importing_from);

    // Set to stable (should succeed without error)
    try cluster.setSlotStable(100);

    // States should still be null
    try std.testing.expectEqual(null, cluster.slot_migration_states[100].migrating_to);
    try std.testing.expectEqual(null, cluster.slot_migration_states[100].importing_from);
}

test "setSlotNode - finalizes ownership without config epoch change" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const my_node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_node_id, "127.0.0.1", 7000);
    defer {
        my_node.deinit(allocator);
        allocator.destroy(my_node);
    }

    cluster.myself = my_node;

    const other_node_id = "abcdefabcdefabcdefabcdefabcdefabcdefabcd".*;
    const other_node = try allocator.create(ClusterNode);
    other_node.* = try ClusterNode.init(allocator, other_node_id, "127.0.0.1", 7001);
    defer {
        other_node.deinit(allocator);
        allocator.destroy(other_node);
    }

    // Add other node to cluster
    const node_key = try allocator.dupe(u8, &other_node_id);
    try cluster.nodes.put(node_key, other_node);

    // Assign slot 100 to myself initially
    const slots = [_]u16{100};
    try cluster.addSlotsToNode(my_node, &slots);

    const initial_epoch = cluster.current_epoch;

    // Reassign to other_node (no importing state, so no epoch bump)
    try cluster.setSlotNode(100, other_node_id, false);

    // Verify slot ownership changed
    try std.testing.expectEqual(other_node, cluster.slots[100]);

    // Verify migration states cleared
    try std.testing.expectEqual(null, cluster.slot_migration_states[100].migrating_to);
    try std.testing.expectEqual(null, cluster.slot_migration_states[100].importing_from);

    // Verify epoch did NOT increase (no importing→owner transition)
    try std.testing.expectEqual(initial_epoch, cluster.current_epoch);
}

test "setSlotNode - generates config epoch on importing to owner transition" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const my_node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_node_id, "127.0.0.1", 7000);
    defer {
        my_node.deinit(allocator);
        allocator.destroy(my_node);
    }

    cluster.myself = my_node;

    // Slot 100 is not owned by myself (importing scenario)
    const source_node_id = "abcdefabcdefabcdefabcdefabcdefabcdefabcd".*;

    // Set as importing
    try cluster.setSlotImporting(100, source_node_id);

    const initial_epoch = cluster.current_epoch;

    // Finalize ownership to myself (importing→owner transition)
    try cluster.setSlotNode(100, my_node_id, false);

    // Verify slot ownership assigned
    try std.testing.expectEqual(my_node, cluster.slots[100]);

    // Verify migration states cleared
    try std.testing.expectEqual(null, cluster.slot_migration_states[100].migrating_to);
    try std.testing.expectEqual(null, cluster.slot_migration_states[100].importing_from);

    // Verify epoch increased (config epoch bump on importing→owner)
    try std.testing.expect(cluster.current_epoch > initial_epoch);
}

test "setSlotNode - error when slot has keys" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const my_node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_node_id, "127.0.0.1", 7000);
    defer {
        my_node.deinit(allocator);
        allocator.destroy(my_node);
    }

    cluster.myself = my_node;

    const other_node_id = "abcdefabcdefabcdefabcdefabcdefabcdefabcd".*;
    const other_node = try allocator.create(ClusterNode);
    other_node.* = try ClusterNode.init(allocator, other_node_id, "127.0.0.1", 7001);
    defer {
        other_node.deinit(allocator);
        allocator.destroy(other_node);
    }

    // Add other node to cluster
    const node_key = try allocator.dupe(u8, &other_node_id);
    try cluster.nodes.put(node_key, other_node);

    // Assign slot 100 to myself
    const slots = [_]u16{100};
    try cluster.addSlotsToNode(my_node, &slots);

    // Try to reassign slot with keys present (simulated by flag)
    const result = cluster.setSlotNode(100, other_node_id, true);

    // Should fail with SlotHasKeys
    try std.testing.expectError(ClusterError.SlotHasKeys, result);
}

test "setSlotNode - allows reassignment when slot is empty" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const my_node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_node_id, "127.0.0.1", 7000);
    defer {
        my_node.deinit(allocator);
        allocator.destroy(my_node);
    }

    cluster.myself = my_node;

    const other_node_id = "abcdefabcdefabcdefabcdefabcdefabcdefabcd".*;
    const other_node = try allocator.create(ClusterNode);
    other_node.* = try ClusterNode.init(allocator, other_node_id, "127.0.0.1", 7001);
    defer {
        other_node.deinit(allocator);
        allocator.destroy(other_node);
    }

    // Add other node to cluster
    const node_key = try allocator.dupe(u8, &other_node_id);
    try cluster.nodes.put(node_key, other_node);

    // Assign slot 100 to myself
    const slots = [_]u16{100};
    try cluster.addSlotsToNode(my_node, &slots);

    // Reassign to other_node (slot is empty)
    try cluster.setSlotNode(100, other_node_id, false);

    // Verify ownership changed
    try std.testing.expectEqual(other_node, cluster.slots[100]);
}

test "setSlot state transitions - MIGRATING to STABLE" {
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

    cluster.myself = node;

    // Assign slot 100
    const slots = [_]u16{100};
    try cluster.addSlotsToNode(node, &slots);

    // Transition: NORMAL → MIGRATING
    const dest_node_id = "abcdefabcdefabcdefabcdefabcdefabcdefabcd".*;
    try cluster.setSlotMigrating(100, dest_node_id);
    try std.testing.expect(cluster.slot_migration_states[100].migrating_to != null);

    // Transition: MIGRATING → STABLE
    try cluster.setSlotStable(100);
    try std.testing.expectEqual(null, cluster.slot_migration_states[100].migrating_to);
    try std.testing.expectEqual(null, cluster.slot_migration_states[100].importing_from);
}

test "setSlot state transitions - IMPORTING to NODE" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const my_node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_node_id, "127.0.0.1", 7000);
    defer {
        my_node.deinit(allocator);
        allocator.destroy(my_node);
    }

    cluster.myself = my_node;

    // Slot 100 is not owned (correct for importing)
    const source_node_id = "abcdefabcdefabcdefabcdefabcdefabcdefabcd".*;

    // Transition: NORMAL → IMPORTING
    try cluster.setSlotImporting(100, source_node_id);
    try std.testing.expect(cluster.slot_migration_states[100].importing_from != null);

    // Transition: IMPORTING → OWNER (finalize with NODE)
    try cluster.setSlotNode(100, my_node_id, false);
    try std.testing.expectEqual(my_node, cluster.slots[100]);
    try std.testing.expectEqual(null, cluster.slot_migration_states[100].importing_from);
    try std.testing.expectEqual(null, cluster.slot_migration_states[100].migrating_to);
}

test "setSlotNode - unknown node ID" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const my_node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_node_id, "127.0.0.1", 7000);
    defer {
        my_node.deinit(allocator);
        allocator.destroy(my_node);
    }

    cluster.myself = my_node;

    // Try to assign slot to unknown node
    const unknown_node_id = "ffffffffffffffffffffffffffffffffffffffff".*;
    const result = cluster.setSlotNode(100, unknown_node_id, false);

    // Should fail with UnknownNode
    try std.testing.expectError(ClusterError.UnknownNode, result);
}

