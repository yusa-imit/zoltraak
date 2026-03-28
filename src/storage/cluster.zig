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

/// Gossip protocol message types
pub const GossipMessageType = enum {
    ping,
    pong,
    meet,
};

/// Gossip information about a node
pub const GossipNodeInfo = struct {
    node_id: [40]u8,
    addr: []const u8,
    port: u16,
    flags: ClusterNode.NodeFlags,
    pong_recv: i64,
};

/// Cluster gossip message
pub const GossipMessage = struct {
    msg_type: GossipMessageType,
    sender_id: [40]u8,
    sender_addr: []const u8,
    sender_port: u16,
    timestamp: i64,
    config_epoch: u64,
    gossip_nodes: []const GossipNodeInfo,
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
    /// Client IDs that have sent ASKING command (for ASK redirect bypass)
    asking_clients: std.AutoHashMap(u64, void),

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
            .asking_clients = std.AutoHashMap(u64, void).init(allocator),
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
        self.asking_clients.deinit();
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

    /// Set ASKING flag for a client (allows next command to execute on IMPORTING slot)
    /// Client must send ASKING before every command that needs to access IMPORTING slot
    pub fn setAsking(self: *ClusterState, client_id: u64) !void {
        try self.asking_clients.put(client_id, {});
    }

    /// Clear ASKING flag for a client (automatically cleared after command execution)
    pub fn clearAsking(self: *ClusterState, client_id: u64) void {
        _ = self.asking_clients.remove(client_id);
    }

    /// Check if client has ASKING flag set
    pub fn hasAsking(self: *const ClusterState, client_id: u64) bool {
        return self.asking_clients.contains(client_id);
    }

    /// Determine if a slot access should be redirected with ASK
    /// Returns the node to redirect to, or null if no redirect needed
    ///
    /// ASK redirect happens when:
    /// - Slot is MIGRATING from current node
    /// - Caller can provide dest_node from migration state
    ///
    /// Returns null (allow execution) when:
    /// - Slot is owned by current node and not MIGRATING
    /// - Slot is IMPORTING and client has ASKING flag set
    pub fn shouldAskRedirect(self: *const ClusterState, slot: u16, client_has_asking: bool) ?*ClusterNode {
        if (slot >= CLUSTER_SLOTS) return null;

        const migration_state = self.slot_migration_states[slot];

        // If slot is MIGRATING, redirect to destination (ASK redirect)
        if (migration_state.migrating_to) |dest_node_id| {
            return self.nodes.get(&dest_node_id);
        }

        // If slot is IMPORTING and client has ASKING flag, allow execution
        if (migration_state.importing_from != null and client_has_asking) {
            return null; // Allow execution
        }

        // If slot is IMPORTING but client hasn't sent ASKING, redirect to source (MOVED)
        // This case is handled by shouldMovedRedirect

        return null; // No ASK redirect needed
    }

    /// Determine if a slot access should be redirected with MOVED
    /// Returns the node to redirect to, or null if no redirect needed
    ///
    /// MOVED redirect happens when:
    /// - Slot is owned by a different node
    /// - Slot is IMPORTING but client hasn't sent ASKING
    ///
    /// Returns null (allow execution) when:
    /// - Slot is owned by current node
    /// - Slot is IMPORTING and client has ASKING flag set
    pub fn shouldMovedRedirect(self: *const ClusterState, slot: u16, client_has_asking: bool) ?*ClusterNode {
        if (slot >= CLUSTER_SLOTS) return null;

        const migration_state = self.slot_migration_states[slot];
        const current_owner = self.slots[slot];

        // If slot is IMPORTING and client has ASKING flag, allow execution
        if (migration_state.importing_from != null and client_has_asking) {
            return null; // Allow execution
        }

        // If slot is not owned by current node, redirect to owner (MOVED)
        if (current_owner != self.myself) {
            return current_owner;
        }

        return null; // No MOVED redirect needed
    }

    /// Create a PING gossip message
    /// PING messages are sent periodically to random nodes to maintain cluster health
    pub fn createPingMessage(self: *const ClusterState, allocator: std.mem.Allocator) !GossipMessage {
        const myself = self.myself orelse return error.NoSelfNode;

        // Select up to 3 random nodes to gossip about
        const gossip_nodes = try self.selectRandomNodesForGossip(allocator, 3);

        return GossipMessage{
            .msg_type = .ping,
            .sender_id = myself.id,
            .sender_addr = myself.addr,
            .sender_port = myself.port,
            .timestamp = std.time.milliTimestamp(),
            .config_epoch = self.current_epoch,
            .gossip_nodes = gossip_nodes,
        };
    }

    /// Create a PONG gossip message (response to PING)
    pub fn createPongMessage(self: *const ClusterState, allocator: std.mem.Allocator) !GossipMessage {
        const myself = self.myself orelse return error.NoSelfNode;

        // Select up to 3 random nodes to gossip about
        const gossip_nodes = try self.selectRandomNodesForGossip(allocator, 3);

        return GossipMessage{
            .msg_type = .pong,
            .sender_id = myself.id,
            .sender_addr = myself.addr,
            .sender_port = myself.port,
            .timestamp = std.time.milliTimestamp(),
            .config_epoch = self.current_epoch,
            .gossip_nodes = gossip_nodes,
        };
    }

    /// Process a MEET message (add new node to cluster)
    /// MEET is like PING but forces the receiver to accept the sender as part of the cluster
    pub fn processMeetMessage(self: *ClusterState, allocator: std.mem.Allocator, msg: *const GossipMessage) !void {
        // Check if node already exists
        const existing = self.nodes.get(&msg.sender_id);
        if (existing != null) {
            return; // Node already known
        }

        // Create new node
        const new_node = try allocator.create(ClusterNode);
        errdefer allocator.destroy(new_node);

        new_node.* = try ClusterNode.init(allocator, msg.sender_id, msg.sender_addr, msg.sender_port);
        new_node.config_epoch = msg.config_epoch;
        new_node.pong_recv = msg.timestamp;
        new_node.flags.master = true; // Default to master
        new_node.flags.handshake = true; // Mark as in handshake state

        // Add to nodes map
        const node_key = try allocator.dupe(u8, &msg.sender_id);
        errdefer allocator.free(node_key);
        try self.nodes.put(node_key, new_node);
    }

    /// Process a PING message and return a PONG response
    pub fn processPingMessage(self: *ClusterState, allocator: std.mem.Allocator, msg: *const GossipMessage) !GossipMessage {
        // Update sender's state if we know them
        if (self.nodes.get(&msg.sender_id)) |sender_node| {
            sender_node.pong_recv = msg.timestamp;
            sender_node.config_epoch = msg.config_epoch;
        }

        // Process gossip information about other nodes
        for (msg.gossip_nodes) |gossip_info| {
            // Skip if it's about ourselves
            if (self.myself) |myself| {
                if (std.mem.eql(u8, &gossip_info.node_id, &myself.id)) {
                    continue;
                }
            }

            // Add or update node information
            const existing = self.nodes.get(&gossip_info.node_id);
            if (existing == null) {
                // Learn about new node
                const new_node = try allocator.create(ClusterNode);
                errdefer allocator.destroy(new_node);

                new_node.* = try ClusterNode.init(allocator, gossip_info.node_id, gossip_info.addr, gossip_info.port);
                errdefer new_node.deinit(allocator);
                new_node.pong_recv = gossip_info.pong_recv;
                new_node.flags = gossip_info.flags;

                const node_key = try allocator.dupe(u8, &gossip_info.node_id);
                errdefer allocator.free(node_key);
                try self.nodes.put(node_key, new_node);
            }
        }

        // Create and return PONG response
        return self.createPongMessage(allocator);
    }

    /// Process a PONG message (update node state)
    pub fn processPongMessage(self: *ClusterState, msg: *const GossipMessage) !void {
        // Update sender's state
        if (self.nodes.get(&msg.sender_id)) |sender_node| {
            sender_node.pong_recv = msg.timestamp;
            sender_node.config_epoch = msg.config_epoch;
            sender_node.flags.handshake = false; // Handshake complete

            // Clear pfail flag if set
            if (sender_node.flags.pfail) {
                sender_node.flags.pfail = false;
            }
        }

        // Process gossip information about other nodes
        for (msg.gossip_nodes) |gossip_info| {
            // Skip if it's about ourselves
            if (self.myself) |myself| {
                if (std.mem.eql(u8, &gossip_info.node_id, &myself.id)) {
                    continue;
                }
            }

            // Update node information if we know about them
            if (self.nodes.get(&gossip_info.node_id)) |known_node| {
                // Update timestamp if newer
                if (gossip_info.pong_recv > known_node.pong_recv) {
                    known_node.pong_recv = gossip_info.pong_recv;
                }
            }
        }
    }

    /// Select random nodes for gossip (up to max_nodes)
    /// Returns array of GossipNodeInfo that caller must free
    pub fn selectRandomNodesForGossip(self: *const ClusterState, allocator: std.mem.Allocator, max_nodes: usize) ![]GossipNodeInfo {
        var result = std.ArrayListUnmanaged(GossipNodeInfo){};
        errdefer result.deinit(allocator);

        var it = self.nodes.valueIterator();
        var count: usize = 0;

        // Simple selection: just take first N nodes
        // TODO: Make this truly random using RNG
        while (it.next()) |node| {
            if (count >= max_nodes) break;

            // Skip ourselves
            if (self.myself) |myself| {
                if (std.mem.eql(u8, &node.*.id, &myself.id)) {
                    continue;
                }
            }

            try result.append(allocator, GossipNodeInfo{
                .node_id = node.*.id,
                .addr = node.*.addr,
                .port = node.*.port,
                .flags = node.*.flags,
                .pong_recv = node.*.pong_recv,
            });

            count += 1;
        }

        return result.toOwnedSlice(allocator);
    }

    /// Update node health status based on ping/pong timestamps
    /// Marks nodes as pfail if no PONG received within timeout (5 seconds)
    pub fn updateNodeHealth(self: *ClusterState) void {
        const now = std.time.milliTimestamp();
        const timeout_ms = 5000; // 5 second timeout

        var it = self.nodes.valueIterator();
        while (it.next()) |node| {
            // Skip ourselves
            if (self.myself) |myself| {
                if (std.mem.eql(u8, &node.*.id, &myself.id)) {
                    continue;
                }
            }

            // Check if node has timed out
            if (node.*.ping_sent > 0 and node.*.pong_recv < node.*.ping_sent) {
                const elapsed = now - node.*.ping_sent;
                if (elapsed > timeout_ms) {
                    // Mark as possibly failing
                    node.*.flags.pfail = true;
                }
            }
        }
    }

    /// Send PING to a specific node (updates ping_sent timestamp)
    pub fn sendPingToNode(self: *ClusterState, node_id: [40]u8) void {
        if (self.nodes.get(&node_id)) |node| {
            node.ping_sent = std.time.milliTimestamp();
        }
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

test "shouldAskRedirect - slot MIGRATING returns destination node" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Create two nodes
    const my_node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_node_id, "127.0.0.1", 7000);
    defer {
        my_node.deinit(allocator);
        allocator.destroy(my_node);
    }

    const dest_node_id = "fedcba9876543210fedcba9876543210fedcba98".*;
    const dest_node = try allocator.create(ClusterNode);
    dest_node.* = try ClusterNode.init(allocator, dest_node_id, "127.0.0.2", 7001);

    cluster.myself = my_node;
    const key = try allocator.dupe(u8, &dest_node_id);
    try cluster.nodes.put(key, dest_node);

    // Assign slot to myself
    cluster.slots[100] = my_node;

    // Set slot as MIGRATING to dest_node
    try cluster.setSlotMigrating(100, dest_node_id);

    // Check redirect - should return dest_node regardless of ASKING flag
    const redirect1 = cluster.shouldAskRedirect(100, false);
    try std.testing.expectEqual(dest_node, redirect1);

    const redirect2 = cluster.shouldAskRedirect(100, true);
    try std.testing.expectEqual(dest_node, redirect2);
}

test "shouldAskRedirect - slot IMPORTING with ASKING allows execution" {
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

    const source_node_id = "fedcba9876543210fedcba9876543210fedcba98".*;

    // Set slot as IMPORTING
    try cluster.setSlotImporting(100, source_node_id);

    // Without ASKING flag, should not redirect (handled by MOVED)
    const redirect1 = cluster.shouldAskRedirect(100, false);
    try std.testing.expectEqual(null, redirect1);

    // With ASKING flag, should allow execution (no redirect)
    const redirect2 = cluster.shouldAskRedirect(100, true);
    try std.testing.expectEqual(null, redirect2);
}

test "shouldMovedRedirect - slot not owned returns owner node" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Create two nodes
    const my_node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_node_id, "127.0.0.1", 7000);
    defer {
        my_node.deinit(allocator);
        allocator.destroy(my_node);
    }

    const owner_node_id = "fedcba9876543210fedcba9876543210fedcba98".*;
    const owner_node = try allocator.create(ClusterNode);
    owner_node.* = try ClusterNode.init(allocator, owner_node_id, "127.0.0.2", 7001);

    cluster.myself = my_node;
    const key = try allocator.dupe(u8, &owner_node_id);
    try cluster.nodes.put(key, owner_node);

    // Assign slot to owner_node (not myself)
    cluster.slots[100] = owner_node;

    // Should redirect to owner_node
    const redirect = cluster.shouldMovedRedirect(100, false);
    try std.testing.expectEqual(owner_node, redirect);
}

test "shouldMovedRedirect - slot owned by myself allows execution" {
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

    // Assign slot to myself
    cluster.slots[100] = my_node;

    // Should allow execution (no redirect)
    const redirect = cluster.shouldMovedRedirect(100, false);
    try std.testing.expectEqual(null, redirect);
}

test "shouldMovedRedirect - IMPORTING with ASKING allows execution" {
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

    const source_node_id = "fedcba9876543210fedcba9876543210fedcba98".*;

    // Set slot as IMPORTING (not owned by myself)
    try cluster.setSlotImporting(100, source_node_id);

    // Without ASKING flag, should redirect (slot not owned)
    const redirect1 = cluster.shouldMovedRedirect(100, false);
    try std.testing.expectEqual(null, redirect1); // No owner set, returns null

    // With ASKING flag, should allow execution
    const redirect2 = cluster.shouldMovedRedirect(100, true);
    try std.testing.expectEqual(null, redirect2);
}

// ==================== Gossip Protocol Tests ====================

test "Gossip message creation - PING" {
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

    // Create PING message
    const ping_msg = try cluster.createPingMessage(allocator);
    defer allocator.free(ping_msg.gossip_nodes);

    // Verify message type
    try std.testing.expectEqual(GossipMessageType.ping, ping_msg.msg_type);
    // Verify sender is myself
    try std.testing.expectEqualSlices(u8, &node_id, &ping_msg.sender_id);
    // Verify timestamp is set
    try std.testing.expect(ping_msg.timestamp > 0);
}

test "Gossip message creation - PONG" {
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

    // Create PONG message
    const pong_msg = try cluster.createPongMessage(allocator);
    defer allocator.free(pong_msg.gossip_nodes);

    // Verify message type
    try std.testing.expectEqual(GossipMessageType.pong, pong_msg.msg_type);
    // Verify sender is myself
    try std.testing.expectEqualSlices(u8, &node_id, &pong_msg.sender_id);
}

test "Process MEET message - add new node" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup myself
    const my_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    cluster.myself = my_node;

    // Create MEET message from another node
    const other_id = "abcdefabcdefabcdefabcdefabcdefabcdefabcd".*;
    var meet_msg = GossipMessage{
        .msg_type = .meet,
        .sender_id = other_id,
        .sender_addr = "127.0.0.1",
        .sender_port = 7001,
        .timestamp = std.time.milliTimestamp(),
        .config_epoch = 1,
        .gossip_nodes = &[_]GossipNodeInfo{},
    };

    // Process MEET message
    try cluster.processMeetMessage(allocator, &meet_msg);

    // Verify new node was added
    const added_node = cluster.nodes.get(&other_id);
    try std.testing.expect(added_node != null);
    try std.testing.expectEqualSlices(u8, "127.0.0.1", added_node.?.addr);
    try std.testing.expectEqual(@as(u16, 7001), added_node.?.port);
}

test "Process PING message - respond with PONG" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup myself
    const my_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    cluster.myself = my_node;

    // Add another node
    const other_id = "abcdefabcdefabcdefabcdefabcdefabcdefabcd".*;
    const other_node = try allocator.create(ClusterNode);
    other_node.* = try ClusterNode.init(allocator, other_id, "127.0.0.1", 7001);
    const node_key = try allocator.dupe(u8, &other_id);
    try cluster.nodes.put(node_key, other_node);

    // Create PING message
    var ping_msg = GossipMessage{
        .msg_type = .ping,
        .sender_id = other_id,
        .sender_addr = "127.0.0.1",
        .sender_port = 7001,
        .timestamp = std.time.milliTimestamp(),
        .config_epoch = 1,
        .gossip_nodes = &[_]GossipNodeInfo{},
    };

    // Process PING (should return PONG)
    const pong_msg = try cluster.processPingMessage(allocator, &ping_msg);
    defer allocator.free(pong_msg.gossip_nodes);

    // Verify PONG response
    try std.testing.expectEqual(GossipMessageType.pong, pong_msg.msg_type);
    try std.testing.expectEqualSlices(u8, &my_id, &pong_msg.sender_id);
}

test "Process PONG message - update node state" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup myself
    const my_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    cluster.myself = my_node;

    // Add another node with old timestamp
    const other_id = "abcdefabcdefabcdefabcdefabcdefabcdefabcd".*;
    const other_node = try allocator.create(ClusterNode);
    other_node.* = try ClusterNode.init(allocator, other_id, "127.0.0.1", 7001);
    other_node.pong_recv = 1000; // Old timestamp
    const node_key = try allocator.dupe(u8, &other_id);
    try cluster.nodes.put(node_key, other_node);

    const now = std.time.milliTimestamp();

    // Create PONG message
    var pong_msg = GossipMessage{
        .msg_type = .pong,
        .sender_id = other_id,
        .sender_addr = "127.0.0.1",
        .sender_port = 7001,
        .timestamp = now,
        .config_epoch = 1,
        .gossip_nodes = &[_]GossipNodeInfo{},
    };

    // Process PONG
    try cluster.processPongMessage(&pong_msg);

    // Verify pong_recv was updated
    const updated_node = cluster.nodes.get(&other_id).?;
    try std.testing.expect(updated_node.pong_recv > 1000);
}

test "Gossip node discovery - learn about third node" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup myself (Node A)
    const my_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    cluster.myself = my_node;

    // Add Node B
    const node_b_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".*;
    const node_b = try allocator.create(ClusterNode);
    node_b.* = try ClusterNode.init(allocator, node_b_id, "127.0.0.1", 7001);
    const node_b_key = try allocator.dupe(u8, &node_b_id);
    try cluster.nodes.put(node_b_key, node_b);

    // Node B tells Node A about Node C via gossip
    const node_c_id = "cccccccccccccccccccccccccccccccccccccccc".*;
    const node_c_info = GossipNodeInfo{
        .node_id = node_c_id,
        .addr = "127.0.0.1",
        .port = 7002,
        .flags = .{ .master = true },
        .pong_recv = std.time.milliTimestamp(),
    };

    var ping_msg = GossipMessage{
        .msg_type = .ping,
        .sender_id = node_b_id,
        .sender_addr = "127.0.0.1",
        .sender_port = 7001,
        .timestamp = std.time.milliTimestamp(),
        .config_epoch = 1,
        .gossip_nodes = &[_]GossipNodeInfo{node_c_info},
    };

    // Process PING with gossip about Node C
    _ = try cluster.processPingMessage(allocator, &ping_msg);

    // Verify Node A learned about Node C
    const node_c = cluster.nodes.get(&node_c_id);
    try std.testing.expect(node_c != null);
    try std.testing.expectEqualSlices(u8, "127.0.0.1", node_c.?.addr);
    try std.testing.expectEqual(@as(u16, 7002), node_c.?.port);
}

test "Node health detection - mark pfail on missing PONG" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup myself
    const my_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    cluster.myself = my_node;

    // Add another node
    const other_id = "abcdefabcdefabcdefabcdefabcdefabcdefabcd".*;
    const other_node = try allocator.create(ClusterNode);
    other_node.* = try ClusterNode.init(allocator, other_id, "127.0.0.1", 7001);
    const node_key = try allocator.dupe(u8, &other_id);
    try cluster.nodes.put(node_key, other_node);

    // Simulate old ping_sent and no pong_recv (timeout)
    const now = std.time.milliTimestamp();
    other_node.ping_sent = now - 6000; // 6 seconds ago (> 5 sec timeout)
    other_node.pong_recv = 0; // Never received PONG

    // Check health
    cluster.updateNodeHealth();

    // Verify node marked as pfail
    const updated_node = cluster.nodes.get(&other_id).?;
    try std.testing.expect(updated_node.flags.pfail);
}

test "Select random nodes for gossip" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup myself
    const my_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    cluster.myself = my_node;

    // Add 10 nodes
    var i: u8 = 0;
    while (i < 10) : (i += 1) {
        var node_id: [40]u8 = undefined;
        @memset(&node_id, '0' + i);
        const node = try allocator.create(ClusterNode);
        node.* = try ClusterNode.init(allocator, node_id, "127.0.0.1", 7000 + i);
        const node_key = try allocator.dupe(u8, &node_id);
        try cluster.nodes.put(node_key, node);
    }

    // Select 3 random nodes
    const selected = try cluster.selectRandomNodesForGossip(allocator, 3);
    defer allocator.free(selected);

    // Verify we got 3 nodes
    try std.testing.expectEqual(@as(usize, 3), selected.len);

    // Verify they are all different
    try std.testing.expect(!std.mem.eql(u8, &selected[0].node_id, &selected[1].node_id));
    try std.testing.expect(!std.mem.eql(u8, &selected[1].node_id, &selected[2].node_id));
    try std.testing.expect(!std.mem.eql(u8, &selected[0].node_id, &selected[2].node_id));
}

test "Background gossip: periodic PING creation" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup myself
    const my_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    cluster.myself = my_node;

    // Add 5 nodes to the cluster
    var i: u8 = 0;
    while (i < 5) : (i += 1) {
        var node_id: [40]u8 = undefined;
        @memset(&node_id, 'a' + i);
        const node = try allocator.create(ClusterNode);
        node.* = try ClusterNode.init(allocator, node_id, "127.0.0.1", 7001 + i);
        const node_key = try allocator.dupe(u8, &node_id);
        try cluster.nodes.put(node_key, node);
    }

    // Create PING message for gossip (this is what background task does)
    const ping_msg = try cluster.createPingMessage(allocator);
    defer allocator.free(ping_msg.gossip_nodes);

    // Verify PING message structure
    try std.testing.expectEqual(GossipMessageType.ping, ping_msg.msg_type);
    try std.testing.expectEqualSlices(u8, &my_id, &ping_msg.sender_id);
    try std.testing.expect(ping_msg.timestamp > 0);
    try std.testing.expectEqual(@as(u64, 1), ping_msg.config_epoch);
    try std.testing.expect(ping_msg.gossip_nodes.len > 0);
    try std.testing.expect(ping_msg.gossip_nodes.len <= 3); // Max 3 nodes gossiped
}

test "Background gossip: node health monitoring" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup myself
    const my_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    cluster.myself = my_node;

    // Add healthy node
    var healthy_id: [40]u8 = undefined;
    @memset(&healthy_id, 'h');
    const healthy_node = try allocator.create(ClusterNode);
    healthy_node.* = try ClusterNode.init(allocator, healthy_id, "127.0.0.1", 7001);
    const healthy_key = try allocator.dupe(u8, &healthy_id);
    try cluster.nodes.put(healthy_key, healthy_node);

    // Add unhealthy node (timed out)
    var unhealthy_id: [40]u8 = undefined;
    @memset(&unhealthy_id, 'u');
    const unhealthy_node = try allocator.create(ClusterNode);
    unhealthy_node.* = try ClusterNode.init(allocator, unhealthy_id, "127.0.0.1", 7002);
    const unhealthy_key = try allocator.dupe(u8, &unhealthy_id);
    try cluster.nodes.put(unhealthy_key, unhealthy_node);

    // Simulate PING sent to both nodes
    const now = std.time.milliTimestamp();
    healthy_node.ping_sent = now - 1000; // 1 second ago
    healthy_node.pong_recv = now - 500; // PONG received 500ms ago (healthy)

    unhealthy_node.ping_sent = now - 6000; // 6 seconds ago
    unhealthy_node.pong_recv = 0; // No PONG received (unhealthy)

    // Update health status (this is what background task does periodically)
    cluster.updateNodeHealth();

    // Verify healthy node is NOT marked as pfail
    const healthy_check = cluster.nodes.get(&healthy_id).?;
    try std.testing.expect(!healthy_check.flags.pfail);

    // Verify unhealthy node IS marked as pfail
    const unhealthy_check = cluster.nodes.get(&unhealthy_id).?;
    try std.testing.expect(unhealthy_check.flags.pfail);
}

test "Background gossip: PING tracking" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup myself
    const my_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    cluster.myself = my_node;

    // Add target node
    var target_id: [40]u8 = undefined;
    @memset(&target_id, 't');
    const target_node = try allocator.create(ClusterNode);
    target_node.* = try ClusterNode.init(allocator, target_id, "127.0.0.1", 7001);
    const target_key = try allocator.dupe(u8, &target_id);
    try cluster.nodes.put(target_key, target_node);

    // Initially, no PING sent
    try std.testing.expectEqual(@as(i64, 0), target_node.ping_sent);

    // Send PING (this is what background task does)
    cluster.sendPingToNode(target_id);

    // Verify PING timestamp was updated
    const updated_node = cluster.nodes.get(&target_id).?;
    try std.testing.expect(updated_node.ping_sent > 0);
}

test "Background gossip: integration - full cycle" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup myself
    const my_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    cluster.myself = my_node;

    // Add 3 nodes
    var i: u8 = 0;
    while (i < 3) : (i += 1) {
        var node_id: [40]u8 = undefined;
        @memset(&node_id, 'a' + i);
        const node = try allocator.create(ClusterNode);
        node.* = try ClusterNode.init(allocator, node_id, "127.0.0.1", 7001 + i);
        const node_key = try allocator.dupe(u8, &node_id);
        try cluster.nodes.put(node_key, node);
    }

    // Simulate one gossip cycle:
    // 1. Create PING message
    const ping_msg = try cluster.createPingMessage(allocator);
    defer allocator.free(ping_msg.gossip_nodes);

    // 2. "Send" PING to one of the gossiped nodes
    if (ping_msg.gossip_nodes.len > 0) {
        cluster.sendPingToNode(ping_msg.gossip_nodes[0].node_id);
    }

    // 3. Verify PING timestamp was set
    if (ping_msg.gossip_nodes.len > 0) {
        const target_node = cluster.nodes.get(&ping_msg.gossip_nodes[0].node_id);
        try std.testing.expect(target_node != null);
        try std.testing.expect(target_node.?.ping_sent > 0);
    }

    // 4. Update health status
    cluster.updateNodeHealth();

    // Verify system is functional (no crashes, data structures intact)
    try std.testing.expectEqual(@as(usize, 4), cluster.nodes.count()); // myself + 3 nodes
}

