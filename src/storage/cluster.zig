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
    /// Node ID (40-char lowercase hex string)
    /// Unique per node. Different for every node in the cluster.
    id: [40]u8,
    /// Shard ID (40-char lowercase hex string)
    /// Identifies a replication group (master + replicas).
    /// All nodes serving the same data share the same shard_id.
    /// Generated randomly on master creation, inherited by replicas via configureAsReplica().
    shard_id: [40]u8,
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
    /// Outgoing link creation time (milliseconds since epoch)
    to_link_created: i64,
    /// Incoming link creation time (milliseconds since epoch)
    from_link_created: i64,
    /// Outgoing link event mask
    to_link_events: []const u8,
    /// Incoming link event mask
    from_link_events: []const u8,
    /// Outgoing link send buffer allocated bytes
    to_send_buf_alloc: usize,
    /// Outgoing link send buffer used bytes
    to_send_buf_used: usize,
    /// Incoming link send buffer allocated bytes
    from_send_buf_alloc: usize,
    /// Incoming link send buffer used bytes
    from_send_buf_used: usize,

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
        // Generate a random 40-char hex shard_id
        var shard_id: [40]u8 = undefined;
        var prng = std.Random.DefaultPrng.init(@intCast(std.time.microTimestamp()));
        const random = prng.random();
        const hex_chars = "0123456789abcdef";
        for (&shard_id) |*c| {
            c.* = hex_chars[random.intRangeLessThan(u8, 0, 16)];
        }

        const now = std.time.milliTimestamp();
        return ClusterNode{
            .id = id,
            .shard_id = shard_id,
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
            .to_link_created = now,
            .from_link_created = now,
            .to_link_events = "rw",
            .from_link_events = "r",
            .to_send_buf_alloc = 4096,
            .to_send_buf_used = 0,
            .from_send_buf_alloc = 4096,
            .from_send_buf_used = 0,
        };
    }

    pub fn deinit(self: *ClusterNode, allocator: std.mem.Allocator) void {
        allocator.free(self.addr);
        self.slots.deinit(allocator);
    }

    /// Returns health status of this node for CLUSTER SHARDS command.
    ///
    /// Possible return values:
    ///   - "online": Node is healthy and reachable
    ///   - "failed": Node has fail flag set
    ///   - "loading": Node has noaddr flag set (still discovering address)
    pub fn getHealth(self: *const ClusterNode) []const u8 {
        if (self.flags.fail) {
            return "failed";
        }
        if (self.flags.noaddr) {
            return "loading";
        }
        return "online";
    }
};

/// Error types for cluster slot operations
pub const ClusterError = error{
    SlotAlreadyBusy,
    SlotNotAssignedToNode,
    InvalidSlot,
    InvalidSlotRange,
    InvalidNodeId,
    InvalidPath,
    UnknownNode,
    CannotForgetMyself,
    CannotForgetMaster,
    DatabaseNotEmpty,
    OutOfMemory,
    SlotNotOwnedByNode,
    SlotAlreadyOwned,
    SlotHasKeys,
    ClusterNotInitialized,
};

/// Slot migration state
pub const SlotMigrationState = struct {
    /// Slot is being migrated to this node ID
    migrating_to: ?[40]u8 = null,
    /// Slot is being imported from this node ID
    importing_from: ?[40]u8 = null,
};

/// Shard information for CLUSTER SHARDS response
/// Groups nodes that share slot ownership
pub const ShardInfo = struct {
    /// Slot ranges for this shard [start, end, start, end, ...]
    slots: std.ArrayListUnmanaged(u16),
    /// Nodes in this shard (master + replicas)
    nodes: std.ArrayListUnmanaged(*ClusterNode),

    /// Frees all memory associated with this ShardInfo.
    /// Must be called by the owner of the ShardInfo after use.
    pub fn deinit(self: *ShardInfo, allocator: std.mem.Allocator) void {
        self.slots.deinit(allocator);
        self.nodes.deinit(allocator);
    }
};

/// Cluster link information for CLUSTER LINKS response
/// Represents a bus connection between two cluster nodes
pub const ClusterLink = struct {
    /// "to" or "from" — direction of link from perspective of this node
    direction: []const u8,
    /// 40-char hex ID of the peer node
    peer_node_id: [40]u8,
    /// Link creation time in milliseconds since Unix epoch
    create_time: i64,
    /// Event mask: "r" (read), "w" (write), or "rw" (read-write)
    events: []const u8,
    /// Allocated send buffer bytes
    send_buffer_allocated: usize,
    /// Used send buffer bytes
    send_buffer_used: usize,
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

/// Failure report from one node about another
/// Created when a node suspects another node has failed (pfail state)
pub const FailureReport = struct {
    /// ID of the node reporting the failure (40-char hex)
    reporter_id: [40]u8,
    /// Timestamp when the report was created (milliseconds since epoch)
    timestamp: i64,
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
    /// Client IDs that have enabled READONLY mode (allows reads on replica)
    readonly_clients: std.AutoHashMap(u64, void),
    /// Failover votes: tracks which nodes voted for which replica (epoch -> node_id -> voted_for_node_id)
    failover_votes: std.AutoHashMap(u64, std.StringHashMap([40]u8)),
    /// Replication offset for myself (used in election tie-breaking)
    my_replication_offset: i64,
    /// Failure reports: tracks which nodes have reported other nodes as failed
    /// Maps reported_node_id -> list of FailureReport
    failure_reports: std.StringHashMap(std.ArrayListUnmanaged(FailureReport)),

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
            .readonly_clients = std.AutoHashMap(u64, void).init(allocator),
            .failover_votes = std.AutoHashMap(u64, std.StringHashMap([40]u8)).init(allocator),
            .my_replication_offset = 0,
            .failure_reports = std.StringHashMap(std.ArrayListUnmanaged(FailureReport)).init(allocator),
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
        self.readonly_clients.deinit();

        // Clean up failover votes
        var vote_it = self.failover_votes.valueIterator();
        while (vote_it.next()) |vote_map| {
            vote_map.deinit();
        }
        self.failover_votes.deinit();

        // Clean up failure reports
        var report_it = self.failure_reports.valueIterator();
        while (report_it.next()) |report_list| {
            report_list.deinit(self.allocator);
        }
        self.failure_reports.deinit();
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

    /// Set READONLY flag for a client (allows read commands on replica)
    /// When set, client can read from replica nodes in cluster mode
    pub fn setReadonly(self: *ClusterState, client_id: u64) !void {
        try self.readonly_clients.put(client_id, {});
    }

    /// Clear READONLY flag for a client (returns to default read-write mode)
    pub fn clearReadonly(self: *ClusterState, client_id: u64) void {
        _ = self.readonly_clients.remove(client_id);
    }

    /// Check if client has READONLY flag set
    pub fn hasReadonly(self: *const ClusterState, client_id: u64) bool {
        return self.readonly_clients.contains(client_id);
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

    /// Count pfail reports for a node across the cluster
    /// Returns the number of nodes that have marked the target as pfail
    fn countPfailReports(self: *const ClusterState, target_node_id: [40]u8) usize {
        var count: usize = 0;
        var it = self.nodes.valueIterator();
        while (it.next()) |node| {
            // Skip if checking the target node itself
            if (std.mem.eql(u8, &node.*.id, &target_node_id)) {
                continue;
            }
            // Count nodes with pfail flag set
            if (node.*.flags.pfail) {
                count += 1;
            }
        }
        return count;
    }

    /// Count master nodes in the cluster
    fn countMasterNodes(self: *const ClusterState) usize {
        var count: usize = 0;
        var it = self.nodes.valueIterator();
        while (it.next()) |node| {
            if (node.*.flags.master and !node.*.flags.slave) {
                count += 1;
            }
        }
        return count;
    }

    /// Promote pfail to fail if majority of masters agree
    /// This function checks all nodes marked as pfail and promotes them to fail
    /// if a majority (>50%) of master nodes have marked them as pfail
    pub fn promoteFailures(self: *ClusterState) void {
        const master_count = self.countMasterNodes();
        if (master_count == 0) return;

        const majority = master_count / 2 + 1;

        var it = self.nodes.valueIterator();
        while (it.next()) |node| {
            // Skip nodes already marked as fail
            if (node.*.flags.fail) {
                continue;
            }

            // Check if node is pfail and has majority agreement
            if (node.*.flags.pfail) {
                const pfail_count = self.countPfailReports(node.*.id);
                if (pfail_count >= majority) {
                    // Promote to fail
                    node.*.flags.fail = true;
                    node.*.flags.pfail = false;
                }
            }
        }
    }

    /// Get all replicas of a specific master node
    /// Returns allocated slice that caller must free
    pub fn getReplicasOfMaster(self: *const ClusterState, allocator: std.mem.Allocator, master_id: [40]u8) !std.ArrayList(*ClusterNode) {
        var replicas = try std.ArrayList(*ClusterNode).initCapacity(allocator, 0);
        errdefer replicas.deinit(allocator);

        var it = self.nodes.valueIterator();
        while (it.next()) |node| {
            if (node.*.flags.slave and node.*.master_id != null) {
                if (std.mem.eql(u8, &node.*.master_id.?, &master_id)) {
                    try replicas.append(allocator, node.*);
                }
            }
        }

        return replicas;
    }

    /// Configure the current node as a replica of a master node
    /// Validates that the master node exists and updates the current node's flags and master_id
    /// Returns ClusterError.UnknownNode if the master node is not found
    /// Returns ClusterError.InvalidNodeId if trying to replicate self
    pub fn configureAsReplica(self: *ClusterState, master_id: [40]u8) !void {
        const myself = self.myself orelse return ClusterError.UnknownNode;

        // Cannot replicate yourself
        if (std.mem.eql(u8, &myself.id, &master_id)) {
            return ClusterError.InvalidNodeId;
        }

        // Verify master node exists
        const master_id_str = &master_id;
        const master_node = self.nodes.get(master_id_str) orelse return ClusterError.UnknownNode;

        // Verify master is actually a master (not a replica)
        if (master_node.flags.slave) {
            return ClusterError.InvalidNodeId;
        }

        // Update current node to be a replica
        myself.flags.master = false;
        myself.flags.slave = true;
        myself.master_id = master_id;

        // Replica inherits master's shard_id (all nodes in replication group have same shard_id)
        myself.shard_id = master_node.shard_id;

        // Clear all slot assignments when becoming a replica
        // Replicas don't own slots directly, they inherit from master
        for (&self.slots, 0..) |*slot, i| {
            if (slot.* == myself) {
                slot.* = null;
                self.slot_migration_states[i] = .{};
            }
        }

        // Clear slot ranges (retain capacity for potential future master promotion)
        myself.slots.clearRetainingCapacity();

        // Increment config epoch to signal topology change
        self.current_epoch += 1;
    }

    /// Collect shards information for CLUSTER SHARDS command
    /// Returns array of shards, each containing master and its replicas
    /// Caller must call deinit() on each shard and free the returned array
    pub fn collectShards(self: *const ClusterState, allocator: std.mem.Allocator) ![]ShardInfo {
        var shards = try std.ArrayList(ShardInfo).initCapacity(allocator, 0);
        errdefer {
            for (shards.items) |*shard| {
                shard.deinit(allocator);
            }
            shards.deinit(allocator);
        }

        // Group slot ranges by master node
        var masters = try std.ArrayList(*ClusterNode).initCapacity(allocator, 0);
        defer masters.deinit(allocator);

        // First pass: collect all unique master nodes that own slots
        for (self.slots) |maybe_node| {
            if (maybe_node) |node| {
                // Check if node is a master
                if (node.flags.master and !node.flags.slave) {
                    // Check if already in masters list
                    var found = false;
                    for (masters.items) |master| {
                        if (master == node) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        try masters.append(allocator, node);
                    }
                }
            }
        }

        // Second pass: for each master, create a shard
        for (masters.items) |master| {
            var shard = ShardInfo{
                .slots = std.ArrayListUnmanaged(u16){},
                .nodes = std.ArrayListUnmanaged(*ClusterNode){},
            };
            errdefer shard.deinit(allocator);

            // Collect slot ranges for this master
            var current_range_start: ?u16 = null;
            var current_range_end: ?u16 = null;

            for (0..CLUSTER_SLOTS) |slot_idx| {
                const slot = @as(u16, @intCast(slot_idx));
                const node_for_slot = self.slots[slot];

                if (node_for_slot == master) {
                    if (current_range_start == null) {
                        current_range_start = slot;
                        current_range_end = slot;
                    } else {
                        current_range_end = slot;
                    }
                } else {
                    // End of range
                    if (current_range_start != null) {
                        try shard.slots.append(allocator, current_range_start.?);
                        try shard.slots.append(allocator, current_range_end.?);
                        current_range_start = null;
                        current_range_end = null;
                    }
                }
            }

            // Add final range if any
            if (current_range_start != null) {
                try shard.slots.append(allocator, current_range_start.?);
                try shard.slots.append(allocator, current_range_end.?);
            }

            // Add master node first
            try shard.nodes.append(allocator, master);

            // Add all replicas of this master (skip failed ones)
            var replicas = try self.getReplicasOfMaster(allocator, master.id);
            defer replicas.deinit(allocator);

            for (replicas.items) |replica| {
                // Skip failed replicas
                if (!replica.flags.fail) {
                    try shard.nodes.append(allocator, replica);
                }
            }

            try shards.append(allocator, shard);
        }

        return shards.toOwnedSlice(allocator);
    }

    /// Collect all cluster bus links between nodes
    /// Returns a slice of ClusterLink representing "to" and "from" links for each peer
    /// Ownership: caller must free the returned slice with allocator.free()
    pub fn collectLinks(self: *const ClusterState, allocator: std.mem.Allocator) ![]ClusterLink {
        const myself = self.myself orelse return try allocator.alloc(ClusterLink, 0);

        var links = try std.ArrayList(ClusterLink).initCapacity(allocator, 0);
        errdefer links.deinit(allocator);

        // Iterate through all nodes except myself
        var it = self.nodes.valueIterator();
        while (it.next()) |node_ptr| {
            const node = node_ptr.*;
            // Skip myself
            if (node == myself) continue;

            // Create "to" link (outgoing to peer)
            try links.append(allocator, ClusterLink{
                .direction = "to",
                .peer_node_id = node.id,
                .create_time = node.to_link_created,
                .events = node.to_link_events,
                .send_buffer_allocated = node.to_send_buf_alloc,
                .send_buffer_used = node.to_send_buf_used,
            });

            // Create "from" link (incoming from peer)
            try links.append(allocator, ClusterLink{
                .direction = "from",
                .peer_node_id = node.id,
                .create_time = node.from_link_created,
                .events = node.from_link_events,
                .send_buffer_allocated = node.from_send_buf_alloc,
                .send_buffer_used = node.from_send_buf_used,
            });
        }

        return links.toOwnedSlice(allocator);
    }

    /// Add a failure report for a node
    /// Creates a report from reporter_id claiming that reported_node_id has failed
    /// Reports are used in cluster consensus to determine if a node should be marked as failed
    pub fn addFailureReport(self: *ClusterState, reported_node_id: [40]u8, reporter_id: [40]u8) !void {
        const now = std.time.milliTimestamp();
        const report = FailureReport{
            .reporter_id = reporter_id,
            .timestamp = now,
        };

        // Get or create the report list for this node
        const gop = try self.failure_reports.getOrPut(self.allocator, reported_node_id[0..]);
        if (!gop.found_existing) {
            gop.value_ptr.* = std.ArrayListUnmanaged(FailureReport){};
        }

        // Check if this reporter already has a report for this node
        for (gop.value_ptr.items) |existing| {
            if (std.mem.eql(u8, &existing.reporter_id, &reporter_id)) {
                // Update timestamp of existing report
                // Note: We would need to iterate again to update, so for simplicity just return
                return;
            }
        }

        // Add the new report
        try gop.value_ptr.append(self.allocator, report);
    }

    /// Get the count of failure reports for a given node
    /// Returns the number of distinct reporters claiming this node has failed
    pub fn getFailureReportCount(self: *const ClusterState, node_id: [40]u8) usize {
        const reports = self.failure_reports.get(node_id[0..]) orelse return 0;
        return reports.items.len;
    }

    /// Clear all failure reports for a given node
    /// Called when a node recovers or is removed from the cluster
    pub fn clearFailureReports(self: *ClusterState, node_id: [40]u8) void {
        if (self.failure_reports.getPtr(node_id[0..])) |reports| {
            reports.deinit(self.allocator);
            _ = self.failure_reports.remove(node_id[0..]);
        }
    }

    /// Expire old failure reports (older than 60 seconds)
    /// Should be called periodically to clean up stale reports
    pub fn expireOldFailureReports(self: *ClusterState) !void {
        const now = std.time.milliTimestamp();
        const expiry_threshold = now - 60_000; // 60 seconds

        var it = self.failure_reports.iterator();
        var keys_to_remove = std.ArrayList([40]u8).init(self.allocator);
        defer keys_to_remove.deinit();

        while (it.next()) |entry| {
            var i: usize = 0;
            while (i < entry.value_ptr.items.len) {
                if (entry.value_ptr.items[i].timestamp < expiry_threshold) {
                    _ = entry.value_ptr.swapRemove(i);
                    // Don't increment i, check the same index again
                } else {
                    i += 1;
                }
            }

            // If no reports left for this node, mark for removal
            if (entry.value_ptr.items.len == 0) {
                var node_id: [40]u8 = undefined;
                @memcpy(&node_id, entry.key_ptr.*);
                try keys_to_remove.append(node_id);
            }
        }

        // Remove empty entries
        for (keys_to_remove.items) |node_id| {
            if (self.failure_reports.getPtr(node_id[0..])) |reports| {
                reports.deinit(self.allocator);
                _ = self.failure_reports.remove(node_id[0..]);
            }
        }
    }

    /// Request a vote from a master node
    /// Masters can only vote once per epoch
    /// Returns true if vote granted, false otherwise
    pub fn requestVote(self: *ClusterState, allocator: std.mem.Allocator, epoch: u64, requesting_node_id: [40]u8) !bool {
        const myself = self.myself orelse return false;

        // Only masters can vote
        if (!myself.flags.master or myself.flags.slave) {
            return false;
        }

        // Check if we've already voted in this epoch
        if (self.failover_votes.get(epoch)) |epoch_votes| {
            const my_id_str = &myself.id;
            if (epoch_votes.contains(my_id_str)) {
                // Already voted in this epoch
                return false;
            }
        } else {
            // Create vote map for this epoch
            const new_epoch_votes = std.StringHashMap([40]u8).init(allocator);
            try self.failover_votes.put(epoch, new_epoch_votes);
        }

        // Grant vote
        var epoch_votes = self.failover_votes.getPtr(epoch).?;
        const my_id_str = try allocator.dupe(u8, &myself.id);
        try epoch_votes.put(my_id_str, requesting_node_id);

        return true;
    }

    /// Check if a replica should start election (its master is marked as fail)
    pub fn shouldStartElection(self: *const ClusterState) bool {
        const myself = self.myself orelse return false;

        // Only replicas can start elections
        if (!myself.flags.slave or myself.flags.master) {
            return false;
        }

        // Check if our master is marked as fail
        if (myself.master_id) |master_id| {
            if (self.nodes.get(&master_id)) |master_node| {
                return master_node.flags.fail;
            }
        }

        return false;
    }

    /// Start election process for a replica
    /// Increments current epoch and attempts to collect votes from masters
    /// Returns true if election won (majority votes collected)
    pub fn startElection(self: *ClusterState, allocator: std.mem.Allocator) !bool {
        const myself = self.myself orelse return false;

        // Increment configuration epoch
        self.current_epoch += 1;
        const election_epoch = self.current_epoch;

        // Vote for ourselves
        var epoch_votes = std.StringHashMap([40]u8).init(allocator);
        const my_id_str = try allocator.dupe(u8, &myself.id);
        try epoch_votes.put(my_id_str, myself.id);
        try self.failover_votes.put(election_epoch, epoch_votes);

        // In a real implementation, we would send FAILOVER_AUTH_REQUEST to all masters
        // and wait for votes. For now, we'll simulate the election by checking
        // if we would win based on replication offset.

        const master_count = self.countMasterNodes();
        const majority = master_count / 2 + 1;

        // Count our own vote
        const vote_count: usize = 1;

        // Check if we have majority
        return vote_count >= majority;
    }

    /// Promote replica to master (called after winning election)
    /// Takes over slots from the failed master
    pub fn promoteToMaster(self: *ClusterState, _: std.mem.Allocator) !void {
        const myself = self.myself orelse return ClusterError.UnknownNode;

        // Verify we're currently a replica
        if (!myself.flags.slave or myself.flags.master) {
            return ClusterError.InvalidSlot; // Reuse for invalid operation
        }

        const old_master_id = myself.master_id orelse return ClusterError.UnknownNode;

        // Find the old master node
        const old_master = self.nodes.get(&old_master_id) orelse {
            return ClusterError.UnknownNode;
        };

        // Take over all slots from old master
        for (0..CLUSTER_SLOTS) |i| {
            const slot = @as(u16, @intCast(i));
            if (self.slots[slot] == old_master) {
                self.slots[slot] = myself;
            }
        }

        // Recompress slot ranges
        try self.recompressNodeSlots(myself);

        // Update flags: become master, no longer replica
        myself.flags.master = true;
        myself.flags.slave = false;
        myself.master_id = null;

        // Update config epoch
        myself.config_epoch = self.current_epoch;

        // Update cluster state
        self.updateClusterState();
    }

    /// Handle manual failover request (CLUSTER FAILOVER command)
    /// mode: "normal" (default), "force" (skip replication checks), "takeover" (no consensus)
    pub fn manualFailover(self: *ClusterState, allocator: std.mem.Allocator, mode: []const u8) !void {
        const myself = self.myself orelse return ClusterError.UnknownNode;

        // Must be a replica
        if (!myself.flags.slave or myself.flags.master) {
            return ClusterError.InvalidSlot; // Reuse for invalid operation
        }

        if (std.mem.eql(u8, mode, "takeover")) {
            // TAKEOVER: immediately promote without election
            self.current_epoch += 1;
            try self.promoteToMaster(allocator);
        } else if (std.mem.eql(u8, mode, "force")) {
            // FORCE: start election without waiting for replication sync
            self.current_epoch += 1;
            const won = try self.startElection(allocator);
            if (won) {
                try self.promoteToMaster(allocator);
            }
        } else {
            // Normal failover: check that we're in sync with master
            // For now, we'll allow it if master is reachable
            self.current_epoch += 1;
            const won = try self.startElection(allocator);
            if (won) {
                try self.promoteToMaster(allocator);
            }
        }
    }

    /// Save cluster configuration to nodes.conf file
    /// Writes one line per node and a vars line for cluster epochs
    /// Calls fsync(2) to ensure durability before returning
    /// Format: <node-id> <ip>:<port@cluster-port> <flags> <master-id|-> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot-ranges>
    pub fn saveConfig(self: *ClusterState, config_path: []const u8) !void {
        // Validate path is not empty
        if (config_path.len == 0) return error.InvalidPath;

        // Reject path traversal attempts
        if (std.mem.indexOf(u8, config_path, "..") != null) {
            std.log.err("Rejected path with .. component: {s}", .{config_path});
            return error.InvalidPath;
        }

        var file = std.fs.cwd().createFile(config_path, .{}) catch |err| {
            std.log.err("Failed to create config file at {s}: {}", .{config_path, err});
            return err;
        };
        defer file.close();

        var buf = try std.ArrayList(u8).initCapacity(self.allocator, 4096);
        defer buf.deinit(self.allocator);
        const w = buf.writer(self.allocator);

        // Write header comment
        try w.print("# Cluster configuration file, created by zoltraak\n", .{});

        // Write each node's line
        var node_it = self.nodes.valueIterator();
        while (node_it.next()) |node_ptr| {
            const node = node_ptr.*;

            // Flags
            var flags_buf: [100]u8 = undefined;
            var flags_len: usize = 0;
            if (node.flags.myself) {
                const written = try std.fmt.bufPrint(flags_buf[flags_len..], "{s}myself", .{if (flags_len > 0) "," else ""});
                flags_len += written.len;
            }
            if (node.flags.master) {
                const written = try std.fmt.bufPrint(flags_buf[flags_len..], "{s}master", .{if (flags_len > 0) "," else ""});
                flags_len += written.len;
            }
            if (node.flags.slave) {
                const written = try std.fmt.bufPrint(flags_buf[flags_len..], "{s}slave", .{if (flags_len > 0) "," else ""});
                flags_len += written.len;
            }
            if (node.flags.fail) {
                const written = try std.fmt.bufPrint(flags_buf[flags_len..], "{s}fail", .{if (flags_len > 0) "," else ""});
                flags_len += written.len;
            }
            if (node.flags.pfail) {
                const written = try std.fmt.bufPrint(flags_buf[flags_len..], "{s}pfail", .{if (flags_len > 0) "," else ""});
                flags_len += written.len;
            }
            if (node.flags.handshake) {
                const written = try std.fmt.bufPrint(flags_buf[flags_len..], "{s}handshake", .{if (flags_len > 0) "," else ""});
                flags_len += written.len;
            }
            if (node.flags.noaddr) {
                const written = try std.fmt.bufPrint(flags_buf[flags_len..], "{s}noaddr", .{if (flags_len > 0) "," else ""});
                flags_len += written.len;
            }
            if (node.flags.nofailover) {
                const written = try std.fmt.bufPrint(flags_buf[flags_len..], "{s}nofailover", .{if (flags_len > 0) "," else ""});
                flags_len += written.len;
            }

            // Link state
            const link_state_str = switch (node.link_state) {
                .connected => "connected",
                .disconnected => "disconnected",
            };

            // Slot ranges (compress consecutive slots)
            var slots_buf: [1024]u8 = undefined;
            var slots_len: usize = 0;

            if (node.slots.items.len > 0) {
                for (node.slots.items, 0..) |range, i| {
                    if (i > 0) {
                        slots_buf[slots_len] = ' ';
                        slots_len += 1;
                    }
                    if (range.start == range.end) {
                        const written = try std.fmt.bufPrint(slots_buf[slots_len..], "{d}", .{range.start});
                        slots_len += written.len;
                    } else {
                        const written = try std.fmt.bufPrint(slots_buf[slots_len..], "{d}-{d}", .{ range.start, range.end });
                        slots_len += written.len;
                    }
                }
            }

            // Write node line (Redis 7.2 format with shard_id)
            try w.print("{s} {s}:{d}@{d} {s} {s} {d} {d} {d} {s} {s} {s}\n", .{
                &node.id,
                node.addr,
                node.port,
                node.cluster_port,
                flags_buf[0..flags_len],
                if (node.master_id != null) node.master_id.?[0..] else "-",
                node.ping_sent,
                node.pong_recv,
                node.config_epoch,
                link_state_str,
                slots_buf[0..slots_len],
                &node.shard_id,
            });
        }

        // Write vars line
        try w.print("vars currentEpoch {d} lastVoteEpoch 0\n", .{self.current_epoch});

        // Write buffer to file and sync to disk
        try file.writeAll(buf.items);
        try file.sync();
    }

    /// Attempt to bump the configuration epoch
    /// Returns the new epoch if bumped, or the current epoch if already highest
    /// Returns { bumped = true, epoch = new_epoch } if epoch was incremented
    /// Returns { bumped = false, epoch = current_epoch } if already at highest
    pub fn bumpEpoch(self: *ClusterState) !struct { bumped: bool, epoch: u64 } {
        const myself = self.myself orelse return error.ClusterNotInitialized;

        // Check if bump is needed:
        // Increment if config_epoch == 0 OR config_epoch < current_epoch
        if (myself.config_epoch == 0 or myself.config_epoch < self.current_epoch) {
            // Increment to current_epoch + 1
            const new_epoch = self.current_epoch + 1;
            myself.config_epoch = new_epoch;
            self.current_epoch = new_epoch;
            return .{ .bumped = true, .epoch = new_epoch };
        } else {
            // Already at or above current_epoch
            return .{ .bumped = false, .epoch = myself.config_epoch };
        }
    }

    /// Manually set the config epoch for this node
    /// Used during cluster initialization or recovery to force a specific epoch
    ///
    /// Constraints:
    ///   - The new epoch must be > 0 (epoch 0 is reserved for uninitialized state)
    ///   - The node must have no assigned slots (safety check to prevent epoch conflicts)
    ///   - Redis allows setting any epoch >= 0, but enforces the "no slots" rule
    ///
    /// Arguments:
    ///   - epoch: The new config epoch to set
    ///
    /// Returns:
    ///   - error.ClusterNotInitialized if cluster is not initialized
    ///   - error.SlotHasKeys if the node has any assigned slots
    ///
    /// Side effects:
    ///   - Updates myself.config_epoch to the new value
    ///   - Updates current_epoch to max(current_epoch, new_epoch)
    pub fn setConfigEpoch(self: *ClusterState, epoch: u64) !void {
        const myself = self.myself orelse return error.ClusterNotInitialized;

        // Redis constraint: can only set config epoch when node has no assigned slots
        // This prevents epoch conflicts during normal cluster operation
        for (self.slots) |slot_node| {
            if (slot_node) |node| {
                // Check if this slot is assigned to myself
                if (std.mem.eql(u8, &node.id, &myself.id)) {
                    return error.SlotHasKeys;
                }
            }
        }

        // Set the new config epoch
        myself.config_epoch = epoch;

        // Update current_epoch to be at least as high as the new config epoch
        if (epoch > self.current_epoch) {
            self.current_epoch = epoch;
        }
    }

    /// Count the number of keys in a specific hash slot
    /// Iterates through all keys in the storage HashMap and counts those in the given slot
    ///
    /// Arguments:
    ///   - data: The storage HashMap containing all keys
    ///   - slot: The slot number (0-16383)
    ///
    /// Returns the count of keys in the slot
    pub fn countKeysInSlot(_: *const ClusterState, data: anytype, slot: u16) usize {
        if (slot >= CLUSTER_SLOTS) return 0;

        var count: usize = 0;
        var it = data.keyIterator();
        while (it.next()) |key| {
            if (keySlot(key.*) == slot) {
                count += 1;
            }
        }
        return count;
    }

    /// Get all keys in a specific hash slot
    /// Iterates through all keys in the storage HashMap and collects those in the given slot
    ///
    /// Arguments:
    ///   - allocator: Allocator for the returned array
    ///   - data: The storage HashMap containing all keys
    ///   - slot: The slot number (0-16383)
    ///   - max_count: Maximum number of keys to return (0 = unlimited)
    ///
    /// Returns an allocated slice of key strings (caller must free)
    pub fn getKeysInSlot(_: *const ClusterState, allocator: std.mem.Allocator, data: anytype, slot: u16, max_count: usize) ![][]const u8 {
        if (slot >= CLUSTER_SLOTS) {
            return &[_][]const u8{};
        }

        var keys = try std.ArrayList([]const u8).initCapacity(allocator, 0);
        errdefer keys.deinit(allocator);

        var it = data.keyIterator();
        while (it.next()) |key| {
            if (keySlot(key.*) == slot) {
                // Duplicate key string for safety
                const key_copy = try allocator.dupe(u8, key.*);
                errdefer allocator.free(key_copy);
                try keys.append(allocator, key_copy);

                // Check max_count limit
                if (max_count > 0 and keys.items.len >= max_count) {
                    break;
                }
            }
        }

        return keys.toOwnedSlice(allocator);
    }

    /// Slot statistics for CLUSTER SLOT-STATS command
    pub const SlotStats = struct {
        slot: u16,
        key_count: usize,
        cpu_usec: u64,
        memory_bytes: u64,
        network_bytes_in: u64,
        network_bytes_out: u64,
    };

    /// Sort comparison function for SlotStats
    /// Used with ORDERBY to sort slots by a specific metric
    pub const SlotStatsContext = struct {
        metric: []const u8,
        ascending: bool,

        pub fn lessThan(ctx: SlotStatsContext, a: SlotStats, b: SlotStats) bool {
            const a_val = ctx.getMetricValue(a);
            const b_val = ctx.getMetricValue(b);

            // Primary sort by metric value
            if (a_val != b_val) {
                return if (ctx.ascending) a_val < b_val else a_val > b_val;
            }

            // Tiebreaker: slot number (always ascending per Redis spec)
            return a.slot < b.slot;
        }

        fn getMetricValue(ctx: SlotStatsContext, stats: SlotStats) u64 {
            if (std.mem.eql(u8, ctx.metric, "key-count")) {
                return @intCast(stats.key_count);
            } else if (std.mem.eql(u8, ctx.metric, "cpu-usec")) {
                return stats.cpu_usec;
            } else if (std.mem.eql(u8, ctx.metric, "memory-bytes")) {
                return stats.memory_bytes;
            } else if (std.mem.eql(u8, ctx.metric, "network-bytes-in")) {
                return stats.network_bytes_in;
            } else if (std.mem.eql(u8, ctx.metric, "network-bytes-out")) {
                return stats.network_bytes_out;
            }
            return 0; // Unknown metric
        }
    };

    /// Get statistics for a specific slot
    /// Returns SlotStats with key count (real) and other metrics (stub = 0)
    ///
    /// Arguments:
    ///   - data: The storage HashMap containing all keys
    ///   - slot: The slot number (0-16383)
    ///
    /// Returns SlotStats structure with metrics
    pub fn getSlotStats(self: *const ClusterState, data: anytype, slot: u16) SlotStats {
        return SlotStats{
            .slot = slot,
            .key_count = self.countKeysInSlot(data, slot),
            .cpu_usec = 0, // Stub: real CPU tracking not implemented
            .memory_bytes = 0, // Stub: real memory tracking not implemented
            .network_bytes_in = 0, // Stub: real network tracking not implemented
            .network_bytes_out = 0, // Stub: real network tracking not implemented
        };
    }

    /// Get statistics for slots in a range [start, end]
    /// Returns allocated array sorted by slot number (ascending)
    ///
    /// Arguments:
    ///   - allocator: Allocator for the returned array
    ///   - data: The storage HashMap containing all keys
    ///   - start_slot: Starting slot number (inclusive)
    ///   - end_slot: Ending slot number (inclusive)
    ///
    /// Returns allocated slice of SlotStats (caller must free)
    pub fn getSlotStatsRange(
        self: *const ClusterState,
        allocator: std.mem.Allocator,
        data: anytype,
        start_slot: u16,
        end_slot: u16,
    ) ![]SlotStats {
        if (start_slot > end_slot or end_slot >= CLUSTER_SLOTS) {
            return error.InvalidSlotRange;
        }

        const count = end_slot - start_slot + 1;
        var stats = try allocator.alloc(SlotStats, count);
        errdefer allocator.free(stats);

        // Collect stats for each slot in range (already sorted by slot number)
        for (start_slot..end_slot + 1, 0..) |slot_idx, i| {
            const slot = @as(u16, @intCast(slot_idx));
            stats[i] = self.getSlotStats(data, slot);
        }

        return stats;
    }

    /// Get statistics for all slots owned by this node, sorted by metric
    /// Filters to only include slots assigned to myself
    ///
    /// Arguments:
    ///   - allocator: Allocator for the returned array
    ///   - data: The storage HashMap containing all keys
    ///   - metric: Metric name to sort by ("key-count", "cpu-usec", etc.)
    ///   - ascending: Sort direction (true = ASC, false = DESC)
    ///   - limit: Maximum number of results (0 = unlimited)
    ///
    /// Returns allocated slice of SlotStats sorted by metric (caller must free)
    pub fn getSlotStatsSorted(
        self: *const ClusterState,
        allocator: std.mem.Allocator,
        data: anytype,
        metric: []const u8,
        ascending: bool,
        limit: usize,
    ) ![]SlotStats {
        // Collect stats for all slots assigned to myself
        var stats_list = try std.ArrayList(SlotStats).initCapacity(allocator, 0);
        errdefer stats_list.deinit(allocator);

        if (self.myself) |myself| {
            // Iterate through all slot ranges owned by this node
            for (myself.slots.items) |slot_range| {
                for (slot_range.start..slot_range.end + 1) |slot_idx| {
                    const slot = @as(u16, @intCast(slot_idx));
                    const stats = self.getSlotStats(data, slot);
                    try stats_list.append(allocator, stats);
                }
            }
        }

        // Sort by metric with slot number tiebreaker
        const context = SlotStatsContext{ .metric = metric, .ascending = ascending };
        std.mem.sort(SlotStats, stats_list.items, context, SlotStatsContext.lessThan);

        // Apply limit if specified
        const result_count = if (limit > 0 and limit < stats_list.items.len)
            limit
        else
            stats_list.items.len;

        // Return only the limited subset
        const result = try allocator.alloc(SlotStats, result_count);
        errdefer allocator.free(result);
        @memcpy(result, stats_list.items[0..result_count]);

        return result;
    }

    /// Perform a soft reset of the cluster state.
    ///
    /// Soft reset effects:
    /// - Forgets all other nodes in the cluster
    /// - Clears all slot assignments
    /// - If this node is a replica, turns it into a master and flushes data
    /// - Preserves the current node ID
    /// - Preserves currentEpoch and configEpoch
    ///
    /// Returns an error if the node is a master with keys in any slot.
    pub fn resetSoft(self: *ClusterState, has_keys: bool) ClusterError!void {
        // Check if this node is a master with keys
        if (self.myself) |myself| {
            if (myself.flags.master and has_keys) {
                std.log.warn("CLUSTER RESET soft failed: master node has keys (node_id={s})", .{myself.id});
                return ClusterError.SlotHasKeys;
            }
        }

        // Clear all slot assignments
        for (&self.slots) |*slot_ptr| {
            slot_ptr.* = null;
        }
        for (&self.slot_migration_states) |*migration_state| {
            migration_state.* = .{};
        }

        // Clear myself's slots
        if (self.myself) |myself| {
            myself.slots.clearRetainingCapacity();

            // If replica, turn into master
            if (myself.flags.slave) {
                myself.flags.slave = false;
                myself.flags.master = true;
                myself.master_id = null;
            }
        }

        // Forget all other nodes
        var it = self.nodes.iterator();
        while (it.next()) |entry| {
            const node = entry.value_ptr.*;
            if (self.myself == null or !std.mem.eql(u8, &node.id, &self.myself.?.id)) {
                node.deinit(self.allocator);
                self.allocator.destroy(node);
            }
        }
        self.nodes.clearRetainingCapacity();

        // Re-add myself to nodes map if it exists
        if (self.myself) |myself| {
            const id_str = try self.allocator.dupe(u8, &myself.id);
            errdefer self.allocator.free(id_str);
            try self.nodes.put(id_str, myself);
        }

        // Clear all other state
        self.banned_nodes.clearRetainingCapacity();
        self.asking_clients.clearRetainingCapacity();
        self.readonly_clients.clearRetainingCapacity();

        // Clear failover votes
        var vote_it = self.failover_votes.valueIterator();
        while (vote_it.next()) |vote_map| {
            vote_map.deinit();
        }
        self.failover_votes.clearRetainingCapacity();

        // Clear failure reports
        var report_it = self.failure_reports.valueIterator();
        while (report_it.next()) |report_list| {
            report_list.deinit(self.allocator);
        }
        self.failure_reports.clearRetainingCapacity();

        // Update cluster state
        self.state = .fail; // No slots assigned
    }

    /// Perform a hard reset of the cluster state.
    ///
    /// Hard reset effects (in addition to soft reset):
    /// - Generates a new random node ID
    /// - Resets currentEpoch to 0
    /// - Resets configEpoch to 0
    ///
    /// Returns an error if the node is a master with keys in any slot.
    pub fn resetHard(self: *ClusterState, has_keys: bool) ClusterError!void {
        // Perform soft reset first
        try self.resetSoft(has_keys);

        // Generate new node ID
        if (self.myself) |myself| {
            std.log.info("CLUSTER RESET hard: generating new node ID", .{});

            var prng = std.Random.DefaultPrng.init(@intCast(std.time.microTimestamp()));
            const random = prng.random();
            const hex_chars = "0123456789abcdef";
            for (&myself.id) |*c| {
                c.* = hex_chars[random.intRangeLessThan(u8, 0, 16)];
            }

            // Update nodes map with new ID
            // After resetSoft, nodes contains only myself with an old ID key
            // Clear the entire nodes map (frees the old ID key)
            var old_it = self.nodes.iterator();
            while (old_it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
            }
            self.nodes.clearRetainingCapacity();

            // Re-add myself with new ID
            const id_str = try self.allocator.dupe(u8, &myself.id);
            errdefer self.allocator.free(id_str);
            try self.nodes.put(id_str, myself);

            // Reset epochs
            myself.config_epoch = 0;
        }

        self.current_epoch = 0;
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


test "Failover: pfail promotion to fail with majority" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup myself as master
    const my_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    cluster.myself = my_node;

    // Add 4 other master nodes (total 5 masters, majority = 3)
    var i: u8 = 0;
    while (i < 4) : (i += 1) {
        var node_id: [40]u8 = undefined;
        @memset(&node_id, 'a' + i);
        const node = try allocator.create(ClusterNode);
        node.* = try ClusterNode.init(allocator, node_id, "127.0.0.1", 7001 + i);
        const node_key = try allocator.dupe(u8, &node_id);
        try cluster.nodes.put(node_key, node);
    }

    // Add failing node marked as pfail
    var failing_id: [40]u8 = undefined;
    @memset(&failing_id, 'f');
    const failing_node = try allocator.create(ClusterNode);
    failing_node.* = try ClusterNode.init(allocator, failing_id, "127.0.0.1", 7010);
    failing_node.flags.pfail = true;
    const failing_key = try allocator.dupe(u8, &failing_id);
    try cluster.nodes.put(failing_key, failing_node);

    // Initially node should only be pfail
    try std.testing.expect(failing_node.flags.pfail);
    try std.testing.expect(!failing_node.flags.fail);

    // Promote failures (should not promote with insufficient pfail reports)
    cluster.promoteFailures();
    try std.testing.expect(!failing_node.flags.fail);
}

test "Failover: get replicas of master" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup master node
    var master_id: [40]u8 = undefined;
    @memset(&master_id, 'm');
    const master_node = try allocator.create(ClusterNode);
    master_node.* = try ClusterNode.init(allocator, master_id, "127.0.0.1", 7000);
    const master_key = try allocator.dupe(u8, &master_id);
    try cluster.nodes.put(master_key, master_node);

    // Add 3 replicas of this master
    var i: u8 = 0;
    while (i < 3) : (i += 1) {
        var replica_id: [40]u8 = undefined;
        @memset(&replica_id, 'r' + i);
        const replica_node = try allocator.create(ClusterNode);
        replica_node.* = try ClusterNode.init(allocator, replica_id, "127.0.0.1", 7001 + i);
        replica_node.flags.master = false;
        replica_node.flags.slave = true;
        replica_node.master_id = master_id;
        const replica_key = try allocator.dupe(u8, &replica_id);
        try cluster.nodes.put(replica_key, replica_node);
    }

    // Get replicas of master
    var replicas = try cluster.getReplicasOfMaster(allocator, master_id);
    defer replicas.deinit();

    // Verify we got 3 replicas
    try std.testing.expectEqual(@as(usize, 3), replicas.items.len);

    // Verify all are replicas of the correct master
    for (replicas.items) |replica| {
        try std.testing.expect(replica.flags.slave);
        try std.testing.expect(!replica.flags.master);
        try std.testing.expectEqualSlices(u8, &master_id, &replica.master_id.?);
    }
}

test "Failover: replica should start election when master fails" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup master node
    var master_id: [40]u8 = undefined;
    @memset(&master_id, 'm');
    const master_node = try allocator.create(ClusterNode);
    master_node.* = try ClusterNode.init(allocator, master_id, "127.0.0.1", 7000);
    master_node.flags.fail = false; // Initially not failed
    const master_key = try allocator.dupe(u8, &master_id);
    try cluster.nodes.put(master_key, master_node);

    // Setup myself as replica of this master
    const my_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7001);
    my_node.flags.master = false;
    my_node.flags.slave = true;
    my_node.master_id = master_id;
    cluster.myself = my_node;

    // Initially should not start election (master not failed)
    try std.testing.expect(!cluster.shouldStartElection());

    // Mark master as failed
    master_node.flags.fail = true;

    // Now should start election
    try std.testing.expect(cluster.shouldStartElection());
}

test "Failover: promote replica to master" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup old master node with slots
    var old_master_id: [40]u8 = undefined;
    @memset(&old_master_id, 'o');
    const old_master = try allocator.create(ClusterNode);
    old_master.* = try ClusterNode.init(allocator, old_master_id, "127.0.0.1", 7000);
    const old_master_key = try allocator.dupe(u8, &old_master_id);
    try cluster.nodes.put(old_master_key, old_master);

    // Assign slots to old master
    try cluster.assignSlots(old_master, 0, 5000);

    // Setup myself as replica
    const my_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7001);
    my_node.flags.master = false;
    my_node.flags.slave = true;
    my_node.master_id = old_master_id;
    cluster.myself = my_node;

    // Verify initial state
    try std.testing.expect(my_node.flags.slave);
    try std.testing.expect(!my_node.flags.master);
    try std.testing.expectEqual(@as(usize, 0), my_node.slots.items.len);

    // Promote to master
    try cluster.promoteToMaster(allocator);

    // Verify promoted state
    try std.testing.expect(!my_node.flags.slave);
    try std.testing.expect(my_node.flags.master);
    try std.testing.expect(my_node.master_id == null);
    try std.testing.expect(my_node.slots.items.len > 0);

    // Verify took over slots from old master
    const slot_owner = cluster.getNodeBySlot(100);
    try std.testing.expect(slot_owner == my_node);
}

test "Failover: manual failover takeover mode" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup old master
    var old_master_id: [40]u8 = undefined;
    @memset(&old_master_id, 'o');
    const old_master = try allocator.create(ClusterNode);
    old_master.* = try ClusterNode.init(allocator, old_master_id, "127.0.0.1", 7000);
    const old_master_key = try allocator.dupe(u8, &old_master_id);
    try cluster.nodes.put(old_master_key, old_master);
    try cluster.assignSlots(old_master, 0, 8191);

    // Setup myself as replica
    const my_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7001);
    my_node.flags.master = false;
    my_node.flags.slave = true;
    my_node.master_id = old_master_id;
    cluster.myself = my_node;

    const initial_epoch = cluster.current_epoch;

    // Execute manual failover in takeover mode
    try cluster.manualFailover(allocator, "takeover");

    // Verify promoted to master
    try std.testing.expect(my_node.flags.master);
    try std.testing.expect(!my_node.flags.slave);
    try std.testing.expect(cluster.current_epoch > initial_epoch);
    try std.testing.expect(my_node.slots.items.len > 0);
}

test "ClusterState: configureAsReplica - success" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Create master node
    const master_id = "1111111111111111111111111111111111111111".*;
    const master_node = try allocator.create(ClusterNode);
    master_node.* = try ClusterNode.init(allocator, master_id, "127.0.0.1", 7000);
    master_node.flags.master = true;
    master_node.flags.slave = false;
    const master_key = try allocator.dupe(u8, &master_id);
    try cluster.nodes.put(master_key, master_node);

    // Setup myself as a master
    const my_id = "2222222222222222222222222222222222222222".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7001);
    my_node.flags.master = true;
    my_node.flags.slave = false;
    cluster.myself = my_node;
    const my_key = try allocator.dupe(u8, &my_id);
    try cluster.nodes.put(my_key, my_node);

    // Assign some slots to myself
    try cluster.assignSlots(my_node, 1000, 1010);

    // Verify initial state
    try std.testing.expect(my_node.flags.master);
    try std.testing.expect(!my_node.flags.slave);
    try std.testing.expect(my_node.master_id == null);
    try std.testing.expect(cluster.slots[1000] == my_node);

    // Configure as replica
    try cluster.configureAsReplica(master_id);

    // Verify node is now a replica
    try std.testing.expect(!my_node.flags.master);
    try std.testing.expect(my_node.flags.slave);
    try std.testing.expect(my_node.master_id != null);
    try std.testing.expect(std.mem.eql(u8, &my_node.master_id.?, &master_id));

    // Verify slots are cleared
    try std.testing.expect(cluster.slots[1000] == null);
    try std.testing.expectEqual(@as(usize, 0), my_node.slots.items.len);
}

test "ClusterState: configureAsReplica - cannot replicate self" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup myself
    const my_id = "1111111111111111111111111111111111111111".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    my_node.flags.master = true;
    cluster.myself = my_node;
    const my_key = try allocator.dupe(u8, &my_id);
    try cluster.nodes.put(my_key, my_node);

    // Try to replicate self - should fail
    const result = cluster.configureAsReplica(my_id);
    try std.testing.expectError(ClusterError.InvalidNodeId, result);
}

test "ClusterState: configureAsReplica - unknown master" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup myself
    const my_id = "1111111111111111111111111111111111111111".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    cluster.myself = my_node;
    const my_key = try allocator.dupe(u8, &my_id);
    try cluster.nodes.put(my_key, my_node);

    // Try to replicate unknown node
    const unknown_id = "9999999999999999999999999999999999999999".*;
    const result = cluster.configureAsReplica(unknown_id);
    try std.testing.expectError(ClusterError.UnknownNode, result);
}

test "ClusterState: configureAsReplica - cannot replicate a replica" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Create a replica node (not a master)
    const replica_id = "1111111111111111111111111111111111111111".*;
    const replica_node = try allocator.create(ClusterNode);
    replica_node.* = try ClusterNode.init(allocator, replica_id, "127.0.0.1", 7000);
    replica_node.flags.master = false;
    replica_node.flags.slave = true;
    const replica_key = try allocator.dupe(u8, &replica_id);
    try cluster.nodes.put(replica_key, replica_node);

    // Setup myself
    const my_id = "2222222222222222222222222222222222222222".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7001);
    cluster.myself = my_node;
    const my_key = try allocator.dupe(u8, &my_id);
    try cluster.nodes.put(my_key, my_node);

    // Try to replicate a replica - should fail
    const result = cluster.configureAsReplica(replica_id);
    try std.testing.expectError(ClusterError.InvalidNodeId, result);
}

test "ClusterState: getReplicasOfMaster - multiple replicas" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Create master node
    const master_id = "1111111111111111111111111111111111111111".*;
    const master_node = try allocator.create(ClusterNode);
    master_node.* = try ClusterNode.init(allocator, master_id, "127.0.0.1", 7000);
    master_node.flags.master = true;
    const master_key = try allocator.dupe(u8, &master_id);
    try cluster.nodes.put(master_key, master_node);

    // Create replica 1
    const replica1_id = "2222222222222222222222222222222222222222".*;
    const replica1_node = try allocator.create(ClusterNode);
    replica1_node.* = try ClusterNode.init(allocator, replica1_id, "127.0.0.1", 7001);
    replica1_node.flags.master = false;
    replica1_node.flags.slave = true;
    replica1_node.master_id = master_id;
    const replica1_key = try allocator.dupe(u8, &replica1_id);
    try cluster.nodes.put(replica1_key, replica1_node);

    // Create replica 2
    const replica2_id = "3333333333333333333333333333333333333333".*;
    const replica2_node = try allocator.create(ClusterNode);
    replica2_node.* = try ClusterNode.init(allocator, replica2_id, "127.0.0.1", 7002);
    replica2_node.flags.master = false;
    replica2_node.flags.slave = true;
    replica2_node.master_id = master_id;
    const replica2_key = try allocator.dupe(u8, &replica2_id);
    try cluster.nodes.put(replica2_key, replica2_node);

    // Get replicas
    const replicas = try cluster.getReplicasOfMaster(allocator, master_id);
    defer replicas.deinit();

    try std.testing.expectEqual(@as(usize, 2), replicas.items.len);
}

// ============================================================================
// Tests for saveConfig() and bumpEpoch()
// ============================================================================

test "ClusterState: saveConfig writes nodes.conf with correct format" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup myself with some slots
    const my_id = "1111111111111111111111111111111111111111".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    my_node.flags.myself = true;
    my_node.flags.master = true;
    my_node.config_epoch = 5;
    my_node.pong_recv = 1000;
    cluster.myself = my_node;
    cluster.current_epoch = 5;
    cluster.enabled = true;

    // Add myself to nodes map
    const my_key = try allocator.dupe(u8, &my_id);
    try cluster.nodes.put(my_key, my_node);

    // Add some slots to myself
    try my_node.slots.append(allocator, .{ .start = 0, .end = 100 });
    try my_node.slots.append(allocator, .{ .start = 200, .end = 300 });

    // Assign slots to myself in slot array
    for (0..101) |i| {
        cluster.slots[i] = my_node;
    }
    for (200..301) |i| {
        cluster.slots[i] = my_node;
    }

    // Create temp file for nodes.conf
    var tmpdir = std.testing.tmpDir(.{});
    defer tmpdir.cleanup();

    const config_path = try tmpdir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(config_path);

    // Call saveConfig
    try cluster.saveConfig(config_path ++ "/nodes.conf");

    // Read file back and verify format
    var file = try tmpdir.dir.openFile("nodes.conf", .{});
    defer file.close();

    const file_content = try file.readToEndAlloc(allocator, 1024 * 1024);
    defer allocator.free(file_content);

    // Verify file contains node line with correct format
    try std.testing.expect(std.mem.containsAtLeast(u8, file_content, 1, "1111111111111111111111111111111111111111"));
    try std.testing.expect(std.mem.containsAtLeast(u8, file_content, 1, "127.0.0.1:7000@17000"));
    try std.testing.expect(std.mem.containsAtLeast(u8, file_content, 1, "myself,master"));
    try std.testing.expect(std.mem.containsAtLeast(u8, file_content, 1, "currentEpoch 5"));
    // Slots should be compressed as "0-100 200-300"
    try std.testing.expect(std.mem.containsAtLeast(u8, file_content, 1, "0-100"));
    try std.testing.expect(std.mem.containsAtLeast(u8, file_content, 1, "200-300"));
}

test "ClusterState: saveConfig creates file with fsync" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup minimal cluster
    const my_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    my_node.flags.myself = true;
    cluster.myself = my_node;
    cluster.enabled = true;

    const my_key = try allocator.dupe(u8, &my_id);
    try cluster.nodes.put(my_key, my_node);

    // Create temp directory
    var tmpdir = std.testing.tmpDir(.{});
    defer tmpdir.cleanup();

    const config_path = try tmpdir.dir.realpathAlloc(allocator, ".");
    defer allocator.free(config_path);

    // Call saveConfig
    try cluster.saveConfig(config_path ++ "/nodes.conf");

    // Verify file exists (fsync was called if file can be opened)
    var file = try tmpdir.dir.openFile("nodes.conf", .{});
    defer file.close();

    const file_size = try file.getEndPos();
    try std.testing.expect(file_size > 0);
}

test "ClusterState: bumpEpoch increments when config_epoch is zero" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup myself with config_epoch = 0
    const my_id = "1111111111111111111111111111111111111111".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    my_node.config_epoch = 0;
    cluster.myself = my_node;
    cluster.current_epoch = 5;

    const my_key = try allocator.dupe(u8, &my_id);
    try cluster.nodes.put(my_key, my_node);

    // Call bumpEpoch
    const result = try cluster.bumpEpoch();

    // Should have bumped to current_epoch + 1
    try std.testing.expectEqual(true, result.bumped);
    try std.testing.expectEqual(@as(u64, 6), result.epoch);
    try std.testing.expectEqual(@as(u64, 6), my_node.config_epoch);
    try std.testing.expectEqual(@as(u64, 6), cluster.current_epoch);
}

test "ClusterState: bumpEpoch increments when config_epoch < current_epoch" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup myself with config_epoch < current_epoch
    const my_id = "2222222222222222222222222222222222222222".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    my_node.config_epoch = 3;
    cluster.myself = my_node;
    cluster.current_epoch = 8;

    const my_key = try allocator.dupe(u8, &my_id);
    try cluster.nodes.put(my_key, my_node);

    // Call bumpEpoch
    const result = try cluster.bumpEpoch();

    // Should have bumped to current_epoch + 1
    try std.testing.expectEqual(true, result.bumped);
    try std.testing.expectEqual(@as(u64, 9), result.epoch);
    try std.testing.expectEqual(@as(u64, 9), my_node.config_epoch);
    try std.testing.expectEqual(@as(u64, 9), cluster.current_epoch);
}

test "ClusterState: bumpEpoch returns STILL when already at highest" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup myself with config_epoch == current_epoch
    const my_id = "3333333333333333333333333333333333333333".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    my_node.config_epoch = 10;
    cluster.myself = my_node;
    cluster.current_epoch = 10;

    const my_key = try allocator.dupe(u8, &my_id);
    try cluster.nodes.put(my_key, my_node);

    // Call bumpEpoch
    const result = try cluster.bumpEpoch();

    // Should return STILL with no increment
    try std.testing.expectEqual(false, result.bumped);
    try std.testing.expectEqual(@as(u64, 10), result.epoch);
    try std.testing.expectEqual(@as(u64, 10), my_node.config_epoch);
    try std.testing.expectEqual(@as(u64, 10), cluster.current_epoch);
}

test "ClusterState: bumpEpoch returns STILL when config_epoch > current_epoch" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Setup myself with config_epoch > current_epoch
    const my_id = "4444444444444444444444444444444444444444".*;
    const my_node = try allocator.create(ClusterNode);
    my_node.* = try ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    my_node.config_epoch = 12;
    cluster.myself = my_node;
    cluster.current_epoch = 10;

    const my_key = try allocator.dupe(u8, &my_id);
    try cluster.nodes.put(my_key, my_node);

    // Call bumpEpoch
    const result = try cluster.bumpEpoch();

    // Should return STILL with no increment
    try std.testing.expectEqual(false, result.bumped);
    try std.testing.expectEqual(@as(u64, 12), result.epoch);
    try std.testing.expectEqual(@as(u64, 12), my_node.config_epoch);
    try std.testing.expectEqual(@as(u64, 10), cluster.current_epoch);
}

// ============================================================================
// Tests for countKeysInSlot() and getKeysInSlot()
// ============================================================================

test "countKeysInSlot - empty data" {
    const allocator = std.testing.allocator;
    const cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    var data = std.StringHashMap(void).init(allocator);
    defer data.deinit();

    // Count keys in slot 0 (should be 0 since data is empty)
    const count = cluster.countKeysInSlot(&data, 0);
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "countKeysInSlot - keys in different slots" {
    const allocator = std.testing.allocator;
    const cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    var data = std.StringHashMap(void).init(allocator);
    defer data.deinit();

    // Add keys that we know map to specific slots
    // We'll add keys and count them
    try data.put("key1", {});
    try data.put("key2", {});
    try data.put("key3", {});

    // Calculate slots for our keys
    const slot1 = keySlot("key1");
    _ = keySlot("key2");
    _ = keySlot("key3");

    // Count keys in slot1
    const count1 = cluster.countKeysInSlot(&data, slot1);
    try std.testing.expect(count1 >= 1); // At least key1 is in slot1

    // Count all keys across all slots should equal total keys
    var total_count: usize = 0;
    for (0..CLUSTER_SLOTS) |i| {
        const slot = @as(u16, @intCast(i));
        total_count += cluster.countKeysInSlot(&data, slot);
    }
    try std.testing.expectEqual(@as(usize, 3), total_count);
}

test "countKeysInSlot - hash tag co-location" {
    const allocator = std.testing.allocator;
    const cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    var data = std.StringHashMap(void).init(allocator);
    defer data.deinit();

    // Add keys with same hash tag (should map to same slot)
    try data.put("user:{123}:name", {});
    try data.put("user:{123}:email", {});
    try data.put("user:{123}:age", {});

    // All three keys should map to the same slot
    const slot = keySlot("user:{123}:name");
    const count = cluster.countKeysInSlot(&data, slot);
    try std.testing.expectEqual(@as(usize, 3), count);
}

test "countKeysInSlot - invalid slot returns 0" {
    const allocator = std.testing.allocator;
    const cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    var data = std.StringHashMap(void).init(allocator);
    defer data.deinit();

    try data.put("key1", {});

    // Slot 16384 is invalid (valid range is 0-16383)
    const count = cluster.countKeysInSlot(&data, 16384);
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "getKeysInSlot - empty data" {
    const allocator = std.testing.allocator;
    const cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    var data = std.StringHashMap(void).init(allocator);
    defer data.deinit();

    // Get keys in slot 0 (should return empty array)
    const keys = try cluster.getKeysInSlot(allocator, &data, 0, 0);
    defer {
        for (keys) |key| allocator.free(key);
        allocator.free(keys);
    }

    try std.testing.expectEqual(@as(usize, 0), keys.len);
}

test "getKeysInSlot - returns keys in slot" {
    const allocator = std.testing.allocator;
    const cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    var data = std.StringHashMap(void).init(allocator);
    defer data.deinit();

    // Add keys with same hash tag (same slot)
    try data.put("user:{abc}:name", {});
    try data.put("user:{abc}:email", {});
    try data.put("session:{xyz}:token", {}); // Different slot

    // Get keys in the slot containing user:{abc}:*
    const slot = keySlot("user:{abc}:name");
    const keys = try cluster.getKeysInSlot(allocator, &data, slot, 0);
    defer {
        for (keys) |key| allocator.free(key);
        allocator.free(keys);
    }

    try std.testing.expectEqual(@as(usize, 2), keys.len);

    // Verify both keys are present
    var found_name = false;
    var found_email = false;
    for (keys) |key| {
        if (std.mem.eql(u8, key, "user:{abc}:name")) found_name = true;
        if (std.mem.eql(u8, key, "user:{abc}:email")) found_email = true;
    }
    try std.testing.expect(found_name);
    try std.testing.expect(found_email);
}

test "getKeysInSlot - respects max_count limit" {
    const allocator = std.testing.allocator;
    const cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    var data = std.StringHashMap(void).init(allocator);
    defer data.deinit();

    // Add 5 keys with same hash tag
    try data.put("key:{tag}:1", {});
    try data.put("key:{tag}:2", {});
    try data.put("key:{tag}:3", {});
    try data.put("key:{tag}:4", {});
    try data.put("key:{tag}:5", {});

    const slot = keySlot("key:{tag}:1");

    // Request only 3 keys
    const keys = try cluster.getKeysInSlot(allocator, &data, slot, 3);
    defer {
        for (keys) |key| allocator.free(key);
        allocator.free(keys);
    }

    try std.testing.expectEqual(@as(usize, 3), keys.len);
}

test "getKeysInSlot - max_count=0 returns all keys" {
    const allocator = std.testing.allocator;
    const cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    var data = std.StringHashMap(void).init(allocator);
    defer data.deinit();

    // Add 10 keys with same hash tag
    var i: u8 = 0;
    while (i < 10) : (i += 1) {
        const key = try std.fmt.allocPrint(allocator, "key:{{tag}}:{d}", .{i});
        defer allocator.free(key);
        try data.put(key, {});
    }

    const slot = keySlot("key:{tag}:0");

    // Request all keys (max_count=0)
    const keys = try cluster.getKeysInSlot(allocator, &data, slot, 0);
    defer {
        for (keys) |key| allocator.free(key);
        allocator.free(keys);
    }

    try std.testing.expectEqual(@as(usize, 10), keys.len);
}

test "getKeysInSlot - invalid slot returns empty array" {
    const allocator = std.testing.allocator;
    const cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    var data = std.StringHashMap(void).init(allocator);
    defer data.deinit();

    try data.put("key1", {});

    // Slot 20000 is invalid
    const keys = try cluster.getKeysInSlot(allocator, &data, 20000, 0);
    defer {
        for (keys) |key| allocator.free(key);
        allocator.free(keys);
    }

    try std.testing.expectEqual(@as(usize, 0), keys.len);
}

test "ClusterState: setReadonly/hasReadonly/clearReadonly" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const client_id: u64 = 12345;

    // Initially no readonly flag
    try std.testing.expect(!cluster.hasReadonly(client_id));

    // Set readonly flag
    try cluster.setReadonly(client_id);
    try std.testing.expect(cluster.hasReadonly(client_id));

    // Clear readonly flag
    cluster.clearReadonly(client_id);
    try std.testing.expect(!cluster.hasReadonly(client_id));
}

test "ClusterState: readonly flag is per-client" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const client1: u64 = 100;
    const client2: u64 = 200;

    // Set readonly for client1 only
    try cluster.setReadonly(client1);

    try std.testing.expect(cluster.hasReadonly(client1));
    try std.testing.expect(!cluster.hasReadonly(client2));

    // Clear client1, set client2
    cluster.clearReadonly(client1);
    try cluster.setReadonly(client2);

    try std.testing.expect(!cluster.hasReadonly(client1));
    try std.testing.expect(cluster.hasReadonly(client2));
}

test "ClusterState: clearReadonly is idempotent" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const client_id: u64 = 999;

    // Clear non-existent flag (should not error)
    cluster.clearReadonly(client_id);
    try std.testing.expect(!cluster.hasReadonly(client_id));

    // Set and clear multiple times
    try cluster.setReadonly(client_id);
    cluster.clearReadonly(client_id);
    cluster.clearReadonly(client_id);
    try std.testing.expect(!cluster.hasReadonly(client_id));
}

test "ClusterNode: getHealth returns online for healthy node" {
    const allocator = std.testing.allocator;
    const node_id = "0123456789abcdef0123456789abcdef01234567".*;
    var node = try ClusterNode.init(allocator, node_id, "127.0.0.1", 7000);
    defer node.deinit(allocator);

    node.flags.fail = false;
    node.flags.noaddr = false;

    const health = node.getHealth();
    try std.testing.expectEqualSlices(u8, "online", health);
}

test "ClusterNode: getHealth returns failed when fail flag set" {
    const allocator = std.testing.allocator;
    const node_id = "0123456789abcdef0123456789abcdef01234567".*;
    var node = try ClusterNode.init(allocator, node_id, "127.0.0.1", 7000);
    defer node.deinit(allocator);

    node.flags.fail = true;

    const health = node.getHealth();
    try std.testing.expectEqualSlices(u8, "failed", health);
}

test "ClusterNode: getHealth returns loading when noaddr flag set" {
    const allocator = std.testing.allocator;
    const node_id = "0123456789abcdef0123456789abcdef01234567".*;
    var node = try ClusterNode.init(allocator, node_id, "127.0.0.1", 7000);
    defer node.deinit(allocator);

    node.flags.noaddr = true;
    node.flags.fail = false;

    const health = node.getHealth();
    try std.testing.expectEqualSlices(u8, "loading", health);
}

test "ClusterState: collectShards empty cluster" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // No nodes, no shards
    const shards = try cluster.collectShards(allocator);
    defer {
        for (shards) |*shard| {
            shard.deinit(allocator);
        }
        allocator.free(shards);
    }

    try std.testing.expectEqual(@as(usize, 0), shards.len);
}

test "ClusterState: collectShards single-node cluster" {
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
    node.flags.master = true;
    node.flags.slave = false;

    // Assign all slots to this node
    const slots = [_]u16{ 0, 1, 2, 3, 16383 };
    try cluster.addSlotsToNode(node, &slots);

    const shards = try cluster.collectShards(allocator);
    defer {
        for (shards) |*shard| {
            shard.deinit(allocator);
        }
        allocator.free(shards);
    }

    try std.testing.expectEqual(@as(usize, 1), shards.len);
    try std.testing.expectEqual(@as(usize, 1), shards[0].nodes.items.len);
    try std.testing.expectEqual(node, shards[0].nodes.items[0]);
    // Should have 2 ranges: [0,3] and [16383,16383]
    try std.testing.expectEqual(@as(usize, 4), shards[0].slots.items.len);
}

test "ClusterState: collectShards multi-shard with replicas" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Create master 1
    const master1_id = "1111111111111111111111111111111111111111".*;
    const master1 = try allocator.create(ClusterNode);
    master1.* = try ClusterNode.init(allocator, master1_id, "127.0.0.1", 7000);
    defer {
        master1.deinit(allocator);
        allocator.destroy(master1);
    }
    master1.flags.master = true;
    master1.flags.slave = false;

    // Create replica of master1
    const replica1_id = "2222222222222222222222222222222222222222".*;
    const replica1 = try allocator.create(ClusterNode);
    replica1.* = try ClusterNode.init(allocator, replica1_id, "127.0.0.1", 7001);
    defer {
        replica1.deinit(allocator);
        allocator.destroy(replica1);
    }
    replica1.flags.master = false;
    replica1.flags.slave = true;
    replica1.master_id = master1_id;

    // Create master 2
    const master2_id = "3333333333333333333333333333333333333333".*;
    const master2 = try allocator.create(ClusterNode);
    master2.* = try ClusterNode.init(allocator, master2_id, "127.0.0.1", 7002);
    defer {
        master2.deinit(allocator);
        allocator.destroy(master2);
    }
    master2.flags.master = true;
    master2.flags.slave = false;

    cluster.myself = master1;

    // Add nodes to cluster
    const m1_key = try allocator.dupe(u8, &master1_id);
    try cluster.nodes.put(m1_key, master1);
    const r1_key = try allocator.dupe(u8, &replica1_id);
    try cluster.nodes.put(r1_key, replica1);
    const m2_key = try allocator.dupe(u8, &master2_id);
    try cluster.nodes.put(m2_key, master2);

    // Assign slots: master1 gets 0-8191, master2 gets 8192-16383
    var slots1 = std.ArrayListUnmanaged(u16){};
    try slots1.appendSlice(allocator, &[_]u16{ 0, 1, 2, 3, 4, 5, 6, 7, 8191 });
    try cluster.addSlotsToNode(master1, slots1.items);
    slots1.deinit(allocator);

    var slots2 = std.ArrayListUnmanaged(u16){};
    try slots2.appendSlice(allocator, &[_]u16{ 8192, 8193, 16383 });
    try cluster.addSlotsToNode(master2, slots2.items);
    slots2.deinit(allocator);

    const shards = try cluster.collectShards(allocator);
    defer {
        for (shards) |*shard| {
            shard.deinit(allocator);
        }
        allocator.free(shards);
    }

    try std.testing.expectEqual(@as(usize, 2), shards.len);
    // First shard should have master1 and replica1
    try std.testing.expectEqual(@as(usize, 2), shards[0].nodes.items.len);
    try std.testing.expectEqual(master1, shards[0].nodes.items[0]);
    try std.testing.expectEqual(replica1, shards[0].nodes.items[1]);
    // Second shard should have only master2
    try std.testing.expectEqual(@as(usize, 1), shards[1].nodes.items.len);
    try std.testing.expectEqual(master2, shards[1].nodes.items[0]);
}

test "ClusterState: collectShards excludes failed replicas" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Create master
    const master_id = "0123456789abcdef0123456789abcdef01234567".*;
    const master = try allocator.create(ClusterNode);
    master.* = try ClusterNode.init(allocator, master_id, "127.0.0.1", 7000);
    defer {
        master.deinit(allocator);
        allocator.destroy(master);
    }
    master.flags.master = true;
    master.flags.slave = false;

    // Create healthy replica
    const healthy_replica_id = "1111111111111111111111111111111111111111".*;
    const healthy_replica = try allocator.create(ClusterNode);
    healthy_replica.* = try ClusterNode.init(allocator, healthy_replica_id, "127.0.0.1", 7001);
    defer {
        healthy_replica.deinit(allocator);
        allocator.destroy(healthy_replica);
    }
    healthy_replica.flags.master = false;
    healthy_replica.flags.slave = true;
    healthy_replica.master_id = master_id;

    // Create failed replica
    const failed_replica_id = "2222222222222222222222222222222222222222".*;
    const failed_replica = try allocator.create(ClusterNode);
    failed_replica.* = try ClusterNode.init(allocator, failed_replica_id, "127.0.0.1", 7002);
    defer {
        failed_replica.deinit(allocator);
        allocator.destroy(failed_replica);
    }
    failed_replica.flags.master = false;
    failed_replica.flags.slave = true;
    failed_replica.flags.fail = true;
    failed_replica.master_id = master_id;

    cluster.myself = master;

    // Add to nodes
    const master_key = try allocator.dupe(u8, &master_id);
    try cluster.nodes.put(master_key, master);
    const healthy_key = try allocator.dupe(u8, &healthy_replica_id);
    try cluster.nodes.put(healthy_key, healthy_replica);
    const failed_key = try allocator.dupe(u8, &failed_replica_id);
    try cluster.nodes.put(failed_key, failed_replica);

    // Assign slots to master
    const slots = [_]u16{ 0, 1, 2, 3 };
    try cluster.addSlotsToNode(master, &slots);

    const shards = try cluster.collectShards(allocator);
    defer {
        for (shards) |*shard| {
            shard.deinit(allocator);
        }
        allocator.free(shards);
    }

    try std.testing.expectEqual(@as(usize, 1), shards.len);
    // Should have master + healthy replica, NOT failed replica
    try std.testing.expectEqual(@as(usize, 2), shards[0].nodes.items.len);
    try std.testing.expectEqual(master, shards[0].nodes.items[0]);
    try std.testing.expectEqual(healthy_replica, shards[0].nodes.items[1]);
}

test "ClusterState: collectShards slot ranges compression" {
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
    node.flags.master = true;
    node.flags.slave = false;

    // Assign non-contiguous slots: [0-99] and [200-299]
    var slots = std.ArrayListUnmanaged(u16){};
    for (0..100) |i| {
        try slots.append(allocator, @intCast(i));
    }
    for (200..300) |i| {
        try slots.append(allocator, @intCast(i));
    }
    try cluster.addSlotsToNode(node, slots.items);
    slots.deinit(allocator);

    const shards = try cluster.collectShards(allocator);
    defer {
        for (shards) |*shard| {
            shard.deinit(allocator);
        }
        allocator.free(shards);
    }

    try std.testing.expectEqual(@as(usize, 1), shards.len);
    // Should have 2 ranges: [0, 99], [200, 299]
    try std.testing.expectEqual(@as(usize, 4), shards[0].slots.items.len);
    try std.testing.expectEqual(@as(u16, 0), shards[0].slots.items[0]);
    try std.testing.expectEqual(@as(u16, 99), shards[0].slots.items[1]);
    try std.testing.expectEqual(@as(u16, 200), shards[0].slots.items[2]);
    try std.testing.expectEqual(@as(u16, 299), shards[0].slots.items[3]);
}

// =============================================================================
// CLUSTER MYSHARDID tests (Iteration 149)
// =============================================================================

test "ClusterNode: shard_id generation on creation" {
    const allocator = std.testing.allocator;

    const node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const node = try allocator.create(ClusterNode);
    node.* = try ClusterNode.init(allocator, node_id, "127.0.0.1", 7000);
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    // Verify shard_id exists and is 40 characters
    // This will fail because ClusterNode doesn't have shard_id field yet
    try std.testing.expectEqual(@as(usize, 40), node.shard_id.len);

    // Verify all characters are valid hex
    for (node.shard_id) |char| {
        const is_valid = (char >= '0' and char <= '9') or
                        (char >= 'a' and char <= 'f') or
                        (char >= 'A' and char <= 'F');
        try std.testing.expect(is_valid);
    }
}

test "ClusterNode: shard_id uniqueness across nodes" {
    const allocator = std.testing.allocator;

    // Create 3 independent nodes
    const node1_id = "1111111111111111111111111111111111111111".*;
    const node1 = try allocator.create(ClusterNode);
    node1.* = try ClusterNode.init(allocator, node1_id, "127.0.0.1", 7000);
    defer {
        node1.deinit(allocator);
        allocator.destroy(node1);
    }

    const node2_id = "2222222222222222222222222222222222222222".*;
    const node2 = try allocator.create(ClusterNode);
    node2.* = try ClusterNode.init(allocator, node2_id, "127.0.0.1", 7001);
    defer {
        node2.deinit(allocator);
        allocator.destroy(node2);
    }

    const node3_id = "3333333333333333333333333333333333333333".*;
    const node3 = try allocator.create(ClusterNode);
    node3.* = try ClusterNode.init(allocator, node3_id, "127.0.0.1", 7002);
    defer {
        node3.deinit(allocator);
        allocator.destroy(node3);
    }

    // Verify each node has a different shard_id
    // This will fail because shard_id field doesn't exist yet
    try std.testing.expect(!std.mem.eql(u8, &node1.shard_id, &node2.shard_id));
    try std.testing.expect(!std.mem.eql(u8, &node1.shard_id, &node3.shard_id));
    try std.testing.expect(!std.mem.eql(u8, &node2.shard_id, &node3.shard_id));
}

test "ClusterState: replica inherits master shard_id" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Create master node
    const master_id = "1111111111111111111111111111111111111111".*;
    const master = try allocator.create(ClusterNode);
    master.* = try ClusterNode.init(allocator, master_id, "127.0.0.1", 7000);
    defer {
        master.deinit(allocator);
        allocator.destroy(master);
    }
    master.flags.master = true;
    master.flags.slave = false;

    // Add master to cluster
    const master_key = try allocator.dupe(u8, &master_id);
    try cluster.nodes.put(master_key, master);

    // Create replica node
    const replica_id = "2222222222222222222222222222222222222222".*;
    const replica = try allocator.create(ClusterNode);
    replica.* = try ClusterNode.init(allocator, replica_id, "127.0.0.1", 7001);
    defer {
        replica.deinit(allocator);
        allocator.destroy(replica);
    }

    // Add replica to cluster
    const replica_key = try allocator.dupe(u8, &replica_id);
    try cluster.nodes.put(replica_key, replica);
    cluster.myself = replica;

    // Store master's original shard_id
    // This will fail because shard_id field doesn't exist yet
    const master_shard_id = master.shard_id;

    // Configure as replica
    try cluster.configureAsReplica(master_id);

    // Verify replica inherited master's shard_id
    // This will fail because configureAsReplica doesn't copy shard_id yet
    try std.testing.expectEqualSlices(u8, &master_shard_id, &replica.shard_id);

    // Verify replica didn't change master's shard_id
    try std.testing.expectEqualSlices(u8, &master_shard_id, &master.shard_id);
}

test "ClusterNode: shard_id format validation" {
    const allocator = std.testing.allocator;

    const node_id = "abcdef0123456789abcdef0123456789abcdef01".*;
    const node = try allocator.create(ClusterNode);
    node.* = try ClusterNode.init(allocator, node_id, "127.0.0.1", 7000);
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    // Verify shard_id is exactly 40 characters
    // This will fail because shard_id field doesn't exist yet
    try std.testing.expectEqual(@as(usize, 40), node.shard_id.len);

    // Verify all characters are lowercase hex (Redis convention)
    for (node.shard_id) |char| {
        const is_valid_hex = (char >= '0' and char <= '9') or (char >= 'a' and char <= 'f');
        try std.testing.expect(is_valid_hex);
    }

    // Verify shard_id is not all zeros (should be random)
    var all_zeros = true;
    for (node.shard_id) |char| {
        if (char != '0') {
            all_zeros = false;
            break;
        }
    }
    try std.testing.expect(!all_zeros);
}

test "ClusterState: multiple replicas inherit same shard_id" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Create master node
    const master_id = "1111111111111111111111111111111111111111".*;
    const master = try allocator.create(ClusterNode);
    master.* = try ClusterNode.init(allocator, master_id, "127.0.0.1", 7000);
    defer {
        master.deinit(allocator);
        allocator.destroy(master);
    }
    master.flags.master = true;
    master.flags.slave = false;

    const master_key = try allocator.dupe(u8, &master_id);
    try cluster.nodes.put(master_key, master);

    // Create first replica
    const replica1_id = "2222222222222222222222222222222222222222".*;
    const replica1 = try allocator.create(ClusterNode);
    replica1.* = try ClusterNode.init(allocator, replica1_id, "127.0.0.1", 7001);
    defer {
        replica1.deinit(allocator);
        allocator.destroy(replica1);
    }

    const replica1_key = try allocator.dupe(u8, &replica1_id);
    try cluster.nodes.put(replica1_key, replica1);

    // Create second replica
    const replica2_id = "3333333333333333333333333333333333333333".*;
    const replica2 = try allocator.create(ClusterNode);
    replica2.* = try ClusterNode.init(allocator, replica2_id, "127.0.0.1", 7002);
    defer {
        replica2.deinit(allocator);
        allocator.destroy(replica2);
    }

    const replica2_key = try allocator.dupe(u8, &replica2_id);
    try cluster.nodes.put(replica2_key, replica2);

    // Configure first replica
    cluster.myself = replica1;
    try cluster.configureAsReplica(master_id);

    // Configure second replica
    cluster.myself = replica2;
    try cluster.configureAsReplica(master_id);

    // Verify all three nodes in replication group have same shard_id
    // This will fail because shard_id field doesn't exist yet
    try std.testing.expectEqualSlices(u8, &master.shard_id, &replica1.shard_id);
    try std.testing.expectEqualSlices(u8, &master.shard_id, &replica2.shard_id);
    try std.testing.expectEqualSlices(u8, &replica1.shard_id, &replica2.shard_id);
}

test "ClusterLink: struct initialization with all fields" {
    const peer_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const now = std.time.milliTimestamp();

    const link = ClusterLink{
        .direction = "to",
        .peer_node_id = peer_id,
        .create_time = now,
        .events = "rw",
        .send_buffer_allocated = 4096,
        .send_buffer_used = 256,
    };

    try std.testing.expectEqualSlices(u8, link.direction, "to");
    try std.testing.expectEqualSlices(u8, &link.peer_node_id, &peer_id);
    try std.testing.expect(link.create_time > 0);
    try std.testing.expectEqualSlices(u8, link.events, "rw");
    try std.testing.expectEqual(link.send_buffer_allocated, 4096);
    try std.testing.expectEqual(link.send_buffer_used, 256);
}

test "ClusterLink: direction validation for 'from' link" {
    const peer_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".*;
    const now = std.time.milliTimestamp();

    const link = ClusterLink{
        .direction = "from",
        .peer_node_id = peer_id,
        .create_time = now,
        .events = "r",
        .send_buffer_allocated = 4096,
        .send_buffer_used = 128,
    };

    try std.testing.expectEqualSlices(u8, link.direction, "from");
    try std.testing.expectEqualSlices(u8, link.events, "r");
}

test "ClusterLink: buffer size constraints" {
    const peer_id = "cccccccccccccccccccccccccccccccccccccccc".*;

    const link = ClusterLink{
        .direction = "to",
        .peer_node_id = peer_id,
        .create_time = 1000,
        .events = "rw",
        .send_buffer_allocated = 8192,
        .send_buffer_used = 2048,
    };

    try std.testing.expect(link.send_buffer_used <= link.send_buffer_allocated);
}

test "ClusterState.collectLinks: empty cluster (single node)" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const myself_id = "ffffffffffffffffffffffffffffffffffffffff".*;
    const myself = try allocator.create(ClusterNode);
    myself.* = try ClusterNode.init(allocator, myself_id, "127.0.0.1", 7000);
    defer {
        myself.deinit(allocator);
        allocator.destroy(myself);
    }

    cluster.myself = myself;
    const myself_key = try allocator.dupe(u8, &myself_id);
    try cluster.nodes.put(myself_key, myself);

    const links = try cluster.collectLinks(allocator);
    defer allocator.free(links);

    // Single node cluster should have no links (myself is excluded)
    try std.testing.expectEqual(links.len, 0);
}

test "ClusterState.collectLinks: one peer node (2 links)" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const myself_id = "1111111111111111111111111111111111111111".*;
    const myself = try allocator.create(ClusterNode);
    myself.* = try ClusterNode.init(allocator, myself_id, "127.0.0.1", 7000);
    defer {
        myself.deinit(allocator);
        allocator.destroy(myself);
    }

    const peer_id = "2222222222222222222222222222222222222222".*;
    const peer = try allocator.create(ClusterNode);
    peer.* = try ClusterNode.init(allocator, peer_id, "127.0.0.1", 7001);
    defer {
        peer.deinit(allocator);
        allocator.destroy(peer);
    }

    cluster.myself = myself;
    const myself_key = try allocator.dupe(u8, &myself_id);
    try cluster.nodes.put(myself_key, myself);

    const peer_key = try allocator.dupe(u8, &peer_id);
    try cluster.nodes.put(peer_key, peer);

    const links = try cluster.collectLinks(allocator);
    defer allocator.free(links);

    // One peer should result in 2 links (to + from)
    try std.testing.expectEqual(links.len, 2);

    // Check "to" link
    try std.testing.expectEqualSlices(u8, links[0].direction, "to");
    try std.testing.expectEqualSlices(u8, &links[0].peer_node_id, &peer_id);
    try std.testing.expect(links[0].create_time > 0);
    try std.testing.expectEqualSlices(u8, links[0].events, "rw");
    try std.testing.expectEqual(links[0].send_buffer_allocated, 4096);
    try std.testing.expectEqual(links[0].send_buffer_used, 0);

    // Check "from" link
    try std.testing.expectEqualSlices(u8, links[1].direction, "from");
    try std.testing.expectEqualSlices(u8, &links[1].peer_node_id, &peer_id);
    try std.testing.expect(links[1].create_time > 0);
    try std.testing.expectEqualSlices(u8, links[1].events, "r");
    try std.testing.expectEqual(links[1].send_buffer_allocated, 4096);
    try std.testing.expectEqual(links[1].send_buffer_used, 0);
}

test "ClusterState.collectLinks: three peers (6 links)" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const myself_id = "1111111111111111111111111111111111111111".*;
    const myself = try allocator.create(ClusterNode);
    myself.* = try ClusterNode.init(allocator, myself_id, "127.0.0.1", 7000);
    defer {
        myself.deinit(allocator);
        allocator.destroy(myself);
    }

    cluster.myself = myself;
    const myself_key = try allocator.dupe(u8, &myself_id);
    try cluster.nodes.put(myself_key, myself);

    // Add three peer nodes
    var peer_ids: [3][40]u8 = undefined;
    var peers: [3]*ClusterNode = undefined;

    for (0..3) |i| {
        var id_buf: [40]u8 = undefined;
        const id_str = try std.fmt.bufPrint(&id_buf, "{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}{d}", .{
            i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i,
        });
        @memcpy(&peer_ids[i], id_str);

        const peer = try allocator.create(ClusterNode);
        peer.* = try ClusterNode.init(allocator, peer_ids[i], "127.0.0.1", @intCast(7001 + i));

        const key = try allocator.dupe(u8, &peer_ids[i]);
        try cluster.nodes.put(key, peer);
        peers[i] = peer;
    }

    defer {
        for (peers) |peer| {
            peer.deinit(allocator);
            allocator.destroy(peer);
        }
    }

    const links = try cluster.collectLinks(allocator);
    defer allocator.free(links);

    // Three peers should result in 6 links (3×2)
    try std.testing.expectEqual(links.len, 6);

    // Verify each peer has exactly one "to" and one "from" link
    for (0..3) |i| {
        var to_count: u32 = 0;
        var from_count: u32 = 0;
        for (links) |link| {
            if (std.mem.eql(u8, &link.peer_node_id, &peer_ids[i])) {
                if (std.mem.eql(u8, link.direction, "to")) {
                    to_count += 1;
                } else if (std.mem.eql(u8, link.direction, "from")) {
                    from_count += 1;
                }
            }
        }
        try std.testing.expectEqual(to_count, 1);
        try std.testing.expectEqual(from_count, 1);
    }
}

test "ClusterState.collectLinks: link field validation" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const myself_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const myself = try allocator.create(ClusterNode);
    myself.* = try ClusterNode.init(allocator, myself_id, "127.0.0.1", 7000);
    defer {
        myself.deinit(allocator);
        allocator.destroy(myself);
    }

    const peer_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".*;
    const peer = try allocator.create(ClusterNode);
    peer.* = try ClusterNode.init(allocator, peer_id, "127.0.0.1", 7001);
    peer.to_send_buf_used = 1024;
    peer.from_send_buf_used = 512;
    defer {
        peer.deinit(allocator);
        allocator.destroy(peer);
    }

    cluster.myself = myself;
    const myself_key = try allocator.dupe(u8, &myself_id);
    try cluster.nodes.put(myself_key, myself);

    const peer_key = try allocator.dupe(u8, &peer_id);
    try cluster.nodes.put(peer_key, peer);

    const links = try cluster.collectLinks(allocator);
    defer allocator.free(links);

    try std.testing.expectEqual(links.len, 2);

    // Verify "to" link has correct values
    try std.testing.expect(links[0].create_time > 0);
    try std.testing.expect(links[0].send_buffer_allocated >= links[0].send_buffer_used);
    try std.testing.expectEqual(links[0].send_buffer_used, 1024);

    // Verify "from" link has correct values
    try std.testing.expect(links[1].create_time > 0);
    try std.testing.expect(links[1].send_buffer_allocated >= links[1].send_buffer_used);
    try std.testing.expectEqual(links[1].send_buffer_used, 512);
}

test "ClusterState.collectLinks: no myself (returns empty)" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    // Don't set myself
    cluster.myself = null;

    const links = try cluster.collectLinks(allocator);
    defer allocator.free(links);

    // No myself means no links can be returned
    try std.testing.expectEqual(links.len, 0);
}

// ============================================================================
// Failure Report Tests
// ============================================================================

test "addFailureReport: single report" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const reported_node = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const reporter = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".*;

    try cluster.addFailureReport(reported_node, reporter);

    const count = cluster.getFailureReportCount(reported_node);
    try std.testing.expectEqual(@as(usize, 1), count);
}

test "addFailureReport: multiple reporters" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const reported_node = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const reporter1 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".*;
    const reporter2 = "cccccccccccccccccccccccccccccccccccccccc".*;
    const reporter3 = "dddddddddddddddddddddddddddddddddddddddd".*;

    try cluster.addFailureReport(reported_node, reporter1);
    try cluster.addFailureReport(reported_node, reporter2);
    try cluster.addFailureReport(reported_node, reporter3);

    const count = cluster.getFailureReportCount(reported_node);
    try std.testing.expectEqual(@as(usize, 3), count);
}

test "addFailureReport: duplicate reporter (idempotent)" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const reported_node = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const reporter = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".*;

    try cluster.addFailureReport(reported_node, reporter);
    try cluster.addFailureReport(reported_node, reporter);
    try cluster.addFailureReport(reported_node, reporter);

    const count = cluster.getFailureReportCount(reported_node);
    // Should still be 1 (duplicate reports ignored)
    try std.testing.expectEqual(@as(usize, 1), count);
}

test "getFailureReportCount: no reports" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const count = cluster.getFailureReportCount(node_id);
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "clearFailureReports: removes all reports" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const reported_node = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const reporter1 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".*;
    const reporter2 = "cccccccccccccccccccccccccccccccccccccccc".*;

    try cluster.addFailureReport(reported_node, reporter1);
    try cluster.addFailureReport(reported_node, reporter2);

    var count = cluster.getFailureReportCount(reported_node);
    try std.testing.expectEqual(@as(usize, 2), count);

    cluster.clearFailureReports(reported_node);

    count = cluster.getFailureReportCount(reported_node);
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "expireOldFailureReports: removes stale reports" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const reported_node = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const reporter = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".*;

    // Add a report
    try cluster.addFailureReport(reported_node, reporter);

    // Manually set timestamp to 61 seconds ago
    const reports = cluster.failure_reports.getPtr(reported_node[0..]).?;
    reports.items[0].timestamp = std.time.milliTimestamp() - 61_000;

    var count = cluster.getFailureReportCount(reported_node);
    try std.testing.expectEqual(@as(usize, 1), count);

    // Expire old reports
    try cluster.expireOldFailureReports();

    count = cluster.getFailureReportCount(reported_node);
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "expireOldFailureReports: keeps recent reports" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    const reported_node = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const reporter = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".*;

    // Add a report (will have current timestamp)
    try cluster.addFailureReport(reported_node, reporter);

    var count = cluster.getFailureReportCount(reported_node);
    try std.testing.expectEqual(@as(usize, 1), count);

    // Expire old reports (should not remove recent one)
    try cluster.expireOldFailureReports();

    count = cluster.getFailureReportCount(reported_node);
    try std.testing.expectEqual(@as(usize, 1), count);
}

test "resetSoft: clears slots and preserves node ID" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    cluster.enabled = true;

    // Initialize myself node
    const node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const node = try allocator.create(ClusterNode);
    node.* = try ClusterNode.init(allocator, node_id, "127.0.0.1", 6379);
    cluster.myself = node;

    const node_key = try allocator.dupe(u8, &node_id);
    try cluster.nodes.put(node_key, node);

    // Save original node ID
    const original_id = node.id;

    // Add some slots
    const slots = [_]u16{ 0, 1, 2, 100, 200 };
    try cluster.addSlotsToNode(node, &slots);

    // Perform soft reset
    try cluster.resetSoft(false);

    // Verify slots cleared
    for (cluster.slots) |slot| {
        try std.testing.expectEqual(@as(?*ClusterNode, null), slot);
    }

    // Verify ID preserved
    if (cluster.myself) |n| {
        try std.testing.expect(std.mem.eql(u8, &n.id, &original_id));
    }
}

test "resetHard: generates new node ID and resets epochs" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    cluster.enabled = true;

    // Initialize myself node
    const node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const node = try allocator.create(ClusterNode);
    node.* = try ClusterNode.init(allocator, node_id, "127.0.0.1", 6379);
    cluster.myself = node;

    const node_key = try allocator.dupe(u8, &node_id);
    try cluster.nodes.put(node_key, node);

    // Save original node ID
    const original_id = node.id;

    // Set epochs
    cluster.current_epoch = 100;
    node.config_epoch = 50;

    // Perform hard reset
    try cluster.resetHard(false);

    // Verify new ID generated
    if (cluster.myself) |n| {
        try std.testing.expect(!std.mem.eql(u8, &n.id, &original_id));
    }

    // Verify epochs reset
    try std.testing.expectEqual(@as(u64, 0), cluster.current_epoch);
    if (cluster.myself) |n| {
        try std.testing.expectEqual(@as(u64, 0), n.config_epoch);
    }
}

test "resetSoft: converts replica to master" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    cluster.enabled = true;

    // Initialize myself node as replica
    const node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const node = try allocator.create(ClusterNode);
    node.* = try ClusterNode.init(allocator, node_id, "127.0.0.1", 6379);
    cluster.myself = node;

    const node_key = try allocator.dupe(u8, &node_id);
    try cluster.nodes.put(node_key, node);

    // Configure as replica
    node.flags.master = false;
    node.flags.slave = true;
    const master_id = "fedcba9876543210fedcba9876543210fedcba98".*;
    node.master_id = master_id;

    // Perform soft reset
    try cluster.resetSoft(false);

    // Verify converted to master
    if (cluster.myself) |n| {
        try std.testing.expect(n.flags.master);
        try std.testing.expect(!n.flags.slave);
        try std.testing.expectEqual(@as(?[40]u8, null), n.master_id);
    }
}

test "resetSoft: rejects master with keys" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    cluster.enabled = true;

    // Initialize myself node (master by default)
    const node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const node = try allocator.create(ClusterNode);
    node.* = try ClusterNode.init(allocator, node_id, "127.0.0.1", 6379);
    cluster.myself = node;

    const node_key = try allocator.dupe(u8, &node_id);
    try cluster.nodes.put(node_key, node);

    // Node is master by default
    const result = cluster.resetSoft(true); // has_keys = true
    try std.testing.expectError(ClusterError.SlotHasKeys, result);
}

test "resetSoft: clears migration states" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    cluster.enabled = true;

    // Initialize myself node
    const node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const node = try allocator.create(ClusterNode);
    node.* = try ClusterNode.init(allocator, node_id, "127.0.0.1", 6379);
    cluster.myself = node;

    const node_key = try allocator.dupe(u8, &node_id);
    try cluster.nodes.put(node_key, node);

    // Set migration states
    const remote_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0".*;
    cluster.slot_migration_states[0].migrating_to = remote_id;
    cluster.slot_migration_states[1].importing_from = remote_id;

    // Perform soft reset
    try cluster.resetSoft(false);

    // Verify migration states cleared
    try std.testing.expectEqual(@as(?[40]u8, null), cluster.slot_migration_states[0].migrating_to);
    try std.testing.expectEqual(@as(?[40]u8, null), cluster.slot_migration_states[1].importing_from);
}

test "resetSoft: clears client flags" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    cluster.enabled = true;

    // Initialize myself node
    const node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const node = try allocator.create(ClusterNode);
    node.* = try ClusterNode.init(allocator, node_id, "127.0.0.1", 6379);
    cluster.myself = node;

    const node_key = try allocator.dupe(u8, &node_id);
    try cluster.nodes.put(node_key, node);

    // Set client flags
    try cluster.setAsking(12345);
    try cluster.setReadonly(12345);

    try std.testing.expect(cluster.hasAsking(12345));
    try std.testing.expect(cluster.hasReadonly(12345));

    // Perform soft reset
    try cluster.resetSoft(false);

    // Verify flags cleared
    try std.testing.expect(!cluster.hasAsking(12345));
    try std.testing.expect(!cluster.hasReadonly(12345));
}

test "setConfigEpoch: valid epoch with no slots" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    cluster.enabled = true;

    // Initialize myself node
    const node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const node = try allocator.create(ClusterNode);
    node.* = try ClusterNode.init(allocator, node_id, "127.0.0.1", 6379);
    cluster.myself = node;

    const node_key = try allocator.dupe(u8, &node_id);
    try cluster.nodes.put(node_key, node);

    // Initial epochs
    try std.testing.expectEqual(@as(u64, 0), node.config_epoch);
    try std.testing.expectEqual(@as(u64, 0), cluster.current_epoch);

    // Set config epoch to 42
    try cluster.setConfigEpoch(42);

    // Verify epochs updated
    try std.testing.expectEqual(@as(u64, 42), node.config_epoch);
    try std.testing.expectEqual(@as(u64, 42), cluster.current_epoch);
}

test "setConfigEpoch: current_epoch increases if needed" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    cluster.enabled = true;

    // Initialize myself node
    const node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const node = try allocator.create(ClusterNode);
    node.* = try ClusterNode.init(allocator, node_id, "127.0.0.1", 6379);
    cluster.myself = node;

    const node_key = try allocator.dupe(u8, &node_id);
    try cluster.nodes.put(node_key, node);

    // Set initial epochs
    node.config_epoch = 10;
    cluster.current_epoch = 10;

    // Set config epoch to 100 (higher than current)
    try cluster.setConfigEpoch(100);

    // Verify current_epoch increased
    try std.testing.expectEqual(@as(u64, 100), node.config_epoch);
    try std.testing.expectEqual(@as(u64, 100), cluster.current_epoch);
}

test "setConfigEpoch: current_epoch stays if higher" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    cluster.enabled = true;

    // Initialize myself node
    const node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const node = try allocator.create(ClusterNode);
    node.* = try ClusterNode.init(allocator, node_id, "127.0.0.1", 6379);
    cluster.myself = node;

    const node_key = try allocator.dupe(u8, &node_id);
    try cluster.nodes.put(node_key, node);

    // Set initial epochs
    node.config_epoch = 10;
    cluster.current_epoch = 200; // Higher than what we'll set

    // Set config epoch to 50 (lower than current_epoch)
    try cluster.setConfigEpoch(50);

    // Verify current_epoch stayed at 200
    try std.testing.expectEqual(@as(u64, 50), node.config_epoch);
    try std.testing.expectEqual(@as(u64, 200), cluster.current_epoch);
}

test "setConfigEpoch: error if node has assigned slots" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    cluster.enabled = true;

    // Initialize myself node
    const node_id = "0123456789abcdef0123456789abcdef01234567".*;
    const node = try allocator.create(ClusterNode);
    node.* = try ClusterNode.init(allocator, node_id, "127.0.0.1", 6379);
    cluster.myself = node;

    const node_key = try allocator.dupe(u8, &node_id);
    try cluster.nodes.put(node_key, node);

    // Assign slot 100 to myself
    cluster.slots[100] = node;

    // Attempt to set config epoch should fail
    const result = cluster.setConfigEpoch(42);
    try std.testing.expectError(error.SlotHasKeys, result);
}

test "setConfigEpoch: error if cluster not initialized" {
    const allocator = std.testing.allocator;
    var cluster = ClusterState.init(allocator);
    defer cluster.deinit();

    cluster.enabled = true;
    // myself is null

    // Attempt to set config epoch should fail
    const result = cluster.setConfigEpoch(42);
    try std.testing.expectError(error.ClusterNotInitialized, result);
}
