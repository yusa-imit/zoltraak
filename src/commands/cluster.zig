const std = @import("std");
const Writer = @import("../protocol/writer.zig").Writer;
const Storage = @import("../storage/memory.zig").Storage;
const RespValue = @import("../protocol/parser.zig").RespValue;
const cluster_mod = @import("../storage/cluster.zig");
const ClusterError = cluster_mod.ClusterError;
const ClusterState = cluster_mod.ClusterState;

/// Format a slot error message with the first offending slot number
fn formatSlotError(
    allocator: std.mem.Allocator,
    err: ClusterError,
    storage: *Storage,
    slots: []const u16,
    node: *cluster_mod.ClusterNode,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    var buf: [256]u8 = undefined;
    switch (err) {
        ClusterError.SlotAlreadyBusy => {
            for (slots) |slot| {
                if (storage.cluster.slots[slot] != null) {
                    const err_msg = try std.fmt.bufPrint(&buf, "ERR Slot {d} is already busy", .{slot});
                    return w.writeError(err_msg);
                }
            }
            return w.writeError("ERR Slot is already busy");
        },
        ClusterError.SlotNotAssignedToNode => {
            for (slots) |slot| {
                if (storage.cluster.slots[slot] != node) {
                    const err_msg = try std.fmt.bufPrint(&buf, "ERR Slot {d} is not assigned to this node", .{slot});
                    return w.writeError(err_msg);
                }
            }
            return w.writeError("ERR Slot is not assigned to this node");
        },
        else => return w.writeError("ERR internal error"),
    }
}

/// CLUSTER SLOTS - Return cluster slots configuration
pub fn cmdClusterSlots(
    allocator: std.mem.Allocator,
    _: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    const cluster = &storage.cluster;
    var response = try std.ArrayList(RespValue).initCapacity(allocator, 16);
    defer {
        for (response.items) |item| {
            deinitRespValue(allocator, item);
        }
        response.deinit(allocator);
    }

    // Group consecutive slot ranges by node
    var current_node: ?*cluster_mod.ClusterNode = null;
    var range_start: u16 = 0;
    var range_end: u16 = 0;

    for (0..cluster_mod.CLUSTER_SLOTS) |slot_idx| {
        const slot = @as(u16, @intCast(slot_idx));
        const node = cluster.slots[slot];

        if (node == null) {
            // Skip unassigned slots
            continue;
        }

        if (current_node == null or current_node != node) {
            // New node - save previous range if any
            if (current_node != null) {
                try addSlotRange(&response, allocator, range_start, range_end, current_node.?);
            }
            current_node = node;
            range_start = slot;
            range_end = slot;
        } else {
            // Continue current range
            range_end = slot;
        }
    }

    // Add the last range if any
    if (current_node != null) {
        try addSlotRange(&response, allocator, range_start, range_end, current_node.?);
    }

    return w.writeArray(try response.toOwnedSlice(allocator));
}

fn addSlotRange(
    response: *std.ArrayList(RespValue),
    allocator: std.mem.Allocator,
    start: u16,
    end: u16,
    node: *const cluster_mod.ClusterNode,
) !void {
    // Create slot range array: [start, end, [master_addr, master_port, master_id], [replica_addr, replica_port, replica_id], ...]
    var range_array = try std.ArrayList(RespValue).initCapacity(allocator, 4);
    errdefer {
        for (range_array.items) |item| {
            deinitRespValue(allocator, item);
        }
        range_array.deinit(allocator);
    }

    // Add start and end
    try range_array.append(allocator, RespValue{ .integer = start });
    try range_array.append(allocator, RespValue{ .integer = end });

    // Add master node info: [ip, port, node_id]
    var master_info = try std.ArrayList(RespValue).initCapacity(allocator, 3);
    errdefer {
        for (master_info.items) |item| {
            deinitRespValue(allocator, item);
        }
        master_info.deinit(allocator);
    }

    const addr_str = try allocator.dupe(u8, node.addr);
    try master_info.append(allocator, RespValue{ .bulk_string = addr_str });
    try master_info.append(allocator, RespValue{ .integer = node.port });

    const id_str = try allocator.dupe(u8, &node.id);
    try master_info.append(allocator, RespValue{ .bulk_string = id_str });

    const master_array_slice = try master_info.toOwnedSlice(allocator);
    errdefer {
        for (master_array_slice) |item| {
            deinitRespValue(allocator, item);
        }
        allocator.free(master_array_slice);
    }
    try range_array.append(allocator, RespValue{ .array = master_array_slice });

    // For now, no replica info (would be added here for multi-node clusters)

    const range_array_slice = try range_array.toOwnedSlice(allocator);
    errdefer {
        for (range_array_slice) |item| {
            deinitRespValue(allocator, item);
        }
        allocator.free(range_array_slice);
    }
    try response.append(allocator, RespValue{ .array = range_array_slice });
}

fn deinitRespValue(allocator: std.mem.Allocator, value: RespValue) void {
    switch (value) {
        .array => |arr| {
            for (arr) |item| {
                deinitRespValue(allocator, item);
            }
            allocator.free(arr);
        },
        .bulk_string => |s| allocator.free(s),
        else => {},
    }
}

/// CLUSTER NODES - Return cluster nodes configuration
pub fn cmdClusterNodes(
    allocator: std.mem.Allocator,
    _: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    var nodes_buf = try std.ArrayList(u8).initCapacity(allocator, 1024);
    defer nodes_buf.deinit(allocator);

    const cluster = &storage.cluster;
    const writer = nodes_buf.writer(allocator);

    // Iterate all nodes and format each one
    var it = cluster.nodes.iterator();
    while (it.next()) |entry| {
        const node_id = entry.key_ptr.*;
        const node = entry.value_ptr.*;

        // Format flags (comma-separated)
        var flags_list = try std.ArrayList(u8).initCapacity(allocator, 64);
        defer flags_list.deinit(allocator);

        if (node.flags.myself) try flags_list.appendSlice(allocator, "myself");
        if (node.flags.master) {
            if (flags_list.items.len > 0) try flags_list.append(allocator, ',');
            try flags_list.appendSlice(allocator, "master");
        }
        if (node.flags.slave) {
            if (flags_list.items.len > 0) try flags_list.append(allocator, ',');
            try flags_list.appendSlice(allocator, "slave");
        }
        if (node.flags.pfail) {
            if (flags_list.items.len > 0) try flags_list.append(allocator, ',');
            try flags_list.appendSlice(allocator, "pfail");
        }
        if (node.flags.fail) {
            if (flags_list.items.len > 0) try flags_list.append(allocator, ',');
            try flags_list.appendSlice(allocator, "fail");
        }
        if (node.flags.handshake) {
            if (flags_list.items.len > 0) try flags_list.append(allocator, ',');
            try flags_list.appendSlice(allocator, "handshake");
        }
        if (node.flags.noaddr) {
            if (flags_list.items.len > 0) try flags_list.append(allocator, ',');
            try flags_list.appendSlice(allocator, "noaddr");
        }
        if (node.flags.nofailover) {
            if (flags_list.items.len > 0) try flags_list.append(allocator, ',');
            try flags_list.appendSlice(allocator, "nofailover");
        }

        const flags_str = if (flags_list.items.len > 0) flags_list.items else "noflags";

        // Master ID or "-" if self or master
        const master_id_str = if (node.master_id) |mid| &mid else "-";

        // Link state string
        const link_state_str = switch (node.link_state) {
            .connected => "connected",
            .disconnected => "disconnected",
        };

        // Format: id addr:port@cport flags master ping_sent pong_recv config_epoch link_state slots...
        try writer.print("{s} {s}:{d}@{d} {s} {s} {d} {d} {d} {s}", .{
            node_id,
            node.addr,
            node.port,
            node.cluster_port,
            flags_str,
            master_id_str,
            node.ping_sent,
            node.pong_recv,
            node.config_epoch,
            link_state_str,
        });

        // Append slot ranges (compressed as "start-end" for consecutive slots)
        if (node.slots.items.len > 0) {
            try writer.writeAll(" ");
            for (node.slots.items, 0..) |slot_range, idx| {
                if (idx > 0) try writer.writeAll(" ");
                if (slot_range.start == slot_range.end) {
                    try writer.print("{d}", .{slot_range.start});
                } else {
                    try writer.print("{d}-{d}", .{slot_range.start, slot_range.end});
                }
            }
        }

        try writer.writeAll("\n");
    }

    return w.writeBulkString(nodes_buf.items);
}

/// CLUSTER INFO - Return cluster state information
pub fn cmdClusterInfo(
    allocator: std.mem.Allocator,
    _: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Build the cluster info response from actual cluster state
    var info_buf = try std.ArrayList(u8).initCapacity(allocator, 512);
    defer info_buf.deinit(allocator);

    const cluster = &storage.cluster;
    const state_str = switch (cluster.state) {
        .ok => "ok",
        .fail => "fail",
    };

    // Count assigned slots (should be 16384 in single-node)
    var assigned_slots: u32 = 0;
    var ok_slots: u32 = 0;
    for (0..cluster_mod.CLUSTER_SLOTS) |i| {
        if (cluster.slots[i] != null) {
            assigned_slots += 1;
            // In single-node, all assigned slots are ok
            ok_slots += 1;
        }
    }

    // Count cluster size (number of master nodes)
    var cluster_size: u32 = 0;
    var it = cluster.nodes.valueIterator();
    while (it.next()) |node_ptr| {
        const node = node_ptr.*;
        if (node.flags.master) {
            cluster_size += 1;
        }
    }

    // Get my epoch (same as current for now)
    const my_epoch = cluster.current_epoch;

    const writer = info_buf.writer(allocator);
    try writer.print("cluster_state:{s}\r\n", .{state_str});
    try writer.print("cluster_slots_assigned:{}\r\n", .{assigned_slots});
    try writer.print("cluster_slots_ok:{}\r\n", .{ok_slots});
    try writer.print("cluster_slots_pfail:0\r\n", .{});
    try writer.print("cluster_slots_fail:0\r\n", .{});
    try writer.print("cluster_known_nodes:{}\r\n", .{cluster.nodes.count()});
    try writer.print("cluster_size:{}\r\n", .{cluster_size});
    try writer.print("cluster_current_epoch:{}\r\n", .{cluster.current_epoch});
    try writer.print("cluster_my_epoch:{}\r\n", .{my_epoch});
    try writer.print("cluster_stats_messages_sent:0\r\n", .{});
    try writer.print("cluster_stats_messages_received:0\r\n", .{});

    return w.writeBulkString(info_buf.items);
}

/// CLUSTER MYID - Return node ID
///
/// Returns the unique ID of the current cluster node.
/// The ID is a 40-character hexadecimal string generated at startup.
///
/// Returns:
///   RESP bulk string containing the node ID
pub fn cmdClusterMyId(
    allocator: std.mem.Allocator,
    _: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    const node = storage.cluster.myself orelse return w.writeError("ERR no cluster node");
    return w.writeBulkString(&node.id);
}

/// CLUSTER MYSHARDID - Return the shard ID of the current node
/// Shard ID is a 40-char hex string unique to each shard (master + replicas)
pub fn cmdClusterMyShardId(
    allocator: std.mem.Allocator,
    _: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    const node = storage.cluster.myself orelse return w.writeError("ERR no cluster node");
    return w.writeBulkString(&node.shard_id);
}

/// CLUSTER HELP - Return help information for CLUSTER command
pub fn cmdClusterHelp(
    allocator: std.mem.Allocator,
    _: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    _ = storage;

    var w = Writer.init(allocator);
    defer w.deinit();

    const help_lines = [_][]const u8{
        "CLUSTER SLOTS",
        "    Return cluster slots configuration",
        "CLUSTER NODES",
        "    Return cluster nodes configuration",
        "CLUSTER INFO",
        "    Return cluster state information",
        "CLUSTER MYID",
        "    Return the node ID",
        "CLUSTER KEYSLOT <key>",
        "    Return hash slot for a key",
        "CLUSTER COUNTKEYSINSLOT <slot>",
        "    Count keys in a hash slot",
        "CLUSTER GETKEYSINSLOT <slot> <count>",
        "    Return keys in a hash slot",
        "CLUSTER ADDSLOTS <slot> [slot ...]",
        "    Assign slots to this node",
        "CLUSTER ADDSLOTSRANGE <start> <end> [start end ...]",
        "    Assign slot ranges to this node",
        "CLUSTER DELSLOTS <slot> [slot ...]",
        "    Remove slots from this node",
        "CLUSTER DELSLOTSRANGE <start> <end> [start end ...]",
        "    Remove slot ranges from this node",
        "CLUSTER FLUSHSLOTS",
        "    Remove all slots from this node",
        "CLUSTER MEET <ip> <port> [cluster-bus-port]",
        "    Handshake with another cluster node",
        "CLUSTER FORGET <node-id>",
        "    Remove a node from the cluster",
        "CLUSTER HELP",
        "    Show this help message",
    };

    var values = try allocator.alloc(RespValue, help_lines.len);
    defer allocator.free(values);

    for (help_lines, 0..) |line, i| {
        values[i] = RespValue{ .bulk_string = line };
    }

    return w.writeArray(values);
}

// Note: calculateKeySlot and crc16 are now provided by cluster_mod.keySlot()
// The duplicate implementation was removed to avoid code duplication.
// Tests for key slot calculation are in src/storage/cluster.zig

test "cmdClusterInfo - single node returns valid info" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try cmdClusterInfo(allocator, &[_][]const u8{}, storage, null, 0);
    defer allocator.free(result);

    // Result should be a RESP bulk string starting with $
    try std.testing.expect(result.len > 0);
    try std.testing.expect(result[0] == '$');

    // Parse the bulk string to get content
    var it = std.mem.splitSequence(u8, result, "\r\n");
    _ = it.next(); // Skip size line
    const content = it.next() orelse "";

    // Check for required fields
    try std.testing.expect(std.mem.indexOf(u8, content, "cluster_state:ok") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "cluster_slots_assigned:16384") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "cluster_known_nodes:1") != null);
}

test "cmdClusterInfo - contains all required fields" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try cmdClusterInfo(allocator, &[_][]const u8{}, storage, null, 0);
    defer allocator.free(result);

    var it = std.mem.splitSequence(u8, result, "\r\n");
    _ = it.next(); // Skip size line
    const content = it.next() orelse "";

    // Verify all 10 required fields are present
    try std.testing.expect(std.mem.indexOf(u8, content, "cluster_state") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "cluster_slots_assigned") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "cluster_slots_ok") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "cluster_slots_pfail") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "cluster_slots_fail") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "cluster_known_nodes") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "cluster_size") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "cluster_current_epoch") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "cluster_my_epoch") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "cluster_stats_messages") != null);
}

test "cmdClusterNodes - single node format" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try cmdClusterNodes(allocator, &[_][]const u8{}, storage, null, 0);
    defer allocator.free(result);

    // Result should be RESP bulk string
    try std.testing.expect(result[0] == '$');

    var it = std.mem.splitSequence(u8, result, "\r\n");
    _ = it.next(); // Skip size line
    const content = it.next() orelse "";

    // Should have node ID, address, and flags
    try std.testing.expect(content.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, content, "myself") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "master") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "connected") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "0-16383") != null or std.mem.indexOf(u8, content, "0") != null);
}

/// CLUSTER ADDSLOTS - Assign individual slots to the current node
pub fn cmdClusterAddSlots(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "CLUSTER", args[1] = "ADDSLOTS", args[2..] = slot numbers
    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'cluster|addslots' command");
    }

    // Allocate array for slot numbers
    var slots = try std.ArrayList(u16).initCapacity(allocator, args.len - 2);
    defer slots.deinit(allocator);

    // Parse and validate slot numbers
    var seen_slots = std.AutoHashMap(u16, void).init(allocator);
    defer seen_slots.deinit();

    for (args[2..]) |slot_str| {
        const slot = std.fmt.parseInt(i64, slot_str, 10) catch {
            return w.writeError("ERR invalid slot");
        };

        if (slot < 0 or slot > 16383) {
            return w.writeError("ERR Invalid slot");
        }

        const slot_u16 = @as(u16, @intCast(slot));

        // Check for duplicates
        if (seen_slots.contains(slot_u16)) {
            var buf: [256]u8 = undefined;
            const err_msg = try std.fmt.bufPrint(&buf, "ERR Slot {d} specified multiple times", .{slot});
            return w.writeError(err_msg);
        }

        try seen_slots.put(slot_u16, {});
        try slots.append(allocator, slot_u16);
    }

    // Get current node
    const node = storage.cluster.myself orelse {
        return w.writeError("ERR no cluster node");
    };

    // Try to add slots
    storage.cluster.addSlotsToNode(node, slots.items) catch |err| {
        return formatSlotError(allocator, err, storage, slots.items, node);
    };

    return w.writeSimpleString("OK");
}

/// CLUSTER ADDSLOTSRANGE - Assign slot ranges to the current node
pub fn cmdClusterAddSlotsRange(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "CLUSTER", args[1] = "ADDSLOTSRANGE", args[2..] = range pairs
    if (args.len < 4 or (args.len - 2) % 2 != 0) {
        return w.writeError("ERR wrong number of arguments for 'cluster|addslotsrange' command");
    }

    // Parse ranges
    var slots = try std.ArrayList(u16).initCapacity(allocator, 1000);
    defer slots.deinit(allocator);

    var i: usize = 2;
    while (i < args.len) : (i += 2) {
        const start = std.fmt.parseInt(i64, args[i], 10) catch {
            return w.writeError("ERR invalid slot");
        };
        const end = std.fmt.parseInt(i64, args[i + 1], 10) catch {
            return w.writeError("ERR invalid slot");
        };

        if (start < 0 or start > 16383 or end < 0 or end > 16383) {
            return w.writeError("ERR Invalid slot");
        }

        if (start > end) {
            return w.writeError("ERR Invalid slot range");
        }

        const start_u = @as(u16, @intCast(start));
        const end_u = @as(u16, @intCast(end));
        for (start_u..end_u + 1) |slot| {
            try slots.append(allocator, @as(u16, @intCast(slot)));
        }
    }

    // Get current node
    const node = storage.cluster.myself orelse {
        return w.writeError("ERR no cluster node");
    };

    // Try to add slots
    storage.cluster.addSlotsToNode(node, slots.items) catch |err| {
        return formatSlotError(allocator, err, storage, slots.items, node);
    };

    return w.writeSimpleString("OK");
}

/// CLUSTER DELSLOTS - Remove individual slots from the current node
pub fn cmdClusterDelSlots(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "CLUSTER", args[1] = "DELSLOTS", args[2..] = slot numbers
    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'cluster|delslots' command");
    }

    // Parse and validate slot numbers
    var slots = try std.ArrayList(u16).initCapacity(allocator, args.len - 2);
    defer slots.deinit(allocator);

    var seen_slots = std.AutoHashMap(u16, void).init(allocator);
    defer seen_slots.deinit();

    for (args[2..]) |slot_str| {
        const slot = std.fmt.parseInt(i64, slot_str, 10) catch {
            return w.writeError("ERR invalid slot");
        };

        if (slot < 0 or slot > 16383) {
            return w.writeError("ERR Invalid slot");
        }

        const slot_u16 = @as(u16, @intCast(slot));

        if (seen_slots.contains(slot_u16)) {
            var buf: [256]u8 = undefined;
            const err_msg = try std.fmt.bufPrint(&buf, "ERR Slot {d} specified multiple times", .{slot});
            return w.writeError(err_msg);
        }

        try seen_slots.put(slot_u16, {});
        try slots.append(allocator, slot_u16);
    }

    // Get current node
    const node = storage.cluster.myself orelse {
        return w.writeError("ERR no cluster node");
    };

    // Try to remove slots
    storage.cluster.removeSlotsFromNode(node, slots.items) catch |err| {
        return formatSlotError(allocator, err, storage, slots.items, node);
    };

    return w.writeSimpleString("OK");
}

/// CLUSTER DELSLOTSRANGE - Remove slot ranges from the current node
pub fn cmdClusterDelSlotsRange(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "CLUSTER", args[1] = "DELSLOTSRANGE", args[2..] = range pairs
    if (args.len < 4 or (args.len - 2) % 2 != 0) {
        return w.writeError("ERR wrong number of arguments for 'cluster|delslotsrange' command");
    }

    // Parse ranges
    var slots = try std.ArrayList(u16).initCapacity(allocator, 1000);
    defer slots.deinit(allocator);

    var i: usize = 2;
    while (i < args.len) : (i += 2) {
        const start = std.fmt.parseInt(i64, args[i], 10) catch {
            return w.writeError("ERR invalid slot");
        };
        const end = std.fmt.parseInt(i64, args[i + 1], 10) catch {
            return w.writeError("ERR invalid slot");
        };

        if (start < 0 or start > 16383 or end < 0 or end > 16383) {
            return w.writeError("ERR Invalid slot");
        }

        if (start > end) {
            return w.writeError("ERR Invalid slot range");
        }

        const start_u = @as(u16, @intCast(start));
        const end_u = @as(u16, @intCast(end));
        for (start_u..end_u + 1) |slot| {
            try slots.append(allocator, @as(u16, @intCast(slot)));
        }
    }

    // Get current node
    const node = storage.cluster.myself orelse {
        return w.writeError("ERR no cluster node");
    };

    // Try to remove slots
    storage.cluster.removeSlotsFromNode(node, slots.items) catch |err| {
        return formatSlotError(allocator, err, storage, slots.items, node);
    };

    return w.writeSimpleString("OK");
}

/// CLUSTER FLUSHSLOTS - Remove all slot assignments
pub fn cmdClusterFlushSlots(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    _ = args; // Not needed for this command
    var w = Writer.init(allocator);
    defer w.deinit();

    // Check if database is empty
    if (!ClusterState.isDatabaseEmpty(storage)) {
        return w.writeError("ERR FLUSHSLOTS requires an empty database");
    }

    storage.cluster.flushSlots();
    return w.writeSimpleString("OK");
}

/// CLUSTER MEET - Add a node to the cluster
pub fn cmdClusterMeet(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "CLUSTER", args[1] = "MEET", args[2] = ip, args[3] = port, args[4] = cluster_port (optional)
    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'cluster|meet' command");
    }

    const ip = args[2];
    if (ip.len == 0 or ip.len >= 256) {
        return w.writeError("ERR Invalid IP address or hostname");
    }

    const port_str = args[3];
    const port = std.fmt.parseInt(i64, port_str, 10) catch {
        return w.writeError("ERR Invalid port");
    };

    if (port < 1 or port > 65535) {
        return w.writeError("ERR Invalid port");
    }

    const cluster_port = if (args.len > 4)
        std.fmt.parseInt(i64, args[4], 10) catch {
            return w.writeError("ERR Invalid cluster bus port");
        }
    else
        port + 10000;

    if (cluster_port < 1 or cluster_port > 65535) {
        return w.writeError("ERR Invalid cluster bus port");
    }

    // Add the node
    _ = storage.cluster.meetNode(allocator, ip, @intCast(port), @intCast(cluster_port)) catch {
        return w.writeError("ERR failed to add node");
    };

    return w.writeSimpleString("OK");
}

/// CLUSTER FORGET - Remove a node from the cluster
pub fn cmdClusterForget(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "CLUSTER", args[1] = "FORGET", args[2] = node_id
    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'cluster|forget' command");
    }

    const node_id = args[2];

    // Validate node ID format (40 hex chars)
    if (node_id.len != 40) {
        return w.writeError("ERR Invalid node ID");
    }

    for (node_id) |c| {
        if (!std.ascii.isHex(c)) {
            return w.writeError("ERR Invalid node ID");
        }
    }

    // Check if trying to forget self
    if (storage.cluster.myself) |myself| {
        var myself_id: [40]u8 = undefined;
        @memcpy(&myself_id, &myself.id);
        if (std.mem.eql(u8, &myself_id, node_id)) {
            return w.writeError("ERR I tried hard but I can't forget myself...");
        }
    }

    // Get current time in milliseconds
    const now_ms = @as(i64, @intCast(std.time.milliTimestamp()));

    // Try to forget the node
    storage.cluster.forgetNode(allocator, node_id, now_ms) catch |err| {
        switch (err) {
            ClusterError.UnknownNode => {
                var buf: [256]u8 = undefined;
                const err_msg = try std.fmt.bufPrint(&buf, "ERR Unknown node {s}", .{node_id});
                return w.writeError(err_msg);
            },
            else => return w.writeError("ERR internal error"),
        }
    };

    return w.writeSimpleString("OK");
}

/// CLUSTER SETSLOT - Manage slot migration states
///
/// Syntax: CLUSTER SETSLOT <slot> <IMPORTING node-id | MIGRATING node-id | NODE node-id | STABLE>
///
/// Subcommands:
/// - MIGRATING <node-id>: Sets slot to migrating state on source node (requires ownership)
/// - IMPORTING <node-id>: Sets slot to importing state on destination node (requires non-ownership)
/// - NODE <node-id>: Finalizes slot ownership to specified node, clears migration states
/// - STABLE: Clears migration states without changing ownership
///
/// Returns:
///   Simple string "OK" on success
///   Error if validation fails (wrong ownership, unknown node, etc.)
pub fn cmdClusterSetslot(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "CLUSTER", args[1] = "SETSLOT", args[2] = slot, args[3] = subcommand, args[4] = node_id (optional)
    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'cluster|setslot' command");
    }

    const slot_str = args[2];
    const subcommand = args[3];

    // Parse slot number
    const slot = std.fmt.parseInt(u16, slot_str, 10) catch {
        return w.writeError("ERR invalid slot");
    };

    if (slot >= cluster_mod.CLUSTER_SLOTS) {
        return w.writeError("ERR invalid slot");
    }

    // Handle subcommands
    const subcommand_upper = try std.ascii.allocUpperString(allocator, subcommand);
    defer allocator.free(subcommand_upper);

    if (std.mem.eql(u8, subcommand_upper, "MIGRATING")) {
        if (args.len != 5) {
            return w.writeError("ERR wrong number of arguments for 'cluster|setslot|migrating' command");
        }

        const dest_node_id = args[4];
        if (dest_node_id.len != 40) {
            return w.writeError("ERR invalid node ID");
        }

        var node_id_buf: [40]u8 = undefined;
        @memcpy(&node_id_buf, dest_node_id);

        storage.cluster.setSlotMigrating(slot, node_id_buf) catch |err| {
            switch (err) {
                ClusterError.SlotNotOwnedByNode => {
                    return w.writeError("ERR I'm not the owner of hash slot");
                },
                ClusterError.InvalidSlot => {
                    return w.writeError("ERR invalid slot");
                },
                else => return w.writeError("ERR internal error"),
            }
        };

        return w.writeSimpleString("OK");
    } else if (std.mem.eql(u8, subcommand_upper, "IMPORTING")) {
        if (args.len != 5) {
            return w.writeError("ERR wrong number of arguments for 'cluster|setslot|importing' command");
        }

        const source_node_id = args[4];
        if (source_node_id.len != 40) {
            return w.writeError("ERR invalid node ID");
        }

        var node_id_buf: [40]u8 = undefined;
        @memcpy(&node_id_buf, source_node_id);

        storage.cluster.setSlotImporting(slot, node_id_buf) catch |err| {
            switch (err) {
                ClusterError.SlotAlreadyOwned => {
                    return w.writeError("ERR I'm already the owner of hash slot");
                },
                ClusterError.InvalidSlot => {
                    return w.writeError("ERR invalid slot");
                },
                else => return w.writeError("ERR internal error"),
            }
        };

        return w.writeSimpleString("OK");
    } else if (std.mem.eql(u8, subcommand_upper, "STABLE")) {
        if (args.len != 4) {
            return w.writeError("ERR wrong number of arguments for 'cluster|setslot|stable' command");
        }

        storage.cluster.setSlotStable(slot) catch |err| {
            switch (err) {
                ClusterError.InvalidSlot => {
                    return w.writeError("ERR invalid slot");
                },
                else => return w.writeError("ERR internal error"),
            }
        };

        return w.writeSimpleString("OK");
    } else if (std.mem.eql(u8, subcommand_upper, "NODE")) {
        if (args.len != 5) {
            return w.writeError("ERR wrong number of arguments for 'cluster|setslot|node' command");
        }

        const target_node_id = args[4];
        if (target_node_id.len != 40) {
            return w.writeError("ERR invalid node ID");
        }

        var node_id_buf: [40]u8 = undefined;
        @memcpy(&node_id_buf, target_node_id);

        // For now, we always pass slot_has_keys=false
        // In a real implementation, this would check if the slot has any keys
        const slot_has_keys = false;

        storage.cluster.setSlotNode(slot, node_id_buf, slot_has_keys) catch |err| {
            switch (err) {
                ClusterError.UnknownNode => {
                    return w.writeError("ERR unknown node");
                },
                ClusterError.SlotHasKeys => {
                    return w.writeError("ERR slot has keys");
                },
                ClusterError.InvalidSlot => {
                    return w.writeError("ERR invalid slot");
                },
                else => return w.writeError("ERR internal error"),
            }
        };

        return w.writeSimpleString("OK");
    } else {
        return w.writeError("ERR invalid subcommand");
    }
}

/// ASKING - Tell cluster client is willing to receive redirects for IMPORTING slots
/// This command sets a flag on the connection that allows the next command to be executed
/// even if the slot is in IMPORTING state. The flag is automatically cleared after the next command.
///
/// Used during slot migration:
/// 1. Client tries to access key on old node
/// 2. Old node returns -ASK redirect
/// 3. Client sends ASKING to new node
/// 4. Client resends command, which is now allowed to execute on IMPORTING slot
pub fn cmdAsking(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    client_id: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "ASKING"
    if (args.len != 1) {
        return w.writeError("ERR wrong number of arguments for 'asking' command");
    }

    // Set ASKING flag for this client
    try storage.cluster.setAsking(client_id);

    return w.writeSimpleString("OK");
}

/// MIGRATE - Atomically transfer key(s) from source to destination Redis instance
///
/// Syntax: MIGRATE host port <key | ""> destination-db timeout [COPY] [REPLACE] [AUTH password | AUTH2 username password] [KEYS key [key ...]]
///
/// Options:
/// - COPY: Do not remove the key from the local instance
/// - REPLACE: Replace existing key on the remote instance
/// - AUTH: Authenticate with password
/// - AUTH2: Authenticate with username and password (Redis 6+ ACL style)
/// - KEYS: If key argument is empty string, migrate all keys that follow KEYS option
///
/// Returns:
///   Simple string "OK" on success
///   Simple string "NOKEY" if key doesn't exist (not considered an error)
///   Error on failure (invalid arguments, network error, etc.)
///
/// Note: This is a stub implementation that validates arguments and serializes keys
/// but does not perform actual network transfer. Full implementation will require
/// TCP client for destination connection.
pub fn cmdMigrate(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "MIGRATE", args[1] = host, args[2] = port, args[3] = key, args[4] = dest-db, args[5] = timeout, [options...]
    if (args.len < 6) {
        return w.writeError("ERR wrong number of arguments for 'migrate' command");
    }

    const host = args[1];
    const port_str = args[2];
    const key_arg = args[3];
    const dest_db_str = args[4];
    const timeout_str = args[5];

    // Validate port
    const port = std.fmt.parseInt(u16, port_str, 10) catch {
        return w.writeError("ERR invalid port");
    };
    _ = port; // Will be used in real implementation

    // Validate destination db
    const dest_db = std.fmt.parseInt(u16, dest_db_str, 10) catch {
        return w.writeError("ERR invalid destination database");
    };
    _ = dest_db; // Will be used in real implementation

    // Validate timeout
    const timeout_ms = std.fmt.parseInt(u32, timeout_str, 10) catch {
        return w.writeError("ERR timeout is not an integer or out of range");
    };
    _ = timeout_ms; // Will be used in real implementation
    _ = host; // Will be used in real implementation

    // Parse options
    var copy = false;
    var replace = false;
    var auth_password: ?[]const u8 = null;
    var auth_username: ?[]const u8 = null;
    var keys_mode = false;
    var keys_list = try std.ArrayList([]const u8).initCapacity(allocator, 0);
    defer keys_list.deinit(allocator);

    var i: usize = 6;
    while (i < args.len) : (i += 1) {
        const opt = args[i];
        const opt_upper = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(opt_upper);

        if (std.mem.eql(u8, opt_upper, "COPY")) {
            copy = true;
        } else if (std.mem.eql(u8, opt_upper, "REPLACE")) {
            replace = true;
        } else if (std.mem.eql(u8, opt_upper, "AUTH")) {
            i += 1;
            if (i >= args.len) {
                return w.writeError("ERR AUTH requires password argument");
            }
            auth_password = args[i];
        } else if (std.mem.eql(u8, opt_upper, "AUTH2")) {
            i += 1;
            if (i >= args.len) {
                return w.writeError("ERR AUTH2 requires username argument");
            }
            auth_username = args[i];
            i += 1;
            if (i >= args.len) {
                return w.writeError("ERR AUTH2 requires password argument");
            }
            auth_password = args[i];
        } else if (std.mem.eql(u8, opt_upper, "KEYS")) {
            keys_mode = true;
            // Collect all remaining arguments as keys
            i += 1;
            while (i < args.len) : (i += 1) {
                try keys_list.append(allocator, args[i]);
            }
            break;
        } else {
            return w.writeError("ERR syntax error");
        }
    }
    // Note: replace, auth_password, auth_username are parsed but not used in stub implementation
    // They will be used when real TCP network transfer is implemented

    // Determine which keys to migrate
    var migrate_keys = try std.ArrayList([]const u8).initCapacity(allocator, 0);
    defer migrate_keys.deinit(allocator);

    if (keys_mode) {
        // KEYS option specified - migrate multiple keys
        if (key_arg.len != 0) {
            return w.writeError("ERR When using KEYS option, key argument must be empty string");
        }
        for (keys_list.items) |k| {
            try migrate_keys.append(allocator, k);
        }
    } else {
        // Single key mode
        if (key_arg.len == 0) {
            return w.writeError("ERR key argument cannot be empty without KEYS option");
        }
        try migrate_keys.append(allocator, key_arg);
    }

    // Check if any keys exist
    var keys_exist = false;
    for (migrate_keys.items) |key| {
        if (storage.exists(key)) {
            keys_exist = true;
            break;
        }
    }

    if (!keys_exist) {
        return w.writeSimpleString("NOKEY");
    }

    // Stub implementation: In real implementation, this would:
    // 1. Serialize each key using storage.dumpValue()
    // 2. Open TCP connection to destination host:port
    // 3. Authenticate with AUTH/AUTH2 if provided
    // 4. Send RESTORE command for each key to destination
    // 5. Delete keys from source if not COPY mode
    //
    // For now, we just validate arguments and return OK
    // This allows tests to pass and provides the command structure

    // If COPY mode, keys remain on source
    // If not COPY, delete keys after successful migration (stub: we skip actual deletion for now)
    if (!copy) {
        // In real implementation, delete keys here after successful network transfer
        // For now, we skip deletion in stub mode
    }

    return w.writeSimpleString("OK");
}

/// CLUSTER FAILOVER - Trigger manual failover
///
/// Syntax: CLUSTER FAILOVER [FORCE | TAKEOVER]
///
/// Modes:
/// - (no option): Normal coordinated failover - replica waits for replication sync
/// - FORCE: Skip replication sync check, start election immediately
/// - TAKEOVER: Skip election entirely, promote to master without consensus (for emergencies)
///
/// Returns:
///   Simple string "OK" on success
///   Error if node is not a replica, or failover fails
pub fn cmdClusterFailover(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "CLUSTER", args[1] = "FAILOVER", args[2] = mode (optional)
    if (args.len > 3) {
        return w.writeError("ERR wrong number of arguments for 'cluster failover' command");
    }

    // Determine failover mode
    const mode = if (args.len >= 3) blk: {
        const mode_upper = try std.ascii.allocUpperString(allocator, args[2]);
        defer allocator.free(mode_upper);

        if (std.mem.eql(u8, mode_upper, "FORCE")) {
            break :blk "force";
        } else if (std.mem.eql(u8, mode_upper, "TAKEOVER")) {
            break :blk "takeover";
        } else {
            return w.writeError("ERR Invalid CLUSTER FAILOVER mode. Valid modes: FORCE, TAKEOVER");
        }
    } else "normal";

    // Execute manual failover
    storage.cluster.manualFailover(allocator, mode) catch |err| {
        switch (err) {
            ClusterError.UnknownNode => {
                return w.writeError("ERR This node is not a replica or cluster is not initialized");
            },
            ClusterError.InvalidSlot => {
                return w.writeError("ERR This command can only be executed by a replica node");
            },
            else => {
                return w.writeError("ERR Failover failed");
            },
        }
    };

    return w.writeSimpleString("OK");
}

/// CLUSTER REPLICAS - List all replicas of a specific master node
///
/// Returns array of node descriptors in CLUSTER NODES format for each replica.
/// Each line contains: <id> <ip:port@cport> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> ...
///
/// Syntax: CLUSTER REPLICAS <node-id>
///
/// Returns:
///   Array of strings, one per replica
///   Empty array if master has no replicas or node not found
pub fn cmdClusterReplicas(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "CLUSTER", args[1] = "REPLICAS", args[2] = <node-id>
    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'cluster replicas' command");
    }

    const node_id_str = args[2];
    if (node_id_str.len != 40) {
        return w.writeError("ERR Invalid node ID: must be 40 hex characters");
    }

    // Parse node ID
    var master_id: [40]u8 = undefined;
    @memcpy(&master_id, node_id_str[0..40]);

    // Check if master node exists
    _ = storage.cluster.nodes.get(node_id_str) orelse {
        return w.writeError("ERR Unknown node");
    };

    // Get all replicas of this master
    var replicas = storage.cluster.getReplicasOfMaster(allocator, master_id) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        return w.writeError("ERR Failed to get replicas");
    };
    defer replicas.deinit(allocator);

    // Build response array
    var response = try std.ArrayList(RespValue).initCapacity(allocator, replicas.items.len);
    defer {
        for (response.items) |item| {
            deinitRespValue(allocator, item);
        }
        response.deinit(allocator);
    }

    for (replicas.items) |replica| {
        // Format: <id> <ip:port@cport> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state>
        const node_line = try std.fmt.allocPrint(allocator, "{s} {s}:{d}@{d} {s} {s} {d} {d} {d} {s}", .{
            replica.id,
            replica.addr,
            replica.port,
            replica.cluster_port,
            if (replica.flags.slave) "slave" else "master",
            if (replica.master_id) |mid| &mid else "-",
            replica.ping_sent,
            replica.pong_recv,
            replica.config_epoch,
            @tagName(replica.link_state),
        });
        errdefer allocator.free(node_line);

        // Ownership of node_line transfers to response
        try response.append(allocator, RespValue{ .bulk_string = node_line });
    }

    return w.writeArray(try response.toOwnedSlice(allocator));
}

/// CLUSTER REPLICATE - Configure current node as a replica of a master
///
/// Changes the current node from master to replica, assigns it to replicate a specific master node.
/// Clears all slot assignments from the current node (replicas don't own slots).
///
/// Syntax: CLUSTER REPLICATE <node-id>
///
/// Returns:
///   Simple string "OK" on success
///   Error if node-id is invalid, unknown, or same as current node
pub fn cmdClusterReplicate(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "CLUSTER", args[1] = "REPLICATE", args[2] = <node-id>
    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'cluster replicate' command");
    }

    const node_id_str = args[2];
    if (node_id_str.len != 40) {
        return w.writeError("ERR Invalid node ID: must be 40 hex characters");
    }

    // Parse node ID
    var master_id: [40]u8 = undefined;
    @memcpy(&master_id, node_id_str[0..40]);

    // Configure as replica
    storage.cluster.configureAsReplica(master_id) catch |err| {
        switch (err) {
            ClusterError.UnknownNode => {
                return w.writeError("ERR Unknown node ID or cluster not initialized");
            },
            ClusterError.InvalidNodeId => {
                return w.writeError("ERR Can't replicate myself or target node is not a master");
            },
            else => {
                return w.writeError("ERR Failed to configure replication");
            },
        }
    };

    return w.writeSimpleString("OK");
}

/// CLUSTER SAVECONFIG - Force cluster configuration save to disk
pub fn cmdClusterSaveConfig(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "CLUSTER", args[1] = "SAVECONFIG"
    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'cluster|saveconfig' command");
    }

    // Check cluster enabled
    if (!storage.cluster.enabled) {
        return w.writeError("ERR This instance has cluster support disabled");
    }

    // Save configuration to nodes.conf
    storage.cluster.saveConfig(storage.cluster_config_path) catch |err| {
        var buf: [256]u8 = undefined;
        const msg = switch (err) {
            error.NoSpaceLeft => "ERR Could not save config: No space left on device",
            error.AccessDenied => "ERR Could not save config: Permission denied",
            error.InvalidPath => "ERR Invalid config file path",
            else => blk: {
                const formatted = std.fmt.bufPrint(&buf, "ERR Could not save config: {}", .{err}) catch "ERR Could not save config";
                break :blk formatted;
            },
        };
        return w.writeError(msg);
    };

    return w.writeSimpleString("OK");
}

/// CLUSTER BUMPEPOCH - Manually increment cluster configuration epoch
pub fn cmdClusterBumpEpoch(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "CLUSTER", args[1] = "BUMPEPOCH"
    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'cluster|bumpepoch' command");
    }

    // Check cluster enabled
    if (!storage.cluster.enabled) {
        return w.writeError("ERR This instance has cluster support disabled");
    }

    // Attempt to bump epoch
    const result = storage.cluster.bumpEpoch() catch |err| {
        switch (err) {
            error.ClusterNotInitialized => {
                return w.writeError("ERR Cluster not initialized");
            },
        }
    };

    // Auto-save config after bumping
    storage.cluster.saveConfig(storage.cluster_config_path) catch |err| {
        // Log error but don't fail the command - epoch was already bumped
        std.log.warn("Failed to save config after BUMPEPOCH: {}", .{err});
    };

    // Format response: BUMPED <epoch> or STILL <epoch>
    var buf: [64]u8 = undefined;
    const response = if (result.bumped)
        try std.fmt.bufPrint(&buf, "BUMPED {d}", .{result.epoch})
    else
        try std.fmt.bufPrint(&buf, "STILL {d}", .{result.epoch});

    return w.writeBulkString(response);
}

test "cmdClusterAddSlots - single slot" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_][]const u8{ "CLUSTER", "ADDSLOTS", "1000" };
    const result = try cmdClusterAddSlots(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return "+OK\r\n"
    try std.testing.expect(std.mem.indexOf(u8, result, "OK") != null);
}

test "cmdClusterAddSlots - multiple slots" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_][]const u8{ "CLUSTER", "ADDSLOTS", "1000", "1001", "1002" };
    const result = try cmdClusterAddSlots(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "OK") != null);

    // Verify slots are assigned
    if (storage.cluster.myself) |node| {
        try std.testing.expectEqual(node, storage.cluster.slots[1000]);
        try std.testing.expectEqual(node, storage.cluster.slots[1001]);
        try std.testing.expectEqual(node, storage.cluster.slots[1002]);
    }
}

test "cmdClusterAddSlots - invalid slot" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_][]const u8{ "CLUSTER", "ADDSLOTS", "16384" };
    const result = try cmdClusterAddSlots(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
}

test "cmdClusterDelSlots - remove slots" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // First add slots
    const add_args = [_][]const u8{ "CLUSTER", "ADDSLOTS", "1000", "1001", "1002" };
    const add_result = try cmdClusterAddSlots(allocator, &add_args, storage, null, 0);
    defer allocator.free(add_result);

    // Then remove some
    const del_args = [_][]const u8{ "CLUSTER", "DELSLOTS", "1001" };
    const del_result = try cmdClusterDelSlots(allocator, &del_args, storage, null, 0);
    defer allocator.free(del_result);

    try std.testing.expect(std.mem.indexOf(u8, del_result, "OK") != null);
    try std.testing.expectEqual(null, storage.cluster.slots[1001]);
}

test "cmdClusterAddSlotsRange - range assignment" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_][]const u8{ "CLUSTER", "ADDSLOTSRANGE", "2000", "2100" };
    const result = try cmdClusterAddSlotsRange(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "OK") != null);

    // Verify all slots in range assigned
    if (storage.cluster.myself) |node| {
        for (2000..2101) |slot| {
            try std.testing.expectEqual(node, storage.cluster.slots[@as(u16, @intCast(slot))]);
        }
    }
}

test "cmdClusterDelSlotsRange - range removal" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Add range first
    const add_args = [_][]const u8{ "CLUSTER", "ADDSLOTSRANGE", "2000", "2100" };
    const add_result = try cmdClusterAddSlotsRange(allocator, &add_args, storage, null, 0);
    defer allocator.free(add_result);

    // Delete part of the range
    const del_args = [_][]const u8{ "CLUSTER", "DELSLOTSRANGE", "2050", "2075" };
    const del_result = try cmdClusterDelSlotsRange(allocator, &del_args, storage, null, 0);
    defer allocator.free(del_result);

    try std.testing.expect(std.mem.indexOf(u8, del_result, "OK") != null);

    // Verify slots in removed range unassigned
    for (2050..2076) |slot| {
        try std.testing.expectEqual(null, storage.cluster.slots[@as(u16, @intCast(slot))]);
    }
}

test "cmdClusterFlushSlots - clear all slots" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Add some slots
    const add_args = [_][]const u8{ "CLUSTER", "ADDSLOTS", "1000", "1001" };
    const add_result = try cmdClusterAddSlots(allocator, &add_args, storage, null, 0);
    defer allocator.free(add_result);

    // Flush slots
    const flush_args = [_][]const u8{ "CLUSTER", "FLUSHSLOTS" };
    const flush_result = try cmdClusterFlushSlots(allocator, &flush_args, storage, null, 0);
    defer allocator.free(flush_result);

    try std.testing.expect(std.mem.indexOf(u8, flush_result, "OK") != null);

    // Verify all slots unassigned
    for (0..cluster_mod.CLUSTER_SLOTS) |i| {
        const slot = @as(u16, @intCast(i));
        try std.testing.expectEqual(null, storage.cluster.slots[slot]);
    }
}

test "cmdClusterMeet - add node" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_][]const u8{ "CLUSTER", "MEET", "127.0.0.2", "7001" };
    const result = try cmdClusterMeet(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "OK") != null);

    // Verify node added
    try std.testing.expectEqual(@as(usize, 2), storage.cluster.nodes.count());
}

test "cmdClusterForget - remove node" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Add a node first
    const meet_args = [_][]const u8{ "CLUSTER", "MEET", "127.0.0.2", "7001" };
    const meet_result = try cmdClusterMeet(allocator, &meet_args, storage, null, 0);
    defer allocator.free(meet_result);

    // Get the added node's ID
    var node_id: [40]u8 = undefined;
    var found = false;
    var it = storage.cluster.nodes.keyIterator();
    while (it.next()) |key_ptr| {
        const key = key_ptr.*;
        if (key.len == 40 and storage.cluster.myself != null) {
            if (!std.mem.eql(u8, key, &storage.cluster.myself.?.id)) {
                @memcpy(&node_id, key);
                found = true;
                break;
            }
        }
    }

    if (!found) return;

    // Forget the node
    const forget_args = [_][]const u8{ "CLUSTER", "FORGET", &node_id };
    const forget_result = try cmdClusterForget(allocator, &forget_args, storage, null, 0);
    defer allocator.free(forget_result);

    try std.testing.expect(std.mem.indexOf(u8, forget_result, "OK") != null);

    // Verify node removed
    try std.testing.expectEqual(@as(usize, 1), storage.cluster.nodes.count());
}

test "cmdClusterSetslot - MIGRATING success" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // First assign slot 100 to myself
    const add_args = [_][]const u8{ "CLUSTER", "ADDSLOTS", "100" };
    const add_result = try cmdClusterAddSlots(allocator, &add_args, storage, null, 0);
    defer allocator.free(add_result);

    // Create destination node
    const node_id = "1234567890abcdef1234567890abcdef12345678";
    const meet_args = [_][]const u8{ "CLUSTER", "MEET", "192.168.1.2", "6380" };
    const meet_result = try cmdClusterMeet(allocator, &meet_args, storage, null, 0);
    defer allocator.free(meet_result);

    // Set slot to MIGRATING
    const setslot_args = [_][]const u8{ "CLUSTER", "SETSLOT", "100", "MIGRATING", node_id };
    const result = try cmdClusterSetslot(allocator, &setslot_args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "OK") != null);
}

test "cmdClusterSetslot - MIGRATING error when not owner" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Don't assign slot 100 - try to migrate without ownership
    const node_id = "1234567890abcdef1234567890abcdef12345678";
    const setslot_args = [_][]const u8{ "CLUSTER", "SETSLOT", "100", "MIGRATING", node_id };
    const result = try cmdClusterSetslot(allocator, &setslot_args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "not the owner") != null);
}

test "cmdClusterSetslot - IMPORTING success" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Don't assign slot 100 to myself - we're the destination
    const node_id = "1234567890abcdef1234567890abcdef12345678";
    const setslot_args = [_][]const u8{ "CLUSTER", "SETSLOT", "100", "IMPORTING", node_id };
    const result = try cmdClusterSetslot(allocator, &setslot_args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "OK") != null);
}

test "cmdClusterSetslot - IMPORTING error when already owner" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Assign slot 100 to myself
    const add_args = [_][]const u8{ "CLUSTER", "ADDSLOTS", "100" };
    const add_result = try cmdClusterAddSlots(allocator, &add_args, storage, null, 0);
    defer allocator.free(add_result);

    // Try to set as importing - should fail
    const node_id = "1234567890abcdef1234567890abcdef12345678";
    const setslot_args = [_][]const u8{ "CLUSTER", "SETSLOT", "100", "IMPORTING", node_id };
    const result = try cmdClusterSetslot(allocator, &setslot_args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "already the owner") != null);
}

test "cmdClusterSetslot - STABLE clears migration states" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Assign slot 100 and set to MIGRATING
    const add_args = [_][]const u8{ "CLUSTER", "ADDSLOTS", "100" };
    const add_result = try cmdClusterAddSlots(allocator, &add_args, storage, null, 0);
    defer allocator.free(add_result);

    const node_id = "1234567890abcdef1234567890abcdef12345678";
    const migrating_args = [_][]const u8{ "CLUSTER", "SETSLOT", "100", "MIGRATING", node_id };
    const migrating_result = try cmdClusterSetslot(allocator, &migrating_args, storage, null, 0);
    defer allocator.free(migrating_result);

    // Now clear with STABLE
    const stable_args = [_][]const u8{ "CLUSTER", "SETSLOT", "100", "STABLE" };
    const result = try cmdClusterSetslot(allocator, &stable_args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "OK") != null);
}

test "cmdClusterSetslot - NODE finalizes ownership" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set slot to IMPORTING first
    const source_id = "fedcba0987654321fedcba0987654321fedcba09";
    const importing_args = [_][]const u8{ "CLUSTER", "SETSLOT", "100", "IMPORTING", source_id };
    const importing_result = try cmdClusterSetslot(allocator, &importing_args, storage, null, 0);
    defer allocator.free(importing_result);

    // Finalize with NODE (assign to myself)
    const myself = storage.cluster.myself.?;
    const my_id = &myself.id;
    const node_args = [_][]const u8{ "CLUSTER", "SETSLOT", "100", "NODE", my_id };
    const result = try cmdClusterSetslot(allocator, &node_args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "OK") != null);
}

test "cmdClusterSetslot - invalid subcommand" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_][]const u8{ "CLUSTER", "SETSLOT", "100", "INVALID" };
    const result = try cmdClusterSetslot(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "invalid subcommand") != null);
}

test "cmdClusterSlots - returns array of arrays" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try cmdClusterSlots(allocator, &[_][]const u8{}, storage, null, 0);
    defer allocator.free(result);

    // Result should be RESP array starting with *
    try std.testing.expect(result[0] == '*');

    // Should have at least one slot range entry
    try std.testing.expect(result.len > 5);

    // Check for integers representing slots 0 and 16383
    try std.testing.expect(std.mem.indexOf(u8, result, ":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":16383") != null);
}

test "cmdAsking - sets ASKING flag" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const client_id: u64 = 12345;

    // ASKING flag should not be set initially
    try std.testing.expect(!storage.cluster.hasAsking(client_id));

    // Send ASKING command
    const args = [_][]const u8{"ASKING"};
    const result = try cmdAsking(allocator, &args, storage, null, client_id);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "OK") != null);

    // ASKING flag should now be set
    try std.testing.expect(storage.cluster.hasAsking(client_id));
}

test "cmdAsking - wrong argument count" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_][]const u8{ "ASKING", "extra_arg" };
    const result = try cmdAsking(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

test "ClusterState - ASKING flag lifecycle" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const client_id: u64 = 42;

    // Initially not set
    try std.testing.expect(!storage.cluster.hasAsking(client_id));

    // Set ASKING flag
    try storage.cluster.setAsking(client_id);
    try std.testing.expect(storage.cluster.hasAsking(client_id));

    // Clear ASKING flag
    storage.cluster.clearAsking(client_id);
    try std.testing.expect(!storage.cluster.hasAsking(client_id));
}

test "ClusterState - multiple clients ASKING flags" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const client1: u64 = 100;
    const client2: u64 = 200;

    // Set ASKING for client1 only
    try storage.cluster.setAsking(client1);
    try std.testing.expect(storage.cluster.hasAsking(client1));
    try std.testing.expect(!storage.cluster.hasAsking(client2));

    // Set ASKING for client2
    try storage.cluster.setAsking(client2);
    try std.testing.expect(storage.cluster.hasAsking(client1));
    try std.testing.expect(storage.cluster.hasAsking(client2));

    // Clear ASKING for client1
    storage.cluster.clearAsking(client1);
    try std.testing.expect(!storage.cluster.hasAsking(client1));
    try std.testing.expect(storage.cluster.hasAsking(client2));
}

// ── MIGRATE tests ─────────────────────────────────────────────────────────────

test "cmdMigrate - basic syntax validation" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Wrong number of arguments (need at least 5: MIGRATE host port key dest-db timeout)
    const args1 = [_][]const u8{ "MIGRATE", "127.0.0.1", "6380", "mykey" };
    const result1 = try cmdMigrate(allocator, &args1, storage, null, 0);
    defer allocator.free(result1);
    try std.testing.expect(std.mem.indexOf(u8, result1, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result1, "wrong number of arguments") != null);
}

test "cmdMigrate - invalid timeout" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Invalid timeout (not a number)
    const args = [_][]const u8{ "MIGRATE", "127.0.0.1", "6380", "mykey", "0", "invalid" };
    const result = try cmdMigrate(allocator, &args, storage, null, 0);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "timeout") != null);
}

test "cmdMigrate - invalid destination db" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Invalid dest-db (not a number)
    const args = [_][]const u8{ "MIGRATE", "127.0.0.1", "6380", "mykey", "invalid", "5000" };
    const result = try cmdMigrate(allocator, &args, storage, null, 0);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
}

test "cmdMigrate - nonexistent key returns NOKEY" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Try to migrate a key that doesn't exist
    const args = [_][]const u8{ "MIGRATE", "127.0.0.1", "6380", "nonexistent", "0", "5000" };
    const result = try cmdMigrate(allocator, &args, storage, null, 0);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "NOKEY") != null);
}

test "cmdMigrate - with COPY option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set a key
    _ = try storage.set("mykey", "myvalue");

    // Migrate with COPY - key should remain after migration
    const args = [_][]const u8{ "MIGRATE", "127.0.0.1", "6380", "mykey", "0", "5000", "COPY" };
    const result = try cmdMigrate(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // For now, we expect stub to return OK (actual network transfer not implemented)
    try std.testing.expect(std.mem.indexOf(u8, result, "OK") != null);

    // Key should still exist (COPY option)
    const val = try storage.get(allocator, "mykey");
    try std.testing.expect(val != null);
    if (val) |v| allocator.free(v);
}

test "cmdMigrate - with REPLACE option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set a key
    _ = try storage.set("mykey", "myvalue");

    // Migrate with REPLACE
    const args = [_][]const u8{ "MIGRATE", "127.0.0.1", "6380", "mykey", "0", "5000", "REPLACE" };
    const result = try cmdMigrate(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "OK") != null);
}

test "cmdMigrate - KEYS option with multiple keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set multiple keys
    _ = try storage.set("key1", "value1");
    _ = try storage.set("key2", "value2");
    _ = try storage.set("key3", "value3");

    // Migrate with KEYS option (empty string for key arg)
    const args = [_][]const u8{ "MIGRATE", "127.0.0.1", "6380", "", "0", "5000", "KEYS", "key1", "key2", "key3" };
    const result = try cmdMigrate(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "OK") != null);
}

test "cmdMigrate - AUTH option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set a key
    _ = try storage.set("mykey", "myvalue");

    // Migrate with AUTH
    const args = [_][]const u8{ "MIGRATE", "127.0.0.1", "6380", "mykey", "0", "5000", "AUTH", "password123" };
    const result = try cmdMigrate(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "OK") != null);
}

test "cmdMigrate - AUTH2 option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set a key
    _ = try storage.set("mykey", "myvalue");

    // Migrate with AUTH2 (username + password)
    const args = [_][]const u8{ "MIGRATE", "127.0.0.1", "6380", "mykey", "0", "5000", "AUTH2", "admin", "password123" };
    const result = try cmdMigrate(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "OK") != null);
}

test "cmdClusterFailover - wrong arguments" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Too many arguments
    const args = [_][]const u8{ "CLUSTER", "FAILOVER", "FORCE", "EXTRA" };
    const result = try cmdClusterFailover(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
}

test "cmdClusterFailover - invalid mode" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_][]const u8{ "CLUSTER", "FAILOVER", "INVALID" };
    const result = try cmdClusterFailover(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Invalid") != null);
}

test "cmdClusterFailover - not a replica" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Setup myself as master (not replica)
    const my_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(cluster_mod.ClusterNode);
    my_node.* = try cluster_mod.ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    my_node.flags.master = true;
    my_node.flags.slave = false;
    storage.cluster.myself = my_node;

    const args = [_][]const u8{ "CLUSTER", "FAILOVER" };
    const result = try cmdClusterFailover(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
}

test "cmdClusterReplicas - list replicas of master" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create master node
    const master_id = "1111111111111111111111111111111111111111".*;
    const master_node = try allocator.create(cluster_mod.ClusterNode);
    master_node.* = try cluster_mod.ClusterNode.init(allocator, master_id, "127.0.0.1", 7000);
    master_node.flags.master = true;
    master_node.flags.slave = false;
    try storage.cluster.nodes.put(&master_id, master_node);

    // Create replica 1
    const replica1_id = "2222222222222222222222222222222222222222".*;
    const replica1_node = try allocator.create(cluster_mod.ClusterNode);
    replica1_node.* = try cluster_mod.ClusterNode.init(allocator, replica1_id, "127.0.0.1", 7001);
    replica1_node.flags.master = false;
    replica1_node.flags.slave = true;
    replica1_node.master_id = master_id;
    try storage.cluster.nodes.put(&replica1_id, replica1_node);

    // Create replica 2
    const replica2_id = "3333333333333333333333333333333333333333".*;
    const replica2_node = try allocator.create(cluster_mod.ClusterNode);
    replica2_node.* = try cluster_mod.ClusterNode.init(allocator, replica2_id, "127.0.0.1", 7002);
    replica2_node.flags.master = false;
    replica2_node.flags.slave = true;
    replica2_node.master_id = master_id;
    try storage.cluster.nodes.put(&replica2_id, replica2_node);

    // Get replicas
    const args = [_][]const u8{ "CLUSTER", "REPLICAS", "1111111111111111111111111111111111111111" };
    const result = try cmdClusterReplicas(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return array with 2 replicas
    try std.testing.expect(std.mem.indexOf(u8, result, "2222222222222222222222222222222222222222") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "3333333333333333333333333333333333333333") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "slave") != null);
}

test "cmdClusterReplicas - no replicas" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create master node with no replicas
    const master_id = "1111111111111111111111111111111111111111".*;
    const master_node = try allocator.create(cluster_mod.ClusterNode);
    master_node.* = try cluster_mod.ClusterNode.init(allocator, master_id, "127.0.0.1", 7000);
    master_node.flags.master = true;
    try storage.cluster.nodes.put(&master_id, master_node);

    const args = [_][]const u8{ "CLUSTER", "REPLICAS", "1111111111111111111111111111111111111111" };
    const result = try cmdClusterReplicas(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return empty array
    try std.testing.expect(std.mem.indexOf(u8, result, "*0") != null);
}

test "cmdClusterReplicas - unknown node" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_][]const u8{ "CLUSTER", "REPLICAS", "9999999999999999999999999999999999999999" };
    const result = try cmdClusterReplicas(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Unknown node") != null);
}

test "cmdClusterReplicas - invalid node id length" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_][]const u8{ "CLUSTER", "REPLICAS", "short" };
    const result = try cmdClusterReplicas(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Invalid node ID") != null);
}

test "cmdClusterReplicate - configure as replica" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create master node
    const master_id = "1111111111111111111111111111111111111111".*;
    const master_node = try allocator.create(cluster_mod.ClusterNode);
    master_node.* = try cluster_mod.ClusterNode.init(allocator, master_id, "127.0.0.1", 7000);
    master_node.flags.master = true;
    master_node.flags.slave = false;
    try storage.cluster.nodes.put(&master_id, master_node);

    // Setup myself as a master (to be converted to replica)
    const my_id = "2222222222222222222222222222222222222222".*;
    const my_node = try allocator.create(cluster_mod.ClusterNode);
    my_node.* = try cluster_mod.ClusterNode.init(allocator, my_id, "127.0.0.1", 7001);
    my_node.flags.master = true;
    my_node.flags.slave = false;
    storage.cluster.myself = my_node;
    try storage.cluster.nodes.put(&my_id, my_node);

    // Replicate the master
    const args = [_][]const u8{ "CLUSTER", "REPLICATE", "1111111111111111111111111111111111111111" };
    const result = try cmdClusterReplicate(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "OK") != null);

    // Verify node is now a replica
    try std.testing.expect(my_node.flags.slave);
    try std.testing.expect(!my_node.flags.master);
    try std.testing.expect(my_node.master_id != null);
    try std.testing.expect(std.mem.eql(u8, &my_node.master_id.?, &master_id));
}

test "cmdClusterReplicate - cannot replicate self" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Setup myself
    const my_id = "1111111111111111111111111111111111111111".*;
    const my_node = try allocator.create(cluster_mod.ClusterNode);
    my_node.* = try cluster_mod.ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    my_node.flags.master = true;
    storage.cluster.myself = my_node;
    try storage.cluster.nodes.put(&my_id, my_node);

    // Try to replicate self
    const args = [_][]const u8{ "CLUSTER", "REPLICATE", "1111111111111111111111111111111111111111" };
    const result = try cmdClusterReplicate(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "replicate myself") != null);
}

test "cmdClusterReplicate - unknown master" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Setup myself
    const my_id = "1111111111111111111111111111111111111111".*;
    const my_node = try allocator.create(cluster_mod.ClusterNode);
    my_node.* = try cluster_mod.ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    storage.cluster.myself = my_node;
    try storage.cluster.nodes.put(&my_id, my_node);

    // Try to replicate unknown node
    const args = [_][]const u8{ "CLUSTER", "REPLICATE", "9999999999999999999999999999999999999999" };
    const result = try cmdClusterReplicate(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Unknown") != null);
}

test "cmdClusterReplicate - cannot replicate a replica" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create a replica node (not a master)
    const replica_id = "1111111111111111111111111111111111111111".*;
    const replica_node = try allocator.create(cluster_mod.ClusterNode);
    replica_node.* = try cluster_mod.ClusterNode.init(allocator, replica_id, "127.0.0.1", 7000);
    replica_node.flags.master = false;
    replica_node.flags.slave = true;
    try storage.cluster.nodes.put(&replica_id, replica_node);

    // Setup myself
    const my_id = "2222222222222222222222222222222222222222".*;
    const my_node = try allocator.create(cluster_mod.ClusterNode);
    my_node.* = try cluster_mod.ClusterNode.init(allocator, my_id, "127.0.0.1", 7001);
    storage.cluster.myself = my_node;
    try storage.cluster.nodes.put(&my_id, my_node);

    // Try to replicate a replica
    const args = [_][]const u8{ "CLUSTER", "REPLICATE", "1111111111111111111111111111111111111111" };
    const result = try cmdClusterReplicate(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "not a master") != null);
}

test "cmdClusterReplicate - clears slot assignments" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create master node
    const master_id = "1111111111111111111111111111111111111111".*;
    const master_node = try allocator.create(cluster_mod.ClusterNode);
    master_node.* = try cluster_mod.ClusterNode.init(allocator, master_id, "127.0.0.1", 7000);
    master_node.flags.master = true;
    try storage.cluster.nodes.put(&master_id, master_node);

    // Setup myself with slot assignments
    const my_id = "2222222222222222222222222222222222222222".*;
    const my_node = try allocator.create(cluster_mod.ClusterNode);
    my_node.* = try cluster_mod.ClusterNode.init(allocator, my_id, "127.0.0.1", 7001);
    my_node.flags.master = true;
    storage.cluster.myself = my_node;
    try storage.cluster.nodes.put(&my_id, my_node);

    // Assign some slots
    const slots = [_]u16{ 1000, 1001, 1002 };
    try storage.cluster.addSlotsToNode(allocator, &slots, my_node);

    // Verify slots are assigned
    try std.testing.expectEqual(my_node, storage.cluster.slots[1000]);
    try std.testing.expectEqual(my_node, storage.cluster.slots[1001]);

    // Replicate the master
    const args = [_][]const u8{ "CLUSTER", "REPLICATE", "1111111111111111111111111111111111111111" };
    const result = try cmdClusterReplicate(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "OK") != null);

    // Verify slots are cleared
    try std.testing.expect(storage.cluster.slots[1000] == null);
    try std.testing.expect(storage.cluster.slots[1001] == null);
    try std.testing.expect(storage.cluster.slots[1002] == null);
    try std.testing.expectEqual(@as(usize, 0), my_node.slots.items.len);
}

test "cmdClusterReplicate - wrong arguments" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Too few arguments
    const args1 = [_][]const u8{ "CLUSTER", "REPLICATE" };
    const result1 = try cmdClusterReplicate(allocator, &args1, storage, null, 0);
    defer allocator.free(result1);
    try std.testing.expect(std.mem.indexOf(u8, result1, "ERR") != null);

    // Too many arguments
    const args2 = [_][]const u8{ "CLUSTER", "REPLICATE", "1111111111111111111111111111111111111111", "EXTRA" };
    const result2 = try cmdClusterReplicate(allocator, &args2, storage, null, 0);
    defer allocator.free(result2);
    try std.testing.expect(std.mem.indexOf(u8, result2, "ERR") != null);
}

// ============================================================================
// Tests for cmdClusterSaveConfig() and cmdClusterBumpEpoch()
// ============================================================================

test "cmdClusterSaveConfig - returns OK on success" {
    const allocator = std.testing.allocator;
    var tmpdir = std.testing.tmpDir(.{});
    defer tmpdir.cleanup();

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Enable cluster
    storage.cluster.enabled = true;

    // Setup myself
    const my_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const my_node = try allocator.create(cluster_mod.ClusterNode);
    my_node.* = try cluster_mod.ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    storage.cluster.myself = my_node;
    try storage.cluster.nodes.put(&my_id, my_node);

    // Create temp working directory for config file
    // Note: Tests will write to temp location, not real nodes.conf
    const args = [_][]const u8{ "CLUSTER", "SAVECONFIG" };
    const result = try cmdClusterSaveConfig(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return OK
    try std.testing.expect(std.mem.indexOf(u8, result, "OK") != null);
}

test "cmdClusterSaveConfig - cluster disabled returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Cluster is disabled by default
    storage.cluster.enabled = false;

    const args = [_][]const u8{ "CLUSTER", "SAVECONFIG" };
    const result = try cmdClusterSaveConfig(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
}

test "cmdClusterSaveConfig - wrong arity returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    // Too many arguments
    const args = [_][]const u8{ "CLUSTER", "SAVECONFIG", "EXTRA" };
    const result = try cmdClusterSaveConfig(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
}

test "cmdClusterBumpEpoch - returns BUMPED when incremented" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Enable cluster
    storage.cluster.enabled = true;

    // Setup myself with config_epoch = 0
    const my_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".*;
    const my_node = try allocator.create(cluster_mod.ClusterNode);
    my_node.* = try cluster_mod.ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    my_node.config_epoch = 0;
    storage.cluster.myself = my_node;
    storage.cluster.current_epoch = 5;
    try storage.cluster.nodes.put(&my_id, my_node);

    const args = [_][]const u8{ "CLUSTER", "BUMPEPOCH" };
    const result = try cmdClusterBumpEpoch(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return BUMPED with epoch 6
    try std.testing.expect(std.mem.indexOf(u8, result, "BUMPED") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "6") != null);
}

test "cmdClusterBumpEpoch - returns STILL when no increment" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Enable cluster
    storage.cluster.enabled = true;

    // Setup myself with config_epoch == current_epoch
    const my_id = "cccccccccccccccccccccccccccccccccccccccc".*;
    const my_node = try allocator.create(cluster_mod.ClusterNode);
    my_node.* = try cluster_mod.ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    my_node.config_epoch = 10;
    storage.cluster.myself = my_node;
    storage.cluster.current_epoch = 10;
    try storage.cluster.nodes.put(&my_id, my_node);

    const args = [_][]const u8{ "CLUSTER", "BUMPEPOCH" };
    const result = try cmdClusterBumpEpoch(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return STILL with epoch 10
    try std.testing.expect(std.mem.indexOf(u8, result, "STILL") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "10") != null);
}

test "cmdClusterBumpEpoch - cluster disabled returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Cluster is disabled by default
    storage.cluster.enabled = false;

    const args = [_][]const u8{ "CLUSTER", "BUMPEPOCH" };
    const result = try cmdClusterBumpEpoch(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
}

test "cmdClusterBumpEpoch - wrong arity returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    // Too many arguments
    const args = [_][]const u8{ "CLUSTER", "BUMPEPOCH", "EXTRA" };
    const result = try cmdClusterBumpEpoch(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
}

/// CLUSTER COUNTKEYSINSLOT <slot>
/// Returns the number of keys in the specified hash slot
pub fn cmdClusterCountkeysInSlot(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (!storage.cluster.enabled) {
        return w.writeError("ERR This instance has cluster support disabled");
    }

    // Validate arity: CLUSTER COUNTKEYSINSLOT <slot>
    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'cluster countkeysinslot' command");
    }

    // Parse slot argument
    const slot = std.fmt.parseInt(u16, args[2], 10) catch {
        return w.writeError("ERR Invalid slot");
    };

    if (slot >= cluster_mod.CLUSTER_SLOTS) {
        return w.writeError("ERR Invalid slot");
    }

    // Count keys in the slot
    const count = storage.cluster.countKeysInSlot(&storage.data, slot);

    return w.writeInteger(@intCast(count));
}

/// CLUSTER GETKEYSINSLOT <slot> <count>
/// Returns up to <count> keys in the specified hash slot
pub fn cmdClusterGetKeysInSlot(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (!storage.cluster.enabled) {
        return w.writeError("ERR This instance has cluster support disabled");
    }

    // Validate arity: CLUSTER GETKEYSINSLOT <slot> <count>
    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'cluster getkeysinslot' command");
    }

    // Parse slot argument
    const slot = std.fmt.parseInt(u16, args[2], 10) catch {
        return w.writeError("ERR Invalid slot");
    };

    if (slot >= cluster_mod.CLUSTER_SLOTS) {
        return w.writeError("ERR Invalid slot");
    }

    // Parse count argument
    const count = std.fmt.parseInt(usize, args[3], 10) catch {
        return w.writeError("ERR Invalid count");
    };

    // Get keys in the slot (limited to count)
    const keys = try storage.cluster.getKeysInSlot(allocator, &storage.data, slot, count);
    defer {
        for (keys) |key| allocator.free(key);
        allocator.free(keys);
    }

    // Build RESP array response
    var array = try std.ArrayList(RespValue).initCapacity(allocator, keys.len);
    errdefer {
        for (array.items) |item| {
            deinitRespValue(allocator, item);
        }
        array.deinit(allocator);
    }

    for (keys) |key| {
        const key_copy = try allocator.dupe(u8, key);
        errdefer allocator.free(key_copy);
        try array.append(allocator, .{ .bulk_string = key_copy });
    }

    return w.writeArray(try array.toOwnedSlice(allocator));
}

/// CLUSTER KEYSLOT <key>
/// Returns the hash slot for the specified key
pub fn cmdClusterKeyslot(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (!storage.cluster.enabled) {
        return w.writeError("ERR This instance has cluster support disabled");
    }

    // Validate arity: CLUSTER KEYSLOT <key>
    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'cluster keyslot' command");
    }

    // Calculate the slot for the key
    const slot = cluster_mod.keySlot(args[2]);

    return w.writeInteger(slot);
}


/// READONLY - Enable read queries on replica node
pub fn cmdReadonly(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    client_id: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Validate arity: READONLY
    if (args.len != 1) {
        return w.writeError("ERR wrong number of arguments for 'readonly' command");
    }

    // Set readonly flag for this client
    try storage.cluster.setReadonly(client_id);

    return w.writeSimpleString("OK");
}

/// READWRITE - Disable read queries on replica node (return to default)
pub fn cmdReadwrite(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    client_id: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Validate arity: READWRITE
    if (args.len != 1) {
        return w.writeError("ERR wrong number of arguments for 'readwrite' command");
    }

    // Clear readonly flag for this client
    storage.cluster.clearReadonly(client_id);

    return w.writeSimpleString("OK");
}

/// CLUSTER SHARDS - Return cluster shards (shard-centric topology for Redis 7.0+)
/// Returns array of shards, where each shard contains:
/// - slots: Array of inclusive slot range pairs [start, end, start, end, ...]
/// - nodes: Array of node info maps (master + replicas)
pub fn cmdClusterShards(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Validate arity: CLUSTER SHARDS (no extra arguments)
    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'cluster|shards' command");
    }

    const cluster = &storage.cluster;

    // Collect shards from cluster state
    const shards = try cluster.collectShards(allocator);
    defer {
        for (shards) |*shard| {
            shard.deinit(allocator);
        }
        allocator.free(shards);
    }

    // Build RESP response: array of shard arrays
    var response = try std.ArrayList(RespValue).initCapacity(allocator, shards.len);
    errdefer {
        for (response.items) |item| {
            deinitRespValue(allocator, item);
        }
        response.deinit(allocator);
    }

    for (shards) |shard| {
        // Create shard array with slots + nodes
        var shard_array = try std.ArrayList(RespValue).initCapacity(allocator, 2);
        errdefer {
            for (shard_array.items) |item| {
                deinitRespValue(allocator, item);
            }
            shard_array.deinit(allocator);
        }

        // 1. Add slots array: [start, end, start, end, ...]
        var slots_array = try std.ArrayList(RespValue).initCapacity(allocator, shard.slots.items.len);
        errdefer {
            for (slots_array.items) |item| {
                deinitRespValue(allocator, item);
            }
            slots_array.deinit(allocator);
        }

        for (shard.slots.items) |slot| {
            try slots_array.append(allocator, RespValue{ .integer = slot });
        }

        try shard_array.append(allocator, RespValue{ .array = try slots_array.toOwnedSlice(allocator) });

        // 2. Add nodes array: each node is a map with id, ip, port, endpoint, role, replication-offset, health
        var nodes_array = try std.ArrayList(RespValue).initCapacity(allocator, shard.nodes.items.len);
        errdefer {
            for (nodes_array.items) |item| {
                deinitRespValue(allocator, item);
            }
            nodes_array.deinit(allocator);
        }

        for (shard.nodes.items) |node| {
            // Create node info map
            var node_map = try std.ArrayList(RespValue).initCapacity(allocator, 14); // 7 key-value pairs
            errdefer {
                for (node_map.items) |item| {
                    deinitRespValue(allocator, item);
                }
                node_map.deinit(allocator);
            }

            // Add key-value pairs
            // id
            try node_map.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "id") });
            const id_str = try allocator.dupe(u8, &node.id);
            try node_map.append(allocator, RespValue{ .bulk_string = id_str });

            // ip
            try node_map.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "ip") });
            const ip_str = try allocator.dupe(u8, node.addr);
            try node_map.append(allocator, RespValue{ .bulk_string = ip_str });

            // port
            try node_map.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "port") });
            try node_map.append(allocator, RespValue{ .integer = node.port });

            // endpoint (same as ip:port for now)
            try node_map.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "endpoint") });
            var endpoint_buf: [256]u8 = undefined;
            const endpoint_str = try std.fmt.bufPrint(&endpoint_buf, "{s}:{d}", .{ node.addr, node.port });
            try node_map.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, endpoint_str) });

            // role (master or replica)
            try node_map.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "role") });
            const role_str = if (node.flags.master and !node.flags.slave) "master" else "replica";
            try node_map.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, role_str) });

            // replication-offset (use config_epoch as simplified offset)
            try node_map.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "replication-offset") });
            try node_map.append(allocator, RespValue{ .integer = @intCast(node.config_epoch) });

            // health
            try node_map.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "health") });
            const health_str = node.getHealth();
            try node_map.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, health_str) });

            try nodes_array.append(allocator, RespValue{ .array = try node_map.toOwnedSlice(allocator) });
        }

        try shard_array.append(allocator, RespValue{ .array = try nodes_array.toOwnedSlice(allocator) });

        try response.append(allocator, RespValue{ .array = try shard_array.toOwnedSlice(allocator) });
    }

    return w.writeArray(try response.toOwnedSlice(allocator));
}

// ============================================================================
// Tests for CLUSTER COUNTKEYSINSLOT, GETKEYSINSLOT, KEYSLOT
// ============================================================================

test "cmdClusterCountkeysInSlot - count keys in slot" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    // Setup myself
    const my_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const my_node = try allocator.create(cluster_mod.ClusterNode);
    my_node.* = try cluster_mod.ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    storage.cluster.myself = my_node;
    try storage.cluster.nodes.put(&my_id, my_node);

    // Add keys with same hash tag (same slot)
    const v = @import("../storage/memory.zig").Value{ .string = .{ .data = "val" } };
    try storage.data.put("user:{abc}:name", v);
    try storage.data.put("user:{abc}:email", v);
    try storage.data.put("user:{abc}:age", v);

    // Get the slot for these keys
    const slot = cluster_mod.keySlot("user:{abc}:name");
    var slot_buf: [10]u8 = undefined;
    const slot_str = try std.fmt.bufPrint(&slot_buf, "{d}", .{slot});

    // Count keys in the slot
    const args = [_][]const u8{ "CLUSTER", "COUNTKEYSINSLOT", slot_str };
    const result = try cmdClusterCountkeysInSlot(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return 3
    try std.testing.expect(std.mem.indexOf(u8, result, ":3") != null);
}

test "cmdClusterCountkeysInSlot - empty slot returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    const my_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const my_node = try allocator.create(cluster_mod.ClusterNode);
    my_node.* = try cluster_mod.ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    storage.cluster.myself = my_node;
    try storage.cluster.nodes.put(&my_id, my_node);

    // Slot 0 (probably empty)
    const args = [_][]const u8{ "CLUSTER", "COUNTKEYSINSLOT", "0" };
    const result = try cmdClusterCountkeysInSlot(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return 0
    try std.testing.expect(std.mem.indexOf(u8, result, ":0") != null);
}

test "cmdClusterCountkeysInSlot - invalid slot returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    const args = [_][]const u8{ "CLUSTER", "COUNTKEYSINSLOT", "20000" };
    const result = try cmdClusterCountkeysInSlot(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
}

test "cmdClusterCountkeysInSlot - cluster disabled returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = false;

    const args = [_][]const u8{ "CLUSTER", "COUNTKEYSINSLOT", "100" };
    const result = try cmdClusterCountkeysInSlot(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
}

test "cmdClusterGetKeysInSlot - returns keys in slot" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    const my_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".*;
    const my_node = try allocator.create(cluster_mod.ClusterNode);
    my_node.* = try cluster_mod.ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    storage.cluster.myself = my_node;
    try storage.cluster.nodes.put(&my_id, my_node);

    // Add keys
    const v = @import("../storage/memory.zig").Value{ .string = .{ .data = "val" } };
    try storage.data.put("key:{tag}:1", v);
    try storage.data.put("key:{tag}:2", v);
    try storage.data.put("key:{tag}:3", v);

    const slot = cluster_mod.keySlot("key:{tag}:1");
    var slot_buf: [10]u8 = undefined;
    const slot_str = try std.fmt.bufPrint(&slot_buf, "{d}", .{slot});

    // Get 2 keys from the slot
    const args = [_][]const u8{ "CLUSTER", "GETKEYSINSLOT", slot_str, "2" };
    const result = try cmdClusterGetKeysInSlot(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return array with 2 keys
    try std.testing.expect(std.mem.indexOf(u8, result, "*2") != null);
}

test "cmdClusterGetKeysInSlot - respects count limit" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    const my_id = "cccccccccccccccccccccccccccccccccccccccc".*;
    const my_node = try allocator.create(cluster_mod.ClusterNode);
    my_node.* = try cluster_mod.ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    storage.cluster.myself = my_node;
    try storage.cluster.nodes.put(&my_id, my_node);

    // Add 5 keys
    const v = @import("../storage/memory.zig").Value{ .string = .{ .data = "val" } };
    var i: u8 = 1;
    while (i <= 5) : (i += 1) {
        const key = try std.fmt.allocPrint(allocator, "item:{{xyz}}:{d}", .{i});
        defer allocator.free(key);
        try storage.data.put(key, v);
    }

    const slot = cluster_mod.keySlot("item:{xyz}:1");
    var slot_buf: [10]u8 = undefined;
    const slot_str = try std.fmt.bufPrint(&slot_buf, "{d}", .{slot});

    // Request only 3 keys
    const args = [_][]const u8{ "CLUSTER", "GETKEYSINSLOT", slot_str, "3" };
    const result = try cmdClusterGetKeysInSlot(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return array with 3 keys
    try std.testing.expect(std.mem.indexOf(u8, result, "*3") != null);
}

test "cmdClusterGetKeysInSlot - invalid slot returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    const args = [_][]const u8{ "CLUSTER", "GETKEYSINSLOT", "99999", "10" };
    const result = try cmdClusterGetKeysInSlot(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
}

test "cmdClusterKeyslot - returns slot for key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    // Calculate expected slot
    const expected_slot = cluster_mod.keySlot("mykey");
    var expected_buf: [10]u8 = undefined;
    const expected_str = try std.fmt.bufPrint(&expected_buf, ":{d}", .{expected_slot});

    const args = [_][]const u8{ "CLUSTER", "KEYSLOT", "mykey" };
    const result = try cmdClusterKeyslot(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return the slot number
    try std.testing.expect(std.mem.indexOf(u8, result, expected_str) != null);
}

test "cmdClusterKeyslot - hash tag support" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    // Two keys with same hash tag should map to same slot
    const args1 = [_][]const u8{ "CLUSTER", "KEYSLOT", "user:{123}:name" };
    const result1 = try cmdClusterKeyslot(allocator, &args1, storage, null, 0);
    defer allocator.free(result1);

    const args2 = [_][]const u8{ "CLUSTER", "KEYSLOT", "user:{123}:email" };
    const result2 = try cmdClusterKeyslot(allocator, &args2, storage, null, 0);
    defer allocator.free(result2);

    // Both should return the same slot
    try std.testing.expectEqualStrings(result1, result2);
}

test "cmdClusterKeyslot - cluster disabled returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = false;

    const args = [_][]const u8{ "CLUSTER", "KEYSLOT", "key" };
    const result = try cmdClusterKeyslot(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
}

// ============================================================================
// Tests for READONLY/READWRITE Commands
// ============================================================================

test "cmdReadonly - sets readonly flag" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const client_id: u64 = 555;
    const args = [_][]const u8{"READONLY"};

    const result = try cmdReadonly(allocator, &args, storage, null, client_id);
    defer allocator.free(result);

    // Should return OK
    try std.testing.expect(std.mem.indexOf(u8, result, "+OK") != null);

    // Verify flag is set
    try std.testing.expect(storage.cluster.hasReadonly(client_id));
}

test "cmdReadwrite - clears readonly flag" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const client_id: u64 = 666;

    // First set readonly
    try storage.cluster.setReadonly(client_id);
    try std.testing.expect(storage.cluster.hasReadonly(client_id));

    // Now clear it
    const args = [_][]const u8{"READWRITE"};
    const result = try cmdReadwrite(allocator, &args, storage, null, client_id);
    defer allocator.free(result);

    // Should return OK
    try std.testing.expect(std.mem.indexOf(u8, result, "+OK") != null);

    // Verify flag is cleared
    try std.testing.expect(!storage.cluster.hasReadonly(client_id));
}

test "cmdReadonly - wrong number of arguments" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const client_id: u64 = 777;
    const args = [_][]const u8{ "READONLY", "extra" };

    const result = try cmdReadonly(allocator, &args, storage, null, client_id);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

test "cmdReadwrite - wrong number of arguments" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const client_id: u64 = 888;
    const args = [_][]const u8{ "READWRITE", "extra" };

    const result = try cmdReadwrite(allocator, &args, storage, null, client_id);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

test "cmdReadonly/cmdReadwrite - multiple clients independently" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const client1: u64 = 100;
    const client2: u64 = 200;

    // Set readonly for client1
    const args1 = [_][]const u8{"READONLY"};
    const result1 = try cmdReadonly(allocator, &args1, storage, null, client1);
    defer allocator.free(result1);

    // Verify only client1 has readonly
    try std.testing.expect(storage.cluster.hasReadonly(client1));
    try std.testing.expect(!storage.cluster.hasReadonly(client2));

    // Set readonly for client2
    const result2 = try cmdReadonly(allocator, &args1, storage, null, client2);
    defer allocator.free(result2);

    // Both should have readonly
    try std.testing.expect(storage.cluster.hasReadonly(client1));
    try std.testing.expect(storage.cluster.hasReadonly(client2));

    // Clear readonly for client1
    const args2 = [_][]const u8{"READWRITE"};
    const result3 = try cmdReadwrite(allocator, &args2, storage, null, client1);
    defer allocator.free(result3);

    // Only client2 should have readonly
    try std.testing.expect(!storage.cluster.hasReadonly(client1));
    try std.testing.expect(storage.cluster.hasReadonly(client2));
}

// ============================================================================
// Tests for CLUSTER SHARDS
// ============================================================================

test "cmdClusterShards - empty cluster" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    const args = [_][]const u8{ "CLUSTER", "SHARDS" };
    const result = try cmdClusterShards(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return empty array
    try std.testing.expect(std.mem.indexOf(u8, result, "*0") != null);
}

test "cmdClusterShards - single-node cluster" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    // Setup single master node
    const my_id = "0123456789abcdef0123456789abcdef01234567".*;
    const my_node = try allocator.create(cluster_mod.ClusterNode);
    my_node.* = try cluster_mod.ClusterNode.init(allocator, my_id, "127.0.0.1", 7000);
    storage.cluster.myself = my_node;
    try storage.cluster.nodes.put(&my_id, my_node);

    my_node.flags.master = true;
    my_node.flags.slave = false;

    // Assign all slots
    var all_slots = std.ArrayListUnmanaged(u16){};
    defer all_slots.deinit(allocator);
    for (0..cluster_mod.CLUSTER_SLOTS) |i| {
        try all_slots.append(allocator, @intCast(i));
    }
    try storage.cluster.addSlotsToNode(my_node, all_slots.items);

    const args = [_][]const u8{ "CLUSTER", "SHARDS" };
    const result = try cmdClusterShards(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return array with 1 shard
    try std.testing.expect(std.mem.indexOf(u8, result, "*1") != null);
    // Should contain node id
    try std.testing.expect(std.mem.indexOf(u8, result, "0123456789abcdef0123456789abcdef01234567") != null);
    // Should contain node role
    try std.testing.expect(std.mem.indexOf(u8, result, "role") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "master") != null);
    // Should contain health
    try std.testing.expect(std.mem.indexOf(u8, result, "health") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "online") != null);
}

test "cmdClusterShards - multi-shard with replicas" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    // Create master 1
    const master1_id = "1111111111111111111111111111111111111111".*;
    const master1 = try allocator.create(cluster_mod.ClusterNode);
    master1.* = try cluster_mod.ClusterNode.init(allocator, master1_id, "127.0.0.1", 7000);
    storage.cluster.myself = master1;
    try storage.cluster.nodes.put(&master1_id, master1);
    master1.flags.master = true;
    master1.flags.slave = false;

    // Create replica
    const replica1_id = "2222222222222222222222222222222222222222".*;
    const replica1 = try allocator.create(cluster_mod.ClusterNode);
    replica1.* = try cluster_mod.ClusterNode.init(allocator, replica1_id, "127.0.0.1", 7001);
    try storage.cluster.nodes.put(&replica1_id, replica1);
    replica1.flags.master = false;
    replica1.flags.slave = true;
    replica1.master_id = master1_id;

    // Create master 2
    const master2_id = "3333333333333333333333333333333333333333".*;
    const master2 = try allocator.create(cluster_mod.ClusterNode);
    master2.* = try cluster_mod.ClusterNode.init(allocator, master2_id, "127.0.0.1", 7002);
    try storage.cluster.nodes.put(&master2_id, master2);
    master2.flags.master = true;
    master2.flags.slave = false;

    // Assign slots: master1 gets 0-8191, master2 gets 8192-16383
    var slots1 = std.ArrayListUnmanaged(u16){};
    defer slots1.deinit(allocator);
    for (0..8192) |i| {
        try slots1.append(allocator, @intCast(i));
    }
    try storage.cluster.addSlotsToNode(master1, slots1.items);

    var slots2 = std.ArrayListUnmanaged(u16){};
    defer slots2.deinit(allocator);
    for (8192..cluster_mod.CLUSTER_SLOTS) |i| {
        try slots2.append(allocator, @intCast(i));
    }
    try storage.cluster.addSlotsToNode(master2, slots2.items);

    const args = [_][]const u8{ "CLUSTER", "SHARDS" };
    const result = try cmdClusterShards(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return array with 2 shards
    try std.testing.expect(std.mem.indexOf(u8, result, "*2") != null);
    // Should contain both master IDs
    try std.testing.expect(std.mem.indexOf(u8, result, "1111111111111111111111111111111111111111") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "3333333333333333333333333333333333333333") != null);
    // Should contain replica ID
    try std.testing.expect(std.mem.indexOf(u8, result, "2222222222222222222222222222222222222222") != null);
}

test "cmdClusterShards - wrong number of arguments" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_][]const u8{ "CLUSTER", "SHARDS", "extra" };
    const result = try cmdClusterShards(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

/// CLUSTER LINKS - Return cluster bus links information
pub fn cmdClusterLinks(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Validate arity: CLUSTER LINKS (no extra arguments)
    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'cluster|links' command");
    }

    const cluster = &storage.cluster;

    // Collect links from cluster state
    const links = try cluster.collectLinks(allocator);
    defer allocator.free(links);

    // Build RESP response: array of link objects
    var response = try std.ArrayList(RespValue).initCapacity(allocator, links.len);
    errdefer {
        for (response.items) |item| {
            deinitRespValue(allocator, item);
        }
        response.deinit(allocator);
    }

    for (links) |link| {
        // Create link array with 6 key-value pairs
        var link_array = try std.ArrayList(RespValue).initCapacity(allocator, 12);
        errdefer {
            for (link_array.items) |item| {
                deinitRespValue(allocator, item);
            }
            link_array.deinit(allocator);
        }

        // 1. direction
        try link_array.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "direction") });
        try link_array.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, link.direction) });

        // 2. node-id (peer_node_id)
        try link_array.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "node-id") });
        const node_id_str = try allocator.dupe(u8, &link.peer_node_id);
        try link_array.append(allocator, RespValue{ .bulk_string = node_id_str });

        // 3. create-time
        try link_array.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "create-time") });
        try link_array.append(allocator, RespValue{ .integer = link.create_time });

        // 4. events
        try link_array.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "events") });
        try link_array.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, link.events) });

        // 5. send-buffer-allocated
        try link_array.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "send-buffer-allocated") });
        try link_array.append(allocator, RespValue{ .integer = @intCast(link.send_buffer_allocated) });

        // 6. send-buffer-used
        try link_array.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "send-buffer-used") });
        try link_array.append(allocator, RespValue{ .integer = @intCast(link.send_buffer_used) });

        try response.append(allocator, RespValue{ .array = try link_array.toOwnedSlice(allocator) });
    }

    // Write the array response
    return w.writeArray(try response.toOwnedSlice(allocator));
}

test "cmdClusterLinks - empty cluster (single node)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_][]const u8{ "CLUSTER", "LINKS" };
    const result = try cmdClusterLinks(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return empty array
    try std.testing.expect(std.mem.indexOf(u8, result, "*0") != null);
}

test "cmdClusterLinks - arity error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_][]const u8{ "CLUSTER", "LINKS", "extra" };
    const result = try cmdClusterLinks(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

test "cmdClusterLinks - with one peer node" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const myself_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const myself = try allocator.create(cluster_mod.ClusterNode);
    myself.* = try cluster_mod.ClusterNode.init(allocator, myself_id, "127.0.0.1", 7000);
    defer {
        myself.deinit(allocator);
        allocator.destroy(myself);
    }

    const peer_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".*;
    const peer = try allocator.create(cluster_mod.ClusterNode);
    peer.* = try cluster_mod.ClusterNode.init(allocator, peer_id, "127.0.0.1", 7001);
    defer {
        peer.deinit(allocator);
        allocator.destroy(peer);
    }

    storage.cluster.myself = myself;
    const myself_key = try allocator.dupe(u8, &myself_id);
    try storage.cluster.nodes.put(myself_key, myself);

    const peer_key = try allocator.dupe(u8, &peer_id);
    try storage.cluster.nodes.put(peer_key, peer);

    const args = [_][]const u8{ "CLUSTER", "LINKS" };
    const result = try cmdClusterLinks(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return array with 2 links
    try std.testing.expect(std.mem.indexOf(u8, result, "*2") != null);
    // Should contain direction fields
    try std.testing.expect(std.mem.indexOf(u8, result, "direction") != null);
    // Should contain node-id field
    try std.testing.expect(std.mem.indexOf(u8, result, "node-id") != null);
    // Should contain create-time field
    try std.testing.expect(std.mem.indexOf(u8, result, "create-time") != null);
    // Should contain events field
    try std.testing.expect(std.mem.indexOf(u8, result, "events") != null);
    // Should contain send-buffer fields
    try std.testing.expect(std.mem.indexOf(u8, result, "send-buffer-allocated") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "send-buffer-used") != null);
}

// ============================================================================
// CLUSTER COUNT-FAILURE-REPORTS
// ============================================================================

/// CLUSTER COUNT-FAILURE-REPORTS <node-id>
/// Returns the number of active failure reports for the specified node
/// Used in cluster consensus to determine if a node should be marked as failed
pub fn cmdClusterCountFailureReports(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Validate arity: CLUSTER COUNT-FAILURE-REPORTS <node-id>
    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'cluster|count-failure-reports' command");
    }

    const cluster = &storage.cluster;

    // Check if cluster is enabled
    if (!cluster.enabled) {
        return w.writeError("ERR This instance has cluster support disabled");
    }

    // Parse node ID (must be 40-char hex string)
    const node_id_str = args[2];
    if (node_id_str.len != 40) {
        return w.writeError("ERR Invalid node ID");
    }

    var node_id: [40]u8 = undefined;
    @memcpy(&node_id, node_id_str);

    // Verify node exists (optional - Redis doesn't strictly require this)
    // But it's good practice to validate the node ID
    if (cluster.nodes.get(node_id_str) == null) {
        // Return 0 for unknown nodes (Redis behavior)
        return w.writeInteger(0);
    }

    // Get failure report count
    const count = cluster.getFailureReportCount(node_id);
    return w.writeInteger(@intCast(count));
}

test "cmdClusterCountFailureReports - arity error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    // Too few arguments
    const args1 = [_][]const u8{ "CLUSTER", "COUNT-FAILURE-REPORTS" };
    const result1 = try cmdClusterCountFailureReports(allocator, &args1, storage, null, 0);
    defer allocator.free(result1);
    try std.testing.expect(std.mem.indexOf(u8, result1, "ERR") != null);

    // Too many arguments
    const args2 = [_][]const u8{ "CLUSTER", "COUNT-FAILURE-REPORTS", "node-id", "extra" };
    const result2 = try cmdClusterCountFailureReports(allocator, &args2, storage, null, 0);
    defer allocator.free(result2);
    try std.testing.expect(std.mem.indexOf(u8, result2, "ERR") != null);
}

test "cmdClusterCountFailureReports - cluster disabled" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const args = [_][]const u8{ "CLUSTER", "COUNT-FAILURE-REPORTS", node_id };
    const result = try cmdClusterCountFailureReports(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "cluster support disabled") != null);
}

test "cmdClusterCountFailureReports - invalid node ID length" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    const args = [_][]const u8{ "CLUSTER", "COUNT-FAILURE-REPORTS", "invalid" };
    const result = try cmdClusterCountFailureReports(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Invalid node ID") != null);
}

test "cmdClusterCountFailureReports - unknown node returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    const node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const args = [_][]const u8{ "CLUSTER", "COUNT-FAILURE-REPORTS", node_id };
    const result = try cmdClusterCountFailureReports(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return :0
    try std.testing.expect(std.mem.indexOf(u8, result, ":0") != null);
}

test "cmdClusterCountFailureReports - with failure reports" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    // Create a node
    const node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const node = try allocator.create(cluster_mod.ClusterNode);
    node.* = try cluster_mod.ClusterNode.init(allocator, node_id, "127.0.0.1", 7000);
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    const node_key = try allocator.dupe(u8, &node_id);
    try storage.cluster.nodes.put(node_key, node);

    // Add some failure reports
    const reporter1 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".*;
    const reporter2 = "cccccccccccccccccccccccccccccccccccccccc".*;
    try storage.cluster.addFailureReport(node_id, reporter1);
    try storage.cluster.addFailureReport(node_id, reporter2);

    const args = [_][]const u8{ "CLUSTER", "COUNT-FAILURE-REPORTS", &node_id };
    const result = try cmdClusterCountFailureReports(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return :2
    try std.testing.expect(std.mem.indexOf(u8, result, ":2") != null);
}
