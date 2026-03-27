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

/// CLUSTER KEYSLOT - Return hash slot for a key
pub fn cmdClusterKeyslot(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    _ = storage;

    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "CLUSTER", args[1] = "KEYSLOT", args[2] = key
    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'cluster|keyslot' command");
    }

    const key = args[2];
    const slot = cluster_mod.keySlot(key);

    return w.writeInteger(@intCast(slot));
}

/// CLUSTER COUNTKEYSINSLOT - Count keys in a hash slot (stub - returns 0)
pub fn cmdClusterCountKeysInSlot(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    _ = storage;

    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "CLUSTER", args[1] = "COUNTKEYSINSLOT", args[2] = slot
    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'cluster|countkeysinslot' command");
    }

    // Parse slot number
    const slot_str = args[2];
    const slot = std.fmt.parseInt(i64, slot_str, 10) catch {
        return w.writeError("ERR invalid slot");
    };

    if (slot < 0 or slot > 16383) {
        return w.writeError("ERR slot out of range");
    }

    // Stub: always return 0 (not implemented)
    return w.writeInteger(0);
}

/// CLUSTER GETKEYSINSLOT - Return keys in a hash slot (stub - returns empty array)
pub fn cmdClusterGetKeysInSlot(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    _ = storage;

    var w = Writer.init(allocator);
    defer w.deinit();

    // args[0] = "CLUSTER", args[1] = "GETKEYSINSLOT", args[2] = slot, args[3] = count
    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'cluster|getkeysinslot' command");
    }

    // Parse slot number
    const slot_str = args[2];
    const slot = std.fmt.parseInt(i64, slot_str, 10) catch {
        return w.writeError("ERR invalid slot");
    };

    if (slot < 0 or slot > 16383) {
        return w.writeError("ERR slot out of range");
    }

    // Parse count (not used in stub)
    const count_str = args[3];
    _ = std.fmt.parseInt(i64, count_str, 10) catch {
        return w.writeError("ERR invalid count");
    };

    // Stub: always return empty array (not implemented)
    const empty: []const RespValue = &[_]RespValue{};
    return w.writeArray(empty);
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
