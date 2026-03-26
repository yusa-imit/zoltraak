const std = @import("std");
const Writer = @import("../protocol/writer.zig").Writer;
const Storage = @import("../storage/memory.zig").Storage;
const RespValue = @import("../protocol/parser.zig").RespValue;
const cluster_mod = @import("../storage/cluster.zig");

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
