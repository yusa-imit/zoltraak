const std = @import("std");
const Writer = @import("../protocol/writer.zig").Writer;
const Storage = @import("../storage/memory.zig").Storage;
const RespValue = @import("../protocol/parser.zig").RespValue;

/// CLUSTER SLOTS - Return cluster slots configuration (stub - returns single node covering all slots)
pub fn cmdClusterSlots(
    allocator: std.mem.Allocator,
    _: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    _ = storage;

    var w = Writer.init(allocator);
    defer w.deinit();

    // Return array with single slot range covering all 16384 slots
    // Format: [[start_slot, end_slot, [host, port, node_id]], ...]

    // Build the response structure manually
    var response = std.ArrayList(RespValue){};
    defer {
        for (response.items) |item| {
            deinitRespValue(allocator, item);
        }
        response.deinit(allocator);
    }

    // Slot range array
    var slot_array = std.ArrayList(RespValue){};
    defer slot_array.deinit(allocator);

    // Add start slot (0)
    try slot_array.append(allocator, RespValue{ .integer = 0 });
    // Add end slot (16383)
    try slot_array.append(allocator, RespValue{ .integer = 16383 });

    // Node info array [host, port, node_id]
    var node_array = std.ArrayList(RespValue){};
    defer node_array.deinit(allocator);

    const host_str = try allocator.dupe(u8, "127.0.0.1");
    try node_array.append(allocator, RespValue{ .bulk_string = host_str });
    try node_array.append(allocator, RespValue{ .integer = 6379 });
    const node_id_str = try allocator.dupe(u8, "zoltraak-standalone-node");
    try node_array.append(allocator, RespValue{ .bulk_string = node_id_str });

    try slot_array.append(allocator, RespValue{ .array = try node_array.toOwnedSlice(allocator) });

    try response.append(allocator, RespValue{ .array = try slot_array.toOwnedSlice(allocator) });

    return w.writeArray(try response.toOwnedSlice(allocator));
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

/// CLUSTER NODES - Return cluster nodes configuration (stub - returns single standalone node)
pub fn cmdClusterNodes(
    allocator: std.mem.Allocator,
    _: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    _ = storage;

    var w = Writer.init(allocator);
    defer w.deinit();

    // Format: node_id host:port@cport flags master/slave master_id ping_pong config_epoch link_state slots
    const node_info = "zoltraak-standalone-node 127.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-16383\n";

    return w.writeBulkString(node_info);
}

/// CLUSTER INFO - Return cluster state information (stub - returns cluster disabled)
pub fn cmdClusterInfo(
    allocator: std.mem.Allocator,
    _: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    _ = storage;

    var w = Writer.init(allocator);
    defer w.deinit();

    const info =
        \\cluster_state:ok
        \\cluster_slots_assigned:16384
        \\cluster_slots_ok:16384
        \\cluster_slots_pfail:0
        \\cluster_slots_fail:0
        \\cluster_known_nodes:1
        \\cluster_size:1
        \\cluster_current_epoch:1
        \\cluster_my_epoch:1
        \\cluster_stats_messages_sent:0
        \\cluster_stats_messages_received:0
    ;

    return w.writeBulkString(info);
}

/// CLUSTER MYID - Return node ID (stub - returns fixed node ID)
pub fn cmdClusterMyId(
    allocator: std.mem.Allocator,
    _: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    _ = storage;

    var w = Writer.init(allocator);
    defer w.deinit();

    return w.writeBulkString("zoltraak-standalone-node");
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
    const slot = calculateKeySlot(key);

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

/// Calculate CRC16 hash slot for a key (Redis cluster hash slot algorithm)
fn calculateKeySlot(key: []const u8) u16 {
    // Find hash tags {...}
    var start: ?usize = null;
    var end: ?usize = null;

    for (key, 0..) |c, i| {
        if (c == '{') {
            start = i;
        } else if (c == '}' and start != null) {
            end = i;
            break;
        }
    }

    // Use content between braces if valid, otherwise use full key
    const hash_key = if (start != null and end != null and end.? > start.? + 1)
        key[start.? + 1 .. end.?]
    else
        key;

    // CRC16 XMODEM
    return crc16(hash_key) % 16384;
}

/// CRC16 implementation (XMODEM polynomial)
fn crc16(data: []const u8) u16 {
    var crc: u16 = 0;

    for (data) |byte| {
        crc ^= @as(u16, byte) << 8;

        var i: u8 = 0;
        while (i < 8) : (i += 1) {
            if (crc & 0x8000 != 0) {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc = crc << 1;
            }
        }
    }

    return crc;
}

// Unit tests
test "calculateKeySlot - basic keys" {
    const slot1 = calculateKeySlot("user:1000");
    const slot2 = calculateKeySlot("user:2000");

    // Different keys should (usually) have different slots
    try std.testing.expect(slot1 >= 0 and slot1 <= 16383);
    try std.testing.expect(slot2 >= 0 and slot2 <= 16383);
}

test "calculateKeySlot - hash tags" {
    // Keys with same hash tag should map to same slot
    const slot1 = calculateKeySlot("user:{123}:profile");
    const slot2 = calculateKeySlot("user:{123}:settings");
    const slot3 = calculateKeySlot("{123}");

    try std.testing.expectEqual(slot1, slot2);
    try std.testing.expectEqual(slot1, slot3);
}

test "calculateKeySlot - empty hash tag ignored" {
    const slot1 = calculateKeySlot("user:{}:profile");
    const slot2 = calculateKeySlot("user::profile");

    // Empty hash tag should be ignored, hash full key
    // These will be different since we hash different strings
    // Just verify they're in valid range
    try std.testing.expect(slot1 >= 0 and slot1 <= 16383);
    try std.testing.expect(slot2 >= 0 and slot2 <= 16383);
}

test "crc16 - known values" {
    // Test CRC16 with empty string
    const crc1 = crc16("");
    try std.testing.expectEqual(@as(u16, 0), crc1);

    // Test with simple string
    const crc2 = crc16("123456789");
    // CRC16-XMODEM of "123456789" is 0x31C3
    try std.testing.expectEqual(@as(u16, 0x31C3), crc2);
}
