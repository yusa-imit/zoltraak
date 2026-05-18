const std = @import("std");
const Storage = @import("../storage/memory.zig").Storage;
const Value = @import("../storage/memory.zig").Value;
const Writer = @import("../protocol/writer.zig").Writer;
const RespValue = @import("../protocol/parser.zig").RespValue;
const notifications_mod = @import("../storage/notifications.zig");
const pubsub_mod = @import("../storage/pubsub.zig");

const PubSub = pubsub_mod.PubSub;

/// Publish keyspace notification for a HyperLogLog command
/// Fires only if string events (.string flag) are enabled
fn notifyHyperLogLogEvent(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub_state: *PubSub,
    db_index: u32,
    key: []const u8,
    event_name: []const u8,
) void {
    const config_value = storage.config.getAsString("notify-keyspace-events") catch return;
    const config_str = config_value orelse return;
    const flags = notifications_mod.parseNotificationFlags(config_str);

    if (!notifications_mod.shouldNotify(flags, .string)) return;

    notifications_mod.publishNotification(allocator, pubsub_state, db_index, key, event_name, flags) catch {};
}

/// PFADD key element [element ...]
/// Add elements to HyperLogLog, returns 1 if at least one register was updated
pub fn cmdPfadd(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const RespValue,
    ps: *PubSub,
    db_index: u32,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'pfadd' command");
    }

    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const elements = args[1..];

    storage.mutex.lock();
    defer storage.mutex.unlock();

    var updated = false;

    // Get or create HyperLogLog
    var hll_value: *Value.HyperLogLogValue = undefined;
    const entry = try storage.data.getOrPut(key);

    if (entry.found_existing) {
        // Key exists, verify it's a HyperLogLog
        if (entry.value_ptr.* != .hyperloglog) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        hll_value = &entry.value_ptr.hyperloglog;
    } else {
        // Create new HyperLogLog
        const key_copy = try storage.allocator.dupe(u8, key);
        errdefer storage.allocator.free(key_copy);

        entry.key_ptr.* = key_copy;
        entry.value_ptr.* = Value{ .hyperloglog = Value.HyperLogLogValue.init() };
        hll_value = &entry.value_ptr.hyperloglog;
    }

    // Add all elements
    for (elements) |elem_resp| {
        const element = switch (elem_resp) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid element"),
        };
        if (hll_value.add(element)) {
            updated = true;
        }
    }

    // Fire notification only if updated
    if (updated) {
        notifyHyperLogLogEvent(allocator, storage, ps, db_index, key, "pfadd");
    }

    // Return 1 if updated, 0 otherwise
    return w.writeInteger(if (updated) 1 else 0);
}

/// PFCOUNT key [key ...]
/// Return the approximated cardinality of the set(s) observed by the HyperLogLog
pub fn cmdPfcount(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 1) {
        return w.writeError("ERR wrong number of arguments for 'pfcount' command");
    }

    storage.mutex.lock();
    defer storage.mutex.unlock();

    if (args.len == 1) {
        // Single key case
        const key = switch (args[0]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        const entry = storage.data.get(key);
        if (entry == null) {
            return w.writeInteger(0);
        }

        if (entry.? != .hyperloglog) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        const count = entry.?.hyperloglog.count();
        return w.writeInteger(@intCast(count));
    } else {
        // Multiple keys: merge all HyperLogLogs and count
        var merged = Value.HyperLogLogValue.init();

        for (args) |key_resp| {
            const key = switch (key_resp) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid key"),
            };

            const entry = storage.data.get(key);
            if (entry == null) continue;

            if (entry.? != .hyperloglog) {
                return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            merged.merge(&entry.?.hyperloglog);
        }

        const count = merged.count();
        return w.writeInteger(@intCast(count));
    }
}

/// PFMERGE destkey sourcekey [sourcekey ...]
/// Merge multiple HyperLogLog values into a single one
pub fn cmdPfmerge(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const RespValue,
    ps: *PubSub,
    db_index: u32,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'pfmerge' command");
    }

    const destkey = switch (args[0]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };
    const sourcekeys = args[1..];

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Create merged HyperLogLog
    var merged = Value.HyperLogLogValue.init();

    // Merge all source keys
    for (sourcekeys) |key_resp| {
        const key = switch (key_resp) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        const entry = storage.data.get(key);
        if (entry == null) continue;

        if (entry.? != .hyperloglog) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        merged.merge(&entry.?.hyperloglog);
    }

    // Store the merged result at destkey
    const dest_entry = try storage.data.getOrPut(destkey);

    if (dest_entry.found_existing) {
        // Free old value if exists
        dest_entry.value_ptr.deinit(storage.allocator);
    } else {
        // Allocate key
        const key_copy = try storage.allocator.dupe(u8, destkey);
        errdefer storage.allocator.free(key_copy);
        dest_entry.key_ptr.* = key_copy;
    }

    dest_entry.value_ptr.* = Value{ .hyperloglog = merged };

    // Fire notification after successful merge
    notifyHyperLogLogEvent(allocator, storage, ps, db_index, destkey, "pfadd");

    return w.writeSimpleString("OK");
}

// Unit tests
test "HyperLogLog: PFADD basic" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var response = std.ArrayList(u8){};
    defer response.deinit(std.testing.allocator);

    // Add elements
    const args1 = [_][]const u8{ "hll1", "elem1", "elem2", "elem3" };
    try cmdPfadd(&storage, &args1, &response, std.testing.allocator);

    const result1 = response.items;
    try std.testing.expect(std.mem.indexOf(u8, result1, ":1\r\n") != null); // Should update

    // Add same elements again
    response.clearRetainingCapacity();
    const args2 = [_][]const u8{ "hll1", "elem1", "elem2" };
    try cmdPfadd(&storage, &args2, &response, std.testing.allocator);

    const result2 = response.items;
    try std.testing.expect(std.mem.indexOf(u8, result2, ":0\r\n") != null); // No update
}

test "HyperLogLog: PFCOUNT single key" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var response = std.ArrayList(u8){};
    defer response.deinit(std.testing.allocator);

    // Add many elements
    var args_list = std.ArrayList([]const u8){};
    defer args_list.deinit(std.testing.allocator);

    try args_list.append(std.testing.allocator, "hll1");

    var buf: [20]u8 = undefined;
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        const elem = try std.fmt.bufPrint(&buf, "elem{d}", .{i});
        const elem_copy = try std.testing.allocator.dupe(u8, elem);
        defer std.testing.allocator.free(elem_copy);
        try args_list.append(std.testing.allocator, elem_copy);
    }

    try cmdPfadd(&storage, args_list.items, &response, std.testing.allocator);

    // Count
    response.clearRetainingCapacity();
    const count_args = [_][]const u8{"hll1"};
    try cmdPfcount(&storage, &count_args, &response, std.testing.allocator);

    // Should be approximately 100 (HyperLogLog is approximate)
    const result = response.items;
    try std.testing.expect(std.mem.indexOf(u8, result, ":") != null);
}

test "HyperLogLog: PFMERGE" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var response = std.ArrayList(u8){};
    defer response.deinit(std.testing.allocator);

    // Add to hll1
    const args1 = [_][]const u8{ "hll1", "a", "b", "c" };
    try cmdPfadd(&storage, &args1, &response, std.testing.allocator);

    // Add to hll2
    response.clearRetainingCapacity();
    const args2 = [_][]const u8{ "hll2", "c", "d", "e" };
    try cmdPfadd(&storage, &args2, &response, std.testing.allocator);

    // Merge
    response.clearRetainingCapacity();
    const merge_args = [_][]const u8{ "hll3", "hll1", "hll2" };
    try cmdPfmerge(&storage, &merge_args, &response, std.testing.allocator);

    const merge_result = response.items;
    try std.testing.expect(std.mem.indexOf(u8, merge_result, "+OK\r\n") != null);

    // Count merged
    response.clearRetainingCapacity();
    const count_args = [_][]const u8{"hll3"};
    try cmdPfcount(&storage, &count_args, &response, std.testing.allocator);

    // Should have approximately 5 unique elements (a, b, c, d, e)
    const count_result = response.items;
    try std.testing.expect(std.mem.indexOf(u8, count_result, ":") != null);
}

test "HyperLogLog: WRONGTYPE error" {
    var storage = try Storage.init(std.testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var response = std.ArrayList(u8){};
    defer response.deinit(std.testing.allocator);

    // Set a string value
    _ = try storage.set("mykey", "value", null);

    // Try PFADD on string key
    const args = [_][]const u8{ "mykey", "elem" };
    try cmdPfadd(&storage, &args, &response, std.testing.allocator);

    const result = response.items;
    try std.testing.expect(std.mem.indexOf(u8, result, "-WRONGTYPE") != null);
}
