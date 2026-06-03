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

    // args[0] = "PFADD", args[1] = key, args[2..] = elements
    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'pfadd' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const elements = args[2..];

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

    // args[0] = "PFCOUNT", args[1..] = keys
    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'pfcount' command");
    }

    storage.mutex.lock();
    defer storage.mutex.unlock();

    if (args.len == 2) {
        // Single key case
        const key = switch (args[1]) {
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

        for (args[1..]) |key_resp| {
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

    // args[0] = "PFMERGE", args[1] = destkey, args[2..] = sourcekeys
    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'pfmerge' command");
    }

    const destkey = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };
    const sourcekeys = args[2..];

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

test "HyperLogLog: PFADD creates key and returns 1" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "myhll" },
        .{ .bulk_string = "elem1" },
        .{ .bulk_string = "elem2" },
    };
    const result = try cmdPfadd(allocator, &storage, &args, &pubsub, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":1\r\n", result);

    // Key must be visible to EXISTS
    try std.testing.expect(storage.exists("myhll"));
}

test "HyperLogLog: PFADD same elements returns 0" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const args1 = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "myhll" },
        .{ .bulk_string = "elem1" },
    };
    const r1 = try cmdPfadd(allocator, &storage, &args1, &pubsub, 0);
    defer allocator.free(r1);
    try std.testing.expectEqualStrings(":1\r\n", r1);

    // Same elements again — no register update
    const args2 = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "myhll" },
        .{ .bulk_string = "elem1" },
    };
    const r2 = try cmdPfadd(allocator, &storage, &args2, &pubsub, 0);
    defer allocator.free(r2);
    try std.testing.expectEqualStrings(":0\r\n", r2);
}

test "HyperLogLog: PFCOUNT returns approximate cardinality" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const add_args = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "hll1" },
        .{ .bulk_string = "a" },
        .{ .bulk_string = "b" },
        .{ .bulk_string = "c" },
    };
    const ar = try cmdPfadd(allocator, &storage, &add_args, &pubsub, 0);
    defer allocator.free(ar);

    const count_args = [_]RespValue{
        .{ .bulk_string = "PFCOUNT" },
        .{ .bulk_string = "hll1" },
    };
    const result = try cmdPfcount(allocator, &storage, &count_args);
    defer allocator.free(result);

    // Should be non-zero — approximate cardinality of {a, b, c}
    try std.testing.expect(std.mem.startsWith(u8, result, ":"));
    const colon_idx = std.mem.indexOf(u8, result, ":").?;
    const num_str = result[colon_idx + 1 .. result.len - 2]; // strip leading ':' and trailing '\r\n'
    const count = try std.fmt.parseInt(i64, num_str, 10);
    try std.testing.expect(count > 0);
}

test "HyperLogLog: PFCOUNT nonexistent key returns 0" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "PFCOUNT" },
        .{ .bulk_string = "nosuchkey" },
    };
    const result = try cmdPfcount(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "HyperLogLog: PFMERGE merges two HLLs" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    const add1 = [_]RespValue{
        .{ .bulk_string = "PFADD" }, .{ .bulk_string = "hll1" },
        .{ .bulk_string = "a" },     .{ .bulk_string = "b" },
    };
    const r1 = try cmdPfadd(allocator, &storage, &add1, &pubsub, 0);
    defer allocator.free(r1);

    const add2 = [_]RespValue{
        .{ .bulk_string = "PFADD" }, .{ .bulk_string = "hll2" },
        .{ .bulk_string = "c" },     .{ .bulk_string = "d" },
    };
    const r2 = try cmdPfadd(allocator, &storage, &add2, &pubsub, 0);
    defer allocator.free(r2);

    const merge_args = [_]RespValue{
        .{ .bulk_string = "PFMERGE" },
        .{ .bulk_string = "hll3" },
        .{ .bulk_string = "hll1" },
        .{ .bulk_string = "hll2" },
    };
    const mr = try cmdPfmerge(allocator, &storage, &merge_args, &pubsub, 0);
    defer allocator.free(mr);
    try std.testing.expectEqualStrings("+OK\r\n", mr);

    // hll3 should contain approximately 4 elements
    const count_args = [_]RespValue{
        .{ .bulk_string = "PFCOUNT" },
        .{ .bulk_string = "hll3" },
    };
    const cr = try cmdPfcount(allocator, &storage, &count_args);
    defer allocator.free(cr);
    try std.testing.expect(std.mem.startsWith(u8, cr, ":"));
    const num_str = cr[1 .. cr.len - 2];
    const count = try std.fmt.parseInt(i64, num_str, 10);
    try std.testing.expect(count >= 3); // at least 3 of the 4 unique elements
}

test "HyperLogLog: PFADD WRONGTYPE error" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();

    _ = try storage.set("mykey", "value", null);

    const args = [_]RespValue{
        .{ .bulk_string = "PFADD" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "elem" },
    };
    const result = try cmdPfadd(allocator, &storage, &args, &pubsub, 0);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "-WRONGTYPE") != null);
}
