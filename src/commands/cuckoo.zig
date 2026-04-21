const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const storage_mod = @import("../storage/memory.zig");
const cuckoo_mod = @import("../storage/cuckoo.zig");

const RespValue = protocol.RespValue;
const Storage = storage_mod.Storage;
const CuckooFilterValue = cuckoo_mod.CuckooFilterValue;

/// CF.RESERVE key capacity [BUCKETSIZE bs] [MAXITERATIONS mi] [EXPANSION exp]
/// Creates a Cuckoo filter with specified parameters
pub fn cmdCfReserve(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) !RespValue {
    if (args.len < 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'cf.reserve' command" };
    }

    // Parse key
    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    // Parse capacity
    const capacity_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid capacity" },
    };
    const capacity = std.fmt.parseInt(u64, capacity_str, 10) catch {
        return RespValue{ .error_string = "ERR capacity must be a valid integer" };
    };

    // Parse optional parameters with defaults
    var bucketsize: u32 = 2;
    var max_iterations: u16 = 20;
    var expansion: u16 = 1;

    var i: usize = 2;
    while (i < args.len) {
        const arg_upper = switch (args[i]) {
            .bulk_string => |s| s,
            else => return RespValue{ .error_string = "ERR invalid argument" },
        };

        // Case-insensitive comparison
        const arg_lower = blk: {
            var buf: [256]u8 = undefined;
            const len = @min(arg_upper.len, buf.len);
            @memcpy(buf[0..len], arg_upper[0..len]);
            for (0..len) |j| {
                buf[j] = std.ascii.toLower(buf[j]);
            }
            break :blk buf[0..len];
        };

        if (std.mem.eql(u8, arg_lower, "bucketsize")) {
            i += 1;
            if (i >= args.len) {
                return RespValue{ .error_string = "ERR BUCKETSIZE requires an argument" };
            }
            const bs_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return RespValue{ .error_string = "ERR invalid bucketsize value" },
            };
            bucketsize = std.fmt.parseInt(u32, bs_str, 10) catch {
                return RespValue{ .error_string = "ERR bucketsize must be a valid integer" };
            };
            if (bucketsize == 0 or bucketsize > 255) {
                return RespValue{ .error_string = "ERR bucketsize must be between 1 and 255" };
            }
        } else if (std.mem.eql(u8, arg_lower, "maxiterations")) {
            i += 1;
            if (i >= args.len) {
                return RespValue{ .error_string = "ERR MAXITERATIONS requires an argument" };
            }
            const mi_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return RespValue{ .error_string = "ERR invalid maxiterations value" },
            };
            max_iterations = std.fmt.parseInt(u16, mi_str, 10) catch {
                return RespValue{ .error_string = "ERR maxiterations must be a valid integer" };
            };
            if (max_iterations == 0) {
                return RespValue{ .error_string = "ERR maxiterations must be greater than 0" };
            }
        } else if (std.mem.eql(u8, arg_lower, "expansion")) {
            i += 1;
            if (i >= args.len) {
                return RespValue{ .error_string = "ERR EXPANSION requires an argument" };
            }
            const exp_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return RespValue{ .error_string = "ERR invalid expansion value" },
            };
            expansion = std.fmt.parseInt(u16, exp_str, 10) catch {
                return RespValue{ .error_string = "ERR expansion must be a valid integer" };
            };
        } else {
            return RespValue{ .error_string = "ERR unknown option" };
        }

        i += 1;
    }

    // Validate capacity
    if (capacity == 0) {
        return RespValue{ .error_string = "ERR capacity must be greater than 0" };
    }

    // Check if key already exists
    storage.mutex.lock();
    defer storage.mutex.unlock();

    if (storage.data.contains(key)) {
        return RespValue{ .error_string = "ERR key already exists" };
    }

    // Create Cuckoo filter
    var cf = CuckooFilterValue.init(allocator, capacity, bucketsize, max_iterations, expansion) catch |err| {
        return switch (err) {
            cuckoo_mod.CuckooError.InvalidCapacity => RespValue{ .error_string = "ERR capacity must be greater than 0" },
            cuckoo_mod.CuckooError.InvalidBucketSize => RespValue{ .error_string = "ERR bucketsize must be between 1 and 255" },
            cuckoo_mod.CuckooError.InvalidMaxIterations => RespValue{ .error_string = "ERR maxiterations must be greater than 0" },
            else => RespValue{ .error_string = "ERR failed to create cuckoo filter" },
        };
    };

    // Insert into storage
    const owned_key = allocator.dupe(u8, key) catch {
        cf.deinit();
        return RespValue{ .error_string = "ERR out of memory" };
    };
    errdefer allocator.free(owned_key);

    const value = storage_mod.Value{ .cuckoo = cf };
    storage.data.put(owned_key, value) catch {
        cf.deinit();
        allocator.free(owned_key);
        return RespValue{ .error_string = "ERR out of memory" };
    };

    return RespValue{ .simple_string = "OK" };
}

/// CF.ADD key item
/// Add an item to the Cuckoo filter, creating it with defaults if it doesn't exist
pub fn cmdCfAdd(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) !RespValue {
    if (args.len != 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'cf.add' command" };
    }

    // Parse key and item
    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    const item = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid item" },
    };

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Check if key exists
    if (storage.data.getEntry(key)) |entry| {
        // Key exists - verify it's a Cuckoo filter
        switch (entry.value_ptr.*) {
            .cuckoo => |*cf| {
                cf.add(item) catch |err| {
                    return switch (err) {
                        cuckoo_mod.CuckooError.FilterFull => RespValue{ .error_string = "ERR cuckoo filter is full" },
                        else => RespValue{ .error_string = "ERR failed to add item" },
                    };
                };
                return RespValue{ .integer = 1 };
            },
            else => return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" },
        }
    } else {
        // Create with defaults: capacity=1000, bucketsize=2, maxiterations=20, expansion=1
        var cf = CuckooFilterValue.init(allocator, 1000, 2, 20, 1) catch {
            return RespValue{ .error_string = "ERR failed to create cuckoo filter" };
        };

        // Try to add item
        cf.add(item) catch |err| {
            cf.deinit();
            return switch (err) {
                cuckoo_mod.CuckooError.FilterFull => RespValue{ .error_string = "ERR cuckoo filter is full" },
                else => RespValue{ .error_string = "ERR failed to add item" },
            };
        };

        // Insert into storage
        const owned_key = allocator.dupe(u8, key) catch {
            cf.deinit();
            return RespValue{ .error_string = "ERR out of memory" };
        };
        errdefer allocator.free(owned_key);

        const value = storage_mod.Value{ .cuckoo = cf };
        storage.data.put(owned_key, value) catch {
            cf.deinit();
            allocator.free(owned_key);
            return RespValue{ .error_string = "ERR out of memory" };
        };

        return RespValue{ .integer = 1 };
    }
}

/// CF.ADDNX key item
/// Add an item to the Cuckoo filter only if it doesn't already exist
/// Returns 1 if added, 0 if already exists
pub fn cmdCfAddnx(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) !RespValue {
    if (args.len != 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'cf.addnx' command" };
    }

    // Parse key and item
    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    const item = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid item" },
    };

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Check if key exists
    if (storage.data.getEntry(key)) |entry| {
        // Key exists - verify it's a Cuckoo filter
        switch (entry.value_ptr.*) {
            .cuckoo => |*cf| {
                const was_added = cf.addnx(item) catch |err| {
                    return switch (err) {
                        cuckoo_mod.CuckooError.FilterFull => RespValue{ .error_string = "ERR cuckoo filter is full" },
                        else => RespValue{ .error_string = "ERR failed to add item" },
                    };
                };
                return RespValue{ .integer = if (was_added) @as(i64, 1) else @as(i64, 0) };
            },
            else => return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" },
        }
    } else {
        // Create with defaults: capacity=1000, bucketsize=2, maxiterations=20, expansion=1
        var cf = CuckooFilterValue.init(allocator, 1000, 2, 20, 1) catch {
            return RespValue{ .error_string = "ERR failed to create cuckoo filter" };
        };

        // Try to add item (should always succeed since new filter is empty)
        const was_added = cf.addnx(item) catch |err| {
            cf.deinit();
            return switch (err) {
                cuckoo_mod.CuckooError.FilterFull => RespValue{ .error_string = "ERR cuckoo filter is full" },
                else => RespValue{ .error_string = "ERR failed to add item" },
            };
        };

        // Insert into storage
        const owned_key = allocator.dupe(u8, key) catch {
            cf.deinit();
            return RespValue{ .error_string = "ERR out of memory" };
        };
        errdefer allocator.free(owned_key);

        const value = storage_mod.Value{ .cuckoo = cf };
        storage.data.put(owned_key, value) catch {
            cf.deinit();
            allocator.free(owned_key);
            return RespValue{ .error_string = "ERR out of memory" };
        };

        return RespValue{ .integer = if (was_added) @as(i64, 1) else @as(i64, 0) };
    }
}

/// CF.EXISTS key item
/// Check if an item exists in the Cuckoo filter
/// Returns 1 if item may exist, 0 if definitely doesn't exist
pub fn cmdCfExists(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) !RespValue {
    _ = allocator; // Not needed for this operation

    if (args.len != 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'cf.exists' command" };
    }

    // Parse key and item
    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    const item = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid item" },
    };

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Check if key exists
    if (storage.data.getEntry(key)) |entry| {
        // Key exists - verify it's a Cuckoo filter
        switch (entry.value_ptr.*) {
            .cuckoo => |*cf| {
                const exists = cf.exists(item);
                return RespValue{ .integer = if (exists) @as(i64, 1) else @as(i64, 0) };
            },
            else => return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" },
        }
    } else {
        // Key doesn't exist, item doesn't exist
        return RespValue{ .integer = 0 };
    }
}

// Unit tests
test "CF.RESERVE with valid parameters" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "1000" },
    };

    const result = try cmdCfReserve(allocator, &storage, &args);

    switch (result) {
        .simple_string => |s| try std.testing.expectEqualStrings("OK", s),
        else => try std.testing.expect(false), // Should be OK
    }
}

test "CF.RESERVE returns error if key exists" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "myfilter";
    const cf = try CuckooFilterValue.init(allocator, 100, 2, 20, 1);

    // Pre-populate key
    const owned_key = try allocator.dupe(u8, key);
    try storage.data.put(owned_key, storage_mod.Value{ .cuckoo = cf });

    const args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "1000" },
    };

    const result = try cmdCfReserve(allocator, &storage, &args);

    switch (result) {
        .error_string => try std.testing.expectEqualStrings("ERR key already exists", result.error_string),
        else => try std.testing.expect(false), // Should be error
    }
}

test "CF.ADD on new key creates filter" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "hello" },
    };

    const result = try cmdCfAdd(allocator, &storage, &args);

    switch (result) {
        .integer => |i| try std.testing.expectEqual(@as(i64, 1), i),
        else => try std.testing.expect(false), // Should be 1
    }
}

test "CF.EXISTS on non-existent key returns 0" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "nonexistent" },
        RespValue{ .bulk_string = "item" },
    };

    const result = try cmdCfExists(allocator, &storage, &args);

    switch (result) {
        .integer => |i| try std.testing.expectEqual(@as(i64, 0), i),
        else => try std.testing.expect(false), // Should be 0
    }
}

test "CF.ADDNX returns 1 for new item" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "newitem" },
    };

    const result = try cmdCfAddnx(allocator, &storage, &args);

    switch (result) {
        .integer => |i| try std.testing.expectEqual(@as(i64, 1), i),
        else => try std.testing.expect(false), // Should be 1
    }
}

test "CF.ADDNX returns 0 for existing item" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "myfilter";
    const item = "item";

    // First ADDNX - should add
    {
        const args = [_]RespValue{
            RespValue{ .bulk_string = key },
            RespValue{ .bulk_string = item },
        };
        _ = try cmdCfAddnx(allocator, &storage, &args);
    }

    // Second ADDNX - should not add
    {
        const args = [_]RespValue{
            RespValue{ .bulk_string = key },
            RespValue{ .bulk_string = item },
        };
        const result = try cmdCfAddnx(allocator, &storage, &args);

        switch (result) {
            .integer => |i| try std.testing.expectEqual(@as(i64, 0), i),
            else => try std.testing.expect(false), // Should be 0
        }
    }
}
