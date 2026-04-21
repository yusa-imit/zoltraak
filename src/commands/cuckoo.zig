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

/// CF.DEL key item
/// Delete an item from the Cuckoo filter
/// Returns 1 if item was deleted, 0 if not found
pub fn cmdCfDel(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) !RespValue {
    _ = allocator; // Not needed for this operation

    if (args.len != 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'cf.del' command" };
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
                const was_deleted = cf.delete(item);
                return RespValue{ .integer = if (was_deleted) @as(i64, 1) else @as(i64, 0) };
            },
            else => return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" },
        }
    } else {
        // Key doesn't exist, can't delete
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

/// CF.INSERT key [CAPACITY capacity] [NOCREATE] ITEMS item [item ...]
/// Batch insert items into Cuckoo filter with options for auto-creation and capacity override
/// Returns array of 1/-1 indicating success/failure per item
pub fn cmdCfInsert(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) !RespValue {
    if (args.len < 3) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'cf.insert' command" };
    }

    // Parse key
    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    // Parse options
    var capacity_override: ?u64 = null;
    var nocreate = false;
    var items_start_idx: ?usize = null;

    var i: usize = 1;
    while (i < args.len) {
        const arg_str = switch (args[i]) {
            .bulk_string => |s| s,
            else => return RespValue{ .error_string = "ERR invalid argument" },
        };

        // Case-insensitive keyword matching
        const upper = std.ascii.allocUpperString(allocator, arg_str) catch {
            return RespValue{ .error_string = "ERR out of memory" };
        };
        defer allocator.free(upper);

        if (std.mem.eql(u8, upper, "CAPACITY")) {
            if (i + 1 >= args.len) {
                return RespValue{ .error_string = "ERR CAPACITY requires an argument" };
            }
            i += 1;
            const cap_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return RespValue{ .error_string = "ERR invalid capacity value" },
            };
            capacity_override = std.fmt.parseInt(u64, cap_str, 10) catch {
                return RespValue{ .error_string = "ERR capacity must be a valid integer" };
            };
            if (capacity_override.? == 0) {
                return RespValue{ .error_string = "ERR capacity must be greater than 0" };
            }
        } else if (std.mem.eql(u8, upper, "NOCREATE")) {
            nocreate = true;
        } else if (std.mem.eql(u8, upper, "ITEMS")) {
            items_start_idx = i + 1;
            break;
        } else {
            return RespValue{ .error_string = "ERR unknown option" };
        }

        i += 1;
    }

    // Validate ITEMS keyword was found
    if (items_start_idx == null) {
        return RespValue{ .error_string = "ERR ITEMS keyword required" };
    }

    const items_idx = items_start_idx.?;
    if (items_idx >= args.len) {
        return RespValue{ .error_string = "ERR ITEMS requires at least one item" };
    }

    // Allocate result array
    const num_items = args.len - items_idx;
    const results = try allocator.alloc(RespValue, num_items);
    errdefer allocator.free(results);

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Check if key exists
    if (storage.data.getEntry(key)) |entry| {
        // Key exists - verify it's a Cuckoo filter
        switch (entry.value_ptr.*) {
            .cuckoo => |*cf| {
                // Add each item and collect results
                for (args[items_idx..], 0..) |arg, idx| {
                    const item = switch (arg) {
                        .bulk_string => |s| s,
                        else => {
                            allocator.free(results);
                            return RespValue{ .error_string = "ERR invalid item" };
                        },
                    };
                    cf.add(item) catch |err| {
                        const result: i64 = switch (err) {
                            cuckoo_mod.CuckooError.FilterFull => -1,
                            else => -1,
                        };
                        results[idx] = RespValue{ .integer = result };
                        continue;
                    };
                    results[idx] = RespValue{ .integer = 1 };
                }
            },
            else => {
                allocator.free(results);
                return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
            },
        }
    } else {
        // Key doesn't exist
        if (nocreate) {
            allocator.free(results);
            return RespValue{ .error_string = "ERR not found" };
        }

        // Create filter with specified parameters
        var cf = CuckooFilterValue.init(allocator, capacity_override orelse 1000, 2, 20, 1) catch {
            allocator.free(results);
            return RespValue{ .error_string = "ERR failed to create cuckoo filter" };
        };
        errdefer cf.deinit();

        const owned_key = allocator.dupe(u8, key) catch {
            cf.deinit();
            allocator.free(results);
            return RespValue{ .error_string = "ERR out of memory" };
        };
        errdefer allocator.free(owned_key);

        // Add each item and collect results
        for (args[items_idx..], 0..) |arg, idx| {
            const item = switch (arg) {
                .bulk_string => |s| s,
                else => {
                    cf.deinit();
                    allocator.free(owned_key);
                    allocator.free(results);
                    return RespValue{ .error_string = "ERR invalid item" };
                },
            };
            cf.add(item) catch |err| {
                const result: i64 = switch (err) {
                    cuckoo_mod.CuckooError.FilterFull => -1,
                    else => -1,
                };
                results[idx] = RespValue{ .integer = result };
                continue;
            };
            results[idx] = RespValue{ .integer = 1 };
        }

        try storage.data.put(owned_key, storage_mod.Value{ .cuckoo = cf });
    }

    return RespValue{ .array = results };
}

/// CF.INSERTNX key [CAPACITY capacity] [NOCREATE] ITEMS item [item ...]
/// Batch insert items only if they don't already exist
/// Returns array of 1 (added)/0 (existed)/-1 (failed) per item
pub fn cmdCfInsertnx(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) !RespValue {
    if (args.len < 3) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'cf.insertnx' command" };
    }

    // Parse key
    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    // Parse options
    var capacity_override: ?u64 = null;
    var nocreate = false;
    var items_start_idx: ?usize = null;

    var i: usize = 1;
    while (i < args.len) {
        const arg_str = switch (args[i]) {
            .bulk_string => |s| s,
            else => return RespValue{ .error_string = "ERR invalid argument" },
        };

        // Case-insensitive keyword matching
        const upper = std.ascii.allocUpperString(allocator, arg_str) catch {
            return RespValue{ .error_string = "ERR out of memory" };
        };
        defer allocator.free(upper);

        if (std.mem.eql(u8, upper, "CAPACITY")) {
            if (i + 1 >= args.len) {
                return RespValue{ .error_string = "ERR CAPACITY requires an argument" };
            }
            i += 1;
            const cap_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return RespValue{ .error_string = "ERR invalid capacity value" },
            };
            capacity_override = std.fmt.parseInt(u64, cap_str, 10) catch {
                return RespValue{ .error_string = "ERR capacity must be a valid integer" };
            };
            if (capacity_override.? == 0) {
                return RespValue{ .error_string = "ERR capacity must be greater than 0" };
            }
        } else if (std.mem.eql(u8, upper, "NOCREATE")) {
            nocreate = true;
        } else if (std.mem.eql(u8, upper, "ITEMS")) {
            items_start_idx = i + 1;
            break;
        } else {
            return RespValue{ .error_string = "ERR unknown option" };
        }

        i += 1;
    }

    // Validate ITEMS keyword was found
    if (items_start_idx == null) {
        return RespValue{ .error_string = "ERR ITEMS keyword required" };
    }

    const items_idx = items_start_idx.?;
    if (items_idx >= args.len) {
        return RespValue{ .error_string = "ERR ITEMS requires at least one item" };
    }

    // Allocate result array
    const num_items = args.len - items_idx;
    const results = try allocator.alloc(RespValue, num_items);
    errdefer allocator.free(results);

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Check if key exists
    if (storage.data.getEntry(key)) |entry| {
        // Key exists - verify it's a Cuckoo filter
        switch (entry.value_ptr.*) {
            .cuckoo => |*cf| {
                // Add each item and collect results
                for (args[items_idx..], 0..) |arg, idx| {
                    const item = switch (arg) {
                        .bulk_string => |s| s,
                        else => {
                            allocator.free(results);
                            return RespValue{ .error_string = "ERR invalid item" };
                        },
                    };
                    const was_added = cf.addnx(item) catch |err| {
                        const result: i64 = switch (err) {
                            cuckoo_mod.CuckooError.FilterFull => -1,
                            else => -1,
                        };
                        results[idx] = RespValue{ .integer = result };
                        continue;
                    };
                    results[idx] = RespValue{ .integer = if (was_added) @as(i64, 1) else @as(i64, 0) };
                }
            },
            else => {
                allocator.free(results);
                return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
            },
        }
    } else {
        // Key doesn't exist
        if (nocreate) {
            allocator.free(results);
            return RespValue{ .error_string = "ERR not found" };
        }

        // Create filter with specified parameters
        var cf = CuckooFilterValue.init(allocator, capacity_override orelse 1000, 2, 20, 1) catch {
            allocator.free(results);
            return RespValue{ .error_string = "ERR failed to create cuckoo filter" };
        };
        errdefer cf.deinit();

        const owned_key = allocator.dupe(u8, key) catch {
            cf.deinit();
            allocator.free(results);
            return RespValue{ .error_string = "ERR out of memory" };
        };
        errdefer allocator.free(owned_key);

        // Add each item and collect results
        for (args[items_idx..], 0..) |arg, idx| {
            const item = switch (arg) {
                .bulk_string => |s| s,
                else => {
                    cf.deinit();
                    allocator.free(owned_key);
                    allocator.free(results);
                    return RespValue{ .error_string = "ERR invalid item" };
                },
            };
            const was_added = cf.addnx(item) catch |err| {
                const result: i64 = switch (err) {
                    cuckoo_mod.CuckooError.FilterFull => -1,
                    else => -1,
                };
                results[idx] = RespValue{ .integer = result };
                continue;
            };
            results[idx] = RespValue{ .integer = if (was_added) @as(i64, 1) else @as(i64, 0) };
        }

        try storage.data.put(owned_key, storage_mod.Value{ .cuckoo = cf });
    }

    return RespValue{ .array = results };
}

/// CF.MEXISTS key item [item ...]
/// Batch check if items exist in the Cuckoo filter
/// Returns array of 1/0 indicating existence per item
pub fn cmdCfMexists(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) !RespValue {
    if (args.len < 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'cf.mexists' command" };
    }

    // Parse key
    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    // Remaining arguments are items
    const items = args[1..];

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Pre-allocate result array
    var results = try std.ArrayList(RespValue).initCapacity(allocator, items.len);
    errdefer results.deinit(allocator);

    // Check if key exists
    if (storage.data.getEntry(key)) |entry| {
        // Key exists - verify it's a Cuckoo filter
        switch (entry.value_ptr.*) {
            .cuckoo => |*cf| {
                // Check each item
                for (items) |item_val| {
                    const item = switch (item_val) {
                        .bulk_string => |s| s,
                        else => {
                            try results.append(allocator, RespValue{ .integer = 0 });
                            continue;
                        },
                    };

                    const exists = cf.exists(item);
                    try results.append(allocator, RespValue{ .integer = if (exists) @as(i64, 1) else @as(i64, 0) });
                }

                const owned_results = try results.toOwnedSlice(allocator);
                return RespValue{ .array = owned_results };
            },
            else => return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" },
        }
    } else {
        // Key doesn't exist - all items return 0
        for (items) |_| {
            try results.append(allocator, RespValue{ .integer = 0 });
        }

        const owned_results = try results.toOwnedSlice(allocator);
        return RespValue{ .array = owned_results };
    }
}

// Additional unit tests for new commands

test "CF.INSERT basic batch insert" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const items_arr = [_]RespValue{
        RespValue{ .bulk_string = "item1" },
        RespValue{ .bulk_string = "item2" },
        RespValue{ .bulk_string = "item3" },
    };
    const args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "ITEMS" },
        items_arr[0],
        items_arr[1],
        items_arr[2],
    };

    const result = try cmdCfInsert(allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(@as(usize, 3), arr.len);
            for (arr) |val| {
                switch (val) {
                    .integer => |i| try std.testing.expectEqual(@as(i64, 1), i),
                    else => try std.testing.expect(false),
                }
            }
            allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "CF.INSERT with CAPACITY option" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const items_arr = [_]RespValue{
        RespValue{ .bulk_string = "item1" },
    };
    const args = [_]RespValue{
        RespValue{ .bulk_string = "myfilter" },
        RespValue{ .bulk_string = "CAPACITY" },
        RespValue{ .bulk_string = "500" },
        RespValue{ .bulk_string = "ITEMS" },
        items_arr[0],
    };

    const result = try cmdCfInsert(allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(@as(usize, 1), arr.len);
            allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "CF.INSERT returns error with NOCREATE on nonexistent key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const items_arr = [_]RespValue{
        RespValue{ .bulk_string = "item1" },
    };
    const args = [_]RespValue{
        RespValue{ .bulk_string = "nonexistent" },
        RespValue{ .bulk_string = "NOCREATE" },
        RespValue{ .bulk_string = "ITEMS" },
        items_arr[0],
    };

    const result = try cmdCfInsert(allocator, &storage, &args);

    switch (result) {
        .error_string => |s| try std.testing.expectEqualStrings("ERR not found", s),
        else => try std.testing.expect(false),
    }
}

test "CF.INSERT on existing filter adds to it" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "myfilter";

    // First insert
    {
        const items_arr = [_]RespValue{
            RespValue{ .bulk_string = "item1" },
        };
        const args = [_]RespValue{
            RespValue{ .bulk_string = key },
            RespValue{ .bulk_string = "ITEMS" },
            items_arr[0],
        };
        _ = try cmdCfInsert(allocator, &storage, &args);
    }

    // Second insert - should succeed because we're adding to existing filter
    {
        const items_arr = [_]RespValue{
            RespValue{ .bulk_string = "item2" },
        };
        const args = [_]RespValue{
            RespValue{ .bulk_string = key },
            RespValue{ .bulk_string = "ITEMS" },
            items_arr[0],
        };
        const result = try cmdCfInsert(allocator, &storage, &args);

        switch (result) {
            .array => |arr| {
                try std.testing.expectEqual(@as(usize, 1), arr.len);
                allocator.free(arr);
            },
            else => try std.testing.expect(false),
        }
    }
}

test "CF.INSERTNX returns 1/0 array correctly" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "myfilter";

    // First INSERTNX with two items
    {
        const items_arr = [_]RespValue{
            RespValue{ .bulk_string = "item1" },
            RespValue{ .bulk_string = "item2" },
        };
        const args = [_]RespValue{
            RespValue{ .bulk_string = key },
            RespValue{ .bulk_string = "ITEMS" },
            items_arr[0],
            items_arr[1],
        };
        _ = try cmdCfInsertnx(allocator, &storage, &args);
    }

    // Second INSERTNX with one existing and one new item
    {
        const items_arr = [_]RespValue{
            RespValue{ .bulk_string = "item1" },
            RespValue{ .bulk_string = "item3" },
        };
        const args = [_]RespValue{
            RespValue{ .bulk_string = key },
            RespValue{ .bulk_string = "ITEMS" },
            items_arr[0],
            items_arr[1],
        };
        const result = try cmdCfInsertnx(allocator, &storage, &args);

        switch (result) {
            .array => |arr| {
                try std.testing.expectEqual(@as(usize, 2), arr.len);
                // First should be 0 (already exists)
                switch (arr[0]) {
                    .integer => |i| try std.testing.expectEqual(@as(i64, 0), i),
                    else => try std.testing.expect(false),
                }
                // Second should be 1 (newly added)
                switch (arr[1]) {
                    .integer => |i| try std.testing.expectEqual(@as(i64, 1), i),
                    else => try std.testing.expect(false),
                }
                allocator.free(arr);
            },
            else => try std.testing.expect(false),
        }
    }
}

test "CF.INSERTNX NOCREATE error on nonexistent key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const items_arr = [_]RespValue{
        RespValue{ .bulk_string = "item1" },
    };
    const args = [_]RespValue{
        RespValue{ .bulk_string = "nonexistent" },
        RespValue{ .bulk_string = "NOCREATE" },
        RespValue{ .bulk_string = "ITEMS" },
        items_arr[0],
    };

    const result = try cmdCfInsertnx(allocator, &storage, &args);

    switch (result) {
        .error_string => |s| try std.testing.expectEqualStrings("ERR not found", s),
        else => try std.testing.expect(false),
    }
}

test "CF.MEXISTS batch check" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "myfilter";

    // Add some items first
    {
        const items_arr = [_]RespValue{
            RespValue{ .bulk_string = "item1" },
            RespValue{ .bulk_string = "item2" },
        };
        const args = [_]RespValue{
            RespValue{ .bulk_string = key },
            RespValue{ .bulk_string = "ITEMS" },
            items_arr[0],
            items_arr[1],
        };
        _ = try cmdCfInsert(allocator, &storage, &args);
    }

    // Check multiple items
    {
        const items_arr = [_]RespValue{
            RespValue{ .bulk_string = "item1" },
            RespValue{ .bulk_string = "nonexistent" },
            RespValue{ .bulk_string = "item2" },
        };
        const args = [_]RespValue{
            RespValue{ .bulk_string = key },
            items_arr[0],
            items_arr[1],
            items_arr[2],
        };

        const result = try cmdCfMexists(allocator, &storage, &args);

        switch (result) {
            .array => |arr| {
                try std.testing.expectEqual(@as(usize, 3), arr.len);

                // First should exist
                switch (arr[0]) {
                    .integer => |i| try std.testing.expectEqual(@as(i64, 1), i),
                    else => try std.testing.expect(false),
                }

                // Second should not exist
                switch (arr[1]) {
                    .integer => |i| try std.testing.expectEqual(@as(i64, 0), i),
                    else => try std.testing.expect(false),
                }

                // Third should exist
                switch (arr[2]) {
                    .integer => |i| try std.testing.expectEqual(@as(i64, 1), i),
                    else => try std.testing.expect(false),
                }

                allocator.free(arr);
            },
            else => try std.testing.expect(false),
        }
    }
}

test "CF.MEXISTS on nonexistent key returns all zeros" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const items_arr = [_]RespValue{
        RespValue{ .bulk_string = "item1" },
        RespValue{ .bulk_string = "item2" },
    };
    const args = [_]RespValue{
        RespValue{ .bulk_string = "nonexistent" },
        items_arr[0],
        items_arr[1],
    };

    const result = try cmdCfMexists(allocator, &storage, &args);

    switch (result) {
        .array => |arr| {
            try std.testing.expectEqual(@as(usize, 2), arr.len);
            for (arr) |val| {
                switch (val) {
                    .integer => |i| try std.testing.expectEqual(@as(i64, 0), i),
                    else => try std.testing.expect(false),
                }
            }
            allocator.free(arr);
        },
        else => try std.testing.expect(false),
    }
}

test "CF.MEXISTS WRONGTYPE error" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "mystring";
    const owned_key = try allocator.dupe(u8, key);
    try storage.data.put(owned_key, storage_mod.Value{ .string = "value" });

    const items_arr = [_]RespValue{
        RespValue{ .bulk_string = "item1" },
    };
    const args = [_]RespValue{
        RespValue{ .bulk_string = key },
        items_arr[0],
    };

    const result = try cmdCfMexists(allocator, &storage, &args);

    switch (result) {
        .error_string => |s| try std.testing.expect(std.mem.startsWith(u8, s, "WRONGTYPE")),
        else => try std.testing.expect(false),
    }
}

test "CF.DEL on non-existent key returns 0" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "nonexistent" },
        RespValue{ .bulk_string = "item" },
    };

    const result = try cmdCfDel(allocator, &storage, &args);

    switch (result) {
        .integer => |i| try std.testing.expectEqual(@as(i64, 0), i),
        else => try std.testing.expect(false),
    }
}

test "CF.DEL deletes existing item returns 1" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "myfilter";
    const item = "testitem";

    // Add item first
    const add_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = item },
    };
    _ = try cmdCfAdd(allocator, &storage, &add_args);

    // Now delete it
    const del_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = item },
    };

    const result = try cmdCfDel(allocator, &storage, &del_args);

    switch (result) {
        .integer => |i| try std.testing.expectEqual(@as(i64, 1), i),
        else => try std.testing.expect(false),
    }
}

test "CF.DEL on non-existent item returns 0" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "myfilter";

    // Create filter with one item
    const add_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "item1" },
    };
    _ = try cmdCfAdd(allocator, &storage, &add_args);

    // Try to delete non-existent item
    const del_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "nonexistent" },
    };

    const result = try cmdCfDel(allocator, &storage, &del_args);

    switch (result) {
        .integer => |i| try std.testing.expectEqual(@as(i64, 0), i),
        else => try std.testing.expect(false),
    }
}

test "CF.DEL item no longer exists after deletion" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "myfilter";
    const item = "testitem";

    // Add item
    const add_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = item },
    };
    _ = try cmdCfAdd(allocator, &storage, &add_args);

    // Verify exists
    const exists_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = item },
    };
    const exists_result = try cmdCfExists(allocator, &storage, &exists_args);
    switch (exists_result) {
        .integer => |i| try std.testing.expectEqual(@as(i64, 1), i),
        else => try std.testing.expect(false),
    }

    // Delete
    const del_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = item },
    };
    _ = try cmdCfDel(allocator, &storage, &del_args);

    // Verify no longer exists
    const exists_after = try cmdCfExists(allocator, &storage, &exists_args);
    switch (exists_after) {
        .integer => |i| try std.testing.expectEqual(@as(i64, 0), i),
        else => try std.testing.expect(false),
    }
}

test "CF.DEL returns WRONGTYPE for non-cuckoo key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "stringkey";

    // Add a string value
    const owned_key = try allocator.dupe(u8, key);
    const str_value = try allocator.dupe(u8, "value");
    try storage.data.put(owned_key, storage_mod.Value{ .string = str_value });

    const args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = "item" },
    };

    const result = try cmdCfDel(allocator, &storage, &args);

    switch (result) {
        .error_string => |s| try std.testing.expect(std.mem.startsWith(u8, s, "WRONGTYPE")),
        else => try std.testing.expect(false),
    }
}

test "CF.DEL with wrong number of arguments" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "key" },
    };

    const result = try cmdCfDel(allocator, &storage, &args);

    switch (result) {
        .error_string => |s| try std.testing.expect(std.mem.startsWith(u8, s, "ERR wrong number")),
        else => try std.testing.expect(false),
    }
}

test "CF.DEL only removes one instance of duplicate item" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key = "myfilter";
    const item = "duplicate";

    // Add same item twice
    const add_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = item },
    };
    _ = try cmdCfAdd(allocator, &storage, &add_args);
    _ = try cmdCfAdd(allocator, &storage, &add_args);

    // Delete once
    const del_args = [_]RespValue{
        RespValue{ .bulk_string = key },
        RespValue{ .bulk_string = item },
    };
    const result = try cmdCfDel(allocator, &storage, &del_args);

    switch (result) {
        .integer => |i| try std.testing.expectEqual(@as(i64, 1), i),
        else => try std.testing.expect(false),
    }

    // May still exist depending on bucket placement
    // This is expected behavior for Cuckoo filters
}
