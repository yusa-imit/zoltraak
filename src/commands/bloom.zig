const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const storage_mod = @import("../storage/memory.zig");
const bloom_mod = @import("../storage/bloom.zig");

const RespValue = protocol.RespValue;
const Storage = storage_mod.Storage;
const BloomFilterValue = bloom_mod.BloomFilterValue;

/// BF.RESERVE key error_rate capacity [EXPANSION expansion] [NONSCALING]
/// Creates a Bloom filter with specified parameters
pub fn cmdBfReserve(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) !RespValue {
    if (args.len < 3) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'bf.reserve' command" };
    }

    // Parse key
    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    // Parse error_rate
    const error_rate_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid error rate" },
    };
    const error_rate = std.fmt.parseFloat(f64, error_rate_str) catch {
        return RespValue{ .error_string = "ERR error rate must be a valid float" };
    };

    // Parse capacity
    const capacity_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid capacity" },
    };
    const capacity = std.fmt.parseInt(u64, capacity_str, 10) catch {
        return RespValue{ .error_string = "ERR capacity must be a valid integer" };
    };

    // Parse optional EXPANSION and NONSCALING flags
    var expansion: u16 = 2; // Default expansion factor
    var nonscaling = false;

    var i: usize = 3;
    while (i < args.len) {
        const arg_upper = switch (args[i]) {
            .bulk_string => |s| s,
            else => return RespValue{ .error_string = "ERR invalid argument" },
        };

        const arg_lower = blk: {
            var buf: [256]u8 = undefined;
            const len = @min(arg_upper.len, buf.len);
            @memcpy(buf[0..len], arg_upper[0..len]);
            for (0..len) |j| {
                buf[j] = std.ascii.toLower(buf[j]);
            }
            break :blk buf[0..len];
        };

        if (std.mem.eql(u8, arg_lower, "expansion")) {
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
        } else if (std.mem.eql(u8, arg_lower, "nonscaling")) {
            nonscaling = true;
        } else {
            return RespValue{ .error_string = "ERR unknown option" };
        }

        i += 1;
    }

    // Check if key already exists
    storage.mutex.lock();
    defer storage.mutex.unlock();

    if (storage.data.contains(key)) {
        return RespValue{ .error_string = "ERR key already exists" };
    }

    // Create bloom filter
    var bf = BloomFilterValue.init(allocator, error_rate, capacity, expansion, nonscaling) catch |err| {
        return switch (err) {
            bloom_mod.BloomError.InvalidErrorRate => RespValue{ .error_string = "ERR error rate must be between 0 and 1" },
            bloom_mod.BloomError.InvalidCapacity => RespValue{ .error_string = "ERR capacity must be greater than 0" },
            else => RespValue{ .error_string = "ERR failed to create bloom filter" },
        };
    };

    // Insert into storage
    const owned_key = allocator.dupe(u8, key) catch {
        bf.deinit();
        return RespValue{ .error_string = "ERR out of memory" };
    };
    errdefer allocator.free(owned_key);

    const value = storage_mod.Value{ .bloom = bf };
    storage.data.put(owned_key, value) catch {
        bf.deinit();
        allocator.free(owned_key);
        return RespValue{ .error_string = "ERR out of memory" };
    };

    return RespValue{ .simple_string = "OK" };
}

/// BF.ADD key item
/// Add an item to the Bloom filter, creating it with defaults if it doesn't exist
pub fn cmdBfAdd(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) !RespValue {
    if (args.len != 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'bf.add' command" };
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
        // Key exists - verify it's a bloom filter
        switch (entry.value_ptr.*) {
            .bloom => |*bf| {
                const result = try bf.add(item);
                return RespValue{ .integer = @intCast(result) };
            },
            else => return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" },
        }
    } else {
        // Create with defaults: error_rate=0.01, capacity=100, expansion=2, nonscaling=false
        var bf = BloomFilterValue.init(allocator, 0.01, 100, 2, false) catch {
            return RespValue{ .error_string = "ERR failed to create bloom filter" };
        };

        const owned_key = allocator.dupe(u8, key) catch {
            bf.deinit();
            return RespValue{ .error_string = "ERR out of memory" };
        };
        errdefer allocator.free(owned_key);

        const result = try bf.add(item);

        try storage.data.put(owned_key, storage_mod.Value{ .bloom = bf });

        return RespValue{ .integer = @intCast(result) };
    }
}

/// BF.EXISTS key item
/// Check if an item exists in the Bloom filter
pub fn cmdBfExists(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) !RespValue {
    _ = allocator;

    if (args.len != 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'bf.exists' command" };
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
        // Key exists - verify it's a bloom filter
        switch (entry.value_ptr.*) {
            .bloom => |bf| {
                const result = bf.exists(item);
                return RespValue{ .integer = if (result) 1 else 0 };
            },
            else => return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" },
        }
    } else {
        // Key doesn't exist - return 0 (not found)
        return RespValue{ .integer = 0 };
    }
}

/// BF.MADD key item [item ...]
/// Add one or more items to the Bloom filter, creating it with defaults if it doesn't exist
/// Returns an array of integers (1 for new, 0 for duplicate)
pub fn cmdBfMadd(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) !RespValue {
    if (args.len < 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'bf.madd' command" };
    }

    // Parse key
    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    // Allocate result array
    const num_items = args.len - 1;
    const results = try allocator.alloc(RespValue, num_items);
    errdefer allocator.free(results);

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Check if key exists
    if (storage.data.getEntry(key)) |entry| {
        // Key exists - verify it's a bloom filter
        switch (entry.value_ptr.*) {
            .bloom => |*bf| {
                // Add each item and collect results
                for (args[1..], 0..) |arg, i| {
                    const item = switch (arg) {
                        .bulk_string => |s| s,
                        else => {
                            allocator.free(results);
                            return RespValue{ .error_string = "ERR invalid item" };
                        },
                    };
                    const result = try bf.add(item);
                    results[i] = RespValue{ .integer = @intCast(result) };
                }
            },
            else => {
                allocator.free(results);
                return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
            },
        }
    } else {
        // Create with defaults: error_rate=0.01, capacity=100, expansion=2, nonscaling=false
        var bf = BloomFilterValue.init(allocator, 0.01, 100, 2, false) catch {
            allocator.free(results);
            return RespValue{ .error_string = "ERR failed to create bloom filter" };
        };
        errdefer bf.deinit();

        const owned_key = allocator.dupe(u8, key) catch {
            bf.deinit();
            allocator.free(results);
            return RespValue{ .error_string = "ERR out of memory" };
        };
        errdefer allocator.free(owned_key);

        // Add each item and collect results
        for (args[1..], 0..) |arg, i| {
            const item = switch (arg) {
                .bulk_string => |s| s,
                else => {
                    bf.deinit();
                    allocator.free(owned_key);
                    allocator.free(results);
                    return RespValue{ .error_string = "ERR invalid item" };
                },
            };
            const result = try bf.add(item);
            results[i] = RespValue{ .integer = @intCast(result) };
        }

        try storage.data.put(owned_key, storage_mod.Value{ .bloom = bf });
    }

    return RespValue{ .array = results };
}

/// BF.MEXISTS key item [item ...]
/// Check if one or more items exist in the Bloom filter
/// Returns an array of integers (1 for exists, 0 for not exists)
pub fn cmdBfMexists(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) !RespValue {
    if (args.len < 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'bf.mexists' command" };
    }

    // Parse key
    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    // Allocate result array
    const num_items = args.len - 1;
    const results = try allocator.alloc(RespValue, num_items);
    errdefer allocator.free(results);

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Check if key exists
    if (storage.data.getEntry(key)) |entry| {
        // Key exists - verify it's a bloom filter
        switch (entry.value_ptr.*) {
            .bloom => |bf| {
                // Check each item and collect results
                for (args[1..], 0..) |arg, i| {
                    const item = switch (arg) {
                        .bulk_string => |s| s,
                        else => {
                            allocator.free(results);
                            return RespValue{ .error_string = "ERR invalid item" };
                        },
                    };
                    const exists = bf.exists(item);
                    results[i] = RespValue{ .integer = if (exists) 1 else 0 };
                }
            },
            else => {
                allocator.free(results);
                return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
            },
        }
    } else {
        // Key doesn't exist - all items return 0
        for (0..num_items) |i| {
            results[i] = RespValue{ .integer = 0 };
        }
    }

    return RespValue{ .array = results };
}
