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

/// BF.INSERT key [CAPACITY capacity] [ERROR error_rate] [EXPANSION expansion] [NOCREATE] [NONSCALING] ITEMS item [item ...]
/// Add one or more items to the Bloom filter with full parameter control
/// - Auto-creates filter with custom parameters if it doesn't exist (unless NOCREATE is set)
/// - NOCREATE: Only insert if filter exists, error otherwise (mutually exclusive with CAPACITY/ERROR)
/// - CAPACITY: Initial capacity for auto-created filter (default: 100)
/// - ERROR: False positive error rate for auto-created filter (default: 0.01)
/// - EXPANSION: Sub-filter expansion factor for scaling (default: 2)
/// - NONSCALING: Disable auto-scaling, return error on capacity overflow
/// Returns an array of integers (1 for new, 0 for duplicate)
pub fn cmdBfInsert(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) !RespValue {
    if (args.len < 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'bf.insert' command" };
    }

    // Parse key
    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    // Parse optional parameters
    var capacity: u64 = 100; // Default capacity
    var error_rate: f64 = 0.01; // Default error rate
    var expansion: u16 = 2; // Default expansion
    var nocreate = false;
    var nonscaling = false;
    var items_start_idx: ?usize = null;
    var has_capacity = false;
    var has_error = false;

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
                return RespValue{ .error_string = "ERR syntax error: CAPACITY requires a value" };
            }
            has_capacity = true;
            i += 1;
            const cap_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return RespValue{ .error_string = "ERR invalid capacity" },
            };
            capacity = std.fmt.parseInt(u64, cap_str, 10) catch {
                return RespValue{ .error_string = "ERR capacity must be a valid positive integer" };
            };
            if (capacity == 0) {
                return RespValue{ .error_string = "ERR capacity must be greater than 0" };
            }
        } else if (std.mem.eql(u8, upper, "ERROR")) {
            if (i + 1 >= args.len) {
                return RespValue{ .error_string = "ERR syntax error: ERROR requires a value" };
            }
            has_error = true;
            i += 1;
            const err_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return RespValue{ .error_string = "ERR invalid error rate" },
            };
            error_rate = std.fmt.parseFloat(f64, err_str) catch {
                return RespValue{ .error_string = "ERR error rate must be a valid float" };
            };
            if (error_rate <= 0.0 or error_rate >= 1.0) {
                return RespValue{ .error_string = "ERR error rate must be between 0 and 1" };
            }
        } else if (std.mem.eql(u8, upper, "EXPANSION")) {
            if (i + 1 >= args.len) {
                return RespValue{ .error_string = "ERR syntax error: EXPANSION requires a value" };
            }
            i += 1;
            const exp_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return RespValue{ .error_string = "ERR invalid expansion" },
            };
            const exp_val = std.fmt.parseInt(u16, exp_str, 10) catch {
                return RespValue{ .error_string = "ERR expansion must be a valid positive integer" };
            };
            if (exp_val == 0) {
                return RespValue{ .error_string = "ERR expansion must be greater than 0" };
            }
            expansion = exp_val;
        } else if (std.mem.eql(u8, upper, "NOCREATE")) {
            nocreate = true;
        } else if (std.mem.eql(u8, upper, "NONSCALING")) {
            nonscaling = true;
        } else if (std.mem.eql(u8, upper, "ITEMS")) {
            items_start_idx = i + 1;
            break;
        } else {
            return RespValue{ .error_string = "ERR syntax error: unknown option or missing ITEMS keyword" };
        }

        i += 1;
    }

    // Validate ITEMS keyword was found
    if (items_start_idx == null) {
        return RespValue{ .error_string = "ERR syntax error: ITEMS keyword required" };
    }

    const items_idx = items_start_idx.?;
    if (items_idx >= args.len) {
        return RespValue{ .error_string = "ERR syntax error: at least one item required after ITEMS" };
    }

    // Validate NOCREATE mutual exclusion with CAPACITY/ERROR
    if (nocreate and (has_capacity or has_error)) {
        return RespValue{ .error_string = "ERR NOCREATE cannot be used with CAPACITY or ERROR" };
    }

    // Allocate result array
    const num_items = args.len - items_idx;
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
                for (args[items_idx..], 0..) |arg, idx| {
                    const item = switch (arg) {
                        .bulk_string => |s| s,
                        else => {
                            allocator.free(results);
                            return RespValue{ .error_string = "ERR invalid item" };
                        },
                    };
                    const result = try bf.add(item);
                    results[idx] = RespValue{ .integer = @intCast(result) };
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
        var bf = BloomFilterValue.init(allocator, error_rate, capacity, expansion, nonscaling) catch {
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
        for (args[items_idx..], 0..) |arg, idx| {
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
            results[idx] = RespValue{ .integer = @intCast(result) };
        }

        try storage.data.put(owned_key, storage_mod.Value{ .bloom = bf });
    }

    return RespValue{ .array = results };
}

/// BF.INFO key [CAPACITY | SIZE | FILTERS | ITEMS | EXPANSION]
/// Return information about a Bloom filter
pub fn cmdBfInfo(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) !RespValue {
    if (args.len < 1 or args.len > 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'bf.info' command" };
    }

    // Parse key
    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    // Parse optional field argument
    const field_arg = if (args.len == 2) switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid field" },
    } else null;

    // Normalize field to lowercase
    const field = if (field_arg) |f| blk: {
        var buf: [32]u8 = undefined;
        const len = @min(f.len, buf.len);
        @memcpy(buf[0..len], f[0..len]);
        for (0..len) |j| {
            buf[j] = std.ascii.toLower(buf[j]);
        }
        break :blk buf[0..len];
    } else null;

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Check if key exists
    const entry = storage.data.getEntry(key) orelse {
        return RespValue{ .error_string = "ERR not found" };
    };

    // Verify it's a bloom filter
    const bf = switch (entry.value_ptr.*) {
        .bloom => |*filter| filter,
        else => return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" },
    };

    // Calculate total size in bytes
    var total_size: u64 = 0;
    for (bf.filters.items) |filter| {
        total_size += filter.bits.len;
    }

    // If specific field requested, return just that value
    if (field) |f| {
        if (std.mem.eql(u8, f, "capacity")) {
            return RespValue{ .integer = @intCast(bf.capacity) };
        } else if (std.mem.eql(u8, f, "size")) {
            return RespValue{ .integer = @intCast(total_size) };
        } else if (std.mem.eql(u8, f, "filters")) {
            return RespValue{ .integer = @intCast(bf.filters.items.len) };
        } else if (std.mem.eql(u8, f, "items")) {
            return RespValue{ .integer = @intCast(bf.total_items_added) };
        } else if (std.mem.eql(u8, f, "expansion")) {
            // Return expansion as integer, or null if NONSCALING
            if (bf.nonscaling) {
                return RespValue{ .null_bulk_string = {} };
            } else {
                return RespValue{ .integer = @intCast(bf.expansion) };
            }
        } else {
            return RespValue{ .error_string = "ERR unknown field" };
        }
    }

    // Return all fields as array of key-value pairs
    const info_fields = try allocator.alloc(RespValue, 10);
    errdefer allocator.free(info_fields);

    // Capacity
    info_fields[0] = RespValue{ .bulk_string = "Capacity" };
    info_fields[1] = RespValue{ .integer = @intCast(bf.capacity) };

    // Size
    info_fields[2] = RespValue{ .bulk_string = "Size" };
    info_fields[3] = RespValue{ .integer = @intCast(total_size) };

    // Number of filters
    info_fields[4] = RespValue{ .bulk_string = "Number of filters" };
    info_fields[5] = RespValue{ .integer = @intCast(bf.filters.items.len) };

    // Number of items inserted
    info_fields[6] = RespValue{ .bulk_string = "Number of items inserted" };
    info_fields[7] = RespValue{ .integer = @intCast(bf.total_items_added) };

    // Expansion rate
    info_fields[8] = RespValue{ .bulk_string = "Expansion rate" };
    if (bf.nonscaling) {
        info_fields[9] = RespValue{ .null_bulk_string = {} };
    } else {
        info_fields[9] = RespValue{ .integer = @intCast(bf.expansion) };
    }

    return RespValue{ .array = info_fields };
}

/// BF.CARD key
/// Returns the cardinality of a Bloom filter (number of items added)
pub fn cmdBfCard(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) !RespValue {
    _ = allocator;

    // Validate arity
    if (args.len != 1) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'bf.card' command" };
    }

    // Parse key
    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Get the value
    const value = storage.data.get(key) orelse {
        // Key doesn't exist, return 0
        return RespValue{ .integer = 0 };
    };

    // Check type
    const bf = switch (value) {
        .bloom => |*bf_ptr| bf_ptr,
        else => return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" },
    };

    // Return the cardinality (total_items_added)
    return RespValue{ .integer = @intCast(bf.total_items_added) };
}

/// BF.SCANDUMP key iterator
/// Incrementally dump Bloom filter state in chunks
pub fn cmdBfScandump(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) !RespValue {
    if (args.len != 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'bf.scandump' command" };
    }

    // Parse key
    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    // Parse iterator
    const iter_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid iterator" },
    };
    const iterator = std.fmt.parseInt(u64, iter_str, 10) catch {
        return RespValue{ .error_string = "ERR iterator must be a valid integer" };
    };

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Get the value
    const value = storage.data.get(key) orelse {
        return RespValue{ .error_string = "ERR key does not exist" };
    };

    // Check type
    const bf = switch (value) {
        .bloom => |*bf_ptr| bf_ptr,
        else => return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" },
    };

    // Perform scan dump
    const result = try bf.scanDump(allocator, iterator);
    errdefer if (result.data) |data| allocator.free(data);

    // Build RESP array response: [iterator, data]
    var resp_array = try std.ArrayList(RespValue).initCapacity(allocator, 2);
    errdefer resp_array.deinit(allocator);

    try resp_array.append(allocator, .{ .integer = @intCast(result.iterator) });

    if (result.data) |data| {
        try resp_array.append(allocator, .{ .bulk_string = data });
    } else {
        try resp_array.append(allocator, .null_bulk_string);
    }

    return RespValue{ .array = try resp_array.toOwnedSlice(allocator) };
}

/// BF.LOADCHUNK key iterator data
/// Incrementally restore Bloom filter from chunks
pub fn cmdBfLoadchunk(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) !RespValue {
    if (args.len != 3) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'bf.loadchunk' command" };
    }

    // Parse key
    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    // Parse iterator
    const iter_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid iterator" },
    };
    const iterator = std.fmt.parseInt(u64, iter_str, 10) catch {
        return RespValue{ .error_string = "ERR iterator must be a valid integer" };
    };

    // Parse data
    const data = switch (args[2]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid data" },
    };

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // For first chunk (iterator=0 on first call), create placeholder filter
    // For subsequent chunks, get existing filter
    // Note: We need to track load context per key - using a simple approach here

    // Get or create filter
    const value = storage.data.get(key);
    var bf: *BloomFilterValue = undefined;
    var context: *BloomFilterValue.LoadContext = undefined;
    var is_new = false;

    if (value == null) {
        // Create new filter
        var new_bf = try BloomFilterValue.init(allocator, 0.01, 10, 2, false);
        errdefer new_bf.deinit();

        // Store in storage
        const key_copy = try allocator.dupe(u8, key);
        errdefer allocator.free(key_copy);

        try storage.data.put(key_copy, .{ .bloom = new_bf });
        bf = &storage.data.getPtr(key_copy).?.bloom;

        // Create context
        const ctx = try allocator.create(BloomFilterValue.LoadContext);
        errdefer allocator.destroy(ctx);
        ctx.* = .{
            .buffer = std.ArrayList(u8).init(allocator),
            .expected_iterator = 0,
        };
        
        // TODO: Store context in a map keyed by key for multi-chunk loads
        // For now, simplified implementation assumes sequential single-key loads
        context = ctx;
        is_new = true;
    } else {
        // Get existing filter
        bf = switch (value.?) {
            .bloom => |*bf_ptr| bf_ptr,
            else => return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" },
        };

        // Create context (simplified - should be persisted)
        const ctx = try allocator.create(BloomFilterValue.LoadContext);
        errdefer allocator.destroy(ctx);
        ctx.* = .{
            .buffer = std.ArrayList(u8).init(allocator),
            .expected_iterator = iterator,
        };
        context = ctx;
    }

    defer {
        context.deinit();
        allocator.destroy(context);
    }

    // Load chunk
    const complete = bf.loadChunk(allocator, context, iterator, data) catch |err| {
        if (is_new) {
            // Clean up newly created filter on error
            const key_in_map = storage.data.getKey(key).?;
            _ = storage.data.remove(key);
            allocator.free(key_in_map);
            bf.deinit();
        }
        return switch (err) {
            error.InvalidIterator => RespValue{ .error_string = "ERR invalid iterator sequence" },
            error.InvalidData => RespValue{ .error_string = "ERR invalid data format" },
            else => RespValue{ .error_string = "ERR failed to load chunk" },
        };
    };

    if (complete) {
        return RespValue{ .simple_string = "OK" };
    } else {
        return RespValue{ .simple_string = "OK" };
    }
}
