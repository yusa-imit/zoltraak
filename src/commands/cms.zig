const std = @import("std");
const Allocator = std.mem.Allocator;
const protocol = @import("../protocol/parser.zig");
const Storage = @import("../storage/memory.zig").Storage;
const Value = @import("../storage/memory.zig").Value;
const CountMinSketchValue = @import("../storage/cms.zig").CountMinSketchValue;

const RespValue = protocol.RespValue;

/// CMS.INITBYDIM key width depth
/// Initialize Count-Min Sketch with explicit dimensions.
///
/// Returns: +OK\r\n
/// Errors:
/// - WRONGTYPE if key exists and is not CMS
/// - ERR invalid width/depth (must be positive integers)
pub fn cmdCmsInitByDim(
    _: Allocator,
    storage: *Storage,
    args: []const RespValue,
) !RespValue {
    if (args.len != 3) return RespValue{ .error_string = "ERR wrong number of arguments for 'CMS.INITBYDIM' command" };

    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };
    const width_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid width" },
    };
    const depth_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid depth" },
    };

    // Parse width
    const width = std.fmt.parseInt(u32, width_str, 10) catch {
        return RespValue{ .error_string = "ERR invalid width: must be a positive integer" };
    };

    // Parse depth
    const depth = std.fmt.parseInt(u32, depth_str, 10) catch {
        return RespValue{ .error_string = "ERR invalid depth: must be a positive integer" };
    };

    // Validate dimensions
    if (width == 0) return RespValue{ .error_string = "ERR invalid width: must be greater than 0" };
    if (depth == 0) return RespValue{ .error_string = "ERR invalid depth: must be greater than 0" };

    // Create Count-Min Sketch
    var cms = CountMinSketchValue.initByDim(storage.allocator, width, depth) catch |err| {
        return switch (err) {
            error.InvalidDimensions => RespValue{ .error_string = "ERR invalid dimensions" },
            error.OutOfMemory => error.OutOfMemory,
        };
    };
    errdefer cms.deinit();

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Check if key exists
    if (storage.data.getPtr(key)) |entry| {
        // Key exists - must be CMS type
        if (entry.* != .count_min_sketch) {
            cms.deinit();
            return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
        }

        // Replace existing CMS
        entry.count_min_sketch.deinit();
        entry.* = Value{ .count_min_sketch = cms };
    } else {
        // Create new key
        const key_copy = try storage.allocator.dupe(u8, key);
        errdefer storage.allocator.free(key_copy);
        try storage.data.put(key_copy, Value{ .count_min_sketch = cms });
    }

    return RespValue{ .simple_string = "OK" };
}

/// CMS.INCRBY key item increment [item increment ...]
/// Increment counters for one or more items.
///
/// Returns: Array of integers (new counts after increment)
/// Errors:
/// - ERR key not found
/// - WRONGTYPE if key is not CMS
/// - ERR invalid increment format (must be i64)
/// - ERR item-increment pairs required
/// - ERR counter overflow/underflow
pub fn cmdCmsIncrBy(
    allocator: Allocator,
    storage: *Storage,
    args: []const RespValue,
) !RespValue {
    // Validate arity: key + at least one item-increment pair
    // args = [key, item1, incr1, item2, incr2, ...] so args.len must be odd (1 + 2n)
    if (args.len < 3 or (args.len - 1) % 2 != 0) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'CMS.INCRBY' command (expected key item increment pairs)" };
    }

    // Parse key
    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Look up key
    const entry = storage.data.getEntry(key) orelse {
        return RespValue{ .error_string = "ERR key not found" };
    };

    // Verify it's a CMS
    if (entry.value_ptr.* != .count_min_sketch) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    var cms = &entry.value_ptr.count_min_sketch;

    // Calculate number of items
    const num_items = (args.len - 1) / 2;

    // Allocate result array
    const results = try allocator.alloc(RespValue, num_items);
    errdefer allocator.free(results);

    // Process each item-increment pair
    var result_idx: usize = 0;
    var arg_idx: usize = 1;

    while (arg_idx < args.len) : (arg_idx += 2) {
        // Parse item
        const item = switch (args[arg_idx]) {
            .bulk_string => |s| s,
            else => {
                allocator.free(results);
                return RespValue{ .error_string = "ERR invalid item" };
            },
        };

        // Parse increment
        const incr_str = switch (args[arg_idx + 1]) {
            .bulk_string => |s| s,
            else => {
                allocator.free(results);
                return RespValue{ .error_string = "ERR invalid increment format" };
            },
        };

        const increment = std.fmt.parseInt(i64, incr_str, 10) catch {
            allocator.free(results);
            return RespValue{ .error_string = "ERR invalid increment: must be a valid integer" };
        };

        // Increment and store result
        const count = cms.incrBy(item, increment) catch |err| {
            allocator.free(results);
            return switch (err) {
                error.CounterOverflow => RespValue{ .error_string = "ERR counter overflow" },
                error.CounterUnderflow => RespValue{ .error_string = "ERR counter underflow" },
            };
        };

        results[result_idx] = RespValue{ .integer = @intCast(@min(count, std.math.maxInt(i64))) };
        result_idx += 1;
    }

    return RespValue{ .array = results };
}

/// CMS.QUERY key item [item ...]
/// Query estimated counts for one or more items.
///
/// Returns: Array of integers (estimated counts)
/// Errors:
/// - ERR key not found
/// - WRONGTYPE if key is not CMS
/// - ERR wrong number of arguments (requires at least one item)
pub fn cmdCmsQuery(
    allocator: Allocator,
    storage: *Storage,
    args: []const RespValue,
) !RespValue {
    // Validate arity: key + at least one item
    if (args.len < 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'CMS.QUERY' command" };
    }

    // Parse key
    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Look up key
    const entry = storage.data.getEntry(key) orelse {
        return RespValue{ .error_string = "ERR key not found" };
    };

    // Verify it's a CMS
    if (entry.value_ptr.* != .count_min_sketch) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    const cms = &entry.value_ptr.count_min_sketch;

    // Calculate number of items to query
    const num_items = args.len - 1;

    // Allocate result array
    const results = try allocator.alloc(RespValue, num_items);
    errdefer allocator.free(results);

    // Query each item
    for (args[1..], 0..) |arg, i| {
        const item = switch (arg) {
            .bulk_string => |s| s,
            else => {
                allocator.free(results);
                return RespValue{ .error_string = "ERR invalid item" };
            },
        };

        const count = cms.query(item);
        results[i] = RespValue{ .integer = @intCast(@min(count, std.math.maxInt(i64))) };
    }

    return RespValue{ .array = results };
}

/// CMS.INITBYPROB key error_rate probability
/// Initialize Count-Min Sketch with error bounds.
///
/// Returns: +OK\r\n
/// Errors:
/// - WRONGTYPE if key exists and is not CMS
/// - ERR invalid error_rate (must be 0 < ε < 1)
/// - ERR invalid probability (must be 0 < δ < 1)
pub fn cmdCmsInitByProb(
    _: Allocator,
    storage: *Storage,
    args: []const RespValue,
) !RespValue {
    if (args.len != 3) return RespValue{ .error_string = "ERR wrong number of arguments for 'CMS.INITBYPROB' command" };

    const key = switch (args[0]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };
    const error_rate_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid error_rate" },
    };
    const probability_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid probability" },
    };

    // Parse error_rate
    const error_rate = std.fmt.parseFloat(f64, error_rate_str) catch {
        return RespValue{ .error_string = "ERR invalid error_rate: must be a float between 0 and 1" };
    };

    // Parse probability
    const probability = std.fmt.parseFloat(f64, probability_str) catch {
        return RespValue{ .error_string = "ERR invalid probability: must be a float between 0 and 1" };
    };

    // Validate parameters
    if (error_rate <= 0.0 or error_rate >= 1.0) {
        return RespValue{ .error_string = "ERR invalid error_rate: must be between 0 and 1 (exclusive)" };
    }
    if (probability <= 0.0 or probability >= 1.0) {
        return RespValue{ .error_string = "ERR invalid probability: must be between 0 and 1 (exclusive)" };
    }

    // Create Count-Min Sketch
    var cms = CountMinSketchValue.initByProb(storage.allocator, error_rate, probability) catch |err| {
        return switch (err) {
            error.InvalidErrorRate => RespValue{ .error_string = "ERR invalid error_rate: must be between 0 and 1 (exclusive)" },
            error.InvalidProbability => RespValue{ .error_string = "ERR invalid probability: must be between 0 and 1 (exclusive)" },
            error.DimensionTooLarge => RespValue{ .error_string = "ERR calculated dimensions exceed maximum allowed size" },
            error.InvalidDimensions => RespValue{ .error_string = "ERR invalid dimensions" },
            error.OutOfMemory => error.OutOfMemory,
        };
    };
    errdefer cms.deinit();

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Check if key exists
    if (storage.data.getPtr(key)) |entry| {
        // Key exists - must be CMS type
        if (entry.* != .count_min_sketch) {
            cms.deinit();
            return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
        }

        // Replace existing CMS
        entry.count_min_sketch.deinit();
        entry.* = Value{ .count_min_sketch = cms };
    } else {
        // Create new key
        const key_copy = try storage.allocator.dupe(u8, key);
        errdefer storage.allocator.free(key_copy);
        try storage.data.put(key_copy, Value{ .count_min_sketch = cms });
    }

    return RespValue{ .simple_string = "OK" };
}

// ============================================================================
// Unit Tests
// ============================================================================

test "cmdCmsInitByDim: creates new CMS with valid dimensions" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const args = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "100", "5" };
    const result = try cmdCmsInitByDim(allocator, &storage, &args);

    try std.testing.expectEqualStrings("OK", result.simple);

    // Verify CMS was created
    const value = storage.get("mysketch").?;
    try std.testing.expect(value.* == .count_min_sketch);
    try std.testing.expectEqual(@as(u32, 100), value.count_min_sketch.width);
    try std.testing.expectEqual(@as(u32, 5), value.count_min_sketch.depth);
}

test "cmdCmsInitByDim: replaces existing CMS" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    // Create initial CMS
    const args1 = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "50", "3" };
    _ = try cmdCmsInitByDim(allocator, &storage, &args1);

    // Replace with new dimensions
    const args2 = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "200", "7" };
    const result = try cmdCmsInitByDim(allocator, &storage, &args2);

    try std.testing.expectEqualStrings("OK", result.simple);

    const value = storage.get("mysketch").?;
    try std.testing.expectEqual(@as(u32, 200), value.count_min_sketch.width);
    try std.testing.expectEqual(@as(u32, 7), value.count_min_sketch.depth);
}

test "cmdCmsInitByDim: returns WRONGTYPE if key is not CMS" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    // Create a string value
    try storage.set("mykey", Value{ .string = try allocator.dupe(u8, "hello") });

    const args = [_][]const u8{ "CMS.INITBYDIM", "mykey", "100", "5" };
    const result = try cmdCmsInitByDim(allocator, &storage, &args);

    try std.testing.expect(std.mem.startsWith(u8, result.error_string, "WRONGTYPE"));
}

test "cmdCmsInitByDim: rejects zero width" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const args = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "0", "5" };
    const result = try cmdCmsInitByDim(allocator, &storage, &args);

    try std.testing.expect(std.mem.indexOf(u8, result.error_string, "width") != null);
}

test "cmdCmsInitByDim: rejects zero depth" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const args = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "100", "0" };
    const result = try cmdCmsInitByDim(allocator, &storage, &args);

    try std.testing.expect(std.mem.indexOf(u8, result.error_string, "depth") != null);
}

test "cmdCmsInitByDim: rejects invalid width format" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const args = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "abc", "5" };
    const result = try cmdCmsInitByDim(allocator, &storage, &args);

    try std.testing.expect(std.mem.indexOf(u8, result.error_string, "width") != null);
}

test "cmdCmsInitByDim: rejects wrong arity" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const args1 = [_][]const u8{ "CMS.INITBYDIM", "mysketch" };
    const result1 = try cmdCmsInitByDim(allocator, &storage, &args1);
    try std.testing.expect(std.mem.indexOf(u8, result1.error_string, "wrong number") != null);

    const args2 = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "100", "5", "extra" };
    const result2 = try cmdCmsInitByDim(allocator, &storage, &args2);
    try std.testing.expect(std.mem.indexOf(u8, result2.error_string, "wrong number") != null);
}

test "cmdCmsInitByProb: creates CMS with error bounds" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const args = [_][]const u8{ "CMS.INITBYPROB", "mysketch", "0.01", "0.01" };
    const result = try cmdCmsInitByProb(allocator, &storage, &args);

    try std.testing.expectEqualStrings("OK", result.simple);

    const value = storage.get("mysketch").?;
    try std.testing.expect(value.* == .count_min_sketch);

    // width = ceil(e / 0.01) = 272
    try std.testing.expectEqual(@as(u32, 272), value.count_min_sketch.width);

    // depth = ceil(ln(100)) = 5
    try std.testing.expectEqual(@as(u32, 5), value.count_min_sketch.depth);
}

test "cmdCmsInitByProb: rejects invalid error_rate" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const args1 = [_][]const u8{ "CMS.INITBYPROB", "mysketch", "0.0", "0.01" };
    const result1 = try cmdCmsInitByProb(allocator, &storage, &args1);
    try std.testing.expect(std.mem.indexOf(u8, result1.error_string, "error_rate") != null);

    const args2 = [_][]const u8{ "CMS.INITBYPROB", "mysketch", "1.0", "0.01" };
    const result2 = try cmdCmsInitByProb(allocator, &storage, &args2);
    try std.testing.expect(std.mem.indexOf(u8, result2.error_string, "error_rate") != null);

    const args3 = [_][]const u8{ "CMS.INITBYPROB", "mysketch", "-0.1", "0.01" };
    const result3 = try cmdCmsInitByProb(allocator, &storage, &args3);
    try std.testing.expect(std.mem.indexOf(u8, result3.error_string, "error_rate") != null);
}

test "cmdCmsInitByProb: rejects invalid probability" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const args1 = [_][]const u8{ "CMS.INITBYPROB", "mysketch", "0.01", "0.0" };
    const result1 = try cmdCmsInitByProb(allocator, &storage, &args1);
    try std.testing.expect(std.mem.indexOf(u8, result1.error_string, "probability") != null);

    const args2 = [_][]const u8{ "CMS.INITBYPROB", "mysketch", "0.01", "1.0" };
    const result2 = try cmdCmsInitByProb(allocator, &storage, &args2);
    try std.testing.expect(std.mem.indexOf(u8, result2.error_string, "probability") != null);
}

test "cmdCmsInitByProb: rejects invalid float format" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const args = [_][]const u8{ "CMS.INITBYPROB", "mysketch", "abc", "0.01" };
    const result = try cmdCmsInitByProb(allocator, &storage, &args);

    try std.testing.expect(std.mem.indexOf(u8, result.error_string, "error_rate") != null);
}

test "cmdCmsIncrBy: increments single item" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    // Create CMS
    const init_args = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "100", "5" };
    _ = try cmdCmsInitByDim(allocator, &storage, &init_args);

    // Increment item
    const args = [_][]const u8{ "CMS.INCRBY", "mysketch", "apple", "5" };
    const result = try cmdCmsIncrBy(allocator, &storage, &args);

    // Should return array with single integer [5]
    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 1), result.array.items.len);
    try std.testing.expectEqual(@as(i64, 5), result.array.items[0].integer);
}

test "cmdCmsIncrBy: increments multiple items" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const init_args = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "100", "5" };
    _ = try cmdCmsInitByDim(allocator, &storage, &init_args);

    // Increment multiple items
    const args = [_][]const u8{ "CMS.INCRBY", "mysketch", "apple", "10", "banana", "20", "cherry", "30" };
    const result = try cmdCmsIncrBy(allocator, &storage, &args);

    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 3), result.array.items.len);
    try std.testing.expectEqual(@as(i64, 10), result.array.items[0].integer);
    try std.testing.expectEqual(@as(i64, 20), result.array.items[1].integer);
    try std.testing.expectEqual(@as(i64, 30), result.array.items[2].integer);
}

test "cmdCmsIncrBy: accumulates increments" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const init_args = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "100", "5" };
    _ = try cmdCmsInitByDim(allocator, &storage, &init_args);

    // First increment
    const args1 = [_][]const u8{ "CMS.INCRBY", "mysketch", "apple", "5" };
    _ = try cmdCmsIncrBy(allocator, &storage, &args1);

    // Second increment
    const args2 = [_][]const u8{ "CMS.INCRBY", "mysketch", "apple", "3" };
    const result = try cmdCmsIncrBy(allocator, &storage, &args2);

    // Should accumulate: 5 + 3 = 8
    try std.testing.expectEqual(@as(i64, 8), result.array.items[0].integer);
}

test "cmdCmsIncrBy: handles negative increments" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const init_args = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "100", "5" };
    _ = try cmdCmsInitByDim(allocator, &storage, &init_args);

    // Increment then decrement
    const args1 = [_][]const u8{ "CMS.INCRBY", "mysketch", "apple", "10" };
    _ = try cmdCmsIncrBy(allocator, &storage, &args1);

    const args2 = [_][]const u8{ "CMS.INCRBY", "mysketch", "apple", "-3" };
    const result = try cmdCmsIncrBy(allocator, &storage, &args2);

    // Should be: 10 - 3 = 7
    try std.testing.expectEqual(@as(i64, 7), result.array.items[0].integer);
}

test "cmdCmsIncrBy: returns error for nonexistent key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const args = [_][]const u8{ "CMS.INCRBY", "nonexistent", "apple", "5" };
    const result = try cmdCmsIncrBy(allocator, &storage, &args);

    try std.testing.expect(std.mem.indexOf(u8, result.error_string, "not found") != null);
}

test "cmdCmsIncrBy: returns WRONGTYPE for non-CMS key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    try storage.set("mykey", Value{ .string = try allocator.dupe(u8, "hello") });

    const args = [_][]const u8{ "CMS.INCRBY", "mykey", "apple", "5" };
    const result = try cmdCmsIncrBy(allocator, &storage, &args);

    try std.testing.expect(std.mem.startsWith(u8, result.error_string, "WRONGTYPE"));
}

test "cmdCmsIncrBy: rejects invalid increment format" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const init_args = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "100", "5" };
    _ = try cmdCmsInitByDim(allocator, &storage, &init_args);

    const args = [_][]const u8{ "CMS.INCRBY", "mysketch", "apple", "abc" };
    const result = try cmdCmsIncrBy(allocator, &storage, &args);

    try std.testing.expect(std.mem.indexOf(u8, result.error_string, "increment") != null);
}

test "cmdCmsIncrBy: rejects odd number of item-increment pairs" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const init_args = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "100", "5" };
    _ = try cmdCmsInitByDim(allocator, &storage, &init_args);

    // Missing increment for second item
    const args = [_][]const u8{ "CMS.INCRBY", "mysketch", "apple", "5", "banana" };
    const result = try cmdCmsIncrBy(allocator, &storage, &args);

    try std.testing.expect(std.mem.indexOf(u8, result.error_string, "pairs") != null);
}

test "cmdCmsIncrBy: detects overflow" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const init_args = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "100", "5" };
    _ = try cmdCmsInitByDim(allocator, &storage, &init_args);

    // Set to near-max
    var buf: [32]u8 = undefined;
    const max_str = try std.fmt.bufPrint(&buf, "{d}", .{std.math.maxInt(i64)});
    const args1 = [_][]const u8{ "CMS.INCRBY", "mysketch", "apple", max_str };
    _ = try cmdCmsIncrBy(allocator, &storage, &args1);

    // Try to increment again
    const args2 = [_][]const u8{ "CMS.INCRBY", "mysketch", "apple", "1" };
    const result = try cmdCmsIncrBy(allocator, &storage, &args2);

    try std.testing.expect(std.mem.indexOf(u8, result.error_string, "overflow") != null);
}

test "cmdCmsQuery: queries single item" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const init_args = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "100", "5" };
    _ = try cmdCmsInitByDim(allocator, &storage, &init_args);

    const incr_args = [_][]const u8{ "CMS.INCRBY", "mysketch", "apple", "42" };
    _ = try cmdCmsIncrBy(allocator, &storage, &incr_args);

    const args = [_][]const u8{ "CMS.QUERY", "mysketch", "apple" };
    const result = try cmdCmsQuery(allocator, &storage, &args);

    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 1), result.array.items.len);
    try std.testing.expectEqual(@as(i64, 42), result.array.items[0].integer);
}

test "cmdCmsQuery: queries multiple items" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const init_args = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "1000", "7" };
    _ = try cmdCmsInitByDim(allocator, &storage, &init_args);

    const incr_args = [_][]const u8{ "CMS.INCRBY", "mysketch", "apple", "10", "banana", "20", "cherry", "30" };
    _ = try cmdCmsIncrBy(allocator, &storage, &incr_args);

    const args = [_][]const u8{ "CMS.QUERY", "mysketch", "apple", "banana", "cherry" };
    const result = try cmdCmsQuery(allocator, &storage, &args);

    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 3), result.array.items.len);
    // With large sketch, should be exact or very close
    try std.testing.expect(result.array.items[0].integer >= 10);
    try std.testing.expect(result.array.items[1].integer >= 20);
    try std.testing.expect(result.array.items[2].integer >= 30);
}

test "cmdCmsQuery: returns zero for nonexistent items" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const init_args = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "100", "5" };
    _ = try cmdCmsInitByDim(allocator, &storage, &init_args);

    const args = [_][]const u8{ "CMS.QUERY", "mysketch", "nonexistent" };
    const result = try cmdCmsQuery(allocator, &storage, &args);

    try std.testing.expectEqual(@as(i64, 0), result.array.items[0].integer);
}

test "cmdCmsQuery: returns error for nonexistent key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const args = [_][]const u8{ "CMS.QUERY", "nonexistent", "apple" };
    const result = try cmdCmsQuery(allocator, &storage, &args);

    try std.testing.expect(std.mem.indexOf(u8, result.error_string, "not found") != null);
}

test "cmdCmsQuery: returns WRONGTYPE for non-CMS key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    try storage.set("mykey", Value{ .string = try allocator.dupe(u8, "hello") });

    const args = [_][]const u8{ "CMS.QUERY", "mykey", "apple" };
    const result = try cmdCmsQuery(allocator, &storage, &args);

    try std.testing.expect(std.mem.startsWith(u8, result.error_string, "WRONGTYPE"));
}

test "cmdCmsQuery: requires at least one item" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const init_args = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "100", "5" };
    _ = try cmdCmsInitByDim(allocator, &storage, &init_args);

    const args = [_][]const u8{ "CMS.QUERY", "mysketch" };
    const result = try cmdCmsQuery(allocator, &storage, &args);

    try std.testing.expect(std.mem.indexOf(u8, result.error_string, "wrong number") != null);
}
