const std = @import("std");
const Allocator = std.mem.Allocator;
const Storage = @import("../storage/memory.zig").Storage;
const Value = @import("../storage/memory.zig").Value;
const CountMinSketchValue = @import("../storage/cms.zig").CountMinSketchValue;
const RespValue = @import("../protocol/writer.zig").RespValue;

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
    args: []const []const u8,
) !RespValue {
    if (args.len != 4) return RespValue{ .err = "ERR wrong number of arguments for 'CMS.INITBYDIM' command" };

    const key = args[1];
    const width_str = args[2];
    const depth_str = args[3];

    // Parse width
    const width = std.fmt.parseInt(u32, width_str, 10) catch {
        return RespValue{ .err = "ERR invalid width: must be a positive integer" };
    };

    // Parse depth
    const depth = std.fmt.parseInt(u32, depth_str, 10) catch {
        return RespValue{ .err = "ERR invalid depth: must be a positive integer" };
    };

    // Validate dimensions
    if (width == 0) return RespValue{ .err = "ERR invalid width: must be greater than 0" };
    if (depth == 0) return RespValue{ .err = "ERR invalid depth: must be greater than 0" };

    // Create Count-Min Sketch
    var cms = CountMinSketchValue.initByDim(storage.allocator, width, depth) catch |err| {
        return switch (err) {
            error.InvalidDimensions => RespValue{ .err = "ERR invalid dimensions" },
            error.OutOfMemory => error.OutOfMemory,
            else => error.OutOfMemory,
        };
    };
    errdefer cms.deinit();

    // Check if key exists
    if (storage.get(key)) |existing_value| {
        // Key exists - must be CMS type
        if (existing_value.* != .count_min_sketch) {
            cms.deinit();
            return RespValue{ .err = "WRONGTYPE Operation against a key holding the wrong kind of value" };
        }

        // Replace existing CMS
        existing_value.count_min_sketch.deinit();
        existing_value.* = Value{ .count_min_sketch = cms };
    } else {
        // Create new key
        try storage.set(key, Value{ .count_min_sketch = cms });
    }

    return RespValue{ .simple = "OK" };
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
    args: []const []const u8,
) !RespValue {
    if (args.len != 4) return RespValue{ .err = "ERR wrong number of arguments for 'CMS.INITBYPROB' command" };

    const key = args[1];
    const error_rate_str = args[2];
    const probability_str = args[3];

    // Parse error_rate
    const error_rate = std.fmt.parseFloat(f64, error_rate_str) catch {
        return RespValue{ .err = "ERR invalid error_rate: must be a float between 0 and 1" };
    };

    // Parse probability
    const probability = std.fmt.parseFloat(f64, probability_str) catch {
        return RespValue{ .err = "ERR invalid probability: must be a float between 0 and 1" };
    };

    // Validate parameters
    if (error_rate <= 0.0 or error_rate >= 1.0) {
        return RespValue{ .err = "ERR invalid error_rate: must be between 0 and 1 (exclusive)" };
    }
    if (probability <= 0.0 or probability >= 1.0) {
        return RespValue{ .err = "ERR invalid probability: must be between 0 and 1 (exclusive)" };
    }

    // Create Count-Min Sketch
    var cms = CountMinSketchValue.initByProb(storage.allocator, error_rate, probability) catch |err| {
        return switch (err) {
            error.InvalidErrorRate => RespValue{ .err = "ERR invalid error_rate: must be between 0 and 1 (exclusive)" },
            error.InvalidProbability => RespValue{ .err = "ERR invalid probability: must be between 0 and 1 (exclusive)" },
            error.DimensionTooLarge => RespValue{ .err = "ERR calculated dimensions exceed maximum allowed size" },
            error.OutOfMemory => error.OutOfMemory,
            else => error.OutOfMemory,
        };
    };
    errdefer cms.deinit();

    // Check if key exists
    if (storage.get(key)) |existing_value| {
        // Key exists - must be CMS type
        if (existing_value.* != .count_min_sketch) {
            cms.deinit();
            return RespValue{ .err = "WRONGTYPE Operation against a key holding the wrong kind of value" };
        }

        // Replace existing CMS
        existing_value.count_min_sketch.deinit();
        existing_value.* = Value{ .count_min_sketch = cms };
    } else {
        // Create new key
        try storage.set(key, Value{ .count_min_sketch = cms });
    }

    return RespValue{ .simple = "OK" };
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

    try std.testing.expect(std.mem.startsWith(u8, result.err, "WRONGTYPE"));
}

test "cmdCmsInitByDim: rejects zero width" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const args = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "0", "5" };
    const result = try cmdCmsInitByDim(allocator, &storage, &args);

    try std.testing.expect(std.mem.indexOf(u8, result.err, "width") != null);
}

test "cmdCmsInitByDim: rejects zero depth" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const args = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "100", "0" };
    const result = try cmdCmsInitByDim(allocator, &storage, &args);

    try std.testing.expect(std.mem.indexOf(u8, result.err, "depth") != null);
}

test "cmdCmsInitByDim: rejects invalid width format" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const args = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "abc", "5" };
    const result = try cmdCmsInitByDim(allocator, &storage, &args);

    try std.testing.expect(std.mem.indexOf(u8, result.err, "width") != null);
}

test "cmdCmsInitByDim: rejects wrong arity" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const args1 = [_][]const u8{ "CMS.INITBYDIM", "mysketch" };
    const result1 = try cmdCmsInitByDim(allocator, &storage, &args1);
    try std.testing.expect(std.mem.indexOf(u8, result1.err, "wrong number") != null);

    const args2 = [_][]const u8{ "CMS.INITBYDIM", "mysketch", "100", "5", "extra" };
    const result2 = try cmdCmsInitByDim(allocator, &storage, &args2);
    try std.testing.expect(std.mem.indexOf(u8, result2.err, "wrong number") != null);
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
    try std.testing.expect(std.mem.indexOf(u8, result1.err, "error_rate") != null);

    const args2 = [_][]const u8{ "CMS.INITBYPROB", "mysketch", "1.0", "0.01" };
    const result2 = try cmdCmsInitByProb(allocator, &storage, &args2);
    try std.testing.expect(std.mem.indexOf(u8, result2.err, "error_rate") != null);

    const args3 = [_][]const u8{ "CMS.INITBYPROB", "mysketch", "-0.1", "0.01" };
    const result3 = try cmdCmsInitByProb(allocator, &storage, &args3);
    try std.testing.expect(std.mem.indexOf(u8, result3.err, "error_rate") != null);
}

test "cmdCmsInitByProb: rejects invalid probability" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const args1 = [_][]const u8{ "CMS.INITBYPROB", "mysketch", "0.01", "0.0" };
    const result1 = try cmdCmsInitByProb(allocator, &storage, &args1);
    try std.testing.expect(std.mem.indexOf(u8, result1.err, "probability") != null);

    const args2 = [_][]const u8{ "CMS.INITBYPROB", "mysketch", "0.01", "1.0" };
    const result2 = try cmdCmsInitByProb(allocator, &storage, &args2);
    try std.testing.expect(std.mem.indexOf(u8, result2.err, "probability") != null);
}

test "cmdCmsInitByProb: rejects invalid float format" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, null, null);
    defer storage.deinit();

    const args = [_][]const u8{ "CMS.INITBYPROB", "mysketch", "abc", "0.01" };
    const result = try cmdCmsInitByProb(allocator, &storage, &args);

    try std.testing.expect(std.mem.indexOf(u8, result.err, "error_rate") != null);
}
