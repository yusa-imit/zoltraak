const std = @import("std");
const Allocator = std.mem.Allocator;

/// Count-Min Sketch probabilistic data structure for frequency estimation.
/// Uses multiple hash functions with pairwise-independent hash family.
pub const CountMinSketchValue = struct {
    width: u32, // Number of counters per hash function
    depth: u32, // Number of hash functions
    counters: [][]u64, // 2D array: counters[depth][width]
    allocator: Allocator,

    /// Initialize Count-Min Sketch with explicit dimensions.
    /// - width: Number of counters per hash function (must be > 0)
    /// - depth: Number of hash functions (must be > 0)
    pub fn initByDim(allocator: Allocator, width: u32, depth: u32) !CountMinSketchValue {
        if (width == 0 or depth == 0) return error.InvalidDimensions;

        // Allocate 2D array: first allocate array of row pointers
        const counters = try allocator.alloc([]u64, depth);
        errdefer allocator.free(counters);

        // Allocate each row and zero-initialize
        var i: u32 = 0;
        errdefer {
            // Cleanup already-allocated rows on error
            var j: u32 = 0;
            while (j < i) : (j += 1) {
                allocator.free(counters[j]);
            }
            allocator.free(counters);
        }

        while (i < depth) : (i += 1) {
            const row = try allocator.alloc(u64, width);
            @memset(row, 0);
            counters[i] = row;
        }

        return CountMinSketchValue{
            .width = width,
            .depth = depth,
            .counters = counters,
            .allocator = allocator,
        };
    }

    /// Initialize Count-Min Sketch with error bounds.
    /// - error_rate: Epsilon (ε), maximum error as fraction (0 < ε < 1)
    /// - probability: Delta (δ), probability of exceeding error (0 < δ < 1)
    ///
    /// Calculates optimal dimensions:
    /// - width = ceil(e / ε) where e ≈ 2.71828
    /// - depth = ceil(ln(1 / δ))
    pub fn initByProb(allocator: Allocator, error_rate: f64, probability: f64) !CountMinSketchValue {
        if (error_rate <= 0.0 or error_rate >= 1.0) return error.InvalidErrorRate;
        if (probability <= 0.0 or probability >= 1.0) return error.InvalidProbability;

        // Calculate optimal dimensions
        const e: f64 = @exp(1.0); // Euler's number ≈ 2.71828
        const width_f = @ceil(e / error_rate);
        const depth_f = @ceil(@log(1.0 / probability));

        // Convert to u32, handle overflow
        if (width_f > @as(f64, @floatFromInt(std.math.maxInt(u32)))) return error.DimensionTooLarge;
        if (depth_f > @as(f64, @floatFromInt(std.math.maxInt(u32)))) return error.DimensionTooLarge;

        const width = @as(u32, @intFromFloat(width_f));
        const depth = @as(u32, @intFromFloat(depth_f));

        return initByDim(allocator, width, depth);
    }

    /// Free all allocated memory.
    pub fn deinit(self: *CountMinSketchValue) void {
        for (self.counters) |row| {
            self.allocator.free(row);
        }
        self.allocator.free(self.counters);
        self.* = undefined;
    }

    /// Clone the Count-Min Sketch (deep copy).
    pub fn clone(self: *const CountMinSketchValue, allocator: Allocator) !CountMinSketchValue {
        var new_cms = try initByDim(allocator, self.width, self.depth);
        errdefer new_cms.deinit();

        // Copy all counter values
        for (self.counters, 0..) |src_row, i| {
            @memcpy(new_cms.counters[i], src_row);
        }

        return new_cms;
    }

    /// Compute hash for item at given hash function index.
    /// Uses MurmurHash3-inspired pairwise-independent hash family:
    /// h_i(x) = (h1(x) + i * h2(x)) mod width
    fn hash(self: *const CountMinSketchValue, item: []const u8, hash_index: u32) u32 {
        // Base hash (h1)
        const h1 = std.hash.Murmur3_32.hash(item);

        // Secondary hash (h2) - use different seed
        const seed: u32 = 0x9747b28c;
        const h2 = std.hash.Murmur3_32.hashWithSeed(item, seed);

        // Combine: (h1 + i * h2) mod width
        const combined = h1 +% (hash_index *% h2);
        return combined % self.width;
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "CountMinSketch: initByDim creates sketch with correct dimensions" {
    const allocator = std.testing.allocator;

    var cms = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer cms.deinit();

    try std.testing.expectEqual(@as(u32, 100), cms.width);
    try std.testing.expectEqual(@as(u32, 5), cms.depth);
    try std.testing.expectEqual(@as(usize, 5), cms.counters.len);
    try std.testing.expectEqual(@as(usize, 100), cms.counters[0].len);

    // Verify all counters are zero-initialized
    for (cms.counters) |row| {
        for (row) |counter| {
            try std.testing.expectEqual(@as(u64, 0), counter);
        }
    }
}

test "CountMinSketch: initByDim rejects zero dimensions" {
    const allocator = std.testing.allocator;

    try std.testing.expectError(error.InvalidDimensions, CountMinSketchValue.initByDim(allocator, 0, 5));
    try std.testing.expectError(error.InvalidDimensions, CountMinSketchValue.initByDim(allocator, 100, 0));
    try std.testing.expectError(error.InvalidDimensions, CountMinSketchValue.initByDim(allocator, 0, 0));
}

test "CountMinSketch: initByProb calculates correct dimensions" {
    const allocator = std.testing.allocator;

    // Error rate 0.01, probability 0.01 → width ≈ 272, depth ≈ 5
    var cms = try CountMinSketchValue.initByProb(allocator, 0.01, 0.01);
    defer cms.deinit();

    // width = ceil(e / 0.01) = ceil(271.828) = 272
    try std.testing.expectEqual(@as(u32, 272), cms.width);

    // depth = ceil(ln(100)) = ceil(4.605) = 5
    try std.testing.expectEqual(@as(u32, 5), cms.depth);
}

test "CountMinSketch: initByProb rejects invalid parameters" {
    const allocator = std.testing.allocator;

    // Invalid error rates
    try std.testing.expectError(error.InvalidErrorRate, CountMinSketchValue.initByProb(allocator, 0.0, 0.01));
    try std.testing.expectError(error.InvalidErrorRate, CountMinSketchValue.initByProb(allocator, 1.0, 0.01));
    try std.testing.expectError(error.InvalidErrorRate, CountMinSketchValue.initByProb(allocator, -0.1, 0.01));

    // Invalid probabilities
    try std.testing.expectError(error.InvalidProbability, CountMinSketchValue.initByProb(allocator, 0.01, 0.0));
    try std.testing.expectError(error.InvalidProbability, CountMinSketchValue.initByProb(allocator, 0.01, 1.0));
    try std.testing.expectError(error.InvalidProbability, CountMinSketchValue.initByProb(allocator, 0.01, -0.1));
}

test "CountMinSketch: clone creates independent copy" {
    const allocator = std.testing.allocator;

    var cms1 = try CountMinSketchValue.initByDim(allocator, 10, 3);
    defer cms1.deinit();

    // Modify some counters
    cms1.counters[0][5] = 42;
    cms1.counters[2][7] = 99;

    var cms2 = try cms1.clone(allocator);
    defer cms2.deinit();

    // Verify dimensions match
    try std.testing.expectEqual(cms1.width, cms2.width);
    try std.testing.expectEqual(cms1.depth, cms2.depth);

    // Verify counter values match
    try std.testing.expectEqual(@as(u64, 42), cms2.counters[0][5]);
    try std.testing.expectEqual(@as(u64, 99), cms2.counters[2][7]);

    // Modify clone, verify original unchanged
    cms2.counters[0][5] = 123;
    try std.testing.expectEqual(@as(u64, 42), cms1.counters[0][5]);
    try std.testing.expectEqual(@as(u64, 123), cms2.counters[0][5]);
}

test "CountMinSketch: hash produces consistent values" {
    const allocator = std.testing.allocator;

    var cms = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer cms.deinit();

    const item = "test_item";

    // Hash should be deterministic
    const h0_1 = cms.hash(item, 0);
    const h0_2 = cms.hash(item, 0);
    try std.testing.expectEqual(h0_1, h0_2);

    // Different hash indices should produce different values (usually)
    const h1 = cms.hash(item, 1);
    const h2 = cms.hash(item, 2);

    // Very unlikely all would collide (width=100)
    const all_same = (h0_1 == h1 and h1 == h2);
    try std.testing.expect(!all_same);

    // All hashes must be within bounds [0, width)
    for (0..5) |i| {
        const h = cms.hash(item, @as(u32, @intCast(i)));
        try std.testing.expect(h < cms.width);
    }
}

test "CountMinSketch: hash produces uniform distribution" {
    const allocator = std.testing.allocator;

    var cms = try CountMinSketchValue.initByDim(allocator, 100, 1);
    defer cms.deinit();

    // Hash many items, count bucket distribution
    var buckets = [_]u32{0} ** 100;
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        var buf: [16]u8 = undefined;
        const item = try std.fmt.bufPrint(&buf, "item_{d}", .{i});
        const h = cms.hash(item, 0);
        buckets[h] += 1;
    }

    // Expected: ~10 items per bucket (1000 / 100)
    // Check that no bucket is completely empty (very unlikely)
    var empty_count: u32 = 0;
    for (buckets) |count| {
        if (count == 0) empty_count += 1;
    }

    // With uniform hash, expect < 5% empty buckets
    try std.testing.expect(empty_count < 5);
}
