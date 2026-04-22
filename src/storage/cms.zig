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

    /// Increment counter for an item by a delta value.
    /// Returns the new estimated count after increment.
    /// Errors:
    /// - error.CounterOverflow if increment would overflow u64
    /// - error.CounterUnderflow if negative increment would underflow below 0
    pub fn incrBy(self: *CountMinSketchValue, item: []const u8, delta: i64) !u64 {
        var min_count: u64 = std.math.maxInt(u64);

        // For each hash function, increment the counter at the hashed position
        var i: u32 = 0;
        while (i < self.depth) : (i += 1) {
            const pos = self.hash(item, i);
            const old_count = self.counters[i][pos];

            // Handle positive increments
            if (delta >= 0) {
                const unsigned_delta = @as(u64, @intCast(delta));
                const overflow_result = @addWithOverflow(old_count, unsigned_delta);
                if (overflow_result[1] != 0) {
                    return error.CounterOverflow;
                }
                self.counters[i][pos] = overflow_result[0];
            } else {
                // Handle negative increments (decrement)
                const unsigned_delta = @as(u64, @intCast(-delta));
                const underflow_result = @subWithOverflow(old_count, unsigned_delta);
                if (underflow_result[1] != 0) {
                    return error.CounterUnderflow;
                }
                self.counters[i][pos] = underflow_result[0];
            }

            // Track minimum count
            if (self.counters[i][pos] < min_count) {
                min_count = self.counters[i][pos];
            }
        }

        return min_count;
    }

    /// Query the estimated count for an item.
    /// Returns the minimum count across all hash functions (Count-Min Sketch property).
    /// Returns 0 for items that have never been incremented.
    pub fn query(self: *const CountMinSketchValue, item: []const u8) u64 {
        var min_count: u64 = std.math.maxInt(u64);

        // For each hash function, get the counter at the hashed position
        var i: u32 = 0;
        while (i < self.depth) : (i += 1) {
            const pos = self.hash(item, i);
            const count = self.counters[i][pos];

            // Track minimum count
            if (count < min_count) {
                min_count = count;
            }
        }

        // Return the minimum count (will be 0 for non-existent items in zero-initialized sketch)
        return min_count;
    }

    /// Merge multiple Count-Min Sketches into this sketch.
    /// All sketches must have identical dimensions (width and depth).
    /// Performs element-wise addition of all counters.
    /// Returns error.DimensionMismatch if any source sketch has different dimensions.
    /// Returns error.CounterOverflow if addition would overflow u64.
    pub fn merge(self: *CountMinSketchValue, sources: []const *const CountMinSketchValue) !void {
        // Validate all sources have matching dimensions
        for (sources) |source| {
            if (source.width != self.width or source.depth != self.depth) {
                return error.DimensionMismatch;
            }
        }

        // Element-wise addition of all counters
        for (0..self.depth) |i| {
            for (0..self.width) |j| {
                var sum = self.counters[i][j];

                // Add each source's counter value
                for (sources) |source| {
                    const overflow_result = @addWithOverflow(sum, source.counters[i][j]);
                    if (overflow_result[1] != 0) {
                        return error.CounterOverflow;
                    }
                    sum = overflow_result[0];
                }

                self.counters[i][j] = sum;
            }
        }
    }

    /// Get metadata information about this Count-Min Sketch.
    /// Returns a struct with width, depth, and total count.
    pub const SketchInfo = struct {
        width: u32,
        depth: u32,
        count: u64, // Total number of increments (sum of all counters)
    };

    pub fn getInfo(self: *const CountMinSketchValue) SketchInfo {
        var total_count: u64 = 0;

        // Sum all counters
        for (self.counters) |row| {
            for (row) |counter| {
                total_count +%= counter; // Wrapping add to prevent overflow
            }
        }

        return SketchInfo{
            .width = self.width,
            .depth = self.depth,
            .count = total_count,
        };
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

test "CountMinSketch: incrBy increments item count" {
    const allocator = std.testing.allocator;

    var cms = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer cms.deinit();

    // Increment "apple" by 3
    const count = try cms.incrBy("apple", 3);
    try std.testing.expectEqual(@as(u64, 3), count);

    // Query should return same count
    const query_count = cms.query("apple");
    try std.testing.expectEqual(@as(u64, 3), query_count);
}

test "CountMinSketch: incrBy accumulates multiple increments" {
    const allocator = std.testing.allocator;

    var cms = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer cms.deinit();

    // Multiple increments to same item
    _ = try cms.incrBy("apple", 5);
    _ = try cms.incrBy("apple", 3);
    const count = try cms.incrBy("apple", 2);

    // Should accumulate: 5 + 3 + 2 = 10
    try std.testing.expectEqual(@as(u64, 10), count);
}

test "CountMinSketch: incrBy handles negative increments" {
    const allocator = std.testing.allocator;

    var cms = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer cms.deinit();

    // Increment then decrement
    _ = try cms.incrBy("apple", 10);
    const count = try cms.incrBy("apple", -3);

    // Should be: 10 - 3 = 7
    try std.testing.expectEqual(@as(u64, 7), count);
}

test "CountMinSketch: incrBy detects overflow" {
    const allocator = std.testing.allocator;

    var cms = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer cms.deinit();

    // Set counter to near-max
    const item = "overflow_test";
    _ = try cms.incrBy(item, std.math.maxInt(i64));

    // Incrementing again should overflow
    try std.testing.expectError(error.CounterOverflow, cms.incrBy(item, 1));
}

test "CountMinSketch: incrBy detects underflow" {
    const allocator = std.testing.allocator;

    var cms = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer cms.deinit();

    // Decrementing from zero should underflow
    try std.testing.expectError(error.CounterUnderflow, cms.incrBy("apple", -1));
}

test "CountMinSketch: query returns zero for non-existent items" {
    const allocator = std.testing.allocator;

    var cms = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer cms.deinit();

    const count = cms.query("nonexistent");
    try std.testing.expectEqual(@as(u64, 0), count);
}

test "CountMinSketch: query returns minimum across all hash functions" {
    const allocator = std.testing.allocator;

    var cms = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer cms.deinit();

    // Add multiple items
    _ = try cms.incrBy("apple", 100);
    _ = try cms.incrBy("banana", 50);
    _ = try cms.incrBy("cherry", 25);

    // Query should return estimate (likely slightly higher due to collisions)
    const count = cms.query("apple");

    // Count should be >= actual (100) due to Count-Min Sketch properties
    try std.testing.expect(count >= 100);
}

test "CountMinSketch: multiple items maintain independent counts" {
    const allocator = std.testing.allocator;

    var cms = try CountMinSketchValue.initByDim(allocator, 1000, 7);
    defer cms.deinit();

    // Add different counts to different items
    _ = try cms.incrBy("apple", 10);
    _ = try cms.incrBy("banana", 20);
    _ = try cms.incrBy("cherry", 30);

    // Queries should return estimates close to actual
    const apple_count = cms.query("apple");
    const banana_count = cms.query("banana");
    const cherry_count = cms.query("cherry");

    // With large sketch, counts should be exact or very close
    try std.testing.expect(apple_count >= 10);
    try std.testing.expect(banana_count >= 20);
    try std.testing.expect(cherry_count >= 30);
}

test "CountMinSketch: hash produces uniform distribution" {
    const allocator = std.testing.allocator;

    var cms = try CountMinSketchValue.initByDim(allocator, 100, 1);
    defer cms.deinit();

    // Hash many items, count bucket distribution
    var buckets = [_]u32{0} ** 100;
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        var buf: [32]u8 = undefined; // Plenty of room for "item_NNNN"
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

test "CountMinSketch: merge combines two sketches" {
    const allocator = std.testing.allocator;

    var cms1 = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer cms1.deinit();

    var cms2 = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer cms2.deinit();

    // Increment different items in each sketch
    _ = try cms1.incrBy("apple", 10);
    _ = try cms2.incrBy("apple", 20);

    // Merge cms2 into cms1
    const sources = [_]*const CountMinSketchValue{&cms2};
    try cms1.merge(&sources);

    // Query should return sum
    const count = cms1.query("apple");
    try std.testing.expectEqual(@as(u64, 30), count);
}

test "CountMinSketch: merge handles multiple sources" {
    const allocator = std.testing.allocator;

    var dest = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer dest.deinit();

    var src1 = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer src1.deinit();

    var src2 = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer src2.deinit();

    var src3 = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer src3.deinit();

    // Add counts to each sketch
    _ = try dest.incrBy("apple", 5);
    _ = try src1.incrBy("apple", 10);
    _ = try src2.incrBy("apple", 15);
    _ = try src3.incrBy("apple", 20);

    // Merge all sources into dest
    const sources = [_]*const CountMinSketchValue{ &src1, &src2, &src3 };
    try dest.merge(&sources);

    // Query should return sum: 5 + 10 + 15 + 20 = 50
    const count = dest.query("apple");
    try std.testing.expectEqual(@as(u64, 50), count);
}

test "CountMinSketch: merge rejects dimension mismatch" {
    const allocator = std.testing.allocator;

    var cms1 = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer cms1.deinit();

    var cms2 = try CountMinSketchValue.initByDim(allocator, 200, 5);
    defer cms2.deinit();

    const sources = [_]*const CountMinSketchValue{&cms2};
    try std.testing.expectError(error.DimensionMismatch, cms1.merge(&sources));
}

test "CountMinSketch: merge rejects depth mismatch" {
    const allocator = std.testing.allocator;

    var cms1 = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer cms1.deinit();

    var cms2 = try CountMinSketchValue.initByDim(allocator, 100, 7);
    defer cms2.deinit();

    const sources = [_]*const CountMinSketchValue{&cms2};
    try std.testing.expectError(error.DimensionMismatch, cms1.merge(&sources));
}

test "CountMinSketch: merge detects overflow" {
    const allocator = std.testing.allocator;

    var cms1 = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer cms1.deinit();

    var cms2 = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer cms2.deinit();

    // Set counters to near-max
    const max_val = std.math.maxInt(u64) - 10;
    cms1.counters[0][0] = max_val;
    cms2.counters[0][0] = 20; // Would overflow when added

    const sources = [_]*const CountMinSketchValue{&cms2};
    try std.testing.expectError(error.CounterOverflow, cms1.merge(&sources));
}

test "CountMinSketch: merge preserves independent counts" {
    const allocator = std.testing.allocator;

    var cms1 = try CountMinSketchValue.initByDim(allocator, 1000, 7);
    defer cms1.deinit();

    var cms2 = try CountMinSketchValue.initByDim(allocator, 1000, 7);
    defer cms2.deinit();

    // Add different items to each sketch
    _ = try cms1.incrBy("apple", 10);
    _ = try cms1.incrBy("banana", 20);
    _ = try cms2.incrBy("apple", 15);
    _ = try cms2.incrBy("cherry", 30);

    // Merge cms2 into cms1
    const sources = [_]*const CountMinSketchValue{&cms2};
    try cms1.merge(&sources);

    // With large sketch, counts should be exact or very close
    const apple_count = cms1.query("apple");
    const banana_count = cms1.query("banana");
    const cherry_count = cms1.query("cherry");

    try std.testing.expect(apple_count >= 25); // 10 + 15
    try std.testing.expect(banana_count >= 20);
    try std.testing.expect(cherry_count >= 30);
}

test "CountMinSketch: getInfo returns correct metadata" {
    const allocator = std.testing.allocator;

    var cms = try CountMinSketchValue.initByDim(allocator, 100, 5);
    defer cms.deinit();

    // Initially, count should be 0
    var info = cms.getInfo();
    try std.testing.expectEqual(@as(u32, 100), info.width);
    try std.testing.expectEqual(@as(u32, 5), info.depth);
    try std.testing.expectEqual(@as(u64, 0), info.count);

    // Add some items
    _ = try cms.incrBy("apple", 10);
    _ = try cms.incrBy("banana", 20);

    // Count should reflect increments (10 + 20) * depth = (10 + 20) * 5 = 150
    // But due to hash collisions, actual sum might differ
    info = cms.getInfo();
    try std.testing.expectEqual(@as(u32, 100), info.width);
    try std.testing.expectEqual(@as(u32, 5), info.depth);
    // Total count should be at least (10 + 20) * 5 = 150 (each item increments depth counters)
    try std.testing.expect(info.count >= 150);
}

test "CountMinSketch: getInfo handles empty sketch" {
    const allocator = std.testing.allocator;

    var cms = try CountMinSketchValue.initByDim(allocator, 50, 3);
    defer cms.deinit();

    const info = cms.getInfo();
    try std.testing.expectEqual(@as(u32, 50), info.width);
    try std.testing.expectEqual(@as(u32, 3), info.depth);
    try std.testing.expectEqual(@as(u64, 0), info.count);
}
