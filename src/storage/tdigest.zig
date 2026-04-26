/// T-Digest probabilistic data structure for approximating quantiles and histograms.
///
/// T-Digest stores a list of weighted centroids and uses them to estimate quantiles
/// with bounded error. This implementation provides foundation functionality:
/// - CREATE: Initialize with compression parameter
/// - ADD: Append values to centroids (simplified, no merging yet)
/// - RESET: Clear centroids, preserve compression
///
/// Full T-Digest algorithm (merging, compression) deferred to iteration 227.

const std = @import("std");

pub const TDigestError = error{
    InvalidCompression,
    InvalidValue,
    InvalidQuantile,
    EmptySketch,
    OutOfMemory,
};

/// Centroid represents a weighted point in the T-Digest.
pub const Centroid = struct {
    mean: f64,
    count: u64,
};

/// TDigestValue stores a simplified T-Digest with basic operations.
pub const TDigestValue = struct {
    compression: u32,
    centroids: std.ArrayList(Centroid),
    min: f64,
    max: f64,
    total_count: u64,
    allocator: std.mem.Allocator,

    /// Initialize a new T-Digest with the given compression parameter.
    ///
    /// Compression parameter controls the maximum number of centroids to maintain.
    /// Must be > 0, typically in range 10-1000 (default 100).
    ///
    /// Ownership: Caller must deinit() when finished.
    pub fn init(allocator: std.mem.Allocator, compression: u32) TDigestError!TDigestValue {
        if (compression == 0) {
            return TDigestError.InvalidCompression;
        }

        const centroids = try std.ArrayList(Centroid).initCapacity(allocator, @min(compression, 1000));

        return TDigestValue{
            .compression = compression,
            .centroids = centroids,
            .min = std.math.inf(f64),
            .max = -std.math.inf(f64),
            .total_count = 0,
            .allocator = allocator,
        };
    }

    /// Deinitialize and free all resources.
    pub fn deinit(self: *TDigestValue) void {
        self.centroids.deinit(self.allocator);
    }

    /// Add a single value to the T-Digest (simplified, no merging yet).
    ///
    /// In the full implementation (iteration 227), values would be merged with
    /// existing centroids based on the compression parameter. For now, each
    /// value becomes its own centroid.
    pub fn add(self: *TDigestValue, value: f64) !void {
        // Reject NaN values (would corrupt min/max tracking)
        if (std.math.isNan(value)) {
            return error.InvalidValue;
        }

        // Update min/max
        if (self.total_count == 0) {
            self.min = value;
            self.max = value;
        } else {
            self.min = @min(self.min, value);
            self.max = @max(self.max, value);
        }

        // Add as new centroid
        try self.centroids.append(self.allocator, Centroid{
            .mean = value,
            .count = 1,
        });

        self.total_count += 1;
    }

    /// Reset the T-Digest, clearing all centroids.
    ///
    /// The compression parameter is preserved. After reset(), the T-Digest
    /// is in the same state as immediately after init().
    pub fn reset(self: *TDigestValue) void {
        self.centroids.clearRetainingCapacity();
        self.min = std.math.inf(f64);
        self.max = -std.math.inf(f64);
        self.total_count = 0;
    }

    /// Merge multiple T-Digest sketches into this one.
    ///
    /// Combines all centroids from source sketches into the destination sketch.
    /// This is a simplified merge implementation that concatenates centroids
    /// without full compression (deferred to future iterations).
    ///
    /// Parameters:
    ///   - sources: Slice of pointers to source TDigestValue sketches
    ///   - compression_override: Optional compression parameter to apply to dest
    ///
    /// Behavior:
    ///   - Clears existing dest centroids before merge
    ///   - Copies all centroids from all non-empty sources
    ///   - Aggregates total_count (sum of all source counts)
    ///   - Tracks min/max across all sources
    ///   - Empty sources (total_count == 0) are skipped
    ///   - If compression_override is provided, updates dest compression
    ///
    /// Ownership: Sources remain unmodified. Dest is modified in place.
    pub fn merge(self: *TDigestValue, sources: []const *TDigestValue, compression_override: ?u32) !void {
        // Apply compression override if provided
        if (compression_override) |new_compression| {
            self.compression = new_compression;
        }

        // Clear existing centroids
        self.centroids.clearRetainingCapacity();
        self.min = std.math.inf(f64);
        self.max = -std.math.inf(f64);
        self.total_count = 0;

        // Merge all sources
        for (sources) |source| {
            // Skip empty sources
            if (source.total_count == 0) {
                continue;
            }

            // Copy all centroids from this source
            for (source.centroids.items) |centroid| {
                try self.centroids.append(self.allocator, centroid);
            }

            // Update min/max
            if (self.total_count == 0) {
                self.min = source.min;
                self.max = source.max;
            } else {
                self.min = @min(self.min, source.min);
                self.max = @max(self.max, source.max);
            }

            // Aggregate total count
            self.total_count += source.total_count;
        }
    }

    /// Estimate quantile value at given quantile (0.0 to 1.0).
    ///
    /// This is a simplified implementation that sorts centroids by mean value
    /// and interpolates. Full T-Digest quantile estimation (weighted interpolation
    /// with compression) deferred to future iterations.
    ///
    /// Parameters:
    ///   - q: Quantile in range [0.0, 1.0], where 0.0 = min, 1.0 = max, 0.5 = median
    ///
    /// Returns: Estimated value at the given quantile
    ///
    /// Errors:
    ///   - InvalidQuantile: q < 0.0 or q > 1.0
    ///   - EmptySketch: No values in sketch
    pub fn quantile(self: *const TDigestValue, q: f64) TDigestError!f64 {
        // Validate quantile range
        if (q < 0.0 or q > 1.0) {
            return TDigestError.InvalidQuantile;
        }

        // Empty sketch
        if (self.total_count == 0) {
            return TDigestError.EmptySketch;
        }

        // Edge cases
        if (q == 0.0) return self.min;
        if (q == 1.0) return self.max;

        // Single value
        if (self.total_count == 1) {
            return self.centroids.items[0].mean;
        }

        // Sort centroids by mean (simplified - full algorithm uses weighted sums)
        const items = self.centroids.items;
        var sorted = try self.allocator.alloc(Centroid, items.len);
        defer self.allocator.free(sorted);
        @memcpy(sorted, items);

        // Bubble sort (simple, O(n^2) - acceptable for now, optimize later if needed)
        var i: usize = 0;
        while (i < sorted.len) : (i += 1) {
            var j: usize = 0;
            while (j < sorted.len - 1 - i) : (j += 1) {
                if (sorted[j].mean > sorted[j + 1].mean) {
                    const tmp = sorted[j];
                    sorted[j] = sorted[j + 1];
                    sorted[j + 1] = tmp;
                }
            }
        }

        // Linear interpolation based on rank position
        const rank_pos = q * @as(f64, @floatFromInt(self.total_count - 1));
        const idx = @as(usize, @intFromFloat(@floor(rank_pos)));
        const frac = rank_pos - @floor(rank_pos);

        if (idx >= sorted.len - 1) {
            return sorted[sorted.len - 1].mean;
        }

        // Interpolate between idx and idx+1
        const v1 = sorted[idx].mean;
        const v2 = sorted[idx + 1].mean;
        return v1 + frac * (v2 - v1);
    }

    /// Compute cumulative distribution function (CDF) at given value.
    ///
    /// Returns the estimated probability that a random value from the distribution
    /// is less than or equal to the given value.
    ///
    /// This is a simplified implementation. Full T-Digest CDF (weighted centroids)
    /// deferred to future iterations.
    ///
    /// Parameters:
    ///   - value: Value to compute CDF for
    ///
    /// Returns: CDF in range [0.0, 1.0]
    ///
    /// Errors:
    ///   - EmptySketch: No values in sketch
    pub fn cdf(self: *const TDigestValue, value: f64) TDigestError!f64 {
        // Empty sketch
        if (self.total_count == 0) {
            return TDigestError.EmptySketch;
        }

        // Values below min have CDF = 0
        if (value < self.min) {
            return 0.0;
        }

        // Values above max have CDF = 1
        if (value >= self.max) {
            return 1.0;
        }

        // Count values <= given value
        var count: u64 = 0;
        for (self.centroids.items) |centroid| {
            if (centroid.mean <= value) {
                count += centroid.count;
            }
        }

        // Return proportion
        return @as(f64, @floatFromInt(count)) / @as(f64, @floatFromInt(self.total_count));
    }

    /// Estimate the rank of a given value in the distribution.
    ///
    /// Returns the estimated number of values less than the given value.
    /// Rank is in range [0, total_count-1].
    ///
    /// This is a simplified implementation based on CDF.
    /// rank(value) ≈ cdf(value) * (total_count - 1)
    ///
    /// Parameters:
    ///   - value: Value to compute rank for
    ///
    /// Returns: Estimated rank as i64 (rounded)
    ///
    /// Errors:
    ///   - EmptySketch: No values in sketch
    pub fn rank(self: *const TDigestValue, value: f64) TDigestError!i64 {
        if (self.total_count == 0) {
            return TDigestError.EmptySketch;
        }

        // Values below min have rank = 0
        if (value < self.min) {
            return 0;
        }

        // Values >= max have rank = total_count - 1
        if (value >= self.max) {
            return @as(i64, @intCast(self.total_count - 1));
        }

        // Use CDF to estimate rank
        const cdf_value = try self.cdf(value);
        const rank_f64 = cdf_value * @as(f64, @floatFromInt(self.total_count - 1));
        return @as(i64, @intFromFloat(@round(rank_f64)));
    }

    /// Estimate the reverse rank of a given value in the distribution.
    ///
    /// Returns the estimated number of values greater than the given value.
    /// Reverse rank is in range [0, total_count-1].
    ///
    /// revrank(value) = (total_count - 1) - rank(value)
    ///
    /// Parameters:
    ///   - value: Value to compute reverse rank for
    ///
    /// Returns: Estimated reverse rank as i64
    ///
    /// Errors:
    ///   - EmptySketch: No values in sketch
    pub fn revrank(self: *const TDigestValue, value: f64) TDigestError!i64 {
        const fwd_rank = try self.rank(value);
        return @as(i64, @intCast(self.total_count - 1)) - fwd_rank;
    }

    /// Get the value at a given rank position.
    ///
    /// Returns the value such that approximately `rank_pos` values are less than it.
    /// This is the inverse of rank().
    ///
    /// byrank(r) ≈ quantile(r / (total_count - 1))
    ///
    /// Parameters:
    ///   - rank_pos: Rank position in range [0, total_count-1]
    ///
    /// Returns: Estimated value at the given rank
    ///
    /// Errors:
    ///   - EmptySketch: No values in sketch
    ///   - InvalidQuantile: rank_pos out of range [0, total_count-1]
    pub fn byrank(self: *const TDigestValue, rank_pos: i64) TDigestError!f64 {
        if (self.total_count == 0) {
            return TDigestError.EmptySketch;
        }

        if (rank_pos < 0 or rank_pos >= self.total_count) {
            return TDigestError.InvalidQuantile;
        }

        // Convert rank to quantile
        if (self.total_count == 1) {
            return self.centroids.items[0].mean;
        }

        const q = @as(f64, @floatFromInt(rank_pos)) / @as(f64, @floatFromInt(self.total_count - 1));
        return try self.quantile(q);
    }

    /// Get the value at a given reverse rank position.
    ///
    /// Returns the value such that approximately `revrank_pos` values are greater than it.
    /// This is the inverse of revrank().
    ///
    /// byrevrank(r) = byrank((total_count - 1) - r)
    ///
    /// Parameters:
    ///   - revrank_pos: Reverse rank position in range [0, total_count-1]
    ///
    /// Returns: Estimated value at the given reverse rank
    ///
    /// Errors:
    ///   - EmptySketch: No values in sketch
    ///   - InvalidQuantile: revrank_pos out of range [0, total_count-1]
    pub fn byrevrank(self: *const TDigestValue, revrank_pos: i64) TDigestError!f64 {
        if (self.total_count == 0) {
            return TDigestError.EmptySketch;
        }

        if (revrank_pos < 0 or revrank_pos >= self.total_count) {
            return TDigestError.InvalidQuantile;
        }

        const fwd_rank = @as(i64, @intCast(self.total_count - 1)) - revrank_pos;
        return try self.byrank(fwd_rank);
    }

    /// Structure containing T-Digest metadata for TDIGEST.INFO command.
    pub const TDigestInfo = struct {
        compression: u32,
        capacity: u64,
        merged_nodes: u64,
        unmerged_nodes: u64,
        merged_weight: u64,
        unmerged_weight: u64,
        total_compressions: u64,
        memory_usage: u64,
    };

    /// Get metadata information about the T-Digest sketch.
    ///
    /// Returns diagnostic information including:
    /// - Compression parameter
    /// - Capacity (buffer size for centroids)
    /// - Number of merged/unmerged nodes (in simplified implementation, all nodes are unmerged)
    /// - Weight of merged/unmerged nodes (count of observations)
    /// - Total number of compressions performed (stub: always 0)
    /// - Memory usage in bytes (approximate)
    ///
    /// Note: This is a simplified implementation. In a full T-Digest with merging,
    /// merged_nodes/merged_weight would track compressed centroids, and
    /// unmerged_nodes/unmerged_weight would track buffered observations.
    ///
    /// Returns: TDigestInfo struct with metadata fields
    pub fn getInfo(self: *const TDigestValue) TDigestInfo {
        // In simplified implementation, all centroids are "unmerged"
        const unmerged_nodes = @as(u64, @intCast(self.centroids.items.len));
        const unmerged_weight = self.total_count;

        // Estimate memory usage:
        // - TDigestValue struct size
        // - Centroids array capacity * sizeof(Centroid)
        const struct_size = @sizeOf(TDigestValue);
        const centroids_size = self.centroids.capacity * @sizeOf(Centroid);
        const memory_usage = @as(u64, struct_size + centroids_size);

        return TDigestInfo{
            .compression = self.compression,
            .capacity = @as(u64, self.centroids.capacity),
            .merged_nodes = 0, // Simplified: no merging yet
            .unmerged_nodes = unmerged_nodes,
            .merged_weight = 0,
            .unmerged_weight = unmerged_weight,
            .total_compressions = 0, // Stub: no compression tracking
            .memory_usage = memory_usage,
        };
    }

    /// Calculate the trimmed mean excluding values outside the specified quantile range.
    ///
    /// Returns a robust average that excludes outliers outside the low/high cutoff quantiles.
    /// This is useful for latency monitoring where occasional spikes would distort the mean.
    ///
    /// Parameters:
    ///   - low_cut_quantile: Floating-point in [0, 1]. Excludes values < this quantile. 0 = no low cut.
    ///   - high_cut_quantile: Floating-point in [0, 1]. Excludes values >= this quantile. 1 = no high cut.
    ///
    /// Returns: Estimated mean of values in the quantile range [low_cut, high_cut)
    ///
    /// Errors:
    ///   - EmptySketch: No values in sketch
    ///   - InvalidQuantile: Quantiles out of [0, 1] or low_cut >= high_cut
    pub fn trimmedMean(self: *const TDigestValue, low_cut_quantile: f64, high_cut_quantile: f64) TDigestError!f64 {
        // Validate sketch not empty
        if (self.total_count == 0) {
            return TDigestError.EmptySketch;
        }

        // Validate quantile range
        if (low_cut_quantile < 0.0 or low_cut_quantile > 1.0) {
            return TDigestError.InvalidQuantile;
        }
        if (high_cut_quantile < 0.0 or high_cut_quantile > 1.0) {
            return TDigestError.InvalidQuantile;
        }
        if (low_cut_quantile >= high_cut_quantile) {
            return TDigestError.InvalidQuantile;
        }

        // Special case: if entire range [0, 1], just compute mean of all values
        if (low_cut_quantile == 0.0 and high_cut_quantile == 1.0) {
            var sum: f64 = 0.0;
            for (self.centroids.items) |centroid| {
                sum += centroid.mean * @as(f64, @floatFromInt(centroid.count));
            }
            return sum / @as(f64, @floatFromInt(self.total_count));
        }

        // Get quantile values for cutoff boundaries
        const low_value = try self.quantile(low_cut_quantile);
        const high_value = try self.quantile(high_cut_quantile);

        // Calculate sum and count of values in range [low_value, high_value]
        // Note: Redis uses inclusive upper bound when high_cut_quantile < 1.0
        var sum: f64 = 0.0;
        var count: u64 = 0;

        for (self.centroids.items) |centroid| {
            // Include if: low_value <= centroid.mean <= high_value
            // Use inclusive upper bound to match Redis behavior
            if (centroid.mean >= low_value and centroid.mean <= high_value) {
                sum += centroid.mean * @as(f64, @floatFromInt(centroid.count));
                count += centroid.count;
            }
        }

        // If no values in range, return NaN (per Redis spec)
        if (count == 0) {
            return std.math.nan(f64);
        }

        return sum / @as(f64, @floatFromInt(count));
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "TDigestValue.init with valid compression" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try std.testing.expectEqual(100, td.compression);
    try std.testing.expectEqual(0, td.total_count);
    try std.testing.expectEqual(std.math.inf(f64), td.min);
    try std.testing.expectEqual(-std.math.inf(f64), td.max);
}

test "TDigestValue.init with zero compression returns error" {
    const allocator = std.testing.allocator;
    const result = TDigestValue.init(allocator, 0);
    try std.testing.expectError(TDigestError.InvalidCompression, result);
}

test "TDigestValue.add single value updates state" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(42.5);

    try std.testing.expectEqual(1, td.total_count);
    try std.testing.expectEqual(42.5, td.min);
    try std.testing.expectEqual(42.5, td.max);
    try std.testing.expectEqual(1, td.centroids.items.len);
    try std.testing.expectEqual(42.5, td.centroids.items[0].mean);
    try std.testing.expectEqual(1, td.centroids.items[0].count);
}

test "TDigestValue.add multiple values" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(5.0);
    try td.add(30.0);

    try std.testing.expectEqual(4, td.total_count);
    try std.testing.expectEqual(5.0, td.min);
    try std.testing.expectEqual(30.0, td.max);
    try std.testing.expectEqual(4, td.centroids.items.len);
}

test "TDigestValue.add negative values" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(-10.5);
    try td.add(5.0);
    try td.add(-20.0);

    try std.testing.expectEqual(3, td.total_count);
    try std.testing.expectEqual(-20.0, td.min);
    try std.testing.expectEqual(5.0, td.max);
}

test "TDigestValue.add infinity values" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(std.math.inf(f64));
    try td.add(-std.math.inf(f64));
    try td.add(0.0);

    try std.testing.expectEqual(3, td.total_count);
    try std.testing.expectEqual(-std.math.inf(f64), td.min);
    try std.testing.expectEqual(std.math.inf(f64), td.max);
}

test "TDigestValue.reset clears centroids" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(1.0);
    try td.add(2.0);
    try td.add(3.0);

    try std.testing.expectEqual(3, td.total_count);
    td.reset();

    try std.testing.expectEqual(0, td.total_count);
    try std.testing.expectEqual(0, td.centroids.items.len);
    try std.testing.expectEqual(std.math.inf(f64), td.min);
    try std.testing.expectEqual(-std.math.inf(f64), td.max);
    try std.testing.expectEqual(100, td.compression);
}

test "TDigestValue.reset preserves compression parameter" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 250);
    defer td.deinit();

    try td.add(1.0);
    td.reset();

    try std.testing.expectEqual(250, td.compression);
    try std.testing.expectEqual(0, td.total_count);
}

test "TDigestValue different compression values" {
    const allocator = std.testing.allocator;
    var td1 = try TDigestValue.init(allocator, 50);
    defer td1.deinit();
    var td2 = try TDigestValue.init(allocator, 200);
    defer td2.deinit();

    try std.testing.expectEqual(50, td1.compression);
    try std.testing.expectEqual(200, td2.compression);
}

test "TDigestValue.add large batch" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    var i: f64 = 0;
    while (i < 1000) : (i += 1.0) {
        try td.add(i);
    }

    try std.testing.expectEqual(1000, td.total_count);
    try std.testing.expectEqual(0.0, td.min);
    try std.testing.expectEqual(999.0, td.max);
}

test "TDigestValue multiple resets" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(1.0);
    try td.add(2.0);
    td.reset();
    try td.add(10.0);
    td.reset();
    try td.add(100.0);

    try std.testing.expectEqual(1, td.total_count);
    try std.testing.expectEqual(100.0, td.min);
    try std.testing.expectEqual(100.0, td.max);
}

test "TDigestValue.add rejects NaN" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    const nan_value = std.math.nan(f64);
    const result = td.add(nan_value);

    try std.testing.expectError(error.InvalidValue, result);
    try std.testing.expectEqual(0, td.total_count);
}

// ============================================================================
// TDIGEST.MERGE Tests (Iteration 227)
// ============================================================================

test "TDigestValue.merge two sketches" {
    const allocator = std.testing.allocator;

    var dest = try TDigestValue.init(allocator, 100);
    defer dest.deinit();

    var source1 = try TDigestValue.init(allocator, 100);
    defer source1.deinit();
    try source1.add(10.0);
    try source1.add(20.0);
    try source1.add(30.0);

    var source2 = try TDigestValue.init(allocator, 100);
    defer source2.deinit();
    try source2.add(40.0);
    try source2.add(50.0);

    const sources = [_]*TDigestValue{ &source1, &source2 };
    try dest.merge(&sources, null);

    // Should combine all values from both sources
    try std.testing.expectEqual(5, dest.total_count);
    try std.testing.expectEqual(10.0, dest.min);
    try std.testing.expectEqual(50.0, dest.max);
    try std.testing.expectEqual(5, dest.centroids.items.len);
}

test "TDigestValue.merge three sketches" {
    const allocator = std.testing.allocator;

    var dest = try TDigestValue.init(allocator, 100);
    defer dest.deinit();

    var source1 = try TDigestValue.init(allocator, 100);
    defer source1.deinit();
    try source1.add(1.0);
    try source1.add(2.0);

    var source2 = try TDigestValue.init(allocator, 100);
    defer source2.deinit();
    try source2.add(3.0);
    try source2.add(4.0);

    var source3 = try TDigestValue.init(allocator, 100);
    defer source3.deinit();
    try source3.add(5.0);
    try source3.add(6.0);

    const sources = [_]*TDigestValue{ &source1, &source2, &source3 };
    try dest.merge(&sources, null);

    try std.testing.expectEqual(6, dest.total_count);
    try std.testing.expectEqual(1.0, dest.min);
    try std.testing.expectEqual(6.0, dest.max);
}

test "TDigestValue.merge with min/max across sources" {
    const allocator = std.testing.allocator;

    var dest = try TDigestValue.init(allocator, 100);
    defer dest.deinit();

    var source1 = try TDigestValue.init(allocator, 100);
    defer source1.deinit();
    try source1.add(-100.0);
    try source1.add(0.0);

    var source2 = try TDigestValue.init(allocator, 100);
    defer source2.deinit();
    try source2.add(50.0);
    try source2.add(200.0);

    const sources = [_]*TDigestValue{ &source1, &source2 };
    try dest.merge(&sources, null);

    // Min should be from source1, max from source2
    try std.testing.expectEqual(-100.0, dest.min);
    try std.testing.expectEqual(200.0, dest.max);
    try std.testing.expectEqual(4, dest.total_count);
}

test "TDigestValue.merge with total_count aggregation" {
    const allocator = std.testing.allocator;

    var dest = try TDigestValue.init(allocator, 100);
    defer dest.deinit();

    var source1 = try TDigestValue.init(allocator, 100);
    defer source1.deinit();
    try source1.add(1.0);
    try source1.add(2.0);
    try source1.add(3.0);

    var source2 = try TDigestValue.init(allocator, 100);
    defer source2.deinit();
    try source2.add(4.0);
    try source2.add(5.0);
    try source2.add(6.0);
    try source2.add(7.0);

    const sources = [_]*TDigestValue{ &source1, &source2 };
    try dest.merge(&sources, null);

    // Should sum both source counts: 3 + 4 = 7
    try std.testing.expectEqual(7, dest.total_count);
}

test "TDigestValue.merge empty source sketch" {
    const allocator = std.testing.allocator;

    var dest = try TDigestValue.init(allocator, 100);
    defer dest.deinit();

    var source1 = try TDigestValue.init(allocator, 100);
    defer source1.deinit();
    try source1.add(10.0);
    try source1.add(20.0);

    // source2 is empty (no values added)
    var source2 = try TDigestValue.init(allocator, 100);
    defer source2.deinit();

    const sources = [_]*TDigestValue{ &source1, &source2 };
    try dest.merge(&sources, null);

    // Should only contain values from source1
    try std.testing.expectEqual(2, dest.total_count);
    try std.testing.expectEqual(10.0, dest.min);
    try std.testing.expectEqual(20.0, dest.max);
}

test "TDigestValue.merge all empty sources" {
    const allocator = std.testing.allocator;

    var dest = try TDigestValue.init(allocator, 100);
    defer dest.deinit();

    var source1 = try TDigestValue.init(allocator, 100);
    defer source1.deinit();

    var source2 = try TDigestValue.init(allocator, 100);
    defer source2.deinit();

    const sources = [_]*TDigestValue{ &source1, &source2 };
    try dest.merge(&sources, null);

    // Dest should remain empty
    try std.testing.expectEqual(0, dest.total_count);
    try std.testing.expectEqual(std.math.inf(f64), dest.min);
    try std.testing.expectEqual(-std.math.inf(f64), dest.max);
}

test "TDigestValue.merge with compression override" {
    const allocator = std.testing.allocator;

    var dest = try TDigestValue.init(allocator, 100);
    defer dest.deinit();

    var source1 = try TDigestValue.init(allocator, 50);
    defer source1.deinit();
    try source1.add(10.0);

    var source2 = try TDigestValue.init(allocator, 200);
    defer source2.deinit();
    try source2.add(20.0);

    const sources = [_]*TDigestValue{ &source1, &source2 };
    const override_compression: u32 = 150;
    try dest.merge(&sources, override_compression);

    // Dest compression should be overridden to 150
    try std.testing.expectEqual(150, dest.compression);
    try std.testing.expectEqual(2, dest.total_count);
}

test "TDigestValue.merge without compression override uses dest compression" {
    const allocator = std.testing.allocator;

    var dest = try TDigestValue.init(allocator, 100);
    defer dest.deinit();

    var source1 = try TDigestValue.init(allocator, 50);
    defer source1.deinit();
    try source1.add(10.0);

    const sources = [_]*TDigestValue{ &source1 };
    try dest.merge(&sources, null);

    // Dest compression should remain unchanged
    try std.testing.expectEqual(100, dest.compression);
}

test "TDigestValue.merge single source" {
    const allocator = std.testing.allocator;

    var dest = try TDigestValue.init(allocator, 100);
    defer dest.deinit();

    var source = try TDigestValue.init(allocator, 100);
    defer source.deinit();
    try source.add(42.0);

    const sources = [_]*TDigestValue{ &source };
    try dest.merge(&sources, null);

    try std.testing.expectEqual(1, dest.total_count);
    try std.testing.expectEqual(42.0, dest.min);
    try std.testing.expectEqual(42.0, dest.max);
}

test "TDigestValue.merge preserves centroids from all sources" {
    const allocator = std.testing.allocator;

    var dest = try TDigestValue.init(allocator, 100);
    defer dest.deinit();

    var source1 = try TDigestValue.init(allocator, 100);
    defer source1.deinit();
    try source1.add(1.0);
    try source1.add(2.0);

    var source2 = try TDigestValue.init(allocator, 100);
    defer source2.deinit();
    try source2.add(3.0);

    const sources = [_]*TDigestValue{ &source1, &source2 };
    try dest.merge(&sources, null);

    // All centroids should be copied
    try std.testing.expectEqual(3, dest.centroids.items.len);

    // Verify values are present (order may vary)
    var found_1 = false;
    var found_2 = false;
    var found_3 = false;
    for (dest.centroids.items) |c| {
        if (c.mean == 1.0) found_1 = true;
        if (c.mean == 2.0) found_2 = true;
        if (c.mean == 3.0) found_3 = true;
    }
    try std.testing.expect(found_1);
    try std.testing.expect(found_2);
    try std.testing.expect(found_3);
}

test "TDigestValue.merge with infinity values" {
    const allocator = std.testing.allocator;

    var dest = try TDigestValue.init(allocator, 100);
    defer dest.deinit();

    var source1 = try TDigestValue.init(allocator, 100);
    defer source1.deinit();
    try source1.add(std.math.inf(f64));

    var source2 = try TDigestValue.init(allocator, 100);
    defer source2.deinit();
    try source2.add(-std.math.inf(f64));

    const sources = [_]*TDigestValue{ &source1, &source2 };
    try dest.merge(&sources, null);

    try std.testing.expectEqual(2, dest.total_count);
    try std.testing.expectEqual(-std.math.inf(f64), dest.min);
    try std.testing.expectEqual(std.math.inf(f64), dest.max);
}

test "TDigestValue.merge into non-empty dest with existing data" {
    const allocator = std.testing.allocator;

    var dest = try TDigestValue.init(allocator, 100);
    defer dest.deinit();
    try dest.add(100.0);
    try dest.add(200.0);

    var source = try TDigestValue.init(allocator, 100);
    defer source.deinit();
    try source.add(300.0);

    const sources = [_]*TDigestValue{ &source };
    try dest.merge(&sources, null);

    // Should combine existing dest data with source data
    try std.testing.expectEqual(3, dest.total_count);
    try std.testing.expectEqual(100.0, dest.min);
    try std.testing.expectEqual(300.0, dest.max);
}

// ============================================================================
// TDIGEST.QUANTILE/CDF/MIN/MAX Tests (Iteration 228)
// ============================================================================

test "TDigestValue.quantile with single value" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(42.5);

    // All quantiles should return the same value
    const q0 = try td.quantile(0.0);
    const q05 = try td.quantile(0.5);
    const q1 = try td.quantile(1.0);

    try std.testing.expectEqual(42.5, q0);
    try std.testing.expectEqual(42.5, q05);
    try std.testing.expectEqual(42.5, q1);
}

test "TDigestValue.quantile with multiple values" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);
    try td.add(40.0);
    try td.add(50.0);

    // 0.0 = min
    const q0 = try td.quantile(0.0);
    try std.testing.expectEqual(10.0, q0);

    // 1.0 = max
    const q1 = try td.quantile(1.0);
    try std.testing.expectEqual(50.0, q1);

    // 0.5 = median (should be around 30.0)
    const q05 = try td.quantile(0.5);
    try std.testing.expectApproxEqRel(30.0, q05, 0.1);
}

test "TDigestValue.quantile empty sketch returns error" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    const result = td.quantile(0.5);
    try std.testing.expectError(error.EmptySketch, result);
}

test "TDigestValue.quantile invalid range returns error" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(42.0);

    const result_low = td.quantile(-0.1);
    try std.testing.expectError(error.InvalidQuantile, result_low);

    const result_high = td.quantile(1.1);
    try std.testing.expectError(error.InvalidQuantile, result_high);
}

test "TDigestValue.quantile boundary values" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(0.0);
    try td.add(100.0);

    const q0 = try td.quantile(0.0);
    try std.testing.expectEqual(0.0, q0);

    const q1 = try td.quantile(1.0);
    try std.testing.expectEqual(100.0, q1);
}

test "TDigestValue.cdf with single value" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(42.5);

    // Values below should have CDF = 0
    const cdf_low = try td.cdf(0.0);
    try std.testing.expectEqual(0.0, cdf_low);

    // Value at point should have CDF = 0.5 (simplified)
    const cdf_at = try td.cdf(42.5);
    try std.testing.expectApproxEqRel(0.5, cdf_at, 0.1);

    // Values above should have CDF = 1
    const cdf_high = try td.cdf(100.0);
    try std.testing.expectEqual(1.0, cdf_high);
}

test "TDigestValue.cdf with multiple values" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);
    try td.add(40.0);
    try td.add(50.0);

    // Values < min should have CDF = 0
    const cdf_low = try td.cdf(5.0);
    try std.testing.expectEqual(0.0, cdf_low);

    // Values > max should have CDF = 1
    const cdf_high = try td.cdf(100.0);
    try std.testing.expectEqual(1.0, cdf_high);

    // Median value should be around 0.5
    const cdf_mid = try td.cdf(30.0);
    try std.testing.expectApproxEqRel(0.5, cdf_mid, 0.15);
}

test "TDigestValue.cdf empty sketch returns error" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    const result = td.cdf(42.0);
    try std.testing.expectError(error.EmptySketch, result);
}

test "TDigestValue.cdf monotonic increasing" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);

    const cdf1 = try td.cdf(15.0);
    const cdf2 = try td.cdf(25.0);
    const cdf3 = try td.cdf(35.0);

    // CDF should be monotonic increasing
    try std.testing.expect(cdf1 <= cdf2);
    try std.testing.expect(cdf2 <= cdf3);
}

// ============================================================================
// TDIGEST.RANK/REVRANK/BYRANK/BYREVRANK Tests (Iteration 229)
// ============================================================================

test "TDigestValue.rank with single value" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(42.0);

    // Single value always has rank 0
    const r = try td.rank(42.0);
    try std.testing.expectEqual(0, r);
}

test "TDigestValue.rank with multiple values" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);
    try td.add(40.0);
    try td.add(50.0);

    // Value below min has rank 0
    const r1 = try td.rank(5.0);
    try std.testing.expectEqual(0, r1);

    // Value >= max has rank total_count - 1 = 4
    const r2 = try td.rank(100.0);
    try std.testing.expectEqual(4, r2);

    // Median value should have rank around 2
    const r3 = try td.rank(30.0);
    try std.testing.expect(r3 >= 1 and r3 <= 3);
}

test "TDigestValue.rank empty sketch returns error" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    const result = td.rank(42.0);
    try std.testing.expectError(error.EmptySketch, result);
}

test "TDigestValue.rank monotonic increasing" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);

    const r1 = try td.rank(15.0);
    const r2 = try td.rank(25.0);
    const r3 = try td.rank(35.0);

    // Ranks should be monotonic increasing
    try std.testing.expect(r1 <= r2);
    try std.testing.expect(r2 <= r3);
}

test "TDigestValue.revrank with single value" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(42.0);

    // Single value: revrank = (1 - 1) - rank(42) = 0 - 0 = 0
    const rr = try td.revrank(42.0);
    try std.testing.expectEqual(0, rr);
}

test "TDigestValue.revrank with multiple values" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);
    try td.add(40.0);
    try td.add(50.0);

    // Value below min: rank=0 → revrank=4-0=4
    const rr1 = try td.revrank(5.0);
    try std.testing.expectEqual(4, rr1);

    // Value >= max: rank=4 → revrank=4-4=0
    const rr2 = try td.revrank(100.0);
    try std.testing.expectEqual(0, rr2);
}

test "TDigestValue.revrank empty sketch returns error" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    const result = td.revrank(42.0);
    try std.testing.expectError(error.EmptySketch, result);
}

test "TDigestValue.revrank monotonic decreasing" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);

    const rr1 = try td.revrank(15.0);
    const rr2 = try td.revrank(25.0);
    const rr3 = try td.revrank(35.0);

    // Revranks should be monotonic decreasing
    try std.testing.expect(rr1 >= rr2);
    try std.testing.expect(rr2 >= rr3);
}

test "TDigestValue.byrank with single value" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(42.0);

    // Single value always at rank 0
    const v = try td.byrank(0);
    try std.testing.expectEqual(42.0, v);
}

test "TDigestValue.byrank with multiple values" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);
    try td.add(40.0);
    try td.add(50.0);

    // Rank 0 should return min
    const v0 = try td.byrank(0);
    try std.testing.expectEqual(10.0, v0);

    // Rank 4 should return max
    const v4 = try td.byrank(4);
    try std.testing.expectEqual(50.0, v4);

    // Rank 2 should be around median
    const v2 = try td.byrank(2);
    try std.testing.expectApproxEqRel(30.0, v2, 0.2);
}

test "TDigestValue.byrank empty sketch returns error" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    const result = td.byrank(0);
    try std.testing.expectError(error.EmptySketch, result);
}

test "TDigestValue.byrank out of range returns error" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);

    const result_low = td.byrank(-1);
    try std.testing.expectError(error.InvalidQuantile, result_low);

    const result_high = td.byrank(3);
    try std.testing.expectError(error.InvalidQuantile, result_high);
}

test "TDigestValue.byrank monotonic increasing" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);
    try td.add(40.0);
    try td.add(50.0);

    const v0 = try td.byrank(0);
    const v2 = try td.byrank(2);
    const v4 = try td.byrank(4);

    // Values should be monotonic increasing
    try std.testing.expect(v0 <= v2);
    try std.testing.expect(v2 <= v4);
}

test "TDigestValue.byrevrank with single value" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(42.0);

    // Single value: revrank 0 → rank 0 → value
    const v = try td.byrevrank(0);
    try std.testing.expectEqual(42.0, v);
}

test "TDigestValue.byrevrank with multiple values" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);
    try td.add(40.0);
    try td.add(50.0);

    // Revrank 0 should return max
    const v0 = try td.byrevrank(0);
    try std.testing.expectEqual(50.0, v0);

    // Revrank 4 should return min
    const v4 = try td.byrevrank(4);
    try std.testing.expectEqual(10.0, v4);
}

test "TDigestValue.byrevrank empty sketch returns error" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    const result = td.byrevrank(0);
    try std.testing.expectError(error.EmptySketch, result);
}

test "TDigestValue.byrevrank out of range returns error" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);

    const result_low = td.byrevrank(-1);
    try std.testing.expectError(error.InvalidQuantile, result_low);

    const result_high = td.byrevrank(2);
    try std.testing.expectError(error.InvalidQuantile, result_high);
}

test "TDigestValue.byrevrank monotonic decreasing" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);
    try td.add(40.0);
    try td.add(50.0);

    const v0 = try td.byrevrank(0);
    const v2 = try td.byrevrank(2);
    const v4 = try td.byrevrank(4);

    // Values should be monotonic decreasing
    try std.testing.expect(v0 >= v2);
    try std.testing.expect(v2 >= v4);
}

test "TDigestValue.rank and byrank are inverses" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);
    try td.add(40.0);
    try td.add(50.0);

    // rank(byrank(r)) ≈ r
    const v = try td.byrank(2);
    const r = try td.rank(v);
    try std.testing.expect(@abs(r - 2) <= 1); // Allow 1 error tolerance
}

test "TDigestValue.revrank and byrevrank are inverses" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);
    try td.add(40.0);
    try td.add(50.0);

    // revrank(byrevrank(r)) ≈ r
    const v = try td.byrevrank(2);
    const rr = try td.revrank(v);
    try std.testing.expect(@abs(rr - 2) <= 1); // Allow 1 error tolerance
}

// ============================================================================
// TDIGEST.INFO Tests (Iteration 230)
// ============================================================================

test "TDigestValue.getInfo empty sketch" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    const info = td.getInfo();

    try std.testing.expectEqual(100, info.compression);
    try std.testing.expectEqual(0, info.unmerged_nodes);
    try std.testing.expectEqual(0, info.unmerged_weight);
    try std.testing.expectEqual(0, info.merged_nodes);
    try std.testing.expectEqual(0, info.merged_weight);
    try std.testing.expectEqual(0, info.total_compressions);
    try std.testing.expect(info.memory_usage > 0); // Should have some memory usage
}

test "TDigestValue.getInfo with values" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);

    const info = td.getInfo();

    try std.testing.expectEqual(100, info.compression);
    try std.testing.expectEqual(3, info.unmerged_nodes); // 3 centroids
    try std.testing.expectEqual(3, info.unmerged_weight); // 3 observations
    try std.testing.expectEqual(0, info.merged_nodes);
    try std.testing.expectEqual(0, info.merged_weight);
    try std.testing.expect(info.capacity >= 3);
}

test "TDigestValue.getInfo capacity tracking" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 250);
    defer td.deinit();

    const info = td.getInfo();

    try std.testing.expectEqual(250, info.compression);
    try std.testing.expect(info.capacity >= 250);
}

test "TDigestValue.getInfo memory usage increases with data" {
    const allocator = std.testing.allocator;
    var td1 = try TDigestValue.init(allocator, 100);
    defer td1.deinit();

    var td2 = try TDigestValue.init(allocator, 100);
    defer td2.deinit();
    try td2.add(1.0);
    try td2.add(2.0);
    try td2.add(3.0);

    const info1 = td1.getInfo();
    const info2 = td2.getInfo();

    // td2 should have higher memory usage due to stored centroids
    try std.testing.expect(info2.memory_usage >= info1.memory_usage);
}

// ============================================================================
// TDIGEST.TRIMMED_MEAN Tests (Iteration 230)
// ============================================================================

test "TDigestValue.trimmedMean no trimming" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);
    try td.add(40.0);
    try td.add(50.0);

    // No trimming: [0, 1] includes all values
    const mean = try td.trimmedMean(0.0, 1.0);

    // Expected mean: (10 + 20 + 30 + 40 + 50) / 5 = 30
    try std.testing.expectApproxEqRel(30.0, mean, 0.01);
}

test "TDigestValue.trimmedMean trim extremes" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(1.0);   // Will be trimmed (low outlier)
    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);
    try td.add(40.0);
    try td.add(50.0);
    try td.add(99.0);  // Will be trimmed (high outlier)

    // Trim bottom 10% and top 10%
    const mean = try td.trimmedMean(0.1, 0.9);

    // Should exclude 1.0 and 99.0, include middle values
    // Approximate: (10 + 20 + 30 + 40 + 50) / 5 = 30
    try std.testing.expect(mean > 10.0 and mean < 50.0);
}

test "TDigestValue.trimmedMean trim bottom half" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);
    try td.add(40.0);
    try td.add(50.0);

    // Trim bottom 50%: should average top half (30, 40, 50)
    const mean = try td.trimmedMean(0.5, 1.0);

    // Expected: approximately (30 + 40 + 50) / 3 = 40
    try std.testing.expect(mean > 30.0);
}

test "TDigestValue.trimmedMean empty sketch returns error" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    const result = td.trimmedMean(0.0, 1.0);
    try std.testing.expectError(error.EmptySketch, result);
}

test "TDigestValue.trimmedMean invalid quantiles" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(42.0);

    // low_cut out of range
    const result1 = td.trimmedMean(-0.1, 0.5);
    try std.testing.expectError(error.InvalidQuantile, result1);

    // high_cut out of range
    const result2 = td.trimmedMean(0.0, 1.1);
    try std.testing.expectError(error.InvalidQuantile, result2);

    // low_cut >= high_cut
    const result3 = td.trimmedMean(0.5, 0.5);
    try std.testing.expectError(error.InvalidQuantile, result3);

    const result4 = td.trimmedMean(0.7, 0.3);
    try std.testing.expectError(error.InvalidQuantile, result4);
}

test "TDigestValue.trimmedMean single value" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(42.0);

    const mean = try td.trimmedMean(0.0, 1.0);
    try std.testing.expectEqual(42.0, mean);
}

test "TDigestValue.trimmedMean symmetric trimming" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    // 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    var i: f64 = 1.0;
    while (i <= 10.0) : (i += 1.0) {
        try td.add(i);
    }

    // Trim bottom 10% and top 10%
    const mean = try td.trimmedMean(0.1, 0.9);

    // Should exclude 1 and 10, average 2-9
    // Expected: (2+3+4+5+6+7+8+9) / 8 = 44 / 8 = 5.5
    try std.testing.expectApproxEqRel(5.5, mean, 0.2);
}

test "TDigestValue.trimmedMean narrow range may return NaN" {
    const allocator = std.testing.allocator;
    var td = try TDigestValue.init(allocator, 100);
    defer td.deinit();

    try td.add(10.0);
    try td.add(20.0);
    try td.add(30.0);
    try td.add(40.0);
    try td.add(50.0);

    // Very narrow range [0.99, 1.0) - will likely return max value or NaN
    const mean = try td.trimmedMean(0.99, 1.0);

    // Either returns max value (50.0) or NaN if range is empty
    try std.testing.expect(std.math.isNan(mean) or mean == 50.0);
}
