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
