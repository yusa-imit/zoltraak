const std = @import("std");

/// Time series data point with timestamp and value
pub const DataPoint = struct {
    timestamp: i64, // Unix timestamp in milliseconds
    value: f64,

    pub fn init(timestamp: i64, value: f64) DataPoint {
        return .{ .timestamp = timestamp, .value = value };
    }
};

/// Duplicate policy for handling overlapping timestamps
pub const DuplicatePolicy = enum {
    block, // Reject duplicate timestamps
    first, // Keep first value
    last, // Keep last value (default)
    min, // Keep minimum value
    max, // Keep maximum value
    sum, // Sum values

    pub fn fromString(s: []const u8) ?DuplicatePolicy {
        if (std.ascii.eqlIgnoreCase(s, "BLOCK")) return .block;
        if (std.ascii.eqlIgnoreCase(s, "FIRST")) return .first;
        if (std.ascii.eqlIgnoreCase(s, "LAST")) return .last;
        if (std.ascii.eqlIgnoreCase(s, "MIN")) return .min;
        if (std.ascii.eqlIgnoreCase(s, "MAX")) return .max;
        if (std.ascii.eqlIgnoreCase(s, "SUM")) return .sum;
        return null;
    }

    pub fn toString(self: DuplicatePolicy) []const u8 {
        return switch (self) {
            .block => "BLOCK",
            .first => "FIRST",
            .last => "LAST",
            .min => "MIN",
            .max => "MAX",
            .sum => "SUM",
        };
    }
};

/// Encoding type for time series chunks
pub const Encoding = enum {
    compressed, // Gorilla compression (stub)
    uncompressed, // Raw storage

    pub fn fromString(s: []const u8) ?Encoding {
        if (std.ascii.eqlIgnoreCase(s, "COMPRESSED")) return .compressed;
        if (std.ascii.eqlIgnoreCase(s, "UNCOMPRESSED")) return .uncompressed;
        return null;
    }

    pub fn toString(self: Encoding) []const u8 {
        return switch (self) {
            .compressed => "COMPRESSED",
            .uncompressed => "UNCOMPRESSED",
        };
    }
};

/// Time series metadata and configuration
pub const TimeSeriesInfo = struct {
    retention_ms: i64, // Retention period in milliseconds, 0 = infinite
    duplicate_policy: DuplicatePolicy,
    encoding: Encoding,
    labels: std.StringHashMap([]const u8), // Key-value labels
    chunk_size: u32, // Maximum samples per chunk (default: 4096)
    total_samples: u64, // Total number of samples
    memory_bytes: u64, // Approximate memory usage (stub)
    first_timestamp: ?i64, // Timestamp of oldest sample
    last_timestamp: ?i64, // Timestamp of newest sample

    pub fn init(allocator: std.mem.Allocator) !TimeSeriesInfo {
        return TimeSeriesInfo{
            .retention_ms = 0,
            .duplicate_policy = .last,
            .encoding = .uncompressed,
            .labels = std.StringHashMap([]const u8).init(allocator),
            .chunk_size = 4096,
            .total_samples = 0,
            .memory_bytes = 0,
            .first_timestamp = null,
            .last_timestamp = null,
        };
    }

    pub fn deinit(self: *TimeSeriesInfo, allocator: std.mem.Allocator) void {
        // Free label keys and values
        var it = self.labels.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            allocator.free(entry.value_ptr.*);
        }
        self.labels.deinit();
    }

    /// Add or update a label
    pub fn setLabel(self: *TimeSeriesInfo, allocator: std.mem.Allocator, key: []const u8, value: []const u8) !void {
        const owned_key = try allocator.dupe(u8, key);
        errdefer allocator.free(owned_key);
        const owned_value = try allocator.dupe(u8, value);
        errdefer allocator.free(owned_value);

        const result = try self.labels.getOrPut(owned_key);
        if (result.found_existing) {
            // Free old key and value
            allocator.free(result.key_ptr.*);
            allocator.free(result.value_ptr.*);
            result.key_ptr.* = owned_key;
        }
        result.value_ptr.* = owned_value;
    }
};

/// Time series value: metadata + data points
pub const TimeSeriesValue = struct {
    info: TimeSeriesInfo,
    samples: std.ArrayList(DataPoint), // Sorted by timestamp
    expires_at: ?i64, // Key-level expiration (separate from retention)
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) !TimeSeriesValue {
        const samples = try std.ArrayList(DataPoint).initCapacity(allocator, 0);
        return TimeSeriesValue{
            .info = try TimeSeriesInfo.init(allocator),
            .samples = samples,
            .expires_at = null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *TimeSeriesValue) void {
        self.info.deinit(self.allocator);
        self.samples.deinit(self.allocator);
    }

    /// Add a data point with duplicate policy enforcement
    ///
    /// Arguments:
    ///   - timestamp: Unix timestamp in milliseconds
    ///   - value: Numeric value for the data point
    ///   - policy_override: Optional override for duplicate policy (for TS.ADD ON_DUPLICATE)
    pub fn add(self: *TimeSeriesValue, timestamp: i64, value: f64, policy_override: ?DuplicatePolicy) !void {
        // Find insertion point (binary search for sorted insertion)
        const index = self.findTimestampIndex(timestamp);

        if (index < self.samples.items.len and self.samples.items[index].timestamp == timestamp) {
            // Duplicate timestamp found - apply duplicate policy (use override if provided)
            const policy = policy_override orelse self.info.duplicate_policy;
            switch (policy) {
                .block => return error.DuplicateTimestamp,
                .first => return, // Keep existing value
                .last => self.samples.items[index].value = value,
                .min => self.samples.items[index].value = @min(self.samples.items[index].value, value),
                .max => self.samples.items[index].value = @max(self.samples.items[index].value, value),
                .sum => self.samples.items[index].value += value,
            }
        } else {
            // Insert new data point at correct position
            try self.samples.insert(self.allocator, index, DataPoint.init(timestamp, value));
            self.info.total_samples += 1;

            // Update first/last timestamps
            if (self.info.first_timestamp == null or timestamp < self.info.first_timestamp.?) {
                self.info.first_timestamp = timestamp;
            }
            if (self.info.last_timestamp == null or timestamp > self.info.last_timestamp.?) {
                self.info.last_timestamp = timestamp;
            }
        }

        // Apply retention policy (remove old samples)
        if (self.info.retention_ms > 0) {
            const cutoff = timestamp - self.info.retention_ms;
            self.applyRetention(cutoff);
        }
    }

    /// Binary search to find insertion point for timestamp
    fn findTimestampIndex(self: *const TimeSeriesValue, timestamp: i64) usize {
        var left: usize = 0;
        var right: usize = self.samples.items.len;

        while (left < right) {
            const mid = left + (right - left) / 2;
            if (self.samples.items[mid].timestamp < timestamp) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        return left;
    }

    /// Remove samples older than cutoff timestamp
    fn applyRetention(self: *TimeSeriesValue, cutoff: i64) void {
        var remove_count: usize = 0;
        for (self.samples.items) |sample| {
            if (sample.timestamp < cutoff) {
                remove_count += 1;
            } else {
                break;
            }
        }

        if (remove_count > 0) {
            // Shift remaining samples to front
            std.mem.copyForwards(DataPoint, self.samples.items[0..], self.samples.items[remove_count..]);
            self.samples.shrinkRetainingCapacity(self.samples.items.len - remove_count);
            self.info.total_samples -= remove_count;

            // Update first timestamp
            if (self.samples.items.len > 0) {
                self.info.first_timestamp = self.samples.items[0].timestamp;
            } else {
                self.info.first_timestamp = null;
                self.info.last_timestamp = null;
            }
        }
    }

    /// Get the most recent sample
    pub fn getLatest(self: *const TimeSeriesValue) ?DataPoint {
        if (self.samples.items.len == 0) return null;
        return self.samples.items[self.samples.items.len - 1];
    }

    /// Increment a data point at the specified timestamp by delta amount
    ///
    /// If timestamp exists:
    ///   - Respects duplicate_policy: returns error.DuplicateTimestamp if policy is BLOCK
    ///   - Otherwise adds delta to existing value
    /// If timestamp doesn't exist, creates new sample with value = delta.
    /// If series is empty, creates sample with value = delta.
    /// Respects retention policy.
    ///
    /// Arguments:
    ///   - timestamp: Unix timestamp in milliseconds
    ///   - delta: Amount to increment by
    pub fn incrementBy(self: *TimeSeriesValue, timestamp: i64, delta: f64) !void {
        // Find the insertion point
        const index = self.findTimestampIndex(timestamp);

        if (index < self.samples.items.len and self.samples.items[index].timestamp == timestamp) {
            // Timestamp already exists - check duplicate policy
            if (self.info.duplicate_policy == .block) {
                return error.DuplicateTimestamp;
            }
            // Apply increment
            self.samples.items[index].value += delta;
        } else {
            // Timestamp doesn't exist - create new sample with delta as value
            try self.samples.insert(self.allocator, index, DataPoint.init(timestamp, delta));
            self.info.total_samples += 1;

            // Update first/last timestamps
            if (self.info.first_timestamp == null or timestamp < self.info.first_timestamp.?) {
                self.info.first_timestamp = timestamp;
            }
            if (self.info.last_timestamp == null or timestamp > self.info.last_timestamp.?) {
                self.info.last_timestamp = timestamp;
            }
        }

        // Apply retention policy (remove old samples)
        if (self.info.retention_ms > 0) {
            const cutoff = timestamp - self.info.retention_ms;
            self.applyRetention(cutoff);
        }
    }

    /// Decrement a data point at the specified timestamp by delta amount
    ///
    /// This is equivalent to calling incrementBy with negated delta.
    ///
    /// Arguments:
    ///   - timestamp: Unix timestamp in milliseconds
    ///   - delta: Amount to decrement by (will be negated internally)
    pub fn decrementBy(self: *TimeSeriesValue, timestamp: i64, delta: f64) !void {
        try self.incrementBy(timestamp, -delta);
    }

    /// Get samples in a time range
    pub fn getRange(self: *const TimeSeriesValue, from_ts: i64, to_ts: i64) []const DataPoint {
        const start_idx = self.findTimestampIndex(from_ts);
        var end_idx = self.findTimestampIndex(to_ts + 1); // Exclusive upper bound

        if (start_idx >= self.samples.items.len) return &[_]DataPoint{};
        if (end_idx > self.samples.items.len) end_idx = self.samples.items.len;

        return self.samples.items[start_idx..end_idx];
    }

    /// Delete all samples in the time range [from_ts, to_ts] inclusive
    ///
    /// Returns the number of samples deleted.
    ///
    /// Arguments:
    ///   - from_ts: Start of the time range (inclusive)
    ///   - to_ts: End of the time range (inclusive)
    pub fn deleteRange(self: *TimeSeriesValue, from_ts: i64, to_ts: i64) usize {
        // Validate range
        if (from_ts > to_ts) return 0;
        if (self.samples.items.len == 0) return 0;

        // Find the indices for the range
        const start_idx = self.findTimestampIndex(from_ts);
        const end_idx = self.findTimestampIndex(to_ts + 1); // Exclusive upper bound

        if (start_idx >= self.samples.items.len) return 0;
        if (start_idx >= end_idx) return 0;

        const delete_count = end_idx - start_idx;

        // Shift remaining samples
        if (end_idx < self.samples.items.len) {
            std.mem.copyForwards(DataPoint, self.samples.items[start_idx..], self.samples.items[end_idx..]);
        }
        self.samples.shrinkRetainingCapacity(self.samples.items.len - delete_count);
        self.info.total_samples -= delete_count;

        // Update first/last timestamps
        if (self.samples.items.len == 0) {
            self.info.first_timestamp = null;
            self.info.last_timestamp = null;
        } else {
            self.info.first_timestamp = self.samples.items[0].timestamp;
            self.info.last_timestamp = self.samples.items[self.samples.items.len - 1].timestamp;
        }

        return delete_count;
    }
};

test "DataPoint init" {
    const dp = DataPoint.init(1234567890, 42.5);
    try std.testing.expectEqual(@as(i64, 1234567890), dp.timestamp);
    try std.testing.expectEqual(@as(f64, 42.5), dp.value);
}

test "DuplicatePolicy fromString" {
    try std.testing.expectEqual(DuplicatePolicy.block, DuplicatePolicy.fromString("BLOCK").?);
    try std.testing.expectEqual(DuplicatePolicy.last, DuplicatePolicy.fromString("last").?);
    try std.testing.expectEqual(@as(?DuplicatePolicy, null), DuplicatePolicy.fromString("INVALID"));
}

test "Encoding fromString" {
    try std.testing.expectEqual(Encoding.compressed, Encoding.fromString("COMPRESSED").?);
    try std.testing.expectEqual(Encoding.uncompressed, Encoding.fromString("uncompressed").?);
}

test "TimeSeriesValue init and deinit" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    try std.testing.expectEqual(@as(u64, 0), ts.info.total_samples);
    try std.testing.expectEqual(@as(?i64, null), ts.info.first_timestamp);
}

test "TimeSeriesValue add single sample" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    try ts.add(1000, 10.5, null);
    try std.testing.expectEqual(@as(usize, 1), ts.samples.items.len);
    try std.testing.expectEqual(@as(i64, 1000), ts.samples.items[0].timestamp);
    try std.testing.expectEqual(@as(f64, 10.5), ts.samples.items[0].value);
    try std.testing.expectEqual(@as(u64, 1), ts.info.total_samples);
}

test "TimeSeriesValue add multiple samples sorted" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    try ts.add(3000, 30.0, null);
    try ts.add(1000, 10.0, null);
    try ts.add(2000, 20.0, null);

    try std.testing.expectEqual(@as(usize, 3), ts.samples.items.len);
    try std.testing.expectEqual(@as(i64, 1000), ts.samples.items[0].timestamp);
    try std.testing.expectEqual(@as(i64, 2000), ts.samples.items[1].timestamp);
    try std.testing.expectEqual(@as(i64, 3000), ts.samples.items[2].timestamp);
}

test "TimeSeriesValue duplicate policy LAST" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    ts.info.duplicate_policy = .last;
    try ts.add(1000, 10.0, null);
    try ts.add(1000, 20.0, null);

    try std.testing.expectEqual(@as(usize, 1), ts.samples.items.len);
    try std.testing.expectEqual(@as(f64, 20.0), ts.samples.items[0].value);
}

test "TimeSeriesValue duplicate policy BLOCK" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    ts.info.duplicate_policy = .block;
    try ts.add(1000, 10.0, null);
    try std.testing.expectError(error.DuplicateTimestamp, ts.add(1000, 20.0, null));
}

test "TimeSeriesValue duplicate policy SUM" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    ts.info.duplicate_policy = .sum;
    try ts.add(1000, 10.0, null);
    try ts.add(1000, 5.0, null);

    try std.testing.expectEqual(@as(usize, 1), ts.samples.items.len);
    try std.testing.expectEqual(@as(f64, 15.0), ts.samples.items[0].value);
}

test "TimeSeriesValue retention policy" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    ts.info.retention_ms = 1000; // 1 second retention
    try ts.add(1000, 10.0, null);
    try ts.add(1500, 15.0, null);
    try ts.add(2500, 25.0, null); // This should trigger retention

    // Only samples within last 1000ms should remain
    try std.testing.expectEqual(@as(usize, 2), ts.samples.items.len);
    try std.testing.expectEqual(@as(i64, 1500), ts.samples.items[0].timestamp);
    try std.testing.expectEqual(@as(i64, 2500), ts.samples.items[1].timestamp);
}

test "TimeSeriesValue getLatest" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    try std.testing.expectEqual(@as(?DataPoint, null), ts.getLatest());

    try ts.add(1000, 10.0, null);
    try ts.add(2000, 20.0, null);

    const latest = ts.getLatest().?;
    try std.testing.expectEqual(@as(i64, 2000), latest.timestamp);
    try std.testing.expectEqual(@as(f64, 20.0), latest.value);
}

test "TimeSeriesValue getRange" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    try ts.add(1000, 10.0, null);
    try ts.add(2000, 20.0, null);
    try ts.add(3000, 30.0, null);
    try ts.add(4000, 40.0, null);

    const range = ts.getRange(1500, 3500);
    try std.testing.expectEqual(@as(usize, 2), range.len);
    try std.testing.expectEqual(@as(i64, 2000), range[0].timestamp);
    try std.testing.expectEqual(@as(i64, 3000), range[1].timestamp);
}

test "TimeSeriesInfo setLabel" {
    const allocator = std.testing.allocator;
    var info = try TimeSeriesInfo.init(allocator);
    defer info.deinit(allocator);

    try info.setLabel(allocator, "sensor", "temp");
    try info.setLabel(allocator, "location", "room1");

    try std.testing.expectEqual(@as(usize, 2), info.labels.count());
    try std.testing.expectEqualStrings("temp", info.labels.get("sensor").?);
    try std.testing.expectEqualStrings("room1", info.labels.get("location").?);
}

test "TimeSeriesValue deleteRange basic" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    try ts.add(1000, 10.0, null);
    try ts.add(2000, 20.0, null);
    try ts.add(3000, 30.0, null);
    try ts.add(4000, 40.0, null);

    const deleted = ts.deleteRange(1500, 3500);
    try std.testing.expectEqual(@as(usize, 2), deleted);
    try std.testing.expectEqual(@as(usize, 2), ts.samples.items.len);
    try std.testing.expectEqual(@as(i64, 1000), ts.samples.items[0].timestamp);
    try std.testing.expectEqual(@as(i64, 4000), ts.samples.items[1].timestamp);
}

test "TimeSeriesValue deleteRange all samples" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    try ts.add(1000, 10.0, null);
    try ts.add(2000, 20.0, null);

    const deleted = ts.deleteRange(0, 5000);
    try std.testing.expectEqual(@as(usize, 2), deleted);
    try std.testing.expectEqual(@as(usize, 0), ts.samples.items.len);
    try std.testing.expectEqual(@as(?i64, null), ts.info.first_timestamp);
    try std.testing.expectEqual(@as(?i64, null), ts.info.last_timestamp);
}

test "TimeSeriesValue deleteRange no match" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    try ts.add(1000, 10.0, null);
    try ts.add(2000, 20.0, null);

    const deleted = ts.deleteRange(3000, 4000);
    try std.testing.expectEqual(@as(usize, 0), deleted);
    try std.testing.expectEqual(@as(usize, 2), ts.samples.items.len);
}

test "TimeSeriesValue deleteRange invalid range" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    try ts.add(1000, 10.0, null);

    const deleted = ts.deleteRange(5000, 3000); // from > to
    try std.testing.expectEqual(@as(usize, 0), deleted);
    try std.testing.expectEqual(@as(usize, 1), ts.samples.items.len);
}

test "TimeSeriesValue deleteRange empty series" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    const deleted = ts.deleteRange(1000, 2000);
    try std.testing.expectEqual(@as(usize, 0), deleted);
}
