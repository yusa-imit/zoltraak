const std = @import("std");

/// Time series label filter for TS.MGET
pub const TimeSeriesFilter = struct {
    label_name: []const u8,
    filter_type: enum { equals, not_equals, in_list, not_in_list, exists, not_exists },
    values: std.ArrayList([]const u8), // For in_list and not_in_list
    allocator: std.mem.Allocator,

    /// Clean up allocated filter resources (label_name and values array).
    pub fn deinit(self: *TimeSeriesFilter) void {
        for (self.values.items) |v| {
            self.allocator.free(v);
        }
        self.values.deinit(self.allocator);
    }

    /// Parse a filter expression like "label=value", "label=(v1,v2)", "label!=value", "label!=", "label="
    /// Returns null if filter expression is invalid
    pub fn parse(expr: []const u8, allocator: std.mem.Allocator) !?TimeSeriesFilter {
        // Find the position of = or !=
        var eq_pos: ?usize = null;
        var is_not_equals = false;

        var i: usize = 0;
        while (i < expr.len) : (i += 1) {
            if (i + 1 < expr.len and expr[i] == '!' and expr[i + 1] == '=') {
                eq_pos = i;
                is_not_equals = true;
                break;
            } else if (expr[i] == '=' and (i == 0 or expr[i - 1] != '!')) {
                eq_pos = i;
                is_not_equals = false;
                break;
            }
        }

        if (eq_pos == null) return null;

        const label_name = expr[0..eq_pos.?];
        const value_start = if (is_not_equals) eq_pos.? + 2 else eq_pos.? + 1;
        const value_part = if (value_start < expr.len) expr[value_start..] else "";

        var filter = TimeSeriesFilter{
            .label_name = try allocator.dupe(u8, label_name),
            .filter_type = undefined,
            .values = try std.ArrayList([]const u8).initCapacity(allocator, 4),
            .allocator = allocator,
        };
        errdefer {
            allocator.free(filter.label_name);
            filter.values.deinit(allocator);
        }

        // Determine filter type based on value part
        if (value_part.len == 0) {
            // label= or label!=
            filter.filter_type = if (is_not_equals) .not_exists else .exists;
        } else if (std.mem.startsWith(u8, value_part, "(") and std.mem.endsWith(u8, value_part, ")")) {
            // label=(v1,v2,...) or label!=(v1,v2,...)
            const values_str = value_part[1 .. value_part.len - 1];
            var iter = std.mem.splitSequence(u8, values_str, ",");
            while (iter.next()) |val| {
                const trimmed = std.mem.trim(u8, val, " \t");
                try filter.values.append(allocator, try allocator.dupe(u8, trimmed));
            }
            filter.filter_type = if (is_not_equals) .not_in_list else .in_list;
        } else {
            // label=value or label!=value
            try filter.values.append(allocator, try allocator.dupe(u8, value_part));
            filter.filter_type = if (is_not_equals) .not_equals else .equals;
        }

        return filter;
    }

    /// Check if a time series matches this filter
    pub fn matches(self: *const TimeSeriesFilter, info: *const TimeSeriesInfo) bool {
        const label_value = info.labels.get(self.label_name);

        switch (self.filter_type) {
            .equals => {
                if (label_value) |v| {
                    return std.mem.eql(u8, v, self.values.items[0]);
                }
                return false;
            },
            .not_equals => {
                if (label_value) |v| {
                    return !std.mem.eql(u8, v, self.values.items[0]);
                }
                return true;
            },
            .in_list => {
                if (label_value) |v| {
                    for (self.values.items) |val| {
                        if (std.mem.eql(u8, v, val)) return true;
                    }
                }
                return false;
            },
            .not_in_list => {
                if (label_value) |v| {
                    for (self.values.items) |val| {
                        if (std.mem.eql(u8, v, val)) return false;
                    }
                    return true;
                }
                return true;
            },
            .exists => {
                return label_value != null;
            },
            .not_exists => {
                return label_value == null;
            },
        }
    }
};

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

    /// Alter time series configuration.
    ///
    /// Updates specified fields of the time series. Fields that are null are left unchanged.
    /// Labels: if provided, completely replaces existing labels (CLEAR all, then add new ones).
    /// ENCODING cannot be changed (immutable after creation).
    ///
    /// Arguments:
    ///   - retention_ms: Optional new retention period (null = unchanged)
    ///   - chunk_size: Optional new chunk size (null = unchanged)
    ///   - duplicate_policy: Optional new duplicate policy (null = unchanged)
    ///   - labels: Optional array of [key, value] pairs to replace all labels (null = unchanged)
    pub fn alter(
        self: *TimeSeriesValue,
        allocator: std.mem.Allocator,
        retention_ms: ?i64,
        chunk_size: ?u32,
        duplicate_policy: ?DuplicatePolicy,
        labels: ?[]const struct { key: []const u8, value: []const u8 },
    ) !void {
        // Update retention if provided
        if (retention_ms) |new_retention| {
            self.info.retention_ms = new_retention;
        }

        // Update chunk size if provided
        if (chunk_size) |new_chunk_size| {
            self.info.chunk_size = new_chunk_size;
        }

        // Update duplicate policy if provided
        if (duplicate_policy) |new_policy| {
            self.info.duplicate_policy = new_policy;
        }

        // Update labels if provided (complete replacement)
        if (labels) |new_labels| {
            // Clear existing labels
            var it = self.info.labels.iterator();
            while (it.next()) |entry| {
                allocator.free(entry.key_ptr.*);
                allocator.free(entry.value_ptr.*);
            }
            self.info.labels.clearRetainingCapacity();

            // Add new labels
            for (new_labels) |label| {
                try self.info.setLabel(allocator, label.key, label.value);
            }
        }
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

test "TimeSeriesValue alter retention" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    // Initial retention should be 0
    try std.testing.expectEqual(@as(i64, 0), ts.info.retention_ms);

    // Alter retention
    try ts.alter(allocator, 86400000, null, null, null);
    try std.testing.expectEqual(@as(i64, 86400000), ts.info.retention_ms);

    // Alter again with different value
    try ts.alter(allocator, 3600000, null, null, null);
    try std.testing.expectEqual(@as(i64, 3600000), ts.info.retention_ms);
}

test "TimeSeriesValue alter chunk_size" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    // Initial chunk size should be 4096
    try std.testing.expectEqual(@as(u32, 4096), ts.info.chunk_size);

    // Alter chunk size
    try ts.alter(allocator, null, 8192, null, null);
    try std.testing.expectEqual(@as(u32, 8192), ts.info.chunk_size);
}

test "TimeSeriesValue alter duplicate_policy" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    // Initial policy should be LAST
    try std.testing.expectEqual(DuplicatePolicy.last, ts.info.duplicate_policy);

    // Alter to SUM
    try ts.alter(allocator, null, null, DuplicatePolicy.sum, null);
    try std.testing.expectEqual(DuplicatePolicy.sum, ts.info.duplicate_policy);

    // Alter to MIN
    try ts.alter(allocator, null, null, DuplicatePolicy.min, null);
    try std.testing.expectEqual(DuplicatePolicy.min, ts.info.duplicate_policy);
}

test "TimeSeriesValue alter labels" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    // Set initial labels
    try ts.info.setLabel(allocator, "sensor", "temp");
    try ts.info.setLabel(allocator, "location", "room1");
    try std.testing.expectEqual(@as(usize, 2), ts.info.labels.count());

    // Alter with new labels (should replace)
    const new_labels = [_]struct { key: []const u8, value: []const u8 }{
        .{ .key = "type", .value = "sensor" },
        .{ .key = "id", .value = "123" },
    };
    try ts.alter(allocator, null, null, null, &new_labels);

    // Verify old labels are gone and new ones are present
    try std.testing.expectEqual(@as(usize, 2), ts.info.labels.count());
    try std.testing.expectEqualStrings("sensor", ts.info.labels.get("type").?);
    try std.testing.expectEqualStrings("123", ts.info.labels.get("id").?);
    try std.testing.expectEqual(@as(?[]const u8, null), ts.info.labels.get("sensor"));
}

test "TimeSeriesValue alter empty labels" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    // Set initial labels
    try ts.info.setLabel(allocator, "sensor", "temp");
    try std.testing.expectEqual(@as(usize, 1), ts.info.labels.count());

    // Alter with empty labels (should clear all)
    const new_labels: [0]struct { key: []const u8, value: []const u8 } = .{};
    try ts.alter(allocator, null, null, null, &new_labels);

    // Verify all labels are cleared
    try std.testing.expectEqual(@as(usize, 0), ts.info.labels.count());
}

test "TimeSeriesValue alter multiple fields" {
    const allocator = std.testing.allocator;
    var ts = try TimeSeriesValue.init(allocator);
    defer ts.deinit();

    // Set initial state
    try ts.info.setLabel(allocator, "sensor", "temp");

    // Alter multiple fields at once
    const new_labels = [_]struct { key: []const u8, value: []const u8 }{
        .{ .key = "type", .value = "metric" },
    };
    try ts.alter(allocator, 3600000, 2048, DuplicatePolicy.max, &new_labels);

    // Verify all changes
    try std.testing.expectEqual(@as(i64, 3600000), ts.info.retention_ms);
    try std.testing.expectEqual(@as(u32, 2048), ts.info.chunk_size);
    try std.testing.expectEqual(DuplicatePolicy.max, ts.info.duplicate_policy);
    try std.testing.expectEqual(@as(usize, 1), ts.info.labels.count());
    try std.testing.expectEqualStrings("metric", ts.info.labels.get("type").?);
}

test "TimeSeriesFilter parse equals" {
    const allocator = std.testing.allocator;

    const filter_opt = try TimeSeriesFilter.parse("sensor=temp", allocator);
    try std.testing.expect(filter_opt != null);
    var filter = filter_opt.?;
    defer filter.deinit();

    try std.testing.expectEqualStrings("sensor", filter.label_name);
    try std.testing.expect(filter.filter_type == .equals);
    try std.testing.expectEqualStrings("temp", filter.values.items[0]);
}

test "TimeSeriesFilter parse not_equals" {
    const allocator = std.testing.allocator;

    const filter_opt = try TimeSeriesFilter.parse("sensor!=temp", allocator);
    try std.testing.expect(filter_opt != null);
    var filter = filter_opt.?;
    defer filter.deinit();

    try std.testing.expect(filter.filter_type == .not_equals);
}

test "TimeSeriesFilter parse in_list" {
    const allocator = std.testing.allocator;

    const filter_opt = try TimeSeriesFilter.parse("sensor=(temp,humid,pressure)", allocator);
    try std.testing.expect(filter_opt != null);
    var filter = filter_opt.?;
    defer filter.deinit();

    try std.testing.expect(filter.filter_type == .in_list);
    try std.testing.expectEqual(@as(usize, 3), filter.values.items.len);
    try std.testing.expectEqualStrings("temp", filter.values.items[0]);
    try std.testing.expectEqualStrings("humid", filter.values.items[1]);
    try std.testing.expectEqualStrings("pressure", filter.values.items[2]);
}

test "TimeSeriesFilter parse exists" {
    const allocator = std.testing.allocator;

    const filter_opt = try TimeSeriesFilter.parse("sensor=", allocator);
    try std.testing.expect(filter_opt != null);
    var filter = filter_opt.?;
    defer filter.deinit();

    try std.testing.expect(filter.filter_type == .exists);
}

test "TimeSeriesFilter parse not_exists" {
    const allocator = std.testing.allocator;

    const filter_opt = try TimeSeriesFilter.parse("sensor!=", allocator);
    try std.testing.expect(filter_opt != null);
    var filter = filter_opt.?;
    defer filter.deinit();

    try std.testing.expect(filter.filter_type == .not_exists);
}

test "TimeSeriesFilter matches equals" {
    const allocator = std.testing.allocator;

    var info = try TimeSeriesInfo.init(allocator);
    defer info.deinit(allocator);
    try info.setLabel(allocator, "sensor", "temp");

    const filter_opt = try TimeSeriesFilter.parse("sensor=temp", allocator);
    var filter = filter_opt.?;
    defer filter.deinit();

    try std.testing.expectEqual(true, filter.matches(&info));
}

test "TimeSeriesFilter matches equals mismatch" {
    const allocator = std.testing.allocator;

    var info = try TimeSeriesInfo.init(allocator);
    defer info.deinit(allocator);
    try info.setLabel(allocator, "sensor", "temp");

    const filter_opt = try TimeSeriesFilter.parse("sensor=humid", allocator);
    var filter = filter_opt.?;
    defer filter.deinit();

    try std.testing.expectEqual(false, filter.matches(&info));
}

test "TimeSeriesFilter matches in_list" {
    const allocator = std.testing.allocator;

    var info = try TimeSeriesInfo.init(allocator);
    defer info.deinit(allocator);
    try info.setLabel(allocator, "sensor", "humid");

    const filter_opt = try TimeSeriesFilter.parse("sensor=(temp,humid,pressure)", allocator);
    var filter = filter_opt.?;
    defer filter.deinit();

    try std.testing.expectEqual(true, filter.matches(&info));
}
