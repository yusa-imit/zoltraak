const std = @import("std");

/// Encoding type for IntSet based on value range
pub const Encoding = enum(u8) {
    int16 = 2, // -32768 to 32767
    int32 = 4, // -2147483648 to 2147483647
    int64 = 8, // Full i64 range
};

/// Compact integer set implementation using sorted array
/// Memory-efficient representation for sets containing only integers
pub const IntSet = struct {
    encoding: Encoding,
    data: std.ArrayListUnmanaged(u8), // Raw bytes storing integers
    length: usize, // Number of elements
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .encoding = .int16,
            .data = std.ArrayListUnmanaged(u8){},
            .length = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.data.deinit(self.allocator);
    }

    /// Returns the encoding needed for this value
    fn encodingForValue(value: i64) Encoding {
        if (value >= std.math.minInt(i16) and value <= std.math.maxInt(i16)) {
            return .int16;
        } else if (value >= std.math.minInt(i32) and value <= std.math.maxInt(i32)) {
            return .int32;
        } else {
            return .int64;
        }
    }

    /// Get integer at position
    fn getAt(self: *const Self, pos: usize) !i64 {
        if (pos >= self.length) return error.IndexOutOfBounds;

        const byte_size = @intFromEnum(self.encoding);
        const offset = pos * byte_size;

        return switch (self.encoding) {
            .int16 => blk: {
                const bytes = self.data.items[offset..][0..2];
                break :blk @as(i64, std.mem.readInt(i16, bytes, .little));
            },
            .int32 => blk: {
                const bytes = self.data.items[offset..][0..4];
                break :blk @as(i64, std.mem.readInt(i32, bytes, .little));
            },
            .int64 => blk: {
                const bytes = self.data.items[offset..][0..8];
                break :blk std.mem.readInt(i64, bytes, .little);
            },
        };
    }

    /// Set integer at position
    fn setAt(self: *Self, pos: usize, value: i64) !void {
        if (pos >= self.length) return error.IndexOutOfBounds;

        const byte_size = @intFromEnum(self.encoding);
        const offset = pos * byte_size;

        switch (self.encoding) {
            .int16 => {
                const v = @as(i16, @intCast(value));
                const bytes = self.data.items[offset..][0..2];
                std.mem.writeInt(i16, bytes, v, .little);
            },
            .int32 => {
                const v = @as(i32, @intCast(value));
                const bytes = self.data.items[offset..][0..4];
                std.mem.writeInt(i32, bytes, v, .little);
            },
            .int64 => {
                const bytes = self.data.items[offset..][0..8];
                std.mem.writeInt(i64, bytes, value, .little);
            },
        }
    }

    /// Binary search for value position
    /// Returns index if found, or insertion point if not found
    fn search(self: *const Self, value: i64) !struct { found: bool, pos: usize } {
        if (self.length == 0) return .{ .found = false, .pos = 0 };

        var left: usize = 0;
        var right: usize = self.length;

        while (left < right) {
            const mid = left + (right - left) / 2;
            const mid_val = try self.getAt(mid);

            if (mid_val == value) {
                return .{ .found = true, .pos = mid };
            } else if (mid_val < value) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        return .{ .found = false, .pos = left };
    }

    /// Check if value exists in set
    pub fn contains(self: *const Self, value: i64) !bool {
        const result = try self.search(value);
        return result.found;
    }

    /// Upgrade encoding to support larger values
    fn upgradeEncoding(self: *Self, new_encoding: Encoding) !void {
        if (@intFromEnum(new_encoding) <= @intFromEnum(self.encoding)) return;

        const new_size = @intFromEnum(new_encoding);
        var new_data = try std.ArrayListUnmanaged(u8).initCapacity(self.allocator, self.length * new_size);
        errdefer new_data.deinit(self.allocator);

        // Copy all values to new encoding
        var i: usize = 0;
        while (i < self.length) : (i += 1) {
            const value = try self.getAt(i);

            // Write value in new encoding
            switch (new_encoding) {
                .int16 => unreachable, // Can't downgrade
                .int32 => {
                    var bytes: [4]u8 = undefined;
                    std.mem.writeInt(i32, &bytes, @intCast(value), .little);
                    try new_data.appendSlice(self.allocator, &bytes);
                },
                .int64 => {
                    var bytes: [8]u8 = undefined;
                    std.mem.writeInt(i64, &bytes, value, .little);
                    try new_data.appendSlice(self.allocator, &bytes);
                },
            }
        }

        self.data.deinit(self.allocator);
        self.data = new_data;
        self.encoding = new_encoding;
    }

    /// Add value to set
    /// Returns true if value was added (didn't exist), false if already existed
    pub fn add(self: *Self, value: i64) !bool {
        // Check if value requires encoding upgrade
        const required_encoding = encodingForValue(value);
        if (@intFromEnum(required_encoding) > @intFromEnum(self.encoding)) {
            try self.upgradeEncoding(required_encoding);
        }

        const result = try self.search(value);
        if (result.found) return false; // Already exists

        // Insert at position
        const byte_size = @intFromEnum(self.encoding);
        const insert_offset = result.pos * byte_size;

        // Resize data array
        const old_len = self.data.items.len;
        try self.data.resize(self.allocator, old_len + byte_size);

        // Shift existing elements
        if (result.pos < self.length) {
            const shift_src = insert_offset;
            const shift_dst = insert_offset + byte_size;
            const shift_len = old_len - insert_offset;
            std.mem.copyBackwards(u8, self.data.items[shift_dst .. shift_dst + shift_len], self.data.items[shift_src .. shift_src + shift_len]);
        }

        self.length += 1;

        // Write new value
        switch (self.encoding) {
            .int16 => {
                std.mem.writeInt(i16, self.data.items[insert_offset..][0..2], @intCast(value), .little);
            },
            .int32 => {
                std.mem.writeInt(i32, self.data.items[insert_offset..][0..4], @intCast(value), .little);
            },
            .int64 => {
                std.mem.writeInt(i64, self.data.items[insert_offset..][0..8], value, .little);
            },
        }

        return true;
    }

    /// Remove value from set
    /// Returns true if value was removed, false if didn't exist
    pub fn remove(self: *Self, value: i64) !bool {
        const result = try self.search(value);
        if (!result.found) return false;

        const byte_size = @intFromEnum(self.encoding);
        const remove_offset = result.pos * byte_size;

        // Shift elements down
        if (result.pos < self.length - 1) {
            const shift_src = remove_offset + byte_size;
            const shift_dst = remove_offset;
            const shift_len = (self.length - result.pos - 1) * byte_size;
            std.mem.copyForwards(u8, self.data.items[shift_dst .. shift_dst + shift_len], self.data.items[shift_src .. shift_src + shift_len]);
        }

        self.length -= 1;
        try self.data.resize(self.allocator, self.length * byte_size);

        return true;
    }

    /// Get all values as a slice (allocates)
    pub fn toSlice(self: *const Self, allocator: std.mem.Allocator) ![]i64 {
        const result = try allocator.alloc(i64, self.length);
        errdefer allocator.free(result);

        var i: usize = 0;
        while (i < self.length) : (i += 1) {
            result[i] = try self.getAt(i);
        }

        return result;
    }

    /// Get random element (requires RNG)
    pub fn random(self: *const Self, rng: std.Random) !?i64 {
        if (self.length == 0) return null;
        const index = rng.uintLessThan(usize, self.length);
        return try self.getAt(index);
    }

    /// Pop random element
    pub fn pop(self: *Self, rng: std.Random) !?i64 {
        if (self.length == 0) return null;
        const index = rng.uintLessThan(usize, self.length);
        const value = try self.getAt(index);
        _ = try self.remove(value);
        return value;
    }
};

// --- TESTS ---

test "IntSet: init and deinit" {
    const allocator = std.testing.allocator;
    var intset = IntSet.init(allocator);
    defer intset.deinit();

    try std.testing.expectEqual(@as(usize, 0), intset.length);
    try std.testing.expectEqual(Encoding.int16, intset.encoding);
}

test "IntSet: add single value" {
    const allocator = std.testing.allocator;
    var intset = IntSet.init(allocator);
    defer intset.deinit();

    const added = try intset.add(42);
    try std.testing.expect(added);
    try std.testing.expectEqual(@as(usize, 1), intset.length);
    try std.testing.expect(try intset.contains(42));
}

test "IntSet: add duplicate returns false" {
    const allocator = std.testing.allocator;
    var intset = IntSet.init(allocator);
    defer intset.deinit();

    _ = try intset.add(42);
    const added_again = try intset.add(42);
    try std.testing.expect(!added_again);
    try std.testing.expectEqual(@as(usize, 1), intset.length);
}

test "IntSet: maintains sorted order" {
    const allocator = std.testing.allocator;
    var intset = IntSet.init(allocator);
    defer intset.deinit();

    _ = try intset.add(5);
    _ = try intset.add(1);
    _ = try intset.add(10);
    _ = try intset.add(3);

    const values = try intset.toSlice(allocator);
    defer allocator.free(values);

    try std.testing.expectEqual(@as(usize, 4), values.len);
    try std.testing.expectEqual(@as(i64, 1), values[0]);
    try std.testing.expectEqual(@as(i64, 3), values[1]);
    try std.testing.expectEqual(@as(i64, 5), values[2]);
    try std.testing.expectEqual(@as(i64, 10), values[3]);
}

test "IntSet: remove existing value" {
    const allocator = std.testing.allocator;
    var intset = IntSet.init(allocator);
    defer intset.deinit();

    _ = try intset.add(1);
    _ = try intset.add(2);
    _ = try intset.add(3);

    const removed = try intset.remove(2);
    try std.testing.expect(removed);
    try std.testing.expectEqual(@as(usize, 2), intset.length);
    try std.testing.expect(!try intset.contains(2));
    try std.testing.expect(try intset.contains(1));
    try std.testing.expect(try intset.contains(3));
}

test "IntSet: remove non-existing value" {
    const allocator = std.testing.allocator;
    var intset = IntSet.init(allocator);
    defer intset.deinit();

    _ = try intset.add(1);
    const removed = try intset.remove(999);
    try std.testing.expect(!removed);
    try std.testing.expectEqual(@as(usize, 1), intset.length);
}

test "IntSet: encoding upgrade int16 to int32" {
    const allocator = std.testing.allocator;
    var intset = IntSet.init(allocator);
    defer intset.deinit();

    _ = try intset.add(100); // int16
    try std.testing.expectEqual(Encoding.int16, intset.encoding);

    _ = try intset.add(100000); // Requires int32
    try std.testing.expectEqual(Encoding.int32, intset.encoding);

    try std.testing.expect(try intset.contains(100));
    try std.testing.expect(try intset.contains(100000));
}

test "IntSet: encoding upgrade int32 to int64" {
    const allocator = std.testing.allocator;
    var intset = IntSet.init(allocator);
    defer intset.deinit();

    _ = try intset.add(100000); // int32
    try std.testing.expectEqual(Encoding.int32, intset.encoding);

    _ = try intset.add(5000000000); // Requires int64
    try std.testing.expectEqual(Encoding.int64, intset.encoding);

    try std.testing.expect(try intset.contains(100000));
    try std.testing.expect(try intset.contains(5000000000));
}

test "IntSet: negative integers" {
    const allocator = std.testing.allocator;
    var intset = IntSet.init(allocator);
    defer intset.deinit();

    _ = try intset.add(-100);
    _ = try intset.add(0);
    _ = try intset.add(100);

    try std.testing.expect(try intset.contains(-100));
    try std.testing.expect(try intset.contains(0));
    try std.testing.expect(try intset.contains(100));

    const values = try intset.toSlice(allocator);
    defer allocator.free(values);

    try std.testing.expectEqual(@as(i64, -100), values[0]);
    try std.testing.expectEqual(@as(i64, 0), values[1]);
    try std.testing.expectEqual(@as(i64, 100), values[2]);
}

test "IntSet: large set" {
    const allocator = std.testing.allocator;
    var intset = IntSet.init(allocator);
    defer intset.deinit();

    // Add 100 values
    var i: i64 = 0;
    while (i < 100) : (i += 1) {
        _ = try intset.add(i * 2); // Even numbers
    }

    try std.testing.expectEqual(@as(usize, 100), intset.length);

    // Verify all exist
    i = 0;
    while (i < 100) : (i += 1) {
        try std.testing.expect(try intset.contains(i * 2));
        try std.testing.expect(!try intset.contains(i * 2 + 1)); // Odd numbers not present
    }
}
