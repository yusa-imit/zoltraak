/// Intset — compact encoding for small integer-only sets
///
/// Redis uses intset when a set contains only integers and is small.
/// Format:
/// - encoding (4 bytes): 2=int16, 4=int32, 8=int64
/// - length (4 bytes): number of integers
/// - contents (variable): sorted array of integers
///
/// All integers stored in little-endian format.
/// Automatically upgrades encoding when larger integers are added.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const IntsetError = error{
    InvalidEncoding,
    OutOfMemory,
    TooLarge,
};

const INTSET_ENC_INT16: u32 = 2;
const INTSET_ENC_INT32: u32 = 4;
const INTSET_ENC_INT64: u32 = 8;

const MAX_INTSET_ENTRIES: u32 = 512; // Redis default threshold

pub const Intset = struct {
    encoding: u32, // 2, 4, or 8
    length: u32,
    contents: []u8, // Raw bytes, sorted integers
    allocator: Allocator,

    /// Create new empty intset with smallest encoding
    pub fn init(allocator: Allocator) !Intset {
        return Intset{
            .encoding = INTSET_ENC_INT16,
            .length = 0,
            .contents = &[_]u8{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Intset) void {
        if (self.contents.len > 0) {
            self.allocator.free(self.contents);
        }
    }

    /// Get number of elements
    pub fn len(self: *const Intset) u32 {
        return self.length;
    }

    /// Check if value exists
    pub fn contains(self: *const Intset, value: i64) bool {
        if (self.length == 0) return false;
        return self.search(value) != null;
    }

    /// Add value (returns true if added, false if already exists)
    pub fn add(self: *Intset, value: i64) !bool {
        // Check if upgrade needed
        const required_enc = requiredEncoding(value);
        if (required_enc > self.encoding) {
            try self.upgradeAndAdd(value, required_enc);
            return true;
        }

        // Binary search for insertion point
        const pos = self.search(value) orelse {
            // Not found, insert at correct position
            const insert_pos = self.findInsertPos(value);
            try self.insertAt(insert_pos, value);
            return true;
        };

        // Already exists
        _ = pos;
        return false;
    }

    /// Remove value (returns true if removed, false if not found)
    pub fn remove(self: *Intset, value: i64) bool {
        if (self.length == 0) return false;

        const pos = self.search(value) orelse return false;

        // Shift elements left
        const elem_size = self.encoding;
        const byte_pos = pos * elem_size;
        const remaining = (self.length - pos - 1) * elem_size;

        if (remaining > 0) {
            const src_start = byte_pos + elem_size;
            std.mem.copyForwards(u8, self.contents[byte_pos..], self.contents[src_start .. src_start + remaining]);
        }

        self.length -= 1;

        // Shrink allocation
        const new_size = self.length * elem_size;
        if (new_size == 0) {
            self.allocator.free(self.contents);
            self.contents = &[_]u8{};
        } else {
            self.contents = self.allocator.realloc(self.contents, new_size) catch self.contents;
        }

        return true;
    }

    /// Get value at index (sorted order)
    pub fn get(self: *const Intset, index: u32) !i64 {
        if (index >= self.length) return error.OutOfBounds;

        const byte_pos = index * self.encoding;
        return switch (self.encoding) {
            INTSET_ENC_INT16 => std.mem.readInt(i16, self.contents[byte_pos..][0..2], .little),
            INTSET_ENC_INT32 => std.mem.readInt(i32, self.contents[byte_pos..][0..4], .little),
            INTSET_ENC_INT64 => std.mem.readInt(i64, self.contents[byte_pos..][0..8], .little),
            else => unreachable,
        };
    }

    /// Binary search (returns index if found, null otherwise)
    fn search(self: *const Intset, value: i64) ?u32 {
        if (self.length == 0) return null;

        var left: u32 = 0;
        var right: u32 = self.length;

        while (left < right) {
            const mid = left + (right - left) / 2;
            const mid_val = self.get(mid) catch unreachable;

            if (mid_val == value) {
                return mid;
            } else if (mid_val < value) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        return null;
    }

    /// Find insertion position for value (binary search)
    fn findInsertPos(self: *const Intset, value: i64) u32 {
        if (self.length == 0) return 0;

        var left: u32 = 0;
        var right: u32 = self.length;

        while (left < right) {
            const mid = left + (right - left) / 2;
            const mid_val = self.get(mid) catch unreachable;

            if (mid_val < value) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        return left;
    }

    /// Insert value at position
    fn insertAt(self: *Intset, pos: u32, value: i64) !void {
        if (self.length >= MAX_INTSET_ENTRIES) return error.TooLarge;

        const elem_size = self.encoding;
        const old_size = self.length * elem_size;
        const new_size = (self.length + 1) * elem_size;

        // Reallocate
        const new_contents = if (old_size == 0)
            try self.allocator.alloc(u8, new_size)
        else
            try self.allocator.realloc(self.contents, new_size);

        self.contents = new_contents;

        // Shift elements right
        const byte_pos = pos * elem_size;
        const remaining = (self.length - pos) * elem_size;
        if (remaining > 0) {
            const src_start = byte_pos;
            const dst_start = byte_pos + elem_size;
            std.mem.copyBackwards(u8, self.contents[dst_start .. dst_start + remaining], self.contents[src_start .. src_start + remaining]);
        }

        // Write new value
        switch (self.encoding) {
            INTSET_ENC_INT16 => {
                std.mem.writeInt(i16, self.contents[byte_pos..][0..2], @intCast(value), .little);
            },
            INTSET_ENC_INT32 => {
                std.mem.writeInt(i32, self.contents[byte_pos..][0..4], @intCast(value), .little);
            },
            INTSET_ENC_INT64 => {
                std.mem.writeInt(i64, self.contents[byte_pos..][0..8], value, .little);
            },
            else => unreachable,
        }

        self.length += 1;
    }

    /// Upgrade encoding and add value
    fn upgradeAndAdd(self: *Intset, value: i64, new_enc: u32) !void {
        const old_len = self.length;

        // Allocate new contents
        const new_size = (old_len + 1) * new_enc;
        const new_contents = try self.allocator.alloc(u8, new_size);
        errdefer self.allocator.free(new_contents);

        // Determine if value goes at start or end
        const prepend = value < 0;

        // Copy and convert old values
        var i: u32 = 0;
        while (i < old_len) : (i += 1) {
            const old_val = try self.get(i);
            const new_pos = if (prepend) i + 1 else i;
            const byte_pos = new_pos * new_enc;

            switch (new_enc) {
                INTSET_ENC_INT32 => {
                    std.mem.writeInt(i32, new_contents[byte_pos..][0..4], @intCast(old_val), .little);
                },
                INTSET_ENC_INT64 => {
                    std.mem.writeInt(i64, new_contents[byte_pos..][0..8], old_val, .little);
                },
                else => unreachable,
            }
        }

        // Write new value at start or end
        const new_val_pos = if (prepend) 0 else old_len;
        const byte_pos = new_val_pos * new_enc;

        switch (new_enc) {
            INTSET_ENC_INT32 => {
                std.mem.writeInt(i32, new_contents[byte_pos..][0..4], @intCast(value), .little);
            },
            INTSET_ENC_INT64 => {
                std.mem.writeInt(i64, new_contents[byte_pos..][0..8], value, .little);
            },
            else => unreachable,
        }

        // Free old contents
        if (self.contents.len > 0) {
            self.allocator.free(self.contents);
        }

        // Update fields
        self.contents = new_contents;
        self.encoding = new_enc;
        self.length = old_len + 1;
    }
};

/// Determine required encoding for value
fn requiredEncoding(value: i64) u32 {
    if (value >= std.math.minInt(i16) and value <= std.math.maxInt(i16)) {
        return INTSET_ENC_INT16;
    } else if (value >= std.math.minInt(i32) and value <= std.math.maxInt(i32)) {
        return INTSET_ENC_INT32;
    } else {
        return INTSET_ENC_INT64;
    }
}

// ============================================================================
// Tests
// ============================================================================

test "intset: init and basic properties" {
    const allocator = std.testing.allocator;

    var is = try Intset.init(allocator);
    defer is.deinit();

    try std.testing.expectEqual(@as(u32, 0), is.len());
    try std.testing.expectEqual(INTSET_ENC_INT16, is.encoding);
}

test "intset: add and contains" {
    const allocator = std.testing.allocator;

    var is = try Intset.init(allocator);
    defer is.deinit();

    try std.testing.expect(try is.add(42));
    try std.testing.expect(try is.add(10));
    try std.testing.expect(try is.add(100));

    try std.testing.expectEqual(@as(u32, 3), is.len());
    try std.testing.expect(is.contains(42));
    try std.testing.expect(is.contains(10));
    try std.testing.expect(is.contains(100));
    try std.testing.expect(!is.contains(99));
}

test "intset: sorted order" {
    const allocator = std.testing.allocator;

    var is = try Intset.init(allocator);
    defer is.deinit();

    try std.testing.expect(try is.add(100));
    try std.testing.expect(try is.add(10));
    try std.testing.expect(try is.add(50));

    // Should be sorted: 10, 50, 100
    try std.testing.expectEqual(@as(i64, 10), try is.get(0));
    try std.testing.expectEqual(@as(i64, 50), try is.get(1));
    try std.testing.expectEqual(@as(i64, 100), try is.get(2));
}

test "intset: duplicates" {
    const allocator = std.testing.allocator;

    var is = try Intset.init(allocator);
    defer is.deinit();

    try std.testing.expect(try is.add(42));
    try std.testing.expect(!try is.add(42)); // Duplicate

    try std.testing.expectEqual(@as(u32, 1), is.len());
}

test "intset: remove" {
    const allocator = std.testing.allocator;

    var is = try Intset.init(allocator);
    defer is.deinit();

    try std.testing.expect(try is.add(10));
    try std.testing.expect(try is.add(20));
    try std.testing.expect(try is.add(30));

    try std.testing.expect(is.remove(20));
    try std.testing.expectEqual(@as(u32, 2), is.len());
    try std.testing.expect(!is.contains(20));

    try std.testing.expect(!is.remove(99)); // Not found
}

test "intset: encoding upgrade to i32" {
    const allocator = std.testing.allocator;

    var is = try Intset.init(allocator);
    defer is.deinit();

    // Start with i16
    try std.testing.expect(try is.add(100));
    try std.testing.expectEqual(INTSET_ENC_INT16, is.encoding);

    // Add value requiring i32
    const large: i64 = 100000;
    try std.testing.expect(try is.add(large));
    try std.testing.expectEqual(INTSET_ENC_INT32, is.encoding);
    try std.testing.expectEqual(@as(u32, 2), is.len());

    // Verify both values exist
    try std.testing.expect(is.contains(100));
    try std.testing.expect(is.contains(large));
}

test "intset: encoding upgrade to i64" {
    const allocator = std.testing.allocator;

    var is = try Intset.init(allocator);
    defer is.deinit();

    // Start with i16
    try std.testing.expect(try is.add(100));
    try std.testing.expectEqual(INTSET_ENC_INT16, is.encoding);

    // Add value requiring i64
    const huge: i64 = 10000000000;
    try std.testing.expect(try is.add(huge));
    try std.testing.expectEqual(INTSET_ENC_INT64, is.encoding);
    try std.testing.expectEqual(@as(u32, 2), is.len());

    // Verify both values exist
    try std.testing.expect(is.contains(100));
    try std.testing.expect(is.contains(huge));
}

test "intset: negative values" {
    const allocator = std.testing.allocator;

    var is = try Intset.init(allocator);
    defer is.deinit();

    try std.testing.expect(try is.add(-50));
    try std.testing.expect(try is.add(0));
    try std.testing.expect(try is.add(50));

    // Should be sorted: -50, 0, 50
    try std.testing.expectEqual(@as(i64, -50), try is.get(0));
    try std.testing.expectEqual(@as(i64, 0), try is.get(1));
    try std.testing.expectEqual(@as(i64, 50), try is.get(2));
}

test "intset: large negative upgrade" {
    const allocator = std.testing.allocator;

    var is = try Intset.init(allocator);
    defer is.deinit();

    try std.testing.expect(try is.add(100));
    try std.testing.expectEqual(INTSET_ENC_INT16, is.encoding);

    // Add large negative (prepend case in upgrade)
    const large_neg: i64 = -100000;
    try std.testing.expect(try is.add(large_neg));
    try std.testing.expectEqual(INTSET_ENC_INT32, is.encoding);

    // Should be sorted: -100000, 100
    try std.testing.expectEqual(large_neg, try is.get(0));
    try std.testing.expectEqual(@as(i64, 100), try is.get(1));
}
