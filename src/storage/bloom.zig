const std = @import("std");

/// Error set for Bloom filter operations
pub const BloomError = error{
    InvalidErrorRate,
    InvalidCapacity,
    CapacityExceeded,
    InvalidExpansion,
};

/// MurmurHash3 128-bit hash for binary-safe string hashing
/// Produces two 64-bit hash values for double hashing
fn murmurHash3(data: []const u8) struct { h1: u64, h2: u64 } {
    const len = data.len;
    var h1: u64 = 0;
    var h2: u64 = 0;

    // Process 16-byte blocks
    var i: usize = 0;
    while (i + 16 <= len) : (i += 16) {
        var k1: u64 = 0;
        var k2: u64 = 0;

        // Little-endian bytes to u64
        for (0..8) |j| {
            k1 |= (@as(u64, data[i + j])) << @as(u6, @intCast(j * 8));
            k2 |= (@as(u64, data[i + 8 + j])) << @as(u6, @intCast(j * 8));
        }

        // Process k1
        k1 = (k1 *% 0x87c4b504c1a2a5c9) & 0xffffffffffffffff;
        k1 = std.math.rotl(u64, k1, 31);
        k1 = (k1 *% 0x4cf5ad432745937f) & 0xffffffffffffffff;
        h1 ^= k1;
        h1 = std.math.rotl(u64, h1, 27);
        h1 = (h1 +% h2);
        h1 = (h1 *% 5 +% 0x52dce729) & 0xffffffffffffffff;

        // Process k2
        k2 = (k2 *% 0x4cf5ad432745937f) & 0xffffffffffffffff;
        k2 = std.math.rotl(u64, k2, 33);
        k2 = (k2 *% 0x87c4b504c1a2a5c9) & 0xffffffffffffffff;
        h2 ^= k2;
        h2 = std.math.rotl(u64, h2, 31);
        h2 = (h2 +% h1);
        h2 = (h2 *% 5 +% 0x27d4eb2d) & 0xffffffffffffffff;
    }

    // Process remaining bytes
    const tail = data[i..];
    if (tail.len > 0) {
        var k1: u64 = 0;
        var k2: u64 = 0;

        if (tail.len >= 15) k2 ^= (@as(u64, tail[14]) << 48);
        if (tail.len >= 14) k2 ^= (@as(u64, tail[13]) << 40);
        if (tail.len >= 13) k2 ^= (@as(u64, tail[12]) << 32);
        if (tail.len >= 12) k2 ^= (@as(u64, tail[11]) << 24);
        if (tail.len >= 11) k2 ^= (@as(u64, tail[10]) << 16);
        if (tail.len >= 10) k2 ^= (@as(u64, tail[9]) << 8);
        if (tail.len >= 9) {
            k2 ^= @as(u64, tail[8]);
            k2 = (k2 *% 0x4cf5ad432745937f) & 0xffffffffffffffff;
            k2 = std.math.rotl(u64, k2, 33);
            k2 = (k2 *% 0x87c4b504c1a2a5c9) & 0xffffffffffffffff;
            h2 ^= k2;
        }

        if (tail.len >= 8) k1 ^= (@as(u64, tail[7]) << 56);
        if (tail.len >= 7) k1 ^= (@as(u64, tail[6]) << 48);
        if (tail.len >= 6) k1 ^= (@as(u64, tail[5]) << 40);
        if (tail.len >= 5) k1 ^= (@as(u64, tail[4]) << 32);
        if (tail.len >= 4) k1 ^= (@as(u64, tail[3]) << 24);
        if (tail.len >= 3) k1 ^= (@as(u64, tail[2]) << 16);
        if (tail.len >= 2) k1 ^= (@as(u64, tail[1]) << 8);
        if (tail.len >= 1) {
            k1 ^= @as(u64, tail[0]);
            k1 = (k1 *% 0x87c4b504c1a2a5c9) & 0xffffffffffffffff;
            k1 = std.math.rotl(u64, k1, 31);
            k1 = (k1 *% 0x4cf5ad432745937f) & 0xffffffffffffffff;
            h1 ^= k1;
        }
    }

    // Finalization
    h1 ^= @as(u64, len);
    h2 ^= @as(u64, len);

    h1 +%= h2;
    h2 +%= h1;

    h1 ^= h1 >> 33;
    h1 = (h1 *% 0xff51afd7ed558ccd) & 0xffffffffffffffff;
    h1 ^= h1 >> 33;

    h2 ^= h2 >> 33;
    h2 = (h2 *% 0xc4ceb9fe1a85ec53) & 0xffffffffffffffff;
    h2 ^= h2 >> 33;

    return .{ .h1 = h1, .h2 = h2 };
}

/// Calculate optimal Bloom filter parameters
/// k: number of hash functions
/// m: size in bits
fn calculateOptimalParams(error_rate: f64, capacity: u64, nonscaling: bool) struct { num_hashes: u8, size_bits: u64 } {
    // k = ceil(-ln(error_rate) / ln(2))
    const ln_e = @log(error_rate);
    const ln_2 = @log(2.0);
    var k = -ln_e / ln_2;
    if (k < @floor(k)) k = @ceil(k);
    var num_hashes = @as(u8, @intCast(@min(@as(u64, @intFromFloat(k)), 255)));

    if (nonscaling and num_hashes > 0) {
        num_hashes -= 1;
    }

    // m = ceil(-capacity * ln(error_rate) / (ln(2)^2))
    const ln_2_sq = ln_2 * ln_2;
    const capacity_f = @as(f64, @floatFromInt(capacity));
    var m = -capacity_f * ln_e / ln_2_sq;
    if (m < @floor(m)) m = @ceil(m);
    const size_bits = @as(u64, @intCast(@max(8, @as(u64, @intFromFloat(m)))));

    return .{ .num_hashes = num_hashes, .size_bits = size_bits };
}

/// Set bit at given index in bit array
fn setBit(bits: []u8, index: u64) void {
    const byte_idx = index / 8;
    const bit_idx = @as(u3, @intCast(index % 8));
    if (byte_idx < bits.len) {
        bits[byte_idx] |= (@as(u8, 1) << bit_idx);
    }
}

/// Check if bit at given index is set in bit array
fn checkBit(bits: []const u8, index: u64) bool {
    const byte_idx = index / 8;
    const bit_idx = @as(u3, @intCast(index % 8));
    if (byte_idx < bits.len) {
        return (bits[byte_idx] & (@as(u8, 1) << bit_idx)) != 0;
    }
    return false;
}

/// Single Bloom filter sub-filter in a scaling Bloom filter
pub const SubFilter = struct {
    bits: []u8,
    size_bits: u64,
    item_count: u64,
    allocator: std.mem.Allocator,

    /// Deinitialize sub-filter and free bit array
    pub fn deinit(self: *SubFilter) void {
        self.allocator.free(self.bits);
    }
};

/// Bloom filter value for Redis storage
pub const BloomFilterValue = struct {
    error_rate: f64,
    capacity: u64,
    expansion: u16,
    nonscaling: bool,
    num_hashes: u8,
    filters: std.ArrayList(SubFilter),
    total_items_added: u64,
    allocator: std.mem.Allocator,
    expires_at: ?i64,

    /// Initialize a new Bloom filter with specified parameters
    pub fn init(
        allocator: std.mem.Allocator,
        error_rate: f64,
        capacity: u64,
        expansion: u16,
        nonscaling: bool,
    ) !BloomFilterValue {
        // Validate parameters
        if (error_rate <= 0.0 or error_rate >= 1.0) return BloomError.InvalidErrorRate;
        if (capacity == 0) return BloomError.InvalidCapacity;

        const params = calculateOptimalParams(error_rate, capacity, nonscaling);

        var filters = try std.ArrayList(SubFilter).initCapacity(allocator, 1);
        errdefer filters.deinit(allocator);

        // Create first sub-filter
        const bits = try allocator.alloc(u8, (params.size_bits + 7) / 8);
        errdefer allocator.free(bits);

        // Initialize all bits to 0
        @memset(bits, 0);

        try filters.append(allocator, .{
            .bits = bits,
            .size_bits = params.size_bits,
            .item_count = 0,
            .allocator = allocator,
        });

        return .{
            .error_rate = error_rate,
            .capacity = capacity,
            .expansion = expansion,
            .nonscaling = nonscaling,
            .num_hashes = params.num_hashes,
            .filters = filters,
            .total_items_added = 0,
            .allocator = allocator,
            .expires_at = null,
        };
    }

    /// Deinitialize Bloom filter and free all sub-filters
    pub fn deinit(self: *BloomFilterValue) void {
        for (self.filters.items) |*filter| {
            filter.deinit();
        }
        self.filters.deinit(self.allocator);
    }

    /// Add an item to the Bloom filter
    /// Returns 1 if item was definitely new, 0 if it might have been in the set
    pub fn add(self: *BloomFilterValue, item: []const u8) !u8 {
        const hashes = murmurHash3(item);
        var is_new: u8 = 1;

        var filter = &self.filters.items[self.filters.items.len - 1];

        // Use double hashing: h_i = (h1 + i * h2) mod m
        for (0..self.num_hashes) |i| {
            const idx = (hashes.h1 +% hashes.h2 *% @as(u64, @intCast(i))) % filter.size_bits;
            if (checkBit(filter.bits, idx)) {
                // Bit was already set, so item might not be new
                is_new = 0;
            }
            setBit(filter.bits, idx);
        }

        // Update item count
        filter.item_count += 1;
        self.total_items_added += 1;

        // Check if we need to scale (add new sub-filter)
        if (!self.nonscaling) {
            // When current filter exceeds capacity * expansion factor, create new one
            const scale_threshold = self.capacity *% @as(u64, self.expansion);
            if (filter.item_count >= scale_threshold) {
                // Create new sub-filter with same parameters
                const params = calculateOptimalParams(self.error_rate, self.capacity, self.nonscaling);
                const bits = try self.allocator.alloc(u8, (params.size_bits + 7) / 8);
                errdefer self.allocator.free(bits);

                @memset(bits, 0);

                try self.filters.append(self.allocator, .{
                    .bits = bits,
                    .size_bits = params.size_bits,
                    .item_count = 0,
                    .allocator = self.allocator,
                });
            }
        }

        return is_new;
    }

    /// Check if item exists in the Bloom filter
    /// Returns true if item is in the set, false if definitely not in the set
    pub fn exists(self: *const BloomFilterValue, item: []const u8) bool {
        const hashes = murmurHash3(item);

        // Check all sub-filters
        for (self.filters.items) |filter| {
            var all_bits_set = true;

            for (0..self.num_hashes) |i| {
                const idx = (hashes.h1 +% hashes.h2 *% @as(u64, @intCast(i))) % filter.size_bits;
                if (!checkBit(filter.bits, idx)) {
                    all_bits_set = false;
                    break;
                }
            }

            // If all bits are set in this filter, item might be present
            if (all_bits_set) {
                return true;
            }
        }

        return false;
    }
};

// ── Unit tests ──────────────────────────────────────────────────────────────

test "MurmurHash3 determinism" {
    const test_string = "hello world";

    const hash1 = murmurHash3(test_string);
    const hash2 = murmurHash3(test_string);

    try std.testing.expectEqual(hash1.h1, hash2.h1);
    try std.testing.expectEqual(hash1.h2, hash2.h2);
}

test "MurmurHash3 different inputs produce different hashes" {
    const h1 = murmurHash3("hello");
    const h2 = murmurHash3("world");

    try std.testing.expect(h1.h1 != h2.h1 or h1.h2 != h2.h2);
}

test "MurmurHash3 empty string" {
    const hash = murmurHash3("");
    // Just verify it doesn't panic and produces consistent values
    const hash2 = murmurHash3("");
    try std.testing.expectEqual(hash.h1, hash2.h1);
}

test "setBit and checkBit operations" {
    var bits: [1]u8 = [_]u8{0};

    // Set bit 0
    setBit(&bits, 0);
    try std.testing.expect(checkBit(&bits, 0));

    // Set bit 7
    setBit(&bits, 7);
    try std.testing.expect(checkBit(&bits, 7));

    // Check unset bit
    try std.testing.expect(!checkBit(&bits, 1));
}

test "calculateOptimalParams basic" {
    const params = calculateOptimalParams(0.01, 100, false);
    try std.testing.expect(params.num_hashes > 0);
    try std.testing.expect(params.size_bits >= 8);
}

test "BloomFilterValue init invalid error rate" {
    const result = BloomFilterValue.init(std.testing.allocator, 0.0, 100, 2, false);
    try std.testing.expectError(BloomError.InvalidErrorRate, result);
}

test "calculateOptimalParams error rates" {
    const p1 = calculateOptimalParams(0.01, 100, false);
    const p2 = calculateOptimalParams(0.001, 100, false);

    // Higher error rate should need fewer bits
    try std.testing.expect(p2.size_bits > p1.size_bits);
}

test "calculateOptimalParams nonscaling reduces hashes" {
    const params_scaling = calculateOptimalParams(0.01, 100, false);
    const params_nonscaling = calculateOptimalParams(0.01, 100, true);

    try std.testing.expect(params_nonscaling.num_hashes <= params_scaling.num_hashes);
}

test "BloomFilterValue init and deinit" {
    var bf = try BloomFilterValue.init(std.testing.allocator, 0.01, 100, 2, false);
    defer bf.deinit();

    try std.testing.expectEqual(bf.total_items_added, 0);
    try std.testing.expectEqual(bf.filters.items.len, 1);
}

test "BloomFilterValue init invalid capacity" {
    const allocator = std.testing.allocator;
    const result = BloomFilterValue.init(allocator, 0.01, 0, 2, false);
    try std.testing.expectError(BloomError.InvalidCapacity, result);
}

test "BloomFilterValue add new item returns 1" {
    const allocator = std.testing.allocator;
    var bf = try BloomFilterValue.init(allocator, 0.01, 100, 2, false);
    defer bf.deinit();

    const result = try bf.add("hello");
    try std.testing.expectEqual(result, 1);
}

test "BloomFilterValue add duplicate returns 0" {
    const allocator = std.testing.allocator;
    var bf = try BloomFilterValue.init(allocator, 0.01, 100, 2, false);
    defer bf.deinit();

    _ = try bf.add("hello");
    const result = try bf.add("hello");
    try std.testing.expectEqual(result, 0);
}

test "BloomFilterValue exists returns true for added item" {
    const allocator = std.testing.allocator;
    var bf = try BloomFilterValue.init(allocator, 0.01, 100, 2, false);
    defer bf.deinit();

    _ = try bf.add("hello");
    const result = bf.exists("hello");
    try std.testing.expect(result);
}

test "BloomFilterValue exists returns false for non-added item" {
    const allocator = std.testing.allocator;
    var bf = try BloomFilterValue.init(allocator, 0.01, 100, 2, false);
    defer bf.deinit();

    const result = bf.exists("hello");
    try std.testing.expect(!result);
}

test "BloomFilterValue multiple items" {
    const allocator = std.testing.allocator;
    var bf = try BloomFilterValue.init(allocator, 0.01, 100, 2, false);
    defer bf.deinit();

    _ = try bf.add("apple");
    _ = try bf.add("banana");
    _ = try bf.add("cherry");

    try std.testing.expect(bf.exists("apple"));
    try std.testing.expect(bf.exists("banana"));
    try std.testing.expect(bf.exists("cherry"));
    try std.testing.expect(!bf.exists("date"));
}

test "BloomFilterValue nonscaling prevents scaling" {
    var bf = try BloomFilterValue.init(std.testing.allocator, 0.01, 10, 2, true);
    defer bf.deinit();

    // Add many items
    for (0..50) |i| {
        var buf: [20]u8 = undefined;
        const item = try std.fmt.bufPrint(&buf, "item{d}", .{i});
        _ = try bf.add(item);
    }

    // Should still have only 1 filter
    try std.testing.expectEqual(bf.filters.items.len, 1);
}

test "BloomFilterValue scaling adds sub-filters" {
    const allocator = std.testing.allocator;
    var bf = try BloomFilterValue.init(allocator, 0.01, 5, 2, false);
    defer bf.deinit();

    // Add items exceeding capacity * expansion
    for (0..25) |i| {
        var buf: [20]u8 = undefined;
        const item = try std.fmt.bufPrint(&buf, "item{d}", .{i});
        _ = try bf.add(item);
    }

    // Should have created multiple filters due to scaling
    try std.testing.expect(bf.filters.items.len > 1);
}
