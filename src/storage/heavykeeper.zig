const std = @import("std");

/// Error set for HeavyKeeper operations
pub const HeavyKeeperError = error{
    InvalidK,
    InvalidWidth,
    InvalidDepth,
    InvalidDecay,
};

/// Hash cell in the count-min structure
const HashCell = struct {
    fingerprint: u8,
    counter: u64,
};

/// Min heap item for maintaining top-K elements
const MinHeapItem = struct {
    key: []u8,
    count: u64,

    fn deinit(self: *MinHeapItem, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
    }
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

/// HeavyKeeper probabilistic data structure for top-K frequency tracking
/// Used by HOTKEYS command to track most frequently accessed keys
pub const HeavyKeeper = struct {
    allocator: std.mem.Allocator,
    k: u32,
    width: u32,
    depth: u32,
    decay: f64,
    hash_table: [][]HashCell,
    heap: std.ArrayList(MinHeapItem),
    prng: std.Random.DefaultPrng,

    /// Initialize a new HeavyKeeper structure
    /// Parameters:
    /// - k: number of top items to track (must be > 0)
    /// - width: number of buckets per row (default: 8)
    /// - depth: number of hash table rows (default: 7)
    /// - decay: exponential decay parameter (must be 0 < decay < 1, default: 0.9)
    pub fn init(allocator: std.mem.Allocator, k: u32, width: u32, depth: u32, decay: f64) !HeavyKeeper {
        if (k == 0) return HeavyKeeperError.InvalidK;
        if (width == 0) return HeavyKeeperError.InvalidWidth;
        if (depth == 0) return HeavyKeeperError.InvalidDepth;
        if (decay <= 0.0 or decay >= 1.0) return HeavyKeeperError.InvalidDecay;

        // Allocate hash table rows
        const hash_table = try allocator.alloc([]HashCell, depth);
        errdefer allocator.free(hash_table);

        // Allocate each row and initialize cells
        for (hash_table, 0..) |*row, i| {
            row.* = try allocator.alloc(HashCell, width);
            errdefer {
                // Clean up previously allocated rows on error
                for (hash_table[0..i]) |prev_row| {
                    allocator.free(prev_row);
                }
            }
            // Initialize all cells to zero
            for (row.*) |*cell| {
                cell.* = .{ .fingerprint = 0, .counter = 0 };
            }
        }

        return HeavyKeeper{
            .allocator = allocator,
            .k = k,
            .width = width,
            .depth = depth,
            .decay = decay,
            .hash_table = hash_table,
            .heap = std.ArrayList(MinHeapItem){},
            .prng = std.Random.DefaultPrng.init(42),
        };
    }

    /// Free all memory used by this HeavyKeeper structure
    pub fn deinit(self: *HeavyKeeper) void {
        // Free heap items
        for (self.heap.items) |*item| {
            item.deinit(self.allocator);
        }
        self.heap.deinit(self.allocator);

        // Free hash table
        for (self.hash_table) |row| {
            self.allocator.free(row);
        }
        self.allocator.free(self.hash_table);
    }

    /// Add an item with a specified weight
    /// Returns the new estimated frequency for the item
    pub fn add(self: *HeavyKeeper, key: []const u8, weight: u64) !u64 {
        if (weight == 0) return 0;

        const hashes = murmurHash3(key);
        const fingerprint: u8 = @truncate(hashes.h1 % 256);

        // Update counters in hash table using double hashing
        // Apply weight incrementally to properly handle decay
        var w: u64 = 0;
        while (w < weight) : (w += 1) {
            for (0..self.depth) |d| {
                const h1 = hashes.h1 % self.width;
                const h2 = hashes.h2 % self.width;
                const bucket_idx = (h1 +% (@as(u64, @intCast(d)) *% h2)) % self.width;

                var cell = &self.hash_table[d][bucket_idx];

                if (cell.counter == 0) {
                    // Empty cell, claim it
                    cell.fingerprint = fingerprint;
                    cell.counter = 1;
                } else if (cell.fingerprint == fingerprint) {
                    // Same fingerprint, increment
                    cell.counter += 1;
                } else {
                    // Different fingerprint, probabilistic decay
                    const decay_prob = std.math.pow(f64, self.decay, @as(f64, @floatFromInt(cell.counter)));
                    const rand_val = self.prng.random().float(f64);

                    if (rand_val < decay_prob) {
                        // Decrement counter
                        cell.counter -= 1;
                        if (cell.counter == 0) {
                            // Cell freed, claim it
                            cell.fingerprint = fingerprint;
                            cell.counter = 1;
                        }
                    }
                }
            }
        }

        // Query to get current count estimate
        const count = self.query(key);

        // Update heap
        try self.updateHeap(key, count);

        return count;
    }

    /// Update the min heap with the new item count
    fn updateHeap(self: *HeavyKeeper, key: []const u8, count: u64) !void {
        // Check if item already in heap
        for (self.heap.items) |*heap_item| {
            if (std.mem.eql(u8, heap_item.key, key)) {
                // Update count and restore heap property
                heap_item.count = count;
                try self.heapifyDown(0);
                return;
            }
        }

        // Item not in heap
        if (self.heap.items.len < self.k) {
            // Heap not full, add item
            const new_item = MinHeapItem{
                .key = try self.allocator.dupe(u8, key),
                .count = count,
            };
            try self.heap.append(self.allocator, new_item);
            try self.heapifyUp(self.heap.items.len - 1);
        } else {
            // Heap full, check if should replace minimum
            if (count > self.heap.items[0].count) {
                // Replace minimum
                var old_item = self.heap.items[0];
                old_item.deinit(self.allocator);

                self.heap.items[0] = MinHeapItem{
                    .key = try self.allocator.dupe(u8, key),
                    .count = count,
                };
                try self.heapifyDown(0);
            }
        }
    }

    /// Restore min heap property by bubbling up from index
    fn heapifyUp(self: *HeavyKeeper, start_idx: usize) !void {
        var idx = start_idx;
        while (idx > 0) {
            const parent_idx = (idx - 1) / 2;
            if (self.heap.items[idx].count >= self.heap.items[parent_idx].count) {
                break;
            }
            // Swap with parent
            const temp = self.heap.items[idx];
            self.heap.items[idx] = self.heap.items[parent_idx];
            self.heap.items[parent_idx] = temp;
            idx = parent_idx;
        }
    }

    /// Restore min heap property by bubbling down from index
    fn heapifyDown(self: *HeavyKeeper, start_idx: usize) !void {
        var idx = start_idx;
        const len = self.heap.items.len;

        while (true) {
            var smallest = idx;
            const left = 2 * idx + 1;
            const right = 2 * idx + 2;

            if (left < len and self.heap.items[left].count < self.heap.items[smallest].count) {
                smallest = left;
            }
            if (right < len and self.heap.items[right].count < self.heap.items[smallest].count) {
                smallest = right;
            }

            if (smallest == idx) {
                break;
            }

            // Swap with smallest child
            const temp = self.heap.items[idx];
            self.heap.items[idx] = self.heap.items[smallest];
            self.heap.items[smallest] = temp;
            idx = smallest;
        }
    }

    /// Query the estimated frequency for a key
    /// Returns 0 if the key is not tracked
    pub fn query(self: *HeavyKeeper, key: []const u8) u64 {
        const hashes = murmurHash3(key);
        const fingerprint: u8 = @truncate(hashes.h1 % 256);

        var min_count: u64 = std.math.maxInt(u64);
        for (0..self.depth) |d| {
            const h1 = hashes.h1 % self.width;
            const h2 = hashes.h2 % self.width;
            const bucket_idx = (h1 +% (@as(u64, @intCast(d)) *% h2)) % self.width;

            const cell = &self.hash_table[d][bucket_idx];
            if (cell.fingerprint == fingerprint) {
                min_count = @min(min_count, cell.counter);
            }
        }

        // If item not found in hash table, return 0
        if (min_count == std.math.maxInt(u64)) {
            return 0;
        }

        return min_count;
    }

    /// Item in the top-K list with frequency estimate
    pub const HotkeyItem = struct {
        key: []const u8,
        count: u64,
    };

    /// Get the list of top-k items sorted by frequency (descending)
    /// Caller owns the returned slice and must free it
    pub fn list(self: *HeavyKeeper, allocator: std.mem.Allocator) ![]HotkeyItem {
        // Copy heap items to a temporary array for sorting
        var items = try std.ArrayList(HotkeyItem).initCapacity(allocator, self.heap.items.len);
        errdefer items.deinit(allocator);

        for (self.heap.items) |heap_item| {
            try items.append(allocator, .{
                .key = heap_item.key,
                .count = heap_item.count,
            });
        }

        // Sort by count descending
        const SortContext = struct {
            pub fn lessThan(_: void, a: HotkeyItem, b: HotkeyItem) bool {
                return a.count > b.count; // Descending order
            }
        };
        std.mem.sort(HotkeyItem, items.items, {}, SortContext.lessThan);

        return try items.toOwnedSlice(allocator);
    }

    /// Metadata about the HeavyKeeper filter
    pub const HeavyKeeperInfo = struct {
        k: u32,
        width: u32,
        depth: u32,
        decay: f64,
    };

    /// Get metadata about this HeavyKeeper filter
    pub fn getInfo(self: *HeavyKeeper) HeavyKeeperInfo {
        return .{
            .k = self.k,
            .width = self.width,
            .depth = self.depth,
            .decay = self.decay,
        };
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "HeavyKeeper: init with valid parameters" {
    const allocator = std.testing.allocator;
    var hk = try HeavyKeeper.init(allocator, 10, 8, 7, 0.9);
    defer hk.deinit();

    try std.testing.expectEqual(@as(u32, 10), hk.k);
    try std.testing.expectEqual(@as(u32, 8), hk.width);
    try std.testing.expectEqual(@as(u32, 7), hk.depth);
    try std.testing.expectEqual(@as(f64, 0.9), hk.decay);
}

test "HeavyKeeper: init rejects invalid k (zero)" {
    const allocator = std.testing.allocator;
    const result = HeavyKeeper.init(allocator, 0, 8, 7, 0.9);
    try std.testing.expectError(HeavyKeeperError.InvalidK, result);
}

test "HeavyKeeper: init rejects invalid width (zero)" {
    const allocator = std.testing.allocator;
    const result = HeavyKeeper.init(allocator, 10, 0, 7, 0.9);
    try std.testing.expectError(HeavyKeeperError.InvalidWidth, result);
}

test "HeavyKeeper: init rejects invalid depth (zero)" {
    const allocator = std.testing.allocator;
    const result = HeavyKeeper.init(allocator, 10, 8, 0, 0.9);
    try std.testing.expectError(HeavyKeeperError.InvalidDepth, result);
}

test "HeavyKeeper: init rejects invalid decay (zero)" {
    const allocator = std.testing.allocator;
    const result = HeavyKeeper.init(allocator, 10, 8, 7, 0.0);
    try std.testing.expectError(HeavyKeeperError.InvalidDecay, result);
}

test "HeavyKeeper: init rejects invalid decay (one)" {
    const allocator = std.testing.allocator;
    const result = HeavyKeeper.init(allocator, 10, 8, 7, 1.0);
    try std.testing.expectError(HeavyKeeperError.InvalidDecay, result);
}

test "HeavyKeeper: init rejects invalid decay (negative)" {
    const allocator = std.testing.allocator;
    const result = HeavyKeeper.init(allocator, 10, 8, 7, -0.5);
    try std.testing.expectError(HeavyKeeperError.InvalidDecay, result);
}

test "HeavyKeeper: add single item with weight 1" {
    const allocator = std.testing.allocator;
    var hk = try HeavyKeeper.init(allocator, 10, 8, 7, 0.9);
    defer hk.deinit();

    const freq = try hk.add("user:1234", 1);
    try std.testing.expect(freq >= 1);
}

test "HeavyKeeper: add single item with custom weight" {
    const allocator = std.testing.allocator;
    var hk = try HeavyKeeper.init(allocator, 10, 8, 7, 0.9);
    defer hk.deinit();

    const freq = try hk.add("user:5678", 10);
    try std.testing.expect(freq >= 10);
}

test "HeavyKeeper: add multiple distinct items" {
    const allocator = std.testing.allocator;
    var hk = try HeavyKeeper.init(allocator, 10, 8, 7, 0.9);
    defer hk.deinit();

    _ = try hk.add("key1", 1);
    _ = try hk.add("key2", 1);
    _ = try hk.add("key3", 1);

    try std.testing.expect(hk.query("key1") >= 1);
    try std.testing.expect(hk.query("key2") >= 1);
    try std.testing.expect(hk.query("key3") >= 1);
}

test "HeavyKeeper: add same item multiple times accumulates count" {
    const allocator = std.testing.allocator;
    var hk = try HeavyKeeper.init(allocator, 10, 8, 7, 0.9);
    defer hk.deinit();

    _ = try hk.add("hotkey", 1);
    const count1 = hk.query("hotkey");
    _ = try hk.add("hotkey", 1);
    const count2 = hk.query("hotkey");
    _ = try hk.add("hotkey", 1);
    const count3 = hk.query("hotkey");

    try std.testing.expect(count2 >= count1);
    try std.testing.expect(count3 >= count2);
}

test "HeavyKeeper: query returns correct frequency estimate" {
    const allocator = std.testing.allocator;
    var hk = try HeavyKeeper.init(allocator, 10, 8, 7, 0.9);
    defer hk.deinit();

    _ = try hk.add("mykey", 5);
    const freq = hk.query("mykey");
    try std.testing.expect(freq >= 5);
}

test "HeavyKeeper: query non-existent key returns 0" {
    const allocator = std.testing.allocator;
    var hk = try HeavyKeeper.init(allocator, 10, 8, 7, 0.9);
    defer hk.deinit();

    const freq = hk.query("nonexistent");
    try std.testing.expectEqual(@as(u64, 0), freq);
}

test "HeavyKeeper: list returns empty array for empty tracker" {
    const allocator = std.testing.allocator;
    var hk = try HeavyKeeper.init(allocator, 10, 8, 7, 0.9);
    defer hk.deinit();

    const items = try hk.list(allocator);
    defer allocator.free(items);

    try std.testing.expectEqual(@as(usize, 0), items.len);
}

test "HeavyKeeper: list returns top-K items sorted by frequency descending" {
    const allocator = std.testing.allocator;
    var hk = try HeavyKeeper.init(allocator, 3, 8, 7, 0.9);
    defer hk.deinit();

    // Add items with different frequencies
    for (0..20) |_| _ = try hk.add("hottest", 1);
    for (0..10) |_| _ = try hk.add("medium", 1);
    for (0..5) |_| _ = try hk.add("warm", 1);
    for (0..2) |_| _ = try hk.add("cold", 1);

    const items = try hk.list(allocator);
    defer allocator.free(items);

    // Should have at most k items
    try std.testing.expect(items.len <= 3);

    // Verify descending order
    if (items.len >= 2) {
        try std.testing.expect(items[0].count >= items[1].count);
    }
    if (items.len >= 3) {
        try std.testing.expect(items[1].count >= items[2].count);
    }
}

test "HeavyKeeper: maintains exactly k items when overflow" {
    const allocator = std.testing.allocator;
    var hk = try HeavyKeeper.init(allocator, 5, 8, 7, 0.9);
    defer hk.deinit();

    // Add more than k distinct items
    for (0..10) |i| {
        var buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&buf, "key{d}", .{i});
        _ = try hk.add(key, 1);
    }

    const items = try hk.list(allocator);
    defer allocator.free(items);

    try std.testing.expect(items.len <= 5);
}

test "HeavyKeeper: getInfo returns correct metadata" {
    const allocator = std.testing.allocator;
    var hk = try HeavyKeeper.init(allocator, 20, 16, 9, 0.85);
    defer hk.deinit();

    const info = hk.getInfo();
    try std.testing.expectEqual(@as(u32, 20), info.k);
    try std.testing.expectEqual(@as(u32, 16), info.width);
    try std.testing.expectEqual(@as(u32, 9), info.depth);
    try std.testing.expectEqual(@as(f64, 0.85), info.decay);
}

test "HeavyKeeper: memory safety with testing allocator" {
    const allocator = std.testing.allocator;
    var hk = try HeavyKeeper.init(allocator, 10, 8, 7, 0.9);
    defer hk.deinit();

    // Add various items
    _ = try hk.add("key1", 1);
    _ = try hk.add("key2", 5);
    _ = try hk.add("key3", 10);

    const items = try hk.list(allocator);
    defer allocator.free(items);

    // Testing allocator will catch leaks
}

test "HeavyKeeper: edge case k=1 maintains single top item" {
    const allocator = std.testing.allocator;
    var hk = try HeavyKeeper.init(allocator, 1, 8, 7, 0.9);
    defer hk.deinit();

    for (0..10) |_| _ = try hk.add("frequent", 1);
    for (0..5) |_| _ = try hk.add("rare", 1);

    const items = try hk.list(allocator);
    defer allocator.free(items);

    try std.testing.expect(items.len <= 1);
    if (items.len == 1) {
        try std.testing.expect(std.mem.eql(u8, items[0].key, "frequent"));
    }
}

test "HeavyKeeper: edge case width=1 single bucket per row" {
    const allocator = std.testing.allocator;
    var hk = try HeavyKeeper.init(allocator, 10, 1, 7, 0.9);
    defer hk.deinit();

    _ = try hk.add("key1", 1);
    _ = try hk.add("key2", 1);

    try std.testing.expect(hk.query("key1") >= 1);
    try std.testing.expect(hk.query("key2") >= 1);
}

test "HeavyKeeper: binary-safe keys" {
    const allocator = std.testing.allocator;
    var hk = try HeavyKeeper.init(allocator, 10, 8, 7, 0.9);
    defer hk.deinit();

    const binary_key = &[_]u8{ 0x00, 0xFF, 0x01, 0xFE };
    _ = try hk.add(binary_key, 5);
    const freq = hk.query(binary_key);
    try std.testing.expect(freq >= 5);
}
