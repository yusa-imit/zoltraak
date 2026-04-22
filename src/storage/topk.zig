const std = @import("std");

/// Error set for Top-K operations
pub const TopKError = error{
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

/// Heap item for maintaining Top-K elements
const HeapItem = struct {
    item: []u8,
    count: u64,
    fingerprint: u8,

    fn deinit(self: *HeapItem, allocator: std.mem.Allocator) void {
        allocator.free(self.item);
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

/// Top-K probabilistic data structure using HeavyKeeper algorithm
/// Tracks the k most frequent items in a stream with probabilistic guarantees
pub const TopKValue = struct {
    /// Allocator for memory management
    allocator: std.mem.Allocator,

    /// Number of top items to track
    k: u32,

    /// Width of hash table (buckets per row)
    width: u32,

    /// Depth of hash table (number of rows)
    depth: u32,

    /// Decay parameter for exponential decay (0 < decay < 1)
    decay: f64,

    /// Hash table: [depth][width] 2D array of hash cells
    hash_table: [][]HashCell,

    /// Min heap maintaining top-k items
    /// Invariant: heap[parent].count <= heap[child].count
    heap: std.ArrayList(HeapItem),

    /// Random number generator for probabilistic decay
    prng: std.Random.DefaultPrng,

    /// Optional expiration timestamp (null if never expires)
    expires_at: ?i64,

    /// Initialize a new Top-K structure with specified parameters
    /// Parameters:
    /// - k: number of top items to track (must be > 0)
    /// - width: number of buckets per row (default: 8)
    /// - depth: number of hash table rows (default: 7)
    /// - decay: exponential decay parameter (must be 0 < decay < 1, default: 0.9)
    pub fn init(allocator: std.mem.Allocator, k: u32, width: u32, depth: u32, decay: f64) !TopKValue {
        if (k == 0) return TopKError.InvalidK;
        if (width == 0) return TopKError.InvalidWidth;
        if (depth == 0) return TopKError.InvalidDepth;
        if (decay <= 0.0 or decay >= 1.0) return TopKError.InvalidDecay;

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

        return TopKValue{
            .allocator = allocator,
            .k = k,
            .width = width,
            .depth = depth,
            .decay = decay,
            .hash_table = hash_table,
            .heap = std.ArrayList(HeapItem){},
            .prng = std.Random.DefaultPrng.init(0),
            .expires_at = null,
        };
    }

    /// Free all memory used by this Top-K structure
    pub fn deinit(self: *TopKValue) void {
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

    /// Add an item to the Top-K structure
    /// Returns the expelled item (if any) when heap is full and minimum is replaced
    /// Uses HeavyKeeper algorithm with exponential decay
    pub fn add(self: *TopKValue, item: []const u8) !?[]const u8 {
        const hashes = murmurHash3(item);
        const fingerprint: u8 = @truncate(hashes.h1 % 256);

        // Update counters in hash table using double hashing
        var min_count: u64 = std.math.maxInt(u64);
        for (0..self.depth) |d| {
            const h1 = hashes.h1 % self.width;
            const h2 = hashes.h2 % self.width;
            const bucket_idx = (h1 +% (@as(u64, @intCast(d)) *% h2)) % self.width;

            var cell = &self.hash_table[d][bucket_idx];

            if (cell.counter == 0) {
                // Empty cell, claim it
                cell.fingerprint = fingerprint;
                cell.counter = 1;
                min_count = @min(min_count, 1);
            } else if (cell.fingerprint == fingerprint) {
                // Same fingerprint, increment
                cell.counter += 1;
                min_count = @min(min_count, cell.counter);
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
                        min_count = @min(min_count, 1);
                    } else {
                        min_count = @min(min_count, cell.counter);
                    }
                } else {
                    min_count = @min(min_count, cell.counter);
                }
            }
        }

        // Update heap
        return try self.updateHeap(item, min_count, fingerprint);
    }

    /// Update the min heap with the new item count
    /// Returns expelled item if heap was full and minimum was replaced
    fn updateHeap(self: *TopKValue, item: []const u8, item_count: u64, fingerprint: u8) !?[]const u8 {
        // Check if item already in heap
        for (self.heap.items) |*heap_item| {
            if (std.mem.eql(u8, heap_item.item, item)) {
                // Update count and restore heap property
                heap_item.count = item_count;
                try self.heapifyDown(0);
                return null;
            }
        }

        // Item not in heap
        if (self.heap.items.len < self.k) {
            // Heap not full, add item
            const new_item = HeapItem{
                .item = try self.allocator.dupe(u8, item),
                .count = item_count,
                .fingerprint = fingerprint,
            };
            try self.heap.append(self.allocator, new_item);
            try self.heapifyUp(self.heap.items.len - 1);
            return null;
        } else {
            // Heap full, check if should replace minimum
            if (item_count > self.heap.items[0].count) {
                // Replace minimum
                var old_item = self.heap.items[0];
                const expelled = try self.allocator.dupe(u8, old_item.item);
                old_item.deinit(self.allocator);

                self.heap.items[0] = HeapItem{
                    .item = try self.allocator.dupe(u8, item),
                    .count = item_count,
                    .fingerprint = fingerprint,
                };
                try self.heapifyDown(0);
                return expelled;
            }
        }

        return null;
    }

    /// Restore min heap property by bubbling up from index
    fn heapifyUp(self: *TopKValue, start_idx: usize) !void {
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
    fn heapifyDown(self: *TopKValue, start_idx: usize) !void {
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

    /// Query if an item is in the Top-K
    /// Returns true if item is in the min heap
    pub fn query(self: *TopKValue, item: []const u8) bool {
        for (self.heap.items) |heap_item| {
            if (std.mem.eql(u8, heap_item.item, item)) {
                return true;
            }
        }
        return false;
    }

    /// Get the estimated count for an item
    /// Returns the minimum count across all hash table rows
    pub fn count(self: *TopKValue, item: []const u8) u64 {
        const hashes = murmurHash3(item);
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
};

// ============================================================================
// Unit Tests
// ============================================================================

test "TopKValue: init with valid parameters" {
    const allocator = std.testing.allocator;
    var topk = try TopKValue.init(allocator, 3, 8, 7, 0.9);
    defer topk.deinit();

    try std.testing.expectEqual(@as(u32, 3), topk.k);
    try std.testing.expectEqual(@as(u32, 8), topk.width);
    try std.testing.expectEqual(@as(u32, 7), topk.depth);
    try std.testing.expectEqual(@as(f64, 0.9), topk.decay);
    try std.testing.expectEqual(@as(usize, 0), topk.heap.items.len);
}

test "TopKValue: init rejects invalid k" {
    const allocator = std.testing.allocator;
    const result = TopKValue.init(allocator, 0, 8, 7, 0.9);
    try std.testing.expectError(TopKError.InvalidK, result);
}

test "TopKValue: init rejects invalid width" {
    const allocator = std.testing.allocator;
    const result = TopKValue.init(allocator, 3, 0, 7, 0.9);
    try std.testing.expectError(TopKError.InvalidWidth, result);
}

test "TopKValue: init rejects invalid depth" {
    const allocator = std.testing.allocator;
    const result = TopKValue.init(allocator, 3, 8, 0, 0.9);
    try std.testing.expectError(TopKError.InvalidDepth, result);
}

test "TopKValue: init rejects invalid decay (zero)" {
    const allocator = std.testing.allocator;
    const result = TopKValue.init(allocator, 3, 8, 7, 0.0);
    try std.testing.expectError(TopKError.InvalidDecay, result);
}

test "TopKValue: init rejects invalid decay (one)" {
    const allocator = std.testing.allocator;
    const result = TopKValue.init(allocator, 3, 8, 7, 1.0);
    try std.testing.expectError(TopKError.InvalidDecay, result);
}

test "TopKValue: init rejects invalid decay (negative)" {
    const allocator = std.testing.allocator;
    const result = TopKValue.init(allocator, 3, 8, 7, -0.5);
    try std.testing.expectError(TopKError.InvalidDecay, result);
}

test "TopKValue: add single item" {
    const allocator = std.testing.allocator;
    var topk = try TopKValue.init(allocator, 3, 8, 7, 0.9);
    defer topk.deinit();

    const expelled = try topk.add("apple");
    try std.testing.expectEqual(@as(?[]const u8, null), expelled);
    try std.testing.expectEqual(@as(usize, 1), topk.heap.items.len);
    try std.testing.expect(topk.query("apple"));
}

test "TopKValue: add multiple items within k" {
    const allocator = std.testing.allocator;
    var topk = try TopKValue.init(allocator, 3, 8, 7, 0.9);
    defer topk.deinit();

    _ = try topk.add("apple");
    _ = try topk.add("banana");
    _ = try topk.add("cherry");

    try std.testing.expectEqual(@as(usize, 3), topk.heap.items.len);
    try std.testing.expect(topk.query("apple"));
    try std.testing.expect(topk.query("banana"));
    try std.testing.expect(topk.query("cherry"));
}

test "TopKValue: query non-existent item" {
    const allocator = std.testing.allocator;
    var topk = try TopKValue.init(allocator, 3, 8, 7, 0.9);
    defer topk.deinit();

    _ = try topk.add("apple");
    try std.testing.expect(!topk.query("banana"));
}

test "TopKValue: count returns minimum across rows" {
    const allocator = std.testing.allocator;
    var topk = try TopKValue.init(allocator, 3, 8, 7, 0.9);
    defer topk.deinit();

    _ = try topk.add("apple");
    const c = topk.count("apple");
    try std.testing.expect(c >= 1);
}

test "TopKValue: count for non-existent item returns 0" {
    const allocator = std.testing.allocator;
    var topk = try TopKValue.init(allocator, 3, 8, 7, 0.9);
    defer topk.deinit();

    const c = topk.count("nonexistent");
    try std.testing.expectEqual(@as(u64, 0), c);
}

test "TopKValue: add duplicate increments count" {
    const allocator = std.testing.allocator;
    var topk = try TopKValue.init(allocator, 3, 8, 7, 0.9);
    defer topk.deinit();

    _ = try topk.add("apple");
    const count1 = topk.count("apple");
    _ = try topk.add("apple");
    const count2 = topk.count("apple");

    try std.testing.expect(count2 >= count1);
    try std.testing.expectEqual(@as(usize, 1), topk.heap.items.len);
}

test "TopKValue: heap maintains top-k items" {
    const allocator = std.testing.allocator;
    var topk = try TopKValue.init(allocator, 2, 8, 7, 0.9);
    defer topk.deinit();

    // Add items with known frequencies
    for (0..10) |_| _ = try topk.add("frequent1");
    for (0..8) |_| _ = try topk.add("frequent2");
    for (0..2) |_| _ = try topk.add("rare");

    try std.testing.expectEqual(@as(usize, 2), topk.heap.items.len);
    try std.testing.expect(topk.query("frequent1"));
    try std.testing.expect(topk.query("frequent2"));
}

test "MurmurHash3: consistent hashing" {
    const h1 = murmurHash3("test");
    const h2 = murmurHash3("test");
    try std.testing.expectEqual(h1.h1, h2.h1);
    try std.testing.expectEqual(h1.h2, h2.h2);
}

test "MurmurHash3: different inputs produce different hashes" {
    const h1 = murmurHash3("test1");
    const h2 = murmurHash3("test2");
    try std.testing.expect(h1.h1 != h2.h1 or h1.h2 != h2.h2);
}
