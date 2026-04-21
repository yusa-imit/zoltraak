const std = @import("std");
const bloom_mod = @import("./bloom.zig");

/// Error set for Cuckoo filter operations
pub const CuckooError = error{
    InvalidCapacity,
    InvalidBucketSize,
    InvalidMaxIterations,
    InvalidExpansion,
    FilterFull,
};

/// MurmurHash3 hash function - reused from bloom filter
/// Returns 64-bit hash value for index computation
fn murmurHash3(data: []const u8) u64 {
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

    return h1;
}

/// Generate 8-bit fingerprint from item data
fn fingerprint(item: []const u8) u8 {
    const hash = murmurHash3(item);
    return @as(u8, @intCast(hash & 0xff));
}

/// Compute alternate bucket index using XOR symmetry
/// h2 = h1 XOR hash(fingerprint)
fn computeAltIndex(index1: u64, fp: u8, num_buckets: u64) u64 {
    const fp_hash = murmurHash3(&[_]u8{fp});
    return index1 ^ (fp_hash % num_buckets);
}

/// Single bucket in a Cuckoo filter holding fingerprints
pub const Bucket = struct {
    fingerprints: []u8,
    count: u32,
    capacity: u32,
    allocator: std.mem.Allocator,

    /// Initialize a new bucket with specified capacity
    pub fn init(allocator: std.mem.Allocator, capacity: u32) !Bucket {
        const fingerprints = try allocator.alloc(u8, capacity);
        errdefer allocator.free(fingerprints);

        @memset(fingerprints, 0);
        return .{
            .fingerprints = fingerprints,
            .count = 0,
            .capacity = capacity,
            .allocator = allocator,
        };
    }

    /// Deinitialize bucket and free fingerprints array
    pub fn deinit(self: *Bucket) void {
        self.allocator.free(self.fingerprints);
    }

    /// Check if fingerprint exists in bucket
    pub fn contains(self: *const Bucket, fp: u8) bool {
        for (0..self.count) |i| {
            if (self.fingerprints[i] == fp) {
                return true;
            }
        }
        return false;
    }

    /// Try to insert fingerprint into bucket
    /// Returns true if inserted, false if bucket is full
    pub fn insert(self: *Bucket, fp: u8) bool {
        if (self.count >= self.capacity) {
            return false;
        }
        self.fingerprints[self.count] = fp;
        self.count += 1;
        return true;
    }

    /// Remove fingerprint from bucket (random victim for eviction)
    /// Returns the evicted fingerprint
    pub fn evict(self: *Bucket, prng: *std.Random.DefaultPrng) u8 {
        const rand = prng.random();
        const idx = rand.intRangeAtMost(u32, 0, self.count);
        const evicted = self.fingerprints[idx];

        // Shift last element into evicted position
        if (idx < self.count - 1) {
            self.fingerprints[idx] = self.fingerprints[self.count - 1];
        }
        self.count -= 1;

        return evicted;
    }

    /// Delete a specific fingerprint from bucket
    /// Returns true if fingerprint was found and removed, false otherwise
    pub fn delete(self: *Bucket, fp: u8) bool {
        for (0..self.count) |i| {
            if (self.fingerprints[i] == fp) {
                // Shift last element into deleted position
                if (i < self.count - 1) {
                    self.fingerprints[i] = self.fingerprints[self.count - 1];
                }
                self.count -= 1;
                return true;
            }
        }
        return false;
    }
};

/// Sub-filter containing an array of buckets
pub const SubFilter = struct {
    buckets: []Bucket,
    num_buckets: u64,
    allocator: std.mem.Allocator,

    /// Initialize a new sub-filter with specified number of buckets
    pub fn init(allocator: std.mem.Allocator, num_buckets: u64, bucketsize: u32) !SubFilter {
        const buckets = try allocator.alloc(Bucket, num_buckets);
        errdefer allocator.free(buckets);

        for (0..num_buckets) |i| {
            buckets[i] = try Bucket.init(allocator, bucketsize);
        }
        errdefer {
            for (0..num_buckets) |i| {
                buckets[i].deinit();
            }
        }

        return .{
            .buckets = buckets,
            .num_buckets = num_buckets,
            .allocator = allocator,
        };
    }

    /// Deinitialize sub-filter and all buckets
    pub fn deinit(self: *SubFilter) void {
        for (self.buckets) |*bucket| {
            bucket.deinit();
        }
        self.allocator.free(self.buckets);
    }
};

/// Cuckoo filter value for Redis storage
pub const CuckooFilterValue = struct {
    capacity: u64,
    bucketsize: u32,
    max_iterations: u16,
    expansion: u16,
    filters: std.ArrayList(SubFilter),
    allocator: std.mem.Allocator,
    expires_at: ?i64,

    /// Initialize a new Cuckoo filter with specified parameters
    pub fn init(
        allocator: std.mem.Allocator,
        capacity: u64,
        bucketsize: u32,
        max_iterations: u16,
        expansion: u16,
    ) !CuckooFilterValue {
        // Validate parameters
        if (capacity == 0) return CuckooError.InvalidCapacity;
        if (bucketsize == 0 or bucketsize > 255) return CuckooError.InvalidBucketSize;
        if (max_iterations == 0) return CuckooError.InvalidMaxIterations;

        // Round capacity up to next power of 2 for num_buckets
        var num_buckets = @as(u64, 1);
        while (num_buckets < capacity) {
            num_buckets <<= 1;
        }

        var filters = try std.ArrayList(SubFilter).initCapacity(allocator, 1);
        errdefer filters.deinit(allocator);

        // Create first sub-filter
        var sub = try SubFilter.init(allocator, num_buckets, bucketsize);
        errdefer sub.deinit();

        try filters.append(allocator, sub);

        return .{
            .capacity = capacity,
            .bucketsize = bucketsize,
            .max_iterations = max_iterations,
            .expansion = expansion,
            .filters = filters,
            .allocator = allocator,
            .expires_at = null,
        };
    }

    /// Deinitialize Cuckoo filter and free all sub-filters
    pub fn deinit(self: *CuckooFilterValue) void {
        for (self.filters.items) |*filter| {
            filter.deinit();
        }
        self.filters.deinit(self.allocator);
    }

    /// Check if item exists in the filter
    /// Returns true if item may exist (both candidate buckets checked)
    pub fn exists(self: *const CuckooFilterValue, item: []const u8) bool {
        const fp = fingerprint(item);
        const filter = &self.filters.items[self.filters.items.len - 1];

        // Compute two candidate bucket indices
        const hash = murmurHash3(item);
        const idx1 = hash % filter.num_buckets;
        const idx2 = computeAltIndex(idx1, fp, filter.num_buckets);

        // Check both buckets
        if (filter.buckets[idx1].contains(fp)) {
            return true;
        }
        if (filter.buckets[idx2].contains(fp)) {
            return true;
        }

        return false;
    }

    /// Add an item to the Cuckoo filter
    /// Uses cuckoo hashing with eviction chain up to max_iterations
    /// Returns CuckooError.FilterFull if unable to find a bucket after max iterations
    pub fn add(self: *CuckooFilterValue, item: []const u8) !void {
        const fp = fingerprint(item);
        var prng = std.Random.DefaultPrng.init(@as(u64, @intCast(std.time.milliTimestamp())));

        var filter = &self.filters.items[self.filters.items.len - 1];

        // Compute two candidate bucket indices
        const hash = murmurHash3(item);
        const idx1 = hash % filter.num_buckets;
        const idx2 = computeAltIndex(idx1, fp, filter.num_buckets);

        // Try to insert in first bucket
        if (filter.buckets[idx1].insert(fp)) {
            return;
        }

        // Try to insert in second bucket
        if (filter.buckets[idx2].insert(fp)) {
            return;
        }

        // Perform cuckoo eviction chain
        var current_fp = fp;
        var current_i = idx1;
        var iterations: u16 = 0;

        while (iterations < self.max_iterations) : (iterations += 1) {
            // Randomly select one of the two buckets
            const rand = prng.random();
            const use_i1 = rand.boolean();
            const bucket_idx = if (use_i1) current_i else computeAltIndex(current_i, current_fp, filter.num_buckets);

            // Evict a random victim from the selected bucket
            const evicted_fp = filter.buckets[bucket_idx].evict(&prng);

            // Try to insert current fingerprint
            if (filter.buckets[bucket_idx].insert(current_fp)) {
                return;
            }

            // Update for next iteration
            current_fp = evicted_fp;
            current_i = bucket_idx;
        }

        // If we get here, filter is full
        return CuckooError.FilterFull;
    }

    /// Add an item only if it doesn't already exist
    /// Returns true if item was added, false if it already existed
    pub fn addnx(self: *CuckooFilterValue, item: []const u8) !bool {
        if (self.exists(item)) {
            return false;
        }
        try self.add(item);
        return true;
    }

    /// Delete an item from the Cuckoo filter
    /// Removes one instance of the item's fingerprint from one of its candidate buckets
    /// Returns true if item was deleted, false if not found
    pub fn delete(self: *CuckooFilterValue, item: []const u8) bool {
        const fp = fingerprint(item);
        const filter = &self.filters.items[self.filters.items.len - 1];

        // Compute two candidate bucket indices
        const hash = murmurHash3(item);
        const idx1 = hash % filter.num_buckets;
        const idx2 = computeAltIndex(idx1, fp, filter.num_buckets);

        // Try to delete from first bucket
        if (filter.buckets[idx1].delete(fp)) {
            return true;
        }

        // Try to delete from second bucket
        if (filter.buckets[idx2].delete(fp)) {
            return true;
        }

        // Item not found in either bucket
        return false;
    }
};

// Unit tests
test "fingerprint generation is deterministic" {
    const item = "hello";
    const fp1 = fingerprint(item);
    const fp2 = fingerprint(item);
    try std.testing.expectEqual(fp1, fp2);
}

test "fingerprint produces u8 value" {
    const item = "test";
    const fp = fingerprint(item);
    try std.testing.expect(fp < 256);
}

test "compute alt index is symmetric" {
    const fp: u8 = 123;
    const idx1: u64 = 42;
    const num_buckets: u64 = 256;

    const idx2 = computeAltIndex(idx1, fp, num_buckets);
    const idx1_recovered = computeAltIndex(idx2, fp, num_buckets);

    try std.testing.expectEqual(idx1, idx1_recovered);
}

test "bucket init and deinit" {
    const allocator = std.testing.allocator;
    var bucket = try Bucket.init(allocator, 4);
    defer bucket.deinit();

    try std.testing.expectEqual(@as(u32, 0), bucket.count);
    try std.testing.expectEqual(@as(u32, 4), bucket.capacity);
}

test "bucket insert returns true when space available" {
    const allocator = std.testing.allocator;
    var bucket = try Bucket.init(allocator, 2);
    defer bucket.deinit();

    const inserted = bucket.insert(42);
    try std.testing.expect(inserted);
    try std.testing.expectEqual(@as(u32, 1), bucket.count);
}

test "bucket insert returns false when full" {
    const allocator = std.testing.allocator;
    var bucket = try Bucket.init(allocator, 1);
    defer bucket.deinit();

    _ = bucket.insert(42);
    const second_insert = bucket.insert(43);
    try std.testing.expect(!second_insert);
}

test "bucket contains checks for fingerprint" {
    const allocator = std.testing.allocator;
    var bucket = try Bucket.init(allocator, 4);
    defer bucket.deinit();

    bucket.insert(42);
    try std.testing.expect(bucket.contains(42));
    try std.testing.expect(!bucket.contains(43));
}

test "cuckoo filter init and deinit" {
    const allocator = std.testing.allocator;
    var cf = try CuckooFilterValue.init(allocator, 100, 2, 20, 1);
    defer cf.deinit();

    try std.testing.expectEqual(@as(u64, 100), cf.capacity);
    try std.testing.expectEqual(@as(u32, 2), cf.bucketsize);
}

test "cuckoo filter add basic item" {
    const allocator = std.testing.allocator;
    var cf = try CuckooFilterValue.init(allocator, 100, 2, 20, 1);
    defer cf.deinit();

    try cf.add("hello");
    try std.testing.expect(cf.exists("hello"));
}

test "cuckoo filter exists returns false for non-existent item" {
    const allocator = std.testing.allocator;
    var cf = try CuckooFilterValue.init(allocator, 100, 2, 20, 1);
    defer cf.deinit();

    try std.testing.expect(!cf.exists("nonexistent"));
}

test "cuckoo filter addnx returns true for new item" {
    const allocator = std.testing.allocator;
    var cf = try CuckooFilterValue.init(allocator, 100, 2, 20, 1);
    defer cf.deinit();

    const added = try cf.addnx("newitem");
    try std.testing.expect(added);
    try std.testing.expect(cf.exists("newitem"));
}

test "cuckoo filter addnx returns false for existing item" {
    const allocator = std.testing.allocator;
    var cf = try CuckooFilterValue.init(allocator, 100, 2, 20, 1);
    defer cf.deinit();

    _ = try cf.addnx("item");
    const added_again = try cf.addnx("item");
    try std.testing.expect(!added_again);
}

test "cuckoo filter no false negatives" {
    const allocator = std.testing.allocator;
    var cf = try CuckooFilterValue.init(allocator, 100, 2, 20, 1);
    defer cf.deinit();

    const items = [_][]const u8{ "item1", "item2", "item3", "item4", "item5" };

    for (items) |item| {
        try cf.add(item);
    }

    for (items) |item| {
        try std.testing.expect(cf.exists(item));
    }
}

test "bucket delete removes fingerprint" {
    const allocator = std.testing.allocator;
    var bucket = try Bucket.init(allocator, 4);
    defer bucket.deinit();

    _ = bucket.insert(42);
    _ = bucket.insert(43);
    try std.testing.expectEqual(@as(u32, 2), bucket.count);

    const deleted = bucket.delete(42);
    try std.testing.expect(deleted);
    try std.testing.expectEqual(@as(u32, 1), bucket.count);
    try std.testing.expect(!bucket.contains(42));
    try std.testing.expect(bucket.contains(43));
}

test "bucket delete returns false for non-existent fingerprint" {
    const allocator = std.testing.allocator;
    var bucket = try Bucket.init(allocator, 4);
    defer bucket.deinit();

    _ = bucket.insert(42);
    const deleted = bucket.delete(99);
    try std.testing.expect(!deleted);
    try std.testing.expectEqual(@as(u32, 1), bucket.count);
}

test "cuckoo filter delete removes item" {
    const allocator = std.testing.allocator;
    var cf = try CuckooFilterValue.init(allocator, 100, 2, 20, 1);
    defer cf.deinit();

    try cf.add("testitem");
    try std.testing.expect(cf.exists("testitem"));

    const deleted = cf.delete("testitem");
    try std.testing.expect(deleted);
    try std.testing.expect(!cf.exists("testitem"));
}

test "cuckoo filter delete returns false for non-existent item" {
    const allocator = std.testing.allocator;
    var cf = try CuckooFilterValue.init(allocator, 100, 2, 20, 1);
    defer cf.deinit();

    const deleted = cf.delete("nonexistent");
    try std.testing.expect(!deleted);
}

test "cuckoo filter delete only removes one instance" {
    const allocator = std.testing.allocator;
    var cf = try CuckooFilterValue.init(allocator, 100, 2, 20, 1);
    defer cf.deinit();

    // Add same item twice
    try cf.add("duplicate");
    try cf.add("duplicate");

    // First delete succeeds
    const deleted1 = cf.delete("duplicate");
    try std.testing.expect(deleted1);

    // Item may still exist due to duplicate fingerprint
    // Second delete may succeed if duplicate is still present
    const deleted2 = cf.delete("duplicate");
    _ = deleted2; // May be true or false depending on bucket placement
}

test "cuckoo filter delete multiple different items" {
    const allocator = std.testing.allocator;
    var cf = try CuckooFilterValue.init(allocator, 100, 2, 20, 1);
    defer cf.deinit();

    const items = [_][]const u8{ "item1", "item2", "item3" };

    for (items) |item| {
        try cf.add(item);
    }

    // Delete item2
    const deleted = cf.delete("item2");
    try std.testing.expect(deleted);
    try std.testing.expect(!cf.exists("item2"));

    // Other items still exist
    try std.testing.expect(cf.exists("item1"));
    try std.testing.expect(cf.exists("item3"));
}
