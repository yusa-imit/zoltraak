const std = @import("std");
const Allocator = std.mem.Allocator;

/// Distance metric for vector similarity search
pub const DistanceMetric = enum {
    l2, // Euclidean distance
    ip, // Inner product
    cosine, // Cosine similarity

    pub fn fromString(s: []const u8) !DistanceMetric {
        if (std.ascii.eqlIgnoreCase(s, "l2")) return .l2;
        if (std.ascii.eqlIgnoreCase(s, "ip")) return .ip;
        if (std.ascii.eqlIgnoreCase(s, "cosine")) return .cosine;
        return error.InvalidMetric;
    }

    pub fn toString(self: DistanceMetric) []const u8 {
        return switch (self) {
            .l2 => "L2",
            .ip => "IP",
            .cosine => "COSINE",
        };
    }
};

/// Quantization type for vector storage
pub const QuantizationType = enum {
    fp32, // 32-bit floating point
    fp16, // 16-bit floating point (not implemented yet)
    int8, // 8-bit integer (not implemented yet)

    pub fn fromString(s: []const u8) !QuantizationType {
        if (std.ascii.eqlIgnoreCase(s, "fp32")) return .fp32;
        if (std.ascii.eqlIgnoreCase(s, "fp16")) return .fp16;
        if (std.ascii.eqlIgnoreCase(s, "int8")) return .int8;
        return error.InvalidQuantization;
    }

    /// Returns the uppercase string representation of the quantization type
    pub fn toString(self: QuantizationType) []const u8 {
        return switch (self) {
            .fp32 => "FP32",
            .fp16 => "FP16",
            .int8 => "INT8",
        };
    }
};

/// Vector entry with embedding and optional attributes
pub const VectorEntry = struct {
    id: []const u8, // Vector identifier (member name)
    embedding: []f32, // Vector data (FP32 for now)
    attributes: std.StringHashMap([]const u8), // Optional key-value attributes
    allocator: Allocator,

    pub fn init(allocator: Allocator, id: []const u8, embedding: []const f32) !VectorEntry {
        const id_copy = try allocator.dupe(u8, id);
        errdefer allocator.free(id_copy);

        const embedding_copy = try allocator.dupe(f32, embedding);
        errdefer allocator.free(embedding_copy);

        return VectorEntry{
            .id = id_copy,
            .embedding = embedding_copy,
            .attributes = std.StringHashMap([]const u8).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *VectorEntry) void {
        self.allocator.free(self.id);
        self.allocator.free(self.embedding);

        var it = self.attributes.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.attributes.deinit();
    }

    /// Set an attribute on this vector
    pub fn setAttribute(self: *VectorEntry, key: []const u8, value: []const u8) !void {
        const key_copy = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_copy);

        const value_copy = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(value_copy);

        if (try self.attributes.fetchPut(key_copy, value_copy)) |prev| {
            self.allocator.free(prev.key);
            self.allocator.free(prev.value);
        }
    }

    /// Get an attribute from this vector
    pub fn getAttribute(self: *const VectorEntry, key: []const u8) ?[]const u8 {
        return self.attributes.get(key);
    }
};

/// Vector set data structure
pub const VectorSetValue = struct {
    vectors: std.StringHashMap(*VectorEntry), // Map of id -> VectorEntry
    dimensionality: usize, // Fixed dimensionality for all vectors
    metric: DistanceMetric, // Distance metric
    quantization: QuantizationType, // Quantization type
    allocator: Allocator,

    /// Initialize a new vector set
    pub fn init(allocator: Allocator, dim: usize, metric: DistanceMetric) !VectorSetValue {
        if (dim == 0) return error.ZeroDimensionality;

        return VectorSetValue{
            .vectors = std.StringHashMap(*VectorEntry).init(allocator),
            .dimensionality = dim,
            .metric = metric,
            .quantization = .fp32, // Default to FP32
            .allocator = allocator,
        };
    }

    /// Cleanup all resources
    pub fn deinit(self: *VectorSetValue) void {
        var it = self.vectors.valueIterator();
        while (it.next()) |entry_ptr| {
            entry_ptr.*.deinit();
            self.allocator.destroy(entry_ptr.*);
        }
        self.vectors.deinit();
    }

    /// Add a vector to the set
    pub fn add(self: *VectorSetValue, id: []const u8, embedding: []const f32) !bool {
        if (embedding.len != self.dimensionality) {
            return error.DimensionMismatch;
        }

        // Check if vector already exists
        if (self.vectors.get(id)) |existing| {
            // Update existing vector
            self.allocator.free(existing.embedding);
            existing.embedding = try self.allocator.dupe(f32, embedding);
            return false; // Existing vector updated
        }

        // Create new vector entry
        const entry = try self.allocator.create(VectorEntry);
        errdefer self.allocator.destroy(entry);

        entry.* = try VectorEntry.init(self.allocator, id, embedding);
        errdefer entry.deinit();

        try self.vectors.put(entry.id, entry);
        return true; // New vector added
    }

    /// Remove a vector from the set
    pub fn remove(self: *VectorSetValue, id: []const u8) !bool {
        if (self.vectors.fetchRemove(id)) |kv| {
            kv.value.deinit();
            self.allocator.destroy(kv.value);
            return true;
        }
        return false;
    }

    /// Get a vector by ID
    pub fn get(self: *const VectorSetValue, id: []const u8) ?*VectorEntry {
        return self.vectors.get(id);
    }

    /// Check if a vector exists
    pub fn contains(self: *const VectorSetValue, id: []const u8) bool {
        return self.vectors.contains(id);
    }

    /// Get cardinality (number of vectors)
    pub fn cardinality(self: *const VectorSetValue) usize {
        return self.vectors.count();
    }

    /// Get a random vector ID
    pub fn randomMember(self: *const VectorSetValue) ?[]const u8 {
        if (self.vectors.count() == 0) return null;

        var it = self.vectors.keyIterator();
        var count: usize = 0;
        const target = @rem(std.crypto.random.int(usize), self.vectors.count());

        while (it.next()) |key| {
            if (count == target) return key.*;
            count += 1;
        }

        return null;
    }

    /// Calculate L2 distance between two vectors
    fn l2Distance(a: []const f32, b: []const f32) f32 {
        std.debug.assert(a.len == b.len);
        var sum: f32 = 0.0;
        for (a, b) |av, bv| {
            const diff = av - bv;
            sum += diff * diff;
        }
        return @sqrt(sum);
    }

    /// Calculate inner product between two vectors
    fn innerProduct(a: []const f32, b: []const f32) f32 {
        std.debug.assert(a.len == b.len);
        var sum: f32 = 0.0;
        for (a, b) |av, bv| {
            sum += av * bv;
        }
        return sum;
    }

    /// Calculate cosine similarity between two vectors
    fn cosineSimilarity(a: []const f32, b: []const f32) f32 {
        std.debug.assert(a.len == b.len);
        var dot: f32 = 0.0;
        var norm_a: f32 = 0.0;
        var norm_b: f32 = 0.0;

        for (a, b) |av, bv| {
            dot += av * bv;
            norm_a += av * av;
            norm_b += bv * bv;
        }

        const denom = @sqrt(norm_a) * @sqrt(norm_b);
        if (denom == 0.0) return 0.0;
        return dot / denom;
    }

    /// Calculate distance between two vectors according to metric
    pub fn distance(self: *const VectorSetValue, a: []const f32, b: []const f32) f32 {
        return switch (self.metric) {
            .l2 => l2Distance(a, b),
            .ip => -innerProduct(a, b), // Negate for minimization
            .cosine => 1.0 - cosineSimilarity(a, b), // Convert similarity to distance
        };
    }
};

// ============================================================================
// Unit tests
// ============================================================================

test "VectorSetValue: init and deinit" {
    const allocator = std.testing.allocator;

    var vs = try VectorSetValue.init(allocator, 128, .l2);
    defer vs.deinit();

    try std.testing.expectEqual(@as(usize, 128), vs.dimensionality);
    try std.testing.expectEqual(DistanceMetric.l2, vs.metric);
    try std.testing.expectEqual(@as(usize, 0), vs.cardinality());
}

test "VectorSetValue: zero dimensionality rejected" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.ZeroDimensionality, VectorSetValue.init(allocator, 0, .l2));
}

test "VectorSetValue: add vector" {
    const allocator = std.testing.allocator;

    var vs = try VectorSetValue.init(allocator, 3, .l2);
    defer vs.deinit();

    const vec1 = [_]f32{ 1.0, 2.0, 3.0 };
    const added = try vs.add("vec1", &vec1);

    try std.testing.expect(added); // New vector
    try std.testing.expectEqual(@as(usize, 1), vs.cardinality());
    try std.testing.expect(vs.contains("vec1"));
}

test "VectorSetValue: dimension mismatch" {
    const allocator = std.testing.allocator;

    var vs = try VectorSetValue.init(allocator, 3, .l2);
    defer vs.deinit();

    const vec_wrong = [_]f32{ 1.0, 2.0 }; // Wrong dimensionality
    try std.testing.expectError(error.DimensionMismatch, vs.add("vec1", &vec_wrong));
}

test "VectorSetValue: update existing vector" {
    const allocator = std.testing.allocator;

    var vs = try VectorSetValue.init(allocator, 3, .l2);
    defer vs.deinit();

    const vec1 = [_]f32{ 1.0, 2.0, 3.0 };
    _ = try vs.add("vec1", &vec1);

    const vec2 = [_]f32{ 4.0, 5.0, 6.0 };
    const added = try vs.add("vec1", &vec2); // Update

    try std.testing.expect(!added); // Updated, not added
    try std.testing.expectEqual(@as(usize, 1), vs.cardinality());

    const entry = vs.get("vec1").?;
    try std.testing.expectEqual(@as(f32, 4.0), entry.embedding[0]);
}

test "VectorSetValue: remove vector" {
    const allocator = std.testing.allocator;

    var vs = try VectorSetValue.init(allocator, 3, .l2);
    defer vs.deinit();

    const vec1 = [_]f32{ 1.0, 2.0, 3.0 };
    _ = try vs.add("vec1", &vec1);

    const removed = try vs.remove("vec1");
    try std.testing.expect(removed);
    try std.testing.expectEqual(@as(usize, 0), vs.cardinality());
    try std.testing.expect(!vs.contains("vec1"));
}

test "VectorSetValue: remove nonexistent vector" {
    const allocator = std.testing.allocator;

    var vs = try VectorSetValue.init(allocator, 3, .l2);
    defer vs.deinit();

    const removed = try vs.remove("nonexistent");
    try std.testing.expect(!removed);
}

test "VectorSetValue: L2 distance" {
    const allocator = std.testing.allocator;

    var vs = try VectorSetValue.init(allocator, 3, .l2);
    defer vs.deinit();

    const a = [_]f32{ 1.0, 0.0, 0.0 };
    const b = [_]f32{ 0.0, 1.0, 0.0 };

    const dist = vs.distance(&a, &b);
    const expected = @sqrt(2.0);
    try std.testing.expectApproxEqRel(expected, dist, 0.0001);
}

test "VectorSetValue: inner product distance" {
    const allocator = std.testing.allocator;

    var vs = try VectorSetValue.init(allocator, 3, .ip);
    defer vs.deinit();

    const a = [_]f32{ 1.0, 2.0, 3.0 };
    const b = [_]f32{ 4.0, 5.0, 6.0 };

    const dist = vs.distance(&a, &b);
    // IP = 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
    // Distance = -32 (negated for minimization)
    try std.testing.expectEqual(@as(f32, -32.0), dist);
}

test "VectorSetValue: cosine distance" {
    const allocator = std.testing.allocator;

    var vs = try VectorSetValue.init(allocator, 2, .cosine);
    defer vs.deinit();

    const a = [_]f32{ 1.0, 0.0 };
    const b = [_]f32{ 1.0, 0.0 };

    const dist = vs.distance(&a, &b);
    // Identical vectors: cosine similarity = 1.0, distance = 0.0
    try std.testing.expectApproxEqRel(@as(f32, 0.0), dist, 0.0001);
}

test "VectorEntry: attributes" {
    const allocator = std.testing.allocator;

    const vec = [_]f32{ 1.0, 2.0, 3.0 };
    var entry = try VectorEntry.init(allocator, "vec1", &vec);
    defer entry.deinit();

    try entry.setAttribute("category", "test");
    try entry.setAttribute("score", "0.95");

    try std.testing.expectEqualStrings("test", entry.getAttribute("category").?);
    try std.testing.expectEqualStrings("0.95", entry.getAttribute("score").?);
    try std.testing.expect(entry.getAttribute("nonexistent") == null);
}

test "DistanceMetric: fromString" {
    try std.testing.expectEqual(DistanceMetric.l2, try DistanceMetric.fromString("l2"));
    try std.testing.expectEqual(DistanceMetric.l2, try DistanceMetric.fromString("L2"));
    try std.testing.expectEqual(DistanceMetric.ip, try DistanceMetric.fromString("ip"));
    try std.testing.expectEqual(DistanceMetric.cosine, try DistanceMetric.fromString("cosine"));
    try std.testing.expectError(error.InvalidMetric, DistanceMetric.fromString("invalid"));
}
