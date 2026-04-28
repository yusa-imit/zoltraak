const std = @import("std");
const Allocator = std.mem.Allocator;
const Storage = @import("../storage/memory.zig").Storage;
const Value = @import("../storage/memory.zig").Value;
const VectorSetValue = @import("../storage/vector.zig").VectorSetValue;
const DistanceMetric = @import("../storage/vector.zig").DistanceMetric;
const QuantizationType = @import("../storage/vector.zig").QuantizationType;
const RespValue = @import("../protocol/parser.zig").RespValue;

/// VADD key dimensionality metric element [element ...]
/// Add one or more vectors to a vector set.
/// Each element is: <id> <embedding_values...>
/// Returns the number of newly added vectors (existing vectors are updated, not counted).
pub fn cmdVadd(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    if (args.len < 5) return RespValue{ .error_string = "ERR wrong number of arguments for 'vadd' command" };

    const key = args[1];
    const dim_str = args[2];
    const metric_str = args[3];

    // Parse dimensionality
    const dimensionality = std.fmt.parseInt(usize, dim_str, 10) catch {
        return RespValue{ .error_string = "ERR invalid dimensionality" };
    };
    if (dimensionality == 0) {
        return RespValue{ .error_string = "ERR dimensionality must be positive" };
    }

    // Parse distance metric
    const metric = DistanceMetric.fromString(metric_str) catch {
        return RespValue{ .error_string = "ERR invalid distance metric, must be L2, IP, or COSINE" };
    };

    // Each element requires: id + dimensionality values
    const element_size = 1 + dimensionality;
    const elements_data = args[4..];
    if (elements_data.len % element_size != 0) {
        return RespValue{ .error_string = "ERR syntax error, element format is: <id> <f32> [<f32> ...]" };
    }

    const num_elements = elements_data.len / element_size;
    if (num_elements == 0) {
        return RespValue{ .error_string = "ERR at least one vector required" };
    }

    // Get or create the vector set
    const result = try storage.data.getOrPut(key);
    if (!result.found_existing) {
        // Create new vector set
        const key_copy = try allocator.dupe(u8, key);
        errdefer allocator.free(key_copy);

        var new_vs = try VectorSetValue.init(allocator, dimensionality, metric);
        errdefer new_vs.deinit();

        result.key_ptr.* = key_copy;
        result.value_ptr.* = Value{ .vector_set = new_vs };
    } else {
        // Validate existing key is a vector set
        if (result.value_ptr.* != .vector_set) {
            return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
        }

        // Validate dimensionality and metric match
        const existing_vs = &result.value_ptr.vector_set;
        if (existing_vs.dimensionality != dimensionality) {
            return RespValue{ .error_string = "ERR dimensionality mismatch with existing vector set" };
        }
        if (existing_vs.metric != metric) {
            return RespValue{ .error_string = "ERR distance metric mismatch with existing vector set" };
        }
    }

    const vs = &result.value_ptr.vector_set;

    // Parse and add all vectors
    var added_count: i64 = 0;
    var idx: usize = 0;
    while (idx < elements_data.len) : (idx += element_size) {
        const id = elements_data[idx];

        // Parse embedding values
        var embedding = try allocator.alloc(f32, dimensionality);
        defer allocator.free(embedding);

        for (0..dimensionality) |i| {
            const val_str = elements_data[idx + 1 + i];
            embedding[i] = std.fmt.parseFloat(f32, val_str) catch {
                return RespValue{ .error_string = "ERR invalid float value" };
            };
        }

        // Add to vector set
        const is_new = try vs.add(id, embedding);
        if (is_new) {
            added_count += 1;
        }
    }

    return RespValue.integer(added_count);
}

/// VCARD key
/// Returns the cardinality (number of vectors) in the vector set.
pub fn cmdVcard(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    _ = allocator;

    if (args.len != 2) return RespValue{ .error_string = "ERR wrong number of arguments for 'vcard' command" };

    const key = args[1];

    const value = storage.data.get(key) orelse {
        return RespValue.integer(0);
    };

    if (value != .vector_set) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    const cardinality: i64 = @intCast(value.vector_set.cardinality());
    return RespValue.integer(cardinality);
}

/// VDIM key
/// Returns the dimensionality of vectors in the vector set.
pub fn cmdVdim(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    _ = allocator;

    if (args.len != 2) return RespValue{ .error_string = "ERR wrong number of arguments for 'vdim' command" };

    const key = args[1];

    const value = storage.data.get(key) orelse {
        return RespValue{ .error_string = "ERR no such key" };
    };

    if (value != .vector_set) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    const dimensionality: i64 = @intCast(value.vector_set.dimensionality);
    return RespValue.integer(dimensionality);
}

/// VEMB key id
/// Returns the embedding of a specific vector in the set.
/// Returns an array of floats as bulk strings.
pub fn cmdVemb(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    if (args.len != 3) return RespValue{ .error_string = "ERR wrong number of arguments for 'vemb' command" };

    const key = args[1];
    const id = args[2];

    const value = storage.data.get(key) orelse {
        return RespValue.null_bulk_string();
    };

    if (value != .vector_set) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    const vs = &value.vector_set;
    const entry = vs.get(id) orelse {
        return RespValue.null_bulk_string();
    };

    // Build array of bulk strings (float values)
    var elements = try allocator.alloc(RespValue, entry.embedding.len);
    errdefer allocator.free(elements);

    for (entry.embedding, 0..) |val, i| {
        // Format float as string
        var buf: [64]u8 = undefined;
        const str = try std.fmt.bufPrint(&buf, "{d}", .{val});
        const str_copy = try allocator.dupe(u8, str);
        errdefer allocator.free(str_copy);

        elements[i] = RespValue{ .bulk_string = str_copy };
    }

    return RespValue{ .array = elements };
}

// ============================================================================
// Unit tests
// ============================================================================

test "cmdVadd: basic add" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VADD", "myvec", "3", "L2", "vec1", "1.0", "2.0", "3.0" };
    const result = try cmdVadd(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 1), result.integer);
}

test "cmdVadd: multiple vectors" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{
        "VADD", "myvec",      "2",    "COSINE",
        "v1",   "1.0",        "2.0",
        "v2",   "3.0",        "4.0",
        "v3",   "5.0",        "6.0",
    };
    const result = try cmdVadd(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 3), result.integer);
}

test "cmdVadd: update existing vector" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add first time
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "IP", "vec1", "1.0", "2.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 1), result1.integer);

    // Add same vector again (update)
    const args2 = [_][]const u8{ "VADD", "myvec", "2", "IP", "vec1", "3.0", "4.0" };
    const result2 = try cmdVadd(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 0), result2.integer); // 0 new vectors
}

test "cmdVadd: dimension mismatch" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create vector set with dim=3
    const args1 = [_][]const u8{ "VADD", "myvec", "3", "L2", "v1", "1.0", "2.0", "3.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Try to add with dim=2 (should fail)
    const args2 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v2", "4.0", "5.0" };
    const result2 = try cmdVadd(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expect(result2 == .err);
}

test "cmdVadd: metric mismatch" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create with L2
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Try to add with COSINE (should fail)
    const args2 = [_][]const u8{ "VADD", "myvec", "2", "COSINE", "v2", "3.0", "4.0" };
    const result2 = try cmdVadd(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expect(result2 == .err);
}

test "cmdVadd: invalid dimensionality" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VADD", "myvec", "0", "L2", "v1", "1.0" };
    const result = try cmdVadd(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

test "cmdVadd: invalid metric" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VADD", "myvec", "2", "INVALID", "v1", "1.0", "2.0" };
    const result = try cmdVadd(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

test "cmdVadd: wrong type" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create a string key
    _ = try storage.set("mykey", .{ .string = try allocator.dupe(u8, "value") });

    const args = [_][]const u8{ "VADD", "mykey", "2", "L2", "v1", "1.0", "2.0" };
    const result = try cmdVadd(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

test "cmdVcard: basic" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vectors
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0", "v2", "3.0", "4.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Get cardinality
    const args2 = [_][]const u8{ "VCARD", "myvec" };
    const result2 = try cmdVcard(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 2), result2.integer);
}

test "cmdVcard: nonexistent key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VCARD", "nonexistent" };
    const result = try cmdVcard(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 0), result.integer);
}

test "cmdVcard: wrong type" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.set("mykey", .{ .string = try allocator.dupe(u8, "value") });

    const args = [_][]const u8{ "VCARD", "mykey" };
    const result = try cmdVcard(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

test "cmdVdim: basic" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vector
    var vec_args = std.ArrayList([]const u8).init(allocator);
    defer vec_args.deinit();

    try vec_args.append("VADD");
    try vec_args.append("myvec");
    try vec_args.append("3");
    try vec_args.append("COSINE");
    try vec_args.append("v1");
    try vec_args.append("1.0");
    try vec_args.append("2.0");
    try vec_args.append("3.0");

    const result1 = try cmdVadd(allocator, &storage, vec_args.items, 3);
    defer result1.deinit(allocator);

    // Get dimensionality
    const args2 = [_][]const u8{ "VDIM", "myvec" };
    const result2 = try cmdVdim(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 3), result2.integer);
}

test "cmdVdim: nonexistent key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VDIM", "nonexistent" };
    const result = try cmdVdim(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

test "cmdVdim: wrong type" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.set("mykey", .{ .string = try allocator.dupe(u8, "value") });

    const args = [_][]const u8{ "VDIM", "mykey" };
    const result = try cmdVdim(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

test "cmdVemb: basic" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vector
    const args1 = [_][]const u8{ "VADD", "myvec", "3", "L2", "v1", "1.5", "2.5", "3.5" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Get embedding
    const args2 = [_][]const u8{ "VEMB", "myvec", "v1" };
    const result2 = try cmdVemb(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expect(result2 == .array);
    try std.testing.expectEqual(@as(usize, 3), result2.array.len);
}

test "cmdVemb: nonexistent vector" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vector
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Get nonexistent vector
    const args2 = [_][]const u8{ "VEMB", "myvec", "v2" };
    const result2 = try cmdVemb(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expect(result2 == .null_bulk_string);
}

test "cmdVemb: nonexistent key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VEMB", "nonexistent", "v1" };
    const result = try cmdVemb(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .null_bulk_string);
}

test "cmdVemb: wrong type" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.set("mykey", .{ .string = try allocator.dupe(u8, "value") });

    const args = [_][]const u8{ "VEMB", "mykey", "v1" };
    const result = try cmdVemb(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

/// VREM key id [id ...]
/// Remove one or more vectors from the vector set.
/// Returns the number of vectors removed (not including non-existent vectors).
pub fn cmdVrem(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    _ = allocator;

    if (args.len < 3) return RespValue{ .error_string = "ERR wrong number of arguments for 'vrem' command" };

    const key = args[1];
    const ids = args[2..];

    const value = storage.data.get(key) orelse {
        return RespValue.integer(0);
    };

    if (value != .vector_set) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    var vs = &value.vector_set;
    var removed_count: i64 = 0;

    for (ids) |id| {
        const was_removed = try vs.remove(id);
        if (was_removed) {
            removed_count += 1;
        }
    }

    return RespValue.integer(removed_count);
}

/// VISMEMBER key id
/// Returns 1 if the vector exists in the set, 0 otherwise.
pub fn cmdVismember(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    _ = allocator;

    if (args.len != 3) return RespValue{ .error_string = "ERR wrong number of arguments for 'vismember' command" };

    const key = args[1];
    const id = args[2];

    const value = storage.data.get(key) orelse {
        return RespValue.integer(0);
    };

    if (value != .vector_set) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    const vs = &value.vector_set;
    const exists = vs.contains(id);

    return RespValue.integer(if (exists) 1 else 0);
}

/// VRANDMEMBER key [count]
/// Returns one or more random vectors from the set.
/// If count is positive: returns up to count distinct vectors.
/// If count is negative: allows duplicates, returns abs(count) vectors.
/// Without count: returns a single vector ID as bulk string.
/// With count: returns an array of vector IDs.
pub fn cmdVrandmember(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    if (args.len < 2 or args.len > 3) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'vrandmember' command" };
    }

    const key = args[1];

    const value = storage.data.get(key) orelse {
        if (args.len == 2) {
            return RespValue.null_bulk_string();
        } else {
            // With count, return empty array
            const empty = try allocator.alloc(RespValue, 0);
            return RespValue{ .array = empty };
        }
    };

    if (value != .vector_set) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    const vs = &value.vector_set;

    // No count argument: return single random member
    if (args.len == 2) {
        const member_id = vs.randomMember() orelse {
            return RespValue.null_bulk_string();
        };
        const id_copy = try allocator.dupe(u8, member_id);
        return RespValue{ .bulk_string = id_copy };
    }

    // Parse count
    const count_str = args[2];
    const count_signed = std.fmt.parseInt(i64, count_str, 10) catch {
        return RespValue{ .error_string = "ERR value is not an integer or out of range" };
    };

    if (count_signed == 0) {
        const empty = try allocator.alloc(RespValue, 0);
        return RespValue{ .array = empty };
    }

    const allow_duplicates = count_signed < 0;
    const count_abs: usize = @intCast(@abs(count_signed));
    const result_count = if (!allow_duplicates)
        @min(count_abs, vs.cardinality())
    else
        count_abs;

    if (result_count == 0) {
        const empty = try allocator.alloc(RespValue, 0);
        return RespValue{ .array = empty };
    }

    var results = try allocator.alloc(RespValue, result_count);
    errdefer allocator.free(results);

    if (allow_duplicates) {
        // Allow duplicates: just call randomMember() count_abs times
        for (0..count_abs) |i| {
            const member_id = vs.randomMember() orelse {
                // Empty set, shouldn't happen since we checked above
                allocator.free(results);
                const empty = try allocator.alloc(RespValue, 0);
                return RespValue{ .array = empty };
            };
            const id_copy = try allocator.dupe(u8, member_id);
            errdefer allocator.free(id_copy);
            results[i] = RespValue{ .bulk_string = id_copy };
        }
    } else {
        // Distinct members: collect all IDs, shuffle, take first N
        var all_ids = try allocator.alloc([]const u8, vs.cardinality());
        defer allocator.free(all_ids);

        var it = vs.vectors.keyIterator();
        var idx: usize = 0;
        while (it.next()) |key_ptr| {
            all_ids[idx] = key_ptr.*;
            idx += 1;
        }

        // Simple shuffle (Fisher-Yates)
        var i = all_ids.len;
        while (i > 1) {
            i -= 1;
            const j = @rem(std.crypto.random.int(usize), i + 1);
            const tmp = all_ids[i];
            all_ids[i] = all_ids[j];
            all_ids[j] = tmp;
        }

        // Take first result_count
        for (0..result_count) |j| {
            const id_copy = try allocator.dupe(u8, all_ids[j]);
            errdefer allocator.free(id_copy);
            results[j] = RespValue{ .bulk_string = id_copy };
        }
    }

    return RespValue{ .array = results };
}

// ============================================================================
// VREM tests
// ============================================================================

test "cmdVrem: basic remove" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vectors
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0", "v2", "3.0", "4.0", "v3", "5.0", "6.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Remove one vector
    const args2 = [_][]const u8{ "VREM", "myvec", "v2" };
    const result2 = try cmdVrem(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 1), result2.integer);

    // Verify cardinality
    const args3 = [_][]const u8{ "VCARD", "myvec" };
    const result3 = try cmdVcard(allocator, &storage, &args3, 3);
    defer result3.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 2), result3.integer);
}

test "cmdVrem: multiple removes" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vectors
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0", "v2", "3.0", "4.0", "v3", "5.0", "6.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Remove multiple vectors
    const args2 = [_][]const u8{ "VREM", "myvec", "v1", "v3" };
    const result2 = try cmdVrem(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 2), result2.integer);

    // Verify only v2 remains
    const args3 = [_][]const u8{ "VCARD", "myvec" };
    const result3 = try cmdVcard(allocator, &storage, &args3, 3);
    defer result3.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 1), result3.integer);
}

test "cmdVrem: nonexistent vector" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vector
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Try to remove nonexistent vector
    const args2 = [_][]const u8{ "VREM", "myvec", "v2" };
    const result2 = try cmdVrem(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 0), result2.integer);
}

test "cmdVrem: nonexistent key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VREM", "nonexistent", "v1" };
    const result = try cmdVrem(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 0), result.integer);
}

test "cmdVrem: wrong type" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.set("mykey", .{ .string = try allocator.dupe(u8, "value") });

    const args = [_][]const u8{ "VREM", "mykey", "v1" };
    const result = try cmdVrem(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

// ============================================================================
// VISMEMBER tests
// ============================================================================

test "cmdVismember: exists" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vector
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Check membership
    const args2 = [_][]const u8{ "VISMEMBER", "myvec", "v1" };
    const result2 = try cmdVismember(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 1), result2.integer);
}

test "cmdVismember: not exists" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vector
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Check nonexistent member
    const args2 = [_][]const u8{ "VISMEMBER", "myvec", "v2" };
    const result2 = try cmdVismember(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 0), result2.integer);
}

test "cmdVismember: nonexistent key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VISMEMBER", "nonexistent", "v1" };
    const result = try cmdVismember(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 0), result.integer);
}

test "cmdVismember: wrong type" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.set("mykey", .{ .string = try allocator.dupe(u8, "value") });

    const args = [_][]const u8{ "VISMEMBER", "mykey", "v1" };
    const result = try cmdVismember(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

// ============================================================================
// VRANDMEMBER tests
// ============================================================================

test "cmdVrandmember: single random" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vectors
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0", "v2", "3.0", "4.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Get random member
    const args2 = [_][]const u8{ "VRANDMEMBER", "myvec" };
    const result2 = try cmdVrandmember(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expect(result2 == .bulk_string);
    // Should be either "v1" or "v2"
    const is_valid = std.mem.eql(u8, result2.bulk_string, "v1") or
        std.mem.eql(u8, result2.bulk_string, "v2");
    try std.testing.expect(is_valid);
}

test "cmdVrandmember: with positive count" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vectors
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0", "v2", "3.0", "4.0", "v3", "5.0", "6.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Get 2 random members (distinct)
    const args2 = [_][]const u8{ "VRANDMEMBER", "myvec", "2" };
    const result2 = try cmdVrandmember(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expect(result2 == .array);
    try std.testing.expectEqual(@as(usize, 2), result2.array.len);
}

test "cmdVrandmember: with negative count" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vector
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Get 3 random members with duplicates allowed
    const args2 = [_][]const u8{ "VRANDMEMBER", "myvec", "-3" };
    const result2 = try cmdVrandmember(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expect(result2 == .array);
    try std.testing.expectEqual(@as(usize, 3), result2.array.len);
    // All should be "v1" since there's only one vector
    for (result2.array) |elem| {
        try std.testing.expect(std.mem.eql(u8, elem.bulk_string, "v1"));
    }
}

test "cmdVrandmember: count exceeds size" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add 2 vectors
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0", "v2", "3.0", "4.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Request 5 distinct members (only 2 available)
    const args2 = [_][]const u8{ "VRANDMEMBER", "myvec", "5" };
    const result2 = try cmdVrandmember(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expect(result2 == .array);
    try std.testing.expectEqual(@as(usize, 2), result2.array.len); // Returns all 2
}

test "cmdVrandmember: count zero" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vector
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Request 0 members
    const args2 = [_][]const u8{ "VRANDMEMBER", "myvec", "0" };
    const result2 = try cmdVrandmember(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expect(result2 == .array);
    try std.testing.expectEqual(@as(usize, 0), result2.array.len);
}

test "cmdVrandmember: nonexistent key without count" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VRANDMEMBER", "nonexistent" };
    const result = try cmdVrandmember(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .null_bulk_string);
}

test "cmdVrandmember: nonexistent key with count" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VRANDMEMBER", "nonexistent", "5" };
    const result = try cmdVrandmember(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 0), result.array.len);
}

test "cmdVrandmember: wrong type" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.set("mykey", .{ .string = try allocator.dupe(u8, "value") });

    const args = [_][]const u8{ "VRANDMEMBER", "mykey" };
    const result = try cmdVrandmember(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

/// VGETATTR key member attribute
/// Returns the value of a specific attribute for a vector member.
/// Returns bulk string if attribute exists, null bulk string otherwise.
/// Returns null for nonexistent key, nonexistent member, or nonexistent attribute.
/// Returns WRONGTYPE error if key holds the wrong type.
pub fn cmdVgetattr(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    if (args.len != 4) return RespValue{ .error_string = "ERR wrong number of arguments for 'vgetattr' command" };

    const key = args[1];
    const member = args[2];
    const attribute = args[3];

    const value = storage.data.get(key) orelse {
        return RespValue.null_bulk_string();
    };

    if (value != .vector_set) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    const vs = &value.vector_set;
    const entry = vs.get(member) orelse {
        return RespValue.null_bulk_string();
    };

    const attr_value = entry.getAttribute(attribute) orelse {
        return RespValue.null_bulk_string();
    };

    // Return the attribute value as a bulk string.
    // The value is a string literal owned by storage, so we must copy it for
    // the RespValue to own.
    const value_copy = try allocator.dupe(u8, attr_value);
    return RespValue{ .bulk_string = value_copy };
}

/// VSETATTR key member attribute value
/// Sets an attribute on a vector member.
/// Returns 1 if the attribute was set successfully, 0 if the member does not exist.
/// Returns error if the key does not exist or holds the wrong type.
pub fn cmdVsetattr(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    _ = allocator;

    if (args.len != 5) return RespValue{ .error_string = "ERR wrong number of arguments for 'vsetattr' command" };

    const key = args[1];
    const member = args[2];
    const attribute = args[3];
    const attr_value = args[4];

    const value = storage.data.get(key) orelse {
        return RespValue{ .error_string = "ERR no such key" };
    };

    if (value != .vector_set) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    var vs = &value.vector_set;
    const entry = vs.get(member) orelse {
        return RespValue.integer(0);
    };

    try entry.setAttribute(attribute, attr_value);
    return RespValue.integer(1);
}

/// VINFO key
/// Returns metadata about a vector set as a flat array of 8 elements:
/// ["dimensionality", <int>, "metric", <string>, "count", <int>, "quantization", <string>]
/// Metric and quantization strings are uppercase.
/// Returns error for nonexistent key or wrong type.
pub fn cmdVinfo(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    if (args.len != 2) return RespValue{ .error_string = "ERR wrong number of arguments for 'vinfo' command" };

    const key = args[1];

    const value = storage.data.get(key) orelse {
        return RespValue{ .error_string = "ERR no such key" };
    };

    if (value != .vector_set) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    const vs = &value.vector_set;

    // Build flat array: ["dimensionality", int, "metric", str, "count", int, "quantization", str]
    var elements = try allocator.alloc(RespValue, 8);
    errdefer allocator.free(elements);

    // Element 0: "dimensionality" label
    const dim_label = try allocator.dupe(u8, "dimensionality");
    errdefer allocator.free(dim_label);
    elements[0] = RespValue{ .bulk_string = dim_label };

    // Element 1: dimensionality value
    elements[1] = RespValue.integer(@intCast(vs.dimensionality));

    // Element 2: "metric" label
    const metric_label = try allocator.dupe(u8, "metric");
    errdefer allocator.free(metric_label);
    elements[2] = RespValue{ .bulk_string = metric_label };

    // Element 3: metric value (uppercase string)
    const metric_str = try allocator.dupe(u8, vs.metric.toString());
    errdefer allocator.free(metric_str);
    elements[3] = RespValue{ .bulk_string = metric_str };

    // Element 4: "count" label
    const count_label = try allocator.dupe(u8, "count");
    errdefer allocator.free(count_label);
    elements[4] = RespValue{ .bulk_string = count_label };

    // Element 5: count value
    elements[5] = RespValue.integer(@intCast(vs.cardinality()));

    // Element 6: "quantization" label
    const quant_label = try allocator.dupe(u8, "quantization");
    errdefer allocator.free(quant_label);
    elements[6] = RespValue{ .bulk_string = quant_label };

    // Element 7: quantization value (uppercase string)
    const quant_str = try allocator.dupe(u8, vs.quantization.toString());
    errdefer allocator.free(quant_str);
    elements[7] = RespValue{ .bulk_string = quant_str };

    return RespValue{ .array = elements };
}

// ============================================================================
// VGETATTR tests
// ============================================================================

test "cmdVgetattr: existing attribute" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vector and set attribute
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    const args2 = [_][]const u8{ "VSETATTR", "myvec", "v1", "category", "test" };
    const result2 = try cmdVsetattr(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    // Get attribute
    const args3 = [_][]const u8{ "VGETATTR", "myvec", "v1", "category" };
    const result3 = try cmdVgetattr(allocator, &storage, &args3, 3);
    defer result3.deinit(allocator);

    try std.testing.expect(result3 == .bulk_string);
    try std.testing.expectEqualStrings("test", result3.bulk_string);
}

test "cmdVgetattr: nonexistent attribute" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vector without attributes
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Get nonexistent attribute
    const args2 = [_][]const u8{ "VGETATTR", "myvec", "v1", "nonexistent" };
    const result2 = try cmdVgetattr(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expect(result2 == .null_bulk_string);
}

test "cmdVgetattr: nonexistent member" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vector
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Get attribute of nonexistent member
    const args2 = [_][]const u8{ "VGETATTR", "myvec", "v_none", "category" };
    const result2 = try cmdVgetattr(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expect(result2 == .null_bulk_string);
}

test "cmdVgetattr: nonexistent key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VGETATTR", "nonexistent", "v1", "category" };
    const result = try cmdVgetattr(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .null_bulk_string);
}

test "cmdVgetattr: wrong type" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.set("mykey", .{ .string = try allocator.dupe(u8, "value") });

    const args = [_][]const u8{ "VGETATTR", "mykey", "v1", "category" };
    const result = try cmdVgetattr(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

test "cmdVgetattr: wrong arity too few" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VGETATTR", "myvec", "v1" };
    const result = try cmdVgetattr(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

test "cmdVgetattr: wrong arity too many" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VGETATTR", "myvec", "v1", "attr", "extra" };
    const result = try cmdVgetattr(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

// ============================================================================
// VSETATTR tests
// ============================================================================

test "cmdVsetattr: set new attribute" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vector
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Set attribute
    const args2 = [_][]const u8{ "VSETATTR", "myvec", "v1", "category", "news" };
    const result2 = try cmdVsetattr(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 1), result2.integer);
}

test "cmdVsetattr: replace existing attribute" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vector and set attribute
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    const args2 = [_][]const u8{ "VSETATTR", "myvec", "v1", "category", "old" };
    const result2 = try cmdVsetattr(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    // Replace attribute
    const args3 = [_][]const u8{ "VSETATTR", "myvec", "v1", "category", "new" };
    const result3 = try cmdVsetattr(allocator, &storage, &args3, 3);
    defer result3.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 1), result3.integer);

    // Verify the new value
    const args4 = [_][]const u8{ "VGETATTR", "myvec", "v1", "category" };
    const result4 = try cmdVgetattr(allocator, &storage, &args4, 3);
    defer result4.deinit(allocator);

    try std.testing.expect(result4 == .bulk_string);
    try std.testing.expectEqualStrings("new", result4.bulk_string);
}

test "cmdVsetattr: nonexistent member returns 0" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vector
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Set attribute on nonexistent member
    const args2 = [_][]const u8{ "VSETATTR", "myvec", "v_none", "category", "test" };
    const result2 = try cmdVsetattr(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 0), result2.integer);
}

test "cmdVsetattr: nonexistent key returns error" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VSETATTR", "nonexistent", "v1", "category", "test" };
    const result = try cmdVsetattr(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

test "cmdVsetattr: wrong type" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.set("mykey", .{ .string = try allocator.dupe(u8, "value") });

    const args = [_][]const u8{ "VSETATTR", "mykey", "v1", "category", "test" };
    const result = try cmdVsetattr(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

test "cmdVsetattr: wrong arity too few" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VSETATTR", "myvec", "v1", "category" };
    const result = try cmdVsetattr(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

test "cmdVsetattr: wrong arity too many" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VSETATTR", "myvec", "v1", "cat", "val", "extra" };
    const result = try cmdVsetattr(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

test "cmdVsetattr: multiple different attributes" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vector
    const args1 = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Set two different attributes
    const args2 = [_][]const u8{ "VSETATTR", "myvec", "v1", "category", "news" };
    const result2 = try cmdVsetattr(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 1), result2.integer);

    const args3 = [_][]const u8{ "VSETATTR", "myvec", "v1", "score", "0.95" };
    const result3 = try cmdVsetattr(allocator, &storage, &args3, 3);
    defer result3.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 1), result3.integer);

    // Verify both attributes
    const args4 = [_][]const u8{ "VGETATTR", "myvec", "v1", "category" };
    const result4 = try cmdVgetattr(allocator, &storage, &args4, 3);
    defer result4.deinit(allocator);
    try std.testing.expectEqualStrings("news", result4.bulk_string);

    const args5 = [_][]const u8{ "VGETATTR", "myvec", "v1", "score" };
    const result5 = try cmdVgetattr(allocator, &storage, &args5, 3);
    defer result5.deinit(allocator);
    try std.testing.expectEqualStrings("0.95", result5.bulk_string);
}

// ============================================================================
// VINFO tests
// ============================================================================

test "cmdVinfo: basic metadata" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add vectors
    const args1 = [_][]const u8{ "VADD", "myvec", "3", "L2", "v1", "1.0", "2.0", "3.0", "v2", "4.0", "5.0", "6.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    // Get info
    const args2 = [_][]const u8{ "VINFO", "myvec" };
    const result2 = try cmdVinfo(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expect(result2 == .array);
    try std.testing.expectEqual(@as(usize, 8), result2.array.len);

    // Check labels and values
    try std.testing.expectEqualStrings("dimensionality", result2.array[0].bulk_string);
    try std.testing.expectEqual(@as(i64, 3), result2.array[1].integer);
    try std.testing.expectEqualStrings("metric", result2.array[2].bulk_string);
    try std.testing.expectEqualStrings("L2", result2.array[3].bulk_string);
    try std.testing.expectEqualStrings("count", result2.array[4].bulk_string);
    try std.testing.expectEqual(@as(i64, 2), result2.array[5].integer);
    try std.testing.expectEqualStrings("quantization", result2.array[6].bulk_string);
    try std.testing.expectEqualStrings("FP32", result2.array[7].bulk_string);
}

test "cmdVinfo: cosine metric" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args1 = [_][]const u8{ "VADD", "myvec", "2", "COSINE", "v1", "1.0", "2.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    const args2 = [_][]const u8{ "VINFO", "myvec" };
    const result2 = try cmdVinfo(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expectEqualStrings("COSINE", result2.array[3].bulk_string);
}

test "cmdVinfo: empty vector set" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create vector set then remove all vectors
    const args1 = [_][]const u8{ "VADD", "myvec", "4", "IP", "v1", "1.0", "2.0", "3.0", "4.0" };
    const result1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer result1.deinit(allocator);

    const args_rem = [_][]const u8{ "VREM", "myvec", "v1" };
    const result_rem = try cmdVrem(allocator, &storage, &args_rem, 3);
    defer result_rem.deinit(allocator);

    const args2 = [_][]const u8{ "VINFO", "myvec" };
    const result2 = try cmdVinfo(allocator, &storage, &args2, 3);
    defer result2.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 4), result2.array[1].integer); // dimensionality preserved
    try std.testing.expectEqualStrings("IP", result2.array[3].bulk_string);
    try std.testing.expectEqual(@as(i64, 0), result2.array[5].integer); // count is 0
}

test "cmdVinfo: nonexistent key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VINFO", "nonexistent" };
    const result = try cmdVinfo(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

test "cmdVinfo: wrong type" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.set("mykey", .{ .string = try allocator.dupe(u8, "value") });

    const args = [_][]const u8{ "VINFO", "mykey" };
    const result = try cmdVinfo(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}

test "cmdVinfo: wrong arity" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{"VINFO"};
    const result = try cmdVinfo(allocator, &storage, &args, 3);
    defer result.deinit(allocator);

    try std.testing.expect(result == .err);
}
