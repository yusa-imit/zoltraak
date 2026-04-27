const std = @import("std");
const Allocator = std.mem.Allocator;
const Storage = @import("../storage/memory.zig").Storage;
const Value = @import("../storage/memory.zig").Value;
const VectorSetValue = @import("../storage/vector.zig").VectorSetValue;
const DistanceMetric = @import("../storage/vector.zig").DistanceMetric;
const RespValue = @import("../protocol/writer.zig").RespValue;

/// VADD key dimensionality metric element [element ...]
/// Add one or more vectors to a vector set.
/// Each element is: <id> <embedding_values...>
/// Returns the number of newly added vectors (existing vectors are updated, not counted).
pub fn cmdVadd(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    if (args.len < 5) return RespValue.err("ERR wrong number of arguments for 'vadd' command");

    const key = args[1];
    const dim_str = args[2];
    const metric_str = args[3];

    // Parse dimensionality
    const dimensionality = std.fmt.parseInt(usize, dim_str, 10) catch {
        return RespValue.err("ERR invalid dimensionality");
    };
    if (dimensionality == 0) {
        return RespValue.err("ERR dimensionality must be positive");
    }

    // Parse distance metric
    const metric = DistanceMetric.fromString(metric_str) catch {
        return RespValue.err("ERR invalid distance metric, must be L2, IP, or COSINE");
    };

    // Each element requires: id + dimensionality values
    const element_size = 1 + dimensionality;
    const elements_data = args[4..];
    if (elements_data.len % element_size != 0) {
        return RespValue.err("ERR syntax error, element format is: <id> <f32> [<f32> ...]");
    }

    const num_elements = elements_data.len / element_size;
    if (num_elements == 0) {
        return RespValue.err("ERR at least one vector required");
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
            return RespValue.err("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        // Validate dimensionality and metric match
        const existing_vs = &result.value_ptr.vector_set;
        if (existing_vs.dimensionality != dimensionality) {
            return RespValue.err("ERR dimensionality mismatch with existing vector set");
        }
        if (existing_vs.metric != metric) {
            return RespValue.err("ERR distance metric mismatch with existing vector set");
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
                return RespValue.err("ERR invalid float value");
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

    if (args.len != 2) return RespValue.err("ERR wrong number of arguments for 'vcard' command");

    const key = args[1];

    const value = storage.data.get(key) orelse {
        return RespValue.integer(0);
    };

    if (value != .vector_set) {
        return RespValue.err("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    const cardinality: i64 = @intCast(value.vector_set.cardinality());
    return RespValue.integer(cardinality);
}

/// VDIM key
/// Returns the dimensionality of vectors in the vector set.
pub fn cmdVdim(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    _ = allocator;

    if (args.len != 2) return RespValue.err("ERR wrong number of arguments for 'vdim' command");

    const key = args[1];

    const value = storage.data.get(key) orelse {
        return RespValue.err("ERR no such key");
    };

    if (value != .vector_set) {
        return RespValue.err("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    const dimensionality: i64 = @intCast(value.vector_set.dimensionality);
    return RespValue.integer(dimensionality);
}

/// VEMB key id
/// Returns the embedding of a specific vector in the set.
/// Returns an array of floats as bulk strings.
pub fn cmdVemb(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    if (args.len != 3) return RespValue.err("ERR wrong number of arguments for 'vemb' command");

    const key = args[1];
    const id = args[2];

    const value = storage.data.get(key) orelse {
        return RespValue.null_bulk_string();
    };

    if (value != .vector_set) {
        return RespValue.err("WRONGTYPE Operation against a key holding the wrong kind of value");
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
