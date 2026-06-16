const std = @import("std");
const Allocator = std.mem.Allocator;
const Storage = @import("../storage/memory.zig").Storage;
const Value = @import("../storage/memory.zig").Value;
const VectorSetValue = @import("../storage/vector.zig").VectorSetValue;
const VectorEntry = @import("../storage/vector.zig").VectorEntry;
const DistanceMetric = @import("../storage/vector.zig").DistanceMetric;
const QuantizationType = @import("../storage/vector.zig").QuantizationType;
const RespValue = @import("../protocol/parser.zig").RespValue;

/// VADD key [REDUCE dim] (VALUES num | FP32) f1..fn element [SETATTR blob] [EF ef] [M m] [CAS] [NOQUANT|Q8|BIN]
/// Redis 8.0 Vector Set add. One element per call.
/// Returns 1 if newly added, 0 if updated.
pub fn cmdVadd(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    if (args.len < 5) return RespValue{ .error_string = "ERR wrong number of arguments for 'vadd' command" };

    const key = args[1];
    var idx: usize = 2;

    // Optional: REDUCE dim (we accept but ignore — compression not yet implemented)
    if (idx < args.len and std.ascii.eqlIgnoreCase(args[idx], "REDUCE")) {
        idx += 1;
        if (idx >= args.len) return RespValue{ .error_string = "ERR syntax error" };
        _ = std.fmt.parseInt(usize, args[idx], 10) catch {
            return RespValue{ .error_string = "ERR invalid REDUCE dim value" };
        };
        idx += 1;
    }

    if (idx >= args.len) return RespValue{ .error_string = "ERR syntax error" };

    // Parse vector input: VALUES num f1..fn
    var embedding: []f32 = undefined;
    var embedding_allocated = false;

    if (std.ascii.eqlIgnoreCase(args[idx], "VALUES")) {
        idx += 1;
        if (idx >= args.len) return RespValue{ .error_string = "ERR syntax error" };
        const num = std.fmt.parseInt(usize, args[idx], 10) catch {
            return RespValue{ .error_string = "ERR invalid VALUES count" };
        };
        if (num == 0) return RespValue{ .error_string = "ERR VALUES count must be positive" };
        idx += 1;
        if (idx + num > args.len) return RespValue{ .error_string = "ERR not enough vector values" };

        embedding = try allocator.alloc(f32, num);
        embedding_allocated = true;
        for (0..num) |i| {
            embedding[i] = std.fmt.parseFloat(f32, args[idx + i]) catch {
                allocator.free(embedding);
                return RespValue{ .error_string = "ERR invalid float value" };
            };
        }
        idx += num;
    } else if (std.ascii.eqlIgnoreCase(args[idx], "FP32")) {
        return RespValue{ .error_string = "ERR FP32 binary format not supported, use VALUES" };
    } else {
        return RespValue{ .error_string = "ERR syntax error, expected VALUES or FP32" };
    }
    defer if (embedding_allocated) allocator.free(embedding);

    // Required: element name follows vector values
    if (idx >= args.len) return RespValue{ .error_string = "ERR missing element name" };
    const element_name = args[idx];
    idx += 1;

    // Optional trailing flags
    var setattr_blob: ?[]const u8 = null;
    while (idx < args.len) {
        const flag = args[idx];
        if (std.ascii.eqlIgnoreCase(flag, "SETATTR")) {
            idx += 1;
            if (idx >= args.len) return RespValue{ .error_string = "ERR SETATTR requires a value" };
            setattr_blob = args[idx];
            idx += 1;
        } else if (std.ascii.eqlIgnoreCase(flag, "CAS") or
                   std.ascii.eqlIgnoreCase(flag, "NOQUANT") or
                   std.ascii.eqlIgnoreCase(flag, "Q8") or
                   std.ascii.eqlIgnoreCase(flag, "BIN"))
        {
            idx += 1;
        } else if (std.ascii.eqlIgnoreCase(flag, "EF") or
                   std.ascii.eqlIgnoreCase(flag, "M"))
        {
            idx += 1;
            if (idx >= args.len) return RespValue{ .error_string = "ERR syntax error" };
            idx += 1;
        } else {
            return RespValue{ .error_string = "ERR syntax error" };
        }
    }

    // Get or create vector set
    const gop = try storage.data.getOrPut(key);
    if (!gop.found_existing) {
        const key_copy = try allocator.dupe(u8, key);
        errdefer allocator.free(key_copy);

        var new_vs = try VectorSetValue.init(allocator, embedding.len, .cosine);
        errdefer new_vs.deinit();

        gop.key_ptr.* = key_copy;
        gop.value_ptr.* = Value{ .vector_set = new_vs };
    } else {
        if (gop.value_ptr.* != .vector_set) {
            return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
        }
        if (gop.value_ptr.vector_set.dimensionality != embedding.len) {
            return RespValue{ .error_string = "ERR dimensionality mismatch with existing vector set" };
        }
    }

    const vs = &gop.value_ptr.vector_set;
    const is_new = try vs.add(element_name, embedding);

    if (setattr_blob) |blob| {
        if (vs.get(element_name)) |entry| {
            try entry.setAttribute("__attr__", blob);
        }
    }

    return RespValue{ .integer = if (is_new) 1 else 0 };
}

/// VCARD key
pub fn cmdVcard(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    _ = allocator;
    if (args.len != 2) return RespValue{ .error_string = "ERR wrong number of arguments for 'vcard' command" };
    const key = args[1];
    const value = storage.data.get(key) orelse return RespValue{ .integer = 0 };
    if (value != .vector_set) return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    return RespValue{ .integer = @intCast(value.vector_set.cardinality()) };
}

/// VDIM key
pub fn cmdVdim(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    _ = allocator;
    if (args.len != 2) return RespValue{ .error_string = "ERR wrong number of arguments for 'vdim' command" };
    const key = args[1];
    const value = storage.data.get(key) orelse return RespValue{ .error_string = "ERR no such key" };
    if (value != .vector_set) return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    return RespValue{ .integer = @intCast(value.vector_set.dimensionality) };
}

/// VEMB key id
pub fn cmdVemb(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    if (args.len != 3) return RespValue{ .error_string = "ERR wrong number of arguments for 'vemb' command" };
    const key = args[1];
    const id = args[2];
    const value = storage.data.get(key) orelse return RespValue{ .null_bulk_string = {} };
    if (value != .vector_set) return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    const entry = value.vector_set.get(id) orelse return RespValue{ .null_bulk_string = {} };

    var elements = try allocator.alloc(RespValue, entry.embedding.len);
    errdefer allocator.free(elements);
    for (entry.embedding, 0..) |val, i| {
        var buf: [64]u8 = undefined;
        const str = try std.fmt.bufPrint(&buf, "{d}", .{val});
        elements[i] = RespValue{ .bulk_string = try allocator.dupe(u8, str) };
    }
    return RespValue{ .array = elements };
}

/// VREM key id [id ...]
pub fn cmdVrem(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    _ = allocator;
    if (args.len < 3) return RespValue{ .error_string = "ERR wrong number of arguments for 'vrem' command" };
    const key = args[1];
    const entry = storage.data.getPtr(key) orelse return RespValue{ .integer = 0 };
    if (entry.* != .vector_set) return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    var vs = &entry.vector_set;
    var removed: i64 = 0;
    for (args[2..]) |id| {
        if (try vs.remove(id)) removed += 1;
    }
    return RespValue{ .integer = removed };
}

/// VISMEMBER key id
pub fn cmdVismember(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    _ = allocator;
    if (args.len != 3) return RespValue{ .error_string = "ERR wrong number of arguments for 'vismember' command" };
    const key = args[1];
    const id = args[2];
    const value = storage.data.get(key) orelse return RespValue{ .integer = 0 };
    if (value != .vector_set) return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    return RespValue{ .integer = if (value.vector_set.contains(id)) 1 else 0 };
}

/// VRANDMEMBER key [count]
pub fn cmdVrandmember(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    if (args.len < 2 or args.len > 3) return RespValue{ .error_string = "ERR wrong number of arguments for 'vrandmember' command" };
    const key = args[1];
    const value = storage.data.get(key) orelse {
        if (args.len == 2) return RespValue{ .null_bulk_string = {} };
        return RespValue{ .array = try allocator.alloc(RespValue, 0) };
    };
    if (value != .vector_set) return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    const vs = &value.vector_set;

    if (args.len == 2) {
        const id = vs.randomMember() orelse return RespValue{ .null_bulk_string = {} };
        return RespValue{ .bulk_string = try allocator.dupe(u8, id) };
    }

    const count_signed = std.fmt.parseInt(i64, args[2], 10) catch {
        return RespValue{ .error_string = "ERR value is not an integer or out of range" };
    };
    if (count_signed == 0) return RespValue{ .array = try allocator.alloc(RespValue, 0) };

    const allow_dups = count_signed < 0;
    const count_abs: usize = @intCast(@abs(count_signed));
    const result_count = if (!allow_dups) @min(count_abs, vs.cardinality()) else count_abs;

    var results = try allocator.alloc(RespValue, result_count);
    errdefer allocator.free(results);

    if (allow_dups) {
        for (0..count_abs) |i| {
            const id = vs.randomMember() orelse {
                allocator.free(results);
                return RespValue{ .array = try allocator.alloc(RespValue, 0) };
            };
            results[i] = RespValue{ .bulk_string = try allocator.dupe(u8, id) };
        }
    } else {
        var all_ids = try allocator.alloc([]const u8, vs.cardinality());
        defer allocator.free(all_ids);
        var it = vs.vectors.keyIterator();
        var i: usize = 0;
        while (it.next()) |k| { all_ids[i] = k.*; i += 1; }
        // Fisher-Yates shuffle
        var j = all_ids.len;
        while (j > 1) {
            j -= 1;
            const r = @rem(std.crypto.random.int(usize), j + 1);
            const tmp = all_ids[j]; all_ids[j] = all_ids[r]; all_ids[r] = tmp;
        }
        for (0..result_count) |k| {
            results[k] = RespValue{ .bulk_string = try allocator.dupe(u8, all_ids[k]) };
        }
    }
    return RespValue{ .array = results };
}

/// VGETATTR key member
/// Redis 8.0: returns the attribute blob stored for this member, or nil.
pub fn cmdVgetattr(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    if (args.len != 3) return RespValue{ .error_string = "ERR wrong number of arguments for 'vgetattr' command" };
    const key = args[1];
    const member = args[2];
    const value = storage.data.get(key) orelse return RespValue{ .null_bulk_string = {} };
    if (value != .vector_set) return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    const entry = value.vector_set.get(member) orelse return RespValue{ .null_bulk_string = {} };
    const blob = entry.getAttribute("__attr__") orelse return RespValue{ .null_bulk_string = {} };
    return RespValue{ .bulk_string = try allocator.dupe(u8, blob) };
}

/// VSETATTR key member blob
/// Redis 8.0: stores an attribute blob for this member. Returns 1 on success, 0 if member not found.
pub fn cmdVsetattr(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    _ = allocator;
    if (args.len != 4) return RespValue{ .error_string = "ERR wrong number of arguments for 'vsetattr' command" };
    const key = args[1];
    const member = args[2];
    const blob = args[3];
    const value = storage.data.get(key) orelse return RespValue{ .error_string = "ERR no such key" };
    if (value != .vector_set) return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    const entry = value.vector_set.get(member) orelse return RespValue{ .integer = 0 };
    try entry.setAttribute("__attr__", blob);
    return RespValue{ .integer = 1 };
}

/// VINFO key
pub fn cmdVinfo(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    if (args.len != 2) return RespValue{ .error_string = "ERR wrong number of arguments for 'vinfo' command" };
    const key = args[1];
    const value = storage.data.get(key) orelse return RespValue{ .error_string = "ERR no such key" };
    if (value != .vector_set) return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    const vs = &value.vector_set;

    var elements = try allocator.alloc(RespValue, 8);
    errdefer allocator.free(elements);
    elements[0] = RespValue{ .bulk_string = try allocator.dupe(u8, "dimensionality") };
    elements[1] = RespValue{ .integer = @intCast(vs.dimensionality) };
    elements[2] = RespValue{ .bulk_string = try allocator.dupe(u8, "metric") };
    elements[3] = RespValue{ .bulk_string = try allocator.dupe(u8, vs.metric.toString()) };
    elements[4] = RespValue{ .bulk_string = try allocator.dupe(u8, "count") };
    elements[5] = RespValue{ .integer = @intCast(vs.cardinality()) };
    elements[6] = RespValue{ .bulk_string = try allocator.dupe(u8, "quantization") };
    elements[7] = RespValue{ .bulk_string = try allocator.dupe(u8, vs.quantization.toString()) };
    return RespValue{ .array = elements };
}

/// VSIM key (ELE element | VALUES num f1..fn | FP32 blob) [COUNT n] [WITHSCORES] [WITHATTRIBS] [EF ef] [EPSILON d] [TRUTH] [NOTHREAD] [FILTER expr] [FILTER-EF n]
/// Redis 8.0 Vector Similarity Search. Returns elements sorted by similarity score (descending).
/// WITHSCORES returns flat array [elem1, score1, elem2, score2, ...].
pub fn cmdVsim(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    if (args.len < 4) return RespValue{ .error_string = "ERR wrong number of arguments for 'vsim' command" };

    const key = args[1];
    var idx: usize = 2;

    // Parse input mode: ELE, VALUES, or FP32
    var query_embedding: []const f32 = undefined;
    var query_buf: ?[]f32 = null;
    defer if (query_buf) |buf| allocator.free(buf);
    var is_ele_query = false;
    var ele_query_name: []const u8 = undefined;

    if (std.ascii.eqlIgnoreCase(args[idx], "ELE")) {
        idx += 1;
        if (idx >= args.len) return RespValue{ .error_string = "ERR ELE requires element name" };
        ele_query_name = args[idx];
        is_ele_query = true;
        idx += 1;
    } else if (std.ascii.eqlIgnoreCase(args[idx], "VALUES")) {
        idx += 1;
        if (idx >= args.len) return RespValue{ .error_string = "ERR VALUES requires count" };
        const num = std.fmt.parseInt(usize, args[idx], 10) catch {
            return RespValue{ .error_string = "ERR invalid VALUES count" };
        };
        idx += 1;
        if (idx + num > args.len) return RespValue{ .error_string = "ERR not enough vector values" };
        const buf = try allocator.alloc(f32, num);
        query_buf = buf;
        for (0..num) |i| {
            buf[i] = std.fmt.parseFloat(f32, args[idx + i]) catch {
                return RespValue{ .error_string = "ERR invalid float value" };
            };
        }
        query_embedding = buf;
        idx += num;
    } else if (std.ascii.eqlIgnoreCase(args[idx], "FP32")) {
        return RespValue{ .error_string = "ERR FP32 binary format not supported, use ELE or VALUES" };
    } else {
        return RespValue{ .error_string = "ERR syntax error, expected ELE, VALUES, or FP32" };
    }

    // Parse options
    var count: usize = 10;
    var with_scores = false;
    var with_attribs = false;
    while (idx < args.len) {
        const opt = args[idx];
        if (std.ascii.eqlIgnoreCase(opt, "COUNT")) {
            idx += 1;
            if (idx >= args.len) return RespValue{ .error_string = "ERR COUNT requires value" };
            count = std.fmt.parseInt(usize, args[idx], 10) catch {
                return RespValue{ .error_string = "ERR value is not an integer or out of range" };
            };
            idx += 1;
        } else if (std.ascii.eqlIgnoreCase(opt, "WITHSCORES")) {
            with_scores = true;
            idx += 1;
        } else if (std.ascii.eqlIgnoreCase(opt, "WITHATTRIBS")) {
            with_attribs = true;
            idx += 1;
        } else if (std.ascii.eqlIgnoreCase(opt, "EF") or
                   std.ascii.eqlIgnoreCase(opt, "EPSILON") or
                   std.ascii.eqlIgnoreCase(opt, "FILTER") or
                   std.ascii.eqlIgnoreCase(opt, "FILTER-EF"))
        {
            idx += 1;
            if (idx >= args.len) return RespValue{ .error_string = "ERR syntax error" };
            idx += 1;
        } else if (std.ascii.eqlIgnoreCase(opt, "TRUTH") or
                   std.ascii.eqlIgnoreCase(opt, "NOTHREAD"))
        {
            idx += 1;
        } else {
            return RespValue{ .error_string = "ERR syntax error" };
        }
    }

    // Get vector set
    const value = storage.data.get(key) orelse {
        return RespValue{ .array = try allocator.alloc(RespValue, 0) };
    };
    if (value != .vector_set) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }
    const vs = &value.vector_set;

    // Resolve ELE query embedding
    if (is_ele_query) {
        const entry = vs.get(ele_query_name) orelse {
            return RespValue{ .error_string = "ERR element not found" };
        };
        query_embedding = entry.embedding;
    } else {
        if (query_embedding.len != vs.dimensionality) {
            return RespValue{ .error_string = "ERR vector dimensionality mismatch" };
        }
    }

    const SimResult = struct {
        id: []const u8,
        score: f32,
    };

    var results = std.ArrayList(SimResult){};
    defer results.deinit(allocator);

    var it = vs.vectors.iterator();
    while (it.next()) |kv| {
        const dist = vs.distance(query_embedding, kv.value_ptr.*.embedding);
        // Convert distance to similarity score ∈ [0, 1]; for cosine dist ∈ [0,2]
        const score: f32 = @max(0.0, 1.0 - dist);
        try results.append(allocator, SimResult{ .id = kv.key_ptr.*, .score = score });
    }

    std.mem.sort(SimResult, results.items, {}, struct {
        pub fn lessThan(_: void, a: SimResult, b: SimResult) bool {
            return a.score > b.score;
        }
    }.lessThan);

    const actual = @min(count, results.items.len);

    if (!with_scores and !with_attribs) {
        var arr = try allocator.alloc(RespValue, actual);
        errdefer allocator.free(arr);
        for (0..actual) |i| {
            arr[i] = RespValue{ .bulk_string = try allocator.dupe(u8, results.items[i].id) };
        }
        return RespValue{ .array = arr };
    }

    // Flat array with optional score and/or attribs fields per element
    const fields_per: usize = 1 + @as(usize, if (with_scores) 1 else 0) + @as(usize, if (with_attribs) 1 else 0);
    var arr = try allocator.alloc(RespValue, actual * fields_per);
    errdefer allocator.free(arr);

    for (0..actual) |i| {
        const item = results.items[i];
        var pos = i * fields_per;
        arr[pos] = RespValue{ .bulk_string = try allocator.dupe(u8, item.id) };
        pos += 1;
        if (with_scores) {
            const s = try std.fmt.allocPrint(allocator, "{d}", .{item.score});
            arr[pos] = RespValue{ .bulk_string = s };
            pos += 1;
        }
        if (with_attribs) {
            const entry = vs.get(item.id);
            if (entry) |e| {
                const blob = e.getAttribute("__attr__");
                if (blob) |b| {
                    arr[pos] = RespValue{ .bulk_string = try allocator.dupe(u8, b) };
                } else {
                    arr[pos] = RespValue{ .null_bulk_string = {} };
                }
            } else {
                arr[pos] = RespValue{ .null_bulk_string = {} };
            }
            pos += 1;
        }
    }
    return RespValue{ .array = arr };
}

/// VRANGE key start stop [WITHEMBEDDINGS]
pub fn cmdVrange(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    if (args.len < 4) return RespValue{ .error_string = "ERR wrong number of arguments for 'vrange' command" };
    const key = args[1];
    var with_embeddings = false;
    for (args[4..]) |flag| {
        if (std.ascii.eqlIgnoreCase(flag, "WITHEMBEDDINGS")) {
            with_embeddings = true;
        } else {
            return RespValue{ .error_string = "ERR syntax error" };
        }
    }
    const value = storage.data.get(key) orelse return RespValue{ .error_string = "ERR no such key" };
    if (value != .vector_set) return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    const vs = &value.vector_set;
    const cnt = vs.cardinality();
    if (cnt == 0) return RespValue{ .array = try allocator.alloc(RespValue, 0) };

    var start = std.fmt.parseInt(i64, args[2], 10) catch return RespValue{ .error_string = "ERR invalid start index" };
    var stop = std.fmt.parseInt(i64, args[3], 10) catch return RespValue{ .error_string = "ERR invalid stop index" };
    const cnt_i64 = @as(i64, @intCast(cnt));
    if (start < 0) start = cnt_i64 + start;
    if (stop < 0) stop = cnt_i64 + stop;
    if (start < 0) start = 0;
    if (stop >= cnt_i64) stop = cnt_i64 - 1;
    if (start > stop) return RespValue{ .array = try allocator.alloc(RespValue, 0) };

    var members = std.ArrayList(*VectorEntry){};
    defer members.deinit(allocator);
    var it = vs.vectors.valueIterator();
    while (it.next()) |ep| try members.append(allocator, ep.*);

    const range_size = @as(usize, @intCast(stop - start + 1));
    var result = try allocator.alloc(RespValue, range_size);
    errdefer allocator.free(result);

    for (0..range_size) |i| {
        const entry = members.items[@as(usize, @intCast(start)) + i];
        if (!with_embeddings) {
            result[i] = RespValue{ .bulk_string = try allocator.dupe(u8, entry.id) };
        } else {
            var item = try allocator.alloc(RespValue, 2);
            item[0] = RespValue{ .bulk_string = try allocator.dupe(u8, entry.id) };
            var emb = try allocator.alloc(RespValue, entry.embedding.len);
            for (entry.embedding, 0..) |v, j| {
                emb[j] = RespValue{ .bulk_string = try std.fmt.allocPrint(allocator, "{d}", .{v}) };
            }
            item[1] = RespValue{ .array = emb };
            result[i] = RespValue{ .array = item };
        }
    }
    return RespValue{ .array = result };
}

/// VLINKS key member — stub (HNSW graph not yet built)
pub fn cmdVlinks(allocator: Allocator, storage: *Storage, args: []const []const u8, _: usize) !RespValue {
    if (args.len != 3) return RespValue{ .error_string = "ERR wrong number of arguments for 'vlinks' command" };
    const key = args[1];
    const member = args[2];
    const value = storage.data.get(key) orelse return RespValue{ .error_string = "ERR no such key" };
    if (value != .vector_set) return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    _ = value.vector_set.get(member) orelse return RespValue{ .error_string = "ERR member not found" };
    return RespValue{ .array = try allocator.alloc(RespValue, 0) };
}

/// Free a RespValue that was returned by a vector command.
/// Only bulk_string and array variants are allocated by vector commands;
/// error_string is always a string literal and must not be freed.
pub fn deinitRespValue(allocator: Allocator, rv: RespValue) void {
    switch (rv) {
        .bulk_string => |s| allocator.free(s),
        .array => |arr| {
            for (arr) |item| deinitRespValue(allocator, item);
            allocator.free(arr);
        },
        else => {},
    }
}

// ============================================================================
// Tests — all use Redis 8.0 VADD syntax: VADD key VALUES num f1..fn element
// ============================================================================

test "cmdVadd: basic add returns 1" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VADD", "myvec", "VALUES", "3", "1.0", "2.0", "3.0", "vec1" };
    const result = try cmdVadd(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 1), result.integer);
}

test "cmdVadd: update existing returns 0" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer r1.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 1), r1.integer);

    const args2 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "3.0", "4.0", "v1" };
    const r2 = try cmdVadd(allocator, &storage, &args2, 3);
    defer r2.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 0), r2.integer);
}

test "cmdVadd: dimensionality mismatch on second add" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args1 = [_][]const u8{ "VADD", "myvec", "VALUES", "3", "1.0", "2.0", "3.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &args1, 3);
    defer r1.deinit(allocator);

    const args2 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v2" };
    const r2 = try cmdVadd(allocator, &storage, &args2, 3);
    defer r2.deinit(allocator);
    try std.testing.expect(r2 == .err);
}

test "cmdVadd: invalid float value" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "notafloat", "2.0", "v1" };
    const result = try cmdVadd(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .err);
}

test "cmdVadd: zero VALUES count rejected" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VADD", "myvec", "VALUES", "0", "v1" };
    const result = try cmdVadd(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .err);
}

test "cmdVadd: wrong type key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.set("mykey", .{ .string = try allocator.dupe(u8, "value") });

    const args = [_][]const u8{ "VADD", "mykey", "VALUES", "2", "1.0", "2.0", "v1" };
    const result = try cmdVadd(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .err);
}

test "cmdVadd: SETATTR option stores blob" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1", "SETATTR", "{\"label\":\"test\"}" };
    const r = try cmdVadd(allocator, &storage, &args, 3);
    defer r.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 1), r.integer);

    // Verify stored via VGETATTR
    const ga = [_][]const u8{ "VGETATTR", "myvec", "v1" };
    const gr = try cmdVgetattr(allocator, &storage, &ga, 3);
    defer gr.deinit(allocator);
    try std.testing.expect(gr == .bulk_string);
    try std.testing.expectEqualStrings("{\"label\":\"test\"}", gr.bulk_string);
}

test "cmdVadd: REDUCE option accepted" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VADD", "myvec", "REDUCE", "2", "VALUES", "3", "1.0", "2.0", "3.0", "v1" };
    const result = try cmdVadd(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 1), result.integer);
}

// ============================================================================
// VCARD tests
// ============================================================================

test "cmdVcard: basic" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3);
    defer r1.deinit(allocator);
    const a2 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "3.0", "4.0", "v2" };
    const r2 = try cmdVadd(allocator, &storage, &a2, 3);
    defer r2.deinit(allocator);

    const args = [_][]const u8{ "VCARD", "myvec" };
    const result = try cmdVcard(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 2), result.integer);
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

// ============================================================================
// VDIM tests
// ============================================================================

test "cmdVdim: basic" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "3", "1.0", "2.0", "3.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3);
    defer r1.deinit(allocator);

    const args = [_][]const u8{ "VDIM", "myvec" };
    const result = try cmdVdim(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 3), result.integer);
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

// ============================================================================
// VEMB tests
// ============================================================================

test "cmdVemb: basic" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "3", "1.5", "2.5", "3.5", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3);
    defer r1.deinit(allocator);

    const args = [_][]const u8{ "VEMB", "myvec", "v1" };
    const result = try cmdVemb(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 3), result.array.len);
}

test "cmdVemb: nonexistent vector" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3);
    defer r1.deinit(allocator);

    const args = [_][]const u8{ "VEMB", "myvec", "v2" };
    const result = try cmdVemb(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .null_bulk_string);
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

// ============================================================================
// VREM tests
// ============================================================================

test "cmdVrem: basic remove" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const a2 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "3.0", "4.0", "v2" };
    const a3 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "5.0", "6.0", "v3" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);
    const r2 = try cmdVadd(allocator, &storage, &a2, 3); defer r2.deinit(allocator);
    const r3 = try cmdVadd(allocator, &storage, &a3, 3); defer r3.deinit(allocator);

    const rem = [_][]const u8{ "VREM", "myvec", "v2" };
    const rr = try cmdVrem(allocator, &storage, &rem, 3);
    defer rr.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 1), rr.integer);

    const card = [_][]const u8{ "VCARD", "myvec" };
    const cr = try cmdVcard(allocator, &storage, &card, 3);
    defer cr.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 2), cr.integer);
}

test "cmdVrem: multiple removes" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const a2 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "3.0", "4.0", "v2" };
    const a3 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "5.0", "6.0", "v3" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);
    const r2 = try cmdVadd(allocator, &storage, &a2, 3); defer r2.deinit(allocator);
    const r3 = try cmdVadd(allocator, &storage, &a3, 3); defer r3.deinit(allocator);

    const rem = [_][]const u8{ "VREM", "myvec", "v1", "v3" };
    const rr = try cmdVrem(allocator, &storage, &rem, 3);
    defer rr.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 2), rr.integer);

    const card = [_][]const u8{ "VCARD", "myvec" };
    const cr = try cmdVcard(allocator, &storage, &card, 3);
    defer cr.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 1), cr.integer);
}

test "cmdVrem: nonexistent vector" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);

    const rem = [_][]const u8{ "VREM", "myvec", "v2" };
    const rr = try cmdVrem(allocator, &storage, &rem, 3);
    defer rr.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 0), rr.integer);
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

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);

    const args = [_][]const u8{ "VISMEMBER", "myvec", "v1" };
    const result = try cmdVismember(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 1), result.integer);
}

test "cmdVismember: not exists" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);

    const args = [_][]const u8{ "VISMEMBER", "myvec", "v2" };
    const result = try cmdVismember(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 0), result.integer);
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

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const a2 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "3.0", "4.0", "v2" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);
    const r2 = try cmdVadd(allocator, &storage, &a2, 3); defer r2.deinit(allocator);

    const args = [_][]const u8{ "VRANDMEMBER", "myvec" };
    const result = try cmdVrandmember(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .bulk_string);
    const valid = std.mem.eql(u8, result.bulk_string, "v1") or std.mem.eql(u8, result.bulk_string, "v2");
    try std.testing.expect(valid);
}

test "cmdVrandmember: with positive count" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const a2 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "3.0", "4.0", "v2" };
    const a3 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "5.0", "6.0", "v3" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);
    const r2 = try cmdVadd(allocator, &storage, &a2, 3); defer r2.deinit(allocator);
    const r3 = try cmdVadd(allocator, &storage, &a3, 3); defer r3.deinit(allocator);

    const args = [_][]const u8{ "VRANDMEMBER", "myvec", "2" };
    const result = try cmdVrandmember(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 2), result.array.len);
}

test "cmdVrandmember: with negative count allows duplicates" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);

    const args = [_][]const u8{ "VRANDMEMBER", "myvec", "-3" };
    const result = try cmdVrandmember(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 3), result.array.len);
    for (result.array) |elem| {
        try std.testing.expectEqualStrings("v1", elem.bulk_string);
    }
}

test "cmdVrandmember: count exceeds size" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const a2 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "3.0", "4.0", "v2" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);
    const r2 = try cmdVadd(allocator, &storage, &a2, 3); defer r2.deinit(allocator);

    const args = [_][]const u8{ "VRANDMEMBER", "myvec", "5" };
    const result = try cmdVrandmember(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 2), result.array.len);
}

test "cmdVrandmember: count zero" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);

    const args = [_][]const u8{ "VRANDMEMBER", "myvec", "0" };
    const result = try cmdVrandmember(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 0), result.array.len);
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

// ============================================================================
// VGETATTR tests (Redis 8.0: 3-arg form — cmd key member)
// ============================================================================

test "cmdVgetattr: returns stored blob" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);

    const sa = [_][]const u8{ "VSETATTR", "myvec", "v1", "{\"category\":\"test\"}" };
    const sr = try cmdVsetattr(allocator, &storage, &sa, 3); defer sr.deinit(allocator);

    const ga = [_][]const u8{ "VGETATTR", "myvec", "v1" };
    const gr = try cmdVgetattr(allocator, &storage, &ga, 3);
    defer gr.deinit(allocator);
    try std.testing.expect(gr == .bulk_string);
    try std.testing.expectEqualStrings("{\"category\":\"test\"}", gr.bulk_string);
}

test "cmdVgetattr: no attribute returns nil" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);

    const ga = [_][]const u8{ "VGETATTR", "myvec", "v1" };
    const gr = try cmdVgetattr(allocator, &storage, &ga, 3);
    defer gr.deinit(allocator);
    try std.testing.expect(gr == .null_bulk_string);
}

test "cmdVgetattr: nonexistent member returns nil" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);

    const ga = [_][]const u8{ "VGETATTR", "myvec", "v_none" };
    const gr = try cmdVgetattr(allocator, &storage, &ga, 3);
    defer gr.deinit(allocator);
    try std.testing.expect(gr == .null_bulk_string);
}

test "cmdVgetattr: nonexistent key returns nil" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const ga = [_][]const u8{ "VGETATTR", "nonexistent", "v1" };
    const gr = try cmdVgetattr(allocator, &storage, &ga, 3);
    defer gr.deinit(allocator);
    try std.testing.expect(gr == .null_bulk_string);
}

test "cmdVgetattr: wrong type" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.set("mykey", .{ .string = try allocator.dupe(u8, "value") });
    const ga = [_][]const u8{ "VGETATTR", "mykey", "v1" };
    const gr = try cmdVgetattr(allocator, &storage, &ga, 3);
    defer gr.deinit(allocator);
    try std.testing.expect(gr == .err);
}

test "cmdVgetattr: wrong arity" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const ga = [_][]const u8{ "VGETATTR", "myvec" };
    const gr = try cmdVgetattr(allocator, &storage, &ga, 3);
    defer gr.deinit(allocator);
    try std.testing.expect(gr == .err);
}

// ============================================================================
// VSETATTR tests (Redis 8.0: 4-arg form — cmd key member blob)
// ============================================================================

test "cmdVsetattr: set blob returns 1" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);

    const sa = [_][]const u8{ "VSETATTR", "myvec", "v1", "{\"x\":1}" };
    const sr = try cmdVsetattr(allocator, &storage, &sa, 3);
    defer sr.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 1), sr.integer);
}

test "cmdVsetattr: replace blob" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);

    const sa1 = [_][]const u8{ "VSETATTR", "myvec", "v1", "old" };
    const sr1 = try cmdVsetattr(allocator, &storage, &sa1, 3); defer sr1.deinit(allocator);
    const sa2 = [_][]const u8{ "VSETATTR", "myvec", "v1", "new" };
    const sr2 = try cmdVsetattr(allocator, &storage, &sa2, 3); defer sr2.deinit(allocator);

    const ga = [_][]const u8{ "VGETATTR", "myvec", "v1" };
    const gr = try cmdVgetattr(allocator, &storage, &ga, 3);
    defer gr.deinit(allocator);
    try std.testing.expectEqualStrings("new", gr.bulk_string);
}

test "cmdVsetattr: nonexistent member returns 0" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);

    const sa = [_][]const u8{ "VSETATTR", "myvec", "v_none", "blob" };
    const sr = try cmdVsetattr(allocator, &storage, &sa, 3);
    defer sr.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 0), sr.integer);
}

test "cmdVsetattr: nonexistent key returns error" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const sa = [_][]const u8{ "VSETATTR", "nonexistent", "v1", "blob" };
    const sr = try cmdVsetattr(allocator, &storage, &sa, 3);
    defer sr.deinit(allocator);
    try std.testing.expect(sr == .err);
}

test "cmdVsetattr: wrong type" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.set("mykey", .{ .string = try allocator.dupe(u8, "value") });
    const sa = [_][]const u8{ "VSETATTR", "mykey", "v1", "blob" };
    const sr = try cmdVsetattr(allocator, &storage, &sa, 3);
    defer sr.deinit(allocator);
    try std.testing.expect(sr == .err);
}

test "cmdVsetattr: wrong arity" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const sa = [_][]const u8{ "VSETATTR", "myvec", "v1" };
    const sr = try cmdVsetattr(allocator, &storage, &sa, 3);
    defer sr.deinit(allocator);
    try std.testing.expect(sr == .err);
}

// ============================================================================
// VINFO tests
// ============================================================================

test "cmdVinfo: basic metadata" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "3", "1.0", "2.0", "3.0", "v1" };
    const a2 = [_][]const u8{ "VADD", "myvec", "VALUES", "3", "4.0", "5.0", "6.0", "v2" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);
    const r2 = try cmdVadd(allocator, &storage, &a2, 3); defer r2.deinit(allocator);

    const args = [_][]const u8{ "VINFO", "myvec" };
    const result = try cmdVinfo(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 8), result.array.len);
    try std.testing.expectEqualStrings("dimensionality", result.array[0].bulk_string);
    try std.testing.expectEqual(@as(i64, 3), result.array[1].integer);
    try std.testing.expectEqualStrings("metric", result.array[2].bulk_string);
    try std.testing.expectEqualStrings("COSINE", result.array[3].bulk_string);
    try std.testing.expectEqualStrings("count", result.array[4].bulk_string);
    try std.testing.expectEqual(@as(i64, 2), result.array[5].integer);
    try std.testing.expectEqualStrings("quantization", result.array[6].bulk_string);
    try std.testing.expectEqualStrings("FP32", result.array[7].bulk_string);
}

test "cmdVinfo: metric is COSINE (default)" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);

    const args = [_][]const u8{ "VINFO", "myvec" };
    const result = try cmdVinfo(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expectEqualStrings("COSINE", result.array[3].bulk_string);
}

test "cmdVinfo: empty vector set shows dim and count=0" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "4", "1.0", "2.0", "3.0", "4.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);
    const rem = [_][]const u8{ "VREM", "myvec", "v1" };
    const rr = try cmdVrem(allocator, &storage, &rem, 3); defer rr.deinit(allocator);

    const args = [_][]const u8{ "VINFO", "myvec" };
    const result = try cmdVinfo(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 4), result.array[1].integer);
    try std.testing.expectEqual(@as(i64, 0), result.array[5].integer);
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

// ============================================================================
// VSIM tests — Redis 8.0 syntax: VSIM key ELE/VALUES ... [COUNT n] [WITHSCORES]
// ============================================================================

test "cmdVsim: ELE basic search" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // v1 at origin, v2 close, v3 far
    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "0.0", "v1" };
    const a2 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "0.9", "0.1", "v2" };
    const a3 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "0.0", "1.0", "v3" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);
    const r2 = try cmdVadd(allocator, &storage, &a2, 3); defer r2.deinit(allocator);
    const r3 = try cmdVadd(allocator, &storage, &a3, 3); defer r3.deinit(allocator);

    const args = [_][]const u8{ "VSIM", "myvec", "ELE", "v1", "COUNT", "3" };
    const result = try cmdVsim(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 3), result.array.len);
    // v1 should be first (score 1.0 — identical to itself)
    try std.testing.expectEqualStrings("v1", result.array[0].bulk_string);
}

test "cmdVsim: ELE with WITHSCORES returns flat array" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "0.0", "v1" };
    const a2 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "0.0", "1.0", "v2" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);
    const r2 = try cmdVadd(allocator, &storage, &a2, 3); defer r2.deinit(allocator);

    const args = [_][]const u8{ "VSIM", "myvec", "ELE", "v1", "COUNT", "2", "WITHSCORES" };
    const result = try cmdVsim(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .array);
    // Flat: [id1, score1, id2, score2] = 4 elements
    try std.testing.expectEqual(@as(usize, 4), result.array.len);
    try std.testing.expectEqualStrings("v1", result.array[0].bulk_string);
    // score for v1 (identical) should be 1.0
    try std.testing.expect(result.array[1] == .bulk_string);
}

test "cmdVsim: VALUES query" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "0.0", "v1" };
    const a2 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "0.0", "1.0", "v2" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);
    const r2 = try cmdVadd(allocator, &storage, &a2, 3); defer r2.deinit(allocator);

    // Query with a vector close to v1
    const args = [_][]const u8{ "VSIM", "myvec", "VALUES", "2", "0.99", "0.01", "COUNT", "2" };
    const result = try cmdVsim(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 2), result.array.len);
    // v1 should rank higher since the query vector is close to it
    try std.testing.expectEqualStrings("v1", result.array[0].bulk_string);
}

test "cmdVsim: nonexistent key returns empty array" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "VSIM", "nonexistent", "ELE", "v1" };
    const result = try cmdVsim(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 0), result.array.len);
}

test "cmdVsim: ELE nonexistent member returns error" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);

    const args = [_][]const u8{ "VSIM", "myvec", "ELE", "nonexistent" };
    const result = try cmdVsim(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .err);
}

test "cmdVsim: COUNT limits results" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "0.0", "v1" };
    const a2 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "0.9", "0.1", "v2" };
    const a3 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "0.8", "0.2", "v3" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);
    const r2 = try cmdVadd(allocator, &storage, &a2, 3); defer r2.deinit(allocator);
    const r3 = try cmdVadd(allocator, &storage, &a3, 3); defer r3.deinit(allocator);

    const args = [_][]const u8{ "VSIM", "myvec", "ELE", "v1", "COUNT", "2" };
    const result = try cmdVsim(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 2), result.array.len);
}

test "cmdVsim: WITHATTRIBS returns attribute blobs" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "0.0", "v1", "SETATTR", "meta1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);

    const args = [_][]const u8{ "VSIM", "myvec", "ELE", "v1", "COUNT", "1", "WITHATTRIBS" };
    const result = try cmdVsim(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .array);
    // Flat: [id1, attr1] = 2 elements
    try std.testing.expectEqual(@as(usize, 2), result.array.len);
    try std.testing.expectEqualStrings("v1", result.array[0].bulk_string);
    try std.testing.expectEqualStrings("meta1", result.array[1].bulk_string);
}

// ============================================================================
// VRANGE tests
// ============================================================================

test "cmdVrange: basic range" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const a2 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "3.0", "4.0", "v2" };
    const a3 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "5.0", "6.0", "v3" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);
    const r2 = try cmdVadd(allocator, &storage, &a2, 3); defer r2.deinit(allocator);
    const r3 = try cmdVadd(allocator, &storage, &a3, 3); defer r3.deinit(allocator);

    const args = [_][]const u8{ "VRANGE", "myvec", "0", "1" };
    const result = try cmdVrange(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 2), result.array.len);
}

test "cmdVrange: with WITHEMBEDDINGS flag" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);

    const args = [_][]const u8{ "VRANGE", "myvec", "0", "0", "WITHEMBEDDINGS" };
    const result = try cmdVrange(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 1), result.array.len);
    try std.testing.expect(result.array[0] == .array);
    try std.testing.expectEqual(@as(usize, 2), result.array[0].array.len); // [id, embedding]
}

// ============================================================================
// VLINKS tests
// ============================================================================

test "cmdVlinks: stub returns empty array" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);

    const args = [_][]const u8{ "VLINKS", "myvec", "v1" };
    const result = try cmdVlinks(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 0), result.array.len);
}

test "cmdVlinks: nonexistent member" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const a1 = [_][]const u8{ "VADD", "myvec", "VALUES", "2", "1.0", "2.0", "v1" };
    const r1 = try cmdVadd(allocator, &storage, &a1, 3); defer r1.deinit(allocator);

    const args = [_][]const u8{ "VLINKS", "myvec", "nonexistent" };
    const result = try cmdVlinks(allocator, &storage, &args, 3);
    defer result.deinit(allocator);
    try std.testing.expect(result == .err);
}
