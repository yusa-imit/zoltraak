const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;
const StreamId = storage_mod.Value.StreamId;

/// XADD key <ID | *> field value [field value ...]
/// Appends a new entry to a stream.
/// ID can be "*" for auto-generation or explicit "ms-seq" format.
/// Returns the ID of the added entry.
pub fn cmdXadd(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4 or (args.len - 3) % 2 != 0) {
        return w.writeError("ERR wrong number of arguments for 'xadd' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const id_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid ID"),
    };

    // Extract field-value pairs
    const num_fields = args.len - 3;
    var fields = try std.ArrayList([]const u8).initCapacity(allocator, num_fields);
    defer fields.deinit(allocator);

    for (args[3..]) |arg| {
        const field = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid field or value"),
        };
        try fields.append(allocator, field);
    }

    // Execute XADD
    const id = storage.xadd(key, id_str, fields.items, null) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.InvalidStreamId => return w.writeError("ERR Invalid stream ID specified as stream command argument"),
        error.StreamIdTooSmall => return w.writeError("ERR The ID specified in XADD is equal or smaller than the target stream top item"),
        else => return err,
    };

    // Format ID as bulk string
    const id_formatted = try id.format(allocator);
    defer allocator.free(id_formatted);

    return w.writeBulkString(id_formatted);
}

/// XLEN key
/// Returns the number of entries in a stream.
pub fn cmdXlen(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'xlen' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const len = storage.xlen(key) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(len orelse 0));
}

/// XRANGE key start end [COUNT count]
/// Returns entries with IDs in the specified range.
/// start and end can be "-" (minimum) or "+" (maximum) or explicit IDs.
pub fn cmdXrange(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'xrange' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const start = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid start ID"),
    };

    const end = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid end ID"),
    };

    // Parse optional COUNT
    var count: ?usize = null;
    var i: usize = 4;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        if (std.ascii.eqlIgnoreCase(opt, "COUNT")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const count_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR value is not an integer or out of range"),
            };
            const count_i64 = std.fmt.parseInt(i64, count_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            if (count_i64 < 0) return w.writeError("ERR COUNT must be >= 0");
            count = @intCast(count_i64);
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Execute XRANGE
    const entries = storage.xrange(allocator, key, start, end, count) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.InvalidStreamId => return w.writeError("ERR Invalid stream ID specified as stream command argument"),
        else => return err,
    };

    if (entries == null) {
        // Key doesn't exist - return empty array
        return w.writeArray(&[_]RespValue{});
    }

    defer allocator.free(entries.?);

    // Format as array of [id, [field1, value1, field2, value2, ...]]
    var result = try std.ArrayList(RespValue).initCapacity(allocator, entries.?.len);
    defer result.deinit(allocator);

    for (entries.?) |entry| {
        // Format entry as [id, fields_array]
        const id_str = try entry.id.format(allocator);
        defer allocator.free(id_str);

        var entry_array = try std.ArrayList(RespValue).initCapacity(allocator, 2);
        defer entry_array.deinit(allocator);

        try entry_array.append(allocator, RespValue{ .bulk_string = id_str });

        // Fields array
        var fields_array = try std.ArrayList(RespValue).initCapacity(allocator, entry.fields.items.len);
        defer fields_array.deinit(allocator);

        for (entry.fields.items) |field| {
            try fields_array.append(allocator, RespValue{ .bulk_string = field });
        }

        const fields_resp = try w.writeArray(fields_array.items);
        defer allocator.free(fields_resp);

        try entry_array.append(allocator, RespValue{ .bulk_string = fields_resp });

        const entry_resp = try w.writeArray(entry_array.items);
        defer allocator.free(entry_resp);

        try result.append(allocator, RespValue{ .bulk_string = entry_resp });
    }

    return w.writeArray(result.items);
}

/// XREVRANGE key start end [COUNT count]
/// Returns entries with IDs in reverse order (newest to oldest).
/// start and end are swapped compared to XRANGE.
pub fn cmdXrevrange(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'xrevrange' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const start = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid start ID"),
    };

    const end = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid end ID"),
    };

    // Parse optional COUNT
    var count: ?usize = null;
    var i: usize = 4;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        if (std.ascii.eqlIgnoreCase(opt, "COUNT")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const count_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR value is not an integer or out of range"),
            };
            const count_i64 = std.fmt.parseInt(i64, count_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            if (count_i64 < 0) return w.writeError("ERR COUNT must be >= 0");
            count = @intCast(count_i64);
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Execute XREVRANGE
    const entries = storage.xrevrange(allocator, key, start, end, count) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.InvalidStreamId => return w.writeError("ERR Invalid stream ID specified as stream command argument"),
        else => return err,
    };

    if (entries == null) {
        return w.writeArray(&[_]RespValue{});
    }

    defer allocator.free(entries.?);

    // Format as array of [id, [field1, value1, field2, value2, ...]]
    var result = try std.ArrayList(RespValue).initCapacity(allocator, entries.?.len);
    defer result.deinit(allocator);

    for (entries.?) |entry| {
        const id_str = try entry.id.format(allocator);
        defer allocator.free(id_str);

        var entry_array = try std.ArrayList(RespValue).initCapacity(allocator, 2);
        defer entry_array.deinit(allocator);

        try entry_array.append(allocator, RespValue{ .bulk_string = id_str });

        var fields_array = try std.ArrayList(RespValue).initCapacity(allocator, entry.fields.items.len);
        defer fields_array.deinit(allocator);

        for (entry.fields.items) |field| {
            try fields_array.append(allocator, RespValue{ .bulk_string = field });
        }

        const fields_resp = try w.writeArray(fields_array.items);
        defer allocator.free(fields_resp);

        try entry_array.append(allocator, RespValue{ .bulk_string = fields_resp });

        const entry_resp = try w.writeArray(entry_array.items);
        defer allocator.free(entry_resp);

        try result.append(allocator, RespValue{ .bulk_string = entry_resp });
    }

    return w.writeArray(result.items);
}

/// XDEL key ID [ID ...]
/// Removes specific entries from a stream by ID.
/// Returns number of entries deleted.
pub fn cmdXdel(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'xdel' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Extract IDs to delete
    var ids = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 2);
    defer ids.deinit(allocator);

    for (args[2..]) |arg| {
        const id = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid ID"),
        };
        try ids.append(allocator, id);
    }

    const deleted = storage.xdel(key, ids.items) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.InvalidStreamId => return w.writeError("ERR Invalid stream ID specified as stream command argument"),
        else => return err,
    };

    return w.writeInteger(@intCast(deleted));
}

/// XTRIM key MAXLEN [~] count
/// Trims the stream to approximately the specified length.
/// Returns number of entries deleted.
pub fn cmdXtrim(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'xtrim' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const strategy = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    if (!std.ascii.eqlIgnoreCase(strategy, "MAXLEN")) {
        return w.writeError("ERR syntax error");
    }

    // Parse optional ~ (approximate) and maxlen value
    var idx: usize = 3;
    const maxlen_arg = switch (args[idx]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    // Skip ~ if present (we always trim exactly anyway)
    const maxlen_str = if (std.mem.eql(u8, maxlen_arg, "~")) blk: {
        idx += 1;
        if (idx >= args.len) return w.writeError("ERR syntax error");
        break :blk switch (args[idx]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
    } else maxlen_arg;

    const maxlen = std.fmt.parseInt(usize, maxlen_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    const deleted = storage.xtrim(key, maxlen) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => return err,
    };

    return w.writeInteger(@intCast(deleted));
}

// ── Unit tests ────────────────────────────────────────────────────────────────

test "streams - XADD with auto ID" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "temp" },
        RespValue{ .bulk_string = "25" },
        RespValue{ .bulk_string = "humidity" },
        RespValue{ .bulk_string = "60" },
    };

    const result = try cmdXadd(allocator, storage, &args);
    defer allocator.free(result);

    // Should return a bulk string with ID format "ms-seq"
    try std.testing.expect(std.mem.indexOf(u8, result, "$") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "-") != null);
}

test "streams - XADD with explicit ID" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "1234567890-0" },
        RespValue{ .bulk_string = "field1" },
        RespValue{ .bulk_string = "value1" },
    };

    const result = try cmdXadd(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "1234567890-0") != null);
}

test "streams - XADD rejects smaller ID" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args1 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "2000-0" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "1" },
    };
    const r1 = try cmdXadd(allocator, storage, &args1);
    defer allocator.free(r1);

    const args2 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "1000-0" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "2" },
    };
    const r2 = try cmdXadd(allocator, storage, &args2);
    defer allocator.free(r2);

    try std.testing.expect(std.mem.indexOf(u8, r2, "smaller") != null);
}

test "streams - XLEN returns count" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add entries
    const xadd1 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "1000-0" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "1" },
    };
    const r1 = try cmdXadd(allocator, storage, &xadd1);
    defer allocator.free(r1);

    const xadd2 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "2000-0" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "2" },
    };
    const r2 = try cmdXadd(allocator, storage, &xadd2);
    defer allocator.free(r2);

    // Check length
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XLEN" },
        RespValue{ .bulk_string = "s" },
    };
    const result = try cmdXlen(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":2\r\n", result);
}

test "streams - XLEN on non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "XLEN" },
        RespValue{ .bulk_string = "nosuchkey" },
    };
    const result = try cmdXlen(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "streams - XREVRANGE returns entries in reverse" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add entries
    const xadd1 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "1000-0" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "1" },
    };
    _ = try cmdXadd(allocator, storage, &xadd1);

    const xadd2 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "2000-0" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "2" },
    };
    _ = try cmdXadd(allocator, storage, &xadd2);

    // Query reverse range
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XREVRANGE" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "+" },
        RespValue{ .bulk_string = "-" },
    };
    const result = try cmdXrevrange(allocator, storage, &args);
    defer allocator.free(result);

    // Should contain 2000-0 before 1000-0
    const idx_2000 = std.mem.indexOf(u8, result, "2000-0").?;
    const idx_1000 = std.mem.indexOf(u8, result, "1000-0").?;
    try std.testing.expect(idx_2000 < idx_1000);
}

test "streams - XDEL removes entries" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add entries
    _ = try cmdXadd(allocator, storage, &[_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "1000-0" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "1" },
    });
    _ = try cmdXadd(allocator, storage, &[_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "2000-0" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "2" },
    });
    _ = try cmdXadd(allocator, storage, &[_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "3000-0" },
        RespValue{ .bulk_string = "c" },
        RespValue{ .bulk_string = "3" },
    });

    // Delete entry 2000-0
    const del_result = try cmdXdel(allocator, storage, &[_]RespValue{
        RespValue{ .bulk_string = "XDEL" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "2000-0" },
    });
    defer allocator.free(del_result);

    try std.testing.expectEqualStrings(":1\r\n", del_result);

    // Verify length is now 2
    const len_result = try cmdXlen(allocator, storage, &[_]RespValue{
        RespValue{ .bulk_string = "XLEN" },
        RespValue{ .bulk_string = "s" },
    });
    defer allocator.free(len_result);

    try std.testing.expectEqualStrings(":2\r\n", len_result);
}

test "streams - XTRIM limits stream length" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add 5 entries
    for (0..5) |i| {
        const id = try std.fmt.allocPrint(allocator, "{d}000-0", .{i + 1});
        defer allocator.free(id);

        _ = try cmdXadd(allocator, storage, &[_]RespValue{
            RespValue{ .bulk_string = "XADD" },
            RespValue{ .bulk_string = "s" },
            RespValue{ .bulk_string = id },
            RespValue{ .bulk_string = "f" },
            RespValue{ .bulk_string = "v" },
        });
    }

    // Trim to 3 entries
    const trim_result = try cmdXtrim(allocator, storage, &[_]RespValue{
        RespValue{ .bulk_string = "XTRIM" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "MAXLEN" },
        RespValue{ .bulk_string = "3" },
    });
    defer allocator.free(trim_result);

    try std.testing.expectEqualStrings(":2\r\n", trim_result); // Deleted 2 entries

    // Verify length is 3
    const len_result = try cmdXlen(allocator, storage, &[_]RespValue{
        RespValue{ .bulk_string = "XLEN" },
        RespValue{ .bulk_string = "s" },
    });
    defer allocator.free(len_result);

    try std.testing.expectEqualStrings(":3\r\n", len_result);
}

test "streams - XRANGE returns entries" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add entries
    const xadd1 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "1000-0" },
        RespValue{ .bulk_string = "temp" },
        RespValue{ .bulk_string = "20" },
    };
    const r1 = try cmdXadd(allocator, storage, &xadd1);
    defer allocator.free(r1);

    const xadd2 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "2000-0" },
        RespValue{ .bulk_string = "temp" },
        RespValue{ .bulk_string = "25" },
    };
    const r2 = try cmdXadd(allocator, storage, &xadd2);
    defer allocator.free(r2);

    // Query range
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XRANGE" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "-" },
        RespValue{ .bulk_string = "+" },
    };
    const result = try cmdXrange(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
}

test "streams - XRANGE with COUNT limit" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add 3 entries
    const fields = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "1000-0" },
        RespValue{ .bulk_string = "x" },
        RespValue{ .bulk_string = "1" },
    };
    const r1 = try cmdXadd(allocator, storage, &fields);
    defer allocator.free(r1);

    const fields2 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "2000-0" },
        RespValue{ .bulk_string = "x" },
        RespValue{ .bulk_string = "2" },
    };
    const r2 = try cmdXadd(allocator, storage, &fields2);
    defer allocator.free(r2);

    const fields3 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "3000-0" },
        RespValue{ .bulk_string = "x" },
        RespValue{ .bulk_string = "3" },
    };
    const r3 = try cmdXadd(allocator, storage, &fields3);
    defer allocator.free(r3);

    // Query with COUNT 2
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XRANGE" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "-" },
        RespValue{ .bulk_string = "+" },
        RespValue{ .bulk_string = "COUNT" },
        RespValue{ .bulk_string = "2" },
    };
    const result = try cmdXrange(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
}
