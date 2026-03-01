const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");
const streams = @import("streams.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;
const StreamId = storage_mod.Value.StreamId;
const StreamEntry = storage_mod.Value.StreamEntry;

/// XGROUP CREATE key groupname <id | $> [MKSTREAM]
/// XGROUP DESTROY key groupname
/// XGROUP SETID key groupname <id | $>
/// XGROUP CREATECONSUMER key groupname consumername
/// XGROUP DELCONSUMER key groupname consumername
pub fn cmdXgroup(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'xgroup' command");
    }

    const subcommand = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid subcommand"),
    };

    if (std.ascii.eqlIgnoreCase(subcommand, "CREATE")) {
        if (args.len < 4) {
            return w.writeError("ERR wrong number of arguments for 'xgroup create' command");
        }

        const key = switch (args[2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        const groupname = switch (args[3]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid group name"),
        };

        const id_str = switch (args[4]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid ID"),
        };

        // Check for MKSTREAM option
        var mkstream = false;
        if (args.len >= 6) {
            const opt = switch (args[5]) {
                .bulk_string => |s| s,
                else => "",
            };
            if (std.ascii.eqlIgnoreCase(opt, "MKSTREAM")) {
                mkstream = true;
            }
        }

        storage.xgroupCreate(key, groupname, id_str) catch |err| switch (err) {
            error.NoKey => {
                if (mkstream) {
                    // Create empty stream first
                    _ = try storage.xadd(key, "0-1", &[_][]const u8{ "placeholder", "value" }, null);
                    // Delete the placeholder entry
                    _ = try storage.xdel(key, &[_][]const u8{"0-1"});
                    // Now create the group
                    try storage.xgroupCreate(key, groupname, id_str);
                } else {
                    return w.writeError("ERR no such key");
                }
            },
            error.GroupExists => return w.writeError("BUSYGROUP Consumer Group name already exists"),
            error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
            else => |e| return e,
        };

        return w.writeSimpleString("OK");
    } else if (std.ascii.eqlIgnoreCase(subcommand, "DESTROY")) {
        if (args.len < 4) {
            return w.writeError("ERR wrong number of arguments for 'xgroup destroy' command");
        }

        const key = switch (args[2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        const groupname = switch (args[3]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid group name"),
        };

        const destroyed = storage.xgroupDestroy(key, groupname) catch |err| switch (err) {
            error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
            else => |e| return e,
        };

        return w.writeInteger(if (destroyed) 1 else 0);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "SETID")) {
        if (args.len < 5) {
            return w.writeError("ERR wrong number of arguments for 'xgroup setid' command");
        }

        const key = switch (args[2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        const groupname = switch (args[3]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid group name"),
        };

        const id_str = switch (args[4]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid ID"),
        };

        storage.xgroupSetId(key, groupname, id_str) catch |err| switch (err) {
            error.NoKey => return w.writeError("ERR no such key"),
            error.NoGroup => return w.writeError("NOGROUP No such consumer group for this key"),
            error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
            else => |e| return e,
        };

        return w.writeSimpleString("OK");
    } else if (std.ascii.eqlIgnoreCase(subcommand, "CREATECONSUMER")) {
        if (args.len < 5) {
            return w.writeError("ERR wrong number of arguments for 'xgroup createconsumer' command");
        }

        const key = switch (args[2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        const groupname = switch (args[3]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid group name"),
        };

        const consumername = switch (args[4]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid consumer name"),
        };

        const created = storage.xgroupCreateConsumer(key, groupname, consumername) catch |err| switch (err) {
            error.NoKey => return w.writeError("ERR no such key"),
            error.NoGroup => return w.writeError("NOGROUP No such consumer group for this key"),
            error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
            else => |e| return e,
        };

        return w.writeInteger(if (created) 1 else 0);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "DELCONSUMER")) {
        if (args.len < 5) {
            return w.writeError("ERR wrong number of arguments for 'xgroup delconsumer' command");
        }

        const key = switch (args[2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        const groupname = switch (args[3]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid group name"),
        };

        const consumername = switch (args[4]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid consumer name"),
        };

        const pending_count = storage.xgroupDelConsumer(key, groupname, consumername) catch |err| switch (err) {
            error.NoKey => return w.writeError("ERR no such key"),
            error.NoGroup => return w.writeError("NOGROUP No such consumer group for this key"),
            error.NoConsumer => return w.writeError("ERR No such consumer in this group"),
            error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
            else => |e| return e,
        };

        return w.writeInteger(pending_count);
    } else {
        return w.writeError("ERR unknown XGROUP subcommand");
    }
}

/// XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
pub fn cmdXread(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'xread' command");
    }

    var count: ?usize = null;
    var streams_idx: ?usize = null;

    // Parse optional arguments
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        if (std.ascii.eqlIgnoreCase(arg, "COUNT")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const count_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            count = std.fmt.parseInt(usize, count_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
        } else if (std.ascii.eqlIgnoreCase(arg, "BLOCK")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const _block_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            // Parse but ignore BLOCK for now (blocking not implemented)
            _ = std.fmt.parseInt(i64, _block_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
        } else if (std.ascii.eqlIgnoreCase(arg, "STREAMS")) {
            streams_idx = i + 1;
            break;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    if (streams_idx == null) {
        return w.writeError("ERR syntax error");
    }

    const start_idx = streams_idx.?;
    const remaining = args.len - start_idx;
    if (remaining < 2 or remaining % 2 != 0) {
        return w.writeError("ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified");
    }

    const num_streams = remaining / 2;
    const keys = args[start_idx .. start_idx + num_streams];
    const ids = args[start_idx + num_streams ..];

    // Build response as array of [key, entries] pairs
    var has_data = false;
    var result_buf = std.ArrayList(u8){};
    defer result_buf.deinit(allocator);
    const result_writer = result_buf.writer(allocator);

    var stream_count: usize = 0;
    var temp_results = std.ArrayList(struct { key: []const u8, entries: std.ArrayList(StreamEntry) }){};
    defer {
        for (temp_results.items) |*item| {
            for (item.entries.items) |*entry| {
                var e = entry.*;
                e.deinit(allocator);
            }
            item.entries.deinit(allocator);
        }
        temp_results.deinit(allocator);
    }

    for (keys, ids) |key_val, id_val| {
        const key = switch (key_val) {
            .bulk_string => |s| s,
            else => continue,
        };
        const id_str = switch (id_val) {
            .bulk_string => |s| s,
            else => continue,
        };

        // Parse start ID ($ means read from end)
        var entries = blk: {
            if (std.mem.eql(u8, id_str, "$")) {
                // Return empty - $ means "only new messages from now"
                break :blk std.ArrayList(StreamEntry){};
            }

            // Use xrange internally - need to convert entries to ArrayList
            const raw_entries = storage.xrange(allocator, key, id_str, "+", count) catch |err| switch (err) {
                error.WrongType => continue,
                else => |e| return e,
            } orelse break :blk std.ArrayList(StreamEntry){};
            defer allocator.free(raw_entries);

            // Convert slice to ArrayList
            var result = std.ArrayList(StreamEntry){};
            for (raw_entries) |entry| {
                // Clone fields
                var cloned_fields = std.ArrayList([]const u8){};
                for (entry.fields.items) |field| {
                    const owned = try allocator.dupe(u8, field);
                    try cloned_fields.append(allocator, owned);
                }

                try result.append(allocator, StreamEntry{
                    .id = entry.id,
                    .fields = cloned_fields,
                });
            }

            break :blk result;
        };

        if (entries.items.len > 0) {
            try temp_results.append(allocator, .{ .key = key, .entries = entries });
            stream_count += 1;
            has_data = true;
        } else {
            entries.deinit(allocator);
        }
    }

    if (!has_data) {
        return w.writeNull();
    }

    // Write array of streams
    try result_writer.print("*{d}\r\n", .{stream_count});

    for (temp_results.items) |item| {
        // Each stream is [key, array_of_entries]
        try result_writer.writeAll("*2\r\n");
        try result_writer.print("${d}\r\n{s}\r\n", .{ item.key.len, item.key });

        // Array of entries
        try result_writer.print("*{d}\r\n", .{item.entries.items.len});
        for (item.entries.items) |entry| {
            // Each entry is [id, [field, value, field, value, ...]]
            try result_writer.writeAll("*2\r\n");

            // ID
            const id_formatted = try entry.id.format(allocator);
            defer allocator.free(id_formatted);
            try result_writer.print("${d}\r\n{s}\r\n", .{ id_formatted.len, id_formatted });

            // Fields array
            try result_writer.print("*{d}\r\n", .{entry.fields.items.len});
            for (entry.fields.items) |field| {
                try result_writer.print("${d}\r\n{s}\r\n", .{ field.len, field });
            }
        }
    }

    return result_buf.toOwnedSlice(allocator);
}

/// XREADGROUP GROUP groupname consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]
pub fn cmdXreadgroup(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 7) {
        return w.writeError("ERR wrong number of arguments for 'xreadgroup' command");
    }

    // Expect GROUP groupname consumer
    const group_arg = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };
    if (!std.ascii.eqlIgnoreCase(group_arg, "GROUP")) {
        return w.writeError("ERR syntax error");
    }

    const groupname = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    const consumer = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    var count: ?usize = null;
    var noack = false;
    var streams_idx: ?usize = null;

    // Parse optional arguments
    var i: usize = 4;
    while (i < args.len) : (i += 1) {
        const arg = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        if (std.ascii.eqlIgnoreCase(arg, "COUNT")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const count_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            count = std.fmt.parseInt(usize, count_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
        } else if (std.ascii.eqlIgnoreCase(arg, "BLOCK")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const _block_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            // Parse but ignore BLOCK for now (blocking not implemented)
            _ = std.fmt.parseInt(i64, _block_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
        } else if (std.ascii.eqlIgnoreCase(arg, "NOACK")) {
            noack = true;
        } else if (std.ascii.eqlIgnoreCase(arg, "STREAMS")) {
            streams_idx = i + 1;
            break;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    if (streams_idx == null) {
        return w.writeError("ERR syntax error");
    }

    const start_idx = streams_idx.?;
    const remaining = args.len - start_idx;
    if (remaining < 2 or remaining % 2 != 0) {
        return w.writeError("ERR Unbalanced XREADGROUP list of streams: for each stream key an ID or '>' must be specified");
    }

    const num_streams = remaining / 2;
    const keys = args[start_idx .. start_idx + num_streams];
    const ids = args[start_idx + num_streams ..];

    // Build response
    var has_data = false;
    var result_buf = std.ArrayList(u8){};
    defer result_buf.deinit(allocator);
    const result_writer = result_buf.writer(allocator);

    var stream_count: usize = 0;
    var temp_results = std.ArrayList(struct { key: []const u8, entries: std.ArrayList(StreamEntry) }){};
    defer {
        for (temp_results.items) |*item| {
            for (item.entries.items) |*entry| {
                var e = entry.*;
                e.deinit(allocator);
            }
            item.entries.deinit(allocator);
        }
        temp_results.deinit(allocator);
    }

    for (keys, ids) |key_val, id_val| {
        const key = switch (key_val) {
            .bulk_string => |s| s,
            else => continue,
        };
        const id_str = switch (id_val) {
            .bulk_string => |s| s,
            else => continue,
        };

        var entries = storage.xreadgroup(allocator, groupname, consumer, key, id_str, count, noack) catch |err| switch (err) {
            error.NoGroup => return w.writeError("NOGROUP No such consumer group for this key"),
            error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
            else => |e| return e,
        } orelse continue;

        if (entries.items.len > 0) {
            try temp_results.append(allocator, .{ .key = key, .entries = entries });
            stream_count += 1;
            has_data = true;
        } else {
            entries.deinit(allocator);
        }
    }

    if (!has_data) {
        return w.writeNull();
    }

    // Write array of streams (same format as XREAD)
    try result_writer.print("*{d}\r\n", .{stream_count});

    for (temp_results.items) |item| {
        try result_writer.writeAll("*2\r\n");
        try result_writer.print("${d}\r\n{s}\r\n", .{ item.key.len, item.key });

        try result_writer.print("*{d}\r\n", .{item.entries.items.len});
        for (item.entries.items) |entry| {
            try result_writer.writeAll("*2\r\n");

            const id_formatted = try entry.id.format(allocator);
            defer allocator.free(id_formatted);
            try result_writer.print("${d}\r\n{s}\r\n", .{ id_formatted.len, id_formatted });

            try result_writer.print("*{d}\r\n", .{entry.fields.items.len});
            for (entry.fields.items) |field| {
                try result_writer.print("${d}\r\n{s}\r\n", .{ field.len, field });
            }
        }
    }

    return result_buf.toOwnedSlice(allocator);
}

/// XACK key groupname id [id ...]
pub fn cmdXack(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'xack' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const groupname = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid group name"),
    };

    // Extract IDs
    var ids = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 3);
    defer ids.deinit(allocator);

    for (args[3..]) |arg| {
        const id_str = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid ID"),
        };
        try ids.append(allocator, id_str);
    }

    const acked = storage.xack(key, groupname, ids.items) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => |e| return e,
    };

    return w.writeInteger(@intCast(acked));
}

// Unit tests

test "XGROUP CREATE - creates consumer group" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create stream first
    const xadd_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "field1" },
        RespValue{ .bulk_string = "value1" },
    };
    const xadd_result = try @import("streams.zig").cmdXadd(allocator, storage, &xadd_args);
    defer allocator.free(xadd_result);

    // Create consumer group
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XGROUP" },
        RespValue{ .bulk_string = "CREATE" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "mygroup" },
        RespValue{ .bulk_string = "0" },
    };
    const result = try cmdXgroup(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "+OK\r\n") != null);
}

test "XREADGROUP - reads new messages" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create stream
    const xadd_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "field1" },
        RespValue{ .bulk_string = "value1" },
    };
    const xadd_result = try @import("streams.zig").cmdXadd(allocator, storage, &xadd_args);
    defer allocator.free(xadd_result);

    // Create consumer group from beginning
    const xgroup_args = [_]RespValue{
        RespValue{ .bulk_string = "XGROUP" },
        RespValue{ .bulk_string = "CREATE" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "mygroup" },
        RespValue{ .bulk_string = "0" },
    };
    const xgroup_result = try cmdXgroup(allocator, storage, &xgroup_args);
    defer allocator.free(xgroup_result);

    // Read messages
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XREADGROUP" },
        RespValue{ .bulk_string = "GROUP" },
        RespValue{ .bulk_string = "mygroup" },
        RespValue{ .bulk_string = "consumer1" },
        RespValue{ .bulk_string = "STREAMS" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = ">" },
    };
    const result = try cmdXreadgroup(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "field1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "value1") != null);
}

test "XACK - acknowledges messages" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create stream and add entry
    const xadd_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "1000-0" },
        RespValue{ .bulk_string = "field1" },
        RespValue{ .bulk_string = "value1" },
    };
    const xadd_result = try @import("streams.zig").cmdXadd(allocator, storage, &xadd_args);
    defer allocator.free(xadd_result);

    // Create consumer group
    const xgroup_args = [_]RespValue{
        RespValue{ .bulk_string = "XGROUP" },
        RespValue{ .bulk_string = "CREATE" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "mygroup" },
        RespValue{ .bulk_string = "0" },
    };
    const xgroup_result = try cmdXgroup(allocator, storage, &xgroup_args);
    defer allocator.free(xgroup_result);

    // Read messages (creates pending entry)
    const xreadgroup_args = [_]RespValue{
        RespValue{ .bulk_string = "XREADGROUP" },
        RespValue{ .bulk_string = "GROUP" },
        RespValue{ .bulk_string = "mygroup" },
        RespValue{ .bulk_string = "consumer1" },
        RespValue{ .bulk_string = "STREAMS" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = ">" },
    };
    const xreadgroup_result = try cmdXreadgroup(allocator, storage, &xreadgroup_args);
    defer allocator.free(xreadgroup_result);

    // Acknowledge message
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XACK" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "mygroup" },
        RespValue{ .bulk_string = "1000-0" },
    };
    const result = try cmdXack(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, ":1\r\n") != null);
}

/// XCLAIM key group consumer min-idle-time ID [ID ...] [IDLE ms] [TIME ms-unix-time]
///        [RETRYCOUNT count] [FORCE] [JUSTID]
pub fn cmdXclaim(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 6) {
        return w.writeError("ERR wrong number of arguments for 'xclaim' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const group = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid group name"),
    };

    const consumer = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid consumer name"),
    };

    const min_idle_str = switch (args[4]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid min-idle-time"),
    };
    const min_idle_time = std.fmt.parseInt(i64, min_idle_str, 10) catch {
        return w.writeError("ERR min-idle-time is not an integer or out of range");
    };
    if (min_idle_time < 0) {
        return w.writeError("ERR Invalid min-idle-time");
    }

    // Collect IDs and options
    var id_list = std.ArrayList([]const u8){};
    defer id_list.deinit(allocator);

    var idle: ?i64 = null;
    var time: ?i64 = null;
    var retrycount: ?u64 = null;
    var force = false;
    var justid = false;

    var i: usize = 5;
    while (i < args.len) : (i += 1) {
        const arg = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        if (std.ascii.eqlIgnoreCase(arg, "IDLE")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const idle_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            idle = std.fmt.parseInt(i64, idle_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
        } else if (std.ascii.eqlIgnoreCase(arg, "TIME")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const time_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            time = std.fmt.parseInt(i64, time_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
        } else if (std.ascii.eqlIgnoreCase(arg, "RETRYCOUNT")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const rc_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            retrycount = std.fmt.parseInt(u64, rc_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
        } else if (std.ascii.eqlIgnoreCase(arg, "FORCE")) {
            force = true;
        } else if (std.ascii.eqlIgnoreCase(arg, "JUSTID")) {
            justid = true;
        } else {
            // This is an ID
            try id_list.append(allocator, arg);
        }
    }

    if (id_list.items.len == 0) {
        return w.writeError("ERR wrong number of arguments for 'xclaim' command");
    }

    const entries = storage.xclaim(
        allocator,
        key,
        group,
        consumer,
        min_idle_time,
        id_list.items,
        idle,
        time,
        retrycount,
        force,
        justid,
    ) catch |err| switch (err) {
        error.NoKey => return w.writeError("ERR no such key"),
        error.NoGroup => return w.writeError("NOGROUP No such consumer group for this key"),
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => |e| return e,
    };

    if (entries == null) {
        return w.writeArray(&[_]RespValue{});
    }

    var result = entries.?;
    defer {
        for (result.items) |*entry| {
            entry.deinit(allocator);
        }
        result.deinit(allocator);
    }

    // Format response manually using RESP protocol
    var result_buf = std.ArrayList(u8){};
    defer result_buf.deinit(allocator);
    const result_writer = result_buf.writer(allocator);

    try result_writer.print("*{d}\r\n", .{result.items.len});
    for (result.items) |*entry| {
        if (justid) {
            // Just return the ID
            const id_str = try std.fmt.allocPrint(allocator, "{d}-{d}", .{ entry.id.ms, entry.id.seq });
            defer allocator.free(id_str);
            try result_writer.print("${d}\r\n{s}\r\n", .{ id_str.len, id_str });
        } else {
            // Return [ID, [field, value, ...]]
            try result_writer.writeAll("*2\r\n");
            const id_str = try std.fmt.allocPrint(allocator, "{d}-{d}", .{ entry.id.ms, entry.id.seq });
            defer allocator.free(id_str);
            try result_writer.print("${d}\r\n{s}\r\n", .{ id_str.len, id_str });
            try result_writer.print("*{d}\r\n", .{entry.fields.items.len});
            for (entry.fields.items) |field| {
                try result_writer.print("${d}\r\n{s}\r\n", .{ field.len, field });
            }
        }
    }

    return result_buf.toOwnedSlice(allocator);
}

/// XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
pub fn cmdXautoclaim(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 6) {
        return w.writeError("ERR wrong number of arguments for 'xautoclaim' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const group = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid group name"),
    };

    const consumer = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid consumer name"),
    };

    const min_idle_str = switch (args[4]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid min-idle-time"),
    };
    const min_idle_time = std.fmt.parseInt(i64, min_idle_str, 10) catch {
        return w.writeError("ERR min-idle-time is not an integer or out of range");
    };
    if (min_idle_time < 0) {
        return w.writeError("ERR Invalid min-idle-time");
    }

    const start = switch (args[5]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid start ID"),
    };

    // Parse options
    var count: usize = 100; // Default count
    var justid = false;

    var i: usize = 6;
    while (i < args.len) : (i += 1) {
        const arg = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        if (std.ascii.eqlIgnoreCase(arg, "COUNT")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const count_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            count = std.fmt.parseInt(usize, count_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            if (count == 0) {
                return w.writeError("ERR COUNT must be > 0");
            }
        } else if (std.ascii.eqlIgnoreCase(arg, "JUSTID")) {
            justid = true;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    const result = storage.xautoclaim(
        allocator,
        key,
        group,
        consumer,
        min_idle_time,
        start,
        count,
        justid,
    ) catch |err| switch (err) {
        error.NoKey => return w.writeError("ERR no such key"),
        error.NoGroup => return w.writeError("NOGROUP No such consumer group for this key"),
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => |e| return e,
    };

    defer {
        if (result.entries) |entries| {
            var mut_entries = entries;
            for (mut_entries.items) |*entry| {
                entry.deinit(allocator);
            }
            mut_entries.deinit(allocator);
        }
    }

    // Format response manually using RESP protocol
    // Response format: [next_cursor, [entries...]]
    var result_buf = std.ArrayList(u8){};
    defer result_buf.deinit(allocator);
    const result_writer = result_buf.writer(allocator);

    try result_writer.writeAll("*2\r\n");

    // Next cursor
    try result_writer.print("${d}\r\n{s}\r\n", .{ result.next_cursor.len, result.next_cursor });

    // Entries
    if (result.entries) |entries| {
        try result_writer.print("*{d}\r\n", .{entries.items.len});
        for (entries.items) |*entry| {
            if (justid) {
                const id_str = try std.fmt.allocPrint(allocator, "{d}-{d}", .{ entry.id.ms, entry.id.seq });
                defer allocator.free(id_str);
                try result_writer.print("${d}\r\n{s}\r\n", .{ id_str.len, id_str });
            } else {
                try result_writer.writeAll("*2\r\n");
                const id_str = try std.fmt.allocPrint(allocator, "{d}-{d}", .{ entry.id.ms, entry.id.seq });
                defer allocator.free(id_str);
                try result_writer.print("${d}\r\n{s}\r\n", .{ id_str.len, id_str });
                try result_writer.print("*{d}\r\n", .{entry.fields.items.len});
                for (entry.fields.items) |field| {
                    try result_writer.print("${d}\r\n{s}\r\n", .{ field.len, field });
                }
            }
        }
    } else {
        try result_writer.writeAll("*0\r\n");
    }

    return result_buf.toOwnedSlice(allocator);
}

test "XCLAIM basic functionality" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create stream and add entry
    const xadd_args1 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "1000-0" },
        RespValue{ .bulk_string = "field1" },
        RespValue{ .bulk_string = "value1" },
    };
    const xadd_result1 = try streams.cmdXadd(allocator, &storage, &xadd_args1);
    defer allocator.free(xadd_result1);

    // Create consumer group
    const xgroup_args = [_]RespValue{
        RespValue{ .bulk_string = "XGROUP" },
        RespValue{ .bulk_string = "CREATE" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "mygroup" },
        RespValue{ .bulk_string = "0" },
    };
    const xgroup_result = try cmdXgroup(allocator, &storage, &xgroup_args);
    defer allocator.free(xgroup_result);

    // Read message with consumer1
    const xreadgroup_args = [_]RespValue{
        RespValue{ .bulk_string = "XREADGROUP" },
        RespValue{ .bulk_string = "GROUP" },
        RespValue{ .bulk_string = "mygroup" },
        RespValue{ .bulk_string = "consumer1" },
        RespValue{ .bulk_string = "STREAMS" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = ">" },
    };
    const xreadgroup_result = try cmdXreadgroup(allocator, &storage, &xreadgroup_args);
    defer allocator.free(xreadgroup_result);

    // Sleep briefly to ensure idle time
    std.time.sleep(std.time.ns_per_ms * 10);

    // Claim message for consumer2
    const xclaim_args = [_]RespValue{
        RespValue{ .bulk_string = "XCLAIM" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "mygroup" },
        RespValue{ .bulk_string = "consumer2" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "1000-0" },
    };
    const xclaim_result = try cmdXclaim(allocator, &storage, &xclaim_args);
    defer allocator.free(xclaim_result);

    try std.testing.expect(std.mem.indexOf(u8, xclaim_result, "1000-0") != null);
    try std.testing.expect(std.mem.indexOf(u8, xclaim_result, "field1") != null);
}

test "XAUTOCLAIM basic functionality" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create stream and add entries
    const xadd_args1 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "1000-0" },
        RespValue{ .bulk_string = "field1" },
        RespValue{ .bulk_string = "value1" },
    };
    const xadd_result1 = try streams.cmdXadd(allocator, &storage, &xadd_args1);
    defer allocator.free(xadd_result1);

    // Create consumer group
    const xgroup_args = [_]RespValue{
        RespValue{ .bulk_string = "XGROUP" },
        RespValue{ .bulk_string = "CREATE" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "mygroup" },
        RespValue{ .bulk_string = "0" },
    };
    const xgroup_result = try cmdXgroup(allocator, &storage, &xgroup_args);
    defer allocator.free(xgroup_result);

    // Read message with consumer1
    const xreadgroup_args = [_]RespValue{
        RespValue{ .bulk_string = "XREADGROUP" },
        RespValue{ .bulk_string = "GROUP" },
        RespValue{ .bulk_string = "mygroup" },
        RespValue{ .bulk_string = "consumer1" },
        RespValue{ .bulk_string = "STREAMS" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = ">" },
    };
    const xreadgroup_result = try cmdXreadgroup(allocator, &storage, &xreadgroup_args);
    defer allocator.free(xreadgroup_result);

    // Sleep briefly
    std.time.sleep(std.time.ns_per_ms * 10);

    // Auto-claim for consumer2
    const xautoclaim_args = [_]RespValue{
        RespValue{ .bulk_string = "XAUTOCLAIM" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "mygroup" },
        RespValue{ .bulk_string = "consumer2" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "0-0" },
    };
    const xautoclaim_result = try cmdXautoclaim(allocator, &storage, &xautoclaim_args);
    defer allocator.free(xautoclaim_result);

    try std.testing.expect(std.mem.indexOf(u8, xautoclaim_result, "1000-0") != null);
}
