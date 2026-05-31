const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");
const notifications_mod = @import("../storage/notifications.zig");
const pubsub_mod = @import("../storage/pubsub.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;
const StreamId = storage_mod.Value.StreamId;
const PubSub = pubsub_mod.PubSub;

/// Publish keyspace notification for a stream command
fn notifyStreamEvent(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub_state: *PubSub,
    db_index: u32,
    key: []const u8,
    event_name: []const u8,
) void {
    const config_value = storage.config.getAsString("notify-keyspace-events") catch return;
    const config_str = config_value orelse return;
    const flags = notifications_mod.parseNotificationFlags(config_str);

    if (!notifications_mod.shouldNotify(flags, .stream)) return;

    notifications_mod.publishNotification(allocator, pubsub_state, db_index, key, event_name, flags) catch {};
}

/// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] *|id field value [field value ...]
/// Appends a new entry to a stream with optional trimming.
/// ID can be "*" for auto-generation or explicit "ms-seq" format.
/// Returns the ID of the added entry or nil if NOMKSTREAM and stream doesn't exist.
pub fn cmdXadd(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'xadd' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse options and find where the ID starts
    var opts = storage_mod.XAddOptions{};
    var idx: usize = 2;

    while (idx < args.len) {
        const arg = switch (args[idx]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        if (std.ascii.eqlIgnoreCase(arg, "NOMKSTREAM")) {
            opts.nomkstream = true;
            idx += 1;
        } else if (std.ascii.eqlIgnoreCase(arg, "MAXLEN")) {
            idx += 1;
            if (idx >= args.len) return w.writeError("ERR syntax error");

            // Check for ~ or =
            const threshold_arg = switch (args[idx]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };

            const maxlen_str = if (std.mem.eql(u8, threshold_arg, "~")) blk: {
                opts.approx = true;
                idx += 1;
                if (idx >= args.len) return w.writeError("ERR syntax error");
                break :blk switch (args[idx]) {
                    .bulk_string => |s| s,
                    else => return w.writeError("ERR syntax error"),
                };
            } else if (std.mem.eql(u8, threshold_arg, "=")) blk: {
                idx += 1;
                if (idx >= args.len) return w.writeError("ERR syntax error");
                break :blk switch (args[idx]) {
                    .bulk_string => |s| s,
                    else => return w.writeError("ERR syntax error"),
                };
            } else threshold_arg;

            opts.maxlen = std.fmt.parseInt(usize, maxlen_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            idx += 1;

            // Check for LIMIT
            if (idx < args.len) {
                const next_arg = switch (args[idx]) {
                    .bulk_string => |s| s,
                    else => return w.writeError("ERR syntax error"),
                };
                if (std.ascii.eqlIgnoreCase(next_arg, "LIMIT")) {
                    idx += 1;
                    if (idx >= args.len) return w.writeError("ERR syntax error");
                    const limit_str = switch (args[idx]) {
                        .bulk_string => |s| s,
                        else => return w.writeError("ERR syntax error"),
                    };
                    opts.limit = std.fmt.parseInt(usize, limit_str, 10) catch {
                        return w.writeError("ERR value is not an integer or out of range");
                    };
                    idx += 1;
                }
            }
        } else if (std.ascii.eqlIgnoreCase(arg, "MINID")) {
            idx += 1;
            if (idx >= args.len) return w.writeError("ERR syntax error");

            // Check for ~ or =
            const threshold_arg = switch (args[idx]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };

            const minid_str = if (std.mem.eql(u8, threshold_arg, "~")) blk: {
                opts.approx = true;
                idx += 1;
                if (idx >= args.len) return w.writeError("ERR syntax error");
                break :blk switch (args[idx]) {
                    .bulk_string => |s| s,
                    else => return w.writeError("ERR syntax error"),
                };
            } else if (std.mem.eql(u8, threshold_arg, "=")) blk: {
                idx += 1;
                if (idx >= args.len) return w.writeError("ERR syntax error");
                break :blk switch (args[idx]) {
                    .bulk_string => |s| s,
                    else => return w.writeError("ERR syntax error"),
                };
            } else threshold_arg;

            opts.minid_str = try allocator.dupe(u8, minid_str);
            idx += 1;

            // Check for LIMIT
            if (idx < args.len) {
                const next_arg = switch (args[idx]) {
                    .bulk_string => |s| s,
                    else => return w.writeError("ERR syntax error"),
                };
                if (std.ascii.eqlIgnoreCase(next_arg, "LIMIT")) {
                    idx += 1;
                    if (idx >= args.len) return w.writeError("ERR syntax error");
                    const limit_str = switch (args[idx]) {
                        .bulk_string => |s| s,
                        else => return w.writeError("ERR syntax error"),
                    };
                    opts.limit = std.fmt.parseInt(usize, limit_str, 10) catch {
                        return w.writeError("ERR value is not an integer or out of range");
                    };
                    idx += 1;
                }
            }
        } else {
            // Not an option, must be ID
            break;
        }
    }

    // Now idx should point to the ID
    if (idx >= args.len) {
        if (opts.minid_str) |m| allocator.free(m);
        return w.writeError("ERR syntax error");
    }

    const id_str = switch (args[idx]) {
        .bulk_string => |s| s,
        else => {
            if (opts.minid_str) |m| allocator.free(m);
            return w.writeError("ERR invalid ID");
        },
    };
    idx += 1;

    // Remaining args should be field-value pairs
    if ((args.len - idx) % 2 != 0) {
        if (opts.minid_str) |m| allocator.free(m);
        return w.writeError("ERR wrong number of arguments for 'xadd' command");
    }

    var fields = try std.ArrayList([]const u8).initCapacity(allocator, args.len - idx);
    defer fields.deinit(allocator);

    for (args[idx..]) |arg| {
        const field = switch (arg) {
            .bulk_string => |s| s,
            else => {
                if (opts.minid_str) |m| allocator.free(m);
                return w.writeError("ERR invalid field or value");
            },
        };
        try fields.append(allocator, field);
    }

    // Execute XADD
    const id = storage.xadd(key, id_str, fields.items, null, opts) catch |err| {
        if (opts.minid_str) |m| allocator.free(m);
        return switch (err) {
            error.WrongType => w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
            error.InvalidStreamId => w.writeError("ERR Invalid stream ID specified as stream command argument"),
            error.StreamIdTooSmall => w.writeError("ERR The ID specified in XADD is equal or smaller than the target stream top item"),
            else => err,
        };
    };

    if (opts.minid_str) |m| allocator.free(m);

    // Return nil if NOMKSTREAM and stream didn't exist
    if (id == null) {
        return w.writeNull();
    }

    // Publish notification
    notifyStreamEvent(allocator, storage, ps, db_index, key, "xadd");

    // Format ID as bulk string
    const id_formatted = try id.?.format(allocator);
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
pub fn cmdXdel(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
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

    // Publish notification if entries were deleted (NEVER fire "del" for streams)
    if (deleted > 0) {
        notifyStreamEvent(allocator, storage, ps, db_index, key, "xdel");
    }

    return w.writeInteger(@intCast(deleted));
}

/// XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]
/// Trims the stream by length (MAXLEN) or by removing old entries (MINID).
/// Returns number of entries deleted.
pub fn cmdXtrim(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
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

    var idx: usize = 3;
    var limit: ?usize = null;

    if (std.ascii.eqlIgnoreCase(strategy, "MAXLEN")) {
        // Parse optional ~ (approximate) and maxlen value
        const maxlen_arg = switch (args[idx]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        // Skip ~ or = if present
        const maxlen_str = if (std.mem.eql(u8, maxlen_arg, "~")) blk: {
            idx += 1;
            if (idx >= args.len) return w.writeError("ERR syntax error");
            break :blk switch (args[idx]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
        } else if (std.mem.eql(u8, maxlen_arg, "=")) blk: {
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
        idx += 1;

        // Check for LIMIT
        if (idx < args.len) {
            const next_arg = switch (args[idx]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            if (std.ascii.eqlIgnoreCase(next_arg, "LIMIT")) {
                idx += 1;
                if (idx >= args.len) return w.writeError("ERR syntax error");
                const limit_str = switch (args[idx]) {
                    .bulk_string => |s| s,
                    else => return w.writeError("ERR syntax error"),
                };
                limit = std.fmt.parseInt(usize, limit_str, 10) catch {
                    return w.writeError("ERR value is not an integer or out of range");
                };
            }
        }

        const deleted = storage.xtrimMaxlen(key, maxlen, limit) catch |err| switch (err) {
            error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
            else => return err,
        };

        // Publish notification if entries were trimmed
        if (deleted > 0) {
            notifyStreamEvent(allocator, storage, ps, db_index, key, "xtrim");
        }

        return w.writeInteger(@intCast(deleted));
    } else if (std.ascii.eqlIgnoreCase(strategy, "MINID")) {
        // Parse optional ~ or = and minid value
        const minid_arg = switch (args[idx]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        // Skip ~ or = if present
        const minid_str = if (std.mem.eql(u8, minid_arg, "~")) blk: {
            idx += 1;
            if (idx >= args.len) return w.writeError("ERR syntax error");
            break :blk switch (args[idx]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
        } else if (std.mem.eql(u8, minid_arg, "=")) blk: {
            idx += 1;
            if (idx >= args.len) return w.writeError("ERR syntax error");
            break :blk switch (args[idx]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
        } else minid_arg;

        idx += 1;

        // Check for LIMIT
        if (idx < args.len) {
            const next_arg = switch (args[idx]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            if (std.ascii.eqlIgnoreCase(next_arg, "LIMIT")) {
                idx += 1;
                if (idx >= args.len) return w.writeError("ERR syntax error");
                const limit_str = switch (args[idx]) {
                    .bulk_string => |s| s,
                    else => return w.writeError("ERR syntax error"),
                };
                limit = std.fmt.parseInt(usize, limit_str, 10) catch {
                    return w.writeError("ERR value is not an integer or out of range");
                };
            }
        }

        const deleted = storage.xtrimMinid(key, minid_str, limit) catch |err| switch (err) {
            error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
            error.InvalidStreamId => return w.writeError("ERR Invalid stream ID specified as stream command argument"),
            else => return err,
        };

        // Publish notification if entries were trimmed
        if (deleted > 0) {
            notifyStreamEvent(allocator, storage, ps, db_index, key, "xtrim");
        }

        return w.writeInteger(@intCast(deleted));
    } else {
        return w.writeError("ERR syntax error");
    }
}

/// XSETID key <ID | $> [ENTRIESADDED entries-added] [MAXDELETEDID max-deleted-id]
/// Set stream metadata (last_id, entries_added, max_deleted_entry_id)
/// Creates stream if it doesn't exist
/// $ placeholder uses current last_id (or 0-0 for empty streams)
pub fn cmdXsetid(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'xsetid' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const new_last_id = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid stream ID specified as stream command argument"),
    };

    // Parse optional ENTRIESADDED and MAXDELETEDID parameters
    var entries_added: ?u64 = null;
    var max_deleted_id: ?[]const u8 = null;

    var idx: usize = 3;
    while (idx < args.len) {
        const param = switch (args[idx]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        if (std.ascii.eqlIgnoreCase(param, "ENTRIESADDED")) {
            idx += 1;
            if (idx >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const value_str = switch (args[idx]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR value is not an integer or out of range"),
            };
            entries_added = std.fmt.parseInt(u64, value_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
        } else if (std.ascii.eqlIgnoreCase(param, "MAXDELETEDID")) {
            idx += 1;
            if (idx >= args.len) {
                return w.writeError("ERR syntax error");
            }
            max_deleted_id = switch (args[idx]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid stream ID specified as stream command argument"),
            };
        } else {
            return w.writeError("ERR syntax error");
        }

        idx += 1;
    }

    // Execute XSETID
    storage.xsetid(key, new_last_id, entries_added, max_deleted_id) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.InvalidStreamId, error.InvalidCharacter, error.Overflow => return w.writeError("ERR invalid stream ID specified as stream command argument"),
        else => return err,
    };

    // Publish notification (NEVER fire "del" for streams)
    notifyStreamEvent(allocator, storage, ps, db_index, key, "xsetid");

    return w.writeSimpleString("OK");
}

/// XCFGSET command handler (Redis 8.6+)
/// Configure stream IDMP (Idempotent Message Processing) settings
/// XCFGSET key [IDMP-DURATION duration] [IDMP-MAXSIZE maxsize]
pub fn cmdXcfgset(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'xcfgset' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse optional IDMP-DURATION and IDMP-MAXSIZE parameters
    var duration: ?u32 = null;
    var maxsize: ?u32 = null;

    var idx: usize = 2;
    while (idx < args.len) {
        const param = switch (args[idx]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        if (std.ascii.eqlIgnoreCase(param, "IDMP-DURATION")) {
            idx += 1;
            if (idx >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const value_str = switch (args[idx]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR value is not an integer or out of range"),
            };
            duration = std.fmt.parseInt(u32, value_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
        } else if (std.ascii.eqlIgnoreCase(param, "IDMP-MAXSIZE")) {
            idx += 1;
            if (idx >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const value_str = switch (args[idx]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR value is not an integer or out of range"),
            };
            maxsize = std.fmt.parseInt(u32, value_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
        } else {
            return w.writeError("ERR syntax error");
        }

        idx += 1;
    }

    // At least one parameter must be provided
    if (duration == null and maxsize == null) {
        return w.writeError("ERR syntax error");
    }

    // Execute XCFGSET
    storage.xcfgset(key, duration, maxsize) catch |err| switch (err) {
        error.NoSuchKey => return w.writeError("ERR no such key"),
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.InvalidDuration => return w.writeError("ERR invalid duration"),
        error.InvalidMaxsize => return w.writeError("ERR invalid maxsize"),
        else => return err,
    };

    return w.writeSimpleString("OK");
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

test "streams - XADD with partial auto-seq ID (ms-*)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "1700000000-*" },
        RespValue{ .bulk_string = "field" },
        RespValue{ .bulk_string = "value" },
    };

    const result = try cmdXadd(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    // Should return "1700000000-0"
    try std.testing.expect(std.mem.indexOf(u8, result, "1700000000-0") != null);
}

test "streams - XADD partial auto-seq increments within same ms" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const args1 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "5000-2" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "1" },
    };
    const r1 = try cmdXadd(allocator, storage, &args1, &ps, 0);
    defer allocator.free(r1);

    const args2 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "5000-*" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "2" },
    };
    const r2 = try cmdXadd(allocator, storage, &args2, &ps, 0);
    defer allocator.free(r2);

    // Should return "5000-3"
    try std.testing.expect(std.mem.indexOf(u8, r2, "5000-3") != null);
}

test "streams - XADD partial auto-seq with new ms resets to 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const args1 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "5000-7" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "1" },
    };
    const r1 = try cmdXadd(allocator, storage, &args1, &ps, 0);
    defer allocator.free(r1);

    const args2 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "6000-*" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "2" },
    };
    const r2 = try cmdXadd(allocator, storage, &args2, &ps, 0);
    defer allocator.free(r2);

    // Should return "6000-0"
    try std.testing.expect(std.mem.indexOf(u8, r2, "6000-0") != null);
}

test "streams - XADD partial auto-seq rejects older ms" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const args1 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "9000-5" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "1" },
    };
    const r1 = try cmdXadd(allocator, storage, &args1, &ps, 0);
    defer allocator.free(r1);

    const args2 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "8000-*" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "2" },
    };
    const r2 = try cmdXadd(allocator, storage, &args2, &ps, 0);
    defer allocator.free(r2);

    // Should return an error about smaller ID
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

/// XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
/// Returns pending entries information for a consumer group
pub fn cmdXpending(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'xpending' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const group_name = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid group name"),
    };

    // Simple form: XPENDING key group
    if (args.len == 3) {
        const info = storage.xpendingSummary(allocator, key, group_name) catch |err| switch (err) {
            error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
            error.NoSuchKey => return w.writeArray(null),
            error.NoSuchGroup => return w.writeError("NOGROUP No such consumer group"),
            else => return err,
        };
        defer allocator.free(info);

        return w.writeSimpleString(info);
    }

    // Extended form: XPENDING key group [IDLE min-idle] start end count [consumer]
    var start_idx: usize = 3;
    var idle_time: ?i64 = null;
    var consumer_name: ?[]const u8 = null;

    // Parse optional IDLE parameter
    if (args.len > 4) {
        const maybe_idle = switch (args[3]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid argument"),
        };
        if (std.mem.eql(u8, maybe_idle, "IDLE")) {
            if (args.len < 6) {
                return w.writeError("ERR wrong number of arguments for 'xpending' command");
            }
            const idle_str = switch (args[4]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid idle time"),
            };
            idle_time = std.fmt.parseInt(i64, idle_str, 10) catch {
                return w.writeError("ERR invalid idle time");
            };
            start_idx = 5;
        }
    }

    if (args.len < start_idx + 3) {
        return w.writeError("ERR wrong number of arguments for 'xpending' command");
    }

    const start = switch (args[start_idx]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid start ID"),
    };

    const end = switch (args[start_idx + 1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid end ID"),
    };

    const count_str = switch (args[start_idx + 2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid count"),
    };

    const count = std.fmt.parseInt(usize, count_str, 10) catch {
        return w.writeError("ERR invalid count");
    };

    // Parse optional consumer parameter
    if (args.len > start_idx + 3) {
        consumer_name = switch (args[start_idx + 3]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid consumer name"),
        };
    }

    const entries = storage.xpendingRange(allocator, key, group_name, start, end, count, consumer_name, idle_time) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.NoSuchKey => return w.writeArray(null),
        error.NoSuchGroup => return w.writeError("NOGROUP No such consumer group"),
        error.InvalidStreamId => return w.writeError("ERR Invalid stream ID"),
        else => return err,
    };
    defer allocator.free(entries);

    return w.writeSimpleString(entries);
}

/// XINFO STREAM key [FULL [COUNT count]]
/// Returns information about the stream
pub fn cmdXinfoStream(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'xinfo' command");
    }

    const subcommand = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid subcommand"),
    };

    // Route to appropriate XINFO subcommand handler
    if (std.mem.eql(u8, subcommand, "CONSUMERS")) {
        // XINFO CONSUMERS key groupname
        if (args.len != 4) {
            return w.writeError("ERR wrong number of arguments for 'xinfo consumers' command");
        }

        const key = switch (args[2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        const group_name = switch (args[3]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid group name"),
        };

        const info = storage.xinfoConsumers(allocator, key, group_name) catch |err| switch (err) {
            error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
            error.NoGroup => return w.writeError("NOGROUP No such consumer group for this key"),
            else => return err,
        };
        // info is already in RESP format, return it directly
        return info;
    } else if (std.mem.eql(u8, subcommand, "GROUPS")) {
        // XINFO GROUPS key
        if (args.len != 3) {
            return w.writeError("ERR wrong number of arguments for 'xinfo groups' command");
        }

        const key = switch (args[2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        const info = storage.xinfoGroups(allocator, key) catch |err| switch (err) {
            error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
            else => return err,
        };
        // info is already in RESP format, return it directly
        return info;
    } else if (!std.mem.eql(u8, subcommand, "STREAM")) {
        return w.writeError("ERR unknown XINFO subcommand");
    }

    // XINFO STREAM handler
    const key = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    var full_mode = false;
    var count_limit: ?usize = null;

    // Parse optional FULL [COUNT count]
    if (args.len > 3) {
        const full_flag = switch (args[3]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid argument"),
        };
        if (std.mem.eql(u8, full_flag, "FULL")) {
            full_mode = true;
            if (args.len > 5) {
                const count_flag = switch (args[4]) {
                    .bulk_string => |s| s,
                    else => return w.writeError("ERR invalid argument"),
                };
                if (std.mem.eql(u8, count_flag, "COUNT")) {
                    const count_str = switch (args[5]) {
                        .bulk_string => |s| s,
                        else => return w.writeError("ERR invalid count"),
                    };
                    count_limit = std.fmt.parseInt(usize, count_str, 10) catch {
                        return w.writeError("ERR invalid count");
                    };
                }
            }
        }
    }

    const info = storage.xinfoStream(allocator, key, full_mode, count_limit) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.NoSuchKey => return w.writeError("ERR no such key"),
        else => return err,
    };
    defer allocator.free(info);

    return w.writeSimpleString(info);
}

test "streams - XPENDING summary shows pending count" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create stream
    const xadd_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "1000-0" },
        RespValue{ .bulk_string = "x" },
        RespValue{ .bulk_string = "1" },
    };
    const r1 = try cmdXadd(allocator, storage, &xadd_args);
    defer allocator.free(r1);

    // Create consumer group
    _ = try storage.xgroupCreate("s", "g", "0", null);

    // Read with consumer group (creates pending entry)
    const read_result = try storage.xreadgroup(allocator, "g", "c1", "s", ">", 10, false);
    if (read_result) |r| {
        r.deinit(allocator);
    }

    // Check pending summary
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XPENDING" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "g" },
    };
    const result = try cmdXpending(allocator, storage, &args);
    defer allocator.free(result);

    // Should show 1 pending message
    try std.testing.expect(std.mem.indexOf(u8, result, ":1\r\n") != null);
}

test "streams - XINFO STREAM shows basic metadata" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add entry
    const xadd_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "1000-0" },
        RespValue{ .bulk_string = "x" },
        RespValue{ .bulk_string = "1" },
    };
    const r1 = try cmdXadd(allocator, storage, &xadd_args);
    defer allocator.free(r1);

    // Get stream info
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XINFO" },
        RespValue{ .bulk_string = "STREAM" },
        RespValue{ .bulk_string = "s" },
    };
    const result = try cmdXinfoStream(allocator, storage, &args);
    defer allocator.free(result);

    // Should contain length and last-entry-id
    try std.testing.expect(std.mem.indexOf(u8, result, "length") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "last-entry-id") != null);
}

test "streams - XSETID creates new stream" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // XSETID on non-existent key should create stream
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XSETID" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "1234-5" },
    };
    const result = try cmdXsetid(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);

    // Verify stream was created with correct last_id
    const len_result = try cmdXlen(allocator, storage, &[_]RespValue{
        RespValue{ .bulk_string = "XLEN" },
        RespValue{ .bulk_string = "s" },
    });
    defer allocator.free(len_result);

    try std.testing.expectEqualStrings(":0\r\n", len_result); // Stream exists but empty
}

test "streams - XSETID with $ placeholder on empty stream" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // XSETID with $ on non-existent stream should use 0-0
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XSETID" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "$" },
    };
    const result = try cmdXsetid(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);

    // Verify stream exists
    const len_result = try cmdXlen(allocator, storage, &[_]RespValue{
        RespValue{ .bulk_string = "XLEN" },
        RespValue{ .bulk_string = "s" },
    });
    defer allocator.free(len_result);

    try std.testing.expectEqualStrings(":0\r\n", len_result);
}

test "streams - XSETID with $ placeholder on existing stream" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add entry to stream
    _ = try cmdXadd(allocator, storage, &[_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "1000-0" },
        RespValue{ .bulk_string = "x" },
        RespValue{ .bulk_string = "1" },
    });

    // XSETID with $ should keep current last_id (1000-0)
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XSETID" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "$" },
    };
    const result = try cmdXsetid(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "streams - XSETID with ENTRIESADDED parameter" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create stream with ENTRIESADDED
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XSETID" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "1000-0" },
        RespValue{ .bulk_string = "ENTRIESADDED" },
        RespValue{ .bulk_string = "100" },
    };
    const result = try cmdXsetid(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "streams - XSETID with MAXDELETEDID parameter" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create stream with MAXDELETEDID
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XSETID" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "2000-0" },
        RespValue{ .bulk_string = "MAXDELETEDID" },
        RespValue{ .bulk_string = "500-10" },
    };
    const result = try cmdXsetid(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "streams - XSETID with all parameters" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create stream with all parameters
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XSETID" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "3000-0" },
        RespValue{ .bulk_string = "ENTRIESADDED" },
        RespValue{ .bulk_string = "150" },
        RespValue{ .bulk_string = "MAXDELETEDID" },
        RespValue{ .bulk_string = "1000-0" },
    };
    const result = try cmdXsetid(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "streams - XSETID WRONGTYPE error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create a string key
    try storage.set("s", "value", null);

    // XSETID should return WRONGTYPE
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XSETID" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "1000-0" },
    };
    const result = try cmdXsetid(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "WRONGTYPE") != null);
}

test "streams - XSETID invalid stream ID" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Invalid ID format
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XSETID" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "invalid" },
    };
    const result = try cmdXsetid(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR Invalid stream ID") != null);
}

test "streams - XSETID invalid ENTRIESADDED value" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Non-integer ENTRIESADDED
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XSETID" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "1000-0" },
        RespValue{ .bulk_string = "ENTRIESADDED" },
        RespValue{ .bulk_string = "notanumber" },
    };
    const result = try cmdXsetid(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR value is not an integer") != null);
}
test "streams - XCFGSET set duration" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create a stream first
    const xadd_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "1" },
    };
    _ = try cmdXadd(allocator, storage, &xadd_args);

    // Set IDMP-DURATION
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XCFGSET" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "IDMP-DURATION" },
        RespValue{ .bulk_string = "200" },
    };
    const result = try cmdXcfgset(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "streams - XCFGSET set maxsize" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create a stream first
    const xadd_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "1" },
    };
    _ = try cmdXadd(allocator, storage, &xadd_args);

    // Set IDMP-MAXSIZE
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XCFGSET" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "IDMP-MAXSIZE" },
        RespValue{ .bulk_string = "500" },
    };
    const result = try cmdXcfgset(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "streams - XCFGSET set both parameters" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create a stream first
    const xadd_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "1" },
    };
    _ = try cmdXadd(allocator, storage, &xadd_args);

    // Set both IDMP-DURATION and IDMP-MAXSIZE
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XCFGSET" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "IDMP-DURATION" },
        RespValue{ .bulk_string = "300" },
        RespValue{ .bulk_string = "IDMP-MAXSIZE" },
        RespValue{ .bulk_string = "750" },
    };
    const result = try cmdXcfgset(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "streams - XCFGSET on non-existent stream" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // XCFGSET should return "ERR no such key"
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XCFGSET" },
        RespValue{ .bulk_string = "nosuchkey" },
        RespValue{ .bulk_string = "IDMP-DURATION" },
        RespValue{ .bulk_string = "100" },
    };
    const result = try cmdXcfgset(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR no such key") != null);
}

test "streams - XCFGSET on wrong type" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create a string key
    try storage.set("s", "value", null);

    // XCFGSET should return WRONGTYPE
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XCFGSET" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "IDMP-DURATION" },
        RespValue{ .bulk_string = "100" },
    };
    const result = try cmdXcfgset(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "WRONGTYPE") != null);
}

test "streams - XCFGSET invalid duration range" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create a stream first
    const xadd_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "1" },
    };
    _ = try cmdXadd(allocator, storage, &xadd_args);

    // Duration < 1 should fail
    const args1 = [_]RespValue{
        RespValue{ .bulk_string = "XCFGSET" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "IDMP-DURATION" },
        RespValue{ .bulk_string = "0" },
    };
    const result1 = try cmdXcfgset(allocator, storage, &args1);
    defer allocator.free(result1);
    try std.testing.expect(std.mem.indexOf(u8, result1, "ERR invalid duration") != null);

    // Duration > 86400 should fail
    const args2 = [_]RespValue{
        RespValue{ .bulk_string = "XCFGSET" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "IDMP-DURATION" },
        RespValue{ .bulk_string = "86401" },
    };
    const result2 = try cmdXcfgset(allocator, storage, &args2);
    defer allocator.free(result2);
    try std.testing.expect(std.mem.indexOf(u8, result2, "ERR invalid duration") != null);
}

test "streams - XCFGSET invalid maxsize range" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create a stream first
    const xadd_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "1" },
    };
    _ = try cmdXadd(allocator, storage, &xadd_args);

    // Maxsize < 1 should fail
    const args1 = [_]RespValue{
        RespValue{ .bulk_string = "XCFGSET" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "IDMP-MAXSIZE" },
        RespValue{ .bulk_string = "0" },
    };
    const result1 = try cmdXcfgset(allocator, storage, &args1);
    defer allocator.free(result1);
    try std.testing.expect(std.mem.indexOf(u8, result1, "ERR invalid maxsize") != null);

    // Maxsize > 10000 should fail
    const args2 = [_]RespValue{
        RespValue{ .bulk_string = "XCFGSET" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "IDMP-MAXSIZE" },
        RespValue{ .bulk_string = "10001" },
    };
    const result2 = try cmdXcfgset(allocator, storage, &args2);
    defer allocator.free(result2);
    try std.testing.expect(std.mem.indexOf(u8, result2, "ERR invalid maxsize") != null);
}

test "streams - XCFGSET no parameters" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create a stream first
    const xadd_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "1" },
    };
    _ = try cmdXadd(allocator, storage, &xadd_args);

    // XCFGSET with no parameters should fail
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XCFGSET" },
        RespValue{ .bulk_string = "s" },
    };
    const result = try cmdXcfgset(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
}

test "streams - XADD NOMKSTREAM returns nil when stream doesn't exist" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Try to add to non-existent stream with NOMKSTREAM
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "nonexistent" },
        RespValue{ .bulk_string = "NOMKSTREAM" },
        RespValue{ .bulk_string = "*" },
        RespValue{ .bulk_string = "field" },
        RespValue{ .bulk_string = "value" },
    };
    const result = try cmdXadd(allocator, storage, &args);
    defer allocator.free(result);

    // Should return nil (null bulk string)
    try std.testing.expect(std.mem.indexOf(u8, result, "$-1") != null);
}

test "streams - XADD NOMKSTREAM adds to existing stream" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create stream first
    const create_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "1000-0" },
        RespValue{ .bulk_string = "a" },
        RespValue{ .bulk_string = "1" },
    };
    _ = try cmdXadd(allocator, storage, &create_args);

    // Add with NOMKSTREAM to existing stream
    const add_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "NOMKSTREAM" },
        RespValue{ .bulk_string = "2000-0" },
        RespValue{ .bulk_string = "b" },
        RespValue{ .bulk_string = "2" },
    };
    const result = try cmdXadd(allocator, storage, &add_args);
    defer allocator.free(result);

    // Should return ID (not nil)
    try std.testing.expect(std.mem.indexOf(u8, result, "$") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "2000-0") != null);
}

test "streams - XADD MAXLEN trims stream after adding" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add entries without MAXLEN
    var i: u32 = 1000;
    while (i < 1005) : (i += 1) {
        const args = [_]RespValue{
            RespValue{ .bulk_string = "XADD" },
            RespValue{ .bulk_string = "s" },
            try std.fmt.allocPrint(allocator, "{d}-0", .{i}),
            RespValue{ .bulk_string = "x" },
            RespValue{ .bulk_string = "y" },
        };
        _ = try cmdXadd(allocator, storage, &args);
    }

    // Now add with MAXLEN=3 (should trim to 3)
    const trim_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "MAXLEN" },
        RespValue{ .bulk_string = "=" },
        RespValue{ .bulk_string = "3" },
        RespValue{ .bulk_string = "2000-0" },
        RespValue{ .bulk_string = "p" },
        RespValue{ .bulk_string = "q" },
    };
    _ = try cmdXadd(allocator, storage, &trim_args);

    // Check length is 3
    const len_args = [_]RespValue{
        RespValue{ .bulk_string = "XLEN" },
        RespValue{ .bulk_string = "s" },
    };
    const len_result = try cmdXlen(allocator, storage, &len_args);
    defer allocator.free(len_result);

    try std.testing.expect(std.mem.indexOf(u8, len_result, ":3") != null);
}

test "streams - XADD MINID trims old entries after adding" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add entries at IDs 1000-0, 2000-0, 3000-0
    const ids = [_][]const u8{ "1000-0", "2000-0", "3000-0" };
    for (ids) |id| {
        const args = [_]RespValue{
            RespValue{ .bulk_string = "XADD" },
            RespValue{ .bulk_string = "s" },
            RespValue{ .bulk_string = id },
            RespValue{ .bulk_string = "f" },
            RespValue{ .bulk_string = "v" },
        };
        _ = try cmdXadd(allocator, storage, &args);
    }

    // Add with MINID=2000-0 (should trim 1000-0)
    const trim_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "MINID" },
        RespValue{ .bulk_string = "=" },
        RespValue{ .bulk_string = "2000-0" },
        RespValue{ .bulk_string = "4000-0" },
        RespValue{ .bulk_string = "x" },
        RespValue{ .bulk_string = "y" },
    };
    _ = try cmdXadd(allocator, storage, &trim_args);

    // Check length is 3 (2000-0, 3000-0, 4000-0)
    const len_args = [_]RespValue{
        RespValue{ .bulk_string = "XLEN" },
        RespValue{ .bulk_string = "s" },
    };
    const len_result = try cmdXlen(allocator, storage, &len_args);
    defer allocator.free(len_result);

    try std.testing.expect(std.mem.indexOf(u8, len_result, ":3") != null);
}

test "streams - XTRIM MINID removes entries with ID less than threshold" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add entries
    const ids = [_][]const u8{ "1000-0", "2000-0", "3000-0", "4000-0" };
    for (ids) |id| {
        const args = [_]RespValue{
            RespValue{ .bulk_string = "XADD" },
            RespValue{ .bulk_string = "s" },
            RespValue{ .bulk_string = id },
            RespValue{ .bulk_string = "a" },
            RespValue{ .bulk_string = "b" },
        };
        _ = try cmdXadd(allocator, storage, &args);
    }

    // XTRIM MINID 3000-0 should delete 1000-0 and 2000-0
    const trim_args = [_]RespValue{
        RespValue{ .bulk_string = "XTRIM" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "MINID" },
        RespValue{ .bulk_string = "=" },
        RespValue{ .bulk_string = "3000-0" },
    };
    const result = try cmdXtrim(allocator, storage, &trim_args);
    defer allocator.free(result);

    // Should have deleted 2 entries
    try std.testing.expect(std.mem.indexOf(u8, result, ":2") != null);

    // Verify remaining entries: 3000-0 and 4000-0
    const len_args = [_]RespValue{
        RespValue{ .bulk_string = "XLEN" },
        RespValue{ .bulk_string = "s" },
    };
    const len_result = try cmdXlen(allocator, storage, &len_args);
    defer allocator.free(len_result);

    try std.testing.expect(std.mem.indexOf(u8, len_result, ":2") != null);
}

test "streams - XTRIM MINID keeps entries at and above threshold" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add entries
    const ids = [_][]const u8{ "1000-0", "2000-0", "3000-0" };
    for (ids) |id| {
        const args = [_]RespValue{
            RespValue{ .bulk_string = "XADD" },
            RespValue{ .bulk_string = "s" },
            RespValue{ .bulk_string = id },
            RespValue{ .bulk_string = "f" },
            RespValue{ .bulk_string = "v" },
        };
        _ = try cmdXadd(allocator, storage, &args);
    }

    // XTRIM MINID 2000-1 should delete only 1000-0
    const trim_args = [_]RespValue{
        RespValue{ .bulk_string = "XTRIM" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "MINID" },
        RespValue{ .bulk_string = "=" },
        RespValue{ .bulk_string = "2000-1" },
    };
    const result = try cmdXtrim(allocator, storage, &trim_args);
    defer allocator.free(result);

    // Should have deleted 1 entry
    try std.testing.expect(std.mem.indexOf(u8, result, ":1") != null);

    // Verify remaining entries count: 2
    const len_args = [_]RespValue{
        RespValue{ .bulk_string = "XLEN" },
        RespValue{ .bulk_string = "s" },
    };
    const len_result = try cmdXlen(allocator, storage, &len_args);
    defer allocator.free(len_result);

    try std.testing.expect(std.mem.indexOf(u8, len_result, ":2") != null);
}

test "streams - XTRIM MAXLEN still works correctly" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add 5 entries
    var i: u32 = 1000;
    while (i < 1005) : (i += 1) {
        const id_str = try std.fmt.allocPrint(allocator, "{d}-0", .{i});
        const args = [_]RespValue{
            RespValue{ .bulk_string = "XADD" },
            RespValue{ .bulk_string = "s" },
            RespValue{ .bulk_string = id_str },
            RespValue{ .bulk_string = "x" },
            RespValue{ .bulk_string = "y" },
        };
        _ = try cmdXadd(allocator, storage, &args);
    }

    // XTRIM MAXLEN 2 should keep only 2 newest
    const trim_args = [_]RespValue{
        RespValue{ .bulk_string = "XTRIM" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "MAXLEN" },
        RespValue{ .bulk_string = "=" },
        RespValue{ .bulk_string = "2" },
    };
    const result = try cmdXtrim(allocator, storage, &trim_args);
    defer allocator.free(result);

    // Should delete 3 entries
    try std.testing.expect(std.mem.indexOf(u8, result, ":3") != null);
}

test "streams - XTRIM with LIMIT parameter" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add 5 entries
    var i: u32 = 1000;
    while (i < 1005) : (i += 1) {
        const id_str = try std.fmt.allocPrint(allocator, "{d}-0", .{i});
        const args = [_]RespValue{
            RespValue{ .bulk_string = "XADD" },
            RespValue{ .bulk_string = "s" },
            RespValue{ .bulk_string = id_str },
            RespValue{ .bulk_string = "f" },
            RespValue{ .bulk_string = "v" },
        };
        _ = try cmdXadd(allocator, storage, &args);
    }

    // XTRIM MAXLEN ~ 1 LIMIT 2 should delete at most 2 entries
    const trim_args = [_]RespValue{
        RespValue{ .bulk_string = "XTRIM" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "MAXLEN" },
        RespValue{ .bulk_string = "~" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "LIMIT" },
        RespValue{ .bulk_string = "2" },
    };
    const result = try cmdXtrim(allocator, storage, &trim_args);
    defer allocator.free(result);

    // Should delete exactly 2 due to LIMIT
    try std.testing.expect(std.mem.indexOf(u8, result, ":2") != null);
}
