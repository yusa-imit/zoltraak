const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");
const notifications_mod = @import("../storage/notifications.zig");
const pubsub_mod = @import("../storage/pubsub.zig");
const streams = @import("streams.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;
const StreamId = storage_mod.Value.StreamId;
const StreamEntry = storage_mod.Value.StreamEntry;
const XRefMode = storage_mod.XRefMode;
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

/// XGROUP CREATE key groupname <id | $> [MKSTREAM] [ENTRIESREAD count]
/// XGROUP DESTROY key groupname
/// XGROUP SETID key groupname <id | $> [ENTRIESREAD count]
/// XGROUP CREATECONSUMER key groupname consumername
/// XGROUP DELCONSUMER key groupname consumername
pub fn cmdXgroup(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
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

        // Parse MKSTREAM and ENTRIESREAD options (Redis 7.0+, either order)
        var mkstream = false;
        var entries_read_opt: ?u64 = null;
        var opt_idx: usize = 5;
        while (opt_idx < args.len) {
            const opt = switch (args[opt_idx]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            if (std.ascii.eqlIgnoreCase(opt, "MKSTREAM")) {
                mkstream = true;
                opt_idx += 1;
            } else if (std.ascii.eqlIgnoreCase(opt, "ENTRIESREAD")) {
                if (opt_idx + 1 >= args.len) return w.writeError("ERR syntax error");
                const count_str = switch (args[opt_idx + 1]) {
                    .bulk_string => |s| s,
                    else => return w.writeError("ERR value is not an integer or out of range"),
                };
                entries_read_opt = std.fmt.parseUnsigned(u64, count_str, 10) catch {
                    return w.writeError("ERR value is not an integer or out of range");
                };
                opt_idx += 2;
            } else {
                return w.writeError("ERR syntax error");
            }
        }

        storage.xgroupCreate(key, groupname, id_str, entries_read_opt) catch |err| switch (err) {
            error.NoKey => {
                if (mkstream) {
                    // Create empty stream first
                    _ = try storage.xadd(key, "0-1", &[_][]const u8{ "placeholder", "value" }, null, .{});
                    // Delete the placeholder entry
                    _ = try storage.xdel(key, &[_][]const u8{"0-1"});
                    // Now create the group
                    try storage.xgroupCreate(key, groupname, id_str, entries_read_opt);
                } else {
                    return w.writeError("ERR no such key");
                }
            },
            error.GroupExists => return w.writeError("BUSYGROUP Consumer Group name already exists"),
            error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
            else => |e| return e,
        };

        // Publish notification (NEVER fire "del" for streams)
        notifyStreamEvent(allocator, storage, ps, db_index, key, "xgroup-create");

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

        // Publish notification if group was destroyed (NEVER fire "del" for streams)
        if (destroyed) {
            notifyStreamEvent(allocator, storage, ps, db_index, key, "xgroup-destroy");
        }

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

        // Parse optional ENTRIESREAD option (Redis 7.0+)
        var entries_read: ?u64 = null;
        var i: usize = 5;
        while (i < args.len) {
            const opt = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            if (std.ascii.eqlIgnoreCase(opt, "ENTRIESREAD")) {
                if (i + 1 >= args.len) return w.writeError("ERR syntax error");
                const count_str = switch (args[i + 1]) {
                    .bulk_string => |s| s,
                    else => return w.writeError("ERR value is not an integer or out of range"),
                };
                entries_read = std.fmt.parseUnsigned(u64, count_str, 10) catch {
                    return w.writeError("ERR value is not an integer or out of range");
                };
                i += 2;
            } else {
                return w.writeError("ERR syntax error");
            }
        }

        storage.xgroupSetId(key, groupname, id_str, entries_read) catch |err| switch (err) {
            error.NoKey => return w.writeError("ERR no such key"),
            error.NoGroup => return w.writeError("NOGROUP No such consumer group for this key"),
            error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
            else => |e| return e,
        };

        // Publish notification (NEVER fire "del" for streams)
        notifyStreamEvent(allocator, storage, ps, db_index, key, "xgroup-setid");

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

        // Publish notification if consumer was created (NEVER fire "del" for streams)
        if (created) {
            notifyStreamEvent(allocator, storage, ps, db_index, key, "xgroup-createconsumer");
        }

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

        // Publish notification (NEVER fire "del" for streams)
        notifyStreamEvent(allocator, storage, ps, db_index, key, "xgroup-delconsumer");

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
    var block_ms: ?i64 = null;
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
            const block_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            const timeout_ms = std.fmt.parseInt(i64, block_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            if (timeout_ms < 0) {
                return w.writeError("ERR timeout is negative");
            }
            block_ms = timeout_ms;
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

    // If no data and BLOCK specified, enter blocking loop
    if (!has_data and block_ms != null) {
        const timeout_ms = block_ms.?;
        const start_time = std.time.milliTimestamp();
        const check_interval_ms = 100; // Check every 100ms

        // Resolve "$" IDs to actual start IDs for blocking retries.
        // Key: key string (points into args), Value: next ID to poll from (exclusive start)
        var dollar_start_ids = std.StringHashMap(StreamId).init(allocator);
        defer dollar_start_ids.deinit();

        for (keys, ids) |key_val, id_val| {
            const k = switch (key_val) {
                .bulk_string => |s| s,
                else => continue,
            };
            const id_str2 = switch (id_val) {
                .bulk_string => |s| s,
                else => continue,
            };
            if (!std.mem.eql(u8, id_str2, "$")) continue;

            // Get current last ID of the stream (null = stream doesn't exist yet)
            const last_id = storage.getStreamLastId(k);
            // Next ID to poll from = last_id + 1 seq (exclusive start)
            const next_id: StreamId = if (last_id) |lid| .{
                .ms = if (lid.seq == std.math.maxInt(u64)) lid.ms +% 1 else lid.ms,
                .seq = if (lid.seq == std.math.maxInt(u64)) 0 else lid.seq + 1,
            } else .{ .ms = 0, .seq = 0 };
            try dollar_start_ids.put(k, next_id);
        }

        // Blocking loop
        while (true) {
            // Check timeout
            const elapsed = std.time.milliTimestamp() - start_time;
            if (timeout_ms > 0 and elapsed >= timeout_ms) {
                // Timeout expired, return null
                return w.writeNull();
            }

            // Sleep before retrying
            std.Thread.sleep(check_interval_ms * std.time.ns_per_ms);

            // Retry reading from streams
            has_data = false;
            stream_count = 0;

            // Clear previous temp results
            for (temp_results.items) |*item| {
                for (item.entries.items) |*entry| {
                    var e = entry.*;
                    e.deinit(allocator);
                }
                item.entries.deinit(allocator);
            }
            temp_results.clearRetainingCapacity();

            // Check all streams again
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
                    // Resolve effective start ID for xrange
                    const effective_start: []const u8 = if (dollar_start_ids.get(key)) |start_id| eff: {
                        // Format start ID as "ms-seq" for xrange
                        const start_str = try std.fmt.allocPrint(allocator, "{d}-{d}", .{ start_id.ms, start_id.seq });
                        break :eff start_str;
                    } else id_str; // Non-$ IDs use as-is

                    defer if (dollar_start_ids.get(key) != null) allocator.free(effective_start);

                    const raw_entries = storage.xrange(allocator, key, effective_start, "+", count) catch |err| switch (err) {
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

            // If we found data, break out of blocking loop
            if (has_data) break;

            // For BLOCK 0 (infinite), keep looping
            // For BLOCK > 0, timeout check at top of loop will handle it
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
    var block_ms: ?i64 = null;
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
            const block_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            const timeout_ms = std.fmt.parseInt(i64, block_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            if (timeout_ms < 0) {
                return w.writeError("ERR timeout is negative");
            }
            block_ms = timeout_ms;
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

    // If no data and BLOCK specified, enter blocking loop
    // Note: XREADGROUP only blocks for ID = ">", not for "0" or specific IDs
    if (!has_data and block_ms != null) {
        // Check if all IDs are ">" (only block for new messages)
        var should_block = true;
        for (ids) |id_val| {
            const id_str = switch (id_val) {
                .bulk_string => |s| s,
                else => {
                    should_block = false;
                    break;
                },
            };
            // Don't block for "0" or specific IDs
            if (!std.mem.eql(u8, id_str, ">")) {
                should_block = false;
                break;
            }
        }

        if (should_block) {
            const timeout_ms = block_ms.?;
            const start_time = std.time.milliTimestamp();
            const check_interval_ms = 100; // Check every 100ms

            // Blocking loop
            while (true) {
                // Check timeout
                const elapsed = std.time.milliTimestamp() - start_time;
                if (timeout_ms > 0 and elapsed >= timeout_ms) {
                    // Timeout expired, return null
                    return w.writeNull();
                }

                // Sleep before retrying
                std.Thread.sleep(check_interval_ms * std.time.ns_per_ms);

                // Retry reading from streams
                has_data = false;
                stream_count = 0;

                // Clear previous temp results
                for (temp_results.items) |*item| {
                    for (item.entries.items) |*entry| {
                        var e = entry.*;
                        e.deinit(allocator);
                    }
                    item.entries.deinit(allocator);
                }
                temp_results.clearRetainingCapacity();

                // Check all streams again
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

                // If we found data, break out of blocking loop
                if (has_data) break;

                // For BLOCK 0 (infinite), keep looping
                // For BLOCK > 0, timeout check at top of loop will handle it
            }
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
        var del_ids = result.deleted_ids;
        del_ids.deinit(allocator);
    }

    // Format response manually using RESP protocol
    // Response format (Redis 7.0+): [next_cursor, [entries...], [deleted_ids...]]
    var result_buf = std.ArrayList(u8){};
    defer result_buf.deinit(allocator);
    const result_writer = result_buf.writer(allocator);

    try result_writer.writeAll("*3\r\n");

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

    // Deleted IDs (entries in PEL that no longer exist in stream)
    try result_writer.print("*{d}\r\n", .{result.deleted_ids.items.len});
    for (result.deleted_ids.items) |del_id| {
        const id_str = try std.fmt.allocPrint(allocator, "{d}-{d}", .{ del_id.ms, del_id.seq });
        defer allocator.free(id_str);
        try result_writer.print("${d}\r\n{s}\r\n", .{ id_str.len, id_str });
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
    std.Thread.sleep(std.time.ns_per_ms * 10);

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
    std.Thread.sleep(std.time.ns_per_ms * 10);

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
    // Verify 3-element response format (Redis 7.0+): [cursor, entries, deleted_ids]
    try std.testing.expect(std.mem.startsWith(u8, xautoclaim_result, "*3\r\n"));
}

test "XAUTOCLAIM returns 3-element array with empty deleted_ids when none deleted" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add entry, read it, then autoclaim (without XDEL)
    const xadd_args = [_]RespValue{
        RespValue{ .bulk_string = "XADD" },
        RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "2000-0" },
        RespValue{ .bulk_string = "k" },
        RespValue{ .bulk_string = "v" },
    };
    const xadd_r = try streams.cmdXadd(allocator, &storage, &xadd_args);
    defer allocator.free(xadd_r);

    const xgroup_args = [_]RespValue{
        RespValue{ .bulk_string = "XGROUP" }, RespValue{ .bulk_string = "CREATE" },
        RespValue{ .bulk_string = "s" },       RespValue{ .bulk_string = "g" },
        RespValue{ .bulk_string = "0" },
    };
    const xgroup_r = try cmdXgroup(allocator, &storage, &xgroup_args);
    defer allocator.free(xgroup_r);

    const xrg_args = [_]RespValue{
        RespValue{ .bulk_string = "XREADGROUP" }, RespValue{ .bulk_string = "GROUP" },
        RespValue{ .bulk_string = "g" },          RespValue{ .bulk_string = "c1" },
        RespValue{ .bulk_string = "STREAMS" },    RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = ">" },
    };
    const xrg_r = try cmdXreadgroup(allocator, &storage, &xrg_args);
    defer allocator.free(xrg_r);

    std.Thread.sleep(std.time.ns_per_ms * 5);

    const xac_args = [_]RespValue{
        RespValue{ .bulk_string = "XAUTOCLAIM" }, RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "g" },          RespValue{ .bulk_string = "c2" },
        RespValue{ .bulk_string = "0" },          RespValue{ .bulk_string = "0-0" },
    };
    const xac_r = try cmdXautoclaim(allocator, &storage, &xac_args);
    defer allocator.free(xac_r);

    // Should start with *3 (3-element array)
    try std.testing.expect(std.mem.startsWith(u8, xac_r, "*3\r\n"));
    // Third element should be empty array *0
    try std.testing.expect(std.mem.indexOf(u8, xac_r, "*0\r\n") != null);
}

test "XAUTOCLAIM deleted_ids contains XDEL'd pending entries" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add two entries
    const xadd1 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" }, RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "3000-0" }, RespValue{ .bulk_string = "k" }, RespValue{ .bulk_string = "v1" },
    };
    const xadd1_r = try streams.cmdXadd(allocator, &storage, &xadd1);
    defer allocator.free(xadd1_r);

    const xadd2 = [_]RespValue{
        RespValue{ .bulk_string = "XADD" }, RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "3001-0" }, RespValue{ .bulk_string = "k" }, RespValue{ .bulk_string = "v2" },
    };
    const xadd2_r = try streams.cmdXadd(allocator, &storage, &xadd2);
    defer allocator.free(xadd2_r);

    // Create group and read both
    const xgroup_args = [_]RespValue{
        RespValue{ .bulk_string = "XGROUP" }, RespValue{ .bulk_string = "CREATE" },
        RespValue{ .bulk_string = "s" },       RespValue{ .bulk_string = "g" },
        RespValue{ .bulk_string = "0" },
    };
    const xgroup_r = try cmdXgroup(allocator, &storage, &xgroup_args);
    defer allocator.free(xgroup_r);

    const xrg_args = [_]RespValue{
        RespValue{ .bulk_string = "XREADGROUP" }, RespValue{ .bulk_string = "GROUP" },
        RespValue{ .bulk_string = "g" },          RespValue{ .bulk_string = "c1" },
        RespValue{ .bulk_string = "COUNT" },      RespValue{ .bulk_string = "10" },
        RespValue{ .bulk_string = "STREAMS" },    RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = ">" },
    };
    const xrg_r = try cmdXreadgroup(allocator, &storage, &xrg_args);
    defer allocator.free(xrg_r);

    // XDEL first entry — now 3000-0 is in PEL but not in stream
    const xdel_args = [_]RespValue{
        RespValue{ .bulk_string = "XDEL" }, RespValue{ .bulk_string = "s" }, RespValue{ .bulk_string = "3000-0" },
    };
    const xdel_r = try streams.cmdXdel(allocator, &storage, &xdel_args);
    defer allocator.free(xdel_r);

    std.Thread.sleep(std.time.ns_per_ms * 5);

    // XAUTOCLAIM — should claim 3001-0 and report 3000-0 as deleted
    const xac_args = [_]RespValue{
        RespValue{ .bulk_string = "XAUTOCLAIM" }, RespValue{ .bulk_string = "s" },
        RespValue{ .bulk_string = "g" },          RespValue{ .bulk_string = "c2" },
        RespValue{ .bulk_string = "0" },          RespValue{ .bulk_string = "0-0" },
    };
    const xac_r = try cmdXautoclaim(allocator, &storage, &xac_args);
    defer allocator.free(xac_r);

    // 3-element response
    try std.testing.expect(std.mem.startsWith(u8, xac_r, "*3\r\n"));
    // The claimed entry 3001-0 should appear
    try std.testing.expect(std.mem.indexOf(u8, xac_r, "3001-0") != null);
    // The deleted entry 3000-0 should appear in the third element
    try std.testing.expect(std.mem.indexOf(u8, xac_r, "3000-0") != null);
}

/// XACKDEL key group [KEEPREF | DELREF | ACKED] IDS numids id [id ...]
/// Acknowledge entries in a consumer group and conditionally delete from stream
pub fn cmdXackdel(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 5) {
        return w.writeError("ERR wrong number of arguments for 'xackdel' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const group_name = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid group name"),
    };

    // Parse optional mode flag
    var mode: XRefMode = .keepref;
    var ids_start_idx: usize = 3;

    if (args.len > 3) {
        const maybe_mode = switch (args[3]) {
            .bulk_string => |s| s,
            else => "",
        };

        if (std.ascii.eqlIgnoreCase(maybe_mode, "KEEPREF")) {
            mode = .keepref;
            ids_start_idx = 4;
        } else if (std.ascii.eqlIgnoreCase(maybe_mode, "DELREF")) {
            mode = .delref;
            ids_start_idx = 4;
        } else if (std.ascii.eqlIgnoreCase(maybe_mode, "ACKED")) {
            mode = .acked;
            ids_start_idx = 4;
        }
    }

    // Find IDS keyword
    if (args.len <= ids_start_idx) {
        return w.writeError("ERR wrong number of arguments for 'xackdel' command");
    }

    const ids_keyword = switch (args[ids_start_idx]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    if (!std.ascii.eqlIgnoreCase(ids_keyword, "IDS")) {
        return w.writeError("ERR syntax error, expected IDS keyword");
    }

    // Parse numids
    if (args.len <= ids_start_idx + 1) {
        return w.writeError("ERR wrong number of arguments for 'xackdel' command");
    }

    const numids_str = switch (args[ids_start_idx + 1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid numids"),
    };

    const numids = std.fmt.parseInt(usize, numids_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    // Extract ID array
    const ids_array_start = ids_start_idx + 2;
    if (args.len < ids_array_start + numids) {
        return w.writeError("ERR wrong number of arguments for 'xackdel' command");
    }

    var id_strings = try allocator.alloc([]const u8, numids);
    defer allocator.free(id_strings);

    for (0..numids) |i| {
        id_strings[i] = switch (args[ids_array_start + i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid ID"),
        };
    }

    // Call storage layer
    const results = storage.xackdel(allocator, key, group_name, id_strings, mode) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.NoGroup => return w.writeError("NOGROUP No such consumer group"),
        else => |e| return e,
    };
    defer allocator.free(results);

    // Format response as array of integers
    var buffer = std.ArrayList(u8){};
    errdefer buffer.deinit(allocator);

    try buffer.append(allocator, '*');
    try std.fmt.format(buffer.writer(allocator), "{d}", .{results.len});
    try buffer.appendSlice(allocator, "\r\n");

    for (results) |status| {
        try buffer.append(allocator, ':');
        try std.fmt.format(buffer.writer(allocator), "{d}", .{status});
        try buffer.appendSlice(allocator, "\r\n");
    }

    return buffer.toOwnedSlice(allocator);
}

/// XDELEX key [KEEPREF | DELREF | ACKED] IDS numids id [id ...]
/// Delete stream entries with consumer group reference control
pub fn cmdXdelex(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'xdelex' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse optional mode flag
    var mode: XRefMode = .keepref;
    var ids_start_idx: usize = 2;

    if (args.len > 2) {
        const maybe_mode = switch (args[2]) {
            .bulk_string => |s| s,
            else => "",
        };

        if (std.ascii.eqlIgnoreCase(maybe_mode, "KEEPREF")) {
            mode = .keepref;
            ids_start_idx = 3;
        } else if (std.ascii.eqlIgnoreCase(maybe_mode, "DELREF")) {
            mode = .delref;
            ids_start_idx = 3;
        } else if (std.ascii.eqlIgnoreCase(maybe_mode, "ACKED")) {
            mode = .acked;
            ids_start_idx = 3;
        }
    }

    // Find IDS keyword
    if (args.len <= ids_start_idx) {
        return w.writeError("ERR wrong number of arguments for 'xdelex' command");
    }

    const ids_keyword = switch (args[ids_start_idx]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    if (!std.ascii.eqlIgnoreCase(ids_keyword, "IDS")) {
        return w.writeError("ERR syntax error, expected IDS keyword");
    }

    // Parse numids
    if (args.len <= ids_start_idx + 1) {
        return w.writeError("ERR wrong number of arguments for 'xdelex' command");
    }

    const numids_str = switch (args[ids_start_idx + 1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid numids"),
    };

    const numids = std.fmt.parseInt(usize, numids_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    // Extract ID array
    const ids_array_start = ids_start_idx + 2;
    if (args.len < ids_array_start + numids) {
        return w.writeError("ERR wrong number of arguments for 'xdelex' command");
    }

    var id_strings = try allocator.alloc([]const u8, numids);
    defer allocator.free(id_strings);

    for (0..numids) |i| {
        id_strings[i] = switch (args[ids_array_start + i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid ID"),
        };
    }

    // Call storage layer
    const results = storage.xdelex(allocator, key, id_strings, mode) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => |e| return e,
    };
    defer allocator.free(results);

    // Format response as array of integers
    var buffer = std.ArrayList(u8){};
    errdefer buffer.deinit(allocator);

    try buffer.append(allocator, '*');
    try std.fmt.format(buffer.writer(allocator), "{d}", .{results.len});
    try buffer.appendSlice(allocator, "\r\n");

    for (results) |status| {
        try buffer.append(allocator, ':');
        try std.fmt.format(buffer.writer(allocator), "{d}", .{status});
        try buffer.appendSlice(allocator, "\r\n");
    }

    return buffer.toOwnedSlice(allocator);
}

test "XACKDEL KEEPREF mode" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create stream
    _ = try storage.xadd("mystream", "1000-0", &[_][]const u8{ "field1", "value1" }, null, .{});
    _ = try storage.xadd("mystream", "1001-0", &[_][]const u8{ "field2", "value2" }, null, .{});

    // Create consumer group
    try storage.xgroupCreate("mystream", "mygroup", "0", null);

    // Read entries
    const entries = try storage.xreadgroup(allocator, "mygroup", "consumer1", "mystream", ">", 10, false);
    try std.testing.expect(entries != null);
    if (entries) |e| {
        try std.testing.expectEqual(@as(usize, 2), e.items.len);
        e.deinit(allocator);
    }

    // XACKDEL with KEEPREF
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XACKDEL" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "mygroup" },
        RespValue{ .bulk_string = "KEEPREF" },
        RespValue{ .bulk_string = "IDS" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "1000-0" },
    };
    const result = try cmdXackdel(allocator, &storage, &args);
    defer allocator.free(result);

    // Verify entry deleted and acknowledged
    try std.testing.expect(std.mem.indexOf(u8, result, ":1") != null);

    // Verify entry gone from stream
    const range = try storage.xrange(allocator, "mystream", "-", "+", null);
    try std.testing.expectEqual(@as(usize, 1), range.items.len);
    range.deinit(allocator);
}

test "XACKDEL ACKED mode with multiple groups" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create stream
    _ = try storage.xadd("mystream", "1000-0", &[_][]const u8{ "field1", "value1" }, null, .{});

    // Create two consumer groups
    try storage.xgroupCreate("mystream", "group1", "0", null);
    try storage.xgroupCreate("mystream", "group2", "0", null);

    // Both groups read
    const entries1 = try storage.xreadgroup(allocator, "group1", "consumer1", "mystream", ">", 10, false);
    try std.testing.expect(entries1 != null);
    if (entries1) |e| e.deinit(allocator);

    const entries2 = try storage.xreadgroup(allocator, "group2", "consumer2", "mystream", ">", 10, false);
    try std.testing.expect(entries2 != null);
    if (entries2) |e| e.deinit(allocator);

    // Group1 XACKDEL with ACKED (should return 2 - not deleted)
    const args1 = [_]RespValue{
        RespValue{ .bulk_string = "XACKDEL" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "group1" },
        RespValue{ .bulk_string = "ACKED" },
        RespValue{ .bulk_string = "IDS" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "1000-0" },
    };
    const result1 = try cmdXackdel(allocator, &storage, &args1);
    defer allocator.free(result1);

    // Should return 2 (not deleted)
    try std.testing.expect(std.mem.indexOf(u8, result1, ":2") != null);

    // Verify entry still in stream
    const range1 = try storage.xrange(allocator, "mystream", "-", "+", null);
    try std.testing.expectEqual(@as(usize, 1), range1.items.len);
    range1.deinit(allocator);

    // Group2 also does XACKDEL with ACKED (now all acknowledged, should delete)
    const args2 = [_]RespValue{
        RespValue{ .bulk_string = "XACKDEL" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "group2" },
        RespValue{ .bulk_string = "ACKED" },
        RespValue{ .bulk_string = "IDS" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "1000-0" },
    };
    const result2 = try cmdXackdel(allocator, &storage, &args2);
    defer allocator.free(result2);

    // Should return 1 (deleted)
    try std.testing.expect(std.mem.indexOf(u8, result2, ":1") != null);

    // Verify entry gone from stream
    const range2 = try storage.xrange(allocator, "mystream", "-", "+", null);
    try std.testing.expectEqual(@as(usize, 0), range2.items.len);
    range2.deinit(allocator);
}

test "XDELEX DELREF cleans dangling references" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create stream
    _ = try storage.xadd("mystream", "1000-0", &[_][]const u8{ "field1", "value1" }, null, .{});

    // Create consumer group and read
    try storage.xgroupCreate("mystream", "mygroup", "0", null);
    const entries = try storage.xreadgroup(allocator, "mygroup", "consumer1", "mystream", ">", 10, false);
    try std.testing.expect(entries != null);
    if (entries) |e| e.deinit(allocator);

    // Regular XDEL (leaves dangling ref in PEL)
    const deleted = try storage.xdel("mystream", &[_][]const u8{"1000-0"});
    try std.testing.expectEqual(@as(usize, 1), deleted);

    // Verify pending still has reference (dangling)
    const pending = try storage.xpendingSummary(allocator, "mystream", "mygroup");
    try std.testing.expectEqual(@as(u64, 1), pending.count);
    pending.deinit(allocator);

    // XDELEX with DELREF on already-deleted entry (should clean dangling ref)
    const args = [_]RespValue{
        RespValue{ .bulk_string = "XDELEX" },
        RespValue{ .bulk_string = "mystream" },
        RespValue{ .bulk_string = "DELREF" },
        RespValue{ .bulk_string = "IDS" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "1000-0" },
    };
    const result = try cmdXdelex(allocator, &storage, &args);
    defer allocator.free(result);

    // Should return -1 (not in stream)
    try std.testing.expect(std.mem.indexOf(u8, result, ":-1") != null);

    // Verify pending now clean
    const pending2 = try storage.xpendingSummary(allocator, "mystream", "mygroup");
    try std.testing.expectEqual(@as(u64, 0), pending2.count);
    pending2.deinit(allocator);
}

test "XGROUP SETID ENTRIESREAD - sets entries_read and enables lag" {
    const allocator = std.testing.allocator;
    var storage = try storage_mod.Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var ps = try pubsub_mod.PubSub.init(allocator);
    defer ps.deinit();

    // Create stream and group
    const fields = [_]RespValue{
        .{ .bulk_string = "field" }, .{ .bulk_string = "value" },
    };
    const xadd_args = [_]RespValue{
        .{ .bulk_string = "XADD" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "*" },
    } ++ fields;
    _ = try streams.cmdXadd(allocator, &storage, &xadd_args, &ps, 0);
    _ = try streams.cmdXadd(allocator, &storage, &xadd_args, &ps, 0);

    const create_args = [_]RespValue{
        .{ .bulk_string = "XGROUP" },
        .{ .bulk_string = "CREATE" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "mygroup" },
        .{ .bulk_string = "$" },
    };
    const create_result = try cmdXgroup(allocator, &storage, &create_args, &ps, 0);
    defer allocator.free(create_result);

    // XGROUP SETID with ENTRIESREAD
    const setid_args = [_]RespValue{
        .{ .bulk_string = "XGROUP" },
        .{ .bulk_string = "SETID" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "mygroup" },
        .{ .bulk_string = "$" },
        .{ .bulk_string = "ENTRIESREAD" },
        .{ .bulk_string = "2" },
    };
    const result = try cmdXgroup(allocator, &storage, &setid_args, &ps, 0);
    defer allocator.free(result);

    // Should return OK
    try std.testing.expectEqualStrings("+OK\r\n", result);

    // XINFO GROUPS should show entries-read:2 and lag:0
    const info = try storage.xinfoGroups(allocator, "mystream");
    defer allocator.free(info);
    try std.testing.expect(std.mem.indexOf(u8, info, "entries-read") != null);
    try std.testing.expect(std.mem.indexOf(u8, info, "$-1") == null); // no null lag
}

test "XGROUP SETID ENTRIESREAD - invalid value returns error" {
    const allocator = std.testing.allocator;
    var storage = try storage_mod.Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var ps = try pubsub_mod.PubSub.init(allocator);
    defer ps.deinit();

    // Create stream and group
    _ = try storage.xgroupCreate("mystream", "mygroup", "$", null);

    const setid_args = [_]RespValue{
        .{ .bulk_string = "XGROUP" },
        .{ .bulk_string = "SETID" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "mygroup" },
        .{ .bulk_string = "$" },
        .{ .bulk_string = "ENTRIESREAD" },
        .{ .bulk_string = "notanumber" },
    };
    const result = try cmdXgroup(allocator, &storage, &setid_args, &ps, 0);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

test "XGROUP CREATE ENTRIESREAD - sets accurate lag for arbitrary start" {
    const allocator = std.testing.allocator;
    var storage = try storage_mod.Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var ps = try pubsub_mod.PubSub.init(allocator);
    defer ps.deinit();

    // Create stream with 3 entries
    _ = try storage.xadd("s", "1000-0", &[_][]const u8{ "k", "v" }, null, .{});
    _ = try storage.xadd("s", "2000-0", &[_][]const u8{ "k", "v" }, null, .{});
    _ = try storage.xadd("s", "3000-0", &[_][]const u8{ "k", "v" }, null, .{});

    // XGROUP CREATE at arbitrary position with ENTRIESREAD 1
    const args = [_]RespValue{
        .{ .bulk_string = "XGROUP" },
        .{ .bulk_string = "CREATE" },
        .{ .bulk_string = "s" },
        .{ .bulk_string = "g" },
        .{ .bulk_string = "1500-0" },
        .{ .bulk_string = "ENTRIESREAD" },
        .{ .bulk_string = "1" },
    };
    const result = try cmdXgroup(allocator, &storage, &args, &ps, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("+OK\r\n", result);

    // Group should have non-null lag: entries_added=3, entries_read=1, lag=2
    const info = try storage.xinfoGroups(allocator, "s");
    defer allocator.free(info);
    try std.testing.expect(std.mem.indexOf(u8, info, "$-1") == null); // no null lag
    try std.testing.expect(std.mem.indexOf(u8, info, ":2") != null); // lag is 2
}

test "XGROUP CREATE MKSTREAM ENTRIESREAD - both options together" {
    const allocator = std.testing.allocator;
    var storage = try storage_mod.Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var ps = try pubsub_mod.PubSub.init(allocator);
    defer ps.deinit();

    // MKSTREAM + ENTRIESREAD on non-existent key
    const args = [_]RespValue{
        .{ .bulk_string = "XGROUP" },
        .{ .bulk_string = "CREATE" },
        .{ .bulk_string = "newstream" },
        .{ .bulk_string = "g" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "MKSTREAM" },
        .{ .bulk_string = "ENTRIESREAD" },
        .{ .bulk_string = "0" },
    };
    const result = try cmdXgroup(allocator, &storage, &args, &ps, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "XGROUP CREATE ENTRIESREAD - invalid value returns error" {
    const allocator = std.testing.allocator;
    var storage = try storage_mod.Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var ps = try pubsub_mod.PubSub.init(allocator);
    defer ps.deinit();

    _ = try storage.xadd("s", "1000-0", &[_][]const u8{ "k", "v" }, null, .{});

    const args = [_]RespValue{
        .{ .bulk_string = "XGROUP" },
        .{ .bulk_string = "CREATE" },
        .{ .bulk_string = "s" },
        .{ .bulk_string = "g" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "ENTRIESREAD" },
        .{ .bulk_string = "notanumber" },
    };
    const result = try cmdXgroup(allocator, &storage, &args, &ps, 0);
    defer allocator.free(result);
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

test "XREAD BLOCK with $ ID on empty stream returns null after timeout" {
    const allocator = std.testing.allocator;
    var storage = try storage_mod.Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // XREAD BLOCK 100 STREAMS nonexistent $
    // Should timeout and return null, not error
    const args = [_]RespValue{
        .{ .bulk_string = "XREAD" },
        .{ .bulk_string = "BLOCK" },
        .{ .bulk_string = "100" },
        .{ .bulk_string = "STREAMS" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "$" },
    };
    const result = try cmdXread(allocator, &storage, &args);
    defer allocator.free(result);

    // Should return null array
    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "XREAD BLOCK with $ ID on non-empty stream returns new entries" {
    const allocator = std.testing.allocator;
    var storage = try storage_mod.Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create stream with one entry
    _ = try storage.xadd("mystream", "1000-0", &[_][]const u8{ "field", "value" }, null, .{});

    // XREAD BLOCK 100 STREAMS mystream $
    // Should immediately timeout since no entries after stream's last ID
    const args = [_]RespValue{
        .{ .bulk_string = "XREAD" },
        .{ .bulk_string = "BLOCK" },
        .{ .bulk_string = "100" },
        .{ .bulk_string = "STREAMS" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "$" },
    };
    const result = try cmdXread(allocator, &storage, &args);
    defer allocator.free(result);

    // Should return null (no new entries after last_id)
    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "XREAD with specific ID (not $) works correctly" {
    const allocator = std.testing.allocator;
    var storage = try storage_mod.Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create stream with entries
    _ = try storage.xadd("mystream", "1000-0", &[_][]const u8{ "field", "value1" }, null, .{});
    _ = try storage.xadd("mystream", "2000-0", &[_][]const u8{ "field", "value2" }, null, .{});

    // XREAD STREAMS mystream 1500-0 (should return entry at 2000-0)
    const args = [_]RespValue{
        .{ .bulk_string = "XREAD" },
        .{ .bulk_string = "STREAMS" },
        .{ .bulk_string = "mystream" },
        .{ .bulk_string = "1500-0" },
    };
    const result = try cmdXread(allocator, &storage, &args);
    defer allocator.free(result);

    // Should return array with stream data
    try std.testing.expect(std.mem.startsWith(u8, result, "*"));
    try std.testing.expect(std.mem.indexOf(u8, result, "2000-0") != null);
}
