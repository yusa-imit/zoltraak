const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");
const persistence_mod = @import("../storage/persistence.zig");
const aof_mod = @import("../storage/aof.zig");
const pubsub_mod = @import("../storage/pubsub.zig");
const lists = @import("lists.zig");
const sets = @import("sets.zig");
const hashes = @import("hashes.zig");
const sorted_sets = @import("sorted_sets.zig");
const pubsub_cmds = @import("pubsub.zig");
const tx_mod = @import("transactions.zig");
pub const TxState = tx_mod.TxState;

const RespValue = protocol.RespValue;
const RespType = protocol.RespType;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;
const Persistence = persistence_mod.Persistence;
pub const Aof = aof_mod.Aof;
pub const PubSub = pubsub_mod.PubSub;

/// Default RDB file path
const DEFAULT_RDB_PATH = "dump.rdb";
/// Default AOF file path
const DEFAULT_AOF_PATH = "appendonly.aof";

/// Execute a RESP command and return the serialized response.
/// Caller owns returned memory and must free it.
/// If `aof` is non-null, write commands are appended to it after successful execution.
/// `ps` and `subscriber_id` are used for SUBSCRIBE / UNSUBSCRIBE / PUBLISH / PUBSUB commands.
/// `tx` holds per-connection transaction state for MULTI/EXEC/DISCARD/WATCH.
pub fn executeCommand(
    allocator: std.mem.Allocator,
    storage: *Storage,
    cmd: RespValue,
    aof: ?*Aof,
    ps: *PubSub,
    subscriber_id: u64,
    tx: *TxState,
) ![]const u8 {
    const array = switch (cmd) {
        .array => |arr| arr,
        else => {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR expected array");
        },
    };

    if (array.len == 0) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR empty command");
    }

    const cmd_name = switch (array[0]) {
        .bulk_string => |s| s,
        else => {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR invalid command format");
        },
    };

    // Command dispatch (case-insensitive)
    const cmd_upper = try std.ascii.allocUpperString(allocator, cmd_name);
    defer allocator.free(cmd_upper);

    // ── Transaction command handling ──────────────────────────────────────────
    // MULTI, EXEC, DISCARD, WATCH, UNWATCH are always handled immediately,
    // even inside a MULTI block.
    if (std.mem.eql(u8, cmd_upper, "MULTI")) {
        return tx_mod.cmdMulti(allocator, tx, array);
    } else if (std.mem.eql(u8, cmd_upper, "DISCARD")) {
        return tx_mod.cmdDiscard(allocator, tx, array);
    } else if (std.mem.eql(u8, cmd_upper, "WATCH")) {
        return tx_mod.cmdWatch(allocator, tx, array);
    } else if (std.mem.eql(u8, cmd_upper, "UNWATCH")) {
        return tx_mod.cmdUnwatch(allocator, tx, array);
    } else if (std.mem.eql(u8, cmd_upper, "EXEC")) {
        return try cmdExec(allocator, storage, aof, ps, subscriber_id, tx);
    }

    // When inside a MULTI block, queue all other commands and return +QUEUED.
    if (tx.active) {
        // We need to re-encode the parsed command back to RESP bytes for queuing.
        // Use the raw input: we store the RESP bytes via the parser's raw slice trick.
        // Since we don't have the raw bytes here, serialize the RespValue instead.
        var w = Writer.init(allocator);
        defer w.deinit();
        const encoded = try w.serialize(cmd);
        errdefer allocator.free(encoded);
        try tx.enqueue(encoded);
        return w.writeSimpleString("QUEUED");
    }

    // Determine if this is a write command that should be AOF-logged
    const is_write_cmd = blk: {
        const write_cmds = [_][]const u8{
            "SET", "DEL",
            "LPUSH", "RPUSH", "LPOP", "RPOP",
            "SADD", "SREM",
            "HSET", "HDEL",
            "ZADD", "ZREM",
            "FLUSHDB", "FLUSHALL",
        };
        for (write_cmds) |wc| {
            if (std.mem.eql(u8, cmd_upper, wc)) break :blk true;
        }
        break :blk false;
    };

    // Execute command
    const response = blk: {
        // String commands
        if (std.mem.eql(u8, cmd_upper, "PING")) {
            break :blk try cmdPing(allocator, array);
        } else if (std.mem.eql(u8, cmd_upper, "SET")) {
            break :blk try cmdSet(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "GET")) {
            break :blk try cmdGet(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "DEL")) {
            break :blk try cmdDel(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "EXISTS")) {
            break :blk try cmdExists(allocator, storage, array);
        }
        // List commands
        else if (std.mem.eql(u8, cmd_upper, "LPUSH")) {
            break :blk try lists.cmdLpush(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "RPUSH")) {
            break :blk try lists.cmdRpush(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LPOP")) {
            break :blk try lists.cmdLpop(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "RPOP")) {
            break :blk try lists.cmdRpop(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LRANGE")) {
            break :blk try lists.cmdLrange(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LLEN")) {
            break :blk try lists.cmdLlen(allocator, storage, array);
        }
        // Set commands
        else if (std.mem.eql(u8, cmd_upper, "SADD")) {
            break :blk try sets.cmdSadd(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SREM")) {
            break :blk try sets.cmdSrem(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SISMEMBER")) {
            break :blk try sets.cmdSismember(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SMEMBERS")) {
            break :blk try sets.cmdSmembers(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SCARD")) {
            break :blk try sets.cmdScard(allocator, storage, array);
        }
        // Hash commands
        else if (std.mem.eql(u8, cmd_upper, "HSET")) {
            break :blk try hashes.cmdHset(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HGET")) {
            break :blk try hashes.cmdHget(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HDEL")) {
            break :blk try hashes.cmdHdel(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HGETALL")) {
            break :blk try hashes.cmdHgetall(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HKEYS")) {
            break :blk try hashes.cmdHkeys(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HVALS")) {
            break :blk try hashes.cmdHvals(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HEXISTS")) {
            break :blk try hashes.cmdHexists(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HLEN")) {
            break :blk try hashes.cmdHlen(allocator, storage, array);
        }
        // Sorted set commands
        else if (std.mem.eql(u8, cmd_upper, "ZADD")) {
            break :blk try sorted_sets.cmdZadd(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZREM")) {
            break :blk try sorted_sets.cmdZrem(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZRANGE")) {
            break :blk try sorted_sets.cmdZrange(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZRANGEBYSCORE")) {
            break :blk try sorted_sets.cmdZrangebyscore(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZSCORE")) {
            break :blk try sorted_sets.cmdZscore(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZCARD")) {
            break :blk try sorted_sets.cmdZcard(allocator, storage, array);
        }
        // Pub/Sub commands
        else if (std.mem.eql(u8, cmd_upper, "SUBSCRIBE")) {
            break :blk try pubsub_cmds.cmdSubscribe(allocator, ps, array, subscriber_id);
        } else if (std.mem.eql(u8, cmd_upper, "UNSUBSCRIBE")) {
            break :blk try pubsub_cmds.cmdUnsubscribe(allocator, ps, array, subscriber_id);
        } else if (std.mem.eql(u8, cmd_upper, "PUBLISH")) {
            break :blk try pubsub_cmds.cmdPublish(allocator, ps, array);
        } else if (std.mem.eql(u8, cmd_upper, "PUBSUB")) {
            break :blk try cmdPubsub(allocator, ps, array);
        }
        // Server / persistence commands
        else if (std.mem.eql(u8, cmd_upper, "SAVE")) {
            break :blk try cmdSave(allocator, storage);
        } else if (std.mem.eql(u8, cmd_upper, "BGSAVE")) {
            break :blk try cmdBgsave(allocator, storage);
        } else if (std.mem.eql(u8, cmd_upper, "BGREWRITEAOF")) {
            break :blk try cmdBgrewriteaof(allocator, storage);
        } else if (std.mem.eql(u8, cmd_upper, "DBSIZE")) {
            break :blk try cmdDbsize(allocator, storage);
        } else if (std.mem.eql(u8, cmd_upper, "FLUSHDB") or std.mem.eql(u8, cmd_upper, "FLUSHALL")) {
            break :blk try cmdFlushall(allocator, storage);
        } else {
            var w = Writer.init(allocator);
            defer w.deinit();
            var buf: [256]u8 = undefined;
            const err_msg = try std.fmt.bufPrint(&buf, "ERR unknown command '{s}'", .{cmd_name});
            return w.writeError(err_msg);
        }
    };

    // Log write commands to AOF (best-effort, skip on error)
    // Also mark any watched keys dirty so EXEC can detect conflicts.
    if (is_write_cmd) {
        // Mark watched keys dirty if the written key is being watched.
        // The key is always args[1] for single-key write commands.
        if (array.len >= 2) {
            const written_key = switch (array[1]) {
                .bulk_string => |s| s,
                else => "",
            };
            if (written_key.len > 0) {
                tx_mod.markWatchedDirty(tx, written_key);
            }
        }

        if (aof) |a| {
            // Build string slice from the RespValue array
            var aof_args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(aof_args);
            var valid = true;
            for (array, 0..) |arg, i| {
                aof_args[i] = switch (arg) {
                    .bulk_string => |s| s,
                    else => { valid = false; break; },
                };
            }
            if (valid) {
                a.appendCommand(aof_args) catch |err| {
                    std.debug.print("AOF write warning: {any}\n", .{err});
                };
            }
        }
    }

    return response;
}

/// EXEC — execute all queued commands in the transaction.
/// Returns a null array if WATCH detected a dirty key, otherwise
/// returns an array of results for each queued command.
fn cmdExec(
    allocator: std.mem.Allocator,
    storage: *Storage,
    aof: ?*Aof,
    ps: *PubSub,
    subscriber_id: u64,
    tx: *TxState,
) anyerror![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (!tx.active) {
        return w.writeError("ERR EXEC without MULTI");
    }

    // If a watched key was modified, abort the transaction.
    if (tx.dirty) {
        tx.reset();
        return w.writeArray(null);
    }

    // Execute each queued command.
    const parser_mod = @import("../protocol/parser.zig");
    const queue_len = tx.queue.items.len;
    var results = try allocator.alloc([]const u8, queue_len);
    defer {
        for (results) |r| allocator.free(r);
        allocator.free(results);
    }

    // We need a fresh TxState for recursive execution (EXEC cannot be nested)
    var inner_tx = TxState.init(allocator);
    defer inner_tx.deinit();

    var executed: usize = 0;
    for (tx.queue.items, 0..) |qc, i| {
        var p = parser_mod.Parser.init(allocator);
        defer p.deinit();

        const parsed_cmd = p.parse(qc.data) catch {
            results[i] = try w.writeError("ERR command parse error during EXEC");
            executed += 1;
            continue;
        };
        defer p.freeValue(parsed_cmd);

        results[i] = executeCommand(
            allocator,
            storage,
            parsed_cmd,
            aof,
            ps,
            subscriber_id,
            &inner_tx,
        ) catch |err| blk: {
            var buf: [64]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "ERR command error: {any}", .{err}) catch "ERR internal error";
            break :blk try w.writeError(msg);
        };
        executed += 1;
    }

    // Build the response array from raw RESP bytes.
    // We concatenate all individual result bytes into one array response.
    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);

    const count_str = try std.fmt.allocPrint(allocator, "*{d}\r\n", .{executed});
    defer allocator.free(count_str);
    try out.appendSlice(allocator, count_str);

    for (results[0..executed]) |r| {
        try out.appendSlice(allocator, r);
    }

    tx.reset();
    return out.toOwnedSlice(allocator);
}

/// PING [message]
/// Returns PONG or echoes the message
fn cmdPing(allocator: std.mem.Allocator, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len == 1) {
        // No argument: return +PONG\r\n
        return w.writeSimpleString("PONG");
    } else if (args.len == 2) {
        // With argument: return bulk string
        const message = switch (args[1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR wrong number of arguments for 'ping' command"),
        };
        return w.writeBulkString(message);
    } else {
        return w.writeError("ERR wrong number of arguments for 'ping' command");
    }
}

/// SET key value [EX seconds] [PX milliseconds] [NX|XX]
/// Returns +OK or $-1 if condition not met
fn cmdSet(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Minimum: SET key value
    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'set' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const value = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid value"),
    };

    var expires_at: ?i64 = null;
    var nx = false;
    var xx = false;

    // Parse options
    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        const opt_upper = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(opt_upper);

        if (std.mem.eql(u8, opt_upper, "EX")) {
            if (expires_at != null) {
                return w.writeError("ERR syntax error");
            }
            i += 1;
            if (i >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const seconds = parseInteger(args[i]) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            if (seconds <= 0) {
                return w.writeError("ERR invalid expire time in 'set' command");
            }
            expires_at = Storage.getCurrentTimestamp() + (seconds * 1000);
        } else if (std.mem.eql(u8, opt_upper, "PX")) {
            if (expires_at != null) {
                return w.writeError("ERR syntax error");
            }
            i += 1;
            if (i >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const milliseconds = parseInteger(args[i]) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            if (milliseconds <= 0) {
                return w.writeError("ERR invalid expire time in 'set' command");
            }
            expires_at = Storage.getCurrentTimestamp() + milliseconds;
        } else if (std.mem.eql(u8, opt_upper, "NX")) {
            if (xx) {
                return w.writeError("ERR syntax error");
            }
            nx = true;
        } else if (std.mem.eql(u8, opt_upper, "XX")) {
            if (nx) {
                return w.writeError("ERR syntax error");
            }
            xx = true;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Check NX condition
    if (nx and storage.exists(key)) {
        return w.writeNull(); // $-1\r\n
    }

    // Check XX condition
    if (xx and !storage.exists(key)) {
        return w.writeNull(); // $-1\r\n
    }

    // Execute SET
    try storage.set(key, value, expires_at);
    return w.writeOK();
}

/// GET key
/// Returns bulk string value or null
fn cmdGet(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'get' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const value = storage.get(key);
    return w.writeBulkString(value);
}

/// DEL key [key ...]
/// Returns integer count of deleted keys
fn cmdDel(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'del' command");
    }

    // Extract keys
    var keys = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 1);
    defer keys.deinit(allocator);

    for (args[1..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    const deleted_count = storage.del(keys.items);
    return w.writeInteger(@intCast(deleted_count));
}

/// EXISTS key [key ...]
/// Returns integer count of existing keys (duplicates counted multiple times)
fn cmdExists(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'exists' command");
    }

    var count: i64 = 0;
    for (args[1..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        if (storage.exists(key)) {
            count += 1;
        }
    }

    return w.writeInteger(count);
}

/// SAVE — synchronous RDB snapshot to dump.rdb
/// Returns +OK on success, -ERR on failure
fn cmdSave(allocator: std.mem.Allocator, storage: *Storage) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    Persistence.save(storage, DEFAULT_RDB_PATH, allocator) catch |err| {
        var buf: [128]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "ERR SAVE failed: {any}", .{err}) catch "ERR SAVE failed";
        return w.writeError(msg);
    };

    return w.writeSimpleString("OK");
}

/// BGSAVE — same as SAVE for now (no background fork on Zig)
fn cmdBgsave(allocator: std.mem.Allocator, storage: *Storage) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    Persistence.save(storage, DEFAULT_RDB_PATH, allocator) catch |err| {
        var buf: [128]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "ERR BGSAVE failed: {any}", .{err}) catch "ERR BGSAVE failed";
        return w.writeError(msg);
    };

    return w.writeSimpleString("Background saving started");
}

/// BGREWRITEAOF — rewrite the AOF from current storage state
fn cmdBgrewriteaof(allocator: std.mem.Allocator, storage: *Storage) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    Aof.rewrite(storage, DEFAULT_AOF_PATH, allocator) catch |err| {
        var buf: [128]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "ERR BGREWRITEAOF failed: {any}", .{err}) catch "ERR BGREWRITEAOF failed";
        return w.writeError(msg);
    };

    return w.writeSimpleString("Background append only file rewriting started");
}

/// DBSIZE — return number of keys in current database
fn cmdDbsize(allocator: std.mem.Allocator, storage: *Storage) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    const size: i64 = @intCast(storage.dbSize());
    return w.writeInteger(size);
}

/// FLUSHDB / FLUSHALL — remove all keys
fn cmdFlushall(allocator: std.mem.Allocator, storage: *Storage) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    storage.flushAll();
    return w.writeSimpleString("OK");
}

/// PUBSUB subcommand [args...]
/// Routes to CHANNELS or NUMSUB sub-commands.
fn cmdPubsub(allocator: std.mem.Allocator, ps: *PubSub, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'pubsub' command");
    }

    const sub_name = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid subcommand"),
    };

    const sub_upper = try std.ascii.allocUpperString(allocator, sub_name);
    defer allocator.free(sub_upper);

    if (std.mem.eql(u8, sub_upper, "CHANNELS")) {
        return pubsub_cmds.cmdPubsubChannels(allocator, ps, args);
    } else if (std.mem.eql(u8, sub_upper, "NUMSUB")) {
        return pubsub_cmds.cmdPubsubNumsub(allocator, ps, args);
    } else {
        var buf: [128]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "ERR unknown subcommand '{s}'", .{sub_name}) catch "ERR unknown subcommand";
        return w.writeError(msg);
    }
}

// Helper functions

fn parseInteger(value: RespValue) !i64 {
    const str = switch (value) {
        .bulk_string => |s| s,
        else => return error.InvalidInteger,
    };
    return std.fmt.parseInt(i64, str, 10);
}

// Embedded unit tests

test "commands - PING no argument" {
    const allocator = std.testing.allocator;

    const args = [_]RespValue{
        RespValue{ .bulk_string = "PING" },
    };

    const result = try cmdPing(allocator, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+PONG\r\n", result);
}

test "commands - PING with message" {
    const allocator = std.testing.allocator;

    const args = [_]RespValue{
        RespValue{ .bulk_string = "PING" },
        RespValue{ .bulk_string = "hello" },
    };

    const result = try cmdPing(allocator, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$5\r\nhello\r\n", result);
}

test "commands - PING too many arguments" {
    const allocator = std.testing.allocator;

    const args = [_]RespValue{
        RespValue{ .bulk_string = "PING" },
        RespValue{ .bulk_string = "arg1" },
        RespValue{ .bulk_string = "arg2" },
    };

    const result = try cmdPing(allocator, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("-ERR wrong number of arguments for 'ping' command\r\n", result);
}

test "commands - SET basic" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqualStrings("value1", storage.get("key1").?);
}

test "commands - SET with EX option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "EX" },
        RespValue{ .bulk_string = "60" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqualStrings("value1", storage.get("key1").?);
}

test "commands - SET with PX option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "PX" },
        RespValue{ .bulk_string = "5000" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqualStrings("value1", storage.get("key1").?);
}

test "commands - SET with NX when key doesn't exist" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "NX" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "commands - SET with NX when key exists" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "existing", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "NX" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
    try std.testing.expectEqualStrings("existing", storage.get("key1").?);
}

test "commands - SET with XX when key exists" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "existing", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "new_value" },
        RespValue{ .bulk_string = "XX" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqualStrings("new_value", storage.get("key1").?);
}

test "commands - SET with XX when key doesn't exist" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "XX" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
    try std.testing.expect(storage.get("key1") == null);
}

test "commands - SET with both NX and XX returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "NX" },
        RespValue{ .bulk_string = "XX" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("-ERR syntax error\r\n", result);
}

test "commands - SET with negative expiration" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "EX" },
        RespValue{ .bulk_string = "-1" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("-ERR invalid expire time in 'set' command\r\n", result);
}

test "commands - GET existing key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "hello", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "key1" },
    };

    const result = try cmdGet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$5\r\nhello\r\n", result);
}

test "commands - GET non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    const result = try cmdGet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "commands - GET wrong number of arguments" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GET" },
    };

    const result = try cmdGet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("-ERR wrong number of arguments for 'get' command\r\n", result);
}

test "commands - DEL single key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DEL" },
        RespValue{ .bulk_string = "key1" },
    };

    const result = try cmdDel(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
    try std.testing.expect(storage.get("key1") == null);
}

test "commands - DEL multiple keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);
    try storage.set("key2", "value2", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DEL" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key2" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    const result = try cmdDel(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":2\r\n", result);
}

test "commands - DEL non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DEL" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    const result = try cmdDel(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "commands - EXISTS single existing key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "EXISTS" },
        RespValue{ .bulk_string = "key1" },
    };

    const result = try cmdExists(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
}

test "commands - EXISTS non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "EXISTS" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    const result = try cmdExists(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "commands - EXISTS multiple keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);
    try storage.set("key2", "value2", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "EXISTS" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key2" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    const result = try cmdExists(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":2\r\n", result);
}

test "commands - EXISTS with duplicate keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "EXISTS" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key1" },
    };

    const result = try cmdExists(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":3\r\n", result);
}

test "commands - executeCommand dispatches PING" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init(allocator);
    defer tx.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "PING" },
    };
    const cmd = RespValue{ .array = &args };

    const result = try executeCommand(allocator, storage, cmd, null, &ps, 0, &tx);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+PONG\r\n", result);
}

test "commands - executeCommand case insensitive" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init(allocator);
    defer tx.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "ping" },
    };
    const cmd = RespValue{ .array = &args };

    const result = try executeCommand(allocator, storage, cmd, null, &ps, 0, &tx);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+PONG\r\n", result);
}

test "commands - executeCommand unknown command" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init(allocator);
    defer tx.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "UNKNOWN" },
    };
    const cmd = RespValue{ .array = &args };

    const result = try executeCommand(allocator, storage, cmd, null, &ps, 0, &tx);
    defer allocator.free(result);

    const expected = "-ERR unknown command 'UNKNOWN'\r\n";
    try std.testing.expectEqualStrings(expected, result);
}
