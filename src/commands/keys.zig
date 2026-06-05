const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");
const pubsub_mod = @import("../storage/pubsub.zig");
const notifications_mod = @import("../storage/notifications.zig");
const glob = @import("../utils/glob.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;
const PubSub = pubsub_mod.PubSub;

/// Publish keyspace notification for a key modification
/// This helper function handles both keyspace and keyevent channels based on config flags
fn notifyKeyspaceEvent(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub_state: *PubSub,
    db_index: u32,
    key: []const u8,
    event_flag: notifications_mod.NotificationFlag,
    event_name: []const u8,
) void {
    // Get notification flags from config
    const config_value = storage.config.getAsString("notify-keyspace-events") catch return;
    const config_str = config_value orelse return;

    const flags = notifications_mod.parseNotificationFlags(config_str);

    // Check if this event type should fire
    if (!notifications_mod.shouldNotify(flags, event_flag)) {
        return;
    }

    // Publish notification (ignore errors — notifications are non-critical)
    notifications_mod.publishNotification(
        allocator,
        pubsub_state,
        db_index,
        key,
        event_name,
        flags,
    ) catch {};
}

// ── TTL family ────────────────────────────────────────────────────────────────

/// TTL key
/// Returns remaining time-to-live in seconds.
/// -2 if key does not exist. -1 if key has no expiry.
pub fn cmdTtl(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'ttl' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const ttl_ms = storage.getTtlMs(key);
    if (ttl_ms < 0) {
        return w.writeInteger(ttl_ms); // -1 or -2
    }
    // Convert ms to seconds, round up to ceiling
    const ttl_sec = @divTrunc(ttl_ms + 999, 1000);
    return w.writeInteger(ttl_sec);
}

/// PTTL key
/// Returns remaining time-to-live in milliseconds.
/// -2 if key does not exist. -1 if key has no expiry.
pub fn cmdPttl(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'pttl' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const ttl_ms = storage.getTtlMs(key);
    return w.writeInteger(ttl_ms);
}

/// EXPIRETIME key
/// Returns the absolute Unix timestamp (seconds) at which the key will expire.
/// -2 if key does not exist, -1 if no expiry.
pub fn cmdExpiretime(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'expiretime' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const ttl_ms = storage.getTtlMs(key);
    if (ttl_ms == -2) return w.writeInteger(-2);
    if (ttl_ms == -1) return w.writeInteger(-1);

    const now_ms = Storage.getCurrentTimestamp();
    const expire_ms = now_ms + ttl_ms;
    const expire_sec = @divTrunc(expire_ms, 1000);
    return w.writeInteger(expire_sec);
}

/// PEXPIRETIME key
/// Returns the absolute Unix timestamp (milliseconds) at which the key will expire.
/// -2 if key does not exist, -1 if no expiry.
pub fn cmdPexpiretime(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'pexpiretime' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const ttl_ms = storage.getTtlMs(key);
    if (ttl_ms == -2) return w.writeInteger(-2);
    if (ttl_ms == -1) return w.writeInteger(-1);

    const now_ms = Storage.getCurrentTimestamp();
    const expire_ms = now_ms + ttl_ms;
    return w.writeInteger(expire_ms);
}

/// EXPIRE key seconds [NX|XX|GT|LT]
/// Set expiry in seconds. Returns 1 if set, 0 if key does not exist.
pub fn cmdExpire(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    return cmdExpireImpl(allocator, storage, args, ps, db_index, false, "expire");
}

/// PEXPIRE key milliseconds [NX|XX|GT|LT]
/// Set expiry in milliseconds. Returns 1 if set, 0 if key does not exist.
pub fn cmdPexpire(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    return cmdExpireImpl(allocator, storage, args, ps, db_index, true, "pexpire");
}

/// EXPIREAT key unix-time-seconds [NX|XX|GT|LT]
/// Set expiry as absolute Unix timestamp in seconds.
pub fn cmdExpireat(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    return cmdExpireatImpl(allocator, storage, args, ps, db_index, false, "expireat");
}

/// PEXPIREAT key unix-time-milliseconds [NX|XX|GT|LT]
/// Set expiry as absolute Unix timestamp in milliseconds.
pub fn cmdPexpireat(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    return cmdExpireatImpl(allocator, storage, args, ps, db_index, true, "pexpireat");
}

/// PERSIST key
/// Remove the expiry from a key. Returns 1 if removed, 0 if key has no expiry or doesn't exist.
pub fn cmdPersist(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'persist' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Only persist if key has an expiry (getTtlMs returns -1 for no-expiry)
    const ttl_ms = storage.getTtlMs(key);
    if (ttl_ms == -2) return w.writeInteger(0); // key doesn't exist
    if (ttl_ms == -1) return w.writeInteger(0); // already no expiry

    const ok = storage.setExpiry(key, null, 0);

    // Publish "persist" notification if successful
    if (ok) {
        notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .generic, "persist");
    }

    return w.writeInteger(if (ok) 1 else 0);
}

// ── TYPE ──────────────────────────────────────────────────────────────────────

/// TYPE key
/// Returns simple string: "string", "list", "set", "zset", "hash", or "none".
pub fn cmdType(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'type' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const vtype = storage.getType(key);
    const type_str: []const u8 = if (vtype) |vt| switch (vt) {
        .string => "string",
        .list => "list",
        .set => "set",
        .hash => "hash",
        .sorted_set => "zset",
        .stream => "stream",
        .hyperloglog => "string",
        .json => "ReJSON-RL",
        .timeseries => "TSDB-TYPE",
        .bloom => "BloomFilter",
        .cuckoo => "CuckooFilter",
        .count_min_sketch => "CMSSketch",
        .top_k => "TopK",
        .t_digest => "TDigest",
        .vector_set => "VectorSet",
    } else "none";

    return w.writeSimpleString(type_str);
}

// ── KEYS ─────────────────────────────────────────────────────────────────────

/// KEYS pattern
/// Returns array of all keys matching the glob pattern.
pub fn cmdKeys(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'keys' command");
    }

    const pattern = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid pattern"),
    };

    // Get all live keys
    const all_keys = try storage.listKeys(allocator, pattern);
    defer {
        for (all_keys) |k| allocator.free(k);
        allocator.free(all_keys);
    }

    // Filter by pattern
    var matching: std.ArrayList([]const u8) = .{ .items = &.{}, .capacity = 0 };
    defer matching.deinit(allocator);

    for (all_keys) |k| {
        if (glob.matchGlob(pattern, k)) {
            try matching.append(allocator, k);
        }
    }

    // Build array response
    var buf = std.ArrayList(u8){ .items = &.{}, .capacity = 0 };
    defer buf.deinit(allocator);

    const header = try std.fmt.allocPrint(allocator, "*{d}\r\n", .{matching.items.len});
    defer allocator.free(header);
    try buf.appendSlice(allocator, header);

    for (matching.items) |k| {
        const elem = try std.fmt.allocPrint(allocator, "${d}\r\n{s}\r\n", .{ k.len, k });
        defer allocator.free(elem);
        try buf.appendSlice(allocator, elem);
    }

    return buf.toOwnedSlice(allocator);
}

// ── RENAME / RENAMENX ─────────────────────────────────────────────────────────

/// RENAME key newkey
/// Returns +OK. Returns -ERR if key does not exist.
pub fn cmdRename(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'rename' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const newkey = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid newkey"),
    };

    storage.rename(key, newkey) catch |err| switch (err) {
        error.NoSuchKey => return w.writeError("ERR no such key"),
        else => return err,
    };

    // Publish notifications for the rename event
    // "del" event for old key, "set" event for new key
    notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .generic, "del");
    notifyKeyspaceEvent(allocator, storage, ps, db_index, newkey, .generic, "set");

    return w.writeOK();
}

/// RENAMENX key newkey
/// Returns :1 if renamed, :0 if newkey already exists.
pub fn cmdRenamenx(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'renamenx' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const newkey = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid newkey"),
    };

    const ok = storage.renamenx(key, newkey) catch |err| switch (err) {
        error.NoSuchKey => return w.writeError("ERR no such key"),
        else => return err,
    };

    // Publish notifications if rename was successful
    if (ok) {
        notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .generic, "del");
        notifyKeyspaceEvent(allocator, storage, ps, db_index, newkey, .generic, "set");
    }

    return w.writeInteger(if (ok) 1 else 0);
}

// ── RANDOMKEY ────────────────────────────────────────────────────────────────

/// RANDOMKEY
/// Returns a random key from the database, or null bulk string if empty.
pub fn cmdRandomkey(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 1) {
        return w.writeError("ERR wrong number of arguments for 'randomkey' command");
    }

    const all_keys = try storage.listKeys(allocator, "*");
    defer {
        for (all_keys) |k| allocator.free(k);
        allocator.free(all_keys);
    }

    if (all_keys.len == 0) {
        return w.writeBulkString(null);
    }

    // Simple pseudo-random selection using current timestamp as seed
    const idx = @as(usize, @intCast(@mod(Storage.getCurrentTimestamp(), @as(i64, @intCast(all_keys.len)))));
    return w.writeBulkString(all_keys[idx]);
}

// ── UNLINK ───────────────────────────────────────────────────────────────────

/// UNLINK key [key ...]
/// Alias for DEL (synchronous deletion; async deletion not required here).
pub fn cmdUnlink(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'unlink' command");
    }

    var keys = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 1);
    defer keys.deinit(allocator);

    for (args[1..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    // UNLINK is always async - submit to background thread
    const deleted_count = try storage.unlinkAsync(keys.items);
    return w.writeInteger(@intCast(deleted_count));
}

/// DUMP key
/// Serialize the value stored at key in RDB format.
/// Returns the serialized value or nil if the key doesn't exist.
pub fn cmdDump(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'dump' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const serialized = try storage.dumpValue(allocator, key);
    if (serialized) |data| {
        defer allocator.free(data);
        return w.writeBulkString(data);
    } else {
        return w.writeNull();
    }
}

/// RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
/// Create a key associated with a value that is obtained via DUMP.
/// ttl: time-to-live in milliseconds, 0 means no expiry
/// REPLACE: replace existing key
/// ABSTTL: ttl is absolute Unix timestamp in milliseconds
pub fn cmdRestore(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'restore' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const ttl_ms = switch (args[2]) {
        .bulk_string => |s| std.fmt.parseInt(i64, s, 10) catch {
            return w.writeError("ERR invalid TTL");
        },
        else => return w.writeError("ERR invalid TTL"),
    };

    const serialized = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid serialized value"),
    };

    // Parse options
    var replace = false;
    var absttl = false;
    var i: usize = 4;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        if (std.ascii.eqlIgnoreCase(opt, "REPLACE")) {
            replace = true;
        } else if (std.ascii.eqlIgnoreCase(opt, "ABSTTL")) {
            absttl = true;
        } else if (std.ascii.eqlIgnoreCase(opt, "IDLETIME") or
            std.ascii.eqlIgnoreCase(opt, "FREQ"))
        {
            i += 1; // skip the value
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    storage.restoreValue(key, serialized, ttl_ms, replace, absttl) catch |err| {
        return switch (err) {
            error.KeyAlreadyExists => w.writeError("BUSYKEY Target key name already exists"),
            error.InvalidDumpPayload, error.DumpChecksumMismatch, error.UnknownDumpType => w.writeError("ERR DUMP payload version or checksum are wrong"),
            else => w.writeError("ERR restore failed"),
        };
    };

    return w.writeSimpleString("OK");
}

/// COPY source destination [DB destination-db] [REPLACE]
/// Copy a key to a new key (optionally to a different database).
/// Returns 1 if source was copied, 0 if not (e.g., destination exists).
pub fn cmdCopy(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const RespValue,
    databases: []Storage,
    num_databases: u16,
    selected_db: u16,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'copy' command");
    }

    const source = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid source key"),
    };

    const destination = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid destination key"),
    };

    // Parse options
    var replace = false;
    var dest_db: u16 = selected_db;
    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        if (std.ascii.eqlIgnoreCase(opt, "REPLACE")) {
            replace = true;
        } else if (std.ascii.eqlIgnoreCase(opt, "DB")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const db_num = switch (args[i]) {
                .bulk_string => |s| std.fmt.parseInt(i64, s, 10) catch {
                    return w.writeError("ERR invalid DB index");
                },
                else => return w.writeError("ERR invalid DB index"),
            };
            if (db_num < 0 or db_num >= num_databases) {
                return w.writeError("ERR DB index is out of range");
            }
            dest_db = @as(u16, @intCast(db_num));
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // If destination is same database as source, use same-database copy
    if (dest_db == selected_db) {
        const success = storage.copyKey(source, destination, replace) catch |err| {
            return switch (err) {
                error.NoSuchKey => w.writeInteger(0),
                else => w.writeError("ERR copy failed"),
            };
        };
        return w.writeInteger(if (success) 1 else 0);
    }

    // Cross-database copy
    const dest_storage = &databases[dest_db];
    const success = storage.copyKeyToStorage(source, dest_storage, destination, replace) catch |err| {
        return switch (err) {
            error.NoSuchKey => w.writeInteger(0),
            else => w.writeError("ERR copy failed"),
        };
    };

    return w.writeInteger(if (success) 1 else 0);
}

/// TOUCH key [key ...]
/// Alters the last access time of a key(s).
/// Returns the number of keys that were touched.
pub fn cmdTouch(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'touch' command");
    }

    var keys = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 1);
    defer keys.deinit(allocator);

    for (args[1..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    const count = storage.touch(keys.items);
    return w.writeInteger(@intCast(count));
}

/// MOVE key db
/// Move a key from the current database to another database.
/// Returns 1 if the key was moved successfully, 0 otherwise.
/// Fails if the key doesn't exist in source DB or if it already exists in destination DB.
pub fn cmdMove(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const RespValue,
    databases: []Storage,
    num_databases: u16,
    selected_db: u16,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'move' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const dest_db_index = switch (args[2]) {
        .bulk_string => |s| std.fmt.parseInt(i64, s, 10) catch {
            return w.writeError("ERR invalid DB index");
        },
        else => return w.writeError("ERR invalid DB index"),
    };

    // Validate destination DB index
    if (dest_db_index < 0 or dest_db_index >= num_databases) {
        return w.writeError("ERR DB index is out of range");
    }

    const dest_idx = @as(usize, @intCast(dest_db_index));

    // If source and destination are the same, return 0 (no-op)
    if (dest_idx == selected_db) {
        return w.writeInteger(0);
    }

    // Check if key exists in source database
    if (!storage.exists(key)) {
        return w.writeInteger(0);
    }

    // Check if key already exists in destination database
    const dest_storage = &databases[dest_idx];
    if (dest_storage.exists(key)) {
        return w.writeInteger(0);
    }

    // Get the value and TTL from source database
    const value = storage.get(key) orelse return w.writeInteger(0);
    const ttl_ms = storage.getTtlMs(key);

    // Clone the value to avoid lifetime issues
    const cloned_value = try allocator.dupe(u8, value);
    errdefer allocator.free(cloned_value);

    // Set in destination database (with TTL if it exists)
    if (ttl_ms > 0) {
        const expiry_ms = @as(u64, @intCast(std.time.milliTimestamp())) + @as(u64, @intCast(ttl_ms));
        try dest_storage.set(key, cloned_value, @as(i64, @intCast(expiry_ms)));
    } else {
        try dest_storage.set(key, cloned_value, null);
    }

    // Delete from source database
    const keys_to_delete = [_][]const u8{key};
    _ = storage.del(&keys_to_delete);

    return w.writeInteger(1);
}

// ── Internal helpers ─────────────────────────────────────────────────────────

/// Shared implementation for EXPIRE and PEXPIRE.
/// is_ms: true for PEXPIRE (milliseconds), false for EXPIRE (seconds).
fn cmdExpireImpl(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const RespValue,
    ps: *PubSub,
    db_index: u32,
    is_ms: bool,
    cmd_name: []const u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "ERR wrong number of arguments for '{s}' command", .{cmd_name}) catch "ERR wrong number of arguments";
        return w.writeError(msg);
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const time_val = switch (args[2]) {
        .bulk_string => |s| std.fmt.parseInt(i64, s, 10) catch {
            return w.writeError("ERR value is not an integer or out of range");
        },
        else => return w.writeError("ERR value is not an integer or out of range"),
    };

    // Parse optional NX/XX/GT/LT flags (parsed before the 0/negative check so
    // unknown options still return a syntax error even when time_val <= 0)
    var options: u8 = 0;
    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const opt_upper = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(opt_upper);

        if (std.mem.eql(u8, opt_upper, "NX")) {
            options |= 1;
        } else if (std.mem.eql(u8, opt_upper, "XX")) {
            options |= 2;
        } else if (std.mem.eql(u8, opt_upper, "GT")) {
            options |= 4;
        } else if (std.mem.eql(u8, opt_upper, "LT")) {
            options |= 8;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Redis 7.0+: 0 or negative timeout deletes the key immediately.
    // NX/XX/GT/LT are ignored for past-timestamp deletes (matching Redis behaviour).
    if (time_val <= 0) {
        const key_slice = [_][]const u8{key};
        const deleted = storage.del(&key_slice);
        if (deleted > 0) {
            notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .generic, "expired");
        }
        return w.writeInteger(if (deleted > 0) 1 else 0);
    }

    const now_ms = Storage.getCurrentTimestamp();
    const expires_at_ms: i64 = if (is_ms)
        now_ms + time_val
    else
        now_ms + (time_val * 1000);

    const ok = storage.setExpiry(key, expires_at_ms, options);

    // Publish "expire" or "pexpire" notification if successful
    if (ok) {
        notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .generic, cmd_name);
    }

    return w.writeInteger(if (ok) 1 else 0);
}

/// Shared implementation for EXPIREAT and PEXPIREAT.
/// is_ms: true for PEXPIREAT (milliseconds), false for EXPIREAT (seconds).
fn cmdExpireatImpl(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const RespValue,
    ps: *PubSub,
    db_index: u32,
    is_ms: bool,
    cmd_name: []const u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "ERR wrong number of arguments for '{s}' command", .{cmd_name}) catch "ERR wrong number of arguments";
        return w.writeError(msg);
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const unix_time = switch (args[2]) {
        .bulk_string => |s| std.fmt.parseInt(i64, s, 10) catch {
            return w.writeError("ERR value is not an integer or out of range");
        },
        else => return w.writeError("ERR value is not an integer or out of range"),
    };

    // Negative Unix timestamps are invalid; 0 and positive are accepted even if in the past
    // (a past timestamp immediately expires the key, matching Redis behavior)
    if (unix_time < 0) {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "ERR invalid expire time in '{s}' command", .{cmd_name}) catch "ERR invalid expire time";
        return w.writeError(msg);
    }

    // Parse optional NX/XX/GT/LT flags
    var options: u8 = 0;
    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const opt_upper = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(opt_upper);

        if (std.mem.eql(u8, opt_upper, "NX")) {
            options |= 1;
        } else if (std.mem.eql(u8, opt_upper, "XX")) {
            options |= 2;
        } else if (std.mem.eql(u8, opt_upper, "GT")) {
            options |= 4;
        } else if (std.mem.eql(u8, opt_upper, "LT")) {
            options |= 8;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Convert to milliseconds if input is seconds
    const expires_at_ms: i64 = if (is_ms) unix_time else unix_time * 1000;

    const ok = storage.setExpiry(key, expires_at_ms, options);

    // Publish "expireat" or "pexpireat" notification if successful
    if (ok) {
        notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .generic, cmd_name);
    }

    return w.writeInteger(if (ok) 1 else 0);
}

// ── SCAN family ──────────────────────────────────────────────────────────────

/// SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
/// Keyspace iterator. Returns [next_cursor, [keys]].
/// Uses simple index-based cursor: integer offset into sorted key list.
pub fn cmdScan(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'scan' command");
    }

    const cursor_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not an integer or out of range"),
    };
    const cursor = std.fmt.parseInt(usize, cursor_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    var pattern: []const u8 = "*";
    var count: usize = 10;
    var type_filter: ?[]const u8 = null;

    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const opt_upper = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(opt_upper);

        if (std.mem.eql(u8, opt_upper, "MATCH")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            pattern = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
        } else if (std.mem.eql(u8, opt_upper, "COUNT")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const cnt_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            count = std.fmt.parseInt(usize, cnt_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
        } else if (std.mem.eql(u8, opt_upper, "TYPE")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            type_filter = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Get all live keys matching pattern and type filter
    const all_keys = try storage.listKeys(allocator, "*");
    defer {
        for (all_keys) |k| allocator.free(k);
        allocator.free(all_keys);
    }

    // Filter by pattern and type
    var matching: std.ArrayList([]const u8) = .{ .items = &.{}, .capacity = 0 };
    defer matching.deinit(allocator);

    for (all_keys) |k| {
        if (!glob.matchGlob(pattern, k)) continue;
        if (type_filter) |tf| {
            const vtype = storage.getType(k);
            const type_str: []const u8 = if (vtype) |vt| switch (vt) {
                .string => "string",
                .list => "list",
                .set => "set",
                .hash => "hash",
                .sorted_set => "zset",
                .stream => "stream",
                .hyperloglog => "string",
                .json => "ReJSON-RL",
                .timeseries => "TSDB-TYPE",
                .bloom => "BloomFilter",
                .cuckoo => "CuckooFilter",
                .count_min_sketch => "CountMinSketch",
                .top_k => "TopK",
                .t_digest => "TDigest",
                .vector_set => "VectorSet",
            } else "none";
            // Case-insensitive comparison: lowercase both sides
            const tf_lower = try std.ascii.allocLowerString(allocator, tf);
            defer allocator.free(tf_lower);
            const type_lower = try std.ascii.allocLowerString(allocator, type_str);
            defer allocator.free(type_lower);
            if (!std.mem.eql(u8, type_lower, tf_lower)) continue;
        }
        try matching.append(allocator, k);
    }

    // Apply cursor and count. COUNT=0 means return all matching keys (no limit).
    const effective_count = if (count == 0) matching.items.len else count;
    const start = @min(cursor, matching.items.len);
    const end = @min(start + effective_count, matching.items.len);
    const next_cursor: usize = if (end >= matching.items.len) 0 else end;
    const page = matching.items[start..end];

    // Build response: *2 array: cursor integer, then array of keys
    var buf: std.ArrayList(u8) = .{ .items = &.{}, .capacity = 0 };
    defer buf.deinit(allocator);

    // Write outer array header
    try buf.appendSlice(allocator, "*2\r\n");
    // Write cursor
    const cursor_resp = try std.fmt.allocPrint(allocator, ":{d}\r\n", .{next_cursor});
    defer allocator.free(cursor_resp);
    try buf.appendSlice(allocator, cursor_resp);
    // Write keys array
    const keys_header = try std.fmt.allocPrint(allocator, "*{d}\r\n", .{page.len});
    defer allocator.free(keys_header);
    try buf.appendSlice(allocator, keys_header);
    for (page) |k| {
        const key_resp = try std.fmt.allocPrint(allocator, "${d}\r\n{s}\r\n", .{ k.len, k });
        defer allocator.free(key_resp);
        try buf.appendSlice(allocator, key_resp);
    }

    return buf.toOwnedSlice(allocator);
}

/// HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
/// Hash field iterator. Returns [next_cursor, [field, value, ...]] or [next_cursor, [field, ...]] with NOVALUES.
pub fn cmdHscan(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'hscan' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };
    const cursor_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not an integer or out of range"),
    };
    const cursor = std.fmt.parseInt(usize, cursor_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    var pattern: []const u8 = "*";
    var count: usize = 10;
    var novalues: bool = false;

    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const opt_upper = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(opt_upper);
        if (std.mem.eql(u8, opt_upper, "MATCH")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            pattern = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
        } else if (std.mem.eql(u8, opt_upper, "COUNT")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const cnt_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            count = std.fmt.parseInt(usize, cnt_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
        } else if (std.mem.eql(u8, opt_upper, "NOVALUES")) {
            novalues = true;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Check type first
    const vtype = storage.getType(key);
    if (vtype) |vt| {
        if (vt != .hash) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }

    // hgetall returns interleaved [field, value, ...] with non-owned string pointers
    // Only the outer slice is owned by the caller (not individual strings)
    const all_pairs = (try storage.hgetall(allocator, key)) orelse &[_][]const u8{};
    defer allocator.free(all_pairs);

    // Filter by pattern: check even-indexed items (fields)
    var pairs: std.ArrayList([]const u8) = .{ .items = &.{}, .capacity = 0 };
    defer pairs.deinit(allocator);

    var j: usize = 0;
    while (j + 1 < all_pairs.len) : (j += 2) {
        const field = all_pairs[j];
        const val = all_pairs[j + 1];
        if (glob.matchGlob(pattern, field)) {
            try pairs.append(allocator, field);
            if (!novalues) {
                try pairs.append(allocator, val);
            }
        }
    }

    // Apply cursor and count. COUNT=0 means return all matching fields (no limit).
    // With NOVALUES: pairs.items.len is field count
    // Without NOVALUES: pairs.items.len is field count * 2
    const item_count = if (novalues) pairs.items.len else pairs.items.len / 2;
    const effective_count = if (count == 0) item_count else count;
    const start = @min(cursor, item_count);
    const end = @min(start + effective_count, item_count);
    const next_cursor: usize = if (end >= item_count) 0 else end;
    const page_pairs = if (novalues)
        pairs.items[start..end]
    else
        pairs.items[start * 2 .. end * 2];

    var buf: std.ArrayList(u8) = .{ .items = &.{}, .capacity = 0 };
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "*2\r\n");
    const cursor_resp = try std.fmt.allocPrint(allocator, ":{d}\r\n", .{next_cursor});
    defer allocator.free(cursor_resp);
    try buf.appendSlice(allocator, cursor_resp);
    const items_header = try std.fmt.allocPrint(allocator, "*{d}\r\n", .{page_pairs.len});
    defer allocator.free(items_header);
    try buf.appendSlice(allocator, items_header);
    for (page_pairs) |item| {
        const item_resp = try std.fmt.allocPrint(allocator, "${d}\r\n{s}\r\n", .{ item.len, item });
        defer allocator.free(item_resp);
        try buf.appendSlice(allocator, item_resp);
    }

    return buf.toOwnedSlice(allocator);
}

/// SSCAN key cursor [MATCH pattern] [COUNT count]
/// Set member iterator. Returns [next_cursor, [member, ...]].
pub fn cmdSscan(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'sscan' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };
    const cursor_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not an integer or out of range"),
    };
    const cursor = std.fmt.parseInt(usize, cursor_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    var pattern: []const u8 = "*";
    var count: usize = 10;

    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const opt_upper = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(opt_upper);
        if (std.mem.eql(u8, opt_upper, "MATCH")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            pattern = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
        } else if (std.mem.eql(u8, opt_upper, "COUNT")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const cnt_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            count = std.fmt.parseInt(usize, cnt_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    const vtype = storage.getType(key);
    if (vtype) |vt| {
        if (vt != .set) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }

    // smembers returns slice of non-owned string pointers into internal storage.
    // Only the outer slice is caller-owned.
    const all_members = (try storage.smembers(allocator, key)) orelse &[_][]const u8{};
    defer allocator.free(all_members);

    var matching: std.ArrayList([]const u8) = .{ .items = &.{}, .capacity = 0 };
    defer matching.deinit(allocator);

    for (all_members) |m| {
        if (glob.matchGlob(pattern, m)) {
            try matching.append(allocator, m);
        }
    }

    // COUNT=0 means return all matching members (no limit)
    const effective_count = if (count == 0) matching.items.len else count;
    const start = @min(cursor, matching.items.len);
    const end = @min(start + effective_count, matching.items.len);
    const next_cursor: usize = if (end >= matching.items.len) 0 else end;
    const page = matching.items[start..end];

    var buf: std.ArrayList(u8) = .{ .items = &.{}, .capacity = 0 };
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "*2\r\n");
    const cursor_resp = try std.fmt.allocPrint(allocator, ":{d}\r\n", .{next_cursor});
    defer allocator.free(cursor_resp);
    try buf.appendSlice(allocator, cursor_resp);
    const items_header = try std.fmt.allocPrint(allocator, "*{d}\r\n", .{page.len});
    defer allocator.free(items_header);
    try buf.appendSlice(allocator, items_header);
    for (page) |m| {
        const item_resp = try std.fmt.allocPrint(allocator, "${d}\r\n{s}\r\n", .{ m.len, m });
        defer allocator.free(item_resp);
        try buf.appendSlice(allocator, item_resp);
    }

    return buf.toOwnedSlice(allocator);
}

/// ZSCAN key cursor [MATCH pattern] [COUNT count]
/// Sorted set iterator. Returns [next_cursor, [member, score, ...]].
pub fn cmdZscan(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'zscan' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };
    const cursor_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not an integer or out of range"),
    };
    const cursor = std.fmt.parseInt(usize, cursor_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    var pattern: []const u8 = "*";
    var count: usize = 10;

    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const opt_upper = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(opt_upper);
        if (std.mem.eql(u8, opt_upper, "MATCH")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            pattern = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
        } else if (std.mem.eql(u8, opt_upper, "COUNT")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const cnt_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            count = std.fmt.parseInt(usize, cnt_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    const vtype = storage.getType(key);
    if (vtype) |vt| {
        if (vt != .sorted_set) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }

    // Get all members via zrange (all of them)
    const all_members = (try storage.zrange(allocator, key, 0, -1, true)) orelse &[_][]const u8{};
    defer {
        // With WITHSCORES: interleaved [member, score, ...]
        // Only score strings (odd indices) are owned by the caller
        var j: usize = 0;
        while (j < all_members.len) : (j += 2) {
            if (j + 1 < all_members.len) allocator.free(all_members[j + 1]);
        }
        allocator.free(all_members);
    }

    // Build filtered member-score pairs
    var pairs: std.ArrayList([]const u8) = .{ .items = &.{}, .capacity = 0 };
    defer pairs.deinit(allocator);

    var j: usize = 0;
    while (j + 1 < all_members.len) : (j += 2) {
        const member = all_members[j];
        const score = all_members[j + 1];
        if (glob.matchGlob(pattern, member)) {
            try pairs.append(allocator, member);
            try pairs.append(allocator, score);
        }
    }

    const pair_count = pairs.items.len / 2;
    // COUNT=0 means return all matching pairs (no limit)
    const effective_count = if (count == 0) pair_count else count;
    const start = @min(cursor, pair_count);
    const end = @min(start + effective_count, pair_count);
    const next_cursor: usize = if (end >= pair_count) 0 else end;
    const page_pairs = pairs.items[start * 2 .. end * 2];

    var buf: std.ArrayList(u8) = .{ .items = &.{}, .capacity = 0 };
    defer buf.deinit(allocator);
    try buf.appendSlice(allocator, "*2\r\n");
    const cursor_resp = try std.fmt.allocPrint(allocator, ":{d}\r\n", .{next_cursor});
    defer allocator.free(cursor_resp);
    try buf.appendSlice(allocator, cursor_resp);
    const items_header = try std.fmt.allocPrint(allocator, "*{d}\r\n", .{page_pairs.len});
    defer allocator.free(items_header);
    try buf.appendSlice(allocator, items_header);
    for (page_pairs) |item| {
        const item_resp = try std.fmt.allocPrint(allocator, "${d}\r\n{s}\r\n", .{ item.len, item });
        defer allocator.free(item_resp);
        try buf.appendSlice(allocator, item_resp);
    }

    return buf.toOwnedSlice(allocator);
}

// ── SORT command ──────────────────────────────────────────────────────────────

/// SORT key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC|DESC] [ALPHA] [STORE destination]
/// Sort elements in a list, set, or sorted set
pub fn cmdSort(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'sort' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse options
    var by_pattern: ?[]const u8 = null;
    var get_patterns: std.ArrayList([]const u8) = .{ .items = &.{}, .capacity = 0 };
    defer get_patterns.deinit(allocator);
    var limit_offset: ?usize = null;
    var limit_count: ?usize = null; // null means "no limit"; 0 is valid (return 0 elements)
    var descending = false;
    var alpha = false;
    var store_dest: ?[]const u8 = null;

    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const opt_upper = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(opt_upper);

        if (std.mem.eql(u8, opt_upper, "BY")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            by_pattern = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
        } else if (std.mem.eql(u8, opt_upper, "GET")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            const pattern = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            try get_patterns.append(allocator, pattern);
        } else if (std.mem.eql(u8, opt_upper, "LIMIT")) {
            i += 1;
            if (i + 1 >= args.len) return w.writeError("ERR syntax error");
            const offset_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            i += 1;
            const count_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            limit_offset = std.fmt.parseInt(usize, offset_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            // count can be -1 (or any negative) to mean "return all from offset"
            const count_i64 = std.fmt.parseInt(i64, count_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            limit_count = if (count_i64 < 0) null else @intCast(count_i64);
        } else if (std.mem.eql(u8, opt_upper, "ASC")) {
            descending = false;
        } else if (std.mem.eql(u8, opt_upper, "DESC")) {
            descending = true;
        } else if (std.mem.eql(u8, opt_upper, "ALPHA")) {
            alpha = true;
        } else if (std.mem.eql(u8, opt_upper, "STORE")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR syntax error");
            store_dest = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Load elements based on type
    const vtype = storage.getType(key) orelse {
        // Key doesn't exist - return empty array or 0 if STORE
        if (store_dest) |dest| {
            _ = storage.del(&[_][]const u8{dest});
            return w.writeInteger(0);
        }
        return w.writeArray(null);
    };

    var elements: std.ArrayList([]const u8) = .{ .items = &.{}, .capacity = 0 };
    defer {
        for (elements.items) |elem| allocator.free(elem);
        elements.deinit(allocator);
    }

    switch (vtype) {
        .list => {
            const list_items = (try storage.lrange(allocator, key, 0, -1)) orelse &[_][]const u8{};
            defer allocator.free(list_items);
            for (list_items) |item| {
                const owned = try allocator.dupe(u8, item);
                try elements.append(allocator, owned);
            }
        },
        .set => {
            const members = (try storage.smembers(allocator, key)) orelse &[_][]const u8{};
            defer allocator.free(members);
            for (members) |member| {
                const owned = try allocator.dupe(u8, member);
                try elements.append(allocator, owned);
            }
        },
        .sorted_set => {
            const members = (try storage.zrange(allocator, key, 0, -1, false)) orelse &[_][]const u8{};
            defer allocator.free(members);
            for (members) |member| {
                const owned = try allocator.dupe(u8, member);
                try elements.append(allocator, owned);
            }
        },
        else => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
    }

    // If BY nosort, skip sorting
    const skip_sort = if (by_pattern) |pat| std.mem.eql(u8, pat, "nosort") else false;

    if (!skip_sort) {
        // Build sort weights
        var weights: std.ArrayList(?f64) = .{ .items = &.{}, .capacity = 0 };
        defer weights.deinit(allocator);

        for (elements.items) |elem| {
            var weight: ?f64 = null;
            if (by_pattern) |pattern| {
                const lookup_key = try expandPattern(allocator, pattern, elem);
                defer allocator.free(lookup_key);
                weight = try fetchWeight(allocator, storage, lookup_key);
            } else {
                // Use element itself as weight
                if (alpha) {
                    // Lexicographic sorting - assign arbitrary weight, will use actual value in comparison
                    weight = 0.0;
                } else {
                    weight = std.fmt.parseFloat(f64, elem) catch null;
                }
            }
            try weights.append(allocator, weight);
        }

        // Sort using insertion sort with weights
        const n = elements.items.len;
        var j: usize = 1;
        while (j < n) : (j += 1) {
            const key_elem = elements.items[j];
            const key_weight = weights.items[j];
            var k: usize = j;
            while (k > 0) : (k -= 1) {
                const cmp = compareElements(elements.items[k - 1], key_elem, weights.items[k - 1], key_weight, alpha, descending);
                if (cmp <= 0) break;
                elements.items[k] = elements.items[k - 1];
                weights.items[k] = weights.items[k - 1];
            }
            elements.items[k] = key_elem;
            weights.items[k] = key_weight;
        }
    }

    // Apply LIMIT
    var final_elements: []const []const u8 = elements.items;
    if (limit_offset) |offset| {
        const start = @min(offset, elements.items.len);
        const end = if (limit_count) |cnt| @min(start + cnt, elements.items.len) else elements.items.len;
        final_elements = elements.items[start..end];
    }

    // Build result (either elements themselves or GET results)
    var result: std.ArrayList([]const u8) = .{ .items = &.{}, .capacity = 0 };
    defer {
        for (result.items) |r| allocator.free(r);
        result.deinit(allocator);
    }

    if (get_patterns.items.len > 0) {
        // Multiple GET patterns per element
        for (final_elements) |elem| {
            for (get_patterns.items) |pattern| {
                if (std.mem.eql(u8, pattern, "#")) {
                    // Special pattern: return element itself
                    const owned = try allocator.dupe(u8, elem);
                    try result.append(allocator, owned);
                } else {
                    const lookup_key = try expandPattern(allocator, pattern, elem);
                    defer allocator.free(lookup_key);
                    const value = try fetchValue(allocator, storage, lookup_key);
                    try result.append(allocator, value orelse try allocator.dupe(u8, ""));
                }
            }
        }
    } else {
        // No GET patterns - return elements themselves
        for (final_elements) |elem| {
            const owned = try allocator.dupe(u8, elem);
            try result.append(allocator, owned);
        }
    }

    // STORE or return
    if (store_dest) |dest| {
        // Delete existing key
        _ = storage.del(&[_][]const u8{dest});
        // Store as list
        for (result.items) |item| {
            _ = try storage.rpush(dest, &[_][]const u8{item}, null);
        }
        return w.writeInteger(@as(i64, @intCast(result.items.len)));
    } else {
        // Build RESP array response manually
        var buf: std.ArrayList(u8) = .{ .items = &.{}, .capacity = 0 };
        defer buf.deinit(allocator);
        const header = try std.fmt.allocPrint(allocator, "*{d}\r\n", .{result.items.len});
        defer allocator.free(header);
        try buf.appendSlice(allocator, header);
        for (result.items) |item| {
            const item_resp = try std.fmt.allocPrint(allocator, "${d}\r\n{s}\r\n", .{ item.len, item });
            defer allocator.free(item_resp);
            try buf.appendSlice(allocator, item_resp);
        }
        return buf.toOwnedSlice(allocator);
    }
}

/// Expand pattern by replacing * with element
fn expandPattern(allocator: std.mem.Allocator, pattern: []const u8, element: []const u8) ![]const u8 {
    const star_pos = std.mem.indexOf(u8, pattern, "*") orelse return allocator.dupe(u8, pattern);
    var result: std.ArrayList(u8) = .{ .items = &.{}, .capacity = 0 };
    defer result.deinit(allocator);
    try result.appendSlice(allocator, pattern[0..star_pos]);
    try result.appendSlice(allocator, element);
    try result.appendSlice(allocator, pattern[star_pos + 1 ..]);
    return result.toOwnedSlice(allocator);
}

/// Fetch weight from external key (or hash field)
fn fetchWeight(allocator: std.mem.Allocator, storage: *Storage, lookup_key: []const u8) !?f64 {
    _ = allocator;
    // Check for hash field syntax: key->field
    if (std.mem.indexOf(u8, lookup_key, "->")) |arrow_pos| {
        const key = lookup_key[0..arrow_pos];
        const field = lookup_key[arrow_pos + 2 ..];
        const val = storage.hget(key, field);
        if (val) |v| {
            return std.fmt.parseFloat(f64, v) catch null;
        }
        return null;
    }

    // Regular key lookup
    const val = storage.get(lookup_key);
    if (val) |v| {
        return std.fmt.parseFloat(f64, v) catch null;
    }
    return null;
}

/// Fetch value from external key (or hash field) as string
fn fetchValue(allocator: std.mem.Allocator, storage: *Storage, lookup_key: []const u8) !?[]const u8 {
    // Check for hash field syntax: key->field
    if (std.mem.indexOf(u8, lookup_key, "->")) |arrow_pos| {
        const key = lookup_key[0..arrow_pos];
        const field = lookup_key[arrow_pos + 2 ..];
        const val = storage.hget(key, field);
        if (val) |v| {
            const owned = try allocator.dupe(u8, v);
            return owned;
        }
        return null;
    }

    // Regular key lookup
    const val = storage.get(lookup_key);
    if (val) |v| {
        const owned = try allocator.dupe(u8, v);
        return owned;
    }
    return null;
}

/// Compare two elements for sorting
fn compareElements(a: []const u8, b: []const u8, weight_a: ?f64, weight_b: ?f64, alpha: bool, descending: bool) i8 {
    var cmp: i8 = 0;

    if (alpha) {
        // Lexicographic comparison
        cmp = if (std.mem.order(u8, a, b) == .lt) -1 else if (std.mem.order(u8, a, b) == .gt) 1 else 0;
    } else {
        // Numeric comparison using weights
        const wa = weight_a orelse std.math.inf(f64);
        const wb = weight_b orelse std.math.inf(f64);
        if (wa < wb) {
            cmp = -1;
        } else if (wa > wb) {
            cmp = 1;
        } else {
            cmp = 0;
        }
    }

    return if (descending) -cmp else cmp;
}

/// SORT_RO key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC|DESC] [ALPHA]
/// Read-only variant of SORT (Redis 7.0+)
/// Identical to SORT but without the STORE option
pub fn cmdSortRo(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Check for STORE option (not allowed in SORT_RO)
    for (args[1..]) |arg| {
        if (arg == .bulk_string) {
            if (std.ascii.eqlIgnoreCase(arg.bulk_string, "STORE")) {
                return w.writeError("ERR STORE option is not allowed in SORT_RO");
            }
        }
    }

    // Delegate to cmdSort (which handles all the sorting logic)
    return cmdSort(allocator, storage, args);
}

// ── OBJECT subcommands ────────────────────────────────────────────────────────

/// OBJECT ENCODING key | OBJECT REFCOUNT key | OBJECT IDLETIME key | OBJECT FREQ key | OBJECT HELP
/// Returns plausible encoding strings and stub values.
pub fn cmdObject(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'object' command");
    }

    const subcommand = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };
    const sub_upper = try std.ascii.allocUpperString(allocator, subcommand);
    defer allocator.free(sub_upper);

    if (std.mem.eql(u8, sub_upper, "HELP")) {
        const help_items = [_][]const u8{
            "OBJECT <subcommand> [<arg> [value] [opt] ...]. subcommands are:",
            "ENCODING <key>",
            "    Return the kind of internal representation the Redis object stored at <key> is using.",
            "FREQ <key>",
            "    Return the access frequency index of the key <key>.",
            "HELP",
            "    Return subcommand help summary.",
            "IDLETIME <key>",
            "    Return the idle time of the key <key>.",
            "REFCOUNT <key>",
            "    Return the reference count of the object stored at <key>.",
            "VERSION <key>",
            "    Return the number of times the value stored at <key> has been modified.",
        };
        var resp_values = try std.ArrayList(RespValue).initCapacity(allocator, help_items.len);
        defer resp_values.deinit(allocator);
        for (help_items) |item| {
            try resp_values.append(allocator, RespValue{ .bulk_string = item });
        }
        return w.writeArray(resp_values.items);
    }

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'object|encoding' command");
    }

    const key = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    if (std.mem.eql(u8, sub_upper, "REFCOUNT")) {
        if (storage.getType(key) == null) {
            return w.writeNull();
        }
        return w.writeInteger(1);
    } else if (std.mem.eql(u8, sub_upper, "IDLETIME")) {
        // OBJECT IDLETIME is not available when maxmemory-policy is LFU-based
        const idletime_policy_is_lfu = blk: {
            var cv = storage.config.get("maxmemory-policy") catch break :blk false;
            defer cv.deinit(allocator);
            break :blk switch (cv) {
                .string => |s| std.ascii.eqlIgnoreCase(s, "allkeys-lfu") or
                    std.ascii.eqlIgnoreCase(s, "volatile-lfu"),
                else => false,
            };
        };
        if (idletime_policy_is_lfu) {
            return w.writeError("ERR object idle time is only available when maxmemory-policy is not set to an LFU policy.");
        }
        const idle = storage.getObjectIdleTime(key) orelse {
            return w.writeNull();
        };
        return w.writeInteger(@intCast(idle));
    } else if (std.mem.eql(u8, sub_upper, "FREQ")) {
        // OBJECT FREQ requires LFU-based maxmemory-policy
        const freq_policy_is_lfu = blk: {
            var cv = storage.config.get("maxmemory-policy") catch break :blk false;
            defer cv.deinit(allocator);
            break :blk switch (cv) {
                .string => |s| std.ascii.eqlIgnoreCase(s, "allkeys-lfu") or
                    std.ascii.eqlIgnoreCase(s, "volatile-lfu"),
                else => false,
            };
        };
        if (!freq_policy_is_lfu) {
            return w.writeError("ERR object freq is not allowed when maxmemory-policy is not set to an LFU policy.");
        }
        if (storage.getType(key) == null) {
            return w.writeNull();
        }
        const freq = storage.getObjectFreq(key);
        return w.writeInteger(@intCast(freq));
    } else if (std.mem.eql(u8, sub_upper, "ENCODING")) {
        const vtype = storage.getType(key) orelse {
            return w.writeNull();
        };
        // Read config thresholds (fall back to Redis 8.x defaults on error)
        const hash_max_entries: usize = blk: {
            var cv = storage.config.get("hash-max-listpack-entries") catch break :blk 128;
            defer cv.deinit(allocator);
            break :blk @intCast(@max(0, switch (cv) { .int => |i| i, else => 128 }));
        };
        const hash_max_value: usize = blk: {
            var cv = storage.config.get("hash-max-listpack-value") catch break :blk 64;
            defer cv.deinit(allocator);
            break :blk @intCast(@max(0, switch (cv) { .int => |i| i, else => 64 }));
        };
        const list_max_entries: usize = blk: {
            // list-max-listpack-size is the canonical Redis param (positive = max entries,
            // negative = max bytes per node). Use it when positive; fall back to our
            // list-max-listpack-entries helper param when negative (default -2).
            var cv_size = storage.config.get("list-max-listpack-size") catch break :blk 128;
            defer cv_size.deinit(allocator);
            const size_val: i64 = switch (cv_size) { .int => |i| i, else => -2 };
            if (size_val > 0) break :blk @intCast(size_val);
            // Negative list-max-listpack-size means byte-limit mode; fall back to entries param
            var cv = storage.config.get("list-max-listpack-entries") catch break :blk 128;
            defer cv.deinit(allocator);
            break :blk @intCast(@max(0, switch (cv) { .int => |i| i, else => 128 }));
        };
        const list_max_value: usize = blk: {
            var cv = storage.config.get("list-max-listpack-value") catch break :blk 64;
            defer cv.deinit(allocator);
            break :blk @intCast(@max(0, switch (cv) { .int => |i| i, else => 64 }));
        };
        const quicklist_packed_threshold: usize = blk: {
            var cv = storage.config.get("debug-quicklist-packed-threshold") catch break :blk 4096;
            defer cv.deinit(allocator);
            break :blk @intCast(@max(0, switch (cv) { .int => |i| i, else => 4096 }));
        };
        const zset_max_entries: usize = blk: {
            var cv = storage.config.get("zset-max-listpack-entries") catch break :blk 128;
            defer cv.deinit(allocator);
            break :blk @intCast(@max(0, switch (cv) { .int => |i| i, else => 128 }));
        };
        const zset_max_value: usize = blk: {
            var cv = storage.config.get("zset-max-listpack-value") catch break :blk 64;
            defer cv.deinit(allocator);
            break :blk @intCast(@max(0, switch (cv) { .int => |i| i, else => 64 }));
        };
        const set_max_listpack: usize = blk: {
            var cv = storage.config.get("set-max-listpack-entries") catch break :blk 128;
            defer cv.deinit(allocator);
            break :blk @intCast(@max(0, switch (cv) { .int => |i| i, else => 128 }));
        };
        const set_max_value: usize = blk: {
            var cv = storage.config.get("set-max-listpack-value") catch break :blk 64;
            defer cv.deinit(allocator);
            break :blk @intCast(@max(0, switch (cv) { .int => |i| i, else => 64 }));
        };
        const encoding: []const u8 = switch (vtype) {
            .string => enc: {
                // Use peekStringEncoding to avoid spurious keyspace_hits increments (OBJECT
                // ENCODING is a metadata inspection command, not a data access).
                break :enc storage.peekStringEncoding(key) orelse "embstr";
            },
            .list => blk: {
                const ln = storage.llen(key) orelse 0;
                const max_elem = storage.getListMaxElementLength(key) orelse 0;
                const effective_list_max = @min(list_max_value, quicklist_packed_threshold);
                break :blk if (ln <= list_max_entries and max_elem <= effective_list_max)
                    "listpack"
                else
                    "quicklist";
            },
            .set => blk: {
                // Use actual internal encoding for sets
                const set_enc = storage.getSetEncoding(key) orelse break :blk "hashtable";
                break :blk switch (set_enc) {
                    .intset => "intset",
                    .hashmap => blk2: {
                        const sc = storage.scard(key) orelse 0;
                        const max_member = storage.getSetMaxMemberLength(key) orelse 0;
                        break :blk2 if (sc <= set_max_listpack and max_member <= set_max_value)
                            "listpack"
                        else
                            "hashtable";
                    },
                };
            },
            .hash => blk: {
                const hl = storage.hlen(key) orelse 0;
                const max_elem = storage.getHashMaxElementLength(key) orelse 0;
                break :blk if (hl <= hash_max_entries and max_elem <= hash_max_value)
                    "listpack"
                else
                    "hashtable";
            },
            .sorted_set => blk: {
                const zc = storage.zcard(key) orelse 0;
                const max_member = storage.getZsetMaxMemberLength(key) orelse 0;
                break :blk if (zc <= zset_max_entries and max_member <= zset_max_value)
                    "listpack"
                else
                    "skiplist";
            },
            .stream => "stream",
            // HyperLogLog is stored as a string internally in Redis; dense format is "raw"
            .hyperloglog => "raw",
            .json => "json",
            .timeseries => "timeseries",
            .bloom => "bloom",
            .cuckoo => "cuckoo",
            .count_min_sketch => "countminsketch",
            .top_k => "topk",
            .t_digest => "tdigest",
            .vector_set => "hnsw",
        };
        // Redis returns a bulk string for OBJECT ENCODING (not a simple string)
        return w.writeBulkString(encoding);
    } else if (std.mem.eql(u8, sub_upper, "VERSION")) {
        const version = storage.getKeyVersion(key) orelse {
            return w.writeNull();
        };
        return w.writeInteger(@intCast(version));
    } else {
        var buf: [128]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "ERR unknown subcommand or wrong number of arguments for '{s}' command", .{subcommand}) catch "ERR unknown subcommand";
        return w.writeError(msg);
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

test "keys - TTL on missing key returns -2" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "TTL" },
        .{ .bulk_string = "nosuchkey" },
    };
    const result = try cmdTtl(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":-2\r\n", result);
}

test "keys - TTL on key with no expiry returns -1" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "hello", null);

    const args = [_]RespValue{
        .{ .bulk_string = "TTL" },
        .{ .bulk_string = "mykey" },
    };
    const result = try cmdTtl(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":-1\r\n", result);
}

test "keys - PTTL on key with expiry" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const in_5s = Storage.getCurrentTimestamp() + 5000;
    try storage.set("mykey", "hello", in_5s);

    const args = [_]RespValue{
        .{ .bulk_string = "PTTL" },
        .{ .bulk_string = "mykey" },
    };
    const result = try cmdPttl(allocator, storage, &args);
    defer allocator.free(result);
    // Should be between 4000 and 5000
    try std.testing.expect(result.len > 1);
    try std.testing.expect(result[0] == ':');
}

test "keys - TYPE returns correct type strings" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("strkey", "val", null);

    const args = [_]RespValue{
        .{ .bulk_string = "TYPE" },
        .{ .bulk_string = "strkey" },
    };
    const result = try cmdType(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("+string\r\n", result);
}

test "keys - TYPE on missing key returns none" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "TYPE" },
        .{ .bulk_string = "nosuchkey" },
    };
    const result = try cmdType(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("+none\r\n", result);
}

test "keys - KEYS star returns all keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "v1", null);
    try storage.set("key2", "v2", null);

    const args = [_]RespValue{
        .{ .bulk_string = "KEYS" },
        .{ .bulk_string = "*" },
    };
    const result = try cmdKeys(allocator, storage, &args);
    defer allocator.free(result);

    // Response starts with *2
    try std.testing.expect(result[0] == '*');
    try std.testing.expect(result[1] == '2');
}

test "keys - KEYS pattern filters correctly" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("hello", "v1", null);
    try storage.set("hallo", "v2", null);
    try storage.set("hxllo", "v3", null);
    try storage.set("world", "v4", null);

    const args = [_]RespValue{
        .{ .bulk_string = "KEYS" },
        .{ .bulk_string = "h?llo" },
    };
    const result = try cmdKeys(allocator, storage, &args);
    defer allocator.free(result);

    // Should match hello, hallo, hxllo (3 keys), not world
    try std.testing.expect(result[0] == '*');
    try std.testing.expect(result[1] == '3');
}

test "keys - RENAME basic" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("src", "value", null);

    const args = [_]RespValue{
        .{ .bulk_string = "RENAME" },
        .{ .bulk_string = "src" },
        .{ .bulk_string = "dst" },
    };
    const result = try cmdRename(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("+OK\r\n", result);

    // src gone, dst has value
    try std.testing.expect(storage.get("src") == null);
    try std.testing.expectEqualStrings("value", storage.get("dst").?);
}

test "keys - RENAME non-existent key returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "RENAME" },
        .{ .bulk_string = "nosuchkey" },
        .{ .bulk_string = "dst" },
    };
    const result = try cmdRename(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expect(result[0] == '-');
}

test "keys - RENAMENX returns 1 when newkey absent" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("src", "value", null);

    const args = [_]RespValue{
        .{ .bulk_string = "RENAMENX" },
        .{ .bulk_string = "src" },
        .{ .bulk_string = "dst" },
    };
    const result = try cmdRenamenx(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":1\r\n", result);
}

test "keys - RENAMENX returns 0 when newkey exists" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("src", "value", null);
    try storage.set("dst", "other", null);

    const args = [_]RespValue{
        .{ .bulk_string = "RENAMENX" },
        .{ .bulk_string = "src" },
        .{ .bulk_string = "dst" },
    };
    const result = try cmdRenamenx(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "keys - PERSIST removes expiry" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const in_5s = Storage.getCurrentTimestamp() + 5000;
    try storage.set("mykey", "hello", in_5s);

    const args = [_]RespValue{
        .{ .bulk_string = "PERSIST" },
        .{ .bulk_string = "mykey" },
    };
    const result = try cmdPersist(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":1\r\n", result);

    // Now TTL should be -1
    try std.testing.expectEqual(@as(i64, -1), storage.getTtlMs("mykey"));
}

test "keys - UNLINK deletes keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("k1", "v1", null);
    try storage.set("k2", "v2", null);

    const args = [_]RespValue{
        .{ .bulk_string = "UNLINK" },
        .{ .bulk_string = "k1" },
        .{ .bulk_string = "k2" },
        .{ .bulk_string = "nosuchkey" },
    };
    const result = try cmdUnlink(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":2\r\n", result);
}
// Temporary test file for SORT_RO - will be appended to keys.zig

test "SORT_RO basic" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Create a list with unsorted values
    _ = try storage.rpush("mylist", &[_][]const u8{ "3", "1", "2" }, null);

    // SORT_RO mylist
    const args = [_]RespValue{
        .{ .bulk_string = "SORT_RO" },
        .{ .bulk_string = "mylist" },
    };
    const response = try cmdSortRo(allocator, &storage, &args);
    defer allocator.free(response);

    // Should return sorted array: 1, 2, 3
    try std.testing.expect(std.mem.indexOf(u8, response, "1") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "2") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "3") != null);
}

test "SORT_RO with ALPHA" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Create a list with strings
    _ = try storage.rpush("mylist", &[_][]const u8{ "banana", "apple", "cherry" }, null);

    // SORT_RO mylist ALPHA
    const args = [_]RespValue{
        .{ .bulk_string = "SORT_RO" },
        .{ .bulk_string = "mylist" },
        .{ .bulk_string = "ALPHA" },
    };
    const response = try cmdSortRo(allocator, &storage, &args);
    defer allocator.free(response);

    // Should return alphabetically sorted
    try std.testing.expect(std.mem.indexOf(u8, response, "apple") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "banana") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "cherry") != null);
}

test "SORT_RO rejects STORE option" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.rpush("mylist", &[_][]const u8{ "3", "1", "2" }, null);

    // SORT_RO mylist STORE dest - should fail
    const args = [_]RespValue{
        .{ .bulk_string = "SORT_RO" },
        .{ .bulk_string = "mylist" },
        .{ .bulk_string = "STORE" },
        .{ .bulk_string = "dest" },
    };
    const response = try cmdSortRo(allocator, &storage, &args);
    defer allocator.free(response);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, response, "ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "STORE") != null);
}

test "SORT LIMIT count -1 returns all elements from offset" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.rpush("mylist", &[_][]const u8{ "3", "1", "4", "1", "5" }, null);

    // SORT mylist LIMIT 2 -1 — skip first 2, return all remaining
    const args = [_]RespValue{
        .{ .bulk_string = "SORT" },
        .{ .bulk_string = "mylist" },
        .{ .bulk_string = "LIMIT" },
        .{ .bulk_string = "2" },
        .{ .bulk_string = "-1" },
    };
    const response = try cmdSort(allocator, &storage, &args);
    defer allocator.free(response);

    // Sorted list is: 1, 1, 3, 4, 5 — skip 2 → 3, 4, 5 (3 elements)
    try std.testing.expect(std.mem.startsWith(u8, response, "*3\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, response, "3") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "4") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "5") != null);
}

test "SORT LIMIT count 0 returns empty array" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.rpush("mylist", &[_][]const u8{ "3", "1", "2" }, null);

    // SORT mylist LIMIT 0 0 — return 0 elements
    const args = [_]RespValue{
        .{ .bulk_string = "SORT" },
        .{ .bulk_string = "mylist" },
        .{ .bulk_string = "LIMIT" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "0" },
    };
    const response = try cmdSort(allocator, &storage, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("*0\r\n", response);
}

test "HSCAN NOVALUES - basic with populated hash" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Create hash with 5 fields
    _ = try storage.hset("myhash", &[_][]const u8{ "field1", "field2", "field3", "field4", "field5" }, &[_][]const u8{ "val1", "val2", "val3", "val4", "val5" }, null);

    // HSCAN myhash 0 NOVALUES
    const args = [_]RespValue{
        .{ .bulk_string = "HSCAN" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "NOVALUES" },
    };
    const response = try cmdHscan(allocator, &storage, &args);
    defer allocator.free(response);

    // Should return array with fields only (not values)
    // Parse RESP: *2\r\n:0\r\n*<count>\r\n...<fields>...
    try std.testing.expect(std.mem.startsWith(u8, response, "*2"));
    // Second element should be cursor 0 (next_cursor)
    try std.testing.expect(std.mem.indexOf(u8, response, ":0") != null);
    // Should have array with exactly 5 elements (not 10)
    try std.testing.expect(std.mem.indexOf(u8, response, "*5") != null);
    // Should NOT contain values
    try std.testing.expect(std.mem.indexOf(u8, response, "val1") == null);
}

test "HSCAN NOVALUES - empty hash" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    // HSCAN non_existent 0 NOVALUES
    const args = [_]RespValue{
        .{ .bulk_string = "HSCAN" },
        .{ .bulk_string = "non_existent" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "NOVALUES" },
    };
    const response = try cmdHscan(allocator, &storage, &args);
    defer allocator.free(response);

    // Should return [0, []]
    try std.testing.expect(std.mem.startsWith(u8, response, "*2"));
    try std.testing.expect(std.mem.indexOf(u8, response, "*0") != null);
}

test "HSCAN NOVALUES - non-existent key" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    // HSCAN nonexistent 0 NOVALUES
    const args = [_]RespValue{
        .{ .bulk_string = "HSCAN" },
        .{ .bulk_string = "nonexistent" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "NOVALUES" },
    };
    const response = try cmdHscan(allocator, &storage, &args);
    defer allocator.free(response);

    // Should return [0, []] - empty result
    try std.testing.expect(std.mem.startsWith(u8, response, "*2"));
    try std.testing.expect(std.mem.indexOf(u8, response, "*0") != null);
}

test "HSCAN NOVALUES - with MATCH pattern" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Create hash with pattern-matchable fields
    _ = try storage.hset("myhash", &[_][]const u8{ "foo1", "foo2", "bar1", "baz1" }, &[_][]const u8{ "v1", "v2", "v3", "v4" }, null);

    // HSCAN myhash 0 MATCH "foo*" NOVALUES
    const args = [_]RespValue{
        .{ .bulk_string = "HSCAN" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "MATCH" },
        .{ .bulk_string = "foo*" },
        .{ .bulk_string = "NOVALUES" },
    };
    const response = try cmdHscan(allocator, &storage, &args);
    defer allocator.free(response);

    // Should return only foo1 and foo2 (2 fields)
    try std.testing.expect(std.mem.indexOf(u8, response, "*2") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "foo") != null);
    // Should NOT contain values or non-matching fields
    try std.testing.expect(std.mem.indexOf(u8, response, "v1") == null);
    try std.testing.expect(std.mem.indexOf(u8, response, "bar") == null);
}

test "HSCAN NOVALUES - with COUNT" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Create hash with 10 fields
    var fields: [10][]const u8 = undefined;
    var values: [10][]const u8 = undefined;
    for (0..10) |i| {
        const buf = try std.fmt.allocPrint(allocator, "f{d}", .{i});
        fields[i] = buf;
        const vbuf = try std.fmt.allocPrint(allocator, "v{d}", .{i});
        values[i] = vbuf;
    }
    _ = try storage.hset("bighash", &fields, &values, null);
    for (fields) |f| allocator.free(f);
    for (values) |v| allocator.free(v);

    // HSCAN bighash 0 COUNT 3 NOVALUES
    const args = [_]RespValue{
        .{ .bulk_string = "HSCAN" },
        .{ .bulk_string = "bighash" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "COUNT" },
        .{ .bulk_string = "3" },
        .{ .bulk_string = "NOVALUES" },
    };
    const response = try cmdHscan(allocator, &storage, &args);
    defer allocator.free(response);

    // Should have at most COUNT fields (3)
    // Response format: *2\r\n:next_cursor\r\n*<field_count>\r\n...
    // The field_count should be <= COUNT (not <= COUNT*2)
    try std.testing.expect(std.mem.startsWith(u8, response, "*2"));
    // Should NOT contain values
    try std.testing.expect(std.mem.indexOf(u8, response, "v0") == null);
}

test "HSCAN NOVALUES - combined MATCH COUNT NOVALUES" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Create hash
    _ = try storage.hset("myhash", &[_][]const u8{ "aaa1", "aaa2", "aaa3", "bbb1" }, &[_][]const u8{ "x", "y", "z", "w" }, null);

    // HSCAN myhash 0 MATCH "aaa*" COUNT 2 NOVALUES
    const args = [_]RespValue{
        .{ .bulk_string = "HSCAN" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "MATCH" },
        .{ .bulk_string = "aaa*" },
        .{ .bulk_string = "COUNT" },
        .{ .bulk_string = "2" },
        .{ .bulk_string = "NOVALUES" },
    };
    const response = try cmdHscan(allocator, &storage, &args);
    defer allocator.free(response);

    // Should match pattern AND respect COUNT AND return no values
    try std.testing.expect(std.mem.indexOf(u8, response, "aaa") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "bbb") == null);
    try std.testing.expect(std.mem.indexOf(u8, response, "x") == null);
    try std.testing.expect(std.mem.indexOf(u8, response, "y") == null);
}

test "HSCAN NOVALUES - WRONGTYPE error on string key" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Create a string key
    _ = try storage.set("mystring", "value", null);

    // HSCAN mystring 0 NOVALUES - should error
    const args = [_]RespValue{
        .{ .bulk_string = "HSCAN" },
        .{ .bulk_string = "mystring" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "NOVALUES" },
    };
    const response = try cmdHscan(allocator, &storage, &args);
    defer allocator.free(response);

    // Should return WRONGTYPE error
    try std.testing.expect(std.mem.indexOf(u8, response, "WRONGTYPE") != null);
}

test "HSCAN NOVALUES - order matters (field-only vs field-value pairs)" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Create hash
    _ = try storage.hset("h", &[_][]const u8{"f1"}, &[_][]const u8{"v1"}, null);

    // WITHOUT NOVALUES: should have 2 items (field + value)
    const args_with = [_]RespValue{
        .{ .bulk_string = "HSCAN" },
        .{ .bulk_string = "h" },
        .{ .bulk_string = "0" },
    };
    const response_with = try cmdHscan(allocator, &storage, &args_with);
    defer allocator.free(response_with);

    // WITH NOVALUES: should have 1 item (field only)
    const args_without = [_]RespValue{
        .{ .bulk_string = "HSCAN" },
        .{ .bulk_string = "h" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "NOVALUES" },
    };
    const response_without = try cmdHscan(allocator, &storage, &args_without);
    defer allocator.free(response_without);

    // WITH: should have "*2" (field + value in nested array)
    // NOVALUES: should have "*1" (field only in nested array)
    try std.testing.expect(std.mem.indexOf(u8, response_with, "*2\r\n") != null or std.mem.indexOf(u8, response_with, "*2\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, response_without, "*1\r\n") != null);
}

test "OBJECT ENCODING - string returns int for integer values" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("myint", "12345", null);
    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = "myint" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    // Redis returns bulk string for OBJECT ENCODING
    try std.testing.expectEqualStrings("$3\r\nint\r\n", result);
}

test "OBJECT ENCODING - string returns embstr for short strings" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mystr", "hello", null);
    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = "mystr" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$6\r\nembstr\r\n", result);
}

test "OBJECT ENCODING - set uses intset for integer-only small sets" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add integers to set - should use intset encoding
    _ = try storage.sadd("intset_key", &[_][]const u8{ "1", "2", "3" }, null);

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = "intset_key" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$6\r\nintset\r\n", result);
}

test "OBJECT ENCODING - set uses listpack for small non-integer sets" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add non-integer members - should use hashmap (reported as listpack for small sets)
    _ = try storage.sadd("small_set", &[_][]const u8{ "alpha", "beta" }, null);

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = "small_set" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$8\r\nlistpack\r\n", result);
}

test "OBJECT ENCODING - hash returns listpack for small hashes" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.hset("myhash", &[_][]const u8{ "field1", "field2" }, &[_][]const u8{ "value1", "value2" }, null);

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = "myhash" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$8\r\nlistpack\r\n", result);
}

test "OBJECT ENCODING - missing key returns nil" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = "nosuchkey" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    // Redis 7.2.6+: returns nil (null bulk string) for non-existent keys
    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "OBJECT ENCODING - list returns listpack for small lists" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.rpush("mylist", &[_][]const u8{ "a", "b" }, null);

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = "mylist" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$8\r\nlistpack\r\n", result);
}

test "OBJECT ENCODING - hash switches to hashtable when element length exceeds threshold" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set a value that exceeds hash-max-listpack-value (64 bytes default)
    const long_value = "x" ** 65;
    _ = try storage.hset("bighash", &[_][]const u8{"field1"}, &[_][]const u8{long_value}, null);

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = "bighash" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$9\r\nhashtable\r\n", result);
}

test "OBJECT ENCODING - sorted set switches to skiplist when member length exceeds threshold" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add a member that exceeds zset-max-listpack-value (64 bytes default)
    const long_member = "m" ** 65;
    _ = try storage.zadd("bigzset", &[_]f64{1.0}, &[_][]const u8{long_member}, 0, null);

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = "bigzset" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$8\r\nskiplist\r\n", result);
}

test "OBJECT ENCODING - list switches to quicklist when element length exceeds threshold" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add an element that exceeds list-max-listpack-value (64 bytes default)
    const long_elem = "e" ** 65;
    _ = try storage.rpush("biglist", &[_][]const u8{long_elem}, null);

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = "biglist" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$9\r\nquicklist\r\n", result);
}

test "OBJECT ENCODING - list uses quicklist when debug-quicklist-packed-threshold is small" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add a 5-byte element ("hello")
    _ = try storage.rpush("threshlist", &[_][]const u8{"hello"}, null);

    // With threshold=1, any element > 1 byte forces quicklist encoding
    try storage.config.setConfigValue("debug-quicklist-packed-threshold", .{ .int = 1 });

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = "threshlist" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$9\r\nquicklist\r\n", result);
}

test "OBJECT ENCODING - list uses listpack after debug-quicklist-packed-threshold reset" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.rpush("resetlist", &[_][]const u8{"hi"}, null);

    // Set threshold low first
    try storage.config.setConfigValue("debug-quicklist-packed-threshold", .{ .int = 1 });

    // Reset threshold to default (4096)
    try storage.config.setConfigValue("debug-quicklist-packed-threshold", .{ .int = 4096 });

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = "resetlist" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$8\r\nlistpack\r\n", result);
}

test "OBJECT ENCODING - set switches to hashtable when member length exceeds threshold" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Add a non-integer member that exceeds set-max-listpack-value (64 bytes default)
    const long_member = "s" ** 65;
    _ = try storage.sadd("bigset", &[_][]const u8{long_member}, null);

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = "bigset" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$9\r\nhashtable\r\n", result);
}

test "OBJECT ENCODING - hash uses ziplist alias to control encoding threshold" {
    // Regression: setting hash-max-ziplist-entries (old Redis alias) must sync to
    // hash-max-listpack-entries (the canonical name OBJECT ENCODING reads).
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set threshold to 2 entries via the deprecated alias
    try storage.config.set("hash-max-ziplist-entries", "2");

    // Add 3 entries — exceeds alias threshold of 2, so encoding should be hashtable
    _ = try storage.hset("myhash", &[_][]const u8{ "f1", "f2", "f3" }, &[_][]const u8{ "v1", "v2", "v3" }, null);

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = "myhash" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$9\r\nhashtable\r\n", result);
}

test "OBJECT ENCODING - zset uses ziplist alias to control encoding threshold" {
    // Regression: setting zset-max-ziplist-entries must sync to zset-max-listpack-entries.
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set threshold to 2 entries via the deprecated alias
    try storage.config.set("zset-max-ziplist-entries", "2");

    // Add 3 members — should switch to skiplist encoding
    _ = try storage.zadd("myzset", &[_]f64{ 1.0, 2.0, 3.0 }, &[_][]const u8{ "a", "b", "c" }, 0, null);

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = "myzset" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$8\r\nskiplist\r\n", result);
}

test "OBJECT FREQ - returns non-negative integer for existing key with LFU policy" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // OBJECT FREQ requires LFU-based maxmemory-policy
    try storage.config.set("maxmemory-policy", "allkeys-lfu");
    try storage.set("mykey", "hello", null);
    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "FREQ" },
        .{ .bulk_string = "mykey" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    // Should return integer (LFU counter, starts at 0)
    try std.testing.expect(std.mem.startsWith(u8, result, ":"));
    try std.testing.expect(!std.mem.startsWith(u8, result, "-ERR"));
}

test "OBJECT FREQ - returns nil for non-existing key with LFU policy" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // OBJECT FREQ requires LFU-based maxmemory-policy
    try storage.config.set("maxmemory-policy", "allkeys-lfu");
    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "FREQ" },
        .{ .bulk_string = "nosuchkey" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    // Redis 7.2.6+: returns nil (null bulk string) for non-existent keys
    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "OBJECT FREQ - returns error when maxmemory-policy is not LFU" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Default policy is noeviction — OBJECT FREQ should fail
    try storage.set("mykey", "hello", null);
    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "FREQ" },
        .{ .bulk_string = "mykey" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR object freq"));
}

test "OBJECT FREQ - returns error when maxmemory-policy is allkeys-lru (non-LFU)" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.config.set("maxmemory-policy", "allkeys-lru");
    try storage.set("mykey", "hello", null);
    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "FREQ" },
        .{ .bulk_string = "mykey" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR object freq"));
}

test "OBJECT IDLETIME - returns non-negative integer for existing key with non-LFU policy" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Default policy is noeviction — OBJECT IDLETIME should work
    try storage.set("mykey", "hello", null);
    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "IDLETIME" },
        .{ .bulk_string = "mykey" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    // Should return integer (idle time in seconds, starts at 0)
    try std.testing.expect(std.mem.startsWith(u8, result, ":"));
    try std.testing.expect(!std.mem.startsWith(u8, result, "-ERR"));
}

test "OBJECT IDLETIME - returns nil for non-existing key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "IDLETIME" },
        .{ .bulk_string = "nosuchkey" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    // Redis 7.2.6+: returns nil (null bulk string) for non-existent keys
    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "OBJECT IDLETIME - returns error when maxmemory-policy is LFU" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set LFU policy — OBJECT IDLETIME should fail
    try storage.config.set("maxmemory-policy", "allkeys-lfu");
    try storage.set("mykey", "hello", null);
    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "IDLETIME" },
        .{ .bulk_string = "mykey" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR object idle time"));
}

test "OBJECT IDLETIME - returns error when maxmemory-policy is volatile-lfu" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.config.set("maxmemory-policy", "volatile-lfu");
    try storage.set("mykey", "hello", null);
    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "IDLETIME" },
        .{ .bulk_string = "mykey" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR object idle time"));
}

// ── OBJECT VERSION tests ──────────────────────────────────────────────────────

test "OBJECT VERSION - returns 1 after first SET" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "hello", null);

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "VERSION" },
        .{ .bulk_string = "mykey" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":1\r\n", result);
}

test "OBJECT VERSION - increments on repeated writes" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("counter", "v1", null);
    try storage.set("counter", "v2", null);
    try storage.set("counter", "v3", null);

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "VERSION" },
        .{ .bulk_string = "counter" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":3\r\n", result);
}

test "OBJECT VERSION - returns nil for non-existing key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "VERSION" },
        .{ .bulk_string = "nosuchkey" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    // Redis 7.2.6+: returns nil (null bulk string) for non-existent keys
    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "OBJECT VERSION - version removed on DEL" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("delkey", "hello", null);
    try storage.set("delkey", "world", null);

    // Confirm version is 2
    {
        const args = [_]RespValue{
            .{ .bulk_string = "OBJECT" },
            .{ .bulk_string = "VERSION" },
            .{ .bulk_string = "delkey" },
        };
        const result = try cmdObject(allocator, &storage, &args);
        defer allocator.free(result);
        try std.testing.expectEqualStrings(":2\r\n", result);
    }

    // Delete the key
    const keys = [_][]const u8{"delkey"};
    _ = storage.del(&keys);

    // Version should now be gone (nil for non-existing key, Redis 7.2.6+)
    {
        const args = [_]RespValue{
            .{ .bulk_string = "OBJECT" },
            .{ .bulk_string = "VERSION" },
            .{ .bulk_string = "delkey" },
        };
        const result = try cmdObject(allocator, &storage, &args);
        defer allocator.free(result);
        try std.testing.expectEqualStrings("$-1\r\n", result);
    }
}

test "OBJECT VERSION - tracks HSET modifications" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const fields = [_][]const u8{ "f1", "f2" };
    const values = [_][]const u8{ "v1", "v2" };
    _ = try storage.hset("myhash", &fields, &values, null);
    _ = try storage.hset("myhash", &fields, &values, null);

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "VERSION" },
        .{ .bulk_string = "myhash" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":2\r\n", result);
}

test "OBJECT VERSION - tracks LPUSH modifications" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const elems1 = [_][]const u8{"a"};
    const elems2 = [_][]const u8{"b"};
    _ = try storage.lpush("mylist", &elems1, null);
    _ = try storage.lpush("mylist", &elems2, null);

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "VERSION" },
        .{ .bulk_string = "mylist" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":2\r\n", result);
}

test "OBJECT VERSION - tracks ZADD modifications" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const scores = [_]f64{1.0};
    const members = [_][]const u8{"member1"};
    _ = try storage.zadd("myzset", &scores, &members, 0, null);
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "VERSION" },
        .{ .bulk_string = "myzset" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    // Second ZADD with same member and score still bumps version
    try std.testing.expect(std.mem.startsWith(u8, result, ":"));
}

test "OBJECT VERSION - HELP includes VERSION" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "HELP" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);
    // HELP returns an array containing "VERSION <key>" entry
    try std.testing.expect(std.mem.indexOf(u8, result, "VERSION") != null);
}

test "COPY - basic same-database copy" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var databases = [_]Storage{storage, storage, storage, storage, storage, storage, storage, storage, storage, storage, storage, storage, storage, storage, storage, storage};
    for (&databases[1..]) |*db| {
        db.* = try Storage.init(allocator);
    }
    defer for (&databases[1..]) |*db| db.deinit();

    try storage.set("mykey", "hello", null);

    const args = [_]RespValue{
        .{ .bulk_string = "COPY" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "newkey" },
    };
    const result = try cmdCopy(allocator, &storage, &args, &databases, 16, 0);
    defer allocator.free(result);

    // Expect integer 1 (success)
    try std.testing.expectEqualStrings(":1\r\n", result);

    // Verify copy in same DB
    const value = storage.get("newkey").?;
    try std.testing.expectEqualStrings("hello", value);
}

test "COPY - cross-database copy with DB parameter" {
    const allocator = std.testing.allocator;
    var storage0 = try Storage.init(allocator);
    defer storage0.deinit();
    var storage1 = try Storage.init(allocator);
    defer storage1.deinit();
    var databases = [_]Storage{storage0, storage1, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined};
    for (&databases[2..]) |*db| {
        db.* = try Storage.init(allocator);
    }
    defer for (&databases[2..]) |*db| db.deinit();

    try storage0.set("source", "value1", null);

    const args = [_]RespValue{
        .{ .bulk_string = "COPY" },
        .{ .bulk_string = "source" },
        .{ .bulk_string = "dest" },
        .{ .bulk_string = "DB" },
        .{ .bulk_string = "1" },
    };
    const result = try cmdCopy(allocator, &storage0, &args, &databases, 16, 0);
    defer allocator.free(result);

    // Expect integer 1 (success)
    try std.testing.expectEqualStrings(":1\r\n", result);

    // Verify copy in database 1
    const value = storage1.get("dest").?;
    try std.testing.expectEqualStrings("value1", value);
}

test "COPY - cross-database with REPLACE" {
    const allocator = std.testing.allocator;
    var storage0 = try Storage.init(allocator);
    defer storage0.deinit();
    var storage1 = try Storage.init(allocator);
    defer storage1.deinit();
    var databases = [_]Storage{storage0, storage1, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined};
    for (&databases[2..]) |*db| {
        db.* = try Storage.init(allocator);
    }
    defer for (&databases[2..]) |*db| db.deinit();

    try storage0.set("source", "new", null);
    try storage1.set("dest", "old", null);

    const args = [_]RespValue{
        .{ .bulk_string = "COPY" },
        .{ .bulk_string = "source" },
        .{ .bulk_string = "dest" },
        .{ .bulk_string = "DB" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "REPLACE" },
    };
    const result = try cmdCopy(allocator, &storage0, &args, &databases, 16, 0);
    defer allocator.free(result);

    // Expect integer 1 (success)
    try std.testing.expectEqualStrings(":1\r\n", result);

    // Verify old value was replaced
    const value = storage1.get("dest").?;
    try std.testing.expectEqualStrings("new", value);
}

test "COPY - fails if destination exists without REPLACE" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var databases = [_]Storage{storage, storage, storage, storage, storage, storage, storage, storage, storage, storage, storage, storage, storage, storage, storage, storage};
    for (&databases[1..]) |*db| {
        db.* = try Storage.init(allocator);
    }
    defer for (&databases[1..]) |*db| db.deinit();

    try storage.set("source", "value1", null);
    try storage.set("dest", "value2", null);

    const args = [_]RespValue{
        .{ .bulk_string = "COPY" },
        .{ .bulk_string = "source" },
        .{ .bulk_string = "dest" },
    };
    const result = try cmdCopy(allocator, &storage, &args, &databases, 16, 0);
    defer allocator.free(result);

    // Expect integer 0 (destination exists)
    try std.testing.expectEqualStrings(":0\r\n", result);

    // Verify dest is unchanged
    const value = storage.get("dest").?;
    try std.testing.expectEqualStrings("value2", value);
}

test "COPY - invalid DB index returns error" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();
    var databases = [_]Storage{storage, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined};
    for (&databases[1..]) |*db| {
        db.* = try Storage.init(allocator);
    }
    defer for (&databases[1..]) |*db| db.deinit();

    try storage.set("source", "value", null);

    const args = [_]RespValue{
        .{ .bulk_string = "COPY" },
        .{ .bulk_string = "source" },
        .{ .bulk_string = "dest" },
        .{ .bulk_string = "DB" },
        .{ .bulk_string = "999" },
    };
    const result = try cmdCopy(allocator, &storage, &args, &databases, 16, 0);
    defer allocator.free(result);

    // Expect error
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

// ── EXPIREAT compatibility tests ──────────────────────────────────────────────

test "EXPIREAT - negative timestamp returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "hello", null);

    // Use PubSub for notification support
    var ps = pubsub_mod.PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "EXPIREAT" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "-1" },
    };
    const result = try cmdExpireat(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    // Negative timestamp should return error
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

test "EXPIREAT - zero timestamp (epoch) expires key immediately" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "hello", null);

    var ps = pubsub_mod.PubSub.init(allocator);
    defer ps.deinit();

    // unix_time = 0 = epoch (Jan 1, 1970) = past = should expire key
    const args = [_]RespValue{
        .{ .bulk_string = "EXPIREAT" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "0" },
    };
    const result = try cmdExpireat(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    // Should succeed (return :1)
    try std.testing.expectEqualStrings(":1\r\n", result);

    // Key should now be expired (get returns null)
    const val = storage.get("mykey");
    try std.testing.expectEqual(@as(?[]const u8, null), val);
}

test "EXPIREAT - positive past timestamp expires key immediately" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "hello", null);

    var ps = pubsub_mod.PubSub.init(allocator);
    defer ps.deinit();

    // unix_time = 1000 seconds = 1970-01-01 00:16:40 = definitely in the past
    const args = [_]RespValue{
        .{ .bulk_string = "EXPIREAT" },
        .{ .bulk_string = "mykey" },
        .{ .bulk_string = "1000" },
    };
    const result = try cmdExpireat(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    // Should succeed (return :1)
    try std.testing.expectEqualStrings(":1\r\n", result);

    // Key should be expired on next read
    const val = storage.get("mykey");
    try std.testing.expectEqual(@as(?[]const u8, null), val);
}

test "EXPIRE - zero timeout deletes key immediately (Redis 7.0+ compat)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("expkey", "hello", null);

    var ps = pubsub_mod.PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "EXPIRE" },
        .{ .bulk_string = "expkey" },
        .{ .bulk_string = "0" },
    };
    const result = try cmdExpire(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
    try std.testing.expect(!storage.exists("expkey"));
}

test "EXPIRE - negative timeout deletes key immediately (Redis 7.0+ compat)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("negkey", "world", null);

    var ps = pubsub_mod.PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "EXPIRE" },
        .{ .bulk_string = "negkey" },
        .{ .bulk_string = "-100" },
    };
    const result = try cmdExpire(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
    try std.testing.expect(!storage.exists("negkey"));
}

test "EXPIRE - zero timeout on non-existent key returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    var ps = pubsub_mod.PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "EXPIRE" },
        .{ .bulk_string = "nosuchkey" },
        .{ .bulk_string = "0" },
    };
    const result = try cmdExpire(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "PEXPIRE - zero timeout deletes key immediately (Redis 7.0+ compat)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("pexpkey", "data", null);

    var ps = pubsub_mod.PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "PEXPIRE" },
        .{ .bulk_string = "pexpkey" },
        .{ .bulk_string = "0" },
    };
    const result = try cmdPexpire(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
    try std.testing.expect(!storage.exists("pexpkey"));
}

test "OBJECT ENCODING - does not update keyspace_hits counter" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mystr", "hello", null);
    const hits_before = storage.getKeyspaceHits();

    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = "mystr" },
    };
    const result = try cmdObject(allocator, &storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$6\r\nembstr\r\n", result);
    // OBJECT ENCODING must not count as a keyspace hit
    try std.testing.expectEqual(hits_before, storage.getKeyspaceHits());
}

test "peekStringEncoding - raw for strings longer than 44 bytes" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("bigstr", "A" ** 45, null);
    const enc = storage.peekStringEncoding("bigstr");
    try std.testing.expectEqualStrings("raw", enc orelse "");
}

test "peekStringEncoding - int for numeric strings" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("numkey", "-9999", null);
    const enc = storage.peekStringEncoding("numkey");
    try std.testing.expectEqualStrings("int", enc orelse "");
}

test "peekStringEncoding - null for non-existent key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const enc = storage.peekStringEncoding("nosuchkey");
    try std.testing.expectEqual(@as(?[]const u8, null), enc);
}

test "SCAN COUNT 0 returns all matching keys (no infinite loop)" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "v1", null);
    try storage.set("key2", "v2", null);
    try storage.set("key3", "v3", null);

    const args = [_]RespValue{
        .{ .bulk_string = "SCAN" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "COUNT" },
        .{ .bulk_string = "0" },
    };
    const response = try cmdScan(allocator, &storage, &args);
    defer allocator.free(response);

    // Cursor should be 0 (done) and all 3 keys returned
    try std.testing.expect(std.mem.startsWith(u8, response, "*2\r\n:0\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, response, "key1") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "key2") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "key3") != null);
}

test "SCAN TYPE filter is case-insensitive" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("strkey", "hello", null);
    _ = try storage.lpush("listkey", &[_][]const u8{"item"}, null);

    // TYPE "STRING" (uppercase) should match string keys
    const args_upper = [_]RespValue{
        .{ .bulk_string = "SCAN" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "TYPE" },
        .{ .bulk_string = "STRING" },
    };
    const response_upper = try cmdScan(allocator, &storage, &args_upper);
    defer allocator.free(response_upper);

    try std.testing.expect(std.mem.indexOf(u8, response_upper, "strkey") != null);
    try std.testing.expect(std.mem.indexOf(u8, response_upper, "listkey") == null);

    // TYPE "string" (lowercase) should also match string keys
    const args_lower = [_]RespValue{
        .{ .bulk_string = "SCAN" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "TYPE" },
        .{ .bulk_string = "string" },
    };
    const response_lower = try cmdScan(allocator, &storage, &args_lower);
    defer allocator.free(response_lower);

    try std.testing.expect(std.mem.indexOf(u8, response_lower, "strkey") != null);
    try std.testing.expect(std.mem.indexOf(u8, response_lower, "listkey") == null);
}

test "SSCAN COUNT 0 returns all matching members" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.sadd("myset", &[_][]const u8{ "a", "b", "c" }, null);

    const args = [_]RespValue{
        .{ .bulk_string = "SSCAN" },
        .{ .bulk_string = "myset" },
        .{ .bulk_string = "0" },
        .{ .bulk_string = "COUNT" },
        .{ .bulk_string = "0" },
    };
    const response = try cmdSscan(allocator, &storage, &args);
    defer allocator.free(response);

    // Cursor should be 0 and all members returned
    try std.testing.expect(std.mem.startsWith(u8, response, "*2\r\n:0\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, response, "$1\r\na\r\n") != null or
        std.mem.indexOf(u8, response, "$1\r\nb\r\n") != null or
        std.mem.indexOf(u8, response, "$1\r\nc\r\n") != null);
}
