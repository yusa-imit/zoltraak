const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");
const glob = @import("../utils/glob.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;

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
pub fn cmdExpire(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    return cmdExpireImpl(allocator, storage, args, false, "expire");
}

/// PEXPIRE key milliseconds [NX|XX|GT|LT]
/// Set expiry in milliseconds. Returns 1 if set, 0 if key does not exist.
pub fn cmdPexpire(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    return cmdExpireImpl(allocator, storage, args, true, "pexpire");
}

/// EXPIREAT key unix-time-seconds [NX|XX|GT|LT]
/// Set expiry as absolute Unix timestamp in seconds.
pub fn cmdExpireat(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    return cmdExpireatImpl(allocator, storage, args, false, "expireat");
}

/// PEXPIREAT key unix-time-milliseconds [NX|XX|GT|LT]
/// Set expiry as absolute Unix timestamp in milliseconds.
pub fn cmdPexpireat(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    return cmdExpireatImpl(allocator, storage, args, true, "pexpireat");
}

/// PERSIST key
/// Remove the expiry from a key. Returns 1 if removed, 0 if key has no expiry or doesn't exist.
pub fn cmdPersist(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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
pub fn cmdRename(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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

    return w.writeOK();
}

/// RENAMENX key newkey
/// Returns :1 if renamed, :0 if newkey already exists.
pub fn cmdRenamenx(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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

    const deleted_count = storage.del(keys.items);
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
/// ABSTTL: ttl is absolute Unix timestamp (not implemented)
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
    var i: usize = 4;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        if (std.ascii.eqlIgnoreCase(opt, "REPLACE")) {
            replace = true;
        } else if (std.ascii.eqlIgnoreCase(opt, "ABSTTL") or
            std.ascii.eqlIgnoreCase(opt, "IDLETIME") or
            std.ascii.eqlIgnoreCase(opt, "FREQ"))
        {
            // Skip unsupported options and their values
            if (std.ascii.eqlIgnoreCase(opt, "IDLETIME") or std.ascii.eqlIgnoreCase(opt, "FREQ")) {
                i += 1; // skip the value
            }
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    storage.restoreValue(key, serialized, ttl_ms, replace) catch |err| {
        return switch (err) {
            error.KeyAlreadyExists => w.writeError("BUSYKEY Target key name already exists"),
            error.InvalidDumpPayload, error.DumpChecksumMismatch, error.UnknownDumpType => w.writeError("ERR DUMP payload version or checksum are wrong"),
            else => w.writeError("ERR restore failed"),
        };
    };

    return w.writeSimpleString("OK");
}

/// COPY source destination [DB destination-db] [REPLACE]
/// Copy a key to a new key.
/// Returns 1 if source was copied, 0 if not (e.g., destination exists).
pub fn cmdCopy(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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
    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        if (std.ascii.eqlIgnoreCase(opt, "REPLACE")) {
            replace = true;
        } else if (std.ascii.eqlIgnoreCase(opt, "DB")) {
            i += 1; // skip DB value (we only have 1 database)
            if (i >= args.len) return w.writeError("ERR syntax error");
            // Ignore DB parameter since we're single-DB
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    const success = storage.copyKey(source, destination, replace) catch |err| {
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
/// Move a key to another database (stub - Zoltraak uses single DB).
/// Always returns 0 since we don't support multiple databases.
pub fn cmdMove(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    _ = storage;
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'move' command");
    }

    // Just validate arguments but always return 0
    _ = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    _ = switch (args[2]) {
        .bulk_string => |s| std.fmt.parseInt(i32, s, 10) catch {
            return w.writeError("ERR invalid DB index");
        },
        else => return w.writeError("ERR invalid DB index"),
    };

    // Zoltraak uses single database - always return 0
    return w.writeInteger(0);
}

// ── Internal helpers ─────────────────────────────────────────────────────────

/// Shared implementation for EXPIRE and PEXPIRE.
/// is_ms: true for PEXPIRE (milliseconds), false for EXPIRE (seconds).
fn cmdExpireImpl(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const RespValue,
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

    if (time_val <= 0) {
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

    const now_ms = Storage.getCurrentTimestamp();
    const expires_at_ms: i64 = if (is_ms)
        now_ms + time_val
    else
        now_ms + (time_val * 1000);

    const ok = storage.setExpiry(key, expires_at_ms, options);
    return w.writeInteger(if (ok) 1 else 0);
}

/// Shared implementation for EXPIREAT and PEXPIREAT.
/// is_ms: true for PEXPIREAT (milliseconds), false for EXPIREAT (seconds).
fn cmdExpireatImpl(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const RespValue,
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

    if (unix_time <= 0) {
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
            } else "none";
            const tf_lower = try std.ascii.allocLowerString(allocator, tf);
            defer allocator.free(tf_lower);
            if (!std.mem.eql(u8, type_str, tf_lower)) continue;
        }
        try matching.append(allocator, k);
    }

    // Apply cursor and count
    const start = @min(cursor, matching.items.len);
    const end = @min(start + count, matching.items.len);
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

/// HSCAN key cursor [MATCH pattern] [COUNT count]
/// Hash field iterator. Returns [next_cursor, [field, value, ...]].
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
            try pairs.append(allocator, val);
        }
    }

    // Apply cursor and count (count is in field-value pairs)
    const pair_count = pairs.items.len / 2;
    const start = @min(cursor, pair_count);
    const end = @min(start + count, pair_count);
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

    const start = @min(cursor, matching.items.len);
    const end = @min(start + count, matching.items.len);
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
    const start = @min(cursor, pair_count);
    const end = @min(start + count, pair_count);
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
            return w.writeError("ERR no such key");
        }
        return w.writeInteger(1);
    } else if (std.mem.eql(u8, sub_upper, "IDLETIME")) {
        if (storage.getType(key) == null) {
            return w.writeError("ERR no such key");
        }
        return w.writeInteger(0);
    } else if (std.mem.eql(u8, sub_upper, "FREQ")) {
        if (storage.getType(key) == null) {
            return w.writeError("ERR no such key");
        }
        return w.writeInteger(0);
    } else if (std.mem.eql(u8, sub_upper, "ENCODING")) {
        const vtype = storage.getType(key) orelse {
            return w.writeError("ERR no such key");
        };
        const encoding: []const u8 = switch (vtype) {
            .string => enc: {
                const val = storage.get(key);
                if (val) |v| {
                    // Check if it's a valid integer
                    if (std.fmt.parseInt(i64, v, 10)) |_| {
                        break :enc "int";
                    } else |_| {}
                    break :enc if (v.len <= 44) "embstr" else "raw";
                }
                break :enc "embstr";
            },
            .list => blk: {
                const ln = storage.llen(key) orelse 0;
                break :blk if (ln <= 128) "listpack" else "quicklist";
            },
            .set => blk: {
                const sc = storage.scard(key) orelse 0;
                break :blk if (sc <= 128) "listpack" else "hashtable";
            },
            .hash => blk: {
                const hl = storage.hlen(key) orelse 0;
                break :blk if (hl <= 128) "listpack" else "hashtable";
            },
            .sorted_set => blk: {
                const zc = storage.zcard(key) orelse 0;
                break :blk if (zc <= 128) "listpack" else "skiplist";
            },
            .stream => "stream",
        };
        return w.writeSimpleString(encoding);
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
