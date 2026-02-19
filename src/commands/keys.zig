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
