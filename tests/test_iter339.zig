const std = @import("std");
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const keys_cmds = zoltraak.commands.keys_cmds;

// Iteration 339: OBJECT ENCODING for lists — proper byte-limit semantics
//
// Redis uses list-max-listpack-size to determine list encoding:
//   positive N  → entry count limit (listpack iff count ≤ N)
//   -1          → 4096-byte node limit
//   -2 (default)→ 8192-byte node limit
//   -3          → 16384-byte node limit
//   -4          → 32768-byte node limit
//   -5          → 65536-byte node limit
//
// Additionally, any element exceeding debug-quicklist-packed-threshold
// (default 4096) forces PLAIN encoding (quicklist).
//
// Old behaviour: used list-max-listpack-value (non-standard, 64 bytes) as
//   a per-element size limit, causing OBJECT ENCODING to return "quicklist"
//   for elements between 65-4096 bytes — wrong vs real Redis.
//
// New behaviour: entry-count or byte-limit semantics matching Redis.

// Helper: run OBJECT ENCODING key and return response (caller frees)
fn objectEncoding(allocator: std.mem.Allocator, storage: *Storage, key: []const u8) ![]const u8 {
    const args = [_]RespValue{
        .{ .bulk_string = "OBJECT" },
        .{ .bulk_string = "ENCODING" },
        .{ .bulk_string = key },
    };
    return keys_cmds.cmdObject(allocator, storage, &args);
}

test "OBJECT ENCODING - list with 65-byte element returns listpack (byte-limit semantics)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Default list-max-listpack-size=-2 → 8192-byte node limit.
    // 65 bytes << 8192 AND 65 < 4096 packed threshold → must be "listpack".
    // Previously returned "quicklist" because 65 > list-max-listpack-value(64).
    const elem = "e" ** 65;
    _ = try storage.rpush("list65", &[_][]const u8{elem}, null);

    const result = try objectEncoding(allocator, storage, "list65");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$8\r\nlistpack\r\n", result);
}

test "OBJECT ENCODING - list with 4097-byte element returns quicklist (exceeds packed threshold)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // 4097 bytes > debug-quicklist-packed-threshold (4096) → PLAIN node → "quicklist".
    const long_elem = "e" ** 4097;
    _ = try storage.rpush("listbig", &[_][]const u8{long_elem}, null);

    const result = try objectEncoding(allocator, storage, "listbig");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$9\r\nquicklist\r\n", result);
}

test "OBJECT ENCODING - list with 200 short elements returns listpack (byte-limit mode, no entry limit)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // 200 × 1 byte = 200 total bytes << 8192 byte limit.
    // Old code used entry-count fallback of 128 and returned "quicklist" (200 > 128).
    // Correct Redis behaviour: "listpack" because byte limit is not exceeded.
    var i: usize = 0;
    while (i < 200) : (i += 1) {
        _ = try storage.rpush("manyshort", &[_][]const u8{"x"}, null);
    }

    const result = try objectEncoding(allocator, storage, "manyshort");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$8\r\nlistpack\r\n", result);
}

test "OBJECT ENCODING - list returns quicklist when total bytes exceed 8192 (default -2 limit)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // 100 elements × 100 bytes = 10000 bytes > 8192 byte limit → "quicklist".
    const elem = "x" ** 100;
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        _ = try storage.rpush("bigbytes", &[_][]const u8{elem}, null);
    }

    const result = try objectEncoding(allocator, storage, "bigbytes");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$9\r\nquicklist\r\n", result);
}

test "OBJECT ENCODING - list entry count mode (positive list-max-listpack-size)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // With list-max-listpack-size=3, a list with 4 entries → "quicklist".
    try storage.config.setConfigValue("list-max-listpack-size", .{ .int = 3 });
    defer storage.config.setConfigValue("list-max-listpack-size", .{ .int = -2 }) catch {};

    _ = try storage.rpush("countlist", &[_][]const u8{ "a", "b", "c", "d" }, null);

    const result = try objectEncoding(allocator, storage, "countlist");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$9\r\nquicklist\r\n", result);
}

test "OBJECT ENCODING - list entry count mode ≤ limit returns listpack" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // With list-max-listpack-size=4, a list with 4 entries → "listpack".
    try storage.config.setConfigValue("list-max-listpack-size", .{ .int = 4 });
    defer storage.config.setConfigValue("list-max-listpack-size", .{ .int = -2 }) catch {};

    _ = try storage.rpush("exactlist", &[_][]const u8{ "a", "b", "c", "d" }, null);

    const result = try objectEncoding(allocator, storage, "exactlist");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$8\r\nlistpack\r\n", result);
}

test "OBJECT ENCODING - list -1 (4096-byte limit): 40 × 100-byte elements → listpack" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // list-max-listpack-size=-1 → 4096-byte limit.
    // 40 × 100 bytes = 4000 bytes ≤ 4096 → "listpack".
    try storage.config.setConfigValue("list-max-listpack-size", .{ .int = -1 });
    defer storage.config.setConfigValue("list-max-listpack-size", .{ .int = -2 }) catch {};

    const elem = "x" ** 100;
    var i: usize = 0;
    while (i < 40) : (i += 1) {
        _ = try storage.rpush("m4096", &[_][]const u8{elem}, null);
    }

    const result = try objectEncoding(allocator, storage, "m4096");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$8\r\nlistpack\r\n", result);
}

test "OBJECT ENCODING - list -1 (4096-byte limit): 42 × 100-byte elements → quicklist" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // list-max-listpack-size=-1 → 4096-byte limit.
    // 42 × 100 bytes = 4200 bytes > 4096 → "quicklist".
    try storage.config.setConfigValue("list-max-listpack-size", .{ .int = -1 });
    defer storage.config.setConfigValue("list-max-listpack-size", .{ .int = -2 }) catch {};

    const elem = "x" ** 100;
    var i: usize = 0;
    while (i < 42) : (i += 1) {
        _ = try storage.rpush("m4200", &[_][]const u8{elem}, null);
    }

    const result = try objectEncoding(allocator, storage, "m4200");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$9\r\nquicklist\r\n", result);
}
