const std = @import("std");
const testing = std.testing;
const protocol = @import("../src/protocol/parser.zig");
const hashes = @import("../src/commands/hashes.zig");
const storage_mod = @import("../src/storage/memory.zig");

const RespValue = protocol.RespValue;
const Storage = storage_mod.Storage;

test "HEXPIRE - set expiration on hash fields" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create hash with fields
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "value2" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    // Set expiration on field1 for 10 seconds
    const expire_args = [_]RespValue{
        .{ .bulk_string = "HEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "10" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try hashes.cmdHexpire(allocator, storage, &expire_args);
    defer allocator.free(response);

    // Should return array with 1 (field expiration set)
    try testing.expect(std.mem.indexOf(u8, response, "*1") != null);
}

test "HPEXPIRE - set expiration on hash fields in milliseconds" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create hash with fields
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    // Set expiration for 5000ms
    const expire_args = [_]RespValue{
        .{ .bulk_string = "HPEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "5000" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try hashes.cmdHpexpire(allocator, storage, &expire_args);
    defer allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "*1") != null);
}

test "HPERSIST - remove expiration from hash fields" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create hash with fields
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    // Set expiration first
    const expire_args = [_]RespValue{
        .{ .bulk_string = "HEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "10" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    _ = try hashes.cmdHexpire(allocator, storage, &expire_args);

    // Now remove expiration
    const persist_args = [_]RespValue{
        .{ .bulk_string = "HPERSIST" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try hashes.cmdHpersist(allocator, storage, &persist_args);
    defer allocator.free(response);

    // Should return array with 1 (expiration removed)
    try testing.expect(std.mem.indexOf(u8, response, "*1") != null);
}

test "HTTL - get TTL in seconds for hash fields" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create hash with fields
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "value2" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    // Set expiration on field1
    const expire_args = [_]RespValue{
        .{ .bulk_string = "HEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "10" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    _ = try hashes.cmdHexpire(allocator, storage, &expire_args);

    // Get TTL for both fields
    const ttl_args = [_]RespValue{
        .{ .bulk_string = "HTTL" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "2" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "field2" },
    };
    const response = try hashes.cmdHttpl(allocator, storage, &ttl_args);
    defer allocator.free(response);

    // Should return array with 2 integers
    try testing.expect(std.mem.indexOf(u8, response, "*2") != null);
    // field1 should have TTL > 0, field2 should be -1 (no expiry)
}

test "HPTTL - get TTL in milliseconds for hash fields" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create hash with fields
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    // Set expiration
    const expire_args = [_]RespValue{
        .{ .bulk_string = "HPEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "5000" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    _ = try hashes.cmdHpexpire(allocator, storage, &expire_args);

    // Get TTL
    const ttl_args = [_]RespValue{
        .{ .bulk_string = "HPTTL" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try hashes.cmdHpttl(allocator, storage, &ttl_args);
    defer allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "*1") != null);
}

test "HEXPIREAT - set expiration at absolute timestamp (seconds)" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create hash
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    // Set expiration at future timestamp
    const future_ts = std.time.timestamp() + 3600; // 1 hour from now
    var ts_buf: [32]u8 = undefined;
    const ts_str = try std.fmt.bufPrint(&ts_buf, "{d}", .{future_ts});

    const expire_args = [_]RespValue{
        .{ .bulk_string = "HEXPIREAT" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = ts_str },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try hashes.cmdHexpireat(allocator, storage, &expire_args);
    defer allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "*1") != null);
}

test "HPEXPIREAT - set expiration at absolute timestamp (milliseconds)" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create hash
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    // Set expiration at future timestamp (ms)
    const future_ts = std.time.milliTimestamp() + 3600000; // 1 hour from now
    var ts_buf: [32]u8 = undefined;
    const ts_str = try std.fmt.bufPrint(&ts_buf, "{d}", .{future_ts});

    const expire_args = [_]RespValue{
        .{ .bulk_string = "HPEXPIREAT" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = ts_str },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try hashes.cmdHpexpireat(allocator, storage, &expire_args);
    defer allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "*1") != null);
}

test "HEXPIRETIME - get expiration timestamp in seconds" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create hash
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    // Set expiration
    const expire_args = [_]RespValue{
        .{ .bulk_string = "HEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "60" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    _ = try hashes.cmdHexpire(allocator, storage, &expire_args);

    // Get expiration time
    const time_args = [_]RespValue{
        .{ .bulk_string = "HEXPIRETIME" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try hashes.cmdHexpiretime(allocator, storage, &time_args);
    defer allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "*1") != null);
}

test "HPEXPIRETIME - get expiration timestamp in milliseconds" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create hash
    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    // Set expiration
    const expire_args = [_]RespValue{
        .{ .bulk_string = "HPEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "60000" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    _ = try hashes.cmdHpexpire(allocator, storage, &expire_args);

    // Get expiration time
    const time_args = [_]RespValue{
        .{ .bulk_string = "HPEXPIRETIME" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try hashes.cmdHpexpiretime(allocator, storage, &time_args);
    defer allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "*1") != null);
}
