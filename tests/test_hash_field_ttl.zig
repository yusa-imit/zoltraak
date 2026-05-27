const std = @import("std");
const testing = std.testing;
const protocol = @import("../src/protocol/parser.zig");
const hashes = @import("../src/commands/hashes.zig");
const storage_mod = @import("../src/storage/memory.zig");
const pubsub_mod = @import("../src/storage/pubsub.zig");

const RespValue = protocol.RespValue;
const Storage = storage_mod.Storage;
const PubSub = pubsub_mod.PubSub;

test "HEXPIRE - set expiration on hash fields" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "value2" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    const expire_args = [_]RespValue{
        .{ .bulk_string = "HEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "10" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try hashes.cmdHexpire(allocator, storage, &expire_args, &ps, 0);
    defer allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "*1") != null);
}

test "HPEXPIRE - set expiration on hash fields in milliseconds" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    const expire_args = [_]RespValue{
        .{ .bulk_string = "HPEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "5000" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try hashes.cmdHpexpire(allocator, storage, &expire_args, &ps, 0);
    defer allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "*1") != null);
}

test "HPERSIST - remove expiration from hash fields" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    const expire_args = [_]RespValue{
        .{ .bulk_string = "HEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "10" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    _ = try hashes.cmdHexpire(allocator, storage, &expire_args, &ps, 0);

    const persist_args = [_]RespValue{
        .{ .bulk_string = "HPERSIST" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try hashes.cmdHpersist(allocator, storage, &persist_args, &ps, 0);
    defer allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "*1") != null);
}

test "HTTL - get TTL in seconds for hash fields" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
        .{ .bulk_string = "field2" },
        .{ .bulk_string = "value2" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    const expire_args = [_]RespValue{
        .{ .bulk_string = "HEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "10" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    _ = try hashes.cmdHexpire(allocator, storage, &expire_args, &ps, 0);

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

    // Array with 2 integers: field1 has TTL ~10s, field2 has -1 (no expiry)
    try testing.expect(std.mem.indexOf(u8, response, "*2") != null);
}

test "HPTTL - get TTL in milliseconds for hash fields" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    const expire_args = [_]RespValue{
        .{ .bulk_string = "HPEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "5000" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    _ = try hashes.cmdHpexpire(allocator, storage, &expire_args, &ps, 0);

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
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    const future_ts = std.time.timestamp() + 3600;
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
    const response = try hashes.cmdHexpireat(allocator, storage, &expire_args, &ps, 0);
    defer allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "*1") != null);
}

test "HPEXPIREAT - set expiration at absolute timestamp (milliseconds)" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    const future_ts = std.time.milliTimestamp() + 3600000;
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
    const response = try hashes.cmdHpexpireat(allocator, storage, &expire_args, &ps, 0);
    defer allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "*1") != null);
}

test "HEXPIRETIME - get expiration timestamp in seconds" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    const expire_args = [_]RespValue{
        .{ .bulk_string = "HEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "60" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    _ = try hashes.cmdHexpire(allocator, storage, &expire_args, &ps, 0);

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
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    const expire_args = [_]RespValue{
        .{ .bulk_string = "HPEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "60000" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    _ = try hashes.cmdHpexpire(allocator, storage, &expire_args, &ps, 0);

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

// --- Tests for Iteration 297: NX/XX/GT/LT options before FIELDS keyword ---

test "HEXPIRE - NX option before FIELDS (set only if no TTL)" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    // NX: set TTL only if field has no TTL
    const expire_args = [_]RespValue{
        .{ .bulk_string = "HEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "60" },
        .{ .bulk_string = "NX" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try hashes.cmdHexpire(allocator, storage, &expire_args, &ps, 0);
    defer allocator.free(response);

    // Should return 1 (TTL was set because field had no expiry)
    try testing.expect(std.mem.indexOf(u8, response, ":1") != null);
}

test "HEXPIRE - XX option before FIELDS (set only if has TTL)" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    // XX: field has no TTL, so condition fails
    const expire_args = [_]RespValue{
        .{ .bulk_string = "HEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "60" },
        .{ .bulk_string = "XX" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try hashes.cmdHexpire(allocator, storage, &expire_args, &ps, 0);
    defer allocator.free(response);

    // Should return 0 (condition not met — no existing TTL)
    try testing.expect(std.mem.indexOf(u8, response, ":0") != null);
}

test "HEXPIRE - GT option before FIELDS (set only if new TTL greater)" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    // Set initial TTL of 10 seconds
    const expire_args1 = [_]RespValue{
        .{ .bulk_string = "HEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "10" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    _ = try hashes.cmdHexpire(allocator, storage, &expire_args1, &ps, 0);

    // GT with 60s (greater than 10s) — should succeed
    const expire_args2 = [_]RespValue{
        .{ .bulk_string = "HEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "60" },
        .{ .bulk_string = "GT" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try hashes.cmdHexpire(allocator, storage, &expire_args2, &ps, 0);
    defer allocator.free(response);

    // Should return 1 (new TTL is greater)
    try testing.expect(std.mem.indexOf(u8, response, ":1") != null);
}

test "HEXPIRE - LT option before FIELDS (set only if new TTL less)" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    // Set initial TTL of 60 seconds
    const expire_args1 = [_]RespValue{
        .{ .bulk_string = "HEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "60" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    _ = try hashes.cmdHexpire(allocator, storage, &expire_args1, &ps, 0);

    // LT with 10s (less than 60s) — should succeed
    const expire_args2 = [_]RespValue{
        .{ .bulk_string = "HEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "10" },
        .{ .bulk_string = "LT" },
        .{ .bulk_string = "FIELDS" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try hashes.cmdHexpire(allocator, storage, &expire_args2, &ps, 0);
    defer allocator.free(response);

    // Should return 1 (new TTL is less)
    try testing.expect(std.mem.indexOf(u8, response, ":1") != null);
}

test "HEXPIRE - case-insensitive FIELDS keyword" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const set_args = [_]RespValue{
        .{ .bulk_string = "HSET" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "field1" },
        .{ .bulk_string = "value1" },
    };
    _ = try hashes.cmdHset(allocator, storage, &set_args);

    // Use lowercase "fields" keyword
    const expire_args = [_]RespValue{
        .{ .bulk_string = "HEXPIRE" },
        .{ .bulk_string = "myhash" },
        .{ .bulk_string = "60" },
        .{ .bulk_string = "fields" },
        .{ .bulk_string = "1" },
        .{ .bulk_string = "field1" },
    };
    const response = try hashes.cmdHexpire(allocator, storage, &expire_args, &ps, 0);
    defer allocator.free(response);

    // Should succeed with case-insensitive FIELDS
    try testing.expect(std.mem.indexOf(u8, response, ":1") != null);
}
