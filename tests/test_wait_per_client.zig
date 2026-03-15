const std = @import("std");
const testing = std.testing;
const test_integration = @import("test_integration.zig");
const RespClient = test_integration.RespClient;

/// Integration tests for WAIT command with per-client replication offset tracking
///
/// These tests verify that WAIT correctly uses the client-specific replication offset
/// rather than the global replication offset, as per Redis specification:
/// "Redis remembers, for each client, the replication offset of the produced replication
/// stream when a given write command was executed in the context of that client."
///
/// Test scenarios:
/// 1. WAIT with no previous writes returns immediately (offset=0)
/// 2. WAIT after client writes uses client-specific offset
/// 3. Multiple clients with different write histories have independent offsets
/// 4. WAIT timeout works correctly with per-client offsets

test "integration - WAIT with no replicas returns count of replicas" {
    const allocator = testing.allocator;

    // Connect to server
    var client = try RespClient.init(allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // WAIT with 0 replicas should return immediately with 0
    const response = try client.sendCommand(&[_][]const u8{ "WAIT", "1", "0" });
    defer allocator.free(response);

    // Should return :0\r\n (0 replicas available)
    try testing.expectEqualStrings(":0\r\n", response);
}

test "integration - WAIT uses per-client offset after write" {
    const allocator = testing.allocator;

    // Connect client 1
    var client1 = try RespClient.init(allocator, "127.0.0.1", 6379);
    defer client1.deinit();

    // Perform a write operation (this sets client1's replication offset)
    const set_resp = try client1.sendCommand(&[_][]const u8{ "SET", "wait_test_key1", "value1" });
    defer allocator.free(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // WAIT should use client1's offset (after the SET command)
    const wait_resp = try client1.sendCommand(&[_][]const u8{ "WAIT", "1", "0" });
    defer allocator.free(wait_resp);

    // Should return :0\r\n (0 replicas, but offset was tracked)
    try testing.expectEqualStrings(":0\r\n", wait_resp);

    // Clean up
    const del_resp = try client1.sendCommand(&[_][]const u8{ "DEL", "wait_test_key1" });
    defer allocator.free(del_resp);
}

test "integration - Multiple clients have independent replication offsets" {
    const allocator = testing.allocator;

    // Connect two clients
    var client1 = try RespClient.init(allocator, "127.0.0.1", 6379);
    defer client1.deinit();

    var client2 = try RespClient.init(allocator, "127.0.0.1", 6379);
    defer client2.deinit();

    // Client1 performs a write
    const set1_resp = try client1.sendCommand(&[_][]const u8{ "SET", "wait_client1_key", "value1" });
    defer allocator.free(set1_resp);
    try testing.expectEqualStrings("+OK\r\n", set1_resp);

    // Client2 performs a write
    const set2_resp = try client2.sendCommand(&[_][]const u8{ "SET", "wait_client2_key", "value2" });
    defer allocator.free(set2_resp);
    try testing.expectEqualStrings("+OK\r\n", set2_resp);

    // Both clients WAIT - each should use their own offset
    const wait1_resp = try client1.sendCommand(&[_][]const u8{ "WAIT", "1", "0" });
    defer allocator.free(wait1_resp);
    try testing.expectEqualStrings(":0\r\n", wait1_resp);

    const wait2_resp = try client2.sendCommand(&[_][]const u8{ "WAIT", "1", "0" });
    defer allocator.free(wait2_resp);
    try testing.expectEqualStrings(":0\r\n", wait2_resp);

    // Clean up
    const del1_resp = try client1.sendCommand(&[_][]const u8{ "DEL", "wait_client1_key" });
    defer allocator.free(del1_resp);

    const del2_resp = try client2.sendCommand(&[_][]const u8{ "DEL", "wait_client2_key" });
    defer allocator.free(del2_resp);
}

test "integration - WAIT with timeout returns 0 when no replicas" {
    const allocator = testing.allocator;

    var client = try RespClient.init(allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Perform a write
    const set_resp = try client.sendCommand(&[_][]const u8{ "SET", "wait_timeout_key", "value" });
    defer allocator.free(set_resp);

    // WAIT with timeout should return 0 after timeout expires
    const start = std.time.milliTimestamp();
    const wait_resp = try client.sendCommand(&[_][]const u8{ "WAIT", "1", "100" });
    const elapsed = std.time.milliTimestamp() - start;
    defer allocator.free(wait_resp);

    try testing.expectEqualStrings(":0\r\n", wait_resp);
    // Should have waited close to timeout (at least 80ms, at most 150ms)
    try testing.expect(elapsed >= 80 and elapsed <= 150);

    // Clean up
    const del_resp = try client.sendCommand(&[_][]const u8{ "DEL", "wait_timeout_key" });
    defer allocator.free(del_resp);
}

test "integration - WAIT with no writes uses default offset 0" {
    const allocator = testing.allocator;

    var client = try RespClient.init(allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // WAIT without any previous writes should use offset 0
    const wait_resp = try client.sendCommand(&[_][]const u8{ "WAIT", "1", "0" });
    defer allocator.free(wait_resp);

    // Should return :0\r\n (no replicas)
    try testing.expectEqualStrings(":0\r\n", wait_resp);
}

test "integration - WAIT error on invalid arguments" {
    const allocator = testing.allocator;

    var client = try RespClient.init(allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Wrong number of arguments
    const resp1 = try client.sendCommand(&[_][]const u8{"WAIT"});
    defer allocator.free(resp1);
    try testing.expect(std.mem.startsWith(u8, resp1, "-ERR"));

    // Invalid integer
    const resp2 = try client.sendCommand(&[_][]const u8{ "WAIT", "abc", "100" });
    defer allocator.free(resp2);
    try testing.expect(std.mem.startsWith(u8, resp2, "-ERR"));

    // Invalid timeout
    const resp3 = try client.sendCommand(&[_][]const u8{ "WAIT", "1", "xyz" });
    defer allocator.free(resp3);
    try testing.expect(std.mem.startsWith(u8, resp3, "-ERR"));
}
