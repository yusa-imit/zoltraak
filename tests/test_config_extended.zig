const std = @import("std");
const testing = std.testing;

// Import test helpers from main integration test file
const RespClient = @import("test_integration.zig").RespClient;
const TestServer = @import("test_integration.zig").TestServer;

// ============================================================================
// Extended CONFIG Command Integration Tests
// ============================================================================
// These tests complement the basic CONFIG tests in test_integration.zig
// and focus on edge cases, glob patterns, and error conditions.

// ── Glob Pattern Tests ──────────────────────────────────────────────────────

test "CONFIG GET - question mark wildcard pattern" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Pattern "?ort" should match "port"
    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET", "?ort" });
    defer testing.allocator.free(response);

    // Verify RESP array format and content
    try testing.expect(std.mem.startsWith(u8, response, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, response, "port") != null);
    try testing.expect(std.mem.indexOf(u8, response, "6379") != null);
}

test "CONFIG GET - character class pattern" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Pattern "[pb]ort" should match "port" but not "databases"
    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET", "[pb]ort" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "port") != null);
    try testing.expect(std.mem.indexOf(u8, response, "databases") == null);
}

test "CONFIG GET - multiple patterns" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Request multiple patterns: max* and append*
    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET", "max*", "append*" });
    defer testing.allocator.free(response);

    // Should match maxmemory, maxmemory-policy, appendonly, appendfsync
    try testing.expect(std.mem.indexOf(u8, response, "maxmemory") != null);
    try testing.expect(std.mem.indexOf(u8, response, "maxmemory-policy") != null);
    try testing.expect(std.mem.indexOf(u8, response, "appendonly") != null);
    try testing.expect(std.mem.indexOf(u8, response, "appendfsync") != null);
}

test "CONFIG GET - no matches returns empty array" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Pattern that doesn't match any parameter
    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET", "nonexistent*" });
    defer testing.allocator.free(response);

    // Should return empty array: *0\r\n
    try testing.expectEqualStrings("*0\r\n", response);
}

test "CONFIG GET - pattern with multiple wildcards" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Pattern with multiple * wildcards
    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET", "*mem*" });
    defer testing.allocator.free(response);

    // Should match maxmemory and maxmemory-policy
    try testing.expect(std.mem.indexOf(u8, response, "maxmemory") != null);
}

// ── Parameter Validation Tests ──────────────────────────────────────────────

test "CONFIG SET - invalid maxmemory-policy enum" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Try to set invalid eviction policy
    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "SET", "maxmemory-policy", "invalid-policy" });
    defer testing.allocator.free(response);

    // Should return error
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, response, "Invalid argument") != null);
}

test "CONFIG SET - valid maxmemory-policy values" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Test all valid eviction policies
    const valid_policies = [_][]const u8{
        "noeviction",
        "allkeys-lru",
        "volatile-lru",
        "allkeys-lfu",
        "volatile-lfu",
        "allkeys-random",
        "volatile-random",
        "volatile-ttl",
    };

    for (valid_policies) |policy| {
        const set_response = try client.sendCommand(&[_][]const u8{ "CONFIG", "SET", "maxmemory-policy", policy });
        defer testing.allocator.free(set_response);
        try testing.expectEqualStrings("+OK\r\n", set_response);

        // Verify it was set
        const get_response = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET", "maxmemory-policy" });
        defer testing.allocator.free(get_response);
        try testing.expect(std.mem.indexOf(u8, get_response, policy) != null);
    }
}

test "CONFIG SET - invalid appendfsync enum" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Try to set invalid appendfsync mode
    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "SET", "appendfsync", "sometimes" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, response, "Invalid argument") != null);
}

test "CONFIG SET - valid appendfsync values" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const valid_modes = [_][]const u8{ "always", "everysec", "no" };

    for (valid_modes) |mode| {
        const set_response = try client.sendCommand(&[_][]const u8{ "CONFIG", "SET", "appendfsync", mode });
        defer testing.allocator.free(set_response);
        try testing.expectEqualStrings("+OK\r\n", set_response);

        const get_response = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET", "appendfsync" });
        defer testing.allocator.free(get_response);
        try testing.expect(std.mem.indexOf(u8, get_response, mode) != null);
    }
}

test "CONFIG SET - negative value for integer parameter" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Try to set negative maxmemory (should fail validation)
    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "SET", "maxmemory", "-1000" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, response, "Invalid argument") != null);
}

test "CONFIG SET - boolean accepts multiple formats" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Test various boolean true formats
    const true_values = [_][]const u8{ "yes", "YES", "true", "TRUE", "1" };
    for (true_values) |val| {
        const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "SET", "appendonly", val });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("+OK\r\n", response);

        const get_response = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET", "appendonly" });
        defer testing.allocator.free(get_response);
        try testing.expect(std.mem.indexOf(u8, get_response, "yes") != null);
    }

    // Test various boolean false formats
    const false_values = [_][]const u8{ "no", "NO", "false", "FALSE", "0" };
    for (false_values) |val| {
        const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "SET", "appendonly", val });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("+OK\r\n", response);

        const get_response = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET", "appendonly" });
        defer testing.allocator.free(get_response);
        try testing.expect(std.mem.indexOf(u8, get_response, "no") != null);
    }
}

// ── Error Handling Tests ────────────────────────────────────────────────────

test "CONFIG GET - wrong number of arguments" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // CONFIG GET with no pattern
    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null);
}

test "CONFIG SET - wrong number of arguments" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // CONFIG SET with only parameter name, no value
    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "SET", "maxmemory" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null);
}

test "CONFIG SET - odd number of arguments fails" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // CONFIG SET with 3 arguments after SET (param1 val1 param2 with no val2)
    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "SET", "maxmemory", "1024", "timeout" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null);
}

test "CONFIG SET - unknown parameter" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "SET", "nonexistent-param", "value" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, response, "Unsupported CONFIG parameter") != null);
}

test "CONFIG SET - read-only parameters cannot be changed" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Test all read-only parameters
    const readonly_params = [_]struct { name: []const u8, value: []const u8 }{
        .{ .name = "port", .value = "9999" },
        .{ .name = "bind", .value = "0.0.0.0" },
        .{ .name = "databases", .value = "16" },
    };

    for (readonly_params) |param| {
        const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "SET", param.name, param.value });
        defer testing.allocator.free(response);

        try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
        try testing.expect(std.mem.indexOf(u8, response, "read-only") != null);
    }
}

test "CONFIG - unknown subcommand" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "INVALID" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, response, "unknown CONFIG subcommand") != null);
}

test "CONFIG - no subcommand" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{"CONFIG"});
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null);
}

// ── RESP Protocol Tests ─────────────────────────────────────────────────────

test "CONFIG GET - RESP array format validation" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET", "port" });
    defer testing.allocator.free(response);

    // Parse RESP array: should be *2\r\n$4\r\nport\r\n$4\r\n6379\r\n
    try testing.expect(std.mem.startsWith(u8, response, "*2\r\n"));

    // Count the number of bulk strings
    var count: usize = 0;
    var idx: usize = 0;
    while (idx < response.len) {
        if (response[idx] == '$') {
            count += 1;
        }
        idx += 1;
    }
    // Should have 2 bulk strings (parameter name and value)
    try testing.expectEqual(@as(usize, 2), count);
}

test "CONFIG SET - multiple parameter pairs RESP validation" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set two parameters at once
    const response = try client.sendCommand(&[_][]const u8{
        "CONFIG",
        "SET",
        "maxmemory",
        "2048",
        "timeout",
        "120",
    });
    defer testing.allocator.free(response);

    // Should return simple string +OK\r\n
    try testing.expectEqualStrings("+OK\r\n", response);

    // Verify both were set
    const get1 = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET", "maxmemory" });
    defer testing.allocator.free(get1);
    try testing.expect(std.mem.indexOf(u8, get1, "2048") != null);

    const get2 = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET", "timeout" });
    defer testing.allocator.free(get2);
    try testing.expect(std.mem.indexOf(u8, get2, "120") != null);
}

// ── REWRITE Persistence Tests ───────────────────────────────────────────────

test "CONFIG REWRITE - multiple parameters persisted" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set multiple parameters
    _ = try client.sendCommand(&[_][]const u8{ "CONFIG", "SET", "maxmemory", "8192" });
    _ = try client.sendCommand(&[_][]const u8{ "CONFIG", "SET", "timeout", "300" });
    _ = try client.sendCommand(&[_][]const u8{ "CONFIG", "SET", "appendonly", "yes" });

    // Rewrite config
    const rewrite_resp = try client.sendCommand(&[_][]const u8{ "CONFIG", "REWRITE" });
    defer testing.allocator.free(rewrite_resp);
    try testing.expectEqualStrings("+OK\r\n", rewrite_resp);

    // Read the config file
    const file = try std.fs.cwd().openFile("zoltraak.conf", .{});
    defer {
        file.close();
        std.fs.cwd().deleteFile("zoltraak.conf") catch {};
    }

    const content = try file.readToEndAlloc(testing.allocator, 1024 * 1024);
    defer testing.allocator.free(content);

    // Verify all parameters are present
    try testing.expect(std.mem.indexOf(u8, content, "maxmemory 8192") != null);
    try testing.expect(std.mem.indexOf(u8, content, "timeout 300") != null);
    try testing.expect(std.mem.indexOf(u8, content, "appendonly yes") != null);
}

test "CONFIG REWRITE - with no arguments" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "REWRITE" });
    defer testing.allocator.free(response);

    try testing.expectEqualStrings("+OK\r\n", response);

    // Cleanup
    std.fs.cwd().deleteFile("zoltraak.conf") catch {};
}

test "CONFIG REWRITE - extra arguments fail" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "REWRITE", "extra" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null);
}

// ── RESETSTAT Tests ─────────────────────────────────────────────────────────

test "CONFIG RESETSTAT - extra arguments fail" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "RESETSTAT", "extra" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null);
}

// ── HELP Tests ──────────────────────────────────────────────────────────────

test "CONFIG HELP - format validation" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "HELP" });
    defer testing.allocator.free(response);

    // Should return RESP array
    try testing.expect(std.mem.startsWith(u8, response, "*"));

    // Verify all subcommands are documented
    const expected_commands = [_][]const u8{ "GET", "SET", "REWRITE", "RESETSTAT" };
    for (expected_commands) |cmd| {
        try testing.expect(std.mem.indexOf(u8, response, cmd) != null);
    }
}

test "CONFIG HELP - extra arguments ignored" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // CONFIG HELP should ignore extra arguments (like Redis)
    const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "HELP", "extra", "args" });
    defer testing.allocator.free(response);

    try testing.expect(std.mem.startsWith(u8, response, "*"));
    try testing.expect(std.mem.indexOf(u8, response, "GET") != null);
}

// ── Case Sensitivity Tests ──────────────────────────────────────────────────

test "CONFIG GET - parameter name case insensitive" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Test various case combinations
    const variations = [_][]const u8{ "maxmemory", "MAXMEMORY", "MaxMemory", "mAxMeMoRy" };

    for (variations) |param_name| {
        const response = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET", param_name });
        defer testing.allocator.free(response);

        try testing.expect(std.mem.indexOf(u8, response, "maxmemory") != null);
    }
}

test "CONFIG SET - parameter name case insensitive" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Set using uppercase
    const set_resp = try client.sendCommand(&[_][]const u8{ "CONFIG", "SET", "MAXMEMORY", "4096" });
    defer testing.allocator.free(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Get using lowercase
    const get_resp = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET", "maxmemory" });
    defer testing.allocator.free(get_resp);
    try testing.expect(std.mem.indexOf(u8, get_resp, "4096") != null);
}

// ── Integration Tests ───────────────────────────────────────────────────────

test "CONFIG - end-to-end workflow" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // 1. Get current configuration
    const initial = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET", "*" });
    defer testing.allocator.free(initial);
    try testing.expect(std.mem.indexOf(u8, initial, "maxmemory") != null);

    // 2. Modify several parameters
    _ = try client.sendCommand(&[_][]const u8{ "CONFIG", "SET", "maxmemory", "1048576" });
    _ = try client.sendCommand(&[_][]const u8{ "CONFIG", "SET", "maxmemory-policy", "allkeys-lru" });
    _ = try client.sendCommand(&[_][]const u8{ "CONFIG", "SET", "timeout", "600" });

    // 3. Verify changes
    const maxmem = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET", "maxmemory" });
    defer testing.allocator.free(maxmem);
    try testing.expect(std.mem.indexOf(u8, maxmem, "1048576") != null);

    const policy = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET", "maxmemory-policy" });
    defer testing.allocator.free(policy);
    try testing.expect(std.mem.indexOf(u8, policy, "allkeys-lru") != null);

    // 4. Reset statistics (should not affect config)
    const reset = try client.sendCommand(&[_][]const u8{ "CONFIG", "RESETSTAT" });
    defer testing.allocator.free(reset);
    try testing.expectEqualStrings("+OK\r\n", reset);

    // 5. Verify config persists after RESETSTAT
    const verify = try client.sendCommand(&[_][]const u8{ "CONFIG", "GET", "maxmemory" });
    defer testing.allocator.free(verify);
    try testing.expect(std.mem.indexOf(u8, verify, "1048576") != null);

    // 6. Write to file
    const rewrite = try client.sendCommand(&[_][]const u8{ "CONFIG", "REWRITE" });
    defer testing.allocator.free(rewrite);
    try testing.expectEqualStrings("+OK\r\n", rewrite);

    // Cleanup
    std.fs.cwd().deleteFile("zoltraak.conf") catch {};
}
