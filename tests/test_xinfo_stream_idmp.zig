const std = @import("std");
const testing = std.testing;
const TestServer = @import("test_integration.zig").TestServer;
const RespClient = @import("test_integration.zig").RespClient;

test "XINFO STREAM includes IDMP configuration fields" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create a stream
    {
        const response = try client.sendCommand(&[_][]const u8{ "XADD", "mystream", "*", "field1", "value1" });
        defer testing.allocator.free(response);
        // Verify it's created
        try testing.expect(response[0] == '$');
    }

    // Configure IDMP settings
    {
        const response = try client.sendCommand(&[_][]const u8{ "XCFGSET", "mystream", "IDMP-DURATION", "300", "IDMP-MAXSIZE", "1000" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("+OK\r\n", response);
    }

    // Get XINFO STREAM and verify IDMP fields are present
    {
        const response = try client.sendCommand(&[_][]const u8{ "XINFO", "STREAM", "mystream" });
        defer testing.allocator.free(response);

        // Response should contain idmp-duration and idmp-maxsize fields
        const contains_idmp_duration = std.mem.indexOf(u8, response, "idmp-duration") != null;
        const contains_idmp_maxsize = std.mem.indexOf(u8, response, "idmp-maxsize") != null;
        const contains_300 = std.mem.indexOf(u8, response, ":300\r\n") != null;
        const contains_1000 = std.mem.indexOf(u8, response, ":1000\r\n") != null;

        try testing.expect(contains_idmp_duration);
        try testing.expect(contains_idmp_maxsize);
        try testing.expect(contains_300);
        try testing.expect(contains_1000);
    }
}

test "XINFO STREAM FULL includes IDMP configuration fields" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create a stream with multiple entries
    {
        _ = try client.sendCommand(&[_][]const u8{ "XADD", "mystream", "*", "field1", "value1" });
        _ = try client.sendCommand(&[_][]const u8{ "XADD", "mystream", "*", "field2", "value2" });
    }

    // Configure IDMP with different values
    {
        const response = try client.sendCommand(&[_][]const u8{ "XCFGSET", "mystream", "IDMP-DURATION", "600" });
        defer testing.allocator.free(response);
        try testing.expectEqualStrings("+OK\r\n", response);
    }

    // Get XINFO STREAM FULL and verify IDMP fields
    {
        const response = try client.sendCommand(&[_][]const u8{ "XINFO", "STREAM", "mystream", "FULL" });
        defer testing.allocator.free(response);

        const contains_idmp_duration = std.mem.indexOf(u8, response, "idmp-duration") != null;
        const contains_idmp_maxsize = std.mem.indexOf(u8, response, "idmp-maxsize") != null;
        const contains_600 = std.mem.indexOf(u8, response, ":600\r\n") != null;
        // Default maxsize is 100
        const contains_100 = std.mem.indexOf(u8, response, ":100\r\n") != null;

        try testing.expect(contains_idmp_duration);
        try testing.expect(contains_idmp_maxsize);
        try testing.expect(contains_600);
        try testing.expect(contains_100);
    }
}

test "XINFO STREAM shows default IDMP values" {
    var server = try TestServer.start(testing.allocator);
    defer server.stop();

    var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
    defer client.deinit();

    // Create a stream without configuring IDMP
    {
        const response = try client.sendCommand(&[_][]const u8{ "XADD", "teststream", "*", "data", "123" });
        defer testing.allocator.free(response);
    }

    // Check XINFO STREAM shows default values (100, 100)
    {
        const response = try client.sendCommand(&[_][]const u8{ "XINFO", "STREAM", "teststream" });
        defer testing.allocator.free(response);

        const contains_idmp_duration = std.mem.indexOf(u8, response, "idmp-duration") != null;
        const contains_idmp_maxsize = std.mem.indexOf(u8, response, "idmp-maxsize") != null;
        // Default values are both 100
        const count_100 = blk: {
            var count: usize = 0;
            var search_start: usize = 0;
            while (std.mem.indexOfPos(u8, response, search_start, ":100\r\n")) |pos| {
                count += 1;
                search_start = pos + 6; // Move past ":100\r\n"
            }
            break :blk count;
        };

        try testing.expect(contains_idmp_duration);
        try testing.expect(contains_idmp_maxsize);
        try testing.expect(count_100 >= 2); // At least 2 occurrences of :100 (duration and maxsize)
    }
}
