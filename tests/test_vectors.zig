const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const writer_mod = @import("../src/protocol/writer.zig");
const server_mod = @import("../src/server.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;

// Integration tests for vector commands
// These test the full RESP protocol request-response cycle

fn sendCommand(allocator: std.mem.Allocator, cmd: []const []const u8) ![]u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    var array = try allocator.alloc(RespValue, cmd.len);
    defer allocator.free(array);

    for (cmd, 0..) |arg, i| {
        array[i] = RespValue{ .bulk_string = arg };
    }

    const request = RespValue{ .array = array };
    return try w.writeRespValue(request);
}

test "VADD: basic add single vector" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VADD", "myvec", "3", "L2", "vec1", "1.0", "2.0", "3.0" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    // Expected response: :1\r\n (1 vector added)
    try std.testing.expectEqualStrings(":1\r\n", request);
}

test "VADD: add multiple vectors" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{
        "VADD", "myvec",      "2",       "COSINE",
        "v1",   "1.0",        "2.0",
        "v2",   "3.0",        "4.0",
        "v3",   "5.0",        "6.0",
    };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    // Expected: :3\r\n (3 vectors added)
    try std.testing.expectEqualStrings(":3\r\n", request);
}

test "VADD: arity error" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VADD", "myvec" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    // Expected: error
    try std.testing.expect(std.mem.startsWith(u8, request, "-ERR"));
}

test "VADD: invalid dimensionality" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VADD", "myvec", "0", "L2", "v1" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "-ERR"));
}

test "VADD: invalid metric" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VADD", "myvec", "2", "INVALID", "v1", "1.0", "2.0" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "-ERR"));
}

test "VADD: incomplete vector data" {
    const allocator = std.testing.allocator;

    // Dimensionality is 3, but only 2 values provided
    const cmd = [_][]const u8{ "VADD", "myvec", "3", "L2", "v1", "1.0", "2.0" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "-ERR"));
}

test "VADD: invalid float value" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "not_a_float", "2.0" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "-ERR"));
}

test "VCARD: basic cardinality" {
    const allocator = std.testing.allocator;

    // First add some vectors
    {
        const cmd = [_][]const u8{
            "VADD", "myvec",      "2",    "L2",
            "v1",   "1.0",        "2.0",
            "v2",   "3.0",        "4.0",
        };
        const request = try sendCommand(allocator, &cmd);
        defer allocator.free(request);
    }

    // Get cardinality
    const cmd = [_][]const u8{ "VCARD", "myvec" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expectEqualStrings(":2\r\n", request);
}

test "VCARD: nonexistent key returns 0" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VCARD", "nonexistent" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expectEqualStrings(":0\r\n", request);
}

test "VCARD: arity error" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{"VCARD"};
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "-ERR"));
}

test "VDIM: basic dimensionality" {
    const allocator = std.testing.allocator;

    // Add vector set with dim=128
    {
        var args = std.ArrayList([]const u8).init(allocator);
        defer args.deinit();

        try args.append("VADD");
        try args.append("myvec");
        try args.append("128");
        try args.append("COSINE");
        try args.append("v1");

        // Add 128 float values
        var i: usize = 0;
        while (i < 128) : (i += 1) {
            try args.append("1.0");
        }

        const request = try sendCommand(allocator, args.items);
        defer allocator.free(request);
    }

    // Get dimensionality
    const cmd = [_][]const u8{ "VDIM", "myvec" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expectEqualStrings(":128\r\n", request);
}

test "VDIM: nonexistent key returns error" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VDIM", "nonexistent" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "-ERR"));
}

test "VDIM: arity error" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{"VDIM"};
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "-ERR"));
}

test "VEMB: basic embedding retrieval" {
    const allocator = std.testing.allocator;

    // Add vector
    {
        const cmd = [_][]const u8{ "VADD", "myvec", "3", "L2", "vec1", "1.5", "2.5", "3.5" };
        const request = try sendCommand(allocator, &cmd);
        defer allocator.free(request);
    }

    // Get embedding
    const cmd = [_][]const u8{ "VEMB", "myvec", "vec1" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    // Expected: array of 3 bulk strings
    // Format: *3\r\n$3\r\n1.5\r\n$3\r\n2.5\r\n$3\r\n3.5\r\n
    try std.testing.expect(std.mem.startsWith(u8, request, "*3\r\n"));
}

test "VEMB: nonexistent vector returns null" {
    const allocator = std.testing.allocator;

    // Add vector set
    {
        const cmd = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
        const request = try sendCommand(allocator, &cmd);
        defer allocator.free(request);
    }

    // Get nonexistent vector
    const cmd = [_][]const u8{ "VEMB", "myvec", "nonexistent" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expectEqualStrings("$-1\r\n", request);
}

test "VEMB: nonexistent key returns null" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VEMB", "nonexistent", "v1" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expectEqualStrings("$-1\r\n", request);
}

test "VEMB: arity error" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VEMB", "myvec" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "-ERR"));
}

test "VADD: update existing vector returns 0" {
    const allocator = std.testing.allocator;

    // Add vector first time
    {
        const cmd = [_][]const u8{ "VADD", "myvec", "2", "IP", "v1", "1.0", "2.0" };
        const request = try sendCommand(allocator, &cmd);
        defer allocator.free(request);
        try std.testing.expectEqualStrings(":1\r\n", request);
    }

    // Add same vector again (update)
    const cmd = [_][]const u8{ "VADD", "myvec", "2", "IP", "v1", "3.0", "4.0" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expectEqualStrings(":0\r\n", request);
}

test "VADD: dimension mismatch error" {
    const allocator = std.testing.allocator;

    // Create vector set with dim=3
    {
        const cmd = [_][]const u8{ "VADD", "myvec", "3", "L2", "v1", "1.0", "2.0", "3.0" };
        const request = try sendCommand(allocator, &cmd);
        defer allocator.free(request);
    }

    // Try to add with dim=2
    const cmd = [_][]const u8{ "VADD", "myvec", "2", "L2", "v2", "4.0", "5.0" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, request, "mismatch") != null);
}

test "VADD: metric mismatch error" {
    const allocator = std.testing.allocator;

    // Create with L2
    {
        const cmd = [_][]const u8{ "VADD", "myvec", "2", "L2", "v1", "1.0", "2.0" };
        const request = try sendCommand(allocator, &cmd);
        defer allocator.free(request);
    }

    // Try with COSINE
    const cmd = [_][]const u8{ "VADD", "myvec", "2", "COSINE", "v2", "3.0", "4.0" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, request, "metric") != null);
}

test "Integration: VADD, VCARD, VDIM, VEMB workflow" {
    const allocator = std.testing.allocator;

    // Add multiple vectors
    {
        const cmd = [_][]const u8{
            "VADD", "testvec",    "4",       "COSINE",
            "doc1", "0.1",        "0.2",     "0.3",     "0.4",
            "doc2", "0.5",        "0.6",     "0.7",     "0.8",
            "doc3", "0.9",        "1.0",     "1.1",     "1.2",
        };
        const request = try sendCommand(allocator, &cmd);
        defer allocator.free(request);
        try std.testing.expectEqualStrings(":3\r\n", request);
    }

    // Check cardinality
    {
        const cmd = [_][]const u8{ "VCARD", "testvec" };
        const request = try sendCommand(allocator, &cmd);
        defer allocator.free(request);
        try std.testing.expectEqualStrings(":3\r\n", request);
    }

    // Check dimensionality
    {
        const cmd = [_][]const u8{ "VDIM", "testvec" };
        const request = try sendCommand(allocator, &cmd);
        defer allocator.free(request);
        try std.testing.expectEqualStrings(":4\r\n", request);
    }

    // Get embedding of doc2
    {
        const cmd = [_][]const u8{ "VEMB", "testvec", "doc2" };
        const request = try sendCommand(allocator, &cmd);
        defer allocator.free(request);
        try std.testing.expect(std.mem.startsWith(u8, request, "*4\r\n"));
    }
}
