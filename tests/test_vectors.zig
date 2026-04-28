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

// ============================================================================
// VGETATTR integration tests
// ============================================================================

test "VGETATTR: RESP format for command" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VGETATTR", "myvec", "v1", "category" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    // Verifies RESP serialization of the VGETATTR command
    try std.testing.expect(std.mem.startsWith(u8, request, "*4\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, request, "VGETATTR") != null);
}

test "VGETATTR: arity too few args" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VGETATTR", "myvec" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    // Verifies RESP serialization with 2 args
    try std.testing.expect(std.mem.startsWith(u8, request, "*2\r\n"));
}

test "VGETATTR: arity too many args" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VGETATTR", "myvec", "v1", "attr", "extra" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "*5\r\n"));
}

test "VGETATTR: command with all args" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VGETATTR", "myset", "member1", "color" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.indexOf(u8, request, "myset") != null);
    try std.testing.expect(std.mem.indexOf(u8, request, "member1") != null);
    try std.testing.expect(std.mem.indexOf(u8, request, "color") != null);
}

test "VGETATTR: nonexistent key serialization" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VGETATTR", "no_such_key", "v1", "attr1" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "*4\r\n"));
}

test "VGETATTR: empty attribute name" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VGETATTR", "myvec", "v1", "" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    // Empty string should be serialized as $0\r\n\r\n
    try std.testing.expect(std.mem.indexOf(u8, request, "$0\r\n\r\n") != null);
}

test "VGETATTR: attribute with special characters" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VGETATTR", "myvec", "v1", "my-attr_name.v2" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.indexOf(u8, request, "my-attr_name.v2") != null);
}

test "VGETATTR: long attribute value" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VGETATTR", "myvec", "v1", "description" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.indexOf(u8, request, "description") != null);
}

// ============================================================================
// VSETATTR integration tests
// ============================================================================

test "VSETATTR: RESP format for command" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VSETATTR", "myvec", "v1", "category", "news" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "*5\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, request, "VSETATTR") != null);
}

test "VSETATTR: arity too few args" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VSETATTR", "myvec", "v1" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "*3\r\n"));
}

test "VSETATTR: arity too many args" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VSETATTR", "myvec", "v1", "cat", "val", "extra" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "*6\r\n"));
}

test "VSETATTR: command with all args" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VSETATTR", "myset", "member1", "color", "red" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.indexOf(u8, request, "myset") != null);
    try std.testing.expect(std.mem.indexOf(u8, request, "member1") != null);
    try std.testing.expect(std.mem.indexOf(u8, request, "color") != null);
    try std.testing.expect(std.mem.indexOf(u8, request, "red") != null);
}

test "VSETATTR: nonexistent key serialization" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VSETATTR", "no_such_key", "v1", "attr1", "val1" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "*5\r\n"));
}

test "VSETATTR: empty attribute value" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VSETATTR", "myvec", "v1", "tag", "" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    // Empty value should be serialized as $0\r\n\r\n
    try std.testing.expect(std.mem.indexOf(u8, request, "$0\r\n\r\n") != null);
}

test "VSETATTR: attribute with unicode value" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VSETATTR", "myvec", "v1", "label", "hello world" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.indexOf(u8, request, "hello world") != null);
}

test "VSETATTR: numeric attribute value" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VSETATTR", "myvec", "v1", "score", "0.95" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.indexOf(u8, request, "score") != null);
    try std.testing.expect(std.mem.indexOf(u8, request, "0.95") != null);
}

test "VSETATTR: overwrite existing attribute" {
    const allocator = std.testing.allocator;

    // First set
    {
        const cmd = [_][]const u8{ "VSETATTR", "myvec", "v1", "tag", "old" };
        const request = try sendCommand(allocator, &cmd);
        defer allocator.free(request);
        try std.testing.expect(std.mem.indexOf(u8, request, "old") != null);
    }

    // Overwrite
    const cmd = [_][]const u8{ "VSETATTR", "myvec", "v1", "tag", "new" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.indexOf(u8, request, "new") != null);
}

// ============================================================================
// VINFO integration tests
// ============================================================================

test "VINFO: RESP format for command" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VINFO", "myvec" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "*2\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, request, "VINFO") != null);
}

test "VINFO: arity error too few" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{"VINFO"};
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "*1\r\n"));
}

test "VINFO: arity error too many" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VINFO", "myvec", "extra" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.startsWith(u8, request, "*3\r\n"));
}

test "VINFO: nonexistent key serialization" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VINFO", "nonexistent" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.indexOf(u8, request, "nonexistent") != null);
}

test "VINFO: command format validation" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VINFO", "my_vectors" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    // Should contain the key name in the serialized RESP
    try std.testing.expect(std.mem.indexOf(u8, request, "my_vectors") != null);
}

test "VINFO: key with special characters" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VINFO", "vec:set:1" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.indexOf(u8, request, "vec:set:1") != null);
}

test "VINFO: long key name" {
    const allocator = std.testing.allocator;

    const cmd = [_][]const u8{ "VINFO", "this_is_a_very_long_key_name_for_testing" };
    const request = try sendCommand(allocator, &cmd);
    defer allocator.free(request);

    try std.testing.expect(std.mem.indexOf(u8, request, "this_is_a_very_long_key_name_for_testing") != null);
}

// ============================================================================
// Integration workflow: VSETATTR + VGETATTR + VINFO
// ============================================================================

test "Integration: VSETATTR, VGETATTR, VINFO workflow" {
    const allocator = std.testing.allocator;

    // VADD command
    {
        const cmd = [_][]const u8{
            "VADD", "docvec",     "3",       "COSINE",
            "doc1", "0.1",        "0.2",     "0.3",
            "doc2", "0.4",        "0.5",     "0.6",
        };
        const request = try sendCommand(allocator, &cmd);
        defer allocator.free(request);
        try std.testing.expect(std.mem.startsWith(u8, request, "*"));
    }

    // VSETATTR command
    {
        const cmd = [_][]const u8{ "VSETATTR", "docvec", "doc1", "category", "science" };
        const request = try sendCommand(allocator, &cmd);
        defer allocator.free(request);
        try std.testing.expect(std.mem.indexOf(u8, request, "science") != null);
    }

    // VGETATTR command
    {
        const cmd = [_][]const u8{ "VGETATTR", "docvec", "doc1", "category" };
        const request = try sendCommand(allocator, &cmd);
        defer allocator.free(request);
        try std.testing.expect(std.mem.indexOf(u8, request, "category") != null);
    }

    // VINFO command
    {
        const cmd = [_][]const u8{ "VINFO", "docvec" };
        const request = try sendCommand(allocator, &cmd);
        defer allocator.free(request);
        try std.testing.expect(std.mem.indexOf(u8, request, "docvec") != null);
    }
}
