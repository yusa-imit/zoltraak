const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const RespValue = protocol.RespValue;

/// HELLO command - Protocol negotiation for RESP2/RESP3
/// Syntax: HELLO [protover [AUTH username password] [SETNAME clientname]]
/// Returns: Map of server information (RESP2 array for now)
pub fn cmdHello(
    allocator: std.mem.Allocator,
    args: []const RespValue,
) ![]const u8 {
    // For simplicity, always return RESP2 array response
    // TODO: Track protocol version per-connection for full RESP3 support
    _ = args; // Parse args if needed in future

    // RESP2 response - use array
    var response_buf = std.ArrayList(u8){};
    errdefer response_buf.deinit(allocator);

    // Build array with server info
    // *14 (7 key-value pairs as alternating elements)
    try response_buf.appendSlice(allocator, "*14\r\n");

    try response_buf.appendSlice(allocator, "$6\r\nserver\r\n$8\r\nzoltraak\r\n");
    try response_buf.appendSlice(allocator, "$7\r\nversion\r\n$5\r\n0.1.0\r\n");
    try response_buf.appendSlice(allocator, "$5\r\nproto\r\n:2\r\n");
    try response_buf.appendSlice(allocator, "$2\r\nid\r\n:1\r\n");
    try response_buf.appendSlice(allocator, "$4\r\nmode\r\n$10\r\nstandalone\r\n");
    try response_buf.appendSlice(allocator, "$4\r\nrole\r\n$6\r\nmaster\r\n");
    try response_buf.appendSlice(allocator, "$7\r\nmodules\r\n*0\r\n");

    return response_buf.toOwnedSlice(allocator);
}

// Embedded unit tests

test "HELLO command - basic response" {
    const allocator = std.testing.allocator;

    const args = [_]RespValue{};

    const result = try cmdHello(allocator, &args);
    defer allocator.free(result);

    // Should return RESP2 array
    try std.testing.expect(std.mem.startsWith(u8, result, "*14\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "zoltraak") != null);
}

test "HELLO command - with version arg" {
    const allocator = std.testing.allocator;

    const args = [_]RespValue{
        RespValue{ .bulk_string = "2" },
    };

    const result = try cmdHello(allocator, &args);
    defer allocator.free(result);

    // Should still return RESP2 array (version tracking not yet implemented)
    try std.testing.expect(std.mem.startsWith(u8, result, "*14\r\n"));
}
