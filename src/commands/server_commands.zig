const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const client_mod = @import("client.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const ClientRegistry = client_mod.ClientRegistry;
const RespProtocol = client_mod.RespProtocol;

/// HELLO command - Protocol negotiation for RESP2/RESP3
/// Syntax: HELLO [protover [AUTH username password] [SETNAME clientname]]
/// Returns: Map of server information (RESP3 map if proto=3, RESP2 array otherwise)
pub fn cmdHello(
    allocator: std.mem.Allocator,
    client_registry: *ClientRegistry,
    client_id: u64,
    args: []const RespValue,
) ![]const u8 {
    // Parse protocol version from args[0] if provided
    var protocol_version: u8 = 2; // Default to RESP2
    var client_name: ?[]const u8 = null;

    if (args.len > 0) {
        const proto_str = switch (args[0]) {
            .bulk_string => |s| s,
            else => {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR Protocol version is not an integer or out of range");
            },
        };

        protocol_version = std.fmt.parseInt(u8, proto_str, 10) catch {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR Protocol version is not an integer or out of range");
        };

        // Validate protocol version
        if (protocol_version != 2 and protocol_version != 3) {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("NOPROTO unsupported protocol version");
        }
    }

    // Parse optional SETNAME argument
    var i: usize = 1;
    while (i < args.len) {
        const option = switch (args[i]) {
            .bulk_string => |s| s,
            else => {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            },
        };

        if (std.ascii.eqlIgnoreCase(option, "SETNAME")) {
            if (i + 1 >= args.len) {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            }

            client_name = switch (args[i + 1]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    return w.writeError("ERR syntax error");
                },
            };

            i += 2;
        } else if (std.ascii.eqlIgnoreCase(option, "AUTH")) {
            // AUTH not implemented yet, skip for now
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR AUTH not supported");
        } else {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR syntax error");
        }
    }

    // Update protocol version in client registry
    const resp_protocol: RespProtocol = if (protocol_version == 3) .RESP3 else .RESP2;
    client_registry.setProtocol(client_id, resp_protocol);

    // Update client name if provided
    if (client_name) |name| {
        try client_registry.setClientName(client_id, name);
    }

    // Build response based on negotiated protocol
    if (protocol_version == 3) {
        // RESP3: Use map type
        var response_buf = std.ArrayList(u8){};
        errdefer response_buf.deinit(allocator);

        // %7 (7 key-value pairs)
        try response_buf.appendSlice(allocator, "%7\r\n");

        try response_buf.appendSlice(allocator, "+server\r\n+zoltraak\r\n");
        try response_buf.appendSlice(allocator, "+version\r\n+0.1.0\r\n");
        try response_buf.appendSlice(allocator, "+proto\r\n:3\r\n");

        // Include actual client ID
        var id_buf = std.ArrayList(u8){};
        try id_buf.writer(allocator).print("+id\r\n:{d}\r\n", .{client_id});
        try response_buf.appendSlice(allocator, try id_buf.toOwnedSlice(allocator));

        try response_buf.appendSlice(allocator, "+mode\r\n+standalone\r\n");
        try response_buf.appendSlice(allocator, "+role\r\n+master\r\n");
        try response_buf.appendSlice(allocator, "+modules\r\n*0\r\n");

        return response_buf.toOwnedSlice(allocator);
    } else {
        // RESP2: Use array (alternating key-value elements)
        var response_buf = std.ArrayList(u8){};
        errdefer response_buf.deinit(allocator);

        // *14 (7 key-value pairs as alternating elements)
        try response_buf.appendSlice(allocator, "*14\r\n");

        try response_buf.appendSlice(allocator, "$6\r\nserver\r\n$8\r\nzoltraak\r\n");
        try response_buf.appendSlice(allocator, "$7\r\nversion\r\n$5\r\n0.1.0\r\n");
        try response_buf.appendSlice(allocator, "$5\r\nproto\r\n:2\r\n");

        // Include actual client ID
        var id_buf = std.ArrayList(u8){};
        try id_buf.writer(allocator).print("$2\r\nid\r\n:{d}\r\n", .{client_id});
        try response_buf.appendSlice(allocator, try id_buf.toOwnedSlice(allocator));

        try response_buf.appendSlice(allocator, "$4\r\nmode\r\n$10\r\nstandalone\r\n");
        try response_buf.appendSlice(allocator, "$4\r\nrole\r\n$6\r\nmaster\r\n");
        try response_buf.appendSlice(allocator, "$7\r\nmodules\r\n*0\r\n");

        return response_buf.toOwnedSlice(allocator);
    }
}

// Embedded unit tests

test "HELLO command - default RESP2" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    const args = [_]RespValue{};

    const result = try cmdHello(allocator, &registry, client_id, &args);
    defer allocator.free(result);

    // Should return RESP2 array
    try std.testing.expect(std.mem.startsWith(u8, result, "*14\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "zoltraak") != null);

    // Verify protocol is RESP2
    try std.testing.expectEqual(RespProtocol.RESP2, registry.getProtocol(client_id));
}

test "HELLO command - negotiate RESP2" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "2" },
    };

    const result = try cmdHello(allocator, &registry, client_id, &args);
    defer allocator.free(result);

    // Should return RESP2 array
    try std.testing.expect(std.mem.startsWith(u8, result, "*14\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "$5\r\nproto\r\n:2\r\n") != null);

    // Verify protocol is set to RESP2
    try std.testing.expectEqual(RespProtocol.RESP2, registry.getProtocol(client_id));
}

test "HELLO command - negotiate RESP3" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "3" },
    };

    const result = try cmdHello(allocator, &registry, client_id, &args);
    defer allocator.free(result);

    // Should return RESP3 map
    try std.testing.expect(std.mem.startsWith(u8, result, "%7\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "+proto\r\n:3\r\n") != null);

    // Verify protocol is set to RESP3
    try std.testing.expectEqual(RespProtocol.RESP3, registry.getProtocol(client_id));
}

test "HELLO command - with SETNAME" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "SETNAME" },
        RespValue{ .bulk_string = "my-client" },
    };

    const result = try cmdHello(allocator, &registry, client_id, &args);
    defer allocator.free(result);

    // Should succeed
    try std.testing.expect(std.mem.startsWith(u8, result, "*14\r\n"));

    // Verify client name was set
    const name = try registry.getClientName(client_id, allocator);
    try std.testing.expect(name != null);
    defer allocator.free(name.?);
    try std.testing.expectEqualStrings("my-client", name.?);
}

test "HELLO command - invalid protocol version" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "5" },
    };

    const result = try cmdHello(allocator, &registry, client_id, &args);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.startsWith(u8, result, "-NOPROTO"));
}
