const std = @import("std");
const net = std.net;
const testing = std.testing;

// Helper to connect to server
fn connectToServer() !net.Stream {
    const address = try net.Address.parseIp("127.0.0.1", 6379);
    return try net.tcpConnectToAddress(address);
}

// Helper to send command and read response
fn sendCommand(stream: net.Stream, command: []const u8) ![]u8 {
    _ = try stream.write(command);

    // Read response
    var buf: [16384]u8 = undefined;
    const n = try stream.read(&buf);

    const allocator = testing.allocator;
    return try allocator.dupe(u8, buf[0..n]);
}

// Helper to free response
fn freeResponse(response: []u8) void {
    testing.allocator.free(response);
}

test "JSON.SET - create key with root path" {
    const stream = try connectToServer();
    defer stream.close();

    // SET a JSON value at root
    const cmd = "*4\r\n$8\r\nJSON.SET\r\n$3\r\ndoc\r\n$1\r\n$\r\n$15\r\n{\"a\":1,\"b\":2}\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings("+OK\r\n", response);
}

test "JSON.SET - with NX flag (key doesn't exist)" {
    const stream = try connectToServer();
    defer stream.close();

    // SET with NX on non-existent key
    const cmd = "*5\r\n$8\r\nJSON.SET\r\n$4\r\ndoc1\r\n$1\r\n$\r\n$7\r\n{\"x\":1}\r\n$2\r\nNX\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings("+OK\r\n", response);
}

test "JSON.SET - with NX flag (key exists)" {
    const stream = try connectToServer();
    defer stream.close();

    // First SET
    _ = try sendCommand(stream, "*4\r\n$8\r\nJSON.SET\r\n$4\r\ndoc2\r\n$1\r\n$\r\n$7\r\n{\"x\":1}\r\n");

    // SET with NX on existing key (should return nil)
    const cmd = "*5\r\n$8\r\nJSON.SET\r\n$4\r\ndoc2\r\n$1\r\n$\r\n$7\r\n{\"y\":2}\r\n$2\r\nNX\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings("$-1\r\n", response);
}

test "JSON.SET - with XX flag (key exists)" {
    const stream = try connectToServer();
    defer stream.close();

    // First SET
    _ = try sendCommand(stream, "*4\r\n$8\r\nJSON.SET\r\n$4\r\ndoc3\r\n$1\r\n$\r\n$7\r\n{\"x\":1}\r\n");

    // SET with XX on existing key
    const cmd = "*5\r\n$8\r\nJSON.SET\r\n$4\r\ndoc3\r\n$1\r\n$\r\n$7\r\n{\"y\":2}\r\n$2\r\nXX\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings("+OK\r\n", response);
}

test "JSON.SET - with XX flag (key doesn't exist)" {
    const stream = try connectToServer();
    defer stream.close();

    // SET with XX on non-existent key (should return nil)
    const cmd = "*5\r\n$8\r\nJSON.SET\r\n$4\r\ndoc4\r\n$1\r\n$\r\n$7\r\n{\"x\":1}\r\n$2\r\nXX\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings("$-1\r\n", response);
}

test "JSON.GET - retrieve entire document" {
    const stream = try connectToServer();
    defer stream.close();

    // SET a JSON value
    _ = try sendCommand(stream, "*4\r\n$8\r\nJSON.SET\r\n$4\r\ndoc5\r\n$1\r\n$\r\n$15\r\n{\"a\":1,\"b\":2}\r\n");

    // GET the entire document
    const cmd = "*3\r\n$8\r\nJSON.GET\r\n$4\r\ndoc5\r\n$1\r\n$\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return bulk string with JSON
    try testing.expect(std.mem.startsWith(u8, response, "$"));
    try testing.expect(std.mem.indexOf(u8, response, "{\"a\":1,\"b\":2}") != null);
}

test "JSON.GET - on non-existent key" {
    const stream = try connectToServer();
    defer stream.close();

    const cmd = "*3\r\n$8\r\nJSON.GET\r\n$11\r\nnonexistent\r\n$1\r\n$\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings("$-1\r\n", response);
}

test "JSON.DEL - delete entire key" {
    const stream = try connectToServer();
    defer stream.close();

    // SET a JSON value
    _ = try sendCommand(stream, "*4\r\n$8\r\nJSON.SET\r\n$4\r\ndoc6\r\n$1\r\n$\r\n$15\r\n{\"a\":1,\"b\":2}\r\n");

    // DELETE the key
    const cmd = "*3\r\n$8\r\nJSON.DEL\r\n$4\r\ndoc6\r\n$1\r\n$\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 1 (number of deletions)
    try testing.expectEqualStrings(":1\r\n", response);
}

test "JSON.DEL - on non-existent key" {
    const stream = try connectToServer();
    defer stream.close();

    const cmd = "*3\r\n$8\r\nJSON.DEL\r\n$11\r\nnonexistent\r\n$1\r\n$\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return 0 (no deletions)
    try testing.expectEqualStrings(":0\r\n", response);
}

test "JSON.TYPE - returns correct type for object" {
    const stream = try connectToServer();
    defer stream.close();

    // SET a JSON object
    _ = try sendCommand(stream, "*4\r\n$8\r\nJSON.SET\r\n$4\r\ndoc7\r\n$1\r\n$\r\n$15\r\n{\"a\":1,\"b\":2}\r\n");

    // GET TYPE
    const cmd = "*3\r\n$9\r\nJSON.TYPE\r\n$4\r\ndoc7\r\n$1\r\n$\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings("$6\r\nobject\r\n", response);
}

test "JSON.TYPE - returns correct type for number" {
    const stream = try connectToServer();
    defer stream.close();

    // SET a JSON number
    _ = try sendCommand(stream, "*4\r\n$8\r\nJSON.SET\r\n$4\r\ndoc8\r\n$1\r\n$\r\n$2\r\n42\r\n");

    // GET TYPE
    const cmd = "*3\r\n$9\r\nJSON.TYPE\r\n$4\r\ndoc8\r\n$1\r\n$\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings("$6\r\nnumber\r\n", response);
}

test "JSON.TYPE - on non-existent key" {
    const stream = try connectToServer();
    defer stream.close();

    const cmd = "*3\r\n$9\r\nJSON.TYPE\r\n$11\r\nnonexistent\r\n$1\r\n$\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings("$-1\r\n", response);
}

test "JSON.SET - wrong arity" {
    const stream = try connectToServer();
    defer stream.close();

    const cmd = "*2\r\n$8\r\nJSON.SET\r\n$3\r\ndoc\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.GET - wrong arity" {
    const stream = try connectToServer();
    defer stream.close();

    const cmd = "*1\r\n$8\r\nJSON.GET\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.DEL - wrong arity" {
    const stream = try connectToServer();
    defer stream.close();

    const cmd = "*1\r\n$8\r\nJSON.DEL\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}
