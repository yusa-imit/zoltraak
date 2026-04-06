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

test "JSON.MGET - retrieve from multiple keys" {
    const stream = try connectToServer();
    defer stream.close();

    // SET two JSON values
    _ = try sendCommand(stream, "*4\r\n$8\r\nJSON.SET\r\n$5\r\nmget1\r\n$1\r\n$\r\n$7\r\n{\"a\":1}\r\n");
    _ = try sendCommand(stream, "*4\r\n$8\r\nJSON.SET\r\n$5\r\nmget2\r\n$1\r\n$\r\n$7\r\n{\"a\":2}\r\n");

    // MGET both keys at path $.a
    const cmd = "*4\r\n$9\r\nJSON.MGET\r\n$5\r\nmget1\r\n$5\r\nmget2\r\n$3\r\n$.a\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with two values
    try testing.expect(std.mem.startsWith(u8, response, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, response, "$1\r\n1\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, "$1\r\n2\r\n") != null);
}

test "JSON.MGET - with non-existent key returns nil" {
    const stream = try connectToServer();
    defer stream.close();

    // SET one JSON value
    _ = try sendCommand(stream, "*4\r\n$8\r\nJSON.SET\r\n$5\r\nmget3\r\n$1\r\n$\r\n$7\r\n{\"a\":1}\r\n");

    // MGET with one existing and one non-existent key
    const cmd = "*4\r\n$9\r\nJSON.MGET\r\n$5\r\nmget3\r\n$11\r\nnonexistent\r\n$3\r\n$.a\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return array with one value and one nil
    try testing.expect(std.mem.startsWith(u8, response, "*2\r\n"));
    try testing.expect(std.mem.indexOf(u8, response, "$1\r\n1\r\n") != null);
    try testing.expect(std.mem.indexOf(u8, response, "$-1\r\n") != null);
}

test "JSON.MGET - wrong arity" {
    const stream = try connectToServer();
    defer stream.close();

    const cmd = "*2\r\n$9\r\nJSON.MGET\r\n$5\r\nmget4\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.NUMINCRBY - increment numeric value" {
    const stream = try connectToServer();
    defer stream.close();

    // SET a JSON document with a number
    _ = try sendCommand(stream, "*4\r\n$8\r\nJSON.SET\r\n$5\r\nincr1\r\n$1\r\n$\r\n$12\r\n{\"count\":10}\r\n");

    // Increment the count by 5
    const cmd = "*4\r\n$14\r\nJSON.NUMINCRBY\r\n$5\r\nincr1\r\n$7\r\n$.count\r\n$1\r\n5\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return bulk string with the new value "15"
    try testing.expect(std.mem.indexOf(u8, response, "15") != null);
}

test "JSON.NUMINCRBY - with negative increment" {
    const stream = try connectToServer();
    defer stream.close();

    // SET a JSON document
    _ = try sendCommand(stream, "*4\r\n$8\r\nJSON.SET\r\n$5\r\nincr2\r\n$1\r\n$\r\n$12\r\n{\"count\":10}\r\n");

    // Decrement the count by 3
    const cmd = "*4\r\n$14\r\nJSON.NUMINCRBY\r\n$5\r\nincr2\r\n$7\r\n$.count\r\n$2\r\n-3\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    // Should return bulk string with the new value "7"
    try testing.expect(std.mem.indexOf(u8, response, "7") != null);
}

test "JSON.NUMINCRBY - on non-numeric value returns error" {
    const stream = try connectToServer();
    defer stream.close();

    // SET a JSON document with a string
    _ = try sendCommand(stream, "*4\r\n$8\r\nJSON.SET\r\n$5\r\nincr3\r\n$1\r\n$\r\n$14\r\n{\"name\":\"test\"}\r\n");

    // Try to increment a string
    const cmd = "*4\r\n$14\r\nJSON.NUMINCRBY\r\n$5\r\nincr3\r\n$6\r\n$.name\r\n$1\r\n5\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, response, "not a number") != null);
}

test "JSON.NUMINCRBY - on non-existent key returns error" {
    const stream = try connectToServer();
    defer stream.close();

    const cmd = "*4\r\n$14\r\nJSON.NUMINCRBY\r\n$11\r\nnonexistent\r\n$7\r\n$.count\r\n$1\r\n5\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.NUMINCRBY - wrong arity" {
    const stream = try connectToServer();
    defer stream.close();

    const cmd = "*3\r\n$14\r\nJSON.NUMINCRBY\r\n$5\r\nincr4\r\n$7\r\n$.count\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.NUMMULTBY - multiply numeric value" {
    const stream = try connectToServer();
    defer stream.close();

    // Set initial document
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nmult1\r\n$1\r\n$\r\n$11\r\n{\"count\":5}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Multiply by 3
    const cmd = "*4\r\n$14\r\nJSON.NUMMULTBY\r\n$5\r\nmult1\r\n$7\r\n$.count\r\n$1\r\n3\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings("$2\r\n15\r\n", response);
}

test "JSON.NUMMULTBY - multiply by negative number" {
    const stream = try connectToServer();
    defer stream.close();

    // Set initial document
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nmult2\r\n$1\r\n$\r\n$12\r\n{\"count\":10}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Multiply by -2
    const cmd = "*4\r\n$14\r\nJSON.NUMMULTBY\r\n$5\r\nmult2\r\n$7\r\n$.count\r\n$2\r\n-2\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings("$3\r\n-20\r\n", response);
}

test "JSON.NUMMULTBY - on non-numeric value" {
    const stream = try connectToServer();
    defer stream.close();

    // Set document with string
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nmult3\r\n$1\r\n$\r\n$13\r\n{\"name\":\"x\"}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);

    const cmd = "*4\r\n$14\r\nJSON.NUMMULTBY\r\n$5\r\nmult3\r\n$6\r\n$.name\r\n$1\r\n3\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.NUMMULTBY - wrong arity" {
    const stream = try connectToServer();
    defer stream.close();

    const cmd = "*3\r\n$14\r\nJSON.NUMMULTBY\r\n$5\r\nmult4\r\n$7\r\n$.count\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.MSET - set multiple keys atomically" {
    const stream = try connectToServer();
    defer stream.close();

    // MSET three documents
    const cmd = "*10\r\n$9\r\nJSON.MSET\r\n$5\r\nmset1\r\n$1\r\n$\r\n$7\r\n{\"a\":1}\r\n$5\r\nmset2\r\n$1\r\n$\r\n$7\r\n{\"b\":2}\r\n$5\r\nmset3\r\n$1\r\n$\r\n$7\r\n{\"c\":3}\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings("+OK\r\n", response);

    // Verify first document
    const get_cmd1 = "*3\r\n$8\r\nJSON.GET\r\n$5\r\nmset1\r\n$3\r\n$.a\r\n";
    const get_resp1 = try sendCommand(stream, get_cmd1);
    defer freeResponse(get_resp1);
    try testing.expectEqualStrings("$1\r\n1\r\n", get_resp1);

    // Verify second document
    const get_cmd2 = "*3\r\n$8\r\nJSON.GET\r\n$5\r\nmset2\r\n$3\r\n$.b\r\n";
    const get_resp2 = try sendCommand(stream, get_cmd2);
    defer freeResponse(get_resp2);
    try testing.expectEqualStrings("$1\r\n2\r\n", get_resp2);
}

test "JSON.MSET - wrong arity (not triplets)" {
    const stream = try connectToServer();
    defer stream.close();

    // Only 2 args after command (need triplets)
    const cmd = "*3\r\n$9\r\nJSON.MSET\r\n$5\r\nmset4\r\n$1\r\n$\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.FORGET - alias for JSON.DEL" {
    const stream = try connectToServer();
    defer stream.close();

    // Set document
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$4\r\nfgt1\r\n$1\r\n$\r\n$7\r\n{\"a\":1}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);

    // Use FORGET to delete
    const cmd = "*2\r\n$11\r\nJSON.FORGET\r\n$4\r\nfgt1\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings(":1\r\n", response);

    // Verify key is gone
    const get_cmd = "*2\r\n$8\r\nJSON.GET\r\n$4\r\nfgt1\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expectEqualStrings("$-1\r\n", get_resp);
}

test "JSON.FORGET - on non-existent key" {
    const stream = try connectToServer();
    defer stream.close();

    const cmd = "*2\r\n$11\r\nJSON.FORGET\r\n$11\r\nnonexistent\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings(":0\r\n", response);
}

test "JSON.STRAPPEND - append to string field" {
    const stream = try connectToServer();
    defer stream.close();

    // Set document with string field
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nstr01\r\n$1\r\n$\r\n$17\r\n{\"name\":\"Hello\"}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Append to string
    const cmd = "*4\r\n$14\r\nJSON.STRAPPEND\r\n$5\r\nstr01\r\n$7\r\n$.name\r\n$6\r\n World\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);
    try testing.expectEqualStrings(":11\r\n", response); // "Hello World".len = 11

    // Verify the string was modified
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$5\r\nstr01\r\n$7\r\n$.name\r\n";
    const get_resp = try sendCommand(stream, get_cmd);
    defer freeResponse(get_resp);
    try testing.expectEqualStrings("$13\r\n\"Hello World\"\r\n", get_resp);
}

test "JSON.STRAPPEND - implicit root path" {
    const stream = try connectToServer();
    defer stream.close();

    // Set root string value
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nstr02\r\n$1\r\n$\r\n$7\r\n\"test\"\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);
    try testing.expectEqualStrings("+OK\r\n", set_resp);

    // Append with implicit root path (3 args)
    const cmd = "*3\r\n$14\r\nJSON.STRAPPEND\r\n$5\r\nstr02\r\n$2\r\n42\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);
    try testing.expectEqualStrings(":6\r\n", response); // "test42".len = 6
}

test "JSON.STRAPPEND - on non-string field returns error" {
    const stream = try connectToServer();
    defer stream.close();

    // Set document with numeric field
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nstr03\r\n$1\r\n$\r\n$12\r\n{\"count\":10}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);

    // Try to append to number
    const cmd = "*4\r\n$14\r\nJSON.STRAPPEND\r\n$5\r\nstr03\r\n$8\r\n$.count\r\n$4\r\ntest\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.STRAPPEND - on non-existent key returns error" {
    const stream = try connectToServer();
    defer stream.close();

    const cmd = "*4\r\n$14\r\nJSON.STRAPPEND\r\n$11\r\nnonexistent\r\n$1\r\n$\r\n$4\r\ntest\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.STRAPPEND - wrong arity" {
    const stream = try connectToServer();
    defer stream.close();

    const cmd = "*2\r\n$14\r\nJSON.STRAPPEND\r\n$5\r\nstr04\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.STRLEN - get string length" {
    const stream = try connectToServer();
    defer stream.close();

    // Set document with string field
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nlen01\r\n$1\r\n$\r\n$17\r\n{\"name\":\"Hello\"}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);

    // Get string length
    const cmd = "*3\r\n$11\r\nJSON.STRLEN\r\n$5\r\nlen01\r\n$7\r\n$.name\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings(":5\r\n", response); // "Hello".len = 5
}

test "JSON.STRLEN - implicit root path" {
    const stream = try connectToServer();
    defer stream.close();

    // Set root string value
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nlen02\r\n$1\r\n$\r\n$6\r\n\"abc\"\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);

    // Get length with implicit root path (2 args)
    const cmd = "*2\r\n$11\r\nJSON.STRLEN\r\n$5\r\nlen02\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings(":3\r\n", response);
}

test "JSON.STRLEN - on non-string field returns null" {
    const stream = try connectToServer();
    defer stream.close();

    // Set document with numeric field
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$5\r\nlen03\r\n$1\r\n$\r\n$12\r\n{\"count\":10}\r\n";
    const set_resp = try sendCommand(stream, set_cmd);
    defer freeResponse(set_resp);

    // Try to get length of number
    const cmd = "*3\r\n$11\r\nJSON.STRLEN\r\n$5\r\nlen03\r\n$8\r\n$.count\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings("$-1\r\n", response);
}

test "JSON.STRLEN - on non-existent key returns null" {
    const stream = try connectToServer();
    defer stream.close();

    const cmd = "*2\r\n$11\r\nJSON.STRLEN\r\n$11\r\nnonexistent\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expectEqualStrings("$-1\r\n", response);
}

test "JSON.STRLEN - wrong arity" {
    const stream = try connectToServer();
    defer stream.close();

    const cmd = "*1\r\n$11\r\nJSON.STRLEN\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);

    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "JSON.TOGGLE - single boolean" {
    const stream = try connectToServer();
    defer stream.close();

    // SET document with boolean
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$4\r\ntest\r\n$1\r\n$\r\n$15\r\n{\"active\":true}\r\n";
    const set_response = try sendCommand(stream, set_cmd);
    defer freeResponse(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // TOGGLE the boolean
    const toggle_cmd = "*3\r\n$11\r\nJSON.TOGGLE\r\n$4\r\ntest\r\n$8\r\n$.active\r\n";
    const toggle_response = try sendCommand(stream, toggle_cmd);
    defer freeResponse(toggle_response);
    try testing.expectEqualStrings(":0\r\n", toggle_response);

    // Verify it's now false
    const get_cmd = "*3\r\n$8\r\nJSON.GET\r\n$4\r\ntest\r\n$8\r\n$.active\r\n";
    const get_response = try sendCommand(stream, get_cmd);
    defer freeResponse(get_response);
    try testing.expectEqualStrings("$7\r\n[false]\r\n", get_response);
}

test "JSON.TOGGLE - multiple booleans with wildcard" {
    const stream = try connectToServer();
    defer stream.close();

    // SET document with multiple booleans
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$4\r\ntest\r\n$1\r\n$\r\n$31\r\n{\"a\":true,\"b\":false,\"c\":true}\r\n";
    const set_response = try sendCommand(stream, set_cmd);
    defer freeResponse(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // TOGGLE all booleans
    const toggle_cmd = "*3\r\n$11\r\nJSON.TOGGLE\r\n$4\r\ntest\r\n$3\r\n$.*\r\n";
    const toggle_response = try sendCommand(stream, toggle_cmd);
    defer freeResponse(toggle_response);
    // Returns array: [0, 1, 0] (true->false, false->true, true->false)
    try testing.expectEqualStrings("*3\r\n:0\r\n:1\r\n:0\r\n", toggle_response);
}

test "JSON.TOGGLE - non-boolean returns null" {
    const stream = try connectToServer();
    defer stream.close();

    // SET document with mixed types
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$4\r\ntest\r\n$1\r\n$\r\n$35\r\n{\"a\":true,\"b\":123,\"c\":\"text\"}\r\n";
    const set_response = try sendCommand(stream, set_cmd);
    defer freeResponse(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // TOGGLE all fields
    const toggle_cmd = "*3\r\n$11\r\nJSON.TOGGLE\r\n$4\r\ntest\r\n$3\r\n$.*\r\n";
    const toggle_response = try sendCommand(stream, toggle_cmd);
    defer freeResponse(toggle_response);
    // Returns: [0, null, null]
    try testing.expectEqualStrings("*3\r\n:0\r\n$-1\r\n$-1\r\n", toggle_response);
}

test "JSON.TOGGLE - root boolean with implicit path" {
    const stream = try connectToServer();
    defer stream.close();

    // SET root as boolean
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$4\r\ntest\r\n$1\r\n$\r\n$4\r\ntrue\r\n";
    const set_response = try sendCommand(stream, set_cmd);
    defer freeResponse(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // TOGGLE without path (implicit $)
    const toggle_cmd = "*2\r\n$11\r\nJSON.TOGGLE\r\n$4\r\ntest\r\n";
    const toggle_response = try sendCommand(stream, toggle_cmd);
    defer freeResponse(toggle_response);
    try testing.expectEqualStrings(":0\r\n", toggle_response);
}

test "JSON.TOGGLE - non-existent key returns null" {
    const stream = try connectToServer();
    defer stream.close();

    const cmd = "*3\r\n$11\r\nJSON.TOGGLE\r\n$11\r\nnonexistent\r\n$1\r\n$\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);
    try testing.expectEqualStrings("$-1\r\n", response);
}

test "JSON.TOGGLE - no matches returns empty array" {
    const stream = try connectToServer();
    defer stream.close();

    // SET document
    const set_cmd = "*4\r\n$8\r\nJSON.SET\r\n$4\r\ntest\r\n$1\r\n$\r\n$7\r\n{\"x\":1}\r\n";
    const set_response = try sendCommand(stream, set_cmd);
    defer freeResponse(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // TOGGLE non-existent path
    const toggle_cmd = "*3\r\n$11\r\nJSON.TOGGLE\r\n$4\r\ntest\r\n$12\r\n$.nonexistent\r\n";
    const toggle_response = try sendCommand(stream, toggle_cmd);
    defer freeResponse(toggle_response);
    try testing.expectEqualStrings("*0\r\n", toggle_response);
}

test "JSON.TOGGLE - wrong type key" {
    const stream = try connectToServer();
    defer stream.close();

    // SET a string key
    const set_cmd = "*3\r\n$3\r\nSET\r\n$4\r\ntest\r\n$5\r\nvalue\r\n";
    const set_response = try sendCommand(stream, set_cmd);
    defer freeResponse(set_response);
    try testing.expectEqualStrings("+OK\r\n", set_response);

    // Try TOGGLE on string key
    const toggle_cmd = "*3\r\n$11\r\nJSON.TOGGLE\r\n$4\r\ntest\r\n$1\r\n$\r\n";
    const toggle_response = try sendCommand(stream, toggle_cmd);
    defer freeResponse(toggle_response);
    try testing.expect(std.mem.startsWith(u8, toggle_response, "-WRONGTYPE"));
}

test "JSON.TOGGLE - wrong arity" {
    const stream = try connectToServer();
    defer stream.close();

    // Too few args
    const cmd = "*1\r\n$11\r\nJSON.TOGGLE\r\n";
    const response = try sendCommand(stream, cmd);
    defer freeResponse(response);
    try testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}
