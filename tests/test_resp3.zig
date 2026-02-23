const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const writer_mod = @import("../src/protocol/writer.zig");

const Parser = protocol.Parser;
const Writer = writer_mod.Writer;
const RespValue = protocol.RespValue;

// Integration tests for RESP3 protocol support

test "RESP3 - parse and serialize boolean" {
    const allocator = std.testing.allocator;

    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    // Parse boolean
    const input = "#t\r\n";
    const parsed = try parser.parse(input);
    defer parser.freeValue(parsed);

    try std.testing.expectEqual(protocol.RespType.boolean, @as(protocol.RespType, parsed));
    try std.testing.expectEqual(true, parsed.boolean);

    // Serialize boolean
    const serialized = try writer.writeBoolean(true);
    defer allocator.free(serialized);

    try std.testing.expectEqualStrings(input, serialized);
}

test "RESP3 - parse and serialize double" {
    const allocator = std.testing.allocator;

    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    // Parse double
    const input = ",3.14\r\n";
    const parsed = try parser.parse(input);
    defer parser.freeValue(parsed);

    try std.testing.expectEqual(protocol.RespType.double, @as(protocol.RespType, parsed));
    try std.testing.expectApproxEqAbs(@as(f64, 3.14), parsed.double, 0.01);

    // Serialize double
    const serialized = try writer.writeDouble(3.14);
    defer allocator.free(serialized);

    try std.testing.expectEqualStrings(input, serialized);
}

test "RESP3 - parse and serialize null" {
    const allocator = std.testing.allocator;

    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    // Parse null
    const input = "_\r\n";
    const parsed = try parser.parse(input);
    defer parser.freeValue(parsed);

    try std.testing.expectEqual(protocol.RespType.resp3_null, @as(protocol.RespType, parsed));

    // Serialize null
    const serialized = try writer.writeResp3Null();
    defer allocator.free(serialized);

    try std.testing.expectEqualStrings(input, serialized);
}

test "RESP3 - parse and serialize big number" {
    const allocator = std.testing.allocator;

    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    // Parse big number
    const input = "(3492890328409238509324850943850943825024385\r\n";
    const parsed = try parser.parse(input);
    defer parser.freeValue(parsed);

    try std.testing.expectEqual(protocol.RespType.big_number, @as(protocol.RespType, parsed));
    try std.testing.expectEqualStrings("3492890328409238509324850943850943825024385", parsed.big_number);

    // Serialize big number
    const serialized = try writer.writeBigNumber("3492890328409238509324850943850943825024385");
    defer allocator.free(serialized);

    try std.testing.expectEqualStrings(input, serialized);
}

test "RESP3 - parse and serialize bulk error" {
    const allocator = std.testing.allocator;

    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    // Parse bulk error
    const input = "!21\r\nSYNTAX invalid syntax\r\n";
    const parsed = try parser.parse(input);
    defer parser.freeValue(parsed);

    try std.testing.expectEqual(protocol.RespType.bulk_error, @as(protocol.RespType, parsed));
    try std.testing.expectEqualStrings("SYNTAX invalid syntax", parsed.bulk_error);

    // Serialize bulk error
    const serialized = try writer.writeBulkError("SYNTAX invalid syntax");
    defer allocator.free(serialized);

    try std.testing.expectEqualStrings(input, serialized);
}

test "RESP3 - parse and serialize verbatim string" {
    const allocator = std.testing.allocator;

    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    // Parse verbatim string
    const input = "=15\r\ntxt:Some string\r\n";
    const parsed = try parser.parse(input);
    defer parser.freeValue(parsed);

    try std.testing.expectEqual(protocol.RespType.verbatim_string, @as(protocol.RespType, parsed));
    try std.testing.expectEqualStrings("txt", parsed.verbatim_string.format);
    try std.testing.expectEqualStrings("Some string", parsed.verbatim_string.data);

    // Serialize verbatim string
    const serialized = try writer.writeVerbatimString("txt", "Some string");
    defer allocator.free(serialized);

    try std.testing.expectEqualStrings(input, serialized);
}

test "RESP3 - parse and serialize map" {
    const allocator = std.testing.allocator;

    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    // Parse map
    const input = "%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n";
    const parsed = try parser.parse(input);
    defer parser.freeValue(parsed);

    try std.testing.expectEqual(protocol.RespType.map, @as(protocol.RespType, parsed));
    try std.testing.expectEqual(@as(usize, 2), parsed.map.len);
    try std.testing.expectEqualStrings("first", parsed.map[0].key.simple_string);
    try std.testing.expectEqual(@as(i64, 1), parsed.map[0].value.integer);

    // Serialize map
    const pairs = [_]struct { key: RespValue, value: RespValue }{
        .{ .key = RespValue{ .simple_string = "first" }, .value = RespValue{ .integer = 1 } },
        .{ .key = RespValue{ .simple_string = "second" }, .value = RespValue{ .integer = 2 } },
    };
    const serialized = try writer.writeMap(&pairs);
    defer allocator.free(serialized);

    try std.testing.expectEqualStrings(input, serialized);
}

test "RESP3 - parse and serialize set" {
    const allocator = std.testing.allocator;

    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    // Parse set
    const input = "~3\r\n+orange\r\n+apple\r\n+banana\r\n";
    const parsed = try parser.parse(input);
    defer parser.freeValue(parsed);

    try std.testing.expectEqual(protocol.RespType.set, @as(protocol.RespType, parsed));
    try std.testing.expectEqual(@as(usize, 3), parsed.set.len);
    try std.testing.expectEqualStrings("orange", parsed.set[0].simple_string);
    try std.testing.expectEqualStrings("apple", parsed.set[1].simple_string);
    try std.testing.expectEqualStrings("banana", parsed.set[2].simple_string);

    // Serialize set
    const elements = [_]RespValue{
        RespValue{ .simple_string = "orange" },
        RespValue{ .simple_string = "apple" },
        RespValue{ .simple_string = "banana" },
    };
    const serialized = try writer.writeSet(&elements);
    defer allocator.free(serialized);

    try std.testing.expectEqualStrings(input, serialized);
}

test "RESP3 - parse and serialize push" {
    const allocator = std.testing.allocator;

    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    // Parse push
    const input = ">3\r\n+subscribe\r\n+channel\r\n:1\r\n";
    const parsed = try parser.parse(input);
    defer parser.freeValue(parsed);

    try std.testing.expectEqual(protocol.RespType.push, @as(protocol.RespType, parsed));
    try std.testing.expectEqual(@as(usize, 3), parsed.push.len);
    try std.testing.expectEqualStrings("subscribe", parsed.push[0].simple_string);
    try std.testing.expectEqualStrings("channel", parsed.push[1].simple_string);
    try std.testing.expectEqual(@as(i64, 1), parsed.push[2].integer);

    // Serialize push
    const elements = [_]RespValue{
        RespValue{ .simple_string = "subscribe" },
        RespValue{ .simple_string = "channel" },
        RespValue{ .integer = 1 },
    };
    const serialized = try writer.writePush(&elements);
    defer allocator.free(serialized);

    try std.testing.expectEqualStrings(input, serialized);
}

test "RESP3 - complex nested structure" {
    const allocator = std.testing.allocator;

    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    // Parse complex structure: array with mixed RESP3 types
    const input = "*5\r\n:1\r\n#t\r\n,3.5\r\n_\r\n$5\r\nhello\r\n";
    const parsed = try parser.parse(input);
    defer parser.freeValue(parsed);

    try std.testing.expectEqual(protocol.RespType.array, @as(protocol.RespType, parsed));
    try std.testing.expectEqual(@as(usize, 5), parsed.array.len);
    try std.testing.expectEqual(@as(i64, 1), parsed.array[0].integer);
    try std.testing.expectEqual(true, parsed.array[1].boolean);
    try std.testing.expectApproxEqAbs(@as(f64, 3.5), parsed.array[2].double, 0.1);
    try std.testing.expectEqual(protocol.RespType.resp3_null, @as(protocol.RespType, parsed.array[3]));
    try std.testing.expectEqualStrings("hello", parsed.array[4].bulk_string);
}
