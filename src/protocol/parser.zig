const std = @import("std");

/// RESP2 protocol value types
pub const RespType = enum {
    simple_string,
    error_string,
    integer,
    bulk_string,
    array,
    null_bulk_string,
    null_array,
};

/// Tagged union representing a RESP2 protocol value
pub const RespValue = union(RespType) {
    simple_string: []const u8,
    error_string: []const u8,
    integer: i64,
    bulk_string: []const u8,
    array: []const RespValue,
    null_bulk_string: void,
    null_array: void,
};

/// RESP2 protocol parsing errors
pub const ParseError = error{
    UnexpectedEOF,
    InvalidType,
    InvalidInteger,
    InvalidLength,
    MalformedProtocol,
    OutOfMemory,
};

/// RESP2 protocol parser
pub const Parser = struct {
    allocator: std.mem.Allocator,
    buffer: []const u8,
    pos: usize,

    /// Initialize a new parser with the given allocator
    pub fn init(allocator: std.mem.Allocator) Parser {
        return Parser{
            .allocator = allocator,
            .buffer = &[_]u8{},
            .pos = 0,
        };
    }

    /// Deinitialize the parser (currently no-op, included for API consistency)
    pub fn deinit(self: *Parser) void {
        _ = self;
    }

    /// Parse a complete RESP message from the given data
    /// Memory allocated for the RespValue must be freed with freeValue()
    pub fn parse(self: *Parser, data: []const u8) ParseError!RespValue {
        self.buffer = data;
        self.pos = 0;
        return self.parseValue();
    }

    /// Free memory associated with a parsed RespValue
    pub fn freeValue(self: *Parser, value: RespValue) void {
        switch (value) {
            .simple_string => |s| self.allocator.free(s),
            .error_string => |s| self.allocator.free(s),
            .bulk_string => |s| self.allocator.free(s),
            .array => |arr| {
                for (arr) |item| {
                    self.freeValue(item);
                }
                self.allocator.free(arr);
            },
            .integer, .null_bulk_string, .null_array => {},
        }
    }

    // Private parsing methods

    fn parseValue(self: *Parser) ParseError!RespValue {
        if (self.pos >= self.buffer.len) {
            return ParseError.UnexpectedEOF;
        }

        const type_byte = self.buffer[self.pos];
        self.pos += 1;

        return switch (type_byte) {
            '+' => RespValue{ .simple_string = try self.parseSimpleString() },
            '-' => RespValue{ .error_string = try self.parseError() },
            ':' => RespValue{ .integer = try self.parseInteger() },
            '$' => try self.parseBulkString(),
            '*' => try self.parseArray(),
            else => ParseError.InvalidType,
        };
    }

    fn parseSimpleString(self: *Parser) ParseError![]const u8 {
        const line = try self.readLine();
        return self.allocator.dupe(u8, line) catch return ParseError.OutOfMemory;
    }

    fn parseError(self: *Parser) ParseError![]const u8 {
        const line = try self.readLine();
        return self.allocator.dupe(u8, line) catch return ParseError.OutOfMemory;
    }

    fn parseInteger(self: *Parser) ParseError!i64 {
        const line = try self.readLine();
        return std.fmt.parseInt(i64, line, 10) catch return ParseError.InvalidInteger;
    }

    fn parseBulkString(self: *Parser) ParseError!RespValue {
        const length_line = try self.readLine();
        const length = std.fmt.parseInt(i64, length_line, 10) catch return ParseError.InvalidLength;

        if (length == -1) {
            return RespValue{ .null_bulk_string = {} };
        }

        if (length < 0) {
            return ParseError.InvalidLength;
        }

        const len: usize = @intCast(length);
        const data = try self.readBytes(len);

        // Read and verify trailing \r\n
        if (self.pos + 2 > self.buffer.len) {
            return ParseError.UnexpectedEOF;
        }
        if (self.buffer[self.pos] != '\r' or self.buffer[self.pos + 1] != '\n') {
            return ParseError.MalformedProtocol;
        }
        self.pos += 2;

        const owned_data = self.allocator.dupe(u8, data) catch return ParseError.OutOfMemory;
        return RespValue{ .bulk_string = owned_data };
    }

    fn parseArray(self: *Parser) ParseError!RespValue {
        const count_line = try self.readLine();
        const count = std.fmt.parseInt(i64, count_line, 10) catch return ParseError.InvalidLength;

        if (count == -1) {
            return RespValue{ .null_array = {} };
        }

        if (count < 0) {
            return ParseError.InvalidLength;
        }

        const len: usize = @intCast(count);
        const elements = self.allocator.alloc(RespValue, len) catch return ParseError.OutOfMemory;
        errdefer self.allocator.free(elements);

        var i: usize = 0;
        errdefer {
            // Free any elements that were successfully parsed before the error
            for (0..i) |j| {
                self.freeValue(elements[j]);
            }
        }

        while (i < len) : (i += 1) {
            elements[i] = try self.parseValue();
        }

        return RespValue{ .array = elements };
    }

    fn readLine(self: *Parser) ParseError![]const u8 {
        const start = self.pos;
        while (self.pos < self.buffer.len) : (self.pos += 1) {
            if (self.buffer[self.pos] == '\r') {
                if (self.pos + 1 >= self.buffer.len) {
                    return ParseError.UnexpectedEOF;
                }
                if (self.buffer[self.pos + 1] == '\n') {
                    const line = self.buffer[start..self.pos];
                    self.pos += 2; // Skip \r\n
                    return line;
                }
            }
        }
        return ParseError.UnexpectedEOF;
    }

    fn readBytes(self: *Parser, count: usize) ParseError![]const u8 {
        if (self.pos + count > self.buffer.len) {
            return ParseError.UnexpectedEOF;
        }
        const data = self.buffer[self.pos .. self.pos + count];
        self.pos += count;
        return data;
    }
};

// Embedded unit tests

test "RESP parser - simple string" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = "+OK\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.simple_string, @as(RespType, result));
    try std.testing.expectEqualStrings("OK", result.simple_string);
}

test "RESP parser - simple string empty" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = "+\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.simple_string, @as(RespType, result));
    try std.testing.expectEqualStrings("", result.simple_string);
}

test "RESP parser - error string" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = "-ERR unknown command\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.error_string, @as(RespType, result));
    try std.testing.expectEqualStrings("ERR unknown command", result.error_string);
}

test "RESP parser - integer positive" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = ":1000\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.integer, @as(RespType, result));
    try std.testing.expectEqual(@as(i64, 1000), result.integer);
}

test "RESP parser - integer negative" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = ":-42\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.integer, @as(RespType, result));
    try std.testing.expectEqual(@as(i64, -42), result.integer);
}

test "RESP parser - integer zero" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = ":0\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.integer, @as(RespType, result));
    try std.testing.expectEqual(@as(i64, 0), result.integer);
}

test "RESP parser - bulk string" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = "$6\r\nfoobar\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.bulk_string, @as(RespType, result));
    try std.testing.expectEqualStrings("foobar", result.bulk_string);
}

test "RESP parser - bulk string empty" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = "$0\r\n\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.bulk_string, @as(RespType, result));
    try std.testing.expectEqualStrings("", result.bulk_string);
}

test "RESP parser - null bulk string" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = "$-1\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.null_bulk_string, @as(RespType, result));
}

test "RESP parser - array with bulk strings" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.array, @as(RespType, result));
    try std.testing.expectEqual(@as(usize, 2), result.array.len);
    try std.testing.expectEqualStrings("foo", result.array[0].bulk_string);
    try std.testing.expectEqualStrings("bar", result.array[1].bulk_string);
}

test "RESP parser - array with mixed types" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = "*3\r\n$3\r\nfoo\r\n:42\r\n+OK\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.array, @as(RespType, result));
    try std.testing.expectEqual(@as(usize, 3), result.array.len);
    try std.testing.expectEqualStrings("foo", result.array[0].bulk_string);
    try std.testing.expectEqual(@as(i64, 42), result.array[1].integer);
    try std.testing.expectEqualStrings("OK", result.array[2].simple_string);
}

test "RESP parser - empty array" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = "*0\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.array, @as(RespType, result));
    try std.testing.expectEqual(@as(usize, 0), result.array.len);
}

test "RESP parser - null array" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = "*-1\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.null_array, @as(RespType, result));
}

test "RESP parser - array with null bulk string" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = "*2\r\n$-1\r\n:42\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.array, @as(RespType, result));
    try std.testing.expectEqual(@as(usize, 2), result.array.len);
    try std.testing.expectEqual(RespType.null_bulk_string, @as(RespType, result.array[0]));
    try std.testing.expectEqual(@as(i64, 42), result.array[1].integer);
}

test "RESP parser - nested arrays" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = "*2\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*1\r\n:42\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.array, @as(RespType, result));
    try std.testing.expectEqual(@as(usize, 2), result.array.len);
    try std.testing.expectEqual(@as(usize, 2), result.array[0].array.len);
    try std.testing.expectEqual(@as(usize, 1), result.array[1].array.len);
}

test "RESP parser - error incomplete data" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = "+OK";
    try std.testing.expectError(ParseError.UnexpectedEOF, parser.parse(input));
}

test "RESP parser - error invalid type byte" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = "@test\r\n";
    try std.testing.expectError(ParseError.InvalidType, parser.parse(input));
}

test "RESP parser - error invalid integer" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = ":notanumber\r\n";
    try std.testing.expectError(ParseError.InvalidInteger, parser.parse(input));
}

test "RESP parser - error bulk string length mismatch" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = "$5\r\nfoo\r\n";
    try std.testing.expectError(ParseError.UnexpectedEOF, parser.parse(input));
}

test "RESP parser - error malformed bulk string terminator" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const input = "$3\r\nfooXX";
    try std.testing.expectError(ParseError.MalformedProtocol, parser.parse(input));
}
