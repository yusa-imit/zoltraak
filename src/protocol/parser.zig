const std = @import("std");

/// Key-value pair for RESP3 map type
pub const MapEntry = struct {
    key: *const RespValue,
    value: *const RespValue,
};

/// RESP protocol value types (RESP2 + RESP3)
pub const RespType = enum {
    // RESP2 types
    simple_string,
    error_string,
    integer,
    bulk_string,
    array,
    null_bulk_string,
    null_array,
    // RESP3 types
    resp3_null,
    boolean,
    double,
    big_number,
    bulk_error,
    verbatim_string,
    map,
    set,
    push,
};

/// Tagged union representing a RESP protocol value (RESP2 + RESP3)
pub const RespValue = union(RespType) {
    // RESP2 types
    simple_string: []const u8,
    error_string: []const u8,
    integer: i64,
    bulk_string: []const u8,
    array: []const RespValue,
    null_bulk_string: void,
    null_array: void,
    // RESP3 types
    resp3_null: void,
    boolean: bool,
    double: f64,
    big_number: []const u8,
    bulk_error: []const u8,
    verbatim_string: struct {
        format: []const u8, // 3-char format
        data: []const u8,
    },
    map: []const MapEntry,
    set: []const RespValue,
    push: []const RespValue,
};

/// RESP protocol parsing errors
pub const ParseError = error{
    UnexpectedEOF,
    InvalidType,
    InvalidInteger,
    InvalidLength,
    MalformedProtocol,
    OutOfMemory,
    InvalidDouble,
    InvalidBoolean,
};

/// RESP protocol parser (supports RESP2 and RESP3)
pub const Parser = struct {
    allocator: std.mem.Allocator,
    buffer: []const u8,
    pos: usize,
    /// Protocol version: 2 for RESP2, 3 for RESP3
    version: u8,

    /// Initialize a new parser with the given allocator
    pub fn init(allocator: std.mem.Allocator) Parser {
        return Parser{
            .allocator = allocator,
            .buffer = &[_]u8{},
            .pos = 0,
            .version = 2, // Default to RESP2
        };
    }

    /// Set the RESP protocol version for this parser
    pub fn setVersion(self: *Parser, version: u8) void {
        self.version = version;
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
            .big_number => |s| self.allocator.free(s),
            .bulk_error => |s| self.allocator.free(s),
            .verbatim_string => |vs| {
                self.allocator.free(vs.format);
                self.allocator.free(vs.data);
            },
            .map => |m| {
                for (m) |kv| {
                    self.freeValue(kv.key.*);
                    self.freeValue(kv.value.*);
                    self.allocator.destroy(kv.key);
                    self.allocator.destroy(kv.value);
                }
                self.allocator.free(m);
            },
            .set => |s| {
                for (s) |item| {
                    self.freeValue(item);
                }
                self.allocator.free(s);
            },
            .push => |arr| {
                for (arr) |item| {
                    self.freeValue(item);
                }
                self.allocator.free(arr);
            },
            .integer, .null_bulk_string, .null_array, .resp3_null, .boolean, .double => {},
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
            // RESP2 types
            '+' => RespValue{ .simple_string = try self.parseSimpleString() },
            '-' => RespValue{ .error_string = try self.parseError() },
            ':' => RespValue{ .integer = try self.parseInteger() },
            '$' => try self.parseBulkString(),
            '*' => try self.parseArray(),
            // RESP3 types
            '_' => RespValue{ .resp3_null = {} }, // Null
            '#' => RespValue{ .boolean = try self.parseBoolean() }, // Boolean
            ',' => RespValue{ .double = try self.parseDouble() }, // Double
            '(' => RespValue{ .big_number = try self.parseBigNumber() }, // Big number
            '!' => RespValue{ .bulk_error = try self.parseBulkError() }, // Bulk error
            '=' => try self.parseVerbatimString(), // Verbatim string
            '%' => try self.parseMap(), // Map
            '~' => try self.parseSet(), // Set
            '>' => try self.parsePush(), // Push
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

    // RESP3 parsing functions

    fn parseBoolean(self: *Parser) ParseError!bool {
        const line = try self.readLine();
        if (std.mem.eql(u8, line, "t")) {
            return true;
        } else if (std.mem.eql(u8, line, "f")) {
            return false;
        }
        return ParseError.InvalidBoolean;
    }

    fn parseDouble(self: *Parser) ParseError!f64 {
        const line = try self.readLine();
        // Handle special cases
        if (std.mem.eql(u8, line, "inf")) {
            return std.math.inf(f64);
        } else if (std.mem.eql(u8, line, "-inf")) {
            return -std.math.inf(f64);
        }
        return std.fmt.parseFloat(f64, line) catch return ParseError.InvalidDouble;
    }

    fn parseBigNumber(self: *Parser) ParseError![]const u8 {
        const line = try self.readLine();
        return self.allocator.dupe(u8, line) catch return ParseError.OutOfMemory;
    }

    fn parseBulkError(self: *Parser) ParseError![]const u8 {
        // Same format as bulk string
        const length_line = try self.readLine();
        const length = std.fmt.parseInt(i64, length_line, 10) catch return ParseError.InvalidLength;

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
        return owned_data;
    }

    fn parseVerbatimString(self: *Parser) ParseError!RespValue {
        const length_line = try self.readLine();
        const length = std.fmt.parseInt(i64, length_line, 10) catch return ParseError.InvalidLength;

        if (length < 4) { // Minimum: 3-byte format + ":"
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

        // Extract format (first 3 bytes) and data (after ":")
        if (data.len < 4 or data[3] != ':') {
            return ParseError.MalformedProtocol;
        }

        const format = self.allocator.dupe(u8, data[0..3]) catch return ParseError.OutOfMemory;
        errdefer self.allocator.free(format);
        const string_data = self.allocator.dupe(u8, data[4..]) catch return ParseError.OutOfMemory;

        return RespValue{
            .verbatim_string = .{
                .format = format,
                .data = string_data,
            },
        };
    }

    fn parseMap(self: *Parser) ParseError!RespValue {
        const count_line = try self.readLine();
        const count = std.fmt.parseInt(i64, count_line, 10) catch return ParseError.InvalidLength;

        if (count < 0) {
            return ParseError.InvalidLength;
        }

        const len: usize = @intCast(count);
        const pairs = self.allocator.alloc(MapEntry, len) catch return ParseError.OutOfMemory;
        errdefer self.allocator.free(pairs);

        var i: usize = 0;
        errdefer {
            for (0..i) |j| {
                self.freeValue(pairs[j].key.*);
                self.freeValue(pairs[j].value.*);
                self.allocator.destroy(pairs[j].key);
                self.allocator.destroy(pairs[j].value);
            }
        }

        while (i < len) : (i += 1) {
            const key_ptr = self.allocator.create(RespValue) catch return ParseError.OutOfMemory;
            errdefer self.allocator.destroy(key_ptr);
            key_ptr.* = try self.parseValue();

            const value_ptr = self.allocator.create(RespValue) catch return ParseError.OutOfMemory;
            errdefer self.allocator.destroy(value_ptr);
            value_ptr.* = try self.parseValue();

            pairs[i] = .{ .key = key_ptr, .value = value_ptr };
        }

        return RespValue{ .map = pairs };
    }

    fn parseSet(self: *Parser) ParseError!RespValue {
        const count_line = try self.readLine();
        const count = std.fmt.parseInt(i64, count_line, 10) catch return ParseError.InvalidLength;

        if (count < 0) {
            return ParseError.InvalidLength;
        }

        const len: usize = @intCast(count);
        const elements = self.allocator.alloc(RespValue, len) catch return ParseError.OutOfMemory;
        errdefer self.allocator.free(elements);

        var i: usize = 0;
        errdefer {
            for (0..i) |j| {
                self.freeValue(elements[j]);
            }
        }

        while (i < len) : (i += 1) {
            elements[i] = try self.parseValue();
        }

        return RespValue{ .set = elements };
    }

    fn parsePush(self: *Parser) ParseError!RespValue {
        const count_line = try self.readLine();
        const count = std.fmt.parseInt(i64, count_line, 10) catch return ParseError.InvalidLength;

        if (count < 0) {
            return ParseError.InvalidLength;
        }

        const len: usize = @intCast(count);
        const elements = self.allocator.alloc(RespValue, len) catch return ParseError.OutOfMemory;
        errdefer self.allocator.free(elements);

        var i: usize = 0;
        errdefer {
            for (0..i) |j| {
                self.freeValue(elements[j]);
            }
        }

        while (i < len) : (i += 1) {
            elements[i] = try self.parseValue();
        }

        return RespValue{ .push = elements };
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

// RESP3 unit tests

test "RESP3 parser - null" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    const input = "_\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.resp3_null, @as(RespType, result));
}

test "RESP3 parser - boolean true" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    const input = "#t\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.boolean, @as(RespType, result));
    try std.testing.expectEqual(true, result.boolean);
}

test "RESP3 parser - boolean false" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    const input = "#f\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.boolean, @as(RespType, result));
    try std.testing.expectEqual(false, result.boolean);
}

test "RESP3 parser - double" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    const input = ",3.14159\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.double, @as(RespType, result));
    try std.testing.expectApproxEqAbs(@as(f64, 3.14159), result.double, 0.00001);
}

test "RESP3 parser - double infinity" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    const input = ",inf\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.double, @as(RespType, result));
    try std.testing.expect(std.math.isInf(result.double));
    try std.testing.expect(std.math.isPositiveInf(result.double));
}

test "RESP3 parser - big number" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    const input = "(123456789012345678901234567890\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.big_number, @as(RespType, result));
    try std.testing.expectEqualStrings("123456789012345678901234567890", result.big_number);
}

test "RESP3 parser - bulk error" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    const input = "!21\r\nSYNTAX invalid syntax\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.bulk_error, @as(RespType, result));
    try std.testing.expectEqualStrings("SYNTAX invalid syntax", result.bulk_error);
}

test "RESP3 parser - verbatim string" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    const input = "=15\r\ntxt:Some string\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.verbatim_string, @as(RespType, result));
    try std.testing.expectEqualStrings("txt", result.verbatim_string.format);
    try std.testing.expectEqualStrings("Some string", result.verbatim_string.data);
}

test "RESP3 parser - map" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    const input = "%2\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.map, @as(RespType, result));
    try std.testing.expectEqual(@as(usize, 2), result.map.len);
    try std.testing.expectEqualStrings("key1", result.map[0].key.simple_string);
    try std.testing.expectEqual(@as(i64, 1), result.map[0].value.integer);
}

test "RESP3 parser - set" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    const input = "~3\r\n+elem1\r\n+elem2\r\n+elem3\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.set, @as(RespType, result));
    try std.testing.expectEqual(@as(usize, 3), result.set.len);
    try std.testing.expectEqualStrings("elem1", result.set[0].simple_string);
}

test "RESP3 parser - push" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    parser.setVersion(3);
    defer parser.deinit();

    const input = ">2\r\n+pubsub\r\n+message\r\n";
    const result = try parser.parse(input);
    defer parser.freeValue(result);

    try std.testing.expectEqual(RespType.push, @as(RespType, result));
    try std.testing.expectEqual(@as(usize, 2), result.push.len);
    try std.testing.expectEqualStrings("pubsub", result.push[0].simple_string);
}
