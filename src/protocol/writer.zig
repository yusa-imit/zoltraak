const std = @import("std");
const parser = @import("parser.zig");
const RespValue = parser.RespValue;
const RespType = parser.RespType;

/// RESP protocol writer for serializing responses (RESP2 + RESP3)
pub const Writer = struct {
    allocator: std.mem.Allocator,
    /// Protocol version: 2 for RESP2, 3 for RESP3
    version: u8,

    /// Initialize a new RESP writer
    pub fn init(allocator: std.mem.Allocator) Writer {
        return Writer{
            .allocator = allocator,
            .version = 2, // Default to RESP2
        };
    }

    /// Set the RESP protocol version for this writer
    pub fn setVersion(self: *Writer, version: u8) void {
        self.version = version;
    }

    /// Deinitialize the writer (currently no-op, included for API consistency)
    pub fn deinit(self: *Writer) void {
        _ = self;
    }

    /// Serialize a RespValue to RESP2 protocol bytes
    /// Caller owns returned memory and must free it
    pub fn serialize(self: *Writer, value: RespValue) ![]const u8 {
        var buffer = std.ArrayList(u8){};
        errdefer buffer.deinit(self.allocator);

        try self.writeValue(&buffer, value);
        return buffer.toOwnedSlice(self.allocator);
    }

    /// Write a simple string response (+OK\r\n)
    pub fn writeSimpleString(self: *Writer, str: []const u8) ![]const u8 {
        var buffer = std.ArrayList(u8){};
        errdefer buffer.deinit(self.allocator);

        try buffer.append(self.allocator, '+');
        try buffer.appendSlice(self.allocator, str);
        try buffer.appendSlice(self.allocator, "\r\n");
        return buffer.toOwnedSlice(self.allocator);
    }

    /// Write an error response (-ERR message\r\n)
    pub fn writeError(self: *Writer, msg: []const u8) ![]const u8 {
        var buffer = std.ArrayList(u8){};
        errdefer buffer.deinit(self.allocator);

        try buffer.append(self.allocator, '-');
        try buffer.appendSlice(self.allocator, msg);
        try buffer.appendSlice(self.allocator, "\r\n");
        return buffer.toOwnedSlice(self.allocator);
    }

    /// Write an integer response (:123\r\n)
    pub fn writeInteger(self: *Writer, value: i64) ![]const u8 {
        var buffer = std.ArrayList(u8){};
        errdefer buffer.deinit(self.allocator);

        try buffer.append(self.allocator, ':');
        try std.fmt.format(buffer.writer(self.allocator), "{d}", .{value});
        try buffer.appendSlice(self.allocator, "\r\n");
        return buffer.toOwnedSlice(self.allocator);
    }

    /// Write a bulk string response ($6\r\nfoobar\r\n)
    /// If str is null, writes null bulk string ($-1\r\n)
    pub fn writeBulkString(self: *Writer, str: ?[]const u8) ![]const u8 {
        var buffer = std.ArrayList(u8){};
        errdefer buffer.deinit(self.allocator);

        if (str) |s| {
            try buffer.append(self.allocator, '$');
            try std.fmt.format(buffer.writer(self.allocator), "{d}", .{s.len});
            try buffer.appendSlice(self.allocator, "\r\n");
            try buffer.appendSlice(self.allocator, s);
            try buffer.appendSlice(self.allocator, "\r\n");
        } else {
            try buffer.appendSlice(self.allocator, "$-1\r\n");
        }
        return buffer.toOwnedSlice(self.allocator);
    }

    /// Write an array response (*2\r\n...\r\n)
    /// If values is null, writes null array (*-1\r\n)
    pub fn writeArray(self: *Writer, values: ?[]const RespValue) ![]const u8 {
        var buffer = std.ArrayList(u8){};
        errdefer buffer.deinit(self.allocator);

        if (values) |vals| {
            try buffer.append(self.allocator, '*');
            try std.fmt.format(buffer.writer(self.allocator), "{d}", .{vals.len});
            try buffer.appendSlice(self.allocator, "\r\n");

            for (vals) |val| {
                try self.writeValue(&buffer, val);
            }
        } else {
            try buffer.appendSlice(self.allocator, "*-1\r\n");
        }
        return buffer.toOwnedSlice(self.allocator);
    }

    /// Write a null bulk string ($-1\r\n)
    pub fn writeNull(self: *Writer) ![]const u8 {
        return self.writeBulkString(null);
    }

    /// Write a simple OK response (+OK\r\n)
    pub fn writeOK(self: *Writer) ![]const u8 {
        return self.writeSimpleString("OK");
    }

    // RESP3-specific write methods

    /// Write a RESP3 null (_\r\n)
    pub fn writeResp3Null(self: *Writer) ![]const u8 {
        var buffer = std.ArrayList(u8){};
        errdefer buffer.deinit(self.allocator);

        try buffer.appendSlice(self.allocator, "_\r\n");
        return buffer.toOwnedSlice(self.allocator);
    }

    /// Write a RESP3 boolean (#t\r\n or #f\r\n)
    pub fn writeBoolean(self: *Writer, value: bool) ![]const u8 {
        var buffer = std.ArrayList(u8){};
        errdefer buffer.deinit(self.allocator);

        try buffer.append(self.allocator, '#');
        try buffer.append(self.allocator, if (value) 't' else 'f');
        try buffer.appendSlice(self.allocator, "\r\n");
        return buffer.toOwnedSlice(self.allocator);
    }

    /// Write a RESP3 double (,3.14\r\n)
    pub fn writeDouble(self: *Writer, value: f64) ![]const u8 {
        var buffer = std.ArrayList(u8){};
        errdefer buffer.deinit(self.allocator);

        try buffer.append(self.allocator, ',');
        if (std.math.isInf(value)) {
            if (std.math.isPositiveInf(value)) {
                try buffer.appendSlice(self.allocator, "inf");
            } else {
                try buffer.appendSlice(self.allocator, "-inf");
            }
        } else {
            try std.fmt.format(buffer.writer(self.allocator), "{d}", .{value});
        }
        try buffer.appendSlice(self.allocator, "\r\n");
        return buffer.toOwnedSlice(self.allocator);
    }

    /// Write a RESP3 big number ((123...\r\n)
    pub fn writeBigNumber(self: *Writer, value: []const u8) ![]const u8 {
        var buffer = std.ArrayList(u8){};
        errdefer buffer.deinit(self.allocator);

        try buffer.append(self.allocator, '(');
        try buffer.appendSlice(self.allocator, value);
        try buffer.appendSlice(self.allocator, "\r\n");
        return buffer.toOwnedSlice(self.allocator);
    }

    /// Write a RESP3 bulk error (!<len>\r\n<error>\r\n)
    pub fn writeBulkError(self: *Writer, error_msg: []const u8) ![]const u8 {
        var buffer = std.ArrayList(u8){};
        errdefer buffer.deinit(self.allocator);

        try buffer.append(self.allocator, '!');
        try std.fmt.format(buffer.writer(self.allocator), "{d}", .{error_msg.len});
        try buffer.appendSlice(self.allocator, "\r\n");
        try buffer.appendSlice(self.allocator, error_msg);
        try buffer.appendSlice(self.allocator, "\r\n");
        return buffer.toOwnedSlice(self.allocator);
    }

    /// Write a RESP3 verbatim string (=<len>\r\n<format>:<data>\r\n)
    pub fn writeVerbatimString(self: *Writer, format: []const u8, data: []const u8) ![]const u8 {
        var buffer = std.ArrayList(u8){};
        errdefer buffer.deinit(self.allocator);

        const total_len = format.len + 1 + data.len; // format + ":" + data
        try buffer.append(self.allocator, '=');
        try std.fmt.format(buffer.writer(self.allocator), "{d}", .{total_len});
        try buffer.appendSlice(self.allocator, "\r\n");
        try buffer.appendSlice(self.allocator, format);
        try buffer.append(self.allocator, ':');
        try buffer.appendSlice(self.allocator, data);
        try buffer.appendSlice(self.allocator, "\r\n");
        return buffer.toOwnedSlice(self.allocator);
    }

    /// Write a RESP3 map (%<count>\r\n<key><value>...\r\n)
    pub fn writeMap(self: *Writer, pairs: []const struct { key: RespValue, value: RespValue }) ![]const u8 {
        var buffer = std.ArrayList(u8){};
        errdefer buffer.deinit(self.allocator);

        try buffer.append(self.allocator, '%');
        try std.fmt.format(buffer.writer(self.allocator), "{d}", .{pairs.len});
        try buffer.appendSlice(self.allocator, "\r\n");

        for (pairs) |pair| {
            try self.writeValue(&buffer, pair.key);
            try self.writeValue(&buffer, pair.value);
        }

        return buffer.toOwnedSlice(self.allocator);
    }

    /// Write a RESP3 set (~<count>\r\n<elem>...\r\n)
    pub fn writeSet(self: *Writer, elements: []const RespValue) ![]const u8 {
        var buffer = std.ArrayList(u8){};
        errdefer buffer.deinit(self.allocator);

        try buffer.append(self.allocator, '~');
        try std.fmt.format(buffer.writer(self.allocator), "{d}", .{elements.len});
        try buffer.appendSlice(self.allocator, "\r\n");

        for (elements) |elem| {
            try self.writeValue(&buffer, elem);
        }

        return buffer.toOwnedSlice(self.allocator);
    }

    /// Write a RESP3 push message (><count>\r\n<elem>...\r\n)
    pub fn writePush(self: *Writer, elements: []const RespValue) ![]const u8 {
        var buffer = std.ArrayList(u8){};
        errdefer buffer.deinit(self.allocator);

        try buffer.append(self.allocator, '>');
        try std.fmt.format(buffer.writer(self.allocator), "{d}", .{elements.len});
        try buffer.appendSlice(self.allocator, "\r\n");

        for (elements) |elem| {
            try self.writeValue(&buffer, elem);
        }

        return buffer.toOwnedSlice(self.allocator);
    }

    // Private helper to write a value to a buffer
    fn writeValue(self: *Writer, buffer: *std.ArrayList(u8), value: RespValue) !void {
        switch (value) {
            // RESP2 types
            .simple_string => |s| {
                try buffer.append(self.allocator, '+');
                try buffer.appendSlice(self.allocator, s);
                try buffer.appendSlice(self.allocator, "\r\n");
            },
            .error_string => |s| {
                try buffer.append(self.allocator, '-');
                try buffer.appendSlice(self.allocator, s);
                try buffer.appendSlice(self.allocator, "\r\n");
            },
            .integer => |i| {
                try buffer.append(self.allocator, ':');
                try std.fmt.format(buffer.writer(self.allocator), "{d}", .{i});
                try buffer.appendSlice(self.allocator, "\r\n");
            },
            .bulk_string => |s| {
                try buffer.append(self.allocator, '$');
                try std.fmt.format(buffer.writer(self.allocator), "{d}", .{s.len});
                try buffer.appendSlice(self.allocator, "\r\n");
                try buffer.appendSlice(self.allocator, s);
                try buffer.appendSlice(self.allocator, "\r\n");
            },
            .null_bulk_string => {
                try buffer.appendSlice(self.allocator, "$-1\r\n");
            },
            .array => |arr| {
                try buffer.append(self.allocator, '*');
                try std.fmt.format(buffer.writer(self.allocator), "{d}", .{arr.len});
                try buffer.appendSlice(self.allocator, "\r\n");
                for (arr) |item| {
                    try self.writeValue(buffer, item);
                }
            },
            .null_array => {
                try buffer.appendSlice(self.allocator, "*-1\r\n");
            },
            // RESP3 types
            .resp3_null => {
                try buffer.appendSlice(self.allocator, "_\r\n");
            },
            .boolean => |b| {
                try buffer.append(self.allocator, '#');
                try buffer.append(self.allocator, if (b) 't' else 'f');
                try buffer.appendSlice(self.allocator, "\r\n");
            },
            .double => |d| {
                try buffer.append(self.allocator, ',');
                if (std.math.isInf(d)) {
                    if (std.math.isPositiveInf(d)) {
                        try buffer.appendSlice(self.allocator, "inf");
                    } else {
                        try buffer.appendSlice(self.allocator, "-inf");
                    }
                } else {
                    try std.fmt.format(buffer.writer(self.allocator), "{d}", .{d});
                }
                try buffer.appendSlice(self.allocator, "\r\n");
            },
            .big_number => |bn| {
                try buffer.append(self.allocator, '(');
                try buffer.appendSlice(self.allocator, bn);
                try buffer.appendSlice(self.allocator, "\r\n");
            },
            .bulk_error => |err| {
                try buffer.append(self.allocator, '!');
                try std.fmt.format(buffer.writer(self.allocator), "{d}", .{err.len});
                try buffer.appendSlice(self.allocator, "\r\n");
                try buffer.appendSlice(self.allocator, err);
                try buffer.appendSlice(self.allocator, "\r\n");
            },
            .verbatim_string => |vs| {
                const total_len = vs.format.len + 1 + vs.data.len;
                try buffer.append(self.allocator, '=');
                try std.fmt.format(buffer.writer(self.allocator), "{d}", .{total_len});
                try buffer.appendSlice(self.allocator, "\r\n");
                try buffer.appendSlice(self.allocator, vs.format);
                try buffer.append(self.allocator, ':');
                try buffer.appendSlice(self.allocator, vs.data);
                try buffer.appendSlice(self.allocator, "\r\n");
            },
            .map => |m| {
                try buffer.append(self.allocator, '%');
                try std.fmt.format(buffer.writer(self.allocator), "{d}", .{m.len});
                try buffer.appendSlice(self.allocator, "\r\n");
                for (m) |kv| {
                    try self.writeValue(buffer, kv.key.*);
                    try self.writeValue(buffer, kv.value.*);
                }
            },
            .set => |s| {
                try buffer.append(self.allocator, '~');
                try std.fmt.format(buffer.writer(self.allocator), "{d}", .{s.len});
                try buffer.appendSlice(self.allocator, "\r\n");
                for (s) |elem| {
                    try self.writeValue(buffer, elem);
                }
            },
            .push => |p| {
                try buffer.append(self.allocator, '>');
                try std.fmt.format(buffer.writer(self.allocator), "{d}", .{p.len});
                try buffer.appendSlice(self.allocator, "\r\n");
                for (p) |elem| {
                    try self.writeValue(buffer, elem);
                }
            },
        }
    }
};

// Embedded unit tests

test "RESP writer - simple string" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const result = try writer.writeSimpleString("OK");
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "RESP writer - simple string empty" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const result = try writer.writeSimpleString("");
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+\r\n", result);
}

test "RESP writer - error" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const result = try writer.writeError("ERR unknown command");
    defer allocator.free(result);

    try std.testing.expectEqualStrings("-ERR unknown command\r\n", result);
}

test "RESP writer - integer positive" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const result = try writer.writeInteger(1000);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1000\r\n", result);
}

test "RESP writer - integer negative" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const result = try writer.writeInteger(-42);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":-42\r\n", result);
}

test "RESP writer - integer zero" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const result = try writer.writeInteger(0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "RESP writer - bulk string" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const result = try writer.writeBulkString("foobar");
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$6\r\nfoobar\r\n", result);
}

test "RESP writer - bulk string empty" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const result = try writer.writeBulkString("");
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$0\r\n\r\n", result);
}

test "RESP writer - null bulk string" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const result = try writer.writeBulkString(null);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "RESP writer - writeNull" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const result = try writer.writeNull();
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "RESP writer - writeOK" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const result = try writer.writeOK();
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "RESP writer - array with bulk strings" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const values = [_]RespValue{
        RespValue{ .bulk_string = "foo" },
        RespValue{ .bulk_string = "bar" },
    };
    const result = try writer.writeArray(&values);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", result);
}

test "RESP writer - array with mixed types" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const values = [_]RespValue{
        RespValue{ .bulk_string = "foo" },
        RespValue{ .integer = 42 },
        RespValue{ .simple_string = "OK" },
    };
    const result = try writer.writeArray(&values);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("*3\r\n$3\r\nfoo\r\n:42\r\n+OK\r\n", result);
}

test "RESP writer - empty array" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const values = [_]RespValue{};
    const result = try writer.writeArray(&values);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("*0\r\n", result);
}

test "RESP writer - null array" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const result = try writer.writeArray(null);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("*-1\r\n", result);
}

test "RESP writer - array with null bulk string" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const values = [_]RespValue{
        RespValue{ .null_bulk_string = {} },
        RespValue{ .integer = 42 },
    };
    const result = try writer.writeArray(&values);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("*2\r\n$-1\r\n:42\r\n", result);
}

test "RESP writer - serialize RespValue simple string" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const value = RespValue{ .simple_string = "PONG" };
    const result = try writer.serialize(value);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+PONG\r\n", result);
}

test "RESP writer - serialize RespValue integer" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    defer writer.deinit();

    const value = RespValue{ .integer = 123 };
    const result = try writer.serialize(value);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":123\r\n", result);
}

test "RESP writer - roundtrip parser to writer" {
    const allocator = std.testing.allocator;

    var p = parser.Parser.init(allocator);
    defer p.deinit();

    var w = Writer.init(allocator);
    defer w.deinit();

    // Parse input
    const input = "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    const parsed = try p.parse(input);
    defer p.freeValue(parsed);

    // Serialize back
    const serialized = try w.serialize(parsed);
    defer allocator.free(serialized);

    try std.testing.expectEqualStrings(input, serialized);
}

// RESP3 writer unit tests

test "RESP3 writer - null" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    const result = try writer.writeResp3Null();
    defer allocator.free(result);

    try std.testing.expectEqualStrings("_\r\n", result);
}

test "RESP3 writer - boolean true" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    const result = try writer.writeBoolean(true);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("#t\r\n", result);
}

test "RESP3 writer - boolean false" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    const result = try writer.writeBoolean(false);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("#f\r\n", result);
}

test "RESP3 writer - double" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    const result = try writer.writeDouble(3.14159);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(",3.14159\r\n", result);
}

test "RESP3 writer - double infinity" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    const result = try writer.writeDouble(std.math.inf(f64));
    defer allocator.free(result);

    try std.testing.expectEqualStrings(",inf\r\n", result);
}

test "RESP3 writer - big number" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    const result = try writer.writeBigNumber("123456789012345678901234567890");
    defer allocator.free(result);

    try std.testing.expectEqualStrings("(123456789012345678901234567890\r\n", result);
}

test "RESP3 writer - bulk error" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    const result = try writer.writeBulkError("SYNTAX invalid syntax");
    defer allocator.free(result);

    try std.testing.expectEqualStrings("!21\r\nSYNTAX invalid syntax\r\n", result);
}

test "RESP3 writer - verbatim string" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    const result = try writer.writeVerbatimString("txt", "Some string");
    defer allocator.free(result);

    try std.testing.expectEqualStrings("=15\r\ntxt:Some string\r\n", result);
}

test "RESP3 writer - map" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    const pairs = [_]struct { key: RespValue, value: RespValue }{
        .{ .key = RespValue{ .simple_string = "key1" }, .value = RespValue{ .integer = 1 } },
        .{ .key = RespValue{ .simple_string = "key2" }, .value = RespValue{ .integer = 2 } },
    };
    const result = try writer.writeMap(&pairs);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("%2\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n", result);
}

test "RESP3 writer - set" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    const elements = [_]RespValue{
        RespValue{ .simple_string = "elem1" },
        RespValue{ .simple_string = "elem2" },
        RespValue{ .simple_string = "elem3" },
    };
    const result = try writer.writeSet(&elements);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("~3\r\n+elem1\r\n+elem2\r\n+elem3\r\n", result);
}

test "RESP3 writer - push" {
    const allocator = std.testing.allocator;
    var writer = Writer.init(allocator);
    writer.setVersion(3);
    defer writer.deinit();

    const elements = [_]RespValue{
        RespValue{ .simple_string = "pubsub" },
        RespValue{ .simple_string = "message" },
    };
    const result = try writer.writePush(&elements);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(">2\r\n+pubsub\r\n+message\r\n", result);
}
