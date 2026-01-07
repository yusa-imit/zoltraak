const std = @import("std");
const parser = @import("parser.zig");
const RespValue = parser.RespValue;
const RespType = parser.RespType;

/// RESP2 protocol writer for serializing responses
pub const Writer = struct {
    allocator: std.mem.Allocator,

    /// Initialize a new RESP writer
    pub fn init(allocator: std.mem.Allocator) Writer {
        return Writer{
            .allocator = allocator,
        };
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

    // Private helper to write a value to a buffer
    fn writeValue(self: *Writer, buffer: *std.ArrayList(u8), value: RespValue) !void {
        switch (value) {
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
