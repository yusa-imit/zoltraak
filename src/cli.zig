const std = @import("std");
const sailor = @import("sailor");
const net = std.net;

// Define CLI flags
const flags = [_]sailor.arg.FlagDef{
    .{ .name = "host", .short = 'h', .type = .string, .default = "127.0.0.1", .help = "Server hostname" },
    .{ .name = "port", .short = 'p', .type = .string, .default = "6379", .help = "Server port" },
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Parse command line arguments
    const ArgParser = sailor.arg.Parser(&flags);
    var parser = ArgParser.init(allocator);
    defer parser.deinit();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    try parser.parse(args[1..]);

    // Get connection settings
    const host = parser.getString("host", "127.0.0.1");
    const port_str = parser.getString("port", "6379");
    const port = try std.fmt.parseInt(u16, port_str, 10);

    // Connect to Zoltraak server
    const address = try net.Address.parseIp(host, port);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    std.debug.print("Connected to {s}:{}\n", .{ host, port });

    // Simple REPL loop using stdin
    var read_buffer: [4096]u8 = undefined;
    var input_buffer: [1024]u8 = undefined;

    while (true) {
        // Print prompt
        std.debug.print("{s}:{}> ", .{ host, port });

        // Read line from stdin manually
        var line_len: usize = 0;
        while (line_len < input_buffer.len) {
            var ch: [1]u8 = undefined;
            const n = std.posix.read(std.posix.STDIN_FILENO, &ch) catch |err| switch (err) {
                error.WouldBlock => continue,
                else => return err,
            };
            if (n == 0) {
                if (line_len == 0) {
                    // EOF with no input
                    std.debug.print("\n", .{});
                    return;
                }
                break;
            }
            if (ch[0] == '\n') break;
            input_buffer[line_len] = ch[0];
            line_len += 1;
        }

        const line = input_buffer[0..line_len];

        // Trim whitespace
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (trimmed.len == 0) continue;

        // Check for quit command
        if (std.ascii.eqlIgnoreCase(trimmed, "quit") or std.ascii.eqlIgnoreCase(trimmed, "exit")) {
            try sendCommand(stream, "QUIT");
            break;
        }

        // Send command to server
        try sendCommand(stream, trimmed);

        // Read response
        const response_len = try stream.read(&read_buffer);
        if (response_len == 0) {
            std.debug.print("Connection closed by server\n", .{});
            break;
        }

        // Display response
        try displayResponse(read_buffer[0..response_len]);
    }

    std.debug.print("Bye!\n", .{});
}

fn sendCommand(stream: net.Stream, cmd: []const u8) !void {
    // Parse command into parts
    var parts = std.ArrayList([]const u8){};
    defer parts.deinit(std.heap.page_allocator);

    var it = std.mem.tokenizeAny(u8, cmd, " \t");
    while (it.next()) |part| {
        try parts.append(std.heap.page_allocator, part);
    }

    if (parts.items.len == 0) return;

    // Build RESP array
    var buffer: [4096]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buffer);
    const writer = fbs.writer();

    // Write array header
    try writer.print("*{d}\r\n", .{parts.items.len});

    // Write each part as bulk string
    for (parts.items) |part| {
        try writer.print("${d}\r\n{s}\r\n", .{ part.len, part });
    }

    // Send to server
    try stream.writeAll(fbs.getWritten());
}

fn displayResponse(data: []const u8) !void {
    var pos: usize = 0;
    while (pos < data.len) {
        const result = try parseRespValue(data, pos);
        try printRespValue(result.value, 0);
        pos = result.next_pos;
    }
}

const RespValue = union(enum) {
    simple_string: []const u8,
    error_msg: []const u8,
    integer: i64,
    bulk_string: ?[]const u8,
    array: []RespValue,
};

const ParseResult = struct {
    value: RespValue,
    next_pos: usize,
};

fn parseRespValue(data: []const u8, start: usize) !ParseResult {
    if (start >= data.len) return error.UnexpectedEndOfData;

    const type_byte = data[start];
    var pos = start + 1;

    switch (type_byte) {
        '+' => {
            // Simple string
            const line_end = std.mem.indexOfPos(u8, data, pos, "\r\n") orelse return error.InvalidFormat;
            const value = data[pos..line_end];
            return .{
                .value = .{ .simple_string = value },
                .next_pos = line_end + 2,
            };
        },
        '-' => {
            // Error
            const line_end = std.mem.indexOfPos(u8, data, pos, "\r\n") orelse return error.InvalidFormat;
            const value = data[pos..line_end];
            return .{
                .value = .{ .error_msg = value },
                .next_pos = line_end + 2,
            };
        },
        ':' => {
            // Integer
            const line_end = std.mem.indexOfPos(u8, data, pos, "\r\n") orelse return error.InvalidFormat;
            const num_str = data[pos..line_end];
            const num = try std.fmt.parseInt(i64, num_str, 10);
            return .{
                .value = .{ .integer = num },
                .next_pos = line_end + 2,
            };
        },
        '$' => {
            // Bulk string
            const line_end = std.mem.indexOfPos(u8, data, pos, "\r\n") orelse return error.InvalidFormat;
            const len_str = data[pos..line_end];
            const len = try std.fmt.parseInt(i64, len_str, 10);

            pos = line_end + 2;

            if (len == -1) {
                return .{
                    .value = .{ .bulk_string = null },
                    .next_pos = pos,
                };
            }

            const ulen = @as(usize, @intCast(len));
            if (pos + ulen + 2 > data.len) return error.UnexpectedEndOfData;

            const value = data[pos .. pos + ulen];
            return .{
                .value = .{ .bulk_string = value },
                .next_pos = pos + ulen + 2,
            };
        },
        '*' => {
            // Array
            const line_end = std.mem.indexOfPos(u8, data, pos, "\r\n") orelse return error.InvalidFormat;
            const count_str = data[pos..line_end];
            const count = try std.fmt.parseInt(i64, count_str, 10);

            pos = line_end + 2;

            if (count == -1) {
                return .{
                    .value = .{ .array = &[_]RespValue{} },
                    .next_pos = pos,
                };
            }

            const ucount = @as(usize, @intCast(count));
            var elements = try std.heap.page_allocator.alloc(RespValue, ucount);

            var i: usize = 0;
            while (i < ucount) : (i += 1) {
                const result = try parseRespValue(data, pos);
                elements[i] = result.value;
                pos = result.next_pos;
            }

            return .{
                .value = .{ .array = elements },
                .next_pos = pos,
            };
        },
        else => return error.InvalidFormat,
    }
}

fn printRespValue(value: RespValue, indent: usize) !void {
    switch (value) {
        .simple_string => |s| std.debug.print("{s}\n", .{s}),
        .error_msg => |e| std.debug.print("(error) {s}\n", .{e}),
        .integer => |i| std.debug.print("(integer) {d}\n", .{i}),
        .bulk_string => |maybe_s| {
            if (maybe_s) |s| {
                std.debug.print("\"{s}\"\n", .{s});
            } else {
                std.debug.print("(nil)\n", .{});
            }
        },
        .array => |arr| {
            if (arr.len == 0) {
                std.debug.print("(empty array)\n", .{});
                return;
            }

            var i: usize = 0;
            while (i < arr.len) : (i += 1) {
                var j: usize = 0;
                while (j < indent) : (j += 1) {
                    std.debug.print(" ", .{});
                }
                std.debug.print("{d}) ", .{i + 1});

                // For nested values, increase indent
                switch (arr[i]) {
                    .array => try printRespValue(arr[i], indent + 3),
                    else => try printRespValue(arr[i], 0),
                }
            }
        },
    }
}
