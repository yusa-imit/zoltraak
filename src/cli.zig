const std = @import("std");
const sailor = @import("sailor");
const net = std.net;
const tui = sailor.tui;
const tui_advanced = @import("tui_advanced.zig");

// Output format enum
const OutputFormat = enum {
    normal,
    raw,
    csv,
    json,

    pub fn fromString(s: []const u8) ?OutputFormat {
        if (std.mem.eql(u8, s, "normal")) return .normal;
        if (std.mem.eql(u8, s, "raw")) return .raw;
        if (std.mem.eql(u8, s, "csv")) return .csv;
        if (std.mem.eql(u8, s, "json")) return .json;
        return null;
    }
};

// Define CLI flags
const flags = [_]sailor.arg.FlagDef{
    .{ .name = "host", .short = 'h', .type = .string, .default = "127.0.0.1", .help = "Server hostname" },
    .{ .name = "port", .short = 'p', .type = .string, .default = "6379", .help = "Server port" },
    .{ .name = "raw", .short = 'r', .type = .bool, .default = "false", .help = "Output raw RESP data without formatting" },
    .{ .name = "csv", .short = 'c', .type = .bool, .default = "false", .help = "Output data in CSV format" },
    .{ .name = "json", .short = 'j', .type = .bool, .default = "false", .help = "Output data in JSON format" },
    .{ .name = "tui", .short = 't', .type = .bool, .default = "false", .help = "Launch TUI key browser" },
    .{ .name = "advanced", .short = 'a', .type = .bool, .default = "false", .help = "Use advanced TUI with Tree/LineChart/Dialog/Notification widgets (requires --tui)" },
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

    // Check if TUI mode is requested
    const use_tui = parser.getBool("tui", false);
    const use_advanced = parser.getBool("advanced", false);

    if (use_tui) {
        if (use_advanced) {
            return runAdvancedTuiMode(allocator, host, port);
        } else {
            return runTuiMode(allocator, host, port);
        }
    }

    // Determine output format
    var output_format = OutputFormat.normal;
    if (parser.getBool("raw", false)) {
        output_format = .raw;
    } else if (parser.getBool("csv", false)) {
        output_format = .csv;
    } else if (parser.getBool("json", false)) {
        output_format = .json;
    }

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
        try displayResponse(read_buffer[0..response_len], output_format);
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

fn displayResponse(data: []const u8, format: OutputFormat) !void {
    switch (format) {
        .raw => {
            // Raw mode: just print the RESP data as-is
            std.debug.print("{s}", .{data});
        },
        .normal => {
            // Normal mode: parse and format nicely
            var pos: usize = 0;
            while (pos < data.len) {
                const result = try parseRespValue(data, pos);
                try printRespValue(result.value, 0);
                pos = result.next_pos;
            }
        },
        .csv => {
            // CSV mode: flatten to CSV format
            var pos: usize = 0;
            while (pos < data.len) {
                const result = try parseRespValue(data, pos);
                try printRespValueCsv(result.value);
                pos = result.next_pos;
            }
        },
        .json => {
            // JSON mode: convert to JSON
            var pos: usize = 0;
            while (pos < data.len) {
                const result = try parseRespValue(data, pos);
                try printRespValueJson(result.value);
                std.debug.print("\n", .{});
                pos = result.next_pos;
            }
        },
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

fn printRespValueCsv(value: RespValue) !void {
    switch (value) {
        .simple_string => |s| std.debug.print("{s}\n", .{s}),
        .error_msg => |e| std.debug.print("ERROR,\"{s}\"\n", .{e}),
        .integer => |i| std.debug.print("{d}\n", .{i}),
        .bulk_string => |maybe_s| {
            if (maybe_s) |s| {
                // Escape quotes in CSV
                var needs_quotes = false;
                for (s) |ch| {
                    if (ch == ',' or ch == '"' or ch == '\n' or ch == '\r') {
                        needs_quotes = true;
                        break;
                    }
                }

                if (needs_quotes) {
                    std.debug.print("\"", .{});
                    for (s) |ch| {
                        if (ch == '"') {
                            std.debug.print("\"\"", .{}); // Escape quotes by doubling
                        } else {
                            std.debug.print("{c}", .{ch});
                        }
                    }
                    std.debug.print("\"\n", .{});
                } else {
                    std.debug.print("{s}\n", .{s});
                }
            } else {
                std.debug.print("\n", .{});
            }
        },
        .array => |arr| {
            var i: usize = 0;
            while (i < arr.len) : (i += 1) {
                if (i > 0) std.debug.print(",", .{});

                switch (arr[i]) {
                    .simple_string => |s| std.debug.print("{s}", .{s}),
                    .bulk_string => |maybe_s| {
                        if (maybe_s) |s| std.debug.print("\"{s}\"", .{s});
                    },
                    .integer => |n| std.debug.print("{d}", .{n}),
                    .error_msg => |e| std.debug.print("\"ERROR:{s}\"", .{e}),
                    .array => {}, // Skip nested arrays in CSV
                }
            }
            std.debug.print("\n", .{});
        },
    }
}

fn printRespValueJson(value: RespValue) error{}!void {
    switch (value) {
        .simple_string => |s| std.debug.print("\"{s}\"", .{s}),
        .error_msg => |e| {
            std.debug.print("{{\"error\":\"{s}\"}}", .{e});
        },
        .integer => |i| std.debug.print("{d}", .{i}),
        .bulk_string => |maybe_s| {
            if (maybe_s) |s| {
                std.debug.print("\"", .{});
                // Escape special characters for JSON
                for (s) |ch| {
                    switch (ch) {
                        '"' => std.debug.print("\\\"", .{}),
                        '\\' => std.debug.print("\\\\", .{}),
                        '\n' => std.debug.print("\\n", .{}),
                        '\r' => std.debug.print("\\r", .{}),
                        '\t' => std.debug.print("\\t", .{}),
                        else => std.debug.print("{c}", .{ch}),
                    }
                }
                std.debug.print("\"", .{});
            } else {
                std.debug.print("null", .{});
            }
        },
        .array => |arr| {
            std.debug.print("[", .{});
            var i: usize = 0;
            while (i < arr.len) : (i += 1) {
                if (i > 0) std.debug.print(",", .{});
                try printRespValueJson(arr[i]);
            }
            std.debug.print("]", .{});
        },
    }
}

// TUI mode implementation
fn runTuiMode(allocator: std.mem.Allocator, host: []const u8, port: u16) !void {
    // Connect to server
    const address = try net.Address.parseIp(host, port);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Initialize Terminal
    var terminal = try tui.Terminal.init(allocator);
    defer terminal.deinit();

    // Fetch initial keys
    var keys_list = std.ArrayList([]const u8){};
    defer {
        for (keys_list.items) |key| {
            allocator.free(key);
        }
        keys_list.deinit(allocator);
    }

    try fetchKeys(allocator, stream, &keys_list);

    // Main state
    var selected_index: usize = 0;
    var running = true;
    var status_msg_buf: [256]u8 = undefined;

    // Enter raw mode
    const stdout_file = std.fs.File{ .handle = std.posix.STDOUT_FILENO };
    try stdout_file.writeAll("\x1b[?1049h"); // Alt screen
    try stdout_file.writeAll("\x1b[?25l"); // Hide cursor
    defer {
        stdout_file.writeAll("\x1b[?25h") catch {}; // Show cursor
        stdout_file.writeAll("\x1b[?1049l") catch {}; // Normal screen
    }

    while (running) {
        // Clear current buffer
        terminal.clear();

        // Prepare status message
        const status_msg = try std.fmt.bufPrint(&status_msg_buf, "Connected to {s}:{} | Keys: {} | Press 'q' to quit, 'r' to refresh, j/k to navigate", .{ host, port, keys_list.items.len });

        // Create frame for the full terminal area
        const area = terminal.size();
        var frame = tui.Frame{
            .buffer = &terminal.current,
            .area = area,
        };

        // Layout: split into left (list) and right (details)
        const list_width = area.width / 3;
        const list_area = tui.Rect.new(0, 0, list_width, area.height - 1);
        const details_area = tui.Rect.new(list_width + 1, 0, area.width - list_width - 1, area.height - 1);

        // Render key list
        try renderKeyList(&frame, list_area, keys_list.items, selected_index);

        // Render key details
        if (keys_list.items.len > 0 and selected_index < keys_list.items.len) {
            const selected_key = keys_list.items[selected_index];
            try renderKeyDetails(allocator, &frame, details_area, stream, selected_key);
        }

        // Render status bar
        frame.setString(0, area.height - 1, status_msg, .{ .fg = tui.Color.black, .bg = tui.Color.white });

        // Flush to screen
        try flushBuffer(&terminal.current, stdout_file);

        // Handle input (blocking read for simplicity)
        const key = try readKey();
        switch (key) {
            'q' => running = false,
            'r' => {
                // Refresh keys
                for (keys_list.items) |k| {
                    allocator.free(k);
                }
                keys_list.clearRetainingCapacity();
                try fetchKeys(allocator, stream, &keys_list);
                selected_index = 0;
            },
            'j' => {
                if (selected_index < keys_list.items.len -| 1) {
                    selected_index += 1;
                }
            },
            'k' => {
                if (selected_index > 0) {
                    selected_index -= 1;
                }
            },
            else => {},
        }
    }
}

fn renderKeyList(frame: *tui.Frame, area: tui.Rect, keys: []const []const u8, selected: usize) !void {
    // Draw border
    const border_style = tui.Style{ .fg = tui.Color.cyan };
    frame.setString(0, 0, "┌", border_style);
    frame.setString(area.width - 1, 0, "┐", border_style);
    var x: u16 = 1;
    while (x < area.width - 1) : (x += 1) {
        frame.setString(x, 0, "─", border_style);
    }

    // Title
    const title = " Keys ";
    frame.setString(2, 0, title, .{ .fg = tui.Color.yellow, .bold = true });

    // List items
    var y: u16 = 1;
    const max_items = @min(keys.len, area.height - 2);
    for (keys[0..max_items], 0..) |key, idx| {
        const is_selected = (idx == selected);
        const style: tui.Style = if (is_selected)
            .{ .fg = tui.Color.black, .bg = tui.Color.cyan, .bold = true }
        else
            .{ .fg = tui.Color.white };

        var key_display: [64]u8 = undefined;
        const key_text = if (key.len > area.width - 3)
            try std.fmt.bufPrint(&key_display, "{s}...", .{key[0 .. area.width - 6]})
        else
            key;

        frame.setString(1, y, key_text, style);
        y += 1;
    }

    // Bottom border
    frame.setString(0, area.height - 1, "└", border_style);
    frame.setString(area.width - 1, area.height - 1, "┘", border_style);
    x = 1;
    while (x < area.width - 1) : (x += 1) {
        frame.setString(x, area.height - 1, "─", border_style);
    }

    // Side borders
    y = 1;
    while (y < area.height - 1) : (y += 1) {
        frame.setString(0, y, "│", border_style);
        frame.setString(area.width - 1, y, "│", border_style);
    }
}

fn renderKeyDetails(allocator: std.mem.Allocator, frame: *tui.Frame, area: tui.Rect, stream: net.Stream, key: []const u8) !void {
    // Draw border
    const border_style = tui.Style{ .fg = tui.Color.cyan };
    frame.setString(0, 0, "┌", border_style);
    frame.setString(area.width - 1, 0, "┐", border_style);
    var x: u16 = 1;
    while (x < area.width - 1) : (x += 1) {
        frame.setString(x, 0, "─", border_style);
    }

    // Title
    const title = " Details ";
    frame.setString(2, 0, title, .{ .fg = tui.Color.yellow, .bold = true });

    // Fetch and display key info
    var y: u16 = 1;

    // Key name
    frame.setString(1, y, "Key: ", .{ .fg = tui.Color.green, .bold = true });
    frame.setString(6, y, key, .{ .fg = tui.Color.white });
    y += 1;

    // Type
    const key_type = try fetchKeyType(allocator, stream, key);
    defer allocator.free(key_type);
    frame.setString(1, y, "Type: ", .{ .fg = tui.Color.green, .bold = true });
    frame.setString(7, y, key_type, .{ .fg = tui.Color.white });
    y += 1;

    // TTL
    const ttl = try fetchKeyTtl(allocator, stream, key);
    defer allocator.free(ttl);
    frame.setString(1, y, "TTL: ", .{ .fg = tui.Color.green, .bold = true });
    frame.setString(6, y, ttl, .{ .fg = tui.Color.white });
    y += 1;

    // Value
    const value = try fetchKeyValue(allocator, stream, key, key_type);
    defer allocator.free(value);
    frame.setString(1, y, "Value: ", .{ .fg = tui.Color.green, .bold = true });
    y += 1;

    // Display value (potentially multi-line)
    var line_iter = std.mem.splitSequence(u8, value, "\n");
    while (line_iter.next()) |line| {
        if (y >= area.height - 1) break;
        const display_line = if (line.len > area.width - 3) line[0 .. area.width - 3] else line;
        frame.setString(2, y, display_line, .{ .fg = tui.Color.white });
        y += 1;
    }

    // Borders
    frame.setString(0, area.height - 1, "└", border_style);
    frame.setString(area.width - 1, area.height - 1, "┘", border_style);
    x = 1;
    while (x < area.width - 1) : (x += 1) {
        frame.setString(x, area.height - 1, "─", border_style);
    }
    y = 1;
    while (y < area.height - 1) : (y += 1) {
        frame.setString(0, y, "│", border_style);
        frame.setString(area.width - 1, y, "│", border_style);
    }
}

fn flushBuffer(buffer: *const tui.Buffer, writer: std.fs.File) !void {
    var buf: [8192]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buf);
    const w = stream.writer();

    // Move cursor to top-left
    try w.writeAll("\x1b[H");

    // Write buffer content
    var y: u16 = 0;
    while (y < buffer.height) : (y += 1) {
        var x: u16 = 0;
        while (x < buffer.width) : (x += 1) {
            if (buffer.getConst(x, y)) |cell| {
                // Apply style
                if (cell.style.fg) |fg| {
                    const fg_code = @intFromEnum(fg);
                    try w.print("\x1b[38;5;{}m", .{fg_code});
                }
                if (cell.style.bg) |bg| {
                    const bg_code = @intFromEnum(bg);
                    try w.print("\x1b[48;5;{}m", .{bg_code});
                }
                if (cell.style.bold) try w.writeAll("\x1b[1m");

                // Write character
                var utf8_buf: [4]u8 = undefined;
                const len = std.unicode.utf8Encode(cell.char, &utf8_buf) catch 1;
                try w.writeAll(utf8_buf[0..len]);

                // Reset style
                try w.writeAll("\x1b[0m");
            }
        }
        if (y < buffer.height - 1) {
            try w.writeAll("\r\n");
        }
    }

    try writer.writeAll(stream.getWritten());
}

fn readKey() !u8 {
    var ch: [1]u8 = undefined;
    const n = try std.posix.read(std.posix.STDIN_FILENO, &ch);
    if (n == 0) return 'q'; // EOF
    return ch[0];
}

fn fetchKeys(allocator: std.mem.Allocator, stream: net.Stream, keys_list: *std.ArrayList([]const u8)) !void {
    // Send KEYS * command
    try sendCommand(stream, "KEYS *");

    // Read response
    var read_buffer: [65536]u8 = undefined;
    const response_len = try stream.read(&read_buffer);
    if (response_len == 0) return error.ConnectionClosed;

    // Parse response
    const result = try parseRespValue(read_buffer[0..response_len], 0);

    switch (result.value) {
        .array => |arr| {
            for (arr) |item| {
                switch (item) {
                    .bulk_string => |maybe_s| {
                        if (maybe_s) |s| {
                            const key_copy = try allocator.dupe(u8, s);
                            try keys_list.append(allocator, key_copy);
                        }
                    },
                    else => {},
                }
            }
        },
        else => {},
    }
}

fn fetchKeyType(allocator: std.mem.Allocator, stream: net.Stream, key: []const u8) ![]const u8 {
    var cmd_buf: [512]u8 = undefined;
    const cmd = try std.fmt.bufPrint(&cmd_buf, "TYPE {s}", .{key});
    try sendCommand(stream, cmd);

    var read_buffer: [1024]u8 = undefined;
    const response_len = try stream.read(&read_buffer);
    if (response_len == 0) return error.ConnectionClosed;

    const result = try parseRespValue(read_buffer[0..response_len], 0);

    return switch (result.value) {
        .simple_string => |s| try allocator.dupe(u8, s),
        .bulk_string => |maybe_s| if (maybe_s) |s| try allocator.dupe(u8, s) else try allocator.dupe(u8, "none"),
        else => try allocator.dupe(u8, "unknown"),
    };
}

fn fetchKeyTtl(allocator: std.mem.Allocator, stream: net.Stream, key: []const u8) ![]const u8 {
    var cmd_buf: [512]u8 = undefined;
    const cmd = try std.fmt.bufPrint(&cmd_buf, "TTL {s}", .{key});
    try sendCommand(stream, cmd);

    var read_buffer: [1024]u8 = undefined;
    const response_len = try stream.read(&read_buffer);
    if (response_len == 0) return error.ConnectionClosed;

    const result = try parseRespValue(read_buffer[0..response_len], 0);

    return switch (result.value) {
        .integer => |i| {
            if (i == -1) {
                return try allocator.dupe(u8, "no expiry");
            } else if (i == -2) {
                return try allocator.dupe(u8, "key missing");
            } else {
                var buf: [64]u8 = undefined;
                const ttl_str = try std.fmt.bufPrint(&buf, "{d}s", .{i});
                return try allocator.dupe(u8, ttl_str);
            }
        },
        else => try allocator.dupe(u8, "unknown"),
    };
}

fn fetchKeyValue(allocator: std.mem.Allocator, stream: net.Stream, key: []const u8, key_type: []const u8) ![]const u8 {
    var cmd_buf: [512]u8 = undefined;
    const cmd = if (std.mem.eql(u8, key_type, "string"))
        try std.fmt.bufPrint(&cmd_buf, "GET {s}", .{key})
    else if (std.mem.eql(u8, key_type, "list"))
        try std.fmt.bufPrint(&cmd_buf, "LRANGE {s} 0 10", .{key})
    else if (std.mem.eql(u8, key_type, "set"))
        try std.fmt.bufPrint(&cmd_buf, "SMEMBERS {s}", .{key})
    else if (std.mem.eql(u8, key_type, "hash"))
        try std.fmt.bufPrint(&cmd_buf, "HGETALL {s}", .{key})
    else if (std.mem.eql(u8, key_type, "zset"))
        try std.fmt.bufPrint(&cmd_buf, "ZRANGE {s} 0 10", .{key})
    else
        try std.fmt.bufPrint(&cmd_buf, "GET {s}", .{key});

    try sendCommand(stream, cmd);

    var read_buffer: [65536]u8 = undefined;
    const response_len = try stream.read(&read_buffer);
    if (response_len == 0) return error.ConnectionClosed;

    const result = try parseRespValue(read_buffer[0..response_len], 0);

    return switch (result.value) {
        .bulk_string => |maybe_s| if (maybe_s) |s| try allocator.dupe(u8, s) else try allocator.dupe(u8, "(nil)"),
        .simple_string => |s| try allocator.dupe(u8, s),
        .integer => |i| blk: {
            var buf: [64]u8 = undefined;
            const int_str = try std.fmt.bufPrint(&buf, "{d}", .{i});
            break :blk try allocator.dupe(u8, int_str);
        },
        .array => |arr| blk: {
            var value_buf = std.ArrayList(u8){};
            defer value_buf.deinit(allocator);
            try value_buf.appendSlice(allocator, "[");
            for (arr, 0..) |item, idx| {
                if (idx > 0) try value_buf.appendSlice(allocator, ", ");
                switch (item) {
                    .bulk_string => |maybe_s| {
                        if (maybe_s) |s| {
                            try value_buf.appendSlice(allocator, s);
                        }
                    },
                    else => {},
                }
                if (idx >= 9) {
                    try value_buf.appendSlice(allocator, "...");
                    break;
                }
            }
            try value_buf.appendSlice(allocator, "]");
            break :blk try allocator.dupe(u8, value_buf.items);
        },
        else => try allocator.dupe(u8, "(unknown)"),
    };
}
// Advanced TUI mode with sailor v0.5.0 widgets
fn runAdvancedTuiMode(allocator: std.mem.Allocator, host: []const u8, port: u16) !void {
    // Connect to server
    const address = try net.Address.parseIp(host, port);
    const stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Initialize Terminal
    var terminal = try tui.Terminal.init(allocator);
    defer terminal.deinit();

    // Initialize Dashboard
    var dashboard = try tui_advanced.Dashboard.init(allocator, &terminal, stream);
    defer dashboard.deinit();

    // Refresh data
    try dashboard.refreshKeys();
    try dashboard.refreshMemoryStats();

    // Enter raw mode
    const stdout_file = std.fs.File{ .handle = std.posix.STDOUT_FILENO };
    try stdout_file.writeAll("\x1b[?1049h"); // Alt screen
    try stdout_file.writeAll("\x1b[?25l"); // Hide cursor
    defer {
        stdout_file.writeAll("\x1b[?25h") catch {}; // Show cursor
        stdout_file.writeAll("\x1b[?1049l") catch {}; // Normal screen
    }

    var running = true;
    while (running) {
        // Clear current buffer
        terminal.clear();

        // Create frame for the full terminal area
        const area = terminal.size();
        var frame = tui.Frame{
            .buffer = &terminal.current,
            .area = area,
        };

        // Layout: 3 columns
        // Left: Tree widget (1/3)
        // Middle: LineChart (1/3)
        // Right: Status (1/3)
        const col_width = area.width / 3;
        const tree_area = tui.Rect.new(0, 0, col_width, area.height - 2);
        const chart_area = tui.Rect.new(col_width, 0, col_width, area.height - 2);
        const status_area = tui.Rect.new(col_width * 2, 0, col_width, area.height - 2);

        // Render Tree widget
        try tui_advanced.renderTree(&frame, tree_area, &dashboard.keys_tree, dashboard.selected_index);

        // Render LineChart widget
        try tui_advanced.renderLineChart(&frame, chart_area, &dashboard.memory_stats);

        // Render status
        frame.setString(status_area.x + 2, status_area.y + 1, "Status", .{ .fg = tui.Color.yellow, .bold = true });
        var status_buf: [64]u8 = undefined;
        const status = try std.fmt.bufPrint(&status_buf, "Keys: {}", .{dashboard.memory_stats.num_keys});
        frame.setString(status_area.x + 2, status_area.y + 3, status, .{ .fg = tui.Color.white });

        // Render help bar
        const help_text = "q:quit r:refresh j/k:navigate d:delete";
        frame.setString(0, area.height - 2, help_text, .{ .fg = tui.Color.black, .bg = tui.Color.white });

        // Render connection info
        var conn_buf: [128]u8 = undefined;
        const conn_info = try std.fmt.bufPrint(&conn_buf, "Connected to {s}:{}", .{ host, port });
        frame.setString(0, area.height - 1, conn_info, .{ .fg = tui.Color.green });

        // Render Dialog if active
        if (dashboard.show_delete_dialog) {
            const selected_key = dashboard.keys_tree.getSelectedKey(dashboard.selected_index) orelse "unknown";
            try tui_advanced.renderDialog(&frame, area, selected_key);
        }

        // Render Notification if active
        if (dashboard.notification_text) |text| {
            if (dashboard.notification_timer > 0) {
                try tui_advanced.renderNotification(&frame, area, text);
                dashboard.notification_timer -= 1;
            } else {
                dashboard.notification_text = null;
            }
        }

        // Flush to screen
        try flushBuffer(&terminal.current, stdout_file);

        // Handle input
        const key = try readKey();
        switch (key) {
            'q' => {
                if (dashboard.show_delete_dialog) {
                    dashboard.closeDeleteDialog();
                } else {
                    running = false;
                }
            },
            'r' => {
                try dashboard.refreshKeys();
                try dashboard.refreshMemoryStats();
                dashboard.showNotification("Data refreshed");
            },
            'j' => {
                const max_keys = dashboard.keys_tree.totalKeys();
                if (dashboard.selected_index < max_keys -| 1) {
                    dashboard.selected_index += 1;
                }
            },
            'k' => {
                if (dashboard.selected_index > 0) {
                    dashboard.selected_index -= 1;
                }
            },
            'd' => {
                if (!dashboard.show_delete_dialog) {
                    dashboard.showDeleteDialog();
                }
            },
            'y', 'Y' => {
                if (dashboard.show_delete_dialog) {
                    try dashboard.deleteSelectedKey();
                }
            },
            'n', 'N' => {
                if (dashboard.show_delete_dialog) {
                    dashboard.closeDeleteDialog();
                    dashboard.showNotification("Delete cancelled");
                }
            },
            else => {},
        }

        // Small delay for notification timer
        std.Thread.sleep(100 * std.time.ns_per_ms);
    }
}
