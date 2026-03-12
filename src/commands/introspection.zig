const std = @import("std");
const Storage = @import("../storage/memory.zig").Storage;
const Writer = @import("../protocol/writer.zig").Writer;

/// MEMORY STATS - Return memory usage statistics (stub implementation)
pub fn cmdMemoryStats(
    allocator: std.mem.Allocator,
    storage: *Storage,
) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    const w = buf.writer(allocator);

    try w.writeAll("$");
    try w.print("{d}", .{200}); // Approximate length
    try w.writeAll("\r\n");
    try w.print("peak.allocated:0\r\n", .{});
    try w.print("total.allocated:0\r\n", .{});
    try w.print("startup.allocated:16384\r\n", .{});
    try w.print("replication.backlog:0\r\n", .{});
    try w.print("clients.slaves:0\r\n", .{});
    try w.print("clients.normal:1\r\n", .{});
    try w.print("aof.buffer:0\r\n", .{});
    try w.print("keys.count:{d}\r\n", .{storage.dbSize()});

    return buf.toOwnedSlice(allocator);
}

/// MEMORY USAGE - Estimate memory usage of a key (stub implementation)
pub fn cmdMemoryUsage(
    allocator: std.mem.Allocator,
    storage: *Storage,
    key: []const u8,
) ![]const u8 {
    const value = storage.data.get(key) orelse {
        return std.fmt.allocPrint(allocator, "$-1\r\n", .{});
    };

    // Rough estimate based on value type
    const estimated_size: i64 = switch (value) {
        .string => |s| @intCast(s.data.len + 50),
        .list => |l| @intCast(l.data.items.len * 50 + 100),
        .set => |s| @intCast(s.data.count() * 50 + 100),
        .hash => |h| @intCast(h.data.count() * 100 + 100),
        .sorted_set => |zs| @intCast(zs.members.count() * 100 + 100),
        .stream => |s| @intCast(s.entries.items.len * 200 + 100),
        .hyperloglog => 12304, // Fixed size for HLL
    };

    return std.fmt.allocPrint(allocator, ":{d}\r\n", .{estimated_size});
}

/// MEMORY DOCTOR - Return memory usage advice (stub)
pub fn cmdMemoryDoctor(allocator: std.mem.Allocator) ![]const u8 {
    const advice = "Sam, I'm sorry, but I can't help you with this. Your memory is fine.";
    return std.fmt.allocPrint(allocator, "${d}\r\n{s}\r\n", .{ advice.len, advice });
}

/// MEMORY HELP - Return MEMORY command help
pub fn cmdMemoryHelp(allocator: std.mem.Allocator) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    const help = [_][]const u8{
        "MEMORY STATS - Show memory usage statistics",
        "MEMORY USAGE <key> - Estimate memory usage of a key",
        "MEMORY DOCTOR - Get memory usage advice",
        "MEMORY HELP - Show this help message",
    };

    try buf.appendSlice(allocator, "*");
    try std.fmt.format(buf.writer(allocator), "{d}", .{help.len});
    try buf.appendSlice(allocator, "\r\n");

    for (help) |line| {
        try buf.appendSlice(allocator, "$");
        try std.fmt.format(buf.writer(allocator), "{d}", .{line.len});
        try buf.appendSlice(allocator, "\r\n");
        try buf.appendSlice(allocator, line);
        try buf.appendSlice(allocator, "\r\n");
    }

    return buf.toOwnedSlice(allocator);
}

/// MEMORY command dispatcher
pub fn cmdMemory(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const []const u8,
) ![]const u8 {
    if (args.len < 1) {
        return std.fmt.allocPrint(allocator, "-ERR wrong number of arguments for 'memory' command\r\n", .{});
    }

    const subcommand = args[0];

    if (std.ascii.eqlIgnoreCase(subcommand, "stats")) {
        return cmdMemoryStats(allocator, storage);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "usage")) {
        if (args.len < 2) {
            return std.fmt.allocPrint(allocator, "-ERR wrong number of arguments for 'memory usage' command\r\n", .{});
        }
        return cmdMemoryUsage(allocator, storage, args[1]);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "doctor")) {
        return cmdMemoryDoctor(allocator);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "help")) {
        return cmdMemoryHelp(allocator);
    } else {
        return std.fmt.allocPrint(allocator, "-ERR unknown subcommand for 'memory' command. Try MEMORY HELP.\r\n", .{});
    }
}

/// SLOWLOG GET - Get slow log entries
pub fn cmdSlowlogGet(allocator: std.mem.Allocator, storage: *Storage, count: ?usize) ![]const u8 {
    const entries = storage.slowlog.getEntries(count);

    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    const w = buf.writer(allocator);

    // Redis returns entries in reverse chronological order (most recent first)
    try w.print("*{d}\r\n", .{entries.len});

    // Iterate in reverse to show most recent first
    var i: usize = entries.len;
    while (i > 0) {
        i -= 1;
        const entry = entries[i];

        // Each entry is an array: [id, timestamp, duration_us, command_array, client_addr, client_name]
        try w.writeAll("*6\r\n");

        // ID
        try w.print(":{d}\r\n", .{entry.id});

        // Timestamp (Unix seconds)
        try w.print(":{d}\r\n", .{@divTrunc(entry.timestamp, 1_000_000)});

        // Duration (microseconds)
        try w.print(":{d}\r\n", .{entry.duration_us});

        // Command (as array of strings) - split by spaces
        var cmd_parts = std.ArrayList([]const u8){};
        defer cmd_parts.deinit(allocator);

        var iter = std.mem.splitSequence(u8, entry.command, " ");
        while (iter.next()) |part| {
            if (part.len > 0) {
                try cmd_parts.append(allocator, part);
            }
        }

        try w.print("*{d}\r\n", .{cmd_parts.items.len});
        for (cmd_parts.items) |part| {
            try w.print("${d}\r\n{s}\r\n", .{ part.len, part });
        }

        // Client IP:port
        try w.print("${d}\r\n{s}\r\n", .{ entry.client_addr.len, entry.client_addr });

        // Client name
        try w.print("${d}\r\n{s}\r\n", .{ entry.client_name.len, entry.client_name });
    }

    return buf.toOwnedSlice(allocator);
}

/// SLOWLOG LEN - Get slow log length
pub fn cmdSlowlogLen(allocator: std.mem.Allocator, storage: *Storage) ![]const u8 {
    const length = storage.slowlog.len();
    return std.fmt.allocPrint(allocator, ":{d}\r\n", .{length});
}

/// SLOWLOG RESET - Reset slow log
pub fn cmdSlowlogReset(allocator: std.mem.Allocator, storage: *Storage) ![]const u8 {
    storage.slowlog.reset();
    return std.fmt.allocPrint(allocator, "+OK\r\n", .{});
}

/// SLOWLOG HELP - Return SLOWLOG command help
pub fn cmdSlowlogHelp(allocator: std.mem.Allocator) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    const help = [_][]const u8{
        "SLOWLOG GET [count] - Get the slow log entries",
        "SLOWLOG LEN - Get the length of the slow log",
        "SLOWLOG RESET - Clear the slow log",
        "SLOWLOG HELP - Show this help message",
    };

    try buf.appendSlice(allocator, "*");
    try std.fmt.format(buf.writer(allocator), "{d}", .{help.len});
    try buf.appendSlice(allocator, "\r\n");

    for (help) |line| {
        try buf.appendSlice(allocator, "$");
        try std.fmt.format(buf.writer(allocator), "{d}", .{line.len});
        try buf.appendSlice(allocator, "\r\n");
        try buf.appendSlice(allocator, line);
        try buf.appendSlice(allocator, "\r\n");
    }

    return buf.toOwnedSlice(allocator);
}

/// SLOWLOG command dispatcher
pub fn cmdSlowlog(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const []const u8,
) ![]const u8 {
    if (args.len < 1) {
        return std.fmt.allocPrint(allocator, "-ERR wrong number of arguments for 'slowlog' command\r\n", .{});
    }

    const subcommand = args[0];

    if (std.ascii.eqlIgnoreCase(subcommand, "get")) {
        var count: ?usize = null;
        if (args.len > 1) {
            count = std.fmt.parseInt(usize, args[1], 10) catch {
                return std.fmt.allocPrint(allocator, "-ERR value is not an integer or out of range\r\n", .{});
            };
        }
        return cmdSlowlogGet(allocator, storage, count);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "len")) {
        return cmdSlowlogLen(allocator, storage);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "reset")) {
        return cmdSlowlogReset(allocator, storage);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "help")) {
        return cmdSlowlogHelp(allocator);
    } else {
        return std.fmt.allocPrint(allocator, "-ERR unknown subcommand for 'slowlog' command. Try SLOWLOG HELP.\r\n", .{});
    }
}

// Unit tests
test "MEMORY STATS" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try cmdMemoryStats(allocator, &storage);
    defer allocator.free(result);

    try std.testing.expect(result.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, result, "keys.count") != null);
}

test "MEMORY USAGE" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set a value first
    _ = try storage.set("testkey", "testvalue", null);

    const result = try cmdMemoryUsage(allocator, &storage, "testkey");
    defer allocator.free(result);

    try std.testing.expect(result.len > 0);
    try std.testing.expect(std.mem.startsWith(u8, result, ":"));
}

test "SLOWLOG GET empty" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try cmdSlowlogGet(allocator, storage, null);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("*0\r\n", result);
}

test "SLOWLOG LEN" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try cmdSlowlogLen(allocator, storage);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "SLOWLOG RESET" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try cmdSlowlogReset(allocator, storage);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "SLOWLOG integration" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Add slow entries directly
    _ = try storage.slowlog.logCommand(15000, "GET key1", "127.0.0.1:12345", "client1");
    _ = try storage.slowlog.logCommand(20000, "SET key2 value2", "127.0.0.1:12346", "");

    // Test LEN
    const len_result = try cmdSlowlogLen(allocator, storage);
    defer allocator.free(len_result);
    try std.testing.expectEqualStrings(":2\r\n", len_result);

    // Test GET
    const get_result = try cmdSlowlogGet(allocator, storage, null);
    defer allocator.free(get_result);
    try std.testing.expect(std.mem.indexOf(u8, get_result, "GET key1") != null);
    try std.testing.expect(std.mem.indexOf(u8, get_result, "SET key2 value2") != null);

    // Test GET with count
    const get_one = try cmdSlowlogGet(allocator, storage, 1);
    defer allocator.free(get_one);
    try std.testing.expect(std.mem.indexOf(u8, get_one, "*1\r\n") != null);

    // Test RESET
    const reset_result = try cmdSlowlogReset(allocator, storage);
    defer allocator.free(reset_result);
    try std.testing.expectEqualStrings("+OK\r\n", reset_result);

    // Verify empty after reset
    const len_after = try cmdSlowlogLen(allocator, storage);
    defer allocator.free(len_after);
    try std.testing.expectEqualStrings(":0\r\n", len_after);
}
