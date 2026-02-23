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

/// SLOWLOG GET - Get slow log entries (stub - always returns empty)
pub fn cmdSlowlogGet(allocator: std.mem.Allocator) ![]const u8 {
    return std.fmt.allocPrint(allocator, "*0\r\n", .{});
}

/// SLOWLOG LEN - Get slow log length (stub - always returns 0)
pub fn cmdSlowlogLen(allocator: std.mem.Allocator) ![]const u8 {
    return std.fmt.allocPrint(allocator, ":0\r\n", .{});
}

/// SLOWLOG RESET - Reset slow log (stub - always returns OK)
pub fn cmdSlowlogReset(allocator: std.mem.Allocator) ![]const u8 {
    return std.fmt.allocPrint(allocator, "+OK\r\n", .{});
}

/// SLOWLOG stub command dispatcher
pub fn cmdSlowlogStub(
    allocator: std.mem.Allocator,
    args: []const []const u8,
) ![]const u8 {
    if (args.len < 1) {
        return std.fmt.allocPrint(allocator, "-ERR wrong number of arguments for 'slowlog' command\r\n", .{});
    }

    const subcommand = args[0];

    if (std.ascii.eqlIgnoreCase(subcommand, "get")) {
        return cmdSlowlogGet(allocator);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "len")) {
        return cmdSlowlogLen(allocator);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "reset")) {
        return cmdSlowlogReset(allocator);
    } else {
        return std.fmt.allocPrint(allocator, "-ERR unknown subcommand for 'slowlog' command. Try SLOWLOG GET, SLOWLOG LEN, or SLOWLOG RESET.\r\n", .{});
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

test "SLOWLOG stub" {
    const allocator = std.testing.allocator;

    const get_result = try cmdSlowlogGet(allocator);
    defer allocator.free(get_result);
    try std.testing.expectEqualStrings("*0\r\n", get_result);

    const len_result = try cmdSlowlogLen(allocator);
    defer allocator.free(len_result);
    try std.testing.expectEqualStrings(":0\r\n", len_result);

    const reset_result = try cmdSlowlogReset(allocator);
    defer allocator.free(reset_result);
    try std.testing.expectEqualStrings("+OK\r\n", reset_result);
}
