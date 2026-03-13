const std = @import("std");
const Storage = @import("../storage/memory.zig").Storage;
const Writer = @import("../protocol/writer.zig").Writer;
const LatencyMonitor = @import("../storage/latency.zig").LatencyMonitor;
const EventType = @import("../storage/latency.zig").EventType;

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

/// LATENCY LATEST - Get latest latency samples for all event types
pub fn cmdLatencyLatest(
    allocator: std.mem.Allocator,
    storage: *Storage,
) ![]const u8 {
    const latest = try storage.latency_monitor.getAllLatest(allocator);
    defer allocator.free(latest);

    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    const w = buf.writer(allocator);

    try w.print("*{d}\r\n", .{latest.len});

    for (latest) |entry| {
        try w.writeAll("*2\r\n");

        // Event name
        const event_name = entry.event.toString();
        try w.print("${d}\r\n{s}\r\n", .{ event_name.len, event_name });

        // Event details array: [timestamp_ms, latency_us]
        try w.writeAll("*2\r\n");
        try w.print(":{d}\r\n", .{entry.sample.timestamp});
        try w.print(":{d}\r\n", .{entry.sample.latency});
    }

    return buf.toOwnedSlice(allocator);
}

/// LATENCY HISTORY - Get latency history for a specific event
pub fn cmdLatencyHistory(
    allocator: std.mem.Allocator,
    storage: *Storage,
    event_name: []const u8,
) ![]const u8 {
    const event_type = EventType.fromString(event_name) orelse {
        return std.fmt.allocPrint(allocator, "-ERR Invalid event type\r\n", .{});
    };

    const history = try storage.latency_monitor.getHistory(event_type, allocator);
    defer if (history) |h| allocator.free(h);

    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    const w = buf.writer(allocator);

    if (history) |h| {
        try w.print("*{d}\r\n", .{h.len});
        for (h) |sample| {
            try w.writeAll("*2\r\n");
            try w.print(":{d}\r\n", .{sample.timestamp});
            try w.print(":{d}\r\n", .{sample.latency});
        }
    } else {
        try w.writeAll("*0\r\n");
    }

    return buf.toOwnedSlice(allocator);
}

/// LATENCY RESET - Reset latency events
pub fn cmdLatencyReset(
    allocator: std.mem.Allocator,
    storage: *Storage,
    event_names: []const []const u8,
) ![]const u8 {
    var reset_count: i64 = 0;

    if (event_names.len == 0) {
        // Reset all events
        storage.latency_monitor.resetAll();
        reset_count = 11; // All event types
    } else {
        // Reset specific events
        for (event_names) |event_name| {
            const event_type = EventType.fromString(event_name) orelse continue;
            if (storage.latency_monitor.resetEvent(event_type)) {
                reset_count += 1;
            }
        }
    }

    return std.fmt.allocPrint(allocator, ":{d}\r\n", .{reset_count});
}

/// LATENCY GRAPH - Generate ASCII art latency graph
pub fn cmdLatencyGraph(
    allocator: std.mem.Allocator,
    storage: *Storage,
    event_name: []const u8,
) ![]const u8 {
    const event_type = EventType.fromString(event_name) orelse {
        return std.fmt.allocPrint(allocator, "-ERR Invalid event type\r\n", .{});
    };

    const history = try storage.latency_monitor.getHistory(event_type, allocator);
    defer if (history) |h| allocator.free(h);

    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    const w = buf.writer(allocator);

    if (history == null or history.?.len == 0) {
        const msg = "No latency data available for this event";
        try w.print("${d}\r\n{s}\r\n", .{ msg.len, msg });
        return buf.toOwnedSlice(allocator);
    }

    const h = history.?;

    // Find max latency for scaling
    var max_latency: u32 = 0;
    for (h) |sample| {
        if (sample.latency > max_latency) {
            max_latency = sample.latency;
        }
    }

    // Generate simple ASCII graph
    var graph = std.ArrayList(u8){};
    defer graph.deinit(allocator);

    const graph_w = graph.writer(allocator);
    try graph_w.print("{s}\n", .{event_name});
    try graph_w.print("Max latency: {d} us\n", .{max_latency});
    try graph_w.print("Samples: {d}\n\n", .{h.len});

    // Simple bar chart (last 40 samples)
    const start_idx = if (h.len > 40) h.len - 40 else 0;
    for (h[start_idx..]) |sample| {
        const bar_len = if (max_latency > 0) (sample.latency * 50) / max_latency else 0;
        var i: u32 = 0;
        while (i < bar_len) : (i += 1) {
            try graph_w.writeAll("#");
        }
        try graph_w.print(" {d}us\n", .{sample.latency});
    }

    const graph_str = try graph.toOwnedSlice(allocator);
    defer allocator.free(graph_str);

    try w.print("${d}\r\n{s}\r\n", .{ graph_str.len, graph_str });
    return buf.toOwnedSlice(allocator);
}

/// LATENCY HISTOGRAM - Get per-command latency histogram
pub fn cmdLatencyHistogram(
    allocator: std.mem.Allocator,
    storage: *Storage,
    command_names: []const []const u8,
) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    const w = buf.writer(allocator);

    if (command_names.len == 0) {
        // Return all command histograms
        const commands = try storage.latency_monitor.getAllCommands(allocator);
        defer allocator.free(commands);

        try w.print("*{d}\r\n", .{commands.len});

        for (commands) |cmd| {
            const hist = storage.latency_monitor.getHistogram(cmd) orelse continue;

            try w.writeAll("*2\r\n");
            try w.print("${d}\r\n{s}\r\n", .{ cmd.len, cmd });

            // Histogram data as map
            try w.print("*{d}\r\n", .{hist.buckets.len * 2});
            for (hist.buckets) |bucket| {
                const key = if (bucket.latency_usec == std.math.maxInt(u32)) "inf" else try std.fmt.allocPrint(allocator, "{d}", .{bucket.latency_usec});
                defer if (bucket.latency_usec != std.math.maxInt(u32)) allocator.free(key);

                try w.print("${d}\r\n{s}\r\n", .{ key.len, key });
                try w.print(":{d}\r\n", .{bucket.count});
            }
        }
    } else {
        // Return specific command histograms
        try w.print("*{d}\r\n", .{command_names.len});

        for (command_names) |cmd| {
            const hist = storage.latency_monitor.getHistogram(cmd);

            if (hist) |h| {
                try w.writeAll("*2\r\n");
                try w.print("${d}\r\n{s}\r\n", .{ cmd.len, cmd });

                try w.print("*{d}\r\n", .{h.buckets.len * 2});
                for (h.buckets) |bucket| {
                    const key = if (bucket.latency_usec == std.math.maxInt(u32)) "inf" else try std.fmt.allocPrint(allocator, "{d}", .{bucket.latency_usec});
                    defer if (bucket.latency_usec != std.math.maxInt(u32)) allocator.free(key);

                    try w.print("${d}\r\n{s}\r\n", .{ key.len, key });
                    try w.print(":{d}\r\n", .{bucket.count});
                }
            } else {
                try w.writeAll("*0\r\n");
            }
        }
    }

    return buf.toOwnedSlice(allocator);
}

/// LATENCY DOCTOR - Provide automated latency analysis
pub fn cmdLatencyDoctor(
    allocator: std.mem.Allocator,
    storage: *Storage,
) ![]const u8 {
    const latest = try storage.latency_monitor.getAllLatest(allocator);
    defer allocator.free(latest);

    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    const w = buf.writer(allocator);

    try w.writeAll("Latency Analysis Report:\n\n");

    if (latest.len == 0) {
        try w.writeAll("No latency events recorded.\n");
    } else {
        var high_latency_count: usize = 0;

        for (latest) |entry| {
            const event_name = entry.event.toString();
            const latency_ms = entry.sample.latency / 1000;

            // Check for high latency (>10ms)
            if (latency_ms > 10) {
                high_latency_count += 1;
                try w.print("WARNING: High latency detected in {s}: {d}ms\n", .{ event_name, latency_ms });
            }
        }

        if (high_latency_count == 0) {
            try w.writeAll("All latency events are within acceptable ranges.\n");
        } else {
            try w.print("\nRecommendations:\n", .{});
            try w.writeAll("- Check for slow commands in SLOWLOG\n");
            try w.writeAll("- Consider enabling AOF less frequently\n");
            try w.writeAll("- Monitor system CPU and I/O usage\n");
        }
    }

    const report = try buf.toOwnedSlice(allocator);
    defer allocator.free(report);

    return std.fmt.allocPrint(allocator, "${d}\r\n{s}\r\n", .{ report.len, report });
}

/// LATENCY HELP - Show LATENCY command help
pub fn cmdLatencyHelp(allocator: std.mem.Allocator) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    const help = [_][]const u8{
        "LATENCY LATEST - Get latest latency samples for all events",
        "LATENCY HISTORY <event> - Get latency history for specific event",
        "LATENCY RESET [event ...] - Reset latency data",
        "LATENCY GRAPH <event> - Generate ASCII art latency graph",
        "LATENCY HISTOGRAM [command ...] - Get per-command latency histogram",
        "LATENCY DOCTOR - Get automated latency analysis",
        "LATENCY HELP - Show this help message",
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

/// LATENCY command dispatcher
pub fn cmdLatency(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const []const u8,
) ![]const u8 {
    if (args.len < 1) {
        return std.fmt.allocPrint(allocator, "-ERR wrong number of arguments for 'latency' command\r\n", .{});
    }

    const subcommand = args[0];

    if (std.ascii.eqlIgnoreCase(subcommand, "latest")) {
        if (args.len != 1) {
            return std.fmt.allocPrint(allocator, "-ERR wrong number of arguments for 'latency latest'\r\n", .{});
        }
        return cmdLatencyLatest(allocator, storage);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "history")) {
        if (args.len != 2) {
            return std.fmt.allocPrint(allocator, "-ERR wrong number of arguments for 'latency history'\r\n", .{});
        }
        return cmdLatencyHistory(allocator, storage, args[1]);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "reset")) {
        const events = if (args.len > 1) args[1..] else &[_][]const u8{};
        return cmdLatencyReset(allocator, storage, events);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "graph")) {
        if (args.len != 2) {
            return std.fmt.allocPrint(allocator, "-ERR wrong number of arguments for 'latency graph'\r\n", .{});
        }
        return cmdLatencyGraph(allocator, storage, args[1]);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "histogram")) {
        const commands = if (args.len > 1) args[1..] else &[_][]const u8{};
        return cmdLatencyHistogram(allocator, storage, commands);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "doctor")) {
        if (args.len != 1) {
            return std.fmt.allocPrint(allocator, "-ERR wrong number of arguments for 'latency doctor'\r\n", .{});
        }
        return cmdLatencyDoctor(allocator, storage);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "help")) {
        return cmdLatencyHelp(allocator);
    } else {
        return std.fmt.allocPrint(allocator, "-ERR unknown LATENCY subcommand '{s}'\r\n", .{subcommand});
    }
}

// Unit tests for LATENCY commands
test "cmdLatencyLatest with no events" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    const result = try cmdLatencyLatest(std.testing.allocator, &storage);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*0\r\n") != null);
}

test "cmdLatencyLatest with events" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    try storage.latency_monitor.recordEvent(.command, 1000);
    try storage.latency_monitor.recordEvent(.fork, 5000);

    const result = try cmdLatencyLatest(std.testing.allocator, &storage);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
}

test "cmdLatencyHistory" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    try storage.latency_monitor.recordEvent(.aof_write, 500);
    try storage.latency_monitor.recordEvent(.aof_write, 1500);

    const result = try cmdLatencyHistory(std.testing.allocator, &storage, "aof-write");
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":500\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":1500\r\n") != null);
}

test "cmdLatencyReset specific event" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    try storage.latency_monitor.recordEvent(.command, 1000);

    const events = [_][]const u8{"command"};
    const result = try cmdLatencyReset(std.testing.allocator, &storage, &events);
    defer std.testing.allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);

    // Verify reset
    const latest = try cmdLatencyLatest(std.testing.allocator, &storage);
    defer std.testing.allocator.free(latest);
    try std.testing.expect(std.mem.indexOf(u8, latest, "*0\r\n") != null);
}

test "cmdLatencyReset all events" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    try storage.latency_monitor.recordEvent(.command, 1000);
    try storage.latency_monitor.recordEvent(.fork, 5000);

    const result = try cmdLatencyReset(std.testing.allocator, &storage, &[_][]const u8{});
    defer std.testing.allocator.free(result);

    try std.testing.expectEqualStrings(":11\r\n", result);
}

test "cmdLatencyGraph" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    try storage.latency_monitor.recordEvent(.fork, 5000);

    const result = try cmdLatencyGraph(std.testing.allocator, &storage, "fork");
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "fork") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "5000us") != null);
}

test "cmdLatencyHistogram" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    try storage.latency_monitor.recordCommandLatency("GET", 100);
    try storage.latency_monitor.recordCommandLatency("SET", 200);

    const result = try cmdLatencyHistogram(std.testing.allocator, &storage, &[_][]const u8{});
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "GET") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "SET") != null);
}

test "cmdLatencyDoctor" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    const result = try cmdLatencyDoctor(std.testing.allocator, &storage);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "Latency Analysis") != null);
}

test "cmdLatencyHelp" {
    const result = try cmdLatencyHelp(std.testing.allocator);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "LATENCY LATEST") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "LATENCY HELP") != null);
}
