const std = @import("std");
const Storage = @import("../storage/memory.zig").Storage;
const Writer = @import("../protocol/writer.zig").Writer;
const LatencyMonitor = @import("../storage/latency.zig").LatencyMonitor;
const EventType = @import("../storage/latency.zig").EventType;

/// MEMORY STATS - Return memory usage statistics as flat RESP array (Redis-compatible)
/// Redis returns alternating bulk string keys and integer/bulk-string values.
pub fn cmdMemoryStats(
    allocator: std.mem.Allocator,
    storage: *Storage,
) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    const w = buf.writer(allocator);
    const tracker = &storage.memory_tracker;

    const keys_count = storage.dbSize();
    const bytes_per_key: usize = if (keys_count > 0)
        tracker.dataset_bytes / keys_count
    else
        0;

    const overhead_total = tracker.overhead_bytes +
        tracker.replication_backlog_bytes +
        tracker.aof_buffer_bytes +
        tracker.clients_normal_bytes +
        tracker.clients_slaves_bytes;

    const dataset_pct = tracker.datasetPercentage();
    const peak_pct = tracker.peakPercentage();
    const frag_ratio = tracker.fragmentationRatio();
    const frag_bytes: isize = @as(isize, @intCast(@min(tracker.current_allocated, std.math.maxInt(isize)))) -
        @as(isize, @intCast(@min(tracker.dataset_bytes, std.math.maxInt(isize))));

    // Compute allocator-level ratios (approximated from RSS vs allocated)
    const rss = storage.memory_tracker.current_allocated; // best approximation
    const alloc_frag_ratio: f64 = if (tracker.current_allocated > 0)
        @as(f64, @floatFromInt(rss)) / @as(f64, @floatFromInt(tracker.current_allocated))
    else
        1.0;
    const alloc_frag_bytes: isize = @as(isize, @intCast(@min(rss, std.math.maxInt(isize)))) -
        @as(isize, @intCast(@min(tracker.current_allocated, std.math.maxInt(isize))));
    const alloc_rss_ratio: f64 = alloc_frag_ratio;
    const alloc_rss_bytes: isize = alloc_frag_bytes;
    const rss_overhead_ratio: f64 = 1.0;
    const rss_overhead_bytes: isize = 0;

    // Defrag stats from defrag task
    const defrag_stats = storage.defrag_task.getStats();
    const defrag_running: i64 = if (defrag_stats.is_running) 1 else 0;

    // MEMORY STATS returns a flat array with 26 key-value pairs = 52 elements.
    // Format: *52\r\n then alternating $key\r\n + integer or bulk-string value.
    const num_pairs: usize = 26;
    try w.print("*{d}\r\n", .{num_pairs * 2});

    // Helper: write a bulk-string key
    const writeBulkKey = struct {
        fn call(writer: anytype, key: []const u8) !void {
            try writer.print("${d}\r\n{s}\r\n", .{ key.len, key });
        }
    }.call;
    // Helper: write an integer value
    const writeInt = struct {
        fn call(writer: anytype, val: i64) !void {
            try writer.print(":{d}\r\n", .{val});
        }
    }.call;
    // Helper: write a float as bulk string (Redis sends doubles as bulk strings in RESP2)
    const writeFloat = struct {
        fn call(writer: anytype, alloc: std.mem.Allocator, val: f64) !void {
            const s = try std.fmt.allocPrint(alloc, "{d:.4}", .{val});
            defer alloc.free(s);
            try writer.print("${d}\r\n{s}\r\n", .{ s.len, s });
        }
    }.call;

    try writeBulkKey(w, "peak.allocated");
    try writeInt(w, @intCast(tracker.peak_allocated));

    try writeBulkKey(w, "total.allocated");
    try writeInt(w, @intCast(tracker.current_allocated));

    try writeBulkKey(w, "startup.allocated");
    try writeInt(w, @intCast(tracker.startup_allocated));

    try writeBulkKey(w, "replication.backlog");
    try writeInt(w, @intCast(tracker.replication_backlog_bytes));

    try writeBulkKey(w, "clients.slaves");
    try writeInt(w, @intCast(tracker.clients_slaves_bytes));

    try writeBulkKey(w, "clients.normal");
    try writeInt(w, @intCast(tracker.clients_normal_bytes));

    try writeBulkKey(w, "aof.buffer");
    try writeInt(w, @intCast(tracker.aof_buffer_bytes));

    try writeBulkKey(w, "overhead.total");
    try writeInt(w, @intCast(overhead_total));

    try writeBulkKey(w, "keys.count");
    try writeInt(w, @intCast(keys_count));

    try writeBulkKey(w, "keys.bytes-per-key");
    try writeInt(w, @intCast(bytes_per_key));

    try writeBulkKey(w, "dataset.bytes");
    try writeInt(w, @intCast(tracker.dataset_bytes));

    try writeBulkKey(w, "dataset.percentage");
    try writeFloat(w, allocator, dataset_pct);

    try writeBulkKey(w, "peak.percentage");
    try writeFloat(w, allocator, peak_pct);

    try writeBulkKey(w, "fragmentation");
    try writeFloat(w, allocator, frag_ratio);

    try writeBulkKey(w, "fragmentation.bytes");
    try writeInt(w, frag_bytes);

    try writeBulkKey(w, "allocator-frag.ratio");
    try writeFloat(w, allocator, alloc_frag_ratio);

    try writeBulkKey(w, "allocator-frag.bytes");
    try writeInt(w, alloc_frag_bytes);

    try writeBulkKey(w, "allocator-rss.ratio");
    try writeFloat(w, allocator, alloc_rss_ratio);

    try writeBulkKey(w, "allocator-rss.bytes");
    try writeInt(w, alloc_rss_bytes);

    try writeBulkKey(w, "rss-overhead.ratio");
    try writeFloat(w, allocator, rss_overhead_ratio);

    try writeBulkKey(w, "rss-overhead.bytes");
    try writeInt(w, rss_overhead_bytes);

    try writeBulkKey(w, "active.defrag.running");
    try writeInt(w, defrag_running);

    try writeBulkKey(w, "active.defrag.hits");
    try writeInt(w, @intCast(defrag_stats.keys_defragmented));

    try writeBulkKey(w, "active.defrag.misses");
    try writeInt(w, @intCast(defrag_stats.keys_scanned -| defrag_stats.keys_defragmented));

    try writeBulkKey(w, "active.defrag.key-hits");
    try writeInt(w, @intCast(defrag_stats.keys_defragmented));

    try writeBulkKey(w, "active.defrag.key-misses");
    try writeInt(w, @intCast(defrag_stats.keys_scanned -| defrag_stats.keys_defragmented));

    return buf.toOwnedSlice(allocator);
}

/// MEMORY USAGE - Estimate memory usage of a key (improved estimation)
pub fn cmdMemoryUsage(
    allocator: std.mem.Allocator,
    storage: *Storage,
    key: []const u8,
    samples: usize,
) ![]const u8 {
    _ = samples; // For future use with nested aggregates

    const value = storage.data.get(key) orelse {
        return std.fmt.allocPrint(allocator, "$-1\r\n", .{});
    };

    // Improved estimate with overhead
    const key_overhead: usize = 48; // HashMap entry overhead
    const ttl_overhead: usize = 8; // Optional i64
    const estimated_size: i64 = blk: {
        const data_size: usize = switch (value) {
            .string => |s| s.data.len + 24, // String struct + data
            .list => |l| l.data.items.len * (@sizeOf([]const u8) + 32) + 64, // ArrayList + items
            .set => |s| s.count() * 64 + 128, // StringHashMap or intset entries
            .hash => |h| h.data.count() * 128 + 128, // FieldValue entries
            .sorted_set => |zs| zs.members.count() * 160 + 256, // Member + score + indices
            .stream => |s| s.entries.items.len * 256 + 512, // Entry struct + metadata
            .hyperloglog => 12304, // 16384 registers
            .json => |j| blk2: { _ = j; break :blk2 512; }, // JSON tree * 6 bits
            .timeseries => |ts| ts.samples.items.len * 16 + 512, // DataPoint + metadata
            .bloom => |bf| blk2: {
                var total: usize = 256; // Bloom struct overhead
                for (bf.filters.items) |filter| {
                    total += filter.bits.len;
                }
                break :blk2 total;
            },
            .cuckoo => |cf| blk2: {
                var total: usize = 256; // Cuckoo struct overhead
                for (cf.filters.items) |filter| {
                    for (filter.buckets) |bucket| {
                        total += bucket.fingerprints.len;
                    }
                }
                break :blk2 total;
            },
            .count_min_sketch => |cms| blk2: {
                const total: usize = 256 + (cms.depth * cms.width * @sizeOf(u64)); // struct overhead + counters
                break :blk2 total;
            },
            .top_k => |tk| blk2: {
                const total: usize = 256 + (tk.depth * tk.width * (@sizeOf(u8) + @sizeOf(u64))) + (tk.k * 64); // struct + hash table + heap
                break :blk2 total;
            },
            .t_digest => |td| blk2: {
                const total: usize = 256 + (td.centroids.items.len * (@sizeOf(f64) + @sizeOf(u64))); // struct + centroids
                break :blk2 total;
            },
            .vector_set => |vs| blk2: {
                const total: usize = 256 + (vs.vectors.count() * vs.dimensionality * @sizeOf(f32)); // struct + vectors
                break :blk2 total;
            },
        };
        break :blk @intCast(key.len + key_overhead + ttl_overhead + data_size);
    };

    return std.fmt.allocPrint(allocator, ":{d}\r\n", .{estimated_size});
}

/// MEMORY DOCTOR - Return memory usage advice (real analysis)
pub fn cmdMemoryDoctor(allocator: std.mem.Allocator, storage: *Storage) ![]const u8 {
    var advice = std.ArrayList(u8){};
    errdefer advice.deinit(allocator);

    const w = advice.writer(allocator);
    const tracker = &storage.memory_tracker;
    const frag_ratio = tracker.fragmentationRatio();
    const keys_count = storage.dbSize();
    const bytes_per_key: usize = if (keys_count > 0)
        tracker.dataset_bytes / keys_count
    else
        0;

    // Analyze memory health
    var has_issues = false;

    // Check fragmentation
    if (frag_ratio > 1.5) {
        try w.print("* High memory fragmentation detected ({d:.2}).\n", .{frag_ratio});
        try w.writeAll("  Consider restarting the server to reduce fragmentation.\n\n");
        has_issues = true;
    }

    // Check peak memory vs current
    const peak_pct = tracker.peakPercentage();
    if (peak_pct < 70.0 and tracker.peak_allocated > tracker.startup_allocated * 2) {
        try w.print("* Peak memory ({d} bytes) is {d:.1}% higher than current ({d} bytes).\n", .{
            tracker.peak_allocated,
            100.0 - peak_pct,
            tracker.current_allocated,
        });
        try w.writeAll("  Memory was freed but not released to OS.\n\n");
        has_issues = true;
    }

    // Check small keys
    if (keys_count > 1000 and bytes_per_key < 100) {
        try w.print("* Large number of small keys detected ({d} keys, {d} bytes/key).\n", .{
            keys_count,
            bytes_per_key,
        });
        try w.writeAll("  Consider using hashes to group related keys.\n\n");
        has_issues = true;
    }

    // Check AOF buffer
    if (tracker.aof_buffer_bytes > 10 * 1024 * 1024) {
        try w.print("* AOF buffer is large ({d} bytes).\n", .{tracker.aof_buffer_bytes});
        try w.writeAll("  AOF rewrite may be needed.\n\n");
        has_issues = true;
    }

    if (!has_issues) {
        try w.writeAll("Hi Sam, I detected no issues in your memory.\n");
    }

    const advice_str = try advice.toOwnedSlice(allocator);
    defer allocator.free(advice_str);

    return std.fmt.allocPrint(allocator, "${d}\r\n{s}\r\n", .{ advice_str.len, advice_str });
}

/// MEMORY PURGE - Attempt to purge dirty pages (no-op for Zig GPA)
pub fn cmdMemoryPurge(allocator: std.mem.Allocator) ![]const u8 {
    // Zig's GeneralPurposeAllocator doesn't support explicit purging
    // In a real Redis implementation, this would call je_purge_dirty_pages() for jemalloc
    return std.fmt.allocPrint(allocator, "+OK\r\n", .{});
}

/// MEMORY MALLOC-STATS - Return allocator statistics (stub)
pub fn cmdMemoryMallocStats(allocator: std.mem.Allocator) ![]const u8 {
    // Zig's GPA doesn't expose detailed internal statistics like jemalloc
    // Return minimal stats
    const stats =
        \\Allocator: Zig GeneralPurposeAllocator
        \\Note: Detailed malloc stats not available (jemalloc-specific)
    ;
    return std.fmt.allocPrint(allocator, "${d}\r\n{s}\r\n", .{ stats.len, stats });
}

/// MEMORY HELP - Return MEMORY command help
pub fn cmdMemoryHelp(allocator: std.mem.Allocator) ![]const u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    const help = [_][]const u8{
        "MEMORY STATS - Show memory usage statistics",
        "MEMORY USAGE <key> [SAMPLES <count>] - Estimate memory usage of a key",
        "MEMORY DOCTOR - Get memory usage advice",
        "MEMORY PURGE - Purge dirty pages (no-op)",
        "MEMORY MALLOC-STATS - Show allocator statistics",
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
        // Parse optional SAMPLES parameter
        var samples: usize = 5; // default
        if (args.len >= 4 and std.ascii.eqlIgnoreCase(args[2], "samples")) {
            samples = std.fmt.parseInt(usize, args[3], 10) catch {
                return std.fmt.allocPrint(allocator, "-ERR value is not an integer or out of range\r\n", .{});
            };
        }
        return cmdMemoryUsage(allocator, storage, args[1], samples);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "doctor")) {
        return cmdMemoryDoctor(allocator, storage);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "purge")) {
        return cmdMemoryPurge(allocator);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "malloc-stats")) {
        return cmdMemoryMallocStats(allocator);
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
test "MEMORY STATS returns flat RESP array not bulk string" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try cmdMemoryStats(allocator, &storage);
    defer allocator.free(result);

    // Must be a flat array (*N\r\n), not a bulk string ($N\r\n)
    try std.testing.expect(result.len > 0);
    try std.testing.expect(std.mem.startsWith(u8, result, "*"));
    // Must NOT be a bulk string
    try std.testing.expect(!std.mem.startsWith(u8, result, "$"));
}

test "MEMORY STATS contains expected keys as RESP bulk strings" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try cmdMemoryStats(allocator, &storage);
    defer allocator.free(result);

    // All keys appear as bulk strings in the flat array
    try std.testing.expect(std.mem.indexOf(u8, result, "keys.count") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "peak.allocated") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "total.allocated") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "dataset.bytes") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "fragmentation") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "active.defrag.running") != null);
}

test "MEMORY STATS keys.count reflects actual key count" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // 0 keys initially — keys.count should be 0
    const result0 = try cmdMemoryStats(allocator, &storage);
    defer allocator.free(result0);
    // The value after "keys.count" key entry should be :0
    // Find the key entry "$10\r\nkeys.count\r\n" and check value is ":0\r\n"
    try std.testing.expect(std.mem.indexOf(u8, result0, "keys.count") != null);

    // Add 3 keys
    _ = try storage.set("k1", "v1", null);
    _ = try storage.set("k2", "v2", null);
    _ = try storage.set("k3", "v3", null);

    const result3 = try cmdMemoryStats(allocator, &storage);
    defer allocator.free(result3);
    try std.testing.expect(std.mem.indexOf(u8, result3, "keys.count") != null);
    // With 3 keys, "keys.count\r\n:3\r\n" should be present
    try std.testing.expect(std.mem.indexOf(u8, result3, ":3\r\n") != null);
}

test "MEMORY STATS has 26 key-value pairs (52 elements total)" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try cmdMemoryStats(allocator, &storage);
    defer allocator.free(result);

    // Should start with *52\r\n (26 pairs × 2)
    try std.testing.expect(std.mem.startsWith(u8, result, "*52\r\n"));
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
///
/// Returns an array where each entry is a 4-element array:
///   [event_name, unix_timestamp_sec, latest_latency_ms, all_time_max_latency_ms]
/// This matches the Redis LATENCY LATEST wire format exactly.
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
        // Each entry: *4 [name, timestamp_sec, latest_ms, max_ms]
        try w.writeAll("*4\r\n");

        // 1) Event name (bulk string)
        const event_name = entry.event.toString();
        try w.print("${d}\r\n{s}\r\n", .{ event_name.len, event_name });

        // 2) Unix timestamp in seconds (LatencySample stores milliseconds)
        const timestamp_sec = @divTrunc(entry.sample.timestamp, 1000);
        try w.print(":{d}\r\n", .{timestamp_sec});

        // 3) Latest latency in milliseconds (stored as microseconds, convert)
        const latest_ms = @divTrunc(entry.sample.latency, 1000);
        try w.print(":{d}\r\n", .{latest_ms});

        // 4) All-time max latency in milliseconds
        const max_ms = @divTrunc(entry.max_latency, 1000);
        try w.print(":{d}\r\n", .{max_ms});
    }

    return buf.toOwnedSlice(allocator);
}

/// LATENCY HISTORY - Get latency history for a specific event
///
/// Returns an array where each entry is a 2-element array:
///   [unix_timestamp_sec, latency_ms]
/// This matches the Redis LATENCY HISTORY wire format exactly.
pub fn cmdLatencyHistory(
    allocator: std.mem.Allocator,
    storage: *Storage,
    event_name: []const u8,
) ![]const u8 {
    const event_type = EventType.fromString(event_name) orelse {
        // Unknown event type → return empty array (Redis behavior)
        return std.fmt.allocPrint(allocator, "*0\r\n", .{});
    };

    const history = try storage.latency_monitor.getHistory(event_type, allocator);
    defer if (history) |h| allocator.free(h);

    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    const w = buf.writer(allocator);

    if (history) |h| {
        try w.print("*{d}\r\n", .{h.len});
        for (h) |sample| {
            // Each entry: *2 [timestamp_sec, latency_ms]
            try w.writeAll("*2\r\n");
            // Timestamp stored in ms → convert to seconds
            const timestamp_sec = @divTrunc(sample.timestamp, 1000);
            try w.print(":{d}\r\n", .{timestamp_sec});
            // Latency stored in microseconds → convert to milliseconds
            const latency_ms = @divTrunc(sample.latency, 1000);
            try w.print(":{d}\r\n", .{latency_ms});
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

    // Record 1ms (1000us) for command and 5ms (5000us) for fork
    try storage.latency_monitor.recordEvent(.command, 1000);
    try storage.latency_monitor.recordEvent(.fork, 5000);

    const result = try cmdLatencyLatest(std.testing.allocator, &storage);
    defer std.testing.allocator.free(result);

    // Should start with *2 (2 event entries)
    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
    // Each entry should be *4 (name, timestamp_sec, latest_ms, max_ms)
    try std.testing.expect(std.mem.indexOf(u8, result, "*4\r\n") != null);
    // Event names present
    try std.testing.expect(std.mem.indexOf(u8, result, "command") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "fork") != null);
    // Latencies in milliseconds: 1000us → 1ms, 5000us → 5ms
    try std.testing.expect(std.mem.indexOf(u8, result, ":1\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":5\r\n") != null);
}

test "cmdLatencyLatest max_latency tracking" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    // Record multiple events - max should be 10000us = 10ms
    try storage.latency_monitor.recordEvent(.command, 2000);
    try storage.latency_monitor.recordEvent(.command, 10000);
    try storage.latency_monitor.recordEvent(.command, 3000);

    const result = try cmdLatencyLatest(std.testing.allocator, &storage);
    defer std.testing.allocator.free(result);

    // Latest latency: 3ms (3000us), max latency: 10ms (10000us)
    try std.testing.expect(std.mem.indexOf(u8, result, ":10\r\n") != null); // max_ms
    try std.testing.expect(std.mem.indexOf(u8, result, ":3\r\n") != null);  // latest_ms
}

test "cmdLatencyHistory" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    // Record 500us and 1500us events
    try storage.latency_monitor.recordEvent(.aof_write, 500);
    try storage.latency_monitor.recordEvent(.aof_write, 1500);

    const result = try cmdLatencyHistory(std.testing.allocator, &storage, "aof-write");
    defer std.testing.allocator.free(result);

    // Should have 2 entries
    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
    // Each entry should be [timestamp_sec, latency_ms]
    // 500us → 0ms, 1500us → 1ms
    try std.testing.expect(std.mem.indexOf(u8, result, ":0\r\n") != null);  // 500us → 0ms
    try std.testing.expect(std.mem.indexOf(u8, result, ":1\r\n") != null);  // 1500us → 1ms
}

test "cmdLatencyHistory unknown event returns empty array" {
    // Regression test: unknown event types were returning ERR instead of empty array.
    // Redis behavior: LATENCY HISTORY <unknown> returns *0\r\n (empty array).
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    const result = try cmdLatencyHistory(std.testing.allocator, &storage, "not-a-real-event");
    defer std.testing.allocator.free(result);

    // Should return empty array, not an error
    try std.testing.expectEqualStrings("*0\r\n", result);
}

test "cmdLatencyHistory returns correct Redis format" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    // Record 5ms (5000us) event
    try storage.latency_monitor.recordEvent(.command, 5000);

    const result = try cmdLatencyHistory(std.testing.allocator, &storage, "command");
    defer std.testing.allocator.free(result);

    // Should have 1 entry: *1 then *2 [timestamp_sec, 5ms]
    try std.testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":5\r\n") != null);  // 5ms
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

test "cmdMemoryStats" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    // Add some keys to populate memory
    _ = try storage.set("key1", "value1", null);
    _ = try storage.set("key2", "value2", null);

    const result = try cmdMemoryStats(std.testing.allocator, &storage);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "peak.allocated:") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "dataset.bytes:") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "keys.count:") != null);
}

test "cmdMemoryUsage existing key" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    _ = try storage.set("testkey", "testvalue", null);

    const result = try cmdMemoryUsage(std.testing.allocator, &storage, "testkey", 5);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, ":"));
    try std.testing.expect(result[result.len - 2] == '\r');
}

test "cmdMemoryUsage missing key" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    const result = try cmdMemoryUsage(std.testing.allocator, &storage, "nonexistent", 5);
    defer std.testing.allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "cmdMemoryDoctor no issues" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    const result = try cmdMemoryDoctor(std.testing.allocator, &storage);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "Hi Sam") != null);
}

test "cmdMemoryPurge" {
    const result = try cmdMemoryPurge(std.testing.allocator);
    defer std.testing.allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "cmdMemoryMallocStats" {
    const result = try cmdMemoryMallocStats(std.testing.allocator);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "Allocator:") != null);
}

test "cmdMemoryHelp" {
    const result = try cmdMemoryHelp(std.testing.allocator);
    defer std.testing.allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "MEMORY STATS") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "MEMORY DOCTOR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "MEMORY PURGE") != null);
}

test "cmdMemory dispatcher" {
    var storage = try Storage.init(std.testing.allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    const stats_args = [_][]const u8{"stats"};
    const stats_result = try cmdMemory(std.testing.allocator, &storage, &stats_args);
    defer std.testing.allocator.free(stats_result);
    try std.testing.expect(std.mem.indexOf(u8, stats_result, "peak.allocated:") != null);

    const help_args = [_][]const u8{"help"};
    const help_result = try cmdMemory(std.testing.allocator, &storage, &help_args);
    defer std.testing.allocator.free(help_result);
    try std.testing.expect(std.mem.indexOf(u8, help_result, "MEMORY STATS") != null);
}
