const std = @import("std");

/// Latency event types tracked by the server
pub const EventType = enum {
    command,
    fast_command,
    fork,
    rdb_unlink_temp_file,
    aof_write,
    aof_fsync_always,
    aof_write_pending_fsync,
    aof_rewrite_diff_write,
    expire_cycle,
    eviction_cycle,
    eviction_del,

    pub fn fromString(s: []const u8) ?EventType {
        if (std.mem.eql(u8, s, "command")) return .command;
        if (std.mem.eql(u8, s, "fast-command")) return .fast_command;
        if (std.mem.eql(u8, s, "fork")) return .fork;
        if (std.mem.eql(u8, s, "rdb-unlink-temp-file")) return .rdb_unlink_temp_file;
        if (std.mem.eql(u8, s, "aof-write")) return .aof_write;
        if (std.mem.eql(u8, s, "aof-fsync-always")) return .aof_fsync_always;
        if (std.mem.eql(u8, s, "aof-write-pending-fsync")) return .aof_write_pending_fsync;
        if (std.mem.eql(u8, s, "aof-rewrite-diff-write")) return .aof_rewrite_diff_write;
        if (std.mem.eql(u8, s, "expire-cycle")) return .expire_cycle;
        if (std.mem.eql(u8, s, "eviction-cycle")) return .eviction_cycle;
        if (std.mem.eql(u8, s, "eviction-del")) return .eviction_del;
        return null;
    }

    pub fn toString(self: EventType) []const u8 {
        return switch (self) {
            .command => "command",
            .fast_command => "fast-command",
            .fork => "fork",
            .rdb_unlink_temp_file => "rdb-unlink-temp-file",
            .aof_write => "aof-write",
            .aof_fsync_always => "aof-fsync-always",
            .aof_write_pending_fsync => "aof-write-pending-fsync",
            .aof_rewrite_diff_write => "aof-rewrite-diff-write",
            .expire_cycle => "expire-cycle",
            .eviction_cycle => "eviction-cycle",
            .eviction_del => "eviction-del",
        };
    }
};

/// Single latency sample
pub const LatencySample = struct {
    timestamp: i64, // Unix timestamp in milliseconds
    latency: u32, // Latency in microseconds

    pub fn init(timestamp: i64, latency: u32) LatencySample {
        return .{
            .timestamp = timestamp,
            .latency = latency,
        };
    }
};

/// Event-sample pair for LATENCY LATEST
pub const EventSample = struct {
    event: EventType,
    sample: LatencySample,
};

/// Ring buffer for latency history (keeps last 160 samples per event)
const HistoryBuffer = struct {
    samples: [160]LatencySample,
    head: usize,
    count: usize,

    pub fn init() HistoryBuffer {
        return .{
            .samples = undefined,
            .head = 0,
            .count = 0,
        };
    }

    pub fn add(self: *HistoryBuffer, sample: LatencySample) void {
        const idx = self.head;
        self.samples[idx] = sample;
        self.head = (self.head + 1) % 160;
        if (self.count < 160) {
            self.count += 1;
        }
    }

    pub fn getLatest(self: *const HistoryBuffer) ?LatencySample {
        if (self.count == 0) return null;
        const idx = if (self.head == 0) 159 else self.head - 1;
        return self.samples[idx];
    }

    pub fn getAll(self: *const HistoryBuffer, allocator: std.mem.Allocator) ![]LatencySample {
        if (self.count == 0) return &[_]LatencySample{};

        var result = try allocator.alloc(LatencySample, self.count);
        var i: usize = 0;
        var pos = if (self.count < 160) 0 else self.head;

        while (i < self.count) : (i += 1) {
            result[i] = self.samples[pos];
            pos = (pos + 1) % 160;
        }

        return result;
    }

    pub fn reset(self: *HistoryBuffer) void {
        self.head = 0;
        self.count = 0;
    }
};

/// Per-command histogram bucket
pub const HistogramBucket = struct {
    latency_usec: u32, // Upper bound in microseconds
    count: u64, // Number of samples <= this latency
};

/// Histogram tracking for a specific command
const CommandHistogram = struct {
    buckets: [16]HistogramBucket,
    total_count: u64,
    total_latency_usec: u64,

    pub fn init() CommandHistogram {
        return .{
            .buckets = [_]HistogramBucket{
                .{ .latency_usec = 1, .count = 0 },
                .{ .latency_usec = 2, .count = 0 },
                .{ .latency_usec = 4, .count = 0 },
                .{ .latency_usec = 8, .count = 0 },
                .{ .latency_usec = 16, .count = 0 },
                .{ .latency_usec = 32, .count = 0 },
                .{ .latency_usec = 64, .count = 0 },
                .{ .latency_usec = 128, .count = 0 },
                .{ .latency_usec = 256, .count = 0 },
                .{ .latency_usec = 512, .count = 0 },
                .{ .latency_usec = 1024, .count = 0 },
                .{ .latency_usec = 2048, .count = 0 },
                .{ .latency_usec = 4096, .count = 0 },
                .{ .latency_usec = 8192, .count = 0 },
                .{ .latency_usec = 16384, .count = 0 },
                .{ .latency_usec = std.math.maxInt(u32), .count = 0 },
            },
            .total_count = 0,
            .total_latency_usec = 0,
        };
    }

    pub fn add(self: *CommandHistogram, latency_usec: u32) void {
        self.total_count += 1;
        self.total_latency_usec += latency_usec;

        for (&self.buckets) |*bucket| {
            if (latency_usec <= bucket.latency_usec) {
                bucket.count += 1;
            }
        }
    }

    pub fn reset(self: *CommandHistogram) void {
        for (&self.buckets) |*bucket| {
            bucket.count = 0;
        }
        self.total_count = 0;
        self.total_latency_usec = 0;
    }
};

/// Latency monitor tracks latency events and command histograms
pub const LatencyMonitor = struct {
    allocator: std.mem.Allocator,
    histories: std.AutoHashMap(EventType, HistoryBuffer),
    histograms: std.StringHashMap(CommandHistogram),
    enabled: bool,

    pub fn init(allocator: std.mem.Allocator) !LatencyMonitor {
        return .{
            .allocator = allocator,
            .histories = std.AutoHashMap(EventType, HistoryBuffer).init(allocator),
            .histograms = std.StringHashMap(CommandHistogram).init(allocator),
            .enabled = true,
        };
    }

    pub fn deinit(self: *LatencyMonitor) void {
        self.histories.deinit();

        var it = self.histograms.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.histograms.deinit();
    }

    /// Record a latency event
    pub fn recordEvent(self: *LatencyMonitor, event_type: EventType, latency_usec: u32) !void {
        if (!self.enabled) return;

        const timestamp = std.time.milliTimestamp();
        const sample = LatencySample.init(timestamp, latency_usec);

        const gop = try self.histories.getOrPut(event_type);
        if (!gop.found_existing) {
            gop.value_ptr.* = HistoryBuffer.init();
        }
        gop.value_ptr.add(sample);
    }

    /// Record command latency for histogram
    pub fn recordCommandLatency(self: *LatencyMonitor, command: []const u8, latency_usec: u32) !void {
        if (!self.enabled) return;

        const gop = try self.histograms.getOrPut(command);
        if (!gop.found_existing) {
            const cmd_copy = try self.allocator.dupe(u8, command);
            gop.key_ptr.* = cmd_copy;
            gop.value_ptr.* = CommandHistogram.init();
        }
        gop.value_ptr.add(latency_usec);
    }

    /// Get latest sample for a specific event type
    pub fn getLatest(self: *const LatencyMonitor, event_type: EventType) ?LatencySample {
        const history = self.histories.get(event_type) orelse return null;
        return history.getLatest();
    }

    /// Get all latest samples (one per event type)
    pub fn getAllLatest(self: *const LatencyMonitor, allocator: std.mem.Allocator) ![]EventSample {
        var result = std.ArrayList(EventSample){};
        errdefer result.deinit(allocator);

        var it = self.histories.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.getLatest()) |sample| {
                try result.append(allocator, .{ .event = entry.key_ptr.*, .sample = sample });
            }
        }

        return try result.toOwnedSlice(allocator);
    }

    /// Get history for a specific event type
    pub fn getHistory(self: *const LatencyMonitor, event_type: EventType, allocator: std.mem.Allocator) !?[]LatencySample {
        const history = self.histories.get(event_type) orelse return null;
        return try history.getAll(allocator);
    }

    /// Reset specific event type
    pub fn resetEvent(self: *LatencyMonitor, event_type: EventType) bool {
        if (self.histories.getPtr(event_type)) |history| {
            history.reset();
            return true;
        }
        return false;
    }

    /// Reset all events
    pub fn resetAll(self: *LatencyMonitor) void {
        var it = self.histories.valueIterator();
        while (it.next()) |history| {
            history.reset();
        }
    }

    /// Get histogram for a specific command
    pub fn getHistogram(self: *const LatencyMonitor, command: []const u8) ?*const CommandHistogram {
        return self.histograms.getPtr(command);
    }

    /// Get all command names with histograms
    pub fn getAllCommands(self: *const LatencyMonitor, allocator: std.mem.Allocator) ![][]const u8 {
        var result = std.ArrayList([]const u8){};
        errdefer result.deinit(allocator);

        var it = self.histograms.keyIterator();
        while (it.next()) |cmd| {
            try result.append(allocator, cmd.*);
        }

        return try result.toOwnedSlice(allocator);
    }
};

// Unit tests
test "LatencyMonitor.init and deinit" {
    var monitor = try LatencyMonitor.init(std.testing.allocator);
    defer monitor.deinit();
    try std.testing.expect(monitor.enabled);
}

test "LatencyMonitor.recordEvent" {
    var monitor = try LatencyMonitor.init(std.testing.allocator);
    defer monitor.deinit();

    try monitor.recordEvent(.command, 1000);
    try monitor.recordEvent(.command, 2000);

    const latest = monitor.getLatest(.command).?;
    try std.testing.expectEqual(@as(u32, 2000), latest.latency);
}

test "LatencyMonitor.getHistory" {
    var monitor = try LatencyMonitor.init(std.testing.allocator);
    defer monitor.deinit();

    try monitor.recordEvent(.fork, 500);
    try monitor.recordEvent(.fork, 1500);
    try monitor.recordEvent(.fork, 2500);

    const history = (try monitor.getHistory(.fork, std.testing.allocator)).?;
    defer std.testing.allocator.free(history);

    try std.testing.expectEqual(@as(usize, 3), history.len);
    try std.testing.expectEqual(@as(u32, 500), history[0].latency);
    try std.testing.expectEqual(@as(u32, 1500), history[1].latency);
    try std.testing.expectEqual(@as(u32, 2500), history[2].latency);
}

test "LatencyMonitor.resetEvent" {
    var monitor = try LatencyMonitor.init(std.testing.allocator);
    defer monitor.deinit();

    try monitor.recordEvent(.aof_write, 3000);
    try std.testing.expect(monitor.getLatest(.aof_write) != null);

    _ = monitor.resetEvent(.aof_write);
    try std.testing.expect(monitor.getLatest(.aof_write) == null);
}

test "LatencyMonitor.recordCommandLatency" {
    var monitor = try LatencyMonitor.init(std.testing.allocator);
    defer monitor.deinit();

    try monitor.recordCommandLatency("GET", 100);
    try monitor.recordCommandLatency("GET", 200);
    try monitor.recordCommandLatency("SET", 150);

    const get_hist = monitor.getHistogram("GET").?;
    try std.testing.expectEqual(@as(u64, 2), get_hist.total_count);
    try std.testing.expectEqual(@as(u64, 300), get_hist.total_latency_usec);

    const set_hist = monitor.getHistogram("SET").?;
    try std.testing.expectEqual(@as(u64, 1), set_hist.total_count);
}

test "CommandHistogram.buckets" {
    var hist = CommandHistogram.init();

    hist.add(10);
    hist.add(50);
    hist.add(200);
    hist.add(1000);

    try std.testing.expectEqual(@as(u64, 4), hist.total_count);
    try std.testing.expectEqual(@as(u64, 1260), hist.total_latency_usec);

    // 10 should fall in bucket <=16
    try std.testing.expect(hist.buckets[4].count >= 1);
    // 1000 should fall in bucket <=1024
    try std.testing.expect(hist.buckets[10].count >= 1);
}

test "HistoryBuffer.ring_buffer" {
    var buf = HistoryBuffer.init();

    // Add 165 samples (more than 160 capacity)
    var i: u32 = 0;
    while (i < 165) : (i += 1) {
        buf.add(LatencySample.init(i, i * 10));
    }

    // Should only keep last 160
    try std.testing.expectEqual(@as(usize, 160), buf.count);

    const latest = buf.getLatest().?;
    try std.testing.expectEqual(@as(u32, 1640), latest.latency);
}

test "EventType.fromString" {
    try std.testing.expectEqual(EventType.command, EventType.fromString("command").?);
    try std.testing.expectEqual(EventType.fast_command, EventType.fromString("fast-command").?);
    try std.testing.expectEqual(EventType.aof_fsync_always, EventType.fromString("aof-fsync-always").?);
    try std.testing.expect(EventType.fromString("invalid") == null);
}

test "LatencyMonitor.getAllLatest" {
    var monitor = try LatencyMonitor.init(std.testing.allocator);
    defer monitor.deinit();

    try monitor.recordEvent(.command, 1000);
    try monitor.recordEvent(.fork, 5000);
    try monitor.recordEvent(.aof_write, 2000);

    const latest = try monitor.getAllLatest(std.testing.allocator);
    defer std.testing.allocator.free(latest);

    try std.testing.expectEqual(@as(usize, 3), latest.len);
}
