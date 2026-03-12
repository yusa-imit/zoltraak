const std = @import("std");

/// A single slow log entry
pub const SlowLogEntry = struct {
    id: u64, // Unique incrementing ID
    timestamp: i64, // Unix timestamp in microseconds
    duration_us: i64, // Command execution time in microseconds
    command: []const u8, // Full command string (owned)
    client_addr: []const u8, // Client IP:port (owned)
    client_name: []const u8, // Client name or empty string (owned)

    pub fn deinit(self: *SlowLogEntry, allocator: std.mem.Allocator) void {
        allocator.free(self.command);
        allocator.free(self.client_addr);
        allocator.free(self.client_name);
    }
};

/// Slow log - ring buffer tracking commands exceeding threshold
pub const SlowLog = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayList(SlowLogEntry),
    next_id: u64,
    max_len: usize,
    threshold_us: i64, // Commands slower than this are logged (microseconds)
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, max_len: usize, threshold_us: i64) SlowLog {
        return SlowLog{
            .allocator = allocator,
            .entries = std.ArrayList(SlowLogEntry){},
            .next_id = 0,
            .max_len = max_len,
            .threshold_us = threshold_us,
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *SlowLog) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.entries.items) |*entry| {
            entry.deinit(self.allocator);
        }
        self.entries.deinit(self.allocator);
    }

    /// Add a command to the slow log if it exceeds the threshold.
    /// Returns true if the command was logged.
    pub fn logCommand(
        self: *SlowLog,
        duration_us: i64,
        command: []const u8,
        client_addr: []const u8,
        client_name: []const u8,
    ) !bool {
        // Don't log if below threshold or threshold is negative (disabled)
        if (self.threshold_us < 0 or duration_us < self.threshold_us) {
            return false;
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        // Create entry with owned strings
        const entry = SlowLogEntry{
            .id = self.next_id,
            .timestamp = std.time.microTimestamp(),
            .duration_us = duration_us,
            .command = try self.allocator.dupe(u8, command),
            .client_addr = try self.allocator.dupe(u8, client_addr),
            .client_name = try self.allocator.dupe(u8, client_name),
        };
        errdefer {
            self.allocator.free(entry.command);
            self.allocator.free(entry.client_addr);
            self.allocator.free(entry.client_name);
        }

        self.next_id += 1;

        // If at capacity, remove oldest entry (ring buffer)
        if (self.entries.items.len >= self.max_len) {
            if (self.entries.items.len > 0) {
                var oldest = self.entries.orderedRemove(0);
                oldest.deinit(self.allocator);
            }
        }

        try self.entries.append(self.allocator, entry);
        return true;
    }

    /// Get the most recent N entries (or all if count is null).
    /// Returns a slice of entries (most recent first).
    /// Caller must NOT free the returned slice or entries.
    pub fn getEntries(self: *SlowLog, count: ?usize) []const SlowLogEntry {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entries = self.entries.items;
        if (entries.len == 0) {
            return &[_]SlowLogEntry{};
        }

        const n = if (count) |c| @min(c, entries.len) else entries.len;

        // Return the last N entries (most recent first in Redis)
        // We need to reverse the slice since our entries are oldest-first
        const start = entries.len - n;
        return entries[start..];
    }

    /// Get the current length of the slow log
    pub fn len(self: *SlowLog) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.entries.items.len;
    }

    /// Reset the slow log (clear all entries)
    pub fn reset(self: *SlowLog) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.entries.items) |*entry| {
            entry.deinit(self.allocator);
        }
        self.entries.clearRetainingCapacity();
    }

    /// Update the max length (resize ring buffer)
    pub fn setMaxLen(self: *SlowLog, new_max_len: usize) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.max_len = new_max_len;

        // Trim entries if new max is smaller
        while (self.entries.items.len > new_max_len) {
            var oldest = self.entries.orderedRemove(0);
            oldest.deinit(self.allocator);
        }
    }

    /// Update the threshold
    pub fn setThreshold(self: *SlowLog, new_threshold_us: i64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.threshold_us = new_threshold_us;
    }
};

// Unit tests
test "SlowLog init and deinit" {
    const allocator = std.testing.allocator;
    var slowlog = SlowLog.init(allocator, 10, 1000);
    defer slowlog.deinit();

    try std.testing.expectEqual(@as(usize, 0), slowlog.len());
}

test "SlowLog logging below threshold" {
    const allocator = std.testing.allocator;
    var slowlog = SlowLog.init(allocator, 10, 1000);
    defer slowlog.deinit();

    const logged = try slowlog.logCommand(500, "GET key", "127.0.0.1:12345", "");
    try std.testing.expect(!logged);
    try std.testing.expectEqual(@as(usize, 0), slowlog.len());
}

test "SlowLog logging above threshold" {
    const allocator = std.testing.allocator;
    var slowlog = SlowLog.init(allocator, 10, 1000);
    defer slowlog.deinit();

    const logged = try slowlog.logCommand(1500, "GET key", "127.0.0.1:12345", "myclient");
    try std.testing.expect(logged);
    try std.testing.expectEqual(@as(usize, 1), slowlog.len());

    const entries = slowlog.getEntries(null);
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqualStrings("GET key", entries[0].command);
    try std.testing.expectEqualStrings("127.0.0.1:12345", entries[0].client_addr);
    try std.testing.expectEqualStrings("myclient", entries[0].client_name);
    try std.testing.expectEqual(@as(i64, 1500), entries[0].duration_us);
}

test "SlowLog ring buffer eviction" {
    const allocator = std.testing.allocator;
    var slowlog = SlowLog.init(allocator, 3, 1000);
    defer slowlog.deinit();

    // Add 5 entries (should evict first 2)
    _ = try slowlog.logCommand(1100, "CMD1", "127.0.0.1:1", "");
    _ = try slowlog.logCommand(1200, "CMD2", "127.0.0.1:2", "");
    _ = try slowlog.logCommand(1300, "CMD3", "127.0.0.1:3", "");
    _ = try slowlog.logCommand(1400, "CMD4", "127.0.0.1:4", "");
    _ = try slowlog.logCommand(1500, "CMD5", "127.0.0.1:5", "");

    try std.testing.expectEqual(@as(usize, 3), slowlog.len());

    const entries = slowlog.getEntries(null);
    try std.testing.expectEqual(@as(usize, 3), entries.len);
    // Should have CMD3, CMD4, CMD5
    try std.testing.expectEqualStrings("CMD3", entries[0].command);
    try std.testing.expectEqualStrings("CMD4", entries[1].command);
    try std.testing.expectEqualStrings("CMD5", entries[2].command);
}

test "SlowLog reset" {
    const allocator = std.testing.allocator;
    var slowlog = SlowLog.init(allocator, 10, 1000);
    defer slowlog.deinit();

    _ = try slowlog.logCommand(1100, "CMD1", "127.0.0.1:1", "");
    _ = try slowlog.logCommand(1200, "CMD2", "127.0.0.1:2", "");

    try std.testing.expectEqual(@as(usize, 2), slowlog.len());

    slowlog.reset();
    try std.testing.expectEqual(@as(usize, 0), slowlog.len());
}

test "SlowLog getEntries with count" {
    const allocator = std.testing.allocator;
    var slowlog = SlowLog.init(allocator, 10, 1000);
    defer slowlog.deinit();

    _ = try slowlog.logCommand(1100, "CMD1", "127.0.0.1:1", "");
    _ = try slowlog.logCommand(1200, "CMD2", "127.0.0.1:2", "");
    _ = try slowlog.logCommand(1300, "CMD3", "127.0.0.1:3", "");

    const entries = slowlog.getEntries(2);
    try std.testing.expectEqual(@as(usize, 2), entries.len);
    // Should get last 2 entries
    try std.testing.expectEqualStrings("CMD2", entries[0].command);
    try std.testing.expectEqualStrings("CMD3", entries[1].command);
}

test "SlowLog disabled (negative threshold)" {
    const allocator = std.testing.allocator;
    var slowlog = SlowLog.init(allocator, 10, -1);
    defer slowlog.deinit();

    const logged = try slowlog.logCommand(999999, "GET key", "127.0.0.1:12345", "");
    try std.testing.expect(!logged);
    try std.testing.expectEqual(@as(usize, 0), slowlog.len());
}

test "SlowLog setMaxLen shrinks buffer" {
    const allocator = std.testing.allocator;
    var slowlog = SlowLog.init(allocator, 10, 1000);
    defer slowlog.deinit();

    _ = try slowlog.logCommand(1100, "CMD1", "127.0.0.1:1", "");
    _ = try slowlog.logCommand(1200, "CMD2", "127.0.0.1:2", "");
    _ = try slowlog.logCommand(1300, "CMD3", "127.0.0.1:3", "");

    try std.testing.expectEqual(@as(usize, 3), slowlog.len());

    slowlog.setMaxLen(2);
    try std.testing.expectEqual(@as(usize, 2), slowlog.len());

    const entries = slowlog.getEntries(null);
    // Should keep last 2 entries
    try std.testing.expectEqualStrings("CMD2", entries[0].command);
    try std.testing.expectEqualStrings("CMD3", entries[1].command);
}
