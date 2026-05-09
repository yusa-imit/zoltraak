const std = @import("std");
const Allocator = std.mem.Allocator;

/// Active defragmentation task for background memory optimization.
/// Scans the storage HashMap and reallocates fragmented values to compact memory.
pub const DefragTask = struct {
    allocator: Allocator,
    thread: ?std.Thread = null,
    running: std.atomic.Value(bool),
    mutex: std.Thread.Mutex,
    /// Condition variable for signaling defrag work
    cond: std.Thread.Condition,
    /// Number of keys scanned in current cycle
    keys_scanned: usize,
    /// Number of keys defragmented (reallocated)
    keys_defragmented: usize,
    /// Total bytes freed by defragmentation
    bytes_freed: usize,

    pub fn init(allocator: Allocator) !DefragTask {
        return DefragTask{
            .allocator = allocator,
            .running = std.atomic.Value(bool).init(false),
            .mutex = std.Thread.Mutex{},
            .cond = std.Thread.Condition{},
            .keys_scanned = 0,
            .keys_defragmented = 0,
            .bytes_freed = 0,
        };
    }

    pub fn deinit(self: *DefragTask) void {
        self.stop();
    }

    /// Start the defragmentation background task
    pub fn start(self: *DefragTask) !void {
        if (self.running.load(.acquire)) {
            return; // Already running
        }
        self.running.store(true, .release);
        self.thread = try std.Thread.spawn(.{}, runDefragLoop, .{self});
    }

    /// Stop the defragmentation background task gracefully
    pub fn stop(self: *DefragTask) void {
        if (!self.running.load(.acquire)) {
            return; // Already stopped
        }
        self.running.store(false, .release);
        // Signal the condition variable to wake up the thread
        self.cond.signal();
        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }

    /// Background loop that periodically scans and defragments
    fn runDefragLoop(self: *DefragTask) void {
        while (self.running.load(.acquire)) {
            // Sleep for 100ms between defrag cycles (configurable in real impl)
            std.Thread.sleep(100 * std.time.ns_per_ms);

            // Perform defragmentation cycle
            self.runCycle() catch |err| {
                // Log error but continue running
                std.log.err("Defrag cycle failed: {}", .{err});
                continue;
            };
        }
    }

    /// Run one defragmentation cycle
    /// Scans keys and attempts to reallocate fragmented values
    fn runCycle(self: *DefragTask) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // This is a placeholder implementation
        // Real defragmentation would require access to Storage and:
        // 1. Iterate over keys in Storage.data HashMap
        // 2. Check each value for fragmentation potential
        // 3. Reallocate string buffers, list nodes, hash entries, etc.
        // 4. Update the value in-place with the compacted version
        // 5. Track statistics (keys scanned, bytes freed)

        // For now, we just increment scan count to show the thread is running
        self.keys_scanned += 1;
    }

    /// Get current defragmentation statistics
    pub fn getStats(self: *DefragTask) DefragStats {
        self.mutex.lock();
        defer self.mutex.unlock();
        return DefragStats{
            .keys_scanned = self.keys_scanned,
            .keys_defragmented = self.keys_defragmented,
            .bytes_freed = self.bytes_freed,
            .is_running = self.running.load(.acquire),
        };
    }

    /// Reset defragmentation statistics
    pub fn resetStats(self: *DefragTask) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.keys_scanned = 0;
        self.keys_defragmented = 0;
        self.bytes_freed = 0;
    }
};

/// Defragmentation statistics
pub const DefragStats = struct {
    keys_scanned: usize,
    keys_defragmented: usize,
    bytes_freed: usize,
    is_running: bool,
};

// Unit tests
test "DefragTask init and deinit" {
    const allocator = std.testing.allocator;
    var task = try DefragTask.init(allocator);
    defer task.deinit();

    try std.testing.expect(!task.running.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), task.keys_scanned);
}

test "DefragTask start and stop" {
    const allocator = std.testing.allocator;
    var task = try DefragTask.init(allocator);
    defer task.deinit();

    try task.start();
    try std.testing.expect(task.running.load(.acquire));

    // Let it run briefly
    std.Thread.sleep(50 * std.time.ns_per_ms);

    task.stop();
    try std.testing.expect(!task.running.load(.acquire));
}

test "DefragTask getStats" {
    const allocator = std.testing.allocator;
    var task = try DefragTask.init(allocator);
    defer task.deinit();

    const stats = task.getStats();
    try std.testing.expectEqual(@as(usize, 0), stats.keys_scanned);
    try std.testing.expectEqual(@as(usize, 0), stats.keys_defragmented);
    try std.testing.expectEqual(@as(usize, 0), stats.bytes_freed);
    try std.testing.expect(!stats.is_running);
}

test "DefragTask resetStats" {
    const allocator = std.testing.allocator;
    var task = try DefragTask.init(allocator);
    defer task.deinit();

    // Manually set some stats
    task.mutex.lock();
    task.keys_scanned = 100;
    task.keys_defragmented = 10;
    task.bytes_freed = 1024;
    task.mutex.unlock();

    task.resetStats();

    const stats = task.getStats();
    try std.testing.expectEqual(@as(usize, 0), stats.keys_scanned);
    try std.testing.expectEqual(@as(usize, 0), stats.keys_defragmented);
    try std.testing.expectEqual(@as(usize, 0), stats.bytes_freed);
}
