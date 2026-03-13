const std = @import("std");

/// MemoryTracker - Tracks memory usage statistics for MEMORY commands
pub const MemoryTracker = struct {
    startup_allocated: usize,
    peak_allocated: usize,
    current_allocated: usize,
    dataset_bytes: usize,
    overhead_bytes: usize,
    replication_backlog_bytes: usize,
    aof_buffer_bytes: usize,
    clients_normal_bytes: usize,
    clients_slaves_bytes: usize,

    pub fn init() MemoryTracker {
        const startup = getProcessMemoryUsage();
        return MemoryTracker{
            .startup_allocated = startup,
            .peak_allocated = startup,
            .current_allocated = startup,
            .dataset_bytes = 0,
            .overhead_bytes = 0,
            .replication_backlog_bytes = 0,
            .aof_buffer_bytes = 0,
            .clients_normal_bytes = 0,
            .clients_slaves_bytes = 0,
        };
    }

    /// Update current memory usage and peak if exceeded
    pub fn update(self: *MemoryTracker, delta: isize) void {
        if (delta > 0) {
            self.current_allocated +%= @intCast(delta);
        } else {
            const abs_delta: usize = @intCast(-delta);
            if (self.current_allocated >= abs_delta) {
                self.current_allocated -= abs_delta;
            } else {
                self.current_allocated = 0;
            }
        }

        if (self.current_allocated > self.peak_allocated) {
            self.peak_allocated = self.current_allocated;
        }
    }

    /// Update dataset bytes (actual key-value data)
    pub fn updateDataset(self: *MemoryTracker, delta: isize) void {
        if (delta > 0) {
            self.dataset_bytes +%= @intCast(delta);
        } else {
            const abs_delta: usize = @intCast(-delta);
            if (self.dataset_bytes >= abs_delta) {
                self.dataset_bytes -= abs_delta;
            } else {
                self.dataset_bytes = 0;
            }
        }
        self.update(delta);
    }

    /// Update overhead bytes (metadata, indices, etc.)
    pub fn updateOverhead(self: *MemoryTracker, delta: isize) void {
        if (delta > 0) {
            self.overhead_bytes +%= @intCast(delta);
        } else {
            const abs_delta: usize = @intCast(-delta);
            if (self.overhead_bytes >= abs_delta) {
                self.overhead_bytes -= abs_delta;
            } else {
                self.overhead_bytes = 0;
            }
        }
        self.update(delta);
    }

    /// Calculate fragmentation ratio
    pub fn fragmentationRatio(self: *const MemoryTracker) f64 {
        const rss = getProcessMemoryUsage();
        if (self.current_allocated == 0) return 1.0;
        return @as(f64, @floatFromInt(rss)) / @as(f64, @floatFromInt(self.current_allocated));
    }

    /// Calculate dataset percentage
    pub fn datasetPercentage(self: *const MemoryTracker) f64 {
        if (self.current_allocated == 0) return 0.0;
        return (@as(f64, @floatFromInt(self.dataset_bytes)) / @as(f64, @floatFromInt(self.current_allocated))) * 100.0;
    }

    /// Calculate peak percentage
    pub fn peakPercentage(self: *const MemoryTracker) f64 {
        if (self.peak_allocated == 0) return 0.0;
        return (@as(f64, @floatFromInt(self.current_allocated)) / @as(f64, @floatFromInt(self.peak_allocated))) * 100.0;
    }

    /// Get process RSS (Resident Set Size) - platform-specific
    fn getProcessMemoryUsage() usize {
        // On macOS/Linux, use /proc/self/statm or getrusage
        // For now, use a simple approximation via GPA
        return 1024 * 1024; // 1MB baseline - placeholder
    }
};

// Unit tests
test "MemoryTracker.init" {
    const tracker = MemoryTracker.init();
    try std.testing.expect(tracker.startup_allocated > 0);
    try std.testing.expectEqual(tracker.startup_allocated, tracker.current_allocated);
    try std.testing.expectEqual(tracker.startup_allocated, tracker.peak_allocated);
    try std.testing.expectEqual(@as(usize, 0), tracker.dataset_bytes);
}

test "MemoryTracker.update positive delta" {
    var tracker = MemoryTracker.init();
    const initial = tracker.current_allocated;
    tracker.update(1000);
    try std.testing.expectEqual(initial + 1000, tracker.current_allocated);
    try std.testing.expectEqual(tracker.current_allocated, tracker.peak_allocated);
}

test "MemoryTracker.update negative delta" {
    var tracker = MemoryTracker.init();
    tracker.update(5000);
    const before = tracker.current_allocated;
    tracker.update(-2000);
    try std.testing.expectEqual(before - 2000, tracker.current_allocated);
}

test "MemoryTracker.updateDataset" {
    var tracker = MemoryTracker.init();
    tracker.updateDataset(1000);
    try std.testing.expectEqual(@as(usize, 1000), tracker.dataset_bytes);
    tracker.updateDataset(-500);
    try std.testing.expectEqual(@as(usize, 500), tracker.dataset_bytes);
}

test "MemoryTracker.fragmentationRatio" {
    var tracker = MemoryTracker.init();
    tracker.update(1000);
    const ratio = tracker.fragmentationRatio();
    try std.testing.expect(ratio >= 0.0);
}

test "MemoryTracker.datasetPercentage" {
    var tracker = MemoryTracker.init();
    tracker.update(10000);
    tracker.dataset_bytes = 5000;
    const pct = tracker.datasetPercentage();
    try std.testing.expect(pct >= 0.0 and pct <= 100.0);
}

test "MemoryTracker.peakPercentage" {
    var tracker = MemoryTracker.init();
    tracker.update(10000); // Peak = 10000
    tracker.update(-2000); // Current = 8000
    const pct = tracker.peakPercentage();
    try std.testing.expect(pct >= 0.0 and pct <= 100.0);
}
