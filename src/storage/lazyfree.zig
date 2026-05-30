const std = @import("std");

/// Lazy freeing work item type
pub const LazyFreeWorkType = enum {
    /// Free a single key's value asynchronously
    free_key,
    /// Flush entire database asynchronously
    flush_db,
    /// Flush all databases asynchronously
    flush_all,
};

/// Lazy freeing work item
pub const LazyFreeWork = struct {
    work_type: LazyFreeWorkType,
    /// Key to free (only for free_key)
    key: ?[]const u8,
    /// Database number (only for flush_db)
    db_num: ?u16,
    /// Opaque pointer to Value (only for free_key) - will be cast to *Value in processWork
    value_ptr: ?*anyopaque,
    /// Allocator for cleanup
    allocator: std.mem.Allocator,

    pub fn deinit(self: *LazyFreeWork) void {
        if (self.key) |k| {
            self.allocator.free(k);
        }
        // CRITICAL: Also clean up value_ptr if not yet processed
        // This handles the case where thread is never started or work is cancelled
        if (self.value_ptr) |ptr| {
            const memory_mod = @import("memory.zig");
            const value: *memory_mod.Value = @ptrCast(@alignCast(ptr));
            value.deinit(self.allocator);
            self.allocator.destroy(value);
        }
    }
};

/// Lazy freeing background task
pub const LazyFreeTask = struct {
    /// Work queue
    queue: std.ArrayList(LazyFreeWork),
    /// Mutex for queue access
    mutex: std.Thread.Mutex,
    /// Background thread
    thread: ?std.Thread,
    /// Running flag (atomic)
    running: std.atomic.Value(bool),
    /// Allocator
    allocator: std.mem.Allocator,
    /// Condition variable for signaling work
    cond: std.Thread.Condition,
    /// Total objects freed by background lazy freeing (for INFO lazyfreed_objects)
    freed_objects: std.atomic.Value(u64),

    /// Initialize lazy free task
    pub fn init(allocator: std.mem.Allocator) !LazyFreeTask {
        var queue = std.ArrayList(LazyFreeWork){};
        errdefer queue.deinit(allocator);

        return LazyFreeTask{
            .queue = queue,
            .mutex = std.Thread.Mutex{},
            .thread = null,
            .running = std.atomic.Value(bool).init(false),
            .allocator = allocator,
            .cond = std.Thread.Condition{},
            .freed_objects = std.atomic.Value(u64).init(0),
        };
    }

    /// Start background thread
    pub fn start(self: *LazyFreeTask) !void {
        self.running.store(true, .release);
        self.thread = try std.Thread.spawn(.{}, workerLoop, .{self});
    }

    /// Stop background thread and wait
    pub fn stop(self: *LazyFreeTask) void {
        self.running.store(false, .release);

        // Signal condition to wake up worker
        self.cond.signal();

        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }

    /// Deinitialize and clean up
    pub fn deinit(self: *LazyFreeTask) void {
        self.stop();

        self.mutex.lock();
        defer self.mutex.unlock();

        // Free remaining work items
        for (self.queue.items) |*item| {
            item.deinit();
        }
        self.queue.deinit(self.allocator);
    }

    /// Submit work to background queue
    /// Note: On error, caller must clean up the work item
    pub fn submitWork(self: *LazyFreeTask, work: LazyFreeWork) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // If append fails, caller must clean up work item
        self.queue.append(self.allocator, work) catch |err| {
            // Unlock mutex before cleaning up
            return err;
        };

        // Signal condition variable to wake worker
        self.cond.signal();
    }

    /// Worker loop (runs in background thread)
    fn workerLoop(self: *LazyFreeTask) void {
        while (self.running.load(.acquire)) {
            self.mutex.lock();

            // Wait for work or shutdown signal
            while (self.queue.items.len == 0 and self.running.load(.acquire)) {
                self.cond.wait(&self.mutex);
            }

            // Get work item if available
            const work_opt = if (self.queue.items.len > 0)
                self.queue.orderedRemove(0)
            else
                null;

            self.mutex.unlock();

            if (work_opt) |work| {
                // Process work item
                var mutable_work = work;
                self.processWork(&mutable_work);
                mutable_work.deinit();
            }
        }
    }

    /// Process a single work item
    fn processWork(self: *LazyFreeTask, work: *LazyFreeWork) void {
        switch (work.work_type) {
            .free_key => {
                // Free the value
                if (work.value_ptr) |ptr| {
                    // Import Value type to call deinit
                    const memory_mod = @import("memory.zig");
                    const value: *memory_mod.Value = @ptrCast(@alignCast(ptr));
                    value.deinit(work.allocator);
                    work.allocator.destroy(value);
                    // Set to null to prevent double-free in deinit()
                    work.value_ptr = null;
                    _ = self.freed_objects.fetchAdd(1, .monotonic);
                }
            },
            .flush_db, .flush_all => {
                // For flush operations, individual keys are submitted as free_key work items
                // This is handled in Storage.flushAllAsync
            },
        }
    }

    /// Get number of pending objects in the lazy free queue
    /// Used for monitoring via INFO stats
    pub fn getPendingCount(self: *LazyFreeTask) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.queue.items.len;
    }

    /// Get total number of objects freed by the background task
    /// Used for INFO lazyfreed_objects
    pub fn getFreedCount(self: *LazyFreeTask) u64 {
        return self.freed_objects.load(.monotonic);
    }
};

test "LazyFreeTask init and deinit" {
    const allocator = std.testing.allocator;

    var task = try LazyFreeTask.init(allocator);
    defer task.deinit();

    try std.testing.expect(!task.running.load(.acquire));
}

test "LazyFreeTask start and stop" {
    const allocator = std.testing.allocator;

    var task = try LazyFreeTask.init(allocator);
    defer task.deinit();

    try task.start();
    try std.testing.expect(task.running.load(.acquire));

    task.stop();
    try std.testing.expect(!task.running.load(.acquire));
}

test "LazyFreeTask submit work" {
    const allocator = std.testing.allocator;

    var task = try LazyFreeTask.init(allocator);
    defer task.deinit();

    try task.start();

    const key = try allocator.dupe(u8, "test_key");
    const work = LazyFreeWork{
        .work_type = .free_key,
        .key = key,
        .db_num = null,
        .value_ptr = null,
        .allocator = allocator,
    };

    try task.submitWork(work);

    // Give worker time to process
    std.time.sleep(100 * std.time.ns_per_ms);

    task.stop();
}

test "LazyFreeWork deinit frees key" {
    const allocator = std.testing.allocator;

    const key = try allocator.dupe(u8, "test_key");
    var work = LazyFreeWork{
        .work_type = .free_key,
        .key = key,
        .db_num = null,
        .value_ptr = null,
        .allocator = allocator,
    };

    work.deinit();
    // Memory leak check will verify key was freed
}
