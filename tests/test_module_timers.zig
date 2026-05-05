const std = @import("std");
const modules = @import("../src/storage/modules.zig");
const ModuleTimerStore = modules.ModuleTimerStore;
const TimerCallback = modules.TimerCallback;

// Test callback state
var test_callback_invocations: u32 = 0;
var test_callback_data: ?*anyopaque = null;

fn testCallback(allocator: std.mem.Allocator, data: ?*anyopaque) void {
    _ = allocator;
    test_callback_invocations += 1;
    test_callback_data = data;
}

fn resetTestState() void {
    test_callback_invocations = 0;
    test_callback_data = null;
}

test "ModuleTimerStore: init and deinit" {
    const allocator = std.testing.allocator;
    var timer_store = ModuleTimerStore.init(allocator);
    defer timer_store.deinit();

    try std.testing.expectEqual(@as(usize, 0), timer_store.timerCount());
}

test "ModuleTimerStore: create one-shot timer" {
    const allocator = std.testing.allocator;
    var timer_store = ModuleTimerStore.init(allocator);
    defer timer_store.deinit();
    resetTestState();

    const timer_id = try timer_store.createTimer(
        "test_module",
        0, // one-shot
        null,
        testCallback,
        null,
    );

    try std.testing.expect(timer_id > 0);
    try std.testing.expectEqual(@as(usize, 1), timer_store.timerCount());
    try std.testing.expect(timer_store.hasTimer(timer_id));
}

test "ModuleTimerStore: create periodic timer" {
    const allocator = std.testing.allocator;
    var timer_store = ModuleTimerStore.init(allocator);
    defer timer_store.deinit();
    resetTestState();

    const timer_id = try timer_store.createTimer(
        "test_module",
        100, // 100ms period
        null,
        testCallback,
        null,
    );

    try std.testing.expect(timer_id > 0);
    try std.testing.expectEqual(@as(usize, 1), timer_store.timerCount());
    try std.testing.expect(timer_store.hasTimer(timer_id));
}

test "ModuleTimerStore: stop timer" {
    const allocator = std.testing.allocator;
    var timer_store = ModuleTimerStore.init(allocator);
    defer timer_store.deinit();
    resetTestState();

    const timer_id = try timer_store.createTimer(
        "test_module",
        100,
        null,
        testCallback,
        null,
    );

    try timer_store.stopTimer(timer_id);
    try std.testing.expectEqual(@as(usize, 0), timer_store.timerCount());
    try std.testing.expect(!timer_store.hasTimer(timer_id));
}

test "ModuleTimerStore: stop nonexistent timer returns error" {
    const allocator = std.testing.allocator;
    var timer_store = ModuleTimerStore.init(allocator);
    defer timer_store.deinit();

    const result = timer_store.stopTimer(9999);
    try std.testing.expectError(error.TimerNotFound, result);
}

test "ModuleTimerStore: process one-shot timer" {
    const allocator = std.testing.allocator;
    var timer_store = ModuleTimerStore.init(allocator);
    defer timer_store.deinit();
    resetTestState();

    // Create a timer that should fire immediately (0ms period)
    _ = try timer_store.createTimer(
        "test_module",
        0,
        null,
        testCallback,
        null,
    );

    // Wait a bit to ensure timer has passed
    std.time.sleep(10 * std.time.ns_per_ms);

    // Process timers
    const fired = timer_store.processTimers();
    try std.testing.expectEqual(@as(usize, 1), fired);
    try std.testing.expectEqual(@as(u32, 1), test_callback_invocations);

    // One-shot timer should be removed after firing
    try std.testing.expectEqual(@as(usize, 0), timer_store.timerCount());
}

test "ModuleTimerStore: process periodic timer" {
    const allocator = std.testing.allocator;
    var timer_store = ModuleTimerStore.init(allocator);
    defer timer_store.deinit();
    resetTestState();

    // Create a timer with short period
    _ = try timer_store.createTimer(
        "test_module",
        10, // 10ms
        null,
        testCallback,
        null,
    );

    // Wait for timer to fire
    std.time.sleep(15 * std.time.ns_per_ms);
    const fired1 = timer_store.processTimers();
    try std.testing.expectEqual(@as(usize, 1), fired1);
    try std.testing.expectEqual(@as(u32, 1), test_callback_invocations);

    // Periodic timer should still exist
    try std.testing.expectEqual(@as(usize, 1), timer_store.timerCount());

    // Wait for it to fire again
    std.time.sleep(15 * std.time.ns_per_ms);
    const fired2 = timer_store.processTimers();
    try std.testing.expectEqual(@as(usize, 1), fired2);
    try std.testing.expectEqual(@as(u32, 2), test_callback_invocations);

    // Still exists
    try std.testing.expectEqual(@as(usize, 1), timer_store.timerCount());
}

test "ModuleTimerStore: timer with private data" {
    const allocator = std.testing.allocator;
    var timer_store = ModuleTimerStore.init(allocator);
    defer timer_store.deinit();
    resetTestState();

    var test_data: i32 = 42;
    _ = try timer_store.createTimer(
        "test_module",
        0,
        &test_data,
        testCallback,
        null,
    );

    std.time.sleep(10 * std.time.ns_per_ms);
    _ = timer_store.processTimers();

    try std.testing.expect(test_callback_data != null);
    const data_ptr: *i32 = @ptrCast(@alignCast(test_callback_data.?));
    try std.testing.expectEqual(@as(i32, 42), data_ptr.*);
}

test "ModuleTimerStore: removeModuleTimers" {
    const allocator = std.testing.allocator;
    var timer_store = ModuleTimerStore.init(allocator);
    defer timer_store.deinit();
    resetTestState();

    // Create timers for different modules
    _ = try timer_store.createTimer("module_a", 100, null, testCallback, null);
    _ = try timer_store.createTimer("module_a", 100, null, testCallback, null);
    _ = try timer_store.createTimer("module_b", 100, null, testCallback, null);

    try std.testing.expectEqual(@as(usize, 3), timer_store.timerCount());

    // Remove all timers for module_a
    timer_store.removeModuleTimers("module_a");
    try std.testing.expectEqual(@as(usize, 1), timer_store.timerCount());

    // Remove module_b timers
    timer_store.removeModuleTimers("module_b");
    try std.testing.expectEqual(@as(usize, 0), timer_store.timerCount());
}

test "ModuleTimerStore: unique timer IDs" {
    const allocator = std.testing.allocator;
    var timer_store = ModuleTimerStore.init(allocator);
    defer timer_store.deinit();

    const id1 = try timer_store.createTimer("test", 100, null, testCallback, null);
    const id2 = try timer_store.createTimer("test", 100, null, testCallback, null);
    const id3 = try timer_store.createTimer("test", 100, null, testCallback, null);

    try std.testing.expect(id1 != id2);
    try std.testing.expect(id2 != id3);
    try std.testing.expect(id1 != id3);
}

test "ModuleTimerStore: multiple timers fire independently" {
    const allocator = std.testing.allocator;
    var timer_store = ModuleTimerStore.init(allocator);
    defer timer_store.deinit();
    resetTestState();

    // Create 3 one-shot timers
    _ = try timer_store.createTimer("test", 0, null, testCallback, null);
    _ = try timer_store.createTimer("test", 0, null, testCallback, null);
    _ = try timer_store.createTimer("test", 0, null, testCallback, null);

    std.time.sleep(10 * std.time.ns_per_ms);

    const fired = timer_store.processTimers();
    try std.testing.expectEqual(@as(usize, 3), fired);
    try std.testing.expectEqual(@as(u32, 3), test_callback_invocations);
    try std.testing.expectEqual(@as(usize, 0), timer_store.timerCount());
}

test "ModuleTimerStore: processTimers with no timers returns 0" {
    const allocator = std.testing.allocator;
    var timer_store = ModuleTimerStore.init(allocator);
    defer timer_store.deinit();

    const fired = timer_store.processTimers();
    try std.testing.expectEqual(@as(usize, 0), fired);
}

test "ModuleTimerStore: timer not yet ready doesn't fire" {
    const allocator = std.testing.allocator;
    var timer_store = ModuleTimerStore.init(allocator);
    defer timer_store.deinit();
    resetTestState();

    // Create timer with long period
    _ = try timer_store.createTimer("test", 10000, null, testCallback, null);

    // Process immediately - should not fire
    const fired = timer_store.processTimers();
    try std.testing.expectEqual(@as(usize, 0), fired);
    try std.testing.expectEqual(@as(u32, 0), test_callback_invocations);
    try std.testing.expectEqual(@as(usize, 1), timer_store.timerCount());
}
