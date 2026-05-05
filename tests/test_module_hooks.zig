const std = @import("std");
const testing = std.testing;
const ModuleStore = @import("../src/storage/modules.zig").ModuleStore;
const HookType = @import("../src/storage/modules.zig").HookType;
const HookCallback = @import("../src/storage/modules.zig").HookCallback;
const KeyInfo = @import("../src/storage/modules.zig").KeyInfo;

test "ModuleStore: register key space notification hook" {
    var store = try ModuleStore.init(testing.allocator);
    defer store.deinit();

    var call_count: usize = 0;
    const test_hook = struct {
        fn callback(ctx: *anyopaque, event_type: i32, event: []const u8, key: *const KeyInfo) void {
            const count_ptr: *usize = @ptrCast(@alignCast(ctx));
            count_ptr.* += 1;
            _ = event_type;
            _ = event;
            _ = key;
        }
    }.callback;

    // Register hook for key space notifications
    try store.registerHook(.keyspace_notification, "test_module", test_hook, &call_count);

    // Verify hook registered
    const hooks = store.getHooks(.keyspace_notification);
    try testing.expectEqual(@as(usize, 1), hooks.len);
}

test "ModuleStore: trigger key space notification" {
    var store = try ModuleStore.init(testing.allocator);
    defer store.deinit();

    var call_count: usize = 0;
    const test_hook = struct {
        fn callback(ctx: *anyopaque, event_type: i32, event: []const u8, key: *const KeyInfo) void {
            const count_ptr: *usize = @ptrCast(@alignCast(ctx));
            count_ptr.* += 1;
            _ = event_type;
            _ = event;
            _ = key;
        }
    }.callback;

    try store.registerHook(.keyspace_notification, "test_module", test_hook, &call_count);

    // Trigger notification
    const key_info = KeyInfo{
        .key = "mykey",
        .db = 0,
    };
    try store.triggerKeyspaceNotification(0, "set", &key_info);

    // Verify hook was called
    try testing.expectEqual(@as(usize, 1), call_count);
}

test "ModuleStore: multiple hooks for same event" {
    var store = try ModuleStore.init(testing.allocator);
    defer store.deinit();

    var call_count1: usize = 0;
    var call_count2: usize = 0;

    const hook1 = struct {
        fn callback(ctx: *anyopaque, event_type: i32, event: []const u8, key: *const KeyInfo) void {
            const count_ptr: *usize = @ptrCast(@alignCast(ctx));
            count_ptr.* += 1;
            _ = event_type;
            _ = event;
            _ = key;
        }
    }.callback;

    const hook2 = struct {
        fn callback(ctx: *anyopaque, event_type: i32, event: []const u8, key: *const KeyInfo) void {
            const count_ptr: *usize = @ptrCast(@alignCast(ctx));
            count_ptr.* += 1;
            _ = event_type;
            _ = event;
            _ = key;
        }
    }.callback;

    try store.registerHook(.keyspace_notification, "module1", hook1, &call_count1);
    try store.registerHook(.keyspace_notification, "module2", hook2, &call_count2);

    const key_info = KeyInfo{
        .key = "testkey",
        .db = 0,
    };
    try store.triggerKeyspaceNotification(0, "set", &key_info);

    // Both hooks should be called
    try testing.expectEqual(@as(usize, 1), call_count1);
    try testing.expectEqual(@as(usize, 1), call_count2);
}

test "ModuleStore: remove module hooks on unload" {
    var store = try ModuleStore.init(testing.allocator);
    defer store.deinit();

    var call_count: usize = 0;
    const test_hook = struct {
        fn callback(ctx: *anyopaque, event_type: i32, event: []const u8, key: *const KeyInfo) void {
            const count_ptr: *usize = @ptrCast(@alignCast(ctx));
            count_ptr.* += 1;
            _ = event_type;
            _ = event;
            _ = key;
        }
    }.callback;

    try store.registerHook(.keyspace_notification, "test_module", test_hook, &call_count);

    // Remove all hooks for module
    try store.removeModuleHooks("test_module");

    // Verify hooks removed
    const hooks = store.getHooks(.keyspace_notification);
    try testing.expectEqual(@as(usize, 0), hooks.len);
}

test "ModuleStore: register command filter hook" {
    var store = try ModuleStore.init(testing.allocator);
    defer store.deinit();

    var filter_count: usize = 0;
    const filter_hook = struct {
        fn callback(ctx: *anyopaque, event_type: i32, event: []const u8, key: *const KeyInfo) void {
            const count_ptr: *usize = @ptrCast(@alignCast(ctx));
            count_ptr.* += 1;
            _ = event_type;
            _ = event;
            _ = key;
        }
    }.callback;

    try store.registerHook(.command_filter, "filter_module", filter_hook, &filter_count);

    const hooks = store.getHooks(.command_filter);
    try testing.expectEqual(@as(usize, 1), hooks.len);
}

test "ModuleStore: trigger command filter" {
    var store = try ModuleStore.init(testing.allocator);
    defer store.deinit();

    var filter_count: usize = 0;
    const filter_hook = struct {
        fn callback(ctx: *anyopaque, event_type: i32, event: []const u8, key: *const KeyInfo) void {
            const count_ptr: *usize = @ptrCast(@alignCast(ctx));
            count_ptr.* += 1;
            _ = event_type;
            _ = event;
            _ = key;
        }
    }.callback;

    try store.registerHook(.command_filter, "filter_module", filter_hook, &filter_count);

    const key_info = KeyInfo{
        .key = "SET",
        .db = 0,
    };
    try store.triggerCommandFilter(&key_info);

    try testing.expectEqual(@as(usize, 1), filter_count);
}

test "ModuleStore: register client state change hook" {
    var store = try ModuleStore.init(testing.allocator);
    defer store.deinit();

    var state_count: usize = 0;
    const state_hook = struct {
        fn callback(ctx: *anyopaque, event_type: i32, event: []const u8, key: *const KeyInfo) void {
            const count_ptr: *usize = @ptrCast(@alignCast(ctx));
            count_ptr.* += 1;
            _ = event_type;
            _ = event;
            _ = key;
        }
    }.callback;

    try store.registerHook(.client_state_change, "state_module", state_hook, &state_count);

    const hooks = store.getHooks(.client_state_change);
    try testing.expectEqual(@as(usize, 1), hooks.len);
}

test "ModuleStore: hook registration with null context" {
    var store = try ModuleStore.init(testing.allocator);
    defer store.deinit();

    const null_ctx_hook = struct {
        fn callback(ctx: *anyopaque, event_type: i32, event: []const u8, key: *const KeyInfo) void {
            // Context can be null - just verify it doesn't crash
            _ = ctx;
            _ = event_type;
            _ = event;
            _ = key;
        }
    }.callback;

    try store.registerHook(.keyspace_notification, "null_module", null_ctx_hook, null);

    const key_info = KeyInfo{
        .key = "testkey",
        .db = 0,
    };
    try store.triggerKeyspaceNotification(0, "set", &key_info);
}

test "ModuleStore: get hooks for specific type" {
    var store = try ModuleStore.init(testing.allocator);
    defer store.deinit();

    var count1: usize = 0;
    var count2: usize = 0;

    const hook_fn = struct {
        fn callback(ctx: *anyopaque, event_type: i32, event: []const u8, key: *const KeyInfo) void {
            _ = ctx;
            _ = event_type;
            _ = event;
            _ = key;
        }
    }.callback;

    try store.registerHook(.keyspace_notification, "mod1", hook_fn, &count1);
    try store.registerHook(.command_filter, "mod2", hook_fn, &count2);

    // Get only keyspace hooks
    const ks_hooks = store.getHooks(.keyspace_notification);
    try testing.expectEqual(@as(usize, 1), ks_hooks.len);

    // Get only command filter hooks
    const cf_hooks = store.getHooks(.command_filter);
    try testing.expectEqual(@as(usize, 1), cf_hooks.len);
}
