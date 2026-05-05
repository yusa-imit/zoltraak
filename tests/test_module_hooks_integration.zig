const std = @import("std");
const testing = std.testing;
const Storage = @import("../src/storage/memory.zig").Storage;
const RespValue = @import("../src/protocol/parser.zig").RespValue;
const HookType = @import("../src/storage/modules.zig").HookType;
const KeyInfo = @import("../src/storage/modules.zig").KeyInfo;

// Integration tests for module hooks via commands

test "MODULE: hook registration survives module lifecycle" {
    var storage = try Storage.init(testing.allocator, 1000000, 16);
    defer storage.deinit();

    var call_count: usize = 0;
    const test_hook = struct {
        fn callback(ctx: ?*anyopaque, event_type: i32, event: []const u8, key: *const KeyInfo) void {
            if (ctx) |c| {
                const count_ptr: *usize = @ptrCast(@alignCast(c));
                count_ptr.* += 1;
            }
            _ = event_type;
            _ = event;
            _ = key;
        }
    }.callback;

    // Register hook directly (simulating module registration during OnLoad)
    try storage.module_store.registerHook(.keyspace_notification, "test_module", test_hook, &call_count);

    // Verify hook count
    const hooks = storage.module_store.getHooks(.keyspace_notification);
    try testing.expectEqual(@as(usize, 1), hooks.len);

    // Clean up
    testing.allocator.free(hooks);
}

test "MODULE: hooks cleaned up on module unload" {
    var storage = try Storage.init(testing.allocator, 1000000, 16);
    defer storage.deinit();

    var call_count: usize = 0;
    const test_hook = struct {
        fn callback(ctx: ?*anyopaque, event_type: i32, event: []const u8, key: *const KeyInfo) void {
            if (ctx) |c| {
                const count_ptr: *usize = @ptrCast(@alignCast(c));
                count_ptr.* += 1;
            }
            _ = event_type;
            _ = event;
            _ = key;
        }
    }.callback;

    // Register hook
    try storage.module_store.registerHook(.keyspace_notification, "cleanup_test", test_hook, &call_count);

    // Verify hook registered
    {
        const hooks = storage.module_store.getHooks(.keyspace_notification);
        defer testing.allocator.free(hooks);
        try testing.expectEqual(@as(usize, 1), hooks.len);
    }

    // Remove hooks for module
    try storage.module_store.removeModuleHooks("cleanup_test");

    // Verify hooks removed
    {
        const hooks = storage.module_store.getHooks(.keyspace_notification);
        defer testing.allocator.free(hooks);
        try testing.expectEqual(@as(usize, 0), hooks.len);
    }
}

test "MODULE: multiple modules can register hooks" {
    var storage = try Storage.init(testing.allocator, 1000000, 16);
    defer storage.deinit();

    var count1: usize = 0;
    var count2: usize = 0;
    var count3: usize = 0;

    const hook_fn = struct {
        fn callback(ctx: ?*anyopaque, event_type: i32, event: []const u8, key: *const KeyInfo) void {
            if (ctx) |c| {
                const count_ptr: *usize = @ptrCast(@alignCast(c));
                count_ptr.* += 1;
            }
            _ = event_type;
            _ = event;
            _ = key;
        }
    }.callback;

    // Register hooks from different modules
    try storage.module_store.registerHook(.keyspace_notification, "module1", hook_fn, &count1);
    try storage.module_store.registerHook(.keyspace_notification, "module2", hook_fn, &count2);
    try storage.module_store.registerHook(.command_filter, "module3", hook_fn, &count3);

    // Verify all hooks registered
    {
        const ks_hooks = storage.module_store.getHooks(.keyspace_notification);
        defer testing.allocator.free(ks_hooks);
        try testing.expectEqual(@as(usize, 2), ks_hooks.len);
    }

    {
        const cf_hooks = storage.module_store.getHooks(.command_filter);
        defer testing.allocator.free(cf_hooks);
        try testing.expectEqual(@as(usize, 1), cf_hooks.len);
    }

    // Trigger keyspace notification
    const key_info = KeyInfo{
        .key = "testkey",
        .db = 0,
    };
    try storage.module_store.triggerKeyspaceNotification(0, "set", &key_info);

    // Only keyspace hooks should be called
    try testing.expectEqual(@as(usize, 1), count1);
    try testing.expectEqual(@as(usize, 1), count2);
    try testing.expectEqual(@as(usize, 0), count3);
}

test "MODULE: hook types are isolated" {
    var storage = try Storage.init(testing.allocator, 1000000, 16);
    defer storage.deinit();

    var ks_count: usize = 0;
    var cf_count: usize = 0;
    var cs_count: usize = 0;

    const hook_fn = struct {
        fn callback(ctx: ?*anyopaque, event_type: i32, event: []const u8, key: *const KeyInfo) void {
            if (ctx) |c| {
                const count_ptr: *usize = @ptrCast(@alignCast(c));
                count_ptr.* += 1;
            }
            _ = event_type;
            _ = event;
            _ = key;
        }
    }.callback;

    // Register different hook types
    try storage.module_store.registerHook(.keyspace_notification, "mod", hook_fn, &ks_count);
    try storage.module_store.registerHook(.command_filter, "mod", hook_fn, &cf_count);
    try storage.module_store.registerHook(.client_state_change, "mod", hook_fn, &cs_count);

    // Trigger keyspace notification only
    const key_info = KeyInfo{
        .key = "key1",
        .db = 0,
    };
    try storage.module_store.triggerKeyspaceNotification(0, "set", &key_info);

    // Only keyspace hook should be called
    try testing.expectEqual(@as(usize, 1), ks_count);
    try testing.expectEqual(@as(usize, 0), cf_count);
    try testing.expectEqual(@as(usize, 0), cs_count);

    // Trigger command filter
    const cmd_info = KeyInfo{
        .key = "GET",
        .db = 0,
    };
    try storage.module_store.triggerCommandFilter(&cmd_info);

    // Now command filter should be called
    try testing.expectEqual(@as(usize, 1), ks_count);
    try testing.expectEqual(@as(usize, 1), cf_count);
    try testing.expectEqual(@as(usize, 0), cs_count);
}

test "MODULE: hook with null context doesn't crash" {
    var storage = try Storage.init(testing.allocator, 1000000, 16);
    defer storage.deinit();

    const null_ctx_hook = struct {
        fn callback(ctx: ?*anyopaque, event_type: i32, event: []const u8, key: *const KeyInfo) void {
            _ = ctx;
            _ = event_type;
            _ = event;
            _ = key;
            // Just verify it doesn't crash with null context
        }
    }.callback;

    try storage.module_store.registerHook(.keyspace_notification, "null_mod", null_ctx_hook, null);

    const key_info = KeyInfo{
        .key = "testkey",
        .db = 0,
    };
    try storage.module_store.triggerKeyspaceNotification(0, "set", &key_info);
}

test "MODULE: removing specific module hooks" {
    var storage = try Storage.init(testing.allocator, 1000000, 16);
    defer storage.deinit();

    var count1: usize = 0;
    var count2: usize = 0;

    const hook_fn = struct {
        fn callback(ctx: ?*anyopaque, event_type: i32, event: []const u8, key: *const KeyInfo) void {
            if (ctx) |c| {
                const count_ptr: *usize = @ptrCast(@alignCast(c));
                count_ptr.* += 1;
            }
            _ = event_type;
            _ = event;
            _ = key;
        }
    }.callback;

    // Register hooks from two modules
    try storage.module_store.registerHook(.keyspace_notification, "mod_a", hook_fn, &count1);
    try storage.module_store.registerHook(.keyspace_notification, "mod_b", hook_fn, &count2);

    // Remove only mod_a hooks
    try storage.module_store.removeModuleHooks("mod_a");

    // Trigger notification
    const key_info = KeyInfo{
        .key = "key",
        .db = 0,
    };
    try storage.module_store.triggerKeyspaceNotification(0, "set", &key_info);

    // Only mod_b hook should be called
    try testing.expectEqual(@as(usize, 0), count1);
    try testing.expectEqual(@as(usize, 1), count2);
}

test "MODULE: hook event parameter passing" {
    var storage = try Storage.init(testing.allocator, 1000000, 16);
    defer storage.deinit();

    const EventCapture = struct {
        event_type: i32 = 0,
        event_name: []const u8 = "",
        key_name: []const u8 = "",
    };

    var capture = EventCapture{};

    const capture_hook = struct {
        fn callback(ctx: ?*anyopaque, event_type: i32, event: []const u8, key: *const KeyInfo) void {
            if (ctx) |c| {
                const cap: *EventCapture = @ptrCast(@alignCast(c));
                cap.event_type = event_type;
                cap.event_name = event;
                cap.key_name = key.key;
            }
        }
    }.callback;

    try storage.module_store.registerHook(.keyspace_notification, "capture_mod", capture_hook, &capture);

    // Trigger with specific parameters
    const key_info = KeyInfo{
        .key = "mykey",
        .db = 3,
    };
    try storage.module_store.triggerKeyspaceNotification(42, "expire", &key_info);

    // Verify parameters were passed correctly
    try testing.expectEqual(@as(i32, 42), capture.event_type);
    try testing.expectEqualStrings("expire", capture.event_name);
    try testing.expectEqualStrings("mykey", capture.key_name);
}
