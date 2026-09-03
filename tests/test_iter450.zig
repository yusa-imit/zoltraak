const std = @import("std");
const zoltraak = @import("zoltraak");
const storage_mod = zoltraak.storage;
const modules_mod = zoltraak.modules;

const Storage = storage_mod.Storage;

// Iteration 450: ModuleStore.getUsedMemoryRatio() (backing RedisModule_GetUsedMemoryRatio())
// was a permanent stub returning 0.0 regardless of actual maxmemory config or live memory
// usage, so any module calling it always saw "unlimited/no pressure" even when the server
// was configured with a real maxmemory limit and near/over it. Fixed by wiring a back-pointer
// from ModuleStore to its owning Storage (set once Storage.init()'s pointer is stable) and
// computing used/limit from storage.memory_tracker.current_allocated vs the parsed
// "maxmemory" config value.

test "getUsedMemoryRatio returns 0.0 when maxmemory is unconfigured (default 0 = unlimited)" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const ratio = storage.module_store.getUsedMemoryRatio();
    try std.testing.expectEqual(@as(f32, 0.0), ratio);
}

test "getUsedMemoryRatio computes used/limit once maxmemory is configured" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("maxmemory", "1000");
    storage.memory_tracker.current_allocated = 250;

    const ratio = storage.module_store.getUsedMemoryRatio();
    try std.testing.expectApproxEqAbs(@as(f32, 0.25), ratio, 0.0001);
}

test "getUsedMemoryRatio can exceed 1.0 when over the configured limit" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("maxmemory", "1000");
    storage.memory_tracker.current_allocated = 1500;

    const ratio = storage.module_store.getUsedMemoryRatio();
    try std.testing.expectApproxEqAbs(@as(f32, 1.5), ratio, 0.0001);
}

test "getUsedMemoryRatio returns 0.0 again once maxmemory is reset to 0" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("maxmemory", "1000");
    storage.memory_tracker.current_allocated = 900;
    try std.testing.expect(storage.module_store.getUsedMemoryRatio() > 0.0);

    try storage.config.set("maxmemory", "0");
    const ratio = storage.module_store.getUsedMemoryRatio();
    try std.testing.expectEqual(@as(f32, 0.0), ratio);
}

test "ModuleCtx.getUsedMemoryRatio delegates to the owning ModuleStore" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set("maxmemory", "2000");
    storage.memory_tracker.current_allocated = 1000;

    var ctx = modules_mod.ModuleCtx{ .name = "testmod", .ver = 1, .store = &storage.module_store };
    const ratio = ctx.getUsedMemoryRatio();
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), ratio, 0.0001);
}
