const std = @import("std");
const testing = std.testing;
const modules = @import("../src/storage/modules.zig");
const ModuleStore = modules.ModuleStore;
const ModuleCtx = modules.ModuleCtx;
const ModuleDataType = modules.ModuleDataType;

// Mock callbacks for testing
fn mockRdbSave(data: *anyopaque, writer: anytype) anyerror!void {
    _ = data;
    _ = writer;
}

fn mockRdbLoad(allocator: std.mem.Allocator, reader: anytype, encver: u32) anyerror!*anyopaque {
    _ = reader;
    _ = encver;
    const ptr = try allocator.create(u8);
    ptr.* = 42;
    return @ptrCast(ptr);
}

fn mockAofRewrite(key: []const u8, data: *anyopaque, writer: anytype) anyerror!void {
    _ = key;
    _ = data;
    _ = writer;
}

fn mockMemUsage(data: *const anyopaque) usize {
    _ = data;
    return 1024;
}

fn mockFree(allocator: std.mem.Allocator, data: *anyopaque) void {
    const ptr: *u8 = @ptrCast(@alignCast(data));
    allocator.destroy(ptr);
}

test "ModuleStore: registerDataType basic registration" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Register a data type
    try store.registerDataType(
        "mymodule",
        "MYTYPE",
        1,
        mockRdbSave,
        mockRdbLoad,
        mockAofRewrite,
        mockMemUsage,
        mockFree,
    );

    // Verify data type is registered
    const dt = store.getDataType("MYTYPE");
    try testing.expect(dt != null);
    if (dt) |d| {
        try testing.expectEqualStrings("MYTYPE", d.name);
        try testing.expectEqualStrings("mymodule", d.module_name);
        try testing.expectEqual(@as(u32, 1), d.encver);
        try testing.expect(d.rdb_save != null);
        try testing.expect(d.rdb_load != null);
        try testing.expect(d.aof_rewrite != null);
        try testing.expect(d.mem_usage != null);
    }
}

test "ModuleStore: registerDataType with minimal callbacks" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Register data type with only required callback (free)
    try store.registerDataType(
        "mymodule",
        "MINTYPE",
        1,
        null, // no rdb_save
        null, // no rdb_load
        null, // no aof_rewrite
        null, // no mem_usage
        mockFree,
    );

    // Verify data type is registered with nulls
    const dt = store.getDataType("MINTYPE");
    try testing.expect(dt != null);
    if (dt) |d| {
        try testing.expectEqualStrings("MINTYPE", d.name);
        try testing.expect(d.rdb_save == null);
        try testing.expect(d.rdb_load == null);
        try testing.expect(d.aof_rewrite == null);
        try testing.expect(d.mem_usage == null);
    }
}

test "ModuleStore: registerDataType empty name error" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Empty name should return InvalidDataTypeName
    const result = store.registerDataType(
        "mymodule",
        "",
        1,
        null,
        null,
        null,
        null,
        mockFree,
    );
    try testing.expectError(error.InvalidDataTypeName, result);
}

test "ModuleStore: registerDataType duplicate error" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Register first time - should succeed
    try store.registerDataType(
        "mymodule",
        "DUPTYPE",
        1,
        null,
        null,
        null,
        null,
        mockFree,
    );

    // Register again with same name - should fail
    const result = store.registerDataType(
        "othermodule",
        "DUPTYPE",
        2,
        null,
        null,
        null,
        null,
        mockFree,
    );
    try testing.expectError(error.DataTypeAlreadyExists, result);
}

test "ModuleStore: getDataType nonexistent returns null" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Get nonexistent data type should return null
    const dt = store.getDataType("NONEXISTENT");
    try testing.expect(dt == null);
}

test "ModuleStore: removeModuleDataTypes removes all module types" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Register multiple data types from different modules
    try store.registerDataType("mymodule", "TYPE1", 1, null, null, null, null, mockFree);
    try store.registerDataType("mymodule", "TYPE2", 1, null, null, null, null, mockFree);
    try store.registerDataType("othermodule", "TYPE3", 1, null, null, null, null, mockFree);

    // Remove mymodule data types
    store.removeModuleDataTypes("mymodule");

    // Verify mymodule types are removed
    try testing.expect(store.getDataType("TYPE1") == null);
    try testing.expect(store.getDataType("TYPE2") == null);

    // Verify other module type still exists
    try testing.expect(store.getDataType("TYPE3") != null);
}

test "ModuleStore: unloadModule removes data types" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Add a module (stub - no actual library)
    const module_info = modules.ModuleInfo{
        .name = try allocator.dupe(u8, "mymodule"),
        .ver = 1,
        .path = try allocator.dupe(u8, "/path/to/module.so"),
        .args = try allocator.alloc([]const u8, 0),
        .lib = null,
    };

    const module_key = try allocator.dupe(u8, "mymodule");
    try store.modules.put(module_key, module_info);

    // Register data types for this module
    try store.registerDataType("mymodule", "TYPE1", 1, null, null, null, null, mockFree);
    try store.registerDataType("mymodule", "TYPE2", 1, null, null, null, null, mockFree);

    // Unload module
    try store.unloadModule("mymodule");

    // Verify data types are removed
    try testing.expect(store.getDataType("TYPE1") == null);
    try testing.expect(store.getDataType("TYPE2") == null);
}

test "ModuleCtx: createDataType method" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Create context
    var ctx = ModuleCtx{
        .name = "testmodule",
        .ver = 1,
        .store = &store,
    };

    // Register data type via context method
    try ctx.createDataType(
        "CTXTYPE",
        1,
        mockRdbSave,
        mockRdbLoad,
        mockAofRewrite,
        mockMemUsage,
        mockFree,
    );

    // Verify data type is registered
    const dt = store.getDataType("CTXTYPE");
    try testing.expect(dt != null);
    if (dt) |d| {
        try testing.expectEqualStrings("testmodule", d.module_name);
        try testing.expectEqualStrings("CTXTYPE", d.name);
    }
}

test "ModuleDataType: callbacks are callable" {
    const allocator = testing.allocator;
    var store = ModuleStore.init(allocator);
    defer store.deinit();

    // Register data type with all callbacks
    try store.registerDataType(
        "mymodule",
        "CALLTYPE",
        1,
        mockRdbSave,
        mockRdbLoad,
        mockAofRewrite,
        mockMemUsage,
        mockFree,
    );

    const dt = store.getDataType("CALLTYPE");
    try testing.expect(dt != null);

    if (dt) |d| {
        // Test memory usage callback
        if (d.mem_usage) |mem_fn| {
            const dummy_data: u8 = 42;
            const size = mem_fn(@ptrCast(&dummy_data));
            try testing.expectEqual(@as(usize, 1024), size);
        }

        // Test RDB load callback (returns allocated data that needs freeing)
        if (d.rdb_load) |load_fn| {
            var dummy_reader = std.io.fixedBufferStream(&[_]u8{});
            const loaded = try load_fn(allocator, dummy_reader.reader(), 1);
            defer d.free(allocator, loaded);

            const ptr: *u8 = @ptrCast(@alignCast(loaded));
            try testing.expectEqual(@as(u8, 42), ptr.*);
        }
    }
}
