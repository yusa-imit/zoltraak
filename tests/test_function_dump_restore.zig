const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const Writer = @import("../src/protocol/writer.zig").Writer;
const Storage = @import("../src/storage/memory.zig").Storage;
const functions = @import("../src/commands/functions.zig");

const RespValue = protocol.RespValue;

test "FUNCTION DUMP empty store" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{ .max_memory = 1024 * 1024 * 100 });
    defer storage.deinit();

    var args = [_][]const u8{ "FUNCTION", "DUMP" };
    const result = try functions.cmdFunctionDump(allocator, &storage, &args);
    defer {
        switch (result) {
            .bulk_string => |s| allocator.free(s),
            else => {},
        }
    }

    try std.testing.expect(result == .bulk_string);
    const payload = result.bulk_string;

    // Should contain magic header + 0 libraries
    try std.testing.expectEqual(@as(usize, 12), payload.len);
    try std.testing.expectEqualSlices(u8, "ZOLFUNC\x01", payload[0..8]);
}

test "FUNCTION DUMP and RESTORE single library" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{ .max_memory = 1024 * 1024 * 100 });
    defer storage.deinit();

    // Load a library
    const code =
        \\#!lua name=mylib
        \\redis.register_function("func1", function() return 42 end)
        \\redis.register_function("func2", function() return "hello" end)
    ;
    var load_args = [_][]const u8{ "FUNCTION", "LOAD", code };
    const load_result = try functions.cmdFunctionLoad(allocator, &storage, &load_args);
    defer {
        switch (load_result) {
            .bulk_string => |s| allocator.free(s),
            .simple_string => |s| allocator.free(s),
            .error_string => |s| allocator.free(s),
            else => {},
        }
    }
    try std.testing.expect(load_result != .error_string);

    // Dump
    var dump_args = [_][]const u8{ "FUNCTION", "DUMP" };
    const dump_result = try functions.cmdFunctionDump(allocator, &storage, &dump_args);
    defer {
        switch (dump_result) {
            .bulk_string => |s| allocator.free(s),
            else => {},
        }
    }
    try std.testing.expect(dump_result == .bulk_string);
    const payload = dump_result.bulk_string;

    // Create new storage and restore
    var storage2 = try Storage.init(allocator, .{ .max_memory = 1024 * 1024 * 100 });
    defer storage2.deinit();

    var restore_args = [_][]const u8{ "FUNCTION", "RESTORE", payload };
    const restore_result = try functions.cmdFunctionRestore(allocator, &storage2, &restore_args);
    defer {
        switch (restore_result) {
            .simple_string => |s| allocator.free(s),
            .error_string => |s| allocator.free(s),
            else => {},
        }
    }
    try std.testing.expect(restore_result == .simple_string);
    try std.testing.expectEqualStrings("OK", restore_result.simple_string);

    // Verify restored library exists
    storage2.mutex.lock();
    defer storage2.mutex.unlock();
    const lib = storage2.functions.getLibrary("mylib");
    try std.testing.expect(lib != null);
    try std.testing.expectEqual(@as(usize, 2), lib.?.functions.count());
}

test "FUNCTION RESTORE with FLUSH mode" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{ .max_memory = 1024 * 1024 * 100 });
    defer storage.deinit();

    // Load first library
    const code1 =
        \\#!lua name=lib1
        \\redis.register_function("func1", function() return 1 end)
    ;
    var load1_args = [_][]const u8{ "FUNCTION", "LOAD", code1 };
    const load1_result = try functions.cmdFunctionLoad(allocator, &storage, &load1_args);
    defer {
        switch (load1_result) {
            .bulk_string => |s| allocator.free(s),
            .simple_string => |s| allocator.free(s),
            .error_string => |s| allocator.free(s),
            else => {},
        }
    }

    // Dump first library
    var dump_args = [_][]const u8{ "FUNCTION", "DUMP" };
    const dump_result = try functions.cmdFunctionDump(allocator, &storage, &dump_args);
    defer {
        switch (dump_result) {
            .bulk_string => |s| allocator.free(s),
            else => {},
        }
    }
    const payload = dump_result.bulk_string;

    // Load second library
    const code2 =
        \\#!lua name=lib2
        \\redis.register_function("func2", function() return 2 end)
    ;
    var load2_args = [_][]const u8{ "FUNCTION", "LOAD", code2 };
    const load2_result = try functions.cmdFunctionLoad(allocator, &storage, &load2_args);
    defer {
        switch (load2_result) {
            .bulk_string => |s| allocator.free(s),
            .simple_string => |s| allocator.free(s),
            .error_string => |s| allocator.free(s),
            else => {},
        }
    }

    storage.mutex.lock();
    try std.testing.expectEqual(@as(usize, 2), storage.functions.libraries.count());
    storage.mutex.unlock();

    // Restore with FLUSH should remove lib2 and restore lib1
    var restore_args = [_][]const u8{ "FUNCTION", "RESTORE", payload, "FLUSH" };
    const restore_result = try functions.cmdFunctionRestore(allocator, &storage, &restore_args);
    defer {
        switch (restore_result) {
            .simple_string => |s| allocator.free(s),
            .error_string => |s| allocator.free(s),
            else => {},
        }
    }
    try std.testing.expect(restore_result == .simple_string);

    storage.mutex.lock();
    defer storage.mutex.unlock();
    try std.testing.expectEqual(@as(usize, 1), storage.functions.libraries.count());
    try std.testing.expect(storage.functions.getLibrary("lib1") != null);
    try std.testing.expect(storage.functions.getLibrary("lib2") == null);
}

test "FUNCTION RESTORE with APPEND mode duplicate error" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{ .max_memory = 1024 * 1024 * 100 });
    defer storage.deinit();

    // Load a library
    const code =
        \\#!lua name=mylib
        \\redis.register_function("func1", function() return 1 end)
    ;
    var load_args = [_][]const u8{ "FUNCTION", "LOAD", code };
    const load_result = try functions.cmdFunctionLoad(allocator, &storage, &load_args);
    defer {
        switch (load_result) {
            .bulk_string => |s| allocator.free(s),
            .simple_string => |s| allocator.free(s),
            .error_string => |s| allocator.free(s),
            else => {},
        }
    }

    // Dump
    var dump_args = [_][]const u8{ "FUNCTION", "DUMP" };
    const dump_result = try functions.cmdFunctionDump(allocator, &storage, &dump_args);
    defer {
        switch (dump_result) {
            .bulk_string => |s| allocator.free(s),
            else => {},
        }
    }
    const payload = dump_result.bulk_string;

    // Attempt to restore with APPEND (default mode) should fail
    var restore_args = [_][]const u8{ "FUNCTION", "RESTORE", payload, "APPEND" };
    const restore_result = try functions.cmdFunctionRestore(allocator, &storage, &restore_args);
    defer {
        switch (restore_result) {
            .simple_string => |s| allocator.free(s),
            .error_string => |s| allocator.free(s),
            else => {},
        }
    }
    try std.testing.expect(restore_result == .error_string);
    try std.testing.expect(std.mem.indexOf(u8, restore_result.error_string, "already exists") != null);
}

test "FUNCTION RESTORE with REPLACE mode" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{ .max_memory = 1024 * 1024 * 100 });
    defer storage.deinit();

    // Load first version
    const code1 =
        \\#!lua name=mylib
        \\redis.register_function("old_func", function() return "old" end)
    ;
    var load1_args = [_][]const u8{ "FUNCTION", "LOAD", code1 };
    const load1_result = try functions.cmdFunctionLoad(allocator, &storage, &load1_args);
    defer {
        switch (load1_result) {
            .bulk_string => |s| allocator.free(s),
            .simple_string => |s| allocator.free(s),
            .error_string => |s| allocator.free(s),
            else => {},
        }
    }

    // Create second version in temp storage
    var storage2 = try Storage.init(allocator, .{ .max_memory = 1024 * 1024 * 100 });
    defer storage2.deinit();

    const code2 =
        \\#!lua name=mylib
        \\redis.register_function("new_func", function() return "new" end)
    ;
    var load2_args = [_][]const u8{ "FUNCTION", "LOAD", code2 };
    const load2_result = try functions.cmdFunctionLoad(allocator, &storage2, &load2_args);
    defer {
        switch (load2_result) {
            .bulk_string => |s| allocator.free(s),
            .simple_string => |s| allocator.free(s),
            .error_string => |s| allocator.free(s),
            else => {},
        }
    }

    // Dump second version
    var dump_args = [_][]const u8{ "FUNCTION", "DUMP" };
    const dump_result = try functions.cmdFunctionDump(allocator, &storage2, &dump_args);
    defer {
        switch (dump_result) {
            .bulk_string => |s| allocator.free(s),
            else => {},
        }
    }
    const payload = dump_result.bulk_string;

    // Restore with REPLACE should replace old version
    var restore_args = [_][]const u8{ "FUNCTION", "RESTORE", payload, "REPLACE" };
    const restore_result = try functions.cmdFunctionRestore(allocator, &storage, &restore_args);
    defer {
        switch (restore_result) {
            .simple_string => |s| allocator.free(s),
            .error_string => |s| allocator.free(s),
            else => {},
        }
    }
    try std.testing.expect(restore_result == .simple_string);

    storage.mutex.lock();
    defer storage.mutex.unlock();
    try std.testing.expect(storage.functions.getFunction("old_func") == null);
    try std.testing.expect(storage.functions.getFunction("new_func") != null);
}

test "FUNCTION RESTORE invalid payload" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{ .max_memory = 1024 * 1024 * 100 });
    defer storage.deinit();

    // Invalid magic header
    const invalid_payload = "INVALID\x01DATA";
    var restore_args = [_][]const u8{ "FUNCTION", "RESTORE", invalid_payload };
    const restore_result = try functions.cmdFunctionRestore(allocator, &storage, &restore_args);
    defer {
        switch (restore_result) {
            .simple_string => |s| allocator.free(s),
            .error_string => |s| allocator.free(s),
            else => {},
        }
    }
    try std.testing.expect(restore_result == .error_string);
    try std.testing.expect(std.mem.indexOf(u8, restore_result.error_string, "invalid payload") != null);
}

test "FUNCTION RESTORE invalid mode" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{ .max_memory = 1024 * 1024 * 100 });
    defer storage.deinit();

    const dummy_payload = "ZOLFUNC\x01\x00\x00\x00\x00"; // Valid magic + 0 libraries
    var restore_args = [_][]const u8{ "FUNCTION", "RESTORE", dummy_payload, "INVALID_MODE" };
    const restore_result = try functions.cmdFunctionRestore(allocator, &storage, &restore_args);
    defer {
        switch (restore_result) {
            .simple_string => |s| allocator.free(s),
            .error_string => |s| allocator.free(s),
            else => {},
        }
    }
    try std.testing.expect(restore_result == .error_string);
    try std.testing.expect(std.mem.indexOf(u8, restore_result.error_string, "invalid") != null);
}

test "FUNCTION DUMP arity error" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{ .max_memory = 1024 * 1024 * 100 });
    defer storage.deinit();

    var args = [_][]const u8{ "FUNCTION", "DUMP", "extra_arg" };
    const result = try functions.cmdFunctionDump(allocator, &storage, &args);
    defer {
        switch (result) {
            .error_string => |s| allocator.free(s),
            else => {},
        }
    }
    try std.testing.expect(result == .error_string);
}

test "FUNCTION RESTORE arity error" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{ .max_memory = 1024 * 1024 * 100 });
    defer storage.deinit();

    var args = [_][]const u8{ "FUNCTION", "RESTORE" };
    const result = try functions.cmdFunctionRestore(allocator, &storage, &args);
    defer {
        switch (result) {
            .error_string => |s| allocator.free(s),
            else => {},
        }
    }
    try std.testing.expect(result == .error_string);
}
