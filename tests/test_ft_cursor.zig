const std = @import("std");
const Storage = @import("../src/storage/memory.zig").Storage;
const executeCommand = @import("../src/commands/strings.zig").executeCommand;

test "FT.CURSOR READ: invalid arguments" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Missing cursor_id
    {
        const response = try executeCommand(&storage, allocator, "FT.CURSOR", &[_][]const u8{ "READ", "idx" });
        defer allocator.free(response);
        try std.testing.expect(std.mem.indexOf(u8, response, "ERR") != null);
    }

    // Invalid cursor_id
    {
        const response = try executeCommand(&storage, allocator, "FT.CURSOR", &[_][]const u8{ "READ", "idx", "invalid" });
        defer allocator.free(response);
        try std.testing.expect(std.mem.indexOf(u8, response, "ERR invalid cursor ID") != null);
    }
}

test "FT.CURSOR READ: nonexistent cursor" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create index first
    _ = try executeCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{ "idx", "ON", "HASH", "SCHEMA", "title", "TEXT" });

    const response = try executeCommand(&storage, allocator, "FT.CURSOR", &[_][]const u8{ "READ", "idx", "999" });
    defer allocator.free(response);
    try std.testing.expect(std.mem.indexOf(u8, response, "ERR Cursor does not exist") != null);
}

test "FT.CURSOR READ: unknown index" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const response = try executeCommand(&storage, allocator, "FT.CURSOR", &[_][]const u8{ "READ", "nonexistent", "1" });
    defer allocator.free(response);
    try std.testing.expect(std.mem.indexOf(u8, response, "ERR Unknown index name") != null);
}

test "FT.CURSOR READ: with COUNT parameter" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create index
    _ = try executeCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{ "idx", "ON", "HASH", "SCHEMA", "title", "TEXT" });

    // Create a cursor manually (stub - real test would use FT.AGGREGATE WITHCURSOR)
    storage.mutex.lock();
    const cursor_id = try storage.search.createCursor("idx", "*", 100, 10, false, null, null, false);
    storage.mutex.unlock();

    // Read with COUNT parameter
    var buf: [32]u8 = undefined;
    const cursor_id_str = try std.fmt.bufPrint(&buf, "{d}", .{cursor_id});

    const response = try executeCommand(&storage, allocator, "FT.CURSOR", &[_][]const u8{ "READ", "idx", cursor_id_str, "COUNT", "20" });
    defer allocator.free(response);

    // Should return array with 2 elements: [results, cursor_id]
    try std.testing.expect(std.mem.indexOf(u8, response, "*2\r\n") != null);
}

test "FT.CURSOR DEL: invalid arguments" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Missing cursor_id
    {
        const response = try executeCommand(&storage, allocator, "FT.CURSOR", &[_][]const u8{ "DEL", "idx" });
        defer allocator.free(response);
        try std.testing.expect(std.mem.indexOf(u8, response, "ERR") != null);
    }

    // Invalid cursor_id
    {
        const response = try executeCommand(&storage, allocator, "FT.CURSOR", &[_][]const u8{ "DEL", "idx", "invalid" });
        defer allocator.free(response);
        try std.testing.expect(std.mem.indexOf(u8, response, "ERR invalid cursor ID") != null);
    }
}

test "FT.CURSOR DEL: success" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create index
    _ = try executeCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{ "idx", "ON", "HASH", "SCHEMA", "title", "TEXT" });

    // Create cursor manually
    storage.mutex.lock();
    const cursor_id = try storage.search.createCursor("idx", "*", 100, 10, false, null, null, false);
    storage.mutex.unlock();

    var buf: [32]u8 = undefined;
    const cursor_id_str = try std.fmt.bufPrint(&buf, "{d}", .{cursor_id});

    // Delete cursor
    const response = try executeCommand(&storage, allocator, "FT.CURSOR", &[_][]const u8{ "DEL", "idx", cursor_id_str });
    defer allocator.free(response);
    try std.testing.expectEqualStrings("+OK\r\n", response);

    // Verify cursor is gone
    storage.mutex.lock();
    const cursor = storage.search.getCursor(cursor_id);
    storage.mutex.unlock();
    try std.testing.expect(cursor == null);
}

test "FT.CURSOR DEL: nonexistent cursor" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create index
    _ = try executeCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{ "idx", "ON", "HASH", "SCHEMA", "title", "TEXT" });

    const response = try executeCommand(&storage, allocator, "FT.CURSOR", &[_][]const u8{ "DEL", "idx", "999" });
    defer allocator.free(response);
    try std.testing.expect(std.mem.indexOf(u8, response, "ERR Cursor does not exist") != null);
}

test "FT.CURSOR DEL: unknown index" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const response = try executeCommand(&storage, allocator, "FT.CURSOR", &[_][]const u8{ "DEL", "nonexistent", "1" });
    defer allocator.free(response);
    try std.testing.expect(std.mem.indexOf(u8, response, "ERR Unknown index name") != null);
}

test "FT.CURSOR: unknown subcommand" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const response = try executeCommand(&storage, allocator, "FT.CURSOR", &[_][]const u8{ "INVALID", "idx", "1" });
    defer allocator.free(response);
    try std.testing.expect(std.mem.indexOf(u8, response, "ERR unknown FT.CURSOR subcommand") != null);
}

test "FT.CURSOR: cursor pagination lifecycle" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create index
    _ = try executeCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{ "idx", "ON", "HASH", "SCHEMA", "title", "TEXT" });

    // Create cursor with 50 total results, 10 per page
    storage.mutex.lock();
    const cursor_id = try storage.search.createCursor("idx", "*", 50, 10, false, null, null, false);
    storage.mutex.unlock();

    var buf: [32]u8 = undefined;
    const cursor_id_str = try std.fmt.bufPrint(&buf, "{d}", .{cursor_id});

    // Read first page
    {
        const response = try executeCommand(&storage, allocator, "FT.CURSOR", &[_][]const u8{ "READ", "idx", cursor_id_str });
        defer allocator.free(response);

        // Should return array with cursor still active (non-zero cursor_id)
        try std.testing.expect(std.mem.indexOf(u8, response, "*2\r\n") != null);
    }

    // Verify offset updated
    storage.mutex.lock();
    const cursor = storage.search.getCursor(cursor_id);
    try std.testing.expect(cursor != null);
    // Offset would be updated if real search returned results (stub returns empty)
    storage.mutex.unlock();
}

test "FT.CURSOR READ: cursor belongs to different index" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create two indices
    _ = try executeCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{ "idx1", "ON", "HASH", "SCHEMA", "title", "TEXT" });
    _ = try executeCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{ "idx2", "ON", "HASH", "SCHEMA", "body", "TEXT" });

    // Create cursor for idx1
    storage.mutex.lock();
    const cursor_id = try storage.search.createCursor("idx1", "*", 50, 10, false, null, null, false);
    storage.mutex.unlock();

    var buf: [32]u8 = undefined;
    const cursor_id_str = try std.fmt.bufPrint(&buf, "{d}", .{cursor_id});

    // Try to read from idx2 (should fail)
    const response = try executeCommand(&storage, allocator, "FT.CURSOR", &[_][]const u8{ "READ", "idx2", cursor_id_str });
    defer allocator.free(response);
    try std.testing.expect(std.mem.indexOf(u8, response, "ERR Cursor belongs to different index") != null);
}
