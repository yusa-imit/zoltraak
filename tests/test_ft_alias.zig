const std = @import("std");
const testing = std.testing;
const Storage = @import("../src/storage/memory.zig").Storage;
const RespValue = @import("../src/protocol/parser.zig").RespValue;

// Storage unit tests

test "SearchStore.addAlias - success" {
    var storage = try Storage.init(testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create index
    try storage.search.createIndex("myindex", .hash);

    // Add alias
    try storage.search.addAlias("myalias", "myindex");

    // Verify alias exists and resolves correctly
    const resolved = try storage.search.getIndexByAlias("myalias");
    try testing.expectEqualStrings("myindex", resolved);
}

test "SearchStore.addAlias - nonexistent index" {
    var storage = try Storage.init(testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Try to add alias for nonexistent index
    const result = storage.search.addAlias("myalias", "nosuchindex");
    try testing.expectError(error.IndexNotFound, result);
}

test "SearchStore.addAlias - duplicate alias" {
    var storage = try Storage.init(testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create index
    try storage.search.createIndex("myindex", .hash);

    // Add alias
    try storage.search.addAlias("myalias", "myindex");

    // Try to add same alias again
    const result = storage.search.addAlias("myalias", "myindex");
    try testing.expectError(error.AliasAlreadyExists, result);
}

test "SearchStore.addAlias - alias equals index name" {
    var storage = try Storage.init(testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create index
    try storage.search.createIndex("myindex", .hash);

    // Try to add alias with same name as index
    const result = storage.search.addAlias("myindex", "myindex");
    try testing.expectError(error.AliasEqualsIndexName, result);
}

test "SearchStore.deleteAlias - success" {
    var storage = try Storage.init(testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create index and alias
    try storage.search.createIndex("myindex", .hash);
    try storage.search.addAlias("myalias", "myindex");

    // Delete alias
    try storage.search.deleteAlias("myalias");

    // Verify alias no longer exists
    const result = storage.search.getIndexByAlias("myalias");
    try testing.expectError(error.IndexNotFound, result);
}

test "SearchStore.deleteAlias - nonexistent" {
    var storage = try Storage.init(testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Try to delete nonexistent alias
    const result = storage.search.deleteAlias("nosuchalias");
    try testing.expectError(error.AliasNotFound, result);
}

test "SearchStore.updateAlias - success" {
    var storage = try Storage.init(testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create two indexes
    try storage.search.createIndex("index1", .hash);
    try storage.search.createIndex("index2", .hash);

    // Add alias pointing to index1
    try storage.search.addAlias("myalias", "index1");

    // Update alias to point to index2
    try storage.search.updateAlias("myalias", "index2");

    // Verify alias now points to index2
    const resolved = try storage.search.getIndexByAlias("myalias");
    try testing.expectEqualStrings("index2", resolved);
}

test "SearchStore.updateAlias - nonexistent alias" {
    var storage = try Storage.init(testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create index
    try storage.search.createIndex("myindex", .hash);

    // Try to update nonexistent alias
    const result = storage.search.updateAlias("nosuchalias", "myindex");
    try testing.expectError(error.AliasNotFound, result);
}

test "SearchStore.updateAlias - nonexistent target index" {
    var storage = try Storage.init(testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create index and alias
    try storage.search.createIndex("myindex", .hash);
    try storage.search.addAlias("myalias", "myindex");

    // Try to update to nonexistent index
    const result = storage.search.updateAlias("myalias", "nosuchindex");
    try testing.expectError(error.IndexNotFound, result);
}

test "SearchStore.getIndexByAlias - resolution" {
    var storage = try Storage.init(testing.allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create index and alias
    try storage.search.createIndex("myindex", .hash);
    try storage.search.addAlias("myalias", "myindex");

    // Resolve by index name - should return same name
    const resolved1 = try storage.search.getIndexByAlias("myindex");
    try testing.expectEqualStrings("myindex", resolved1);

    // Resolve by alias - should return actual index name
    const resolved2 = try storage.search.getIndexByAlias("myalias");
    try testing.expectEqualStrings("myindex", resolved2);

    // Nonexistent - should error
    const result = storage.search.getIndexByAlias("nosuchname");
    try testing.expectError(error.IndexNotFound, result);
}

// Integration tests (RESP protocol)

const strings = @import("../src/commands/strings.zig");

fn executeCommand(storage: *Storage, allocator: std.mem.Allocator, cmd_name: []const u8, args: []const []const u8) ![]const u8 {
    return try strings.handleCommand(storage, allocator, cmd_name, args);
}

test "FT.ALIASADD: basic success" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create index
    const result1 = try executeCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{ "myindex", "ON", "HASH", "SCHEMA", "title", "TEXT" });
    defer allocator.free(result1);

    // Add alias
    const result2 = try executeCommand(&storage, allocator, "FT.ALIASADD", &[_][]const u8{ "myalias", "myindex" });
    defer allocator.free(result2);

    try testing.expect(std.mem.indexOf(u8, result2, "OK") != null);
}

test "FT.ALIASADD: nonexistent index" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try executeCommand(&storage, allocator, "FT.ALIASADD", &[_][]const u8{ "myalias", "nosuchindex" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "Unknown index") != null);
}

test "FT.ALIASADD: duplicate alias" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create index and add alias
    _ = try executeCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{ "myindex", "ON", "HASH", "SCHEMA", "title", "TEXT" });

    const result1 = try executeCommand(&storage, allocator, "FT.ALIASADD", &[_][]const u8{ "myalias", "myindex" });
    defer allocator.free(result1);

    // Try to add same alias again
    const result2 = try executeCommand(&storage, allocator, "FT.ALIASADD", &[_][]const u8{ "myalias", "myindex" });
    defer allocator.free(result2);

    try testing.expect(std.mem.indexOf(u8, result2, "Alias already exists") != null);
}

test "FT.ALIASADD: arity error" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try executeCommand(&storage, allocator, "FT.ALIASADD", &[_][]const u8{"myalias"});
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "wrong number of arguments") != null);
}

test "FT.ALIASDEL: basic success" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create index and alias
    _ = try executeCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{ "myindex", "ON", "HASH", "SCHEMA", "title", "TEXT" });
    _ = try executeCommand(&storage, allocator, "FT.ALIASADD", &[_][]const u8{ "myalias", "myindex" });

    // Delete alias
    const result = try executeCommand(&storage, allocator, "FT.ALIASDEL", &[_][]const u8{"myalias"});
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "OK") != null);
}

test "FT.ALIASDEL: nonexistent alias" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try executeCommand(&storage, allocator, "FT.ALIASDEL", &[_][]const u8{"nosuchalias"});
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "Alias does not exist") != null);
}

test "FT.ALIASUPDATE: basic success" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create two indexes
    _ = try executeCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{ "index1", "ON", "HASH", "SCHEMA", "title", "TEXT" });
    _ = try executeCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{ "index2", "ON", "HASH", "SCHEMA", "body", "TEXT" });

    // Add alias to index1
    _ = try executeCommand(&storage, allocator, "FT.ALIASADD", &[_][]const u8{ "myalias", "index1" });

    // Update alias to index2
    const result = try executeCommand(&storage, allocator, "FT.ALIASUPDATE", &[_][]const u8{ "myalias", "index2" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "OK") != null);
}

test "FT.ALIASUPDATE: nonexistent alias" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create index
    _ = try executeCommand(&storage, allocator, "FT.CREATE", &[_][]const u8{ "myindex", "ON", "HASH", "SCHEMA", "title", "TEXT" });

    // Try to update nonexistent alias
    const result = try executeCommand(&storage, allocator, "FT.ALIASUPDATE", &[_][]const u8{ "nosuchalias", "myindex" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "Alias does not exist") != null);
}
