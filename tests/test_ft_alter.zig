const std = @import("std");
const Storage = @import("../src/storage/memory.zig").Storage;
const search_cmds = @import("../src/commands/search.zig");
const RespValue = @import("../src/protocol/writer.zig").RespValue;

test "FT.ALTER: add TEXT field to existing index" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    // Create index first
    const create_args = [_][]const u8{ "myindex", "ON", "HASH", "SCHEMA", "field1", "TEXT" };
    const create_result = try search_cmds.cmdFtCreate(&storage, allocator, &create_args);
    try std.testing.expectEqualStrings("OK", create_result.simple_string);

    // Alter index: add new field
    const alter_args = [_][]const u8{ "myindex", "SCHEMA", "ADD", "field2", "NUMERIC" };
    const alter_result = try search_cmds.cmdFtAlter(&storage, allocator, &alter_args);

    try std.testing.expectEqualStrings("OK", alter_result.simple_string);

    // Verify field was added via FT.INFO
    const info_args = [_][]const u8{"myindex"};
    const info_result = try search_cmds.cmdFtInfo(&storage, allocator, &info_args);
    defer deinitRespValue(allocator, info_result);

    // Check that info contains attributes section with 2 fields
    const arr = info_result.array;
    var found_attributes = false;
    for (arr, 0..) |elem, i| {
        if (elem == .bulk_string and std.mem.eql(u8, elem.bulk_string, "attributes")) {
            found_attributes = true;
            // Next element should be the attributes array
            if (i + 1 < arr.len) {
                const attrs = arr[i + 1].array;
                // Each field is an array: should have 2 fields
                try std.testing.expectEqual(@as(usize, 2), attrs.len);
            }
            break;
        }
    }
    try std.testing.expect(found_attributes);
}

test "FT.ALTER: add field with SORTABLE option" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    // Create index
    const create_args = [_][]const u8{ "idx", "ON", "JSON", "SCHEMA", "name", "TEXT" };
    _ = try search_cmds.cmdFtCreate(&storage, allocator, &create_args);

    // Add field with SORTABLE
    const alter_args = [_][]const u8{ "idx", "SCHEMA", "ADD", "age", "NUMERIC", "SORTABLE" };
    const result = try search_cmds.cmdFtAlter(&storage, allocator, &alter_args);

    try std.testing.expectEqualStrings("OK", result.simple_string);
}

test "FT.ALTER: add field with AS alias" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    const create_args = [_][]const u8{ "idx", "ON", "HASH", "SCHEMA", "f1", "TAG" };
    _ = try search_cmds.cmdFtCreate(&storage, allocator, &create_args);

    // Add field with alias
    const alter_args = [_][]const u8{ "idx", "SCHEMA", "ADD", "full_name", "TEXT", "AS", "name" };
    const result = try search_cmds.cmdFtAlter(&storage, allocator, &alter_args);

    try std.testing.expectEqualStrings("OK", result.simple_string);
}

test "FT.ALTER: error on nonexistent index" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    const args = [_][]const u8{ "nonexistent", "SCHEMA", "ADD", "field", "TEXT" };
    const result = try search_cmds.cmdFtAlter(&storage, allocator, &args);

    try std.testing.expectEqualStrings("ERR Unknown index name", result.error_string);
}

test "FT.ALTER: error on wrong arity" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    // Too few arguments
    const args = [_][]const u8{ "idx", "SCHEMA", "ADD" };
    const result = try search_cmds.cmdFtAlter(&storage, allocator, &args);

    try std.testing.expectEqualStrings("ERR wrong number of arguments for 'FT.ALTER' command", result.error_string);
}

test "FT.ALTER: error on missing SCHEMA keyword" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    const create_args = [_][]const u8{ "idx", "ON", "HASH", "SCHEMA", "f1", "TEXT" };
    _ = try search_cmds.cmdFtCreate(&storage, allocator, &create_args);

    const args = [_][]const u8{ "idx", "ADD", "field", "TEXT" };
    const result = try search_cmds.cmdFtAlter(&storage, allocator, &args);

    try std.testing.expectEqualStrings("ERR syntax error, expected SCHEMA after index name", result.error_string);
}

test "FT.ALTER: error on missing ADD keyword" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    const create_args = [_][]const u8{ "idx", "ON", "HASH", "SCHEMA", "f1", "TEXT" };
    _ = try search_cmds.cmdFtCreate(&storage, allocator, &create_args);

    const args = [_][]const u8{ "idx", "SCHEMA", "field", "TEXT" };
    const result = try search_cmds.cmdFtAlter(&storage, allocator, &args);

    try std.testing.expectEqualStrings("ERR syntax error, expected ADD after SCHEMA", result.error_string);
}

test "FT.ALTER: error on invalid field type" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    const create_args = [_][]const u8{ "idx", "ON", "HASH", "SCHEMA", "f1", "TEXT" };
    _ = try search_cmds.cmdFtCreate(&storage, allocator, &create_args);

    const args = [_][]const u8{ "idx", "SCHEMA", "ADD", "field", "INVALID" };
    const result = try search_cmds.cmdFtAlter(&storage, allocator, &args);

    try std.testing.expectEqualStrings("ERR invalid field type", result.error_string);
}

test "FT.ALTER: add multiple fields" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    const create_args = [_][]const u8{ "idx", "ON", "HASH", "SCHEMA", "f1", "TEXT" };
    _ = try search_cmds.cmdFtCreate(&storage, allocator, &create_args);

    // Add first field
    const alter1_args = [_][]const u8{ "idx", "SCHEMA", "ADD", "f2", "NUMERIC" };
    const result1 = try search_cmds.cmdFtAlter(&storage, allocator, &alter1_args);
    try std.testing.expectEqualStrings("OK", result1.simple_string);

    // Add second field
    const alter2_args = [_][]const u8{ "idx", "SCHEMA", "ADD", "f3", "TAG" };
    const result2 = try search_cmds.cmdFtAlter(&storage, allocator, &alter2_args);
    try std.testing.expectEqualStrings("OK", result2.simple_string);

    // Verify 3 fields total
    const info_args = [_][]const u8{"idx"};
    const info_result = try search_cmds.cmdFtInfo(&storage, allocator, &info_args);
    defer deinitRespValue(allocator, info_result);

    const arr = info_result.array;
    for (arr, 0..) |elem, i| {
        if (elem == .bulk_string and std.mem.eql(u8, elem.bulk_string, "attributes")) {
            if (i + 1 < arr.len) {
                const attrs = arr[i + 1].array;
                try std.testing.expectEqual(@as(usize, 3), attrs.len);
            }
            break;
        }
    }
}

test "FT.ALTER: add field with multiple options" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, .{});
    defer storage.deinit();

    const create_args = [_][]const u8{ "idx", "ON", "JSON", "SCHEMA", "f1", "TEXT" };
    _ = try search_cmds.cmdFtCreate(&storage, allocator, &create_args);

    // Add field with SORTABLE and NOINDEX
    const alter_args = [_][]const u8{ "idx", "SCHEMA", "ADD", "f2", "TEXT", "SORTABLE", "NOINDEX", "NOSTEM" };
    const result = try search_cmds.cmdFtAlter(&storage, allocator, &alter_args);

    try std.testing.expectEqualStrings("OK", result.simple_string);
}

/// Helper to recursively clean up RespValue
fn deinitRespValue(allocator: std.mem.Allocator, value: RespValue) void {
    switch (value) {
        .array => |arr| {
            for (arr) |elem| {
                deinitRespValue(allocator, elem);
            }
            allocator.free(arr);
        },
        else => {},
    }
}
