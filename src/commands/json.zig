const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const storage_mod = @import("../storage/memory.zig");
const json_value_mod = @import("../storage/json_value.zig");
const jsonpath_mod = @import("../storage/jsonpath.zig");

const RespValue = protocol.RespValue;
const Storage = storage_mod.Storage;
const Value = storage_mod.Value;
const JsonNode = json_value_mod.JsonNode;
const JsonPath = jsonpath_mod.JsonPath;

/// JSON.SET key path value [NX|XX]
/// Sets a JSON value at the specified path
pub fn cmdJsonSet(
    storage: *Storage,
    args: []const RespValue,
    allocator: std.mem.Allocator,
) !RespValue {
    if (args.len < 4 or args.len > 5) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'json.set' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    const path_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid path" },
    };

    const json_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid JSON value" },
    };

    // Parse optional NX/XX flag
    var nx_mode = false;
    var xx_mode = false;
    if (args.len == 5) {
        const flag = switch (args[4]) {
            .bulk_string => |s| s,
            else => return RespValue{ .error_string = "ERR invalid option" },
        };

        if (std.ascii.eqlIgnoreCase(flag, "NX")) {
            nx_mode = true;
        } else if (std.ascii.eqlIgnoreCase(flag, "XX")) {
            xx_mode = true;
        } else {
            return RespValue{ .error_string = "ERR syntax error" };
        }
    }

    // Parse path
    var path = JsonPath.parse(allocator, path_str) catch {
        return RespValue{ .error_string = "ERR invalid path syntax" };
    };
    defer path.deinit();

    // Parse JSON value
    const new_node = JsonNode.parse(storage.allocator, json_str) catch {
        return RespValue{ .error_string = "ERR invalid JSON string" };
    };
    errdefer {
        new_node.deinit(storage.allocator);
        storage.allocator.destroy(new_node);
    }

    // Check if path is root - only root paths can create keys
    if (!path.canCreate()) {
        // Non-root path - key must exist
        const existing = storage.data.get(key);
        if (existing == null) {
            new_node.deinit(storage.allocator);
            storage.allocator.destroy(new_node);
            return RespValue{ .null_bulk_string = {} };
        }

        // Key exists but not JSON type
        if (existing.? != .json) {
            new_node.deinit(storage.allocator);
            storage.allocator.destroy(new_node);
            return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
        }

        // XX mode - must exist at path (we can't easily check this for non-root paths)
        // NX mode - must not exist at path
        // For simplicity in Phase 12.1, we only support NX/XX on root paths
        new_node.deinit(storage.allocator);
        storage.allocator.destroy(new_node);
        return RespValue{ .error_string = "ERR path must be root for JSON.SET with NX/XX" };
    }

    // Root path - check NX/XX conditions
    const existing = storage.data.get(key);
    if (nx_mode and existing != null) {
        new_node.deinit(storage.allocator);
        storage.allocator.destroy(new_node);
        return RespValue{ .null_bulk_string = {} };
    }

    if (xx_mode and existing == null) {
        new_node.deinit(storage.allocator);
        storage.allocator.destroy(new_node);
        return RespValue{ .null_bulk_string = {} };
    }

    // Create or replace JSON value
    const json_value = Value{
        .json = .{
            .root = new_node,
            .expires_at = null,
            .allocator = storage.allocator,
        },
    };

    // Store the value
    if (storage.data.getPtr(key)) |value_ptr| {
        // Key exists - replace it
        value_ptr.deinit(storage.allocator);
        value_ptr.* = json_value;
    } else {
        // Key doesn't exist - create it
        const owned_key = try storage.allocator.dupe(u8, key);
        errdefer storage.allocator.free(owned_key);
        try storage.data.put(owned_key, json_value);
    }

    return RespValue{ .simple_string = "OK" };
}

/// JSON.GET key [path]
/// Gets a JSON value from the specified path
pub fn cmdJsonGet(
    storage: *Storage,
    args: []const RespValue,
    allocator: std.mem.Allocator,
) !RespValue {
    if (args.len < 2 or args.len > 3) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'json.get' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    const path_str = if (args.len == 3)
        switch (args[2]) {
            .bulk_string => |s| s,
            else => return RespValue{ .error_string = "ERR invalid path" },
        }
    else
        "$"; // Default to root

    // Check if key exists
    const entry = storage.data.get(key) orelse {
        return RespValue{ .null_bulk_string = {} };
    };

    // Check if value is JSON
    if (entry != .json) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    // Parse path
    var path = JsonPath.parse(allocator, path_str) catch {
        return RespValue{ .error_string = "ERR invalid path syntax" };
    };
    defer path.deinit();

    // Evaluate path
    const json_val = &entry.json;
    var results = try path.evaluate(json_val.root, allocator);
    defer results.deinit(allocator);

    // Convert results to JSON string
    if (results.items.len == 0) {
        // No matches - return empty array
        const empty = try allocator.dupe(u8, "[]");
        return RespValue{ .bulk_string = empty };
    }

    if (results.items.len == 1) {
        // Single result
        const json_str = try results.items[0].stringify(allocator);
        return RespValue{ .bulk_string = json_str };
    }

    // Multiple results - return as array
    var buf : std.ArrayList(u8) = .{};
    errdefer buf.deinit(allocator);

    try buf.append(allocator, '[');
    for (results.items, 0..) |node, i| {
        if (i > 0) try buf.append(allocator, ',');
        const node_str = try node.stringify(allocator);
        defer allocator.free(node_str);
        try buf.appendSlice(allocator, node_str);
    }
    try buf.append(allocator, ']');

    const result = try buf.toOwnedSlice(allocator);
    return RespValue{ .bulk_string = result };
}

/// JSON.DEL key [path]
/// Deletes values at the specified path
pub fn cmdJsonDel(
    storage: *Storage,
    args: []const RespValue,
    allocator: std.mem.Allocator,
) !RespValue {
    if (args.len < 2 or args.len > 3) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'json.del' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    const path_str = if (args.len == 3)
        switch (args[2]) {
            .bulk_string => |s| s,
            else => return RespValue{ .error_string = "ERR invalid path" },
        }
    else
        "$"; // Default to root

    // Check if key exists
    const entry = storage.data.get(key) orelse {
        return RespValue{ .integer = 0 };
    };

    // Check if value is JSON
    if (entry != .json) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    // Parse path
    var path = JsonPath.parse(allocator, path_str) catch {
        return RespValue{ .error_string = "ERR invalid path syntax" };
    };
    defer path.deinit();

    // If path is root, delete the entire key
    if (path.canCreate()) {
        // Delete the key
        if (storage.data.fetchRemove(key)) |kv| {
            storage.allocator.free(kv.key);
            var val = kv.value;
            val.deinit(storage.allocator);
            return RespValue{ .integer = 1 };
        }
        return RespValue{ .integer = 0 };
    }

    // For non-root paths, we would need to delete matching nodes
    // This is complex and deferred to future iterations
    // For now, return 0
    return RespValue{ .integer = 0 };
}

/// JSON.TYPE key [path]
/// Returns the type of the value at the specified path
pub fn cmdJsonType(
    storage: *Storage,
    args: []const RespValue,
    allocator: std.mem.Allocator,
) !RespValue {
    if (args.len < 2 or args.len > 3) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'json.type' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    const path_str = if (args.len == 3)
        switch (args[2]) {
            .bulk_string => |s| s,
            else => return RespValue{ .error_string = "ERR invalid path" },
        }
    else
        "$"; // Default to root

    // Check if key exists
    const entry = storage.data.get(key) orelse {
        return RespValue{ .null_bulk_string = {} };
    };

    // Check if value is JSON
    if (entry != .json) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    // Parse path
    var path = JsonPath.parse(allocator, path_str) catch {
        return RespValue{ .error_string = "ERR invalid path syntax" };
    };
    defer path.deinit();

    // Evaluate path
    const json_val = &entry.json;
    var results = try path.evaluate(json_val.root, allocator);
    defer results.deinit(allocator);

    // Return type name(s)
    if (results.items.len == 0) {
        return RespValue{ .null_bulk_string = {} };
    }

    if (results.items.len == 1) {
        // Single result - return type name
        const type_name = results.items[0].typeName();
        const owned = try allocator.dupe(u8, type_name);
        return RespValue{ .bulk_string = owned };
    }

    // Multiple results - return array of type names
    var arr: std.ArrayList(RespValue) = .{};
    errdefer {
        for (arr.items) |item| {
            switch (item) {
                .bulk_string => |s| allocator.free(s),
                else => {},
            }
        }
        arr.deinit(allocator);
    }

    for (results.items) |node| {
        const type_name = node.typeName();
        const owned = try allocator.dupe(u8, type_name);
        try arr.append(allocator, RespValue{ .bulk_string = owned });
    }

    const result = try arr.toOwnedSlice(allocator);
    return RespValue{ .array = result };
}

// ============================================================================
// Unit Tests
// ============================================================================

test "JSON.SET creates key with root path" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":1}" },
    };

    const result = try cmdJsonSet(&storage, &args, allocator);
    try std.testing.expectEqualStrings("OK", result.simple_string);

    // Verify value was stored
    const entry = storage.data.get("doc").?;
    try std.testing.expect(entry == .json);
}

test "JSON.SET with NX flag" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // First set should succeed
    const args1 = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":1}" },
        RespValue{ .bulk_string = "NX" },
    };

    const result1 = try cmdJsonSet(&storage, &args1, allocator);
    try std.testing.expectEqualStrings("OK", result1.simple_string);

    // Second set with NX should fail
    const args2 = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"b\":2}" },
        RespValue{ .bulk_string = "NX" },
    };

    const result2 = try cmdJsonSet(&storage, &args2, allocator);
    try std.testing.expect(result2 == .null_bulk_string);
}

test "JSON.SET with XX flag" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set with XX on non-existent key should fail
    const args1 = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":1}" },
        RespValue{ .bulk_string = "XX" },
    };

    const result1 = try cmdJsonSet(&storage, &args1, allocator);
    try std.testing.expect(result1 == .null_bulk_string);

    // Create key without XX
    const args2 = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":1}" },
    };

    const result2 = try cmdJsonSet(&storage, &args2, allocator);
    try std.testing.expectEqualStrings("OK", result2.simple_string);

    // Now XX should succeed
    const args3 = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"b\":2}" },
        RespValue{ .bulk_string = "XX" },
    };

    const result3 = try cmdJsonSet(&storage, &args3, allocator);
    try std.testing.expectEqualStrings("OK", result3.simple_string);
}

test "JSON.GET returns entire document" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set a JSON document
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":1,\"b\":2}" },
    };

    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Get the document
    const get_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.GET" },
        RespValue{ .bulk_string = "doc" },
    };

    const result = try cmdJsonGet(&storage, &get_args, allocator);
    defer allocator.free(result.bulk_string);

    // Parse result to verify it's valid JSON
    const node = try JsonNode.parse(allocator, result.bulk_string);
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    try std.testing.expect(node.* == .object);
}

test "JSON.GET with path $.a" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set a JSON document
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":1,\"b\":2}" },
    };

    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Get field "a"
    const get_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.GET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$.a" },
    };

    const result = try cmdJsonGet(&storage, &get_args, allocator);
    defer allocator.free(result.bulk_string);

    try std.testing.expectEqualStrings("1", result.bulk_string);
}

test "JSON.GET on non-existent key returns nil" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.GET" },
        RespValue{ .bulk_string = "nonexistent" },
    };

    const result = try cmdJsonGet(&storage, &args, allocator);
    try std.testing.expect(result == .null_bulk_string);
}

test "JSON.DEL deletes entire key" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set a JSON document
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":1}" },
    };

    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Delete the document
    const del_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.DEL" },
        RespValue{ .bulk_string = "doc" },
    };

    const result = try cmdJsonDel(&storage, &del_args, allocator);
    try std.testing.expectEqual(@as(i64, 1), result.integer);

    // Verify key is gone
    try std.testing.expect(storage.data.get("doc") == null);
}

test "JSON.DEL on non-existent key returns 0" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.DEL" },
        RespValue{ .bulk_string = "nonexistent" },
    };

    const result = try cmdJsonDel(&storage, &args, allocator);
    try std.testing.expectEqual(@as(i64, 0), result.integer);
}

test "JSON.TYPE returns correct type" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set a JSON document with various types
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":1,\"b\":\"hello\",\"c\":true,\"d\":null,\"e\":[],\"f\":{}}" },
    };

    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Check type of root
    const root_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.TYPE" },
        RespValue{ .bulk_string = "doc" },
    };

    const root_result = try cmdJsonType(&storage, &root_args, allocator);
    defer allocator.free(root_result.bulk_string);
    try std.testing.expectEqualStrings("object", root_result.bulk_string);

    // Check type of number field
    const num_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.TYPE" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$.a" },
    };

    const num_result = try cmdJsonType(&storage, &num_args, allocator);
    defer allocator.free(num_result.bulk_string);
    try std.testing.expectEqualStrings("number", num_result.bulk_string);

    // Check type of string field
    const str_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.TYPE" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$.b" },
    };

    const str_result = try cmdJsonType(&storage, &str_args, allocator);
    defer allocator.free(str_result.bulk_string);
    try std.testing.expectEqualStrings("string", str_result.bulk_string);
}

test "JSON.TYPE on non-existent key returns nil" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.TYPE" },
        RespValue{ .bulk_string = "nonexistent" },
    };

    const result = try cmdJsonType(&storage, &args, allocator);
    try std.testing.expect(result == .null_bulk_string);
}
