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

/// JSON.MGET key [key ...] path
/// Gets JSON values from multiple keys at the specified path
pub fn cmdJsonMget(
    storage: *Storage,
    args: []const RespValue,
    allocator: std.mem.Allocator,
) !RespValue {
    if (args.len < 3) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'json.mget' command" };
    }

    // Last argument is the path
    const path_str = switch (args[args.len - 1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid path" },
    };

    // Parse path once
    var path = JsonPath.parse(allocator, path_str) catch {
        return RespValue{ .error_string = "ERR invalid path syntax" };
    };
    defer path.deinit();

    // Build result array
    var results: std.ArrayList(RespValue) = .{};
    errdefer {
        for (results.items) |item| {
            switch (item) {
                .bulk_string => |s| allocator.free(s),
                else => {},
            }
        }
        results.deinit(allocator);
    }

    // Process each key (all but the last arg)
    var i: usize = 1;
    while (i < args.len - 1) : (i += 1) {
        const key = switch (args[i]) {
            .bulk_string => |s| s,
            else => {
                try results.append(allocator, RespValue{ .null_bulk_string = {} });
                continue;
            },
        };

        // Check if key exists
        const entry = storage.data.get(key);
        if (entry == null) {
            try results.append(allocator, RespValue{ .null_bulk_string = {} });
            continue;
        }

        // Check if value is JSON
        if (entry.? != .json) {
            try results.append(allocator, RespValue{ .null_bulk_string = {} });
            continue;
        }

        // Evaluate path
        const json_val = &entry.?.json;
        var path_results = try path.evaluate(json_val.root, allocator);
        defer path_results.deinit(allocator);

        // Convert results to JSON string
        if (path_results.items.len == 0) {
            try results.append(allocator, RespValue{ .null_bulk_string = {} });
        } else if (path_results.items.len == 1) {
            // Single result
            const json_str = try path_results.items[0].stringify(allocator);
            try results.append(allocator, RespValue{ .bulk_string = json_str });
        } else {
            // Multiple results - return as array string
            var buf: std.ArrayList(u8) = .{};
            errdefer buf.deinit(allocator);

            try buf.append(allocator, '[');
            for (path_results.items, 0..) |node, j| {
                if (j > 0) try buf.append(allocator, ',');
                const node_str = try node.stringify(allocator);
                defer allocator.free(node_str);
                try buf.appendSlice(allocator, node_str);
            }
            try buf.append(allocator, ']');

            const result = try buf.toOwnedSlice(allocator);
            try results.append(allocator, RespValue{ .bulk_string = result });
        }
    }

    const result = try results.toOwnedSlice(allocator);
    return RespValue{ .array = result };
}

/// JSON.NUMINCRBY key path value
/// Increments a numeric value at the specified path
pub fn cmdJsonNumincrby(
    storage: *Storage,
    args: []const RespValue,
    allocator: std.mem.Allocator,
) !RespValue {
    if (args.len != 4) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'json.numincrby' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    const path_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid path" },
    };

    const increment_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid increment value" },
    };

    // Parse increment value
    const increment = std.fmt.parseFloat(f64, increment_str) catch {
        return RespValue{ .error_string = "ERR increment value is not a number" };
    };

    // Check if key exists
    const entry = storage.data.getPtr(key) orelse {
        return RespValue{ .error_string = "ERR key does not exist" };
    };

    // Check if value is JSON
    if (entry.* != .json) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    // Parse path
    var path = JsonPath.parse(allocator, path_str) catch {
        return RespValue{ .error_string = "ERR invalid path syntax" };
    };
    defer path.deinit();

    // Evaluate path to find the target node
    const json_val = &entry.json;
    var results = try path.evaluate(json_val.root, allocator);
    defer results.deinit(allocator);

    if (results.items.len == 0) {
        return RespValue{ .error_string = "ERR path does not exist" };
    }

    // For now, only support incrementing a single value (first result)
    const target_node = results.items[0];
    if (target_node.* != .number) {
        return RespValue{ .error_string = "ERR path value is not a number" };
    }

    // Increment the value
    target_node.number += increment;

    // Return the new value as a string
    var buf: [32]u8 = undefined;
    const result_str = try std.fmt.bufPrint(&buf, "{d}", .{target_node.number});
    const owned = try allocator.dupe(u8, result_str);
    return RespValue{ .bulk_string = owned };
}

/// JSON.NUMMULTBY key path value
/// Multiplies a numeric value at the specified path
pub fn cmdJsonNummultby(
    storage: *Storage,
    args: []const RespValue,
    allocator: std.mem.Allocator,
) !RespValue {
    if (args.len != 4) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'json.nummultby' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    const path_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid path" },
    };

    const multiplier_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid multiplier value" },
    };

    // Parse multiplier value
    const multiplier = std.fmt.parseFloat(f64, multiplier_str) catch {
        return RespValue{ .error_string = "ERR multiplier value is not a number" };
    };

    // Check if key exists
    const entry = storage.data.getPtr(key) orelse {
        return RespValue{ .error_string = "ERR key does not exist" };
    };

    // Check if value is JSON
    if (entry.* != .json) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    // Parse path
    var path = JsonPath.parse(allocator, path_str) catch {
        return RespValue{ .error_string = "ERR invalid path syntax" };
    };
    defer path.deinit();

    // Evaluate path to find the target node
    const json_val = &entry.json;
    var results = try path.evaluate(json_val.root, allocator);
    defer results.deinit(allocator);

    if (results.items.len == 0) {
        return RespValue{ .error_string = "ERR path does not exist" };
    }

    // For now, only support multiplying a single value (first result)
    const target_node = results.items[0];
    if (target_node.* != .number) {
        return RespValue{ .error_string = "ERR path value is not a number" };
    }

    // Multiply the number
    target_node.number *= multiplier;

    // Return the new value as a string
    var buf: [32]u8 = undefined;
    const result_str = try std.fmt.bufPrint(&buf, "{d}", .{target_node.number});
    const owned = try allocator.dupe(u8, result_str);
    return RespValue{ .bulk_string = owned };
}

/// JSON.MSET key1 path1 value1 [key2 path2 value2 ...]
/// Sets multiple JSON values atomically
pub fn cmdJsonMset(
    storage: *Storage,
    args: []const RespValue,
    allocator: std.mem.Allocator,
) !RespValue {
    // Need at least command + 3 args (key path value), and must be in triplets
    if (args.len < 4 or (args.len - 1) % 3 != 0) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'json.mset' command" };
    }

    const num_sets = (args.len - 1) / 3;

    // First pass: validate all arguments and parse all JSON documents
    var parsed_docs = try allocator.alloc(Value.JsonValue, num_sets);
    defer {
        for (parsed_docs) |*doc| {
            doc.deinit(allocator);
        }
        allocator.free(parsed_docs);
    }

    var keys = try allocator.alloc([]const u8, num_sets);
    defer allocator.free(keys);

    var paths = try allocator.alloc([]const u8, num_sets);
    defer allocator.free(paths);

    for (0..num_sets) |i| {
        const arg_idx = 1 + i * 3;

        // Extract key
        keys[i] = switch (args[arg_idx]) {
            .bulk_string => |s| s,
            else => return RespValue{ .error_string = "ERR invalid key" },
        };

        // Extract path
        paths[i] = switch (args[arg_idx + 1]) {
            .bulk_string => |s| s,
            else => return RespValue{ .error_string = "ERR invalid path" },
        };

        // Parse path to check if it's root
        var path = JsonPath.parse(allocator, paths[i]) catch {
            return RespValue{ .error_string = "ERR invalid path syntax" };
        };
        defer path.deinit();

        if (!path.canCreate()) {
            return RespValue{ .error_string = "ERR new objects must be created at the root" };
        }

        // Extract and parse JSON value
        const json_str = switch (args[arg_idx + 2]) {
            .bulk_string => |s| s,
            else => return RespValue{ .error_string = "ERR invalid JSON value" },
        };

        const node = JsonNode.parse(allocator, json_str) catch {
            return RespValue{ .error_string = "ERR invalid JSON syntax" };
        };
        parsed_docs[i] = Value.JsonValue{
            .root = node,
            .expires_at = null,
            .allocator = allocator,
        };
    }

    // Second pass: atomically set all values
    for (0..num_sets) |i| {
        // Clone the parsed document for storage
        const cloned_root = try parsed_docs[i].root.clone(allocator);
        const cloned = Value.JsonValue{
            .root = cloned_root,
            .expires_at = null,
            .allocator = allocator,
        };
        errdefer {
            cloned_root.deinit(allocator);
            allocator.destroy(cloned_root);
        }

        // Check if key exists
        if (storage.data.getPtr(keys[i])) |entry| {
            // Key exists - replace if it's JSON, error if wrong type
            if (entry.* != .json) {
                // Clean up the clone before returning error
                cloned_root.deinit(allocator);
                allocator.destroy(cloned_root);
                return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
            }

            // Free old JSON value
            entry.json.deinit(allocator);
            entry.* = .{ .json = cloned };
        } else {
            // Key doesn't exist - create new entry
            try storage.data.put(try allocator.dupe(u8, keys[i]), .{ .json = cloned });
        }
    }

    return RespValue{ .simple_string = "OK" };
}

/// JSON.FORGET key [path]
/// Alias for JSON.DEL - deletes JSON value at path
pub fn cmdJsonForget(
    storage: *Storage,
    args: []const RespValue,
    allocator: std.mem.Allocator,
) !RespValue {
    // JSON.FORGET is just an alias for JSON.DEL
    return cmdJsonDel(storage, args, allocator);
}

test "JSON.MGET with multiple keys" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set multiple JSON documents
    const set_args1 = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":1}" },
    };
    _ = try cmdJsonSet(&storage, &set_args1, allocator);

    const set_args2 = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc2" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":2}" },
    };
    _ = try cmdJsonSet(&storage, &set_args2, allocator);

    // MGET both documents
    const mget_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.MGET" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "doc2" },
        RespValue{ .bulk_string = "$.a" },
    };

    const result = try cmdJsonMget(&storage, &mget_args, allocator);
    defer {
        for (result.array) |item| {
            switch (item) {
                .bulk_string => |s| allocator.free(s),
                else => {},
            }
        }
        allocator.free(result.array);
    }

    try std.testing.expectEqual(@as(usize, 2), result.array.len);
    try std.testing.expectEqualStrings("1", result.array[0].bulk_string);
    try std.testing.expectEqualStrings("2", result.array[1].bulk_string);
}

test "JSON.MGET with non-existent key returns nil" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set one document
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":1}" },
    };
    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // MGET with one existing and one non-existent key
    const mget_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.MGET" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "nonexistent" },
        RespValue{ .bulk_string = "$.a" },
    };

    const result = try cmdJsonMget(&storage, &mget_args, allocator);
    defer {
        for (result.array) |item| {
            switch (item) {
                .bulk_string => |s| allocator.free(s),
                else => {},
            }
        }
        allocator.free(result.array);
    }

    try std.testing.expectEqual(@as(usize, 2), result.array.len);
    try std.testing.expectEqualStrings("1", result.array[0].bulk_string);
    try std.testing.expect(result.array[1] == .null_bulk_string);
}

test "JSON.NUMINCRBY increments numeric value" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set a JSON document with a number
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"count\":10}" },
    };
    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Increment the count by 5
    const incr_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.NUMINCRBY" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$.count" },
        RespValue{ .bulk_string = "5" },
    };

    const result = try cmdJsonNumincrby(&storage, &incr_args, allocator);
    defer allocator.free(result.bulk_string);

    // Result should be "15"
    const new_val = try std.fmt.parseFloat(f64, result.bulk_string);
    try std.testing.expectEqual(@as(f64, 15.0), new_val);

    // Verify the value was updated in storage
    const get_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.GET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$.count" },
    };

    const get_result = try cmdJsonGet(&storage, &get_args, allocator);
    defer allocator.free(get_result.bulk_string);

    const stored_val = try std.fmt.parseFloat(f64, get_result.bulk_string);
    try std.testing.expectEqual(@as(f64, 15.0), stored_val);
}

test "JSON.NUMINCRBY with negative increment" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set a JSON document
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"count\":10}" },
    };
    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Decrement the count by 3
    const incr_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.NUMINCRBY" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$.count" },
        RespValue{ .bulk_string = "-3" },
    };

    const result = try cmdJsonNumincrby(&storage, &incr_args, allocator);
    defer allocator.free(result.bulk_string);

    const new_val = try std.fmt.parseFloat(f64, result.bulk_string);
    try std.testing.expectEqual(@as(f64, 7.0), new_val);
}

test "JSON.NUMINCRBY on non-numeric value returns error" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set a JSON document with a string
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"name\":\"test\"}" },
    };
    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Try to increment a string
    const incr_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.NUMINCRBY" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$.name" },
        RespValue{ .bulk_string = "5" },
    };

    const result = try cmdJsonNumincrby(&storage, &incr_args, allocator);
    try std.testing.expect(result == .error_string);
    try std.testing.expect(std.mem.indexOf(u8, result.error_string, "not a number") != null);
}

test "JSON.NUMMULTBY multiplies numeric value" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set initial document
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"count\":10}" },
    };
    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Multiply by 3
    const mult_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.NUMMULTBY" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$.count" },
        RespValue{ .bulk_string = "3" },
    };

    const result = try cmdJsonNummultby(&storage, &mult_args, allocator);
    defer allocator.free(result.bulk_string);

    try std.testing.expectEqualStrings("30", result.bulk_string);
}

test "JSON.NUMMULTBY with negative multiplier" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set initial document
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"count\":10}" },
    };
    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Multiply by -2
    const mult_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.NUMMULTBY" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$.count" },
        RespValue{ .bulk_string = "-2" },
    };

    const result = try cmdJsonNummultby(&storage, &mult_args, allocator);
    defer allocator.free(result.bulk_string);

    try std.testing.expectEqualStrings("-20", result.bulk_string);
}

test "JSON.NUMMULTBY on non-numeric value returns error" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set document with string field
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"name\":\"test\"}" },
    };
    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Try to multiply string
    const mult_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.NUMMULTBY" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$.name" },
        RespValue{ .bulk_string = "3" },
    };

    const result = try cmdJsonNummultby(&storage, &mult_args, allocator);
    try std.testing.expect(result == .error_string);
}

test "JSON.MSET sets multiple keys atomically" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // MSET three documents
    const mset_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.MSET" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":1}" },
        RespValue{ .bulk_string = "doc2" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"b\":2}" },
        RespValue{ .bulk_string = "doc3" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"c\":3}" },
    };

    const result = try cmdJsonMset(&storage, &mset_args, allocator);
    try std.testing.expect(result == .simple_string);
    try std.testing.expectEqualStrings("OK", result.simple_string);

    // Verify all three documents exist
    try std.testing.expect(storage.data.get("doc1") != null);
    try std.testing.expect(storage.data.get("doc2") != null);
    try std.testing.expect(storage.data.get("doc3") != null);
}

test "JSON.MSET with wrong arity returns error" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Only 2 args after command (need triplets)
    const mset_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.MSET" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$" },
    };

    const result = try cmdJsonMset(&storage, &mset_args, allocator);
    try std.testing.expect(result == .error_string);
}

test "JSON.FORGET deletes key like JSON.DEL" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set document
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":1}" },
    };
    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Use FORGET to delete
    const forget_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.FORGET" },
        RespValue{ .bulk_string = "doc1" },
    };

    const result = try cmdJsonForget(&storage, &forget_args, allocator);
    try std.testing.expectEqual(@as(usize, 1), result.integer);
    try std.testing.expect(storage.data.get("doc1") == null);
}

/// JSON.STRAPPEND key [path] value
/// Appends a string to the JSON string at path
pub fn cmdJsonStrappend(
    storage: *Storage,
    args: []const RespValue,
    allocator: std.mem.Allocator,
) !RespValue {
    if (args.len < 3 or args.len > 4) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'json.strappend' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    // If 3 args: key value (implicit root path "$")
    // If 4 args: key path value
    const path_str = if (args.len == 3) "$" else switch (args[2]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid path" },
    };

    const append_str = switch (args[if (args.len == 3) @as(usize, 2) else @as(usize, 3)]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid string value" },
    };

    // Check if key exists
    const entry = storage.data.getPtr(key) orelse {
        return RespValue{ .error_string = "ERR key does not exist" };
    };

    // Check if value is JSON
    if (entry.* != .json) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    // Parse path
    var path = JsonPath.parse(allocator, path_str) catch {
        return RespValue{ .error_string = "ERR invalid path syntax" };
    };
    defer path.deinit();

    // Evaluate path to find the target node
    const json_val = &entry.json;
    var results = try path.evaluate(json_val.root, allocator);
    defer results.deinit(allocator);

    if (results.items.len == 0) {
        return RespValue{ .error_string = "ERR path does not exist" };
    }

    // For now, only support appending to a single value (first result)
    const target_node = results.items[0];
    if (target_node.* != .string) {
        return RespValue{ .error_string = "ERR path value is not a string" };
    }

    // Append the string
    const old_str = target_node.string;
    const new_str = try std.mem.concat(storage.allocator, u8, &[_][]const u8{ old_str, append_str });
    storage.allocator.free(old_str);
    target_node.string = new_str;

    // Return the new string length as an integer
    return RespValue{ .integer = @intCast(new_str.len) };
}

/// JSON.STRLEN key [path]
/// Returns the length of the JSON string at path
pub fn cmdJsonStrlen(
    storage: *Storage,
    args: []const RespValue,
    allocator: std.mem.Allocator,
) !RespValue {
    if (args.len < 2 or args.len > 3) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'json.strlen' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    // If 2 args: key (implicit root path "$")
    // If 3 args: key path
    const path_str = if (args.len == 2) "$" else switch (args[2]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid path" },
    };

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

    // Evaluate path to find the target node
    const json_val = &entry.json;
    var results = try path.evaluate(json_val.root, allocator);
    defer results.deinit(allocator);

    if (results.items.len == 0) {
        return RespValue{ .null_bulk_string = {} };
    }

    // Return array if multiple results, otherwise single integer
    if (results.items.len > 1) {
        var arr: std.ArrayList(RespValue) = .{};
        errdefer arr.deinit(allocator);

        for (results.items) |node| {
            if (node.* == .string) {
                try arr.append(allocator, RespValue{ .integer = @intCast(node.string.len) });
            } else {
                try arr.append(allocator, RespValue{ .null_bulk_string = {} });
            }
        }

        const result = try arr.toOwnedSlice(allocator);
        return RespValue{ .array = result };
    } else {
        // Single result
        const node = results.items[0];
        if (node.* == .string) {
            return RespValue{ .integer = @intCast(node.string.len) };
        } else {
            return RespValue{ .null_bulk_string = {} };
        }
    }
}

/// JSON.TOGGLE key [path]
/// Toggles a boolean value at the specified path
/// Returns array of integers (0/1) for JSONPath, single integer for legacy path
/// Returns null for non-boolean values
pub fn cmdJsonToggle(
    storage: *Storage,
    args: []const RespValue,
    allocator: std.mem.Allocator,
) !RespValue {
    if (args.len < 2 or args.len > 3) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'json.toggle' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    // If 2 args: key (implicit root path "$")
    // If 3 args: key path
    const path_str = if (args.len == 2) "$" else switch (args[2]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid path" },
    };

    // Check if key exists
    const entry = storage.data.getPtr(key) orelse {
        return RespValue{ .null_bulk_string = {} };
    };

    // Check if value is JSON
    if (entry.* != .json) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    // Parse path
    var path = JsonPath.parse(allocator, path_str) catch {
        return RespValue{ .error_string = "ERR invalid path syntax" };
    };
    defer path.deinit();

    // Evaluate path to find the target nodes
    const json_val = &entry.json;
    var results = try path.evaluate(json_val.root, allocator);
    defer results.deinit(allocator);

    if (results.items.len == 0) {
        // No matches - return empty array for JSONPath
        return RespValue{ .array = &[_]RespValue{} };
    }

    // Toggle all boolean values and collect results
    var arr: std.ArrayList(RespValue) = .{};
    errdefer arr.deinit(allocator);

    for (results.items) |node| {
        if (node.* == .bool) {
            // Toggle boolean value in-place
            node.bool = !node.bool;
            const new_val: i64 = if (node.bool) 1 else 0;
            try arr.append(allocator, RespValue{ .integer = new_val });
        } else {
            // Non-boolean value - return null
            try arr.append(allocator, RespValue{ .null_bulk_string = {} });
        }
    }

    // Return array if multiple results, otherwise single value for legacy path
    if (results.items.len == 1) {
        // Single result - return the first element directly
        const result_val = arr.items[0];
        arr.deinit(allocator);
        return result_val;
    } else {
        // Multiple results - return array
        const result = try arr.toOwnedSlice(allocator);
        return RespValue{ .array = result };
    }
}

/// JSON.CLEAR key [path]
/// Clears values at the specified path
/// Objects and arrays are emptied, numbers are set to 0
/// Strings, booleans, and null are left unchanged
pub fn cmdJsonClear(
    storage: *Storage,
    args: []const RespValue,
    allocator: std.mem.Allocator,
) !RespValue {
    if (args.len < 2 or args.len > 3) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'json.clear' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    // If 2 args: key (implicit root path "$")
    // If 3 args: key path
    const path_str = if (args.len == 2) "$" else switch (args[2]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid path" },
    };

    // Check if key exists
    const entry = storage.data.getPtr(key) orelse {
        return RespValue{ .null_bulk_string = {} };
    };

    // Check if value is JSON
    if (entry.* != .json) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    // Parse path
    var path = JsonPath.parse(allocator, path_str) catch {
        return RespValue{ .error_string = "ERR invalid path syntax" };
    };
    defer path.deinit();

    // Evaluate path to find the target nodes
    const json_val = &entry.json;
    var results = try path.evaluate(json_val.root, allocator);
    defer results.deinit(allocator);

    if (results.items.len == 0) {
        // No matches - return 0
        return RespValue{ .integer = 0 };
    }

    // Clear all matching values and count modifications
    var count: i64 = 0;
    for (results.items) |node| {
        const modified = clearNode(node, storage.allocator) catch |err| {
            std.log.err("Failed to clear JSON node: {}", .{err});
            return RespValue{ .error_string = "ERR failed to clear JSON value" };
        };
        if (modified) {
            count += 1;
        }
    }

    return RespValue{ .integer = count };
}

/// Helper function to recursively free RespValue memory.
///
/// Handles nested arrays and frees all owned strings.
/// Arguments:
///   - value: RespValue to deinitialize
///   - allocator: Allocator that was used to allocate the value
fn deinitRespValue(value: *const RespValue, allocator: std.mem.Allocator) void {
    switch (value.*) {
        .array => |arr| {
            for (arr) |*item| {
                deinitRespValue(item, allocator);
            }
            // Cast away const for free
            allocator.free(@constCast(arr));
        },
        .bulk_string => |s| allocator.free(@constCast(s)),
        .simple_string => |s| allocator.free(@constCast(s)),
        .error_string => |s| allocator.free(@constCast(s)),
        else => {},
    }
}

/// Helper function to clear a JSON node according to Redis semantics.
///
/// Clearing behavior by type:
///   - object: Removes all key-value pairs, resulting in empty object {}
///   - array: Removes all elements, resulting in empty array []
///   - number: Sets value to 0.0
///   - string/bool/null: No modification (returns false)
///
/// Arguments:
///   - node: Pointer to the JsonNode to clear
///   - allocator: Memory allocator for temporary operations
///
/// Returns true if the node was modified, false otherwise.
/// Returns error if memory allocation fails during clearing.
fn clearNode(node: *JsonNode, allocator: std.mem.Allocator) !bool {
    switch (node.*) {
        .object => |*obj| {
            if (obj.count() > 0) {
                // Collect keys to delete (we can't iterate and modify at the same time)
                var keys_to_delete = try std.ArrayList([]const u8).initCapacity(allocator, obj.count());
                defer keys_to_delete.deinit(allocator);

                var it = obj.iterator();
                while (it.next()) |entry| {
                    try keys_to_delete.append(allocator, entry.key_ptr.*);
                }

                // Now delete the collected keys
                for (keys_to_delete.items) |key| {
                    if (obj.fetchRemove(key)) |kv| {
                        allocator.free(kv.key);
                        kv.value.deinit(allocator);
                        allocator.destroy(kv.value);
                    }
                }

                return true;
            }
            return false;
        },
        .array => |*arr| {
            if (arr.items.len > 0) {
                // Clear all elements from the array
                for (arr.items) |item| {
                    item.deinit(allocator);
                    allocator.destroy(item);
                }
                arr.clearRetainingCapacity();
                return true;
            }
            return false;
        },
        .number => |*n| {
            if (n.* != 0.0) {
                // Set number to 0 if not already 0
                n.* = 0.0;
                return true;
            }
            return false;
        },
        // Strings, booleans, and null are not cleared
        .string, .bool, .null => return false,
    }
}

/// JSON.ARRINDEX key path value [start [stop]]
/// Finds the index of the first occurrence of value in an array
pub fn cmdJsonArrindex(
    storage: *Storage,
    args: []const RespValue,
    allocator: std.mem.Allocator,
) !RespValue {
    if (args.len < 4 or args.len > 6) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'json.arrindex' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    const path_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid path" },
    };

    const value_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid value" },
    };

    // Parse optional start and stop indices
    var start_idx: i64 = 0;
    var stop_idx: i64 = 0; // 0 means end
    if (args.len >= 5) {
        start_idx = std.fmt.parseInt(i64, switch (args[4]) {
            .bulk_string => |s| s,
            else => return RespValue{ .error_string = "ERR invalid start index" },
        }, 10) catch {
            return RespValue{ .error_string = "ERR invalid start index" };
        };
    }
    if (args.len >= 6) {
        stop_idx = std.fmt.parseInt(i64, switch (args[5]) {
            .bulk_string => |s| s,
            else => return RespValue{ .error_string = "ERR invalid stop index" },
        }, 10) catch {
            return RespValue{ .error_string = "ERR invalid stop index" };
        };
    }

    // Check if key exists
    const entry = storage.data.getPtr(key) orelse {
        return RespValue{ .null_bulk_string = {} };
    };

    // Check if value is JSON
    if (entry.* != .json) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    // Parse path
    var path = JsonPath.parse(allocator, path_str) catch {
        return RespValue{ .error_string = "ERR invalid path syntax" };
    };
    defer path.deinit();

    // Parse value to search for
    const search_node = JsonNode.parse(allocator, value_str) catch {
        return RespValue{ .error_string = "ERR invalid JSON value" };
    };
    defer {
        search_node.deinit(allocator);
        allocator.destroy(search_node);
    }

    // Evaluate path to find the target arrays
    const json_val = &entry.json;
    var results = try path.evaluate(json_val.root, allocator);
    defer results.deinit(allocator);

    if (results.items.len == 0) {
        return RespValue{ .null_bulk_string = {} };
    }

    // If single result, check if it's an array
    if (results.items.len == 1) {
        if (results.items[0].* != .array) {
            // Not an array - return null
            return RespValue{ .null_bulk_string = {} };
        }
        const idx = searchInArray(results.items[0], search_node, start_idx, stop_idx);
        return RespValue{ .integer = idx };
    }

    // Multiple results - return array of indices
    var indices = try allocator.alloc(RespValue, results.items.len);
    errdefer allocator.free(indices);

    for (results.items, 0..) |node, i| {
        if (node.* != .array) {
            // Not an array - return null for this result
            indices[i] = RespValue{ .null_bulk_string = {} };
        } else {
            const idx = searchInArray(node, search_node, start_idx, stop_idx);
            indices[i] = RespValue{ .integer = idx };
        }
    }

    return RespValue{ .array = indices };
}

/// Helper function to search for a value in an array
/// Returns the index of the first occurrence, or -1 if not found or not an array
/// Arguments:
///   - node: Pointer to the JsonNode to search in (must be an array)
///   - search_value: JsonNode value to search for
///   - start_idx: Starting index (default 0), supports negative indices
///   - stop_idx: Stopping index (default 0 = end), supports negative indices
///
/// Returns the index as i64 (-1 if not found or invalid array)
fn searchInArray(
    node: *JsonNode,
    search_value: *const JsonNode,
    start_idx: i64,
    stop_idx: i64,
) i64 {
    // Node must be an array
    if (node.* != .array) {
        return -1;
    }

    const array = &node.array;
    const len = array.items.len;

    if (len == 0) {
        return -1;
    }

    // Normalize start index
    var start: i64 = start_idx;
    if (start < 0) {
        start = @max(0, @as(i64, @intCast(len)) + start);
    } else {
        start = @min(start, @as(i64, @intCast(len)));
    }

    // Normalize stop index (0 means end)
    var stop: i64 = @as(i64, @intCast(len));
    if (stop_idx != 0) {
        stop = stop_idx;
        if (stop < 0) {
            stop = @max(0, @as(i64, @intCast(len)) + stop);
        } else {
            stop = @min(stop, @as(i64, @intCast(len)));
        }
    }

    // Check for invalid range
    if (start >= stop) {
        return -1;
    }

    // Search for the value
    var i: i64 = start;
    while (i < stop) : (i += 1) {
        const idx = @as(usize, @intCast(i));
        if (idx >= len) break;

        if (jsonNodeEquals(array.items[idx], search_value)) {
            return i;
        }
    }

    return -1;
}

/// Compares two JsonNode values for deep equality.
///
/// Performs type-sensitive comparison:
/// - Primitives: Direct value comparison
/// - Numbers: Approximate equality (handles 2 == 2.0)
/// - Strings: Byte-wise comparison
/// - Arrays: Element-wise recursive comparison
/// - Objects: Key-value recursive comparison
///
/// Returns true if nodes are structurally and semantically equal.
fn jsonNodeEquals(a: *const JsonNode, b: *const JsonNode) bool {
    switch (a.*) {
        .null => return b.* == .null,
        .bool => |a_bool| {
            if (b.* == .bool) {
                return a_bool == b.bool;
            }
            return false;
        },
        .number => |a_num| {
            if (b.* == .number) {
                // Numeric comparison (handle 2 == 2.0)
                return std.math.approxEqAbs(f64, a_num, b.number, 1e-10);
            }
            return false;
        },
        .string => |a_str| {
            if (b.* == .string) {
                return std.mem.eql(u8, a_str, b.string);
            }
            return false;
        },
        .array => |a_arr| {
            if (b.* != .array) return false;
            if (a_arr.items.len != b.array.items.len) return false;
            for (a_arr.items, b.array.items) |a_item, b_item| {
                if (!jsonNodeEquals(a_item, b_item)) return false;
            }
            return true;
        },
        .object => |a_obj| {
            if (b.* != .object) return false;
            if (a_obj.count() != b.object.count()) return false;
            var it = a_obj.iterator();
            while (it.next()) |entry| {
                const b_val = b.object.get(entry.key_ptr.*) orelse return false;
                if (!jsonNodeEquals(entry.value_ptr.*, b_val)) return false;
            }
            return true;
        },
    }
}

test "JSON.STRAPPEND appends to string" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set document with string field
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"name\":\"Hello\"}" },
    };
    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Append to string
    const append_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.STRAPPEND" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$.name" },
        RespValue{ .bulk_string = " World" },
    };

    const result = try cmdJsonStrappend(&storage, &append_args, allocator);
    try std.testing.expectEqual(@as(usize, 11), result.integer); // "Hello World".len

    // Verify the string was modified
    const get_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.GET" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$.name" },
    };
    const get_result = try cmdJsonGet(&storage, &get_args, allocator);
    defer allocator.free(get_result.bulk_string);
    try std.testing.expectEqualStrings("\"Hello World\"", get_result.bulk_string);
}

test "JSON.STRAPPEND on non-string returns error" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set document with numeric field
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"count\":10}" },
    };
    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Try to append to number
    const append_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.STRAPPEND" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$.count" },
        RespValue{ .bulk_string = "test" },
    };

    const result = try cmdJsonStrappend(&storage, &append_args, allocator);
    try std.testing.expect(result == .error_string);
}

test "JSON.STRLEN returns string length" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set document with string field
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"name\":\"Hello\"}" },
    };
    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Get string length
    const strlen_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.STRLEN" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$.name" },
    };

    const result = try cmdJsonStrlen(&storage, &strlen_args, allocator);
    try std.testing.expectEqual(@as(usize, 5), result.integer);
}

test "JSON.STRLEN on non-string returns null" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set document with numeric field
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"count\":10}" },
    };
    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Try to get length of number
    const strlen_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.STRLEN" },
        RespValue{ .bulk_string = "doc1" },
        RespValue{ .bulk_string = "$.count" },
    };

    const result = try cmdJsonStrlen(&storage, &strlen_args, allocator);
    try std.testing.expect(result == .null_bulk_string);
}

test "JSON.STRLEN on non-existent key returns null" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    const strlen_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.STRLEN" },
        RespValue{ .bulk_string = "nonexistent" },
    };

    const result = try cmdJsonStrlen(&storage, &strlen_args, allocator);
    try std.testing.expect(result == .null_bulk_string);
}

test "JSON.TOGGLE toggles single boolean" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set document with boolean field
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"active\":true}" },
    };
    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Toggle the boolean
    const toggle_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.TOGGLE" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$.active" },
    };

    const result = try cmdJsonToggle(&storage, &toggle_args, allocator);
    try std.testing.expect(result == .integer);
    try std.testing.expectEqual(@as(i64, 0), result.integer);

    // Verify document was modified
    const get_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.GET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$.active" },
    };

    const get_result = try cmdJsonGet(&storage, &get_args, allocator);
    defer protocol.deinitRespValue(&get_result, allocator);

    try std.testing.expect(get_result == .bulk_string);
    try std.testing.expectEqualStrings("[false]", get_result.bulk_string);
}

test "JSON.TOGGLE toggles multiple booleans" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set document with multiple boolean fields
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":true,\"b\":false,\"c\":true}" },
    };
    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Toggle all booleans with wildcard
    const toggle_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.TOGGLE" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$.*" },
    };

    const result = try cmdJsonToggle(&storage, &toggle_args, allocator);
    defer protocol.deinitRespValue(&result, allocator);

    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 3), result.array.len);

    // Check each toggled value
    try std.testing.expectEqual(@as(i64, 0), result.array[0].integer); // true -> false (0)
    try std.testing.expectEqual(@as(i64, 1), result.array[1].integer); // false -> true (1)
    try std.testing.expectEqual(@as(i64, 0), result.array[2].integer); // true -> false (0)
}

test "JSON.TOGGLE on non-boolean returns null" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set document with mixed types
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":true,\"b\":123,\"c\":\"text\"}" },
    };
    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Toggle all fields
    const toggle_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.TOGGLE" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$.*" },
    };

    const result = try cmdJsonToggle(&storage, &toggle_args, allocator);
    defer protocol.deinitRespValue(&result, allocator);

    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 3), result.array.len);

    // a is boolean - toggled to false (0)
    try std.testing.expectEqual(@as(i64, 0), result.array[0].integer);

    // b is number - returns null
    try std.testing.expect(result.array[1] == .null_bulk_string);

    // c is string - returns null
    try std.testing.expect(result.array[2] == .null_bulk_string);
}

test "JSON.TOGGLE on non-existent key returns null" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    const toggle_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.TOGGLE" },
        RespValue{ .bulk_string = "nonexistent" },
        RespValue{ .bulk_string = "$" },
    };

    const result = try cmdJsonToggle(&storage, &toggle_args, allocator);
    try std.testing.expect(result == .null_bulk_string);
}

test "JSON.TOGGLE with no matches returns empty array" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set document
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"x\":1}" },
    };
    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Toggle non-existent path
    const toggle_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.TOGGLE" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$.nonexistent" },
    };

    const result = try cmdJsonToggle(&storage, &toggle_args, allocator);
    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 0), result.array.len);
}

test "JSON.TOGGLE on root boolean" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Set root as boolean
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "doc" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "true" },
    };
    _ = try cmdJsonSet(&storage, &set_args, allocator);

    // Toggle root (implicit $)
    const toggle_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.TOGGLE" },
        RespValue{ .bulk_string = "doc" },
    };

    const result = try cmdJsonToggle(&storage, &toggle_args, allocator);
    try std.testing.expect(result == .integer);
    try std.testing.expectEqual(@as(i64, 0), result.integer);

    // Verify toggled to false
    const get_args = [_]RespValue{
        RespValue{ .bulk_string = "JSON.GET" },
        RespValue{ .bulk_string = "doc" },
    };

    const get_result = try cmdJsonGet(&storage, &get_args, allocator);
    defer protocol.deinitRespValue(&get_result, allocator);

    try std.testing.expect(get_result == .bulk_string);
    try std.testing.expectEqualStrings("false", get_result.bulk_string);
}

test "JSON.TOGGLE validates arity" {
    const allocator = std.testing.allocator;

    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Too few args
    const too_few = [_]RespValue{
        RespValue{ .bulk_string = "JSON.TOGGLE" },
    };

    const result1 = try cmdJsonToggle(&storage, &too_few, allocator);
    try std.testing.expect(result1 == .error_string);
    try std.testing.expect(std.mem.indexOf(u8, result1.error_string, "wrong number of arguments") != null);

    // Too many args
    const too_many = [_]RespValue{
        RespValue{ .bulk_string = "JSON.TOGGLE" },
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "extra" },
    };

    const result2 = try cmdJsonToggle(&storage, &too_many, allocator);
    try std.testing.expect(result2 == .error_string);
    try std.testing.expect(std.mem.indexOf(u8, result2.error_string, "wrong number of arguments") != null);
}

/// JSON.ARRAPPEND key [path] value [value ...]
/// Append JSON values to an array at path
/// Returns array of integers (new lengths) for each matching array, or null for non-arrays
pub fn cmdJsonArrappend(
    storage: *Storage,
    args: []const RespValue,
    allocator: std.mem.Allocator,
) !RespValue {
    if (args.len < 4) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'json.arrappend' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    const path_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid path" },
    };

    // All remaining args are values to append (at least one)
    const values_start = 3;
    const values_count = args.len - values_start;

    // Check if key exists
    const entry = storage.data.get(key);
    if (entry == null) {
        return RespValue{ .error_string = "ERR key does not exist" };
    }

    // Check type
    if (entry.? != .json) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    // Parse path
    var path = JsonPath.parse(allocator, path_str) catch {
        return RespValue{ .error_string = "ERR invalid path syntax" };
    };
    defer path.deinit();

    // Parse all values to append
    var values_to_append = try std.ArrayList(*JsonNode).initCapacity(allocator, values_count);
    defer {
        for (values_to_append.items) |v| {
            v.deinit(allocator);
            allocator.destroy(v);
        }
        values_to_append.deinit(allocator);
    }

    for (values_start..args.len) |i| {
        const value_str = switch (args[i]) {
            .bulk_string => |s| s,
            else => return RespValue{ .error_string = "ERR invalid JSON value" },
        };

        const node = JsonNode.parse(allocator, value_str) catch {
            return RespValue{ .error_string = "ERR invalid JSON string" };
        };
        try values_to_append.append(allocator, node);
    }

    // Evaluate path to find the target arrays
    const json_val = &entry.?.json;
    var results = try path.evaluate(json_val.root, allocator);
    defer results.deinit(allocator);

    // If no matches, return empty array
    if (results.items.len == 0) {
        return RespValue{ .array = &.{} };
    }

    // Build result array - one integer per matched node
    var result_array = try std.ArrayList(RespValue).initCapacity(allocator, results.items.len);
    errdefer {
        for (result_array.items) |*item| {
            switch (item.*) {
                .bulk_string => |s| allocator.free(s),
                else => {},
            }
        }
        result_array.deinit(allocator);
    }

    for (results.items) |node| {
        switch (node.*) {
            .array => |*arr| {
                // Append all values to this array
                for (values_to_append.items) |value| {
                    // Clone the value for this array
                    const cloned = try value.clone(allocator);
                    errdefer {
                        cloned.deinit(allocator);
                        allocator.destroy(cloned);
                    }
                    try arr.append(allocator, cloned);
                }

                // Return the new length
                const new_len = arr.items.len;
                try result_array.append(allocator, RespValue{ .integer = @intCast(new_len) });
            },
            else => {
                // Non-array: return null
                try result_array.append(allocator, RespValue{ .null_bulk_string = {} });
            },
        }
    }

    const owned_result = try result_array.toOwnedSlice(allocator);
    return RespValue{ .array = owned_result };
}

/// JSON.ARRINSERT key path index value [value ...]
/// Inserts one or more JSON values into an array at a specified index
/// Index can be negative (count from end): -1 = before last element
/// Returns: array of integers (new lengths) or nulls for each matched path
/// Handles the JSON.ARRINSERT command to insert values into a JSON array at a specified index.
///
/// Syntax: JSON.ARRINSERT key path index value [value ...]
///
/// Inserts one or more JSON values into the array at the specified index.
/// Negative indices are supported (count from the end).
///
/// Arguments:
///   - storage: Storage instance
///   - args: Command arguments [command, key, path, index, value1, value2, ...]
///   - allocator: Memory allocator
///
/// Returns:
///   - Array of integers (new lengths for each matched array)
///   - Array containing null for non-array targets
///   - Error for out-of-bounds indices
///
/// Errors:
///   - ERR wrong number of arguments: fewer than 5 args
///   - ERR key does not exist: key not found
///   - WRONGTYPE: key exists but is not JSON type
///   - ERR invalid path syntax: malformed JSONPath
///   - ERR index must be an integer: non-numeric index
///   - ERR index out of range: index < -(len+1) or index > len
pub fn cmdJsonArrinsert(
    storage: *Storage,
    args: []const RespValue,
    allocator: std.mem.Allocator,
) !RespValue {
    // Minimum: JSON.ARRINSERT key path index value
    if (args.len < 5) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'json.arrinsert' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid key" },
    };

    const path_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid path" },
    };

    const index_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return RespValue{ .error_string = "ERR invalid index" },
    };

    // Parse index
    const index = std.fmt.parseInt(i64, index_str, 10) catch {
        return RespValue{ .error_string = "ERR index must be an integer" };
    };

    // All remaining args are values to insert (at least one)
    // Arguments: [0]=command, [1]=key, [2]=path, [3]=index, [4..]=values
    const values_start = 4;
    const values_count = args.len - values_start;

    // Check if key exists
    const entry = storage.data.get(key);
    if (entry == null) {
        return RespValue{ .error_string = "ERR key does not exist" };
    }

    // Check type
    if (entry.? != .json) {
        return RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    // Parse path
    var path = JsonPath.parse(allocator, path_str) catch {
        return RespValue{ .error_string = "ERR invalid path syntax" };
    };
    defer path.deinit();

    // Parse all values to insert
    var values_to_insert = try std.ArrayList(*JsonNode).initCapacity(allocator, values_count);
    defer {
        for (values_to_insert.items) |v| {
            v.deinit(allocator);
            allocator.destroy(v);
        }
        values_to_insert.deinit(allocator);
    }

    for (values_start..args.len) |i| {
        const value_str = switch (args[i]) {
            .bulk_string => |s| s,
            else => return RespValue{ .error_string = "ERR invalid JSON value" },
        };

        const node = JsonNode.parse(allocator, value_str) catch {
            return RespValue{ .error_string = "ERR invalid JSON string" };
        };
        errdefer {
            node.deinit(allocator);
            allocator.destroy(node);
        }
        try values_to_insert.append(allocator, node);
    }

    // Evaluate path to find the target arrays
    const json_val = &entry.?.json;
    var results = try path.evaluate(json_val.root, allocator);
    defer results.deinit(allocator);

    // If no matches, return empty array
    if (results.items.len == 0) {
        return RespValue{ .array = &.{} };
    }

    // Build result array - one integer per matched node
    var result_array = try std.ArrayList(RespValue).initCapacity(allocator, results.items.len);
    errdefer {
        for (result_array.items) |*item| {
            switch (item.*) {
                .bulk_string => |s| allocator.free(s),
                else => {},
            }
        }
        result_array.deinit(allocator);
    }

    for (results.items) |node| {
        switch (node.*) {
            .array => |*arr| {
                // Normalize index
                const array_len = @as(i64, @intCast(arr.items.len));
                var normalized_idx: i64 = undefined;

                if (index < 0) {
                    // Negative index: count from end
                    normalized_idx = array_len + index + 1;
                } else {
                    normalized_idx = index;
                }

                // Check bounds: -(len+1) <= index <= len
                if (normalized_idx < 0 or normalized_idx > array_len) {
                    try result_array.append(allocator, RespValue{ .error_string = "ERR index out of range" });
                    continue;
                }

                const uindex = @as(usize, @intCast(normalized_idx));

                // Clone all values for this array
                var cloned_values = try std.ArrayList(*JsonNode).initCapacity(allocator, values_count);
                defer cloned_values.deinit(allocator);

                for (values_to_insert.items) |value| {
                    const cloned = try value.clone(allocator);
                    errdefer {
                        cloned.deinit(allocator);
                        allocator.destroy(cloned);
                    }
                    try cloned_values.append(allocator, cloned);
                }

                // Insert all cloned values at once
                try arr.insertSlice(allocator, uindex, cloned_values.items);

                // Return the new length
                const new_len = arr.items.len;
                try result_array.append(allocator, RespValue{ .integer = @intCast(new_len) });
            },
            else => {
                // Non-array: return null
                try result_array.append(allocator, RespValue{ .null_bulk_string = {} });
            },
        }
    }

    const owned_result = try result_array.toOwnedSlice(allocator);
    return RespValue{ .array = owned_result };
}

test "JSON.ARRAPPEND - append single value to array" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set up initial JSON with array
    const args_set = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"arr\":[1,2,3]}" },
    };
    const set_result = try cmdJsonSet(&storage, &args_set, allocator);
    try std.testing.expect(set_result == .simple_string);
    try std.testing.expectEqualStrings("OK", set_result.simple_string);

    // Append value to array
    const args_append = [_]RespValue{
        RespValue{ .bulk_string = "JSON.ARRAPPEND" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "$.arr" },
        RespValue{ .bulk_string = "4" },
    };
    const result = try cmdJsonArrappend(&storage, &args_append, allocator);
    defer deinitRespValue(result, allocator);

    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 1), result.array.len);
    try std.testing.expect(result.array[0] == .integer);
    try std.testing.expectEqual(@as(i64, 4), result.array[0].integer);

    // Verify the array was modified
    const args_get = [_]RespValue{
        RespValue{ .bulk_string = "JSON.GET" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "$.arr" },
    };
    const get_result = try cmdJsonGet(&storage, &args_get, allocator);
    defer deinitRespValue(get_result, allocator);

    try std.testing.expect(get_result == .bulk_string);
    try std.testing.expectEqualStrings("[1,2,3,4]", get_result.bulk_string);
}

test "JSON.ARRAPPEND - append multiple values" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args_set = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"arr\":[1]}" },
    };
    const set_result = try cmdJsonSet(&storage, &args_set, allocator);
    try std.testing.expect(set_result == .simple_string);

    // Append multiple values
    const args_append = [_]RespValue{
        RespValue{ .bulk_string = "JSON.ARRAPPEND" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "$.arr" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "3" },
        RespValue{ .bulk_string = "4" },
    };
    const result = try cmdJsonArrappend(&storage, &args_append, allocator);
    defer deinitRespValue(result, allocator);

    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 1), result.array.len);
    try std.testing.expectEqual(@as(i64, 4), result.array[0].integer);
}

test "JSON.ARRAPPEND - non-array returns null" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args_set = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"num\":42}" },
    };
    const set_result = try cmdJsonSet(&storage, &args_set, allocator);
    try std.testing.expect(set_result == .simple_string);

    // Try to append to non-array
    const args_append = [_]RespValue{
        RespValue{ .bulk_string = "JSON.ARRAPPEND" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "$.num" },
        RespValue{ .bulk_string = "1" },
    };
    const result = try cmdJsonArrappend(&storage, &args_append, allocator);
    defer deinitRespValue(result, allocator);

    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 1), result.array.len);
    try std.testing.expect(result.array[0] == .null_bulk_string);
}

test "JSON.ARRAPPEND - multiple arrays" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args_set = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"a\":[1],\"b\":[2]}" },
    };
    const set_result = try cmdJsonSet(&storage, &args_set, allocator);
    try std.testing.expect(set_result == .simple_string);

    // Append to all arrays with wildcard
    const args_append = [_]RespValue{
        RespValue{ .bulk_string = "JSON.ARRAPPEND" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "$.*" },
        RespValue{ .bulk_string = "99" },
    };
    const result = try cmdJsonArrappend(&storage, &args_append, allocator);
    defer deinitRespValue(result, allocator);

    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 2), result.array.len);
    try std.testing.expectEqual(@as(i64, 2), result.array[0].integer);
    try std.testing.expectEqual(@as(i64, 2), result.array[1].integer);
}

test "JSON.ARRAPPEND - key does not exist" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args_append = [_]RespValue{
        RespValue{ .bulk_string = "JSON.ARRAPPEND" },
        RespValue{ .bulk_string = "nonexistent" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "1" },
    };
    const result = try cmdJsonArrappend(&storage, &args_append, allocator);

    try std.testing.expect(result == .error_string);
    try std.testing.expect(std.mem.indexOf(u8, result.error_string, "key does not exist") != null);
}

test "JSON.ARRAPPEND - wrong type" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create a non-JSON key
    const string_val = try storage.allocator.dupe(u8, "not json");
    try storage.data.put("mykey", Value{ .string = string_val });

    const args_append = [_]RespValue{
        RespValue{ .bulk_string = "JSON.ARRAPPEND" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "1" },
    };
    const result = try cmdJsonArrappend(&storage, &args_append, allocator);

    try std.testing.expect(result == .error_string);
    try std.testing.expect(std.mem.indexOf(u8, result.error_string, "WRONGTYPE") != null);
}

test "JSON.ARRAPPEND - arity error" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const too_few = [_]RespValue{
        RespValue{ .bulk_string = "JSON.ARRAPPEND" },
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "$" },
    };

    const result = try cmdJsonArrappend(&storage, &too_few, allocator);
    try std.testing.expect(result == .error_string);
    try std.testing.expect(std.mem.indexOf(u8, result.error_string, "wrong number of arguments") != null);
}

test "JSON.ARRAPPEND - append complex objects" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args_set = [_]RespValue{
        RespValue{ .bulk_string = "JSON.SET" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "$" },
        RespValue{ .bulk_string = "{\"arr\":[]}" },
    };
    const set_result = try cmdJsonSet(&storage, &args_set, allocator);
    try std.testing.expect(set_result == .simple_string);

    // Append objects and arrays
    const args_append = [_]RespValue{
        RespValue{ .bulk_string = "JSON.ARRAPPEND" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "$.arr" },
        RespValue{ .bulk_string = "{\"name\":\"Alice\"}" },
        RespValue{ .bulk_string = "[1,2,3]" },
        RespValue{ .bulk_string = "\"string\"" },
    };
    const result = try cmdJsonArrappend(&storage, &args_append, allocator);
    defer deinitRespValue(result, allocator);

    try std.testing.expect(result == .array);
    try std.testing.expectEqual(@as(usize, 1), result.array.len);
    try std.testing.expectEqual(@as(i64, 3), result.array[0].integer);
}
