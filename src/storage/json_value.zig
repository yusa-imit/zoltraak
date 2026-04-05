const std = @import("std");

/// Internal JSON document representation
/// Optimized for sub-document queries and atomic modifications
pub const JsonNode = union(enum) {
    null: void,
    bool: bool,
    number: f64,
    string: []const u8,
    array: std.ArrayList(*JsonNode),
    object: std.StringHashMap(*JsonNode),

    /// Parse JSON string into JsonNode tree
    /// Caller owns the returned node and must call deinit()
    pub fn parse(allocator: std.mem.Allocator, json_str: []const u8) !*JsonNode {
        // Use std.json.parseFromSlice for initial parse
        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_str, .{});
        defer parsed.deinit();

        // Convert std.json.Value to JsonNode
        return try convertFromStdJson(allocator, parsed.value);
    }

    /// Convert std.json.Value to JsonNode
    fn convertFromStdJson(allocator: std.mem.Allocator, value: std.json.Value) !*JsonNode {
        const node = try allocator.create(JsonNode);
        errdefer allocator.destroy(node);

        node.* = switch (value) {
            .null => JsonNode{ .null = {} },
            .bool => |b| JsonNode{ .bool = b },
            .integer => |i| JsonNode{ .number = @floatFromInt(i) },
            .float => |f| JsonNode{ .number = f },
            .number_string => |ns| blk: {
                const f = std.fmt.parseFloat(f64, ns) catch 0.0;
                break :blk JsonNode{ .number = f };
            },
            .string => |s| blk: {
                const owned_str = try allocator.dupe(u8, s);
                break :blk JsonNode{ .string = owned_str };
            },
            .array => |arr| blk: {
                var list: std.ArrayList(*JsonNode) = .{};
                errdefer {
                    for (list.items) |item| {
                        item.deinit(allocator);
                        allocator.destroy(item);
                    }
                    list.deinit(allocator);
                }

                for (arr.items) |item| {
                    const child = try convertFromStdJson(allocator, item);
                    try list.append(allocator, child);
                }
                break :blk JsonNode{ .array = list };
            },
            .object => |obj| blk: {
                var map = std.StringHashMap(*JsonNode).init(allocator);
                errdefer {
                    var it = map.iterator();
                    while (it.next()) |entry| {
                        allocator.free(entry.key_ptr.*);
                        entry.value_ptr.*.deinit(allocator);
                        allocator.destroy(entry.value_ptr.*);
                    }
                    map.deinit();
                }

                var it = obj.iterator();
                while (it.next()) |entry| {
                    const owned_key = try allocator.dupe(u8, entry.key_ptr.*);
                    errdefer allocator.free(owned_key);
                    const child = try convertFromStdJson(allocator, entry.value_ptr.*);
                    errdefer {
                        child.deinit(allocator);
                        allocator.destroy(child);
                    }
                    try map.put(owned_key, child);
                }
                break :blk JsonNode{ .object = map };
            },
        };

        return node;
    }

    /// Serialize JsonNode tree to JSON string
    /// Caller owns the returned string
    pub fn stringify(self: *const JsonNode, allocator: std.mem.Allocator) ![]const u8 {
        var buf: std.ArrayList(u8) = .{};
        errdefer buf.deinit(allocator);

        try stringifyInternal(self, &buf, allocator);
        return try buf.toOwnedSlice(allocator);
    }

    fn stringifyInternal(self: *const JsonNode, buf: *std.ArrayList(u8), allocator: std.mem.Allocator) !void {
        switch (self.*) {
            .null => try buf.appendSlice(allocator, "null"),
            .bool => |b| {
                if (b) {
                    try buf.appendSlice(allocator, "true");
                } else {
                    try buf.appendSlice(allocator, "false");
                }
            },
            .number => |n| {
                const str = try std.fmt.allocPrint(allocator, "{d}", .{n});
                defer allocator.free(str);
                try buf.appendSlice(allocator, str);
            },
            .string => |s| {
                try buf.append(allocator, '"');
                // TODO: escape special characters
                for (s) |c| {
                    switch (c) {
                        '"' => try buf.appendSlice(allocator, "\\\""),
                        '\\' => try buf.appendSlice(allocator, "\\\\"),
                        '\n' => try buf.appendSlice(allocator, "\\n"),
                        '\r' => try buf.appendSlice(allocator, "\\r"),
                        '\t' => try buf.appendSlice(allocator, "\\t"),
                        else => try buf.append(allocator, c),
                    }
                }
                try buf.append(allocator, '"');
            },
            .array => |arr| {
                try buf.append(allocator, '[');
                for (arr.items, 0..) |item, i| {
                    if (i > 0) try buf.append(allocator, ',');
                    try stringifyInternal(item, buf, allocator);
                }
                try buf.append(allocator, ']');
            },
            .object => |obj| {
                try buf.append(allocator, '{');
                var it = obj.iterator();
                var first = true;
                while (it.next()) |entry| {
                    if (!first) try buf.append(allocator, ',');
                    first = false;

                    try buf.append(allocator, '"');
                    try buf.appendSlice(allocator, entry.key_ptr.*);
                    try buf.append(allocator, '"');
                    try buf.append(allocator, ':');
                    try stringifyInternal(entry.value_ptr.*, buf, allocator);
                }
                try buf.append(allocator, '}');
            },
        }
    }

    /// Deep clone a node
    /// Caller owns the returned node
    pub fn clone(self: *const JsonNode, allocator: std.mem.Allocator) !*JsonNode {
        const node = try allocator.create(JsonNode);
        errdefer allocator.destroy(node);

        node.* = switch (self.*) {
            .null => JsonNode{ .null = {} },
            .bool => |b| JsonNode{ .bool = b },
            .number => |n| JsonNode{ .number = n },
            .string => |s| blk: {
                const owned_str = try allocator.dupe(u8, s);
                break :blk JsonNode{ .string = owned_str };
            },
            .array => |arr| blk: {
                var list: std.ArrayList(*JsonNode) = .{};
                errdefer {
                    for (list.items) |item| {
                        item.deinit(allocator);
                        allocator.destroy(item);
                    }
                    list.deinit(allocator);
                }

                for (arr.items) |item| {
                    const child = try item.clone(allocator);
                    try list.append(allocator, child);
                }
                break :blk JsonNode{ .array = list };
            },
            .object => |obj| blk: {
                var map = std.StringHashMap(*JsonNode).init(allocator);
                errdefer {
                    var it = map.iterator();
                    while (it.next()) |entry| {
                        allocator.free(entry.key_ptr.*);
                        entry.value_ptr.*.deinit(allocator);
                        allocator.destroy(entry.value_ptr.*);
                    }
                    map.deinit();
                }

                var it = obj.iterator();
                while (it.next()) |entry| {
                    const owned_key = try allocator.dupe(u8, entry.key_ptr.*);
                    errdefer allocator.free(owned_key);
                    const child = try entry.value_ptr.*.clone(allocator);
                    errdefer {
                        child.deinit(allocator);
                        allocator.destroy(child);
                    }
                    try map.put(owned_key, child);
                }
                break :blk JsonNode{ .object = map };
            },
        };

        return node;
    }

    /// Recursively free all child nodes
    pub fn deinit(self: *JsonNode, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .null, .bool, .number => {},
            .string => |s| allocator.free(s),
            .array => |*arr| {
                for (arr.items) |item| {
                    item.deinit(allocator);
                    allocator.destroy(item);
                }
                arr.deinit(allocator);
            },
            .object => |*obj| {
                var it = obj.iterator();
                while (it.next()) |entry| {
                    allocator.free(entry.key_ptr.*);
                    entry.value_ptr.*.deinit(allocator);
                    allocator.destroy(entry.value_ptr.*);
                }
                obj.deinit();
            },
        }
    }

    /// Get type name as string
    pub fn typeName(self: *const JsonNode) []const u8 {
        return switch (self.*) {
            .null => "null",
            .bool => "boolean",
            .number => "number",
            .string => "string",
            .array => "array",
            .object => "object",
        };
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "parse null" {
    const allocator = std.testing.allocator;
    const node = try JsonNode.parse(allocator, "null");
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    try std.testing.expectEqual(JsonNode.null, node.*);
}

test "parse boolean" {
    const allocator = std.testing.allocator;

    const node_true = try JsonNode.parse(allocator, "true");
    defer {
        node_true.deinit(allocator);
        allocator.destroy(node_true);
    }
    try std.testing.expect(node_true.bool);

    const node_false = try JsonNode.parse(allocator, "false");
    defer {
        node_false.deinit(allocator);
        allocator.destroy(node_false);
    }
    try std.testing.expect(!node_false.bool);
}

test "parse number" {
    const allocator = std.testing.allocator;

    const node = try JsonNode.parse(allocator, "42.5");
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    try std.testing.expectEqual(@as(f64, 42.5), node.number);
}

test "parse string" {
    const allocator = std.testing.allocator;

    const node = try JsonNode.parse(allocator, "\"hello world\"");
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    try std.testing.expectEqualStrings("hello world", node.string);
}

test "parse array" {
    const allocator = std.testing.allocator;

    const node = try JsonNode.parse(allocator, "[1, 2, 3]");
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    try std.testing.expectEqual(@as(usize, 3), node.array.items.len);
    try std.testing.expectEqual(@as(f64, 1.0), node.array.items[0].number);
    try std.testing.expectEqual(@as(f64, 2.0), node.array.items[1].number);
    try std.testing.expectEqual(@as(f64, 3.0), node.array.items[2].number);
}

test "parse object" {
    const allocator = std.testing.allocator;

    const node = try JsonNode.parse(allocator, "{\"a\":1,\"b\":2}");
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    try std.testing.expectEqual(@as(usize, 2), node.object.count());
    const a = node.object.get("a").?;
    try std.testing.expectEqual(@as(f64, 1.0), a.number);
    const b = node.object.get("b").?;
    try std.testing.expectEqual(@as(f64, 2.0), b.number);
}

test "stringify null" {
    const allocator = std.testing.allocator;

    const node = try JsonNode.parse(allocator, "null");
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    const str = try node.stringify(allocator);
    defer allocator.free(str);

    try std.testing.expectEqualStrings("null", str);
}

test "stringify nested object" {
    const allocator = std.testing.allocator;

    const node = try JsonNode.parse(allocator, "{\"a\":{\"b\":1}}");
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    const str = try node.stringify(allocator);
    defer allocator.free(str);

    // Verify it's valid JSON by parsing it again
    const reparsed = try JsonNode.parse(allocator, str);
    defer {
        reparsed.deinit(allocator);
        allocator.destroy(reparsed);
    }

    try std.testing.expectEqual(@as(usize, 1), reparsed.object.count());
}

test "clone node" {
    const allocator = std.testing.allocator;

    const node = try JsonNode.parse(allocator, "{\"a\":[1,2,3]}");
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    const cloned = try node.clone(allocator);
    defer {
        cloned.deinit(allocator);
        allocator.destroy(cloned);
    }

    // Verify cloned data matches original
    try std.testing.expectEqual(@as(usize, 1), cloned.object.count());
    const arr = cloned.object.get("a").?;
    try std.testing.expectEqual(@as(usize, 3), arr.array.items.len);
}

test "typeName" {
    const allocator = std.testing.allocator;

    const null_node = try JsonNode.parse(allocator, "null");
    defer {
        null_node.deinit(allocator);
        allocator.destroy(null_node);
    }
    try std.testing.expectEqualStrings("null", null_node.typeName());

    const bool_node = try JsonNode.parse(allocator, "true");
    defer {
        bool_node.deinit(allocator);
        allocator.destroy(bool_node);
    }
    try std.testing.expectEqualStrings("boolean", bool_node.typeName());

    const number_node = try JsonNode.parse(allocator, "42");
    defer {
        number_node.deinit(allocator);
        allocator.destroy(number_node);
    }
    try std.testing.expectEqualStrings("number", number_node.typeName());

    const string_node = try JsonNode.parse(allocator, "\"test\"");
    defer {
        string_node.deinit(allocator);
        allocator.destroy(string_node);
    }
    try std.testing.expectEqualStrings("string", string_node.typeName());

    const array_node = try JsonNode.parse(allocator, "[]");
    defer {
        array_node.deinit(allocator);
        allocator.destroy(array_node);
    }
    try std.testing.expectEqualStrings("array", array_node.typeName());

    const object_node = try JsonNode.parse(allocator, "{}");
    defer {
        object_node.deinit(allocator);
        allocator.destroy(object_node);
    }
    try std.testing.expectEqualStrings("object", object_node.typeName());
}
