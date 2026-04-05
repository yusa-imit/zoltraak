const std = @import("std");
const JsonNode = @import("json_value.zig").JsonNode;

/// JSONPath query parser supporting both legacy and RFC 9535 syntax
pub const JsonPath = struct {
    segments: std.ArrayList(PathSegment),
    is_legacy: bool, // false if starts with $, true if starts with .
    allocator: std.mem.Allocator,

    pub const PathSegment = union(enum) {
        root, // $ or .
        child: []const u8, // .key or $.key
        index: i64, // [0], [1], [-1] (negative = from end)
        wildcard, // .* or $.*
        recursive: []const u8, // ..key or $..key

        pub fn deinit(self: *PathSegment, allocator: std.mem.Allocator) void {
            switch (self.*) {
                .child => |s| allocator.free(s),
                .recursive => |s| allocator.free(s),
                else => {},
            }
        }
    };

    /// Parse JSONPath query string into segments
    pub fn parse(allocator: std.mem.Allocator, query: []const u8) !JsonPath {
        var segments : std.ArrayList(PathSegment) = .{};
        errdefer {
            for (segments.items) |*seg| {
                seg.deinit(allocator);
            }
            segments.deinit(allocator);
        }

        if (query.len == 0) {
            return error.EmptyPath;
        }

        const is_legacy = query[0] == '.';
        const is_jsonpath = query[0] == '$';

        if (!is_legacy and !is_jsonpath) {
            return error.InvalidPathStart;
        }

        // Add root segment
        try segments.append(allocator, .root);

        var i: usize = 1;
        while (i < query.len) {
            if (query[i] == '.') {
                // Check for recursive descent (..)
                if (i + 1 < query.len and query[i + 1] == '.') {
                    i += 2; // Skip ..

                    // Recursive wildcard or recursive descent with key
                    if (i < query.len and query[i] == '*') {
                        // ..* is not standard, treat as error for now
                        return error.InvalidRecursiveWildcard;
                    } else {
                        // Get the key name
                        const start = i;
                        while (i < query.len and query[i] != '.' and query[i] != '[') {
                            i += 1;
                        }
                        const key = try allocator.dupe(u8, query[start..i]);
                        try segments.append(allocator, .{ .recursive = key });
                    }
                } else {
                    // Single dot - child access
                    i += 1; // Skip .
                    if (i >= query.len) break;

                    if (query[i] == '*') {
                        try segments.append(allocator, .wildcard);
                        i += 1;
                    } else {
                        const start = i;
                        while (i < query.len and query[i] != '.' and query[i] != '[') {
                            i += 1;
                        }
                        const key = try allocator.dupe(u8, query[start..i]);
                        try segments.append(allocator, .{ .child = key });
                    }
                }
            } else if (query[i] == '[') {
                // Array access
                i += 1; // Skip [
                const start = i;
                while (i < query.len and query[i] != ']') {
                    i += 1;
                }
                if (i >= query.len) {
                    return error.UnmatchedBracket;
                }

                const index_str = query[start..i];
                if (std.mem.eql(u8, index_str, "*")) {
                    try segments.append(allocator, .wildcard);
                } else {
                    const index = try std.fmt.parseInt(i64, index_str, 10);
                    try segments.append(allocator, .{ .index = index });
                }

                i += 1; // Skip ]
            } else {
                return error.InvalidPathSyntax;
            }
        }

        return JsonPath{
            .segments = segments,
            .is_legacy = is_legacy,
            .allocator = allocator,
        };
    }

    /// Evaluate path against a JSON document, returns all matching nodes
    pub fn evaluate(self: *const JsonPath, root: *const JsonNode, allocator: std.mem.Allocator) !std.ArrayList(*JsonNode) {
        var results : std.ArrayList(*JsonNode) = .{};
        errdefer results.deinit(allocator);

        // Start with root node
        try results.append(allocator, @constCast(root));

        // Apply each segment
        for (self.segments.items[1..]) |segment| {
            var new_results : std.ArrayList(*JsonNode) = .{};
            errdefer new_results.deinit(allocator);

            for (results.items) |node| {
                try applySegment(node, segment, &new_results, allocator);
            }

            results.deinit(allocator);
            results = new_results;
        }

        return results;
    }

    fn applySegment(
        node: *JsonNode,
        segment: PathSegment,
        results: *std.ArrayList(*JsonNode),
        allocator: std.mem.Allocator,
    ) !void {
        switch (segment) {
            .root => {
                try results.append(allocator, node);
            },
            .child => |key| {
                if (node.* == .object) {
                    if (node.object.get(key)) |child| {
                        try results.append(allocator, child);
                    }
                }
            },
            .index => |idx| {
                if (node.* == .array) {
                    const len: i64 = @intCast(node.array.items.len);
                    var actual_idx = idx;
                    if (actual_idx < 0) {
                        actual_idx = len + actual_idx;
                    }
                    if (actual_idx >= 0 and actual_idx < len) {
                        const uidx: usize = @intCast(actual_idx);
                        try results.append(allocator, node.array.items[uidx]);
                    }
                }
            },
            .wildcard => {
                switch (node.*) {
                    .array => |arr| {
                        for (arr.items) |item| {
                            try results.append(allocator, item);
                        }
                    },
                    .object => |obj| {
                        var it = obj.valueIterator();
                        while (it.next()) |value| {
                            try results.append(allocator, value.*);
                        }
                    },
                    else => {},
                }
            },
            .recursive => |key| {
                try recursiveDescent(node, key, results, allocator);
            },
        }
    }

    fn recursiveDescent(
        node: *JsonNode,
        key: []const u8,
        results: *std.ArrayList(*JsonNode),
        allocator: std.mem.Allocator,
    ) !void {
        // Check current node
        if (node.* == .object) {
            if (node.object.get(key)) |child| {
                try results.append(allocator, child);
            }
        }

        // Recurse into children
        switch (node.*) {
            .array => |arr| {
                for (arr.items) |item| {
                    try recursiveDescent(item, key, results, allocator);
                }
            },
            .object => |obj| {
                var it = obj.valueIterator();
                while (it.next()) |value| {
                    try recursiveDescent(value.*, key, results, allocator);
                }
            },
            else => {},
        }
    }

    /// Check if path can create new keys (only root paths $ or . can create)
    pub fn canCreate(self: *const JsonPath) bool {
        return self.segments.items.len == 1;
    }

    pub fn deinit(self: *JsonPath) void {
        for (self.segments.items) |*seg| {
            seg.deinit(self.allocator);
        }
        self.segments.deinit(self.allocator);
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "parse root path $" {
    const allocator = std.testing.allocator;

    var path = try JsonPath.parse(allocator, "$");
    defer path.deinit();

    try std.testing.expectEqual(@as(usize, 1), path.segments.items.len);
    try std.testing.expect(!path.is_legacy);
    try std.testing.expect(path.canCreate());
}

test "parse root path ." {
    const allocator = std.testing.allocator;

    var path = try JsonPath.parse(allocator, ".");
    defer path.deinit();

    try std.testing.expectEqual(@as(usize, 1), path.segments.items.len);
    try std.testing.expect(path.is_legacy);
    try std.testing.expect(path.canCreate());
}

test "parse child path $.a" {
    const allocator = std.testing.allocator;

    var path = try JsonPath.parse(allocator, "$.a");
    defer path.deinit();

    try std.testing.expectEqual(@as(usize, 2), path.segments.items.len);
    try std.testing.expectEqualStrings("a", path.segments.items[1].child);
    try std.testing.expect(!path.canCreate());
}

test "parse child path .a.b" {
    const allocator = std.testing.allocator;

    var path = try JsonPath.parse(allocator, ".a.b");
    defer path.deinit();

    try std.testing.expectEqual(@as(usize, 3), path.segments.items.len);
    try std.testing.expectEqualStrings("a", path.segments.items[1].child);
    try std.testing.expectEqualStrings("b", path.segments.items[2].child);
}

test "parse array index $[0]" {
    const allocator = std.testing.allocator;

    var path = try JsonPath.parse(allocator, "$[0]");
    defer path.deinit();

    try std.testing.expectEqual(@as(usize, 2), path.segments.items.len);
    try std.testing.expectEqual(@as(i64, 0), path.segments.items[1].index);
}

test "parse negative index $[-1]" {
    const allocator = std.testing.allocator;

    var path = try JsonPath.parse(allocator, "$[-1]");
    defer path.deinit();

    try std.testing.expectEqual(@as(usize, 2), path.segments.items.len);
    try std.testing.expectEqual(@as(i64, -1), path.segments.items[1].index);
}

test "parse wildcard $.*" {
    const allocator = std.testing.allocator;

    var path = try JsonPath.parse(allocator, "$.*");
    defer path.deinit();

    try std.testing.expectEqual(@as(usize, 2), path.segments.items.len);
    try std.testing.expectEqual(JsonPath.PathSegment.wildcard, path.segments.items[1]);
}

test "parse recursive descent $..a" {
    const allocator = std.testing.allocator;

    var path = try JsonPath.parse(allocator, "$..a");
    defer path.deinit();

    try std.testing.expectEqual(@as(usize, 2), path.segments.items.len);
    try std.testing.expectEqualStrings("a", path.segments.items[1].recursive);
}

test "evaluate child path" {
    const allocator = std.testing.allocator;

    const json = "{\"a\":1,\"b\":2}";
    const node = try JsonNode.parse(allocator, json);
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    var path = try JsonPath.parse(allocator, "$.a");
    defer path.deinit();

    var results = try path.evaluate(node, allocator);
    defer results.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), results.items.len);
    try std.testing.expectEqual(@as(f64, 1.0), results.items[0].number);
}

test "evaluate array index" {
    const allocator = std.testing.allocator;

    const json = "[1,2,3]";
    const node = try JsonNode.parse(allocator, json);
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    var path = try JsonPath.parse(allocator, "$[1]");
    defer path.deinit();

    var results = try path.evaluate(node, allocator);
    defer results.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), results.items.len);
    try std.testing.expectEqual(@as(f64, 2.0), results.items[0].number);
}

test "evaluate negative array index" {
    const allocator = std.testing.allocator;

    const json = "[1,2,3]";
    const node = try JsonNode.parse(allocator, json);
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    var path = try JsonPath.parse(allocator, "$[-1]");
    defer path.deinit();

    var results = try path.evaluate(node, allocator);
    defer results.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), results.items.len);
    try std.testing.expectEqual(@as(f64, 3.0), results.items[0].number);
}

test "evaluate wildcard on object" {
    const allocator = std.testing.allocator;

    const json = "{\"a\":1,\"b\":2}";
    const node = try JsonNode.parse(allocator, json);
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    var path = try JsonPath.parse(allocator, "$.*");
    defer path.deinit();

    var results = try path.evaluate(node, allocator);
    defer results.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), results.items.len);
}

test "evaluate recursive descent" {
    const allocator = std.testing.allocator;

    const json = "{\"a\":{\"a\":1},\"b\":{\"a\":2}}";
    const node = try JsonNode.parse(allocator, json);
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    var path = try JsonPath.parse(allocator, "$..a");
    defer path.deinit();

    var results = try path.evaluate(node, allocator);
    defer results.deinit(allocator);

    // Should find 3 matches: top-level "a" object + 2 nested "a" values
    try std.testing.expectEqual(@as(usize, 3), results.items.len);
}
