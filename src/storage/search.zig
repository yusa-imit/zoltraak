const std = @import("std");
const Allocator = std.mem.Allocator;

/// Field type for search index schema
pub const FieldType = enum {
    text,
    tag,
    numeric,
    geo,
    vector,
    geoshape,

    pub fn fromString(s: []const u8) !FieldType {
        if (std.mem.eql(u8, s, "TEXT")) return .text;
        if (std.mem.eql(u8, s, "TAG")) return .tag;
        if (std.mem.eql(u8, s, "NUMERIC")) return .numeric;
        if (std.mem.eql(u8, s, "GEO")) return .geo;
        if (std.mem.eql(u8, s, "VECTOR")) return .vector;
        if (std.mem.eql(u8, s, "GEOSHAPE")) return .geoshape;
        return error.InvalidFieldType;
    }
};

/// Index data source type
pub const IndexOn = enum {
    hash,
    json,

    pub fn fromString(s: []const u8) !IndexOn {
        if (std.mem.eql(u8, s, "HASH")) return .hash;
        if (std.mem.eql(u8, s, "JSON")) return .json;
        return error.InvalidIndexOn;
    }
};

/// Field schema definition
pub const FieldSchema = struct {
    name: []const u8,
    field_type: FieldType,
    /// Optional alias for field (AS clause)
    alias: ?[]const u8,
    /// SORTABLE flag
    sortable: bool,
    /// NOINDEX flag
    noindex: bool,
    /// NOSTEM flag (TEXT only)
    nostem: bool,
    /// WEIGHT for TEXT fields
    weight: f64,
    /// SEPARATOR for TAG fields
    separator: u8,
    /// CASESENSITIVE for TAG fields
    casesensitive: bool,

    allocator: Allocator,

    pub fn init(allocator: Allocator, name: []const u8, field_type: FieldType) !FieldSchema {
        return FieldSchema{
            .name = try allocator.dupe(u8, name),
            .field_type = field_type,
            .alias = null,
            .sortable = false,
            .noindex = false,
            .nostem = false,
            .weight = 1.0,
            .separator = ',',
            .casesensitive = false,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *FieldSchema) void {
        self.allocator.free(self.name);
        if (self.alias) |alias| {
            self.allocator.free(alias);
        }
    }
};

/// Search index definition
pub const SearchIndex = struct {
    /// Index name
    name: []const u8,
    /// Data source type (HASH or JSON)
    index_on: IndexOn,
    /// Key prefix filter (optional)
    prefix: ?[]const u8,
    /// Field schemas
    fields: std.ArrayList(FieldSchema),
    /// Creation timestamp
    created_at: i64,

    allocator: Allocator,

    pub fn init(allocator: Allocator, name: []const u8, index_on: IndexOn) !SearchIndex {
        return SearchIndex{
            .name = try allocator.dupe(u8, name),
            .index_on = index_on,
            .prefix = null,
            .fields = try std.ArrayList(FieldSchema).initCapacity(allocator, 0),
            .created_at = std.time.timestamp(),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SearchIndex) void {
        self.allocator.free(self.name);
        if (self.prefix) |prefix| {
            self.allocator.free(prefix);
        }
        for (self.fields.items) |*field| {
            field.deinit();
        }
        self.fields.deinit(self.allocator);
    }

    /// Add field schema to index
    pub fn addField(self: *SearchIndex, field: FieldSchema) !void {
        try self.fields.append(self.allocator, field);
    }

    /// Set key prefix filter
    pub fn setPrefix(self: *SearchIndex, prefix: []const u8) !void {
        if (self.prefix) |old| {
            self.allocator.free(old);
        }
        self.prefix = try self.allocator.dupe(u8, prefix);
    }
};

/// Search index store — manages all indices
pub const SearchStore = struct {
    /// Map: index_name -> SearchIndex
    indices: std.StringHashMap(SearchIndex),
    allocator: Allocator,

    pub fn init(allocator: Allocator) SearchStore {
        return SearchStore{
            .indices = std.StringHashMap(SearchIndex).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SearchStore) void {
        var it = self.indices.iterator();
        while (it.next()) |entry| {
            var index = entry.value_ptr;
            index.deinit();
        }
        self.indices.deinit();
    }

    /// Create a new search index
    pub fn createIndex(self: *SearchStore, name: []const u8, index_on: IndexOn) !void {
        if (self.indices.contains(name)) {
            return error.IndexAlreadyExists;
        }

        const index = try SearchIndex.init(self.allocator, name, index_on);
        errdefer {
            var idx = index;
            idx.deinit();
        }

        try self.indices.put(name, index);
    }

    /// Get index by name (mutable)
    pub fn getIndex(self: *SearchStore, name: []const u8) ?*SearchIndex {
        return self.indices.getPtr(name);
    }

    /// Drop index by name
    pub fn dropIndex(self: *SearchStore, name: []const u8) !void {
        if (self.indices.fetchRemove(name)) |kv| {
            var index = kv.value;
            index.deinit();
        } else {
            return error.IndexNotFound;
        }
    }

    /// List all index names
    pub fn listIndices(self: *SearchStore, allocator: Allocator) ![][]const u8 {
        var names = try std.ArrayList([]const u8).initCapacity(allocator, 0);
        errdefer names.deinit(allocator);

        var it = self.indices.keyIterator();
        while (it.next()) |name| {
            try names.append(allocator, try allocator.dupe(u8, name.*));
        }

        return try names.toOwnedSlice(allocator);
    }
};

// Unit tests
test "SearchStore: create and list indices" {
    const allocator = std.testing.allocator;

    var store = SearchStore.init(allocator);
    defer store.deinit();

    try store.createIndex("idx1", .hash);
    try store.createIndex("idx2", .json);

    const names = try store.listIndices(allocator);
    defer {
        for (names) |name| {
            allocator.free(name);
        }
        allocator.free(names);
    }

    try std.testing.expectEqual(@as(usize, 2), names.len);
}

test "SearchStore: duplicate index error" {
    const allocator = std.testing.allocator;

    var store = SearchStore.init(allocator);
    defer store.deinit();

    try store.createIndex("idx1", .hash);
    const result = store.createIndex("idx1", .hash);
    try std.testing.expectError(error.IndexAlreadyExists, result);
}

test "SearchStore: drop index" {
    const allocator = std.testing.allocator;

    var store = SearchStore.init(allocator);
    defer store.deinit();

    try store.createIndex("idx1", .hash);
    try store.dropIndex("idx1");

    const names = try store.listIndices(allocator);
    defer allocator.free(names);

    try std.testing.expectEqual(@as(usize, 0), names.len);
}

test "SearchStore: drop nonexistent index" {
    const allocator = std.testing.allocator;

    var store = SearchStore.init(allocator);
    defer store.deinit();

    const result = store.dropIndex("nonexistent");
    try std.testing.expectError(error.IndexNotFound, result);
}

test "SearchIndex: add fields" {
    const allocator = std.testing.allocator;

    var index = try SearchIndex.init(allocator, "myindex", .hash);
    defer index.deinit();

    const field1 = try FieldSchema.init(allocator, "title", .text);
    const field2 = try FieldSchema.init(allocator, "price", .numeric);

    try index.addField(field1);
    try index.addField(field2);

    try std.testing.expectEqual(@as(usize, 2), index.fields.items.len);
    try std.testing.expectEqual(FieldType.text, index.fields.items[0].field_type);
    try std.testing.expectEqual(FieldType.numeric, index.fields.items[1].field_type);
}

test "SearchIndex: set prefix" {
    const allocator = std.testing.allocator;

    var index = try SearchIndex.init(allocator, "myindex", .hash);
    defer index.deinit();

    try index.setPrefix("product:");
    try std.testing.expect(index.prefix != null);
    try std.testing.expectEqualStrings("product:", index.prefix.?);
}

test "FieldType: fromString" {
    try std.testing.expectEqual(FieldType.text, try FieldType.fromString("TEXT"));
    try std.testing.expectEqual(FieldType.tag, try FieldType.fromString("TAG"));
    try std.testing.expectEqual(FieldType.numeric, try FieldType.fromString("NUMERIC"));
    try std.testing.expectEqual(FieldType.geo, try FieldType.fromString("GEO"));
    try std.testing.expectEqual(FieldType.vector, try FieldType.fromString("VECTOR"));
    try std.testing.expectEqual(FieldType.geoshape, try FieldType.fromString("GEOSHAPE"));
    try std.testing.expectError(error.InvalidFieldType, FieldType.fromString("INVALID"));
}

test "IndexOn: fromString" {
    try std.testing.expectEqual(IndexOn.hash, try IndexOn.fromString("HASH"));
    try std.testing.expectEqual(IndexOn.json, try IndexOn.fromString("JSON"));
    try std.testing.expectError(error.InvalidIndexOn, IndexOn.fromString("INVALID"));
}
