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

    /// Performs basic text search on indexed documents (stub implementation)
    ///
    /// This initial version performs a linear scan over all keys matching the index prefix.
    /// Future iterations will implement inverted index for efficient lookups.
    ///
    /// Arguments:
    ///   storage: pointer to Storage for accessing data HashMap
    ///   allocator: allocator for results
    ///   query: search query string (simple text match or "*" wildcard)
    ///   limit_offset: pagination offset (default 0)
    ///   limit_count: pagination count (default 10)
    ///   nocontent: if true, return only document IDs
    ///   return_fields: optional array of field names to return (null = all fields)
    ///   sortby_field: optional field name to sort by (must be SORTABLE)
    ///   sortby_desc: sort descending if true (default false/ASC)
    ///
    /// Returns:
    ///   SearchResult struct with total_count and documents
    pub fn search(
        self: *SearchIndex,
        storage: anytype,
        allocator: Allocator,
        query: []const u8,
        limit_offset: usize,
        limit_count: usize,
        nocontent: bool,
        return_fields: ?[]const []const u8,
        sortby_field: ?[]const u8,
        sortby_desc: bool,
    ) !SearchResult {
        _ = self;
        _ = storage;
        _ = sortby_field;
        _ = sortby_desc;

        // Parse query into terms (simple space-separated for now)
        const is_wildcard = std.mem.eql(u8, query, "*");
        var terms = try std.ArrayList([]const u8).initCapacity(allocator, 0);
        defer terms.deinit(allocator);

        if (!is_wildcard) {
            var iter = std.mem.splitScalar(u8, query, ' ');
            while (iter.next()) |term| {
                if (term.len > 0) {
                    try terms.append(allocator, term);
                }
            }
        }

        // Stub: For now, return empty results
        // Real implementation will iterate storage.data and match documents
        var documents = try std.ArrayList(Document).initCapacity(allocator, 0);
        errdefer {
            for (documents.items) |*doc| {
                doc.deinit(allocator);
            }
            documents.deinit(allocator);
        }

        // Apply pagination
        const start = @min(limit_offset, documents.items.len);
        const end = @min(limit_offset + limit_count, documents.items.len);

        // Create return slice
        const result_docs = try allocator.alloc(Document, end - start);
        errdefer allocator.free(result_docs);

        // Copy documents (this is a stub - real implementation will populate from storage)
        _ = nocontent;
        _ = return_fields;

        return SearchResult{
            .total_count = documents.items.len,
            .documents = result_docs,
            .allocator = allocator,
        };
    }

    /// Performs aggregation pipeline on search results (stub implementation)
    ///
    /// This initial version returns empty results. Future iterations will implement:
    /// - GROUP BY with field grouping
    /// - REDUCE operations (COUNT, SUM, AVG, MIN, MAX)
    /// - SORTBY clause
    /// - LOAD clause for field selection
    ///
    /// Arguments:
    ///   storage: pointer to Storage for accessing data HashMap
    ///   allocator: allocator for results
    ///   query: search query string
    ///   load_fields: optional fields to load into pipeline
    ///   groupby_fields: optional fields to group by
    ///   reduce_ops: reduce operations to apply
    ///   sortby_fields: optional fields to sort by
    ///   sortby_orders: sort orders (true=DESC, false=ASC)
    ///   limit_offset: pagination offset
    ///   limit_count: pagination count
    ///
    /// Returns:
    ///   AggregateResult with row count and aggregated rows
    pub fn aggregate(
        self: *SearchIndex,
        storage: anytype,
        allocator: Allocator,
        query: []const u8,
        load_fields: ?[]const []const u8,
        groupby_fields: ?[]const []const u8,
        reduce_ops: []const ReduceOp,
        sortby_fields: ?[]const []const u8,
        sortby_orders: ?[]bool,
        limit_offset: usize,
        limit_count: usize,
    ) !AggregateResult {
        // TODO: Real implementation will use all parameters for:
        // 1. search() with query to get base documents
        // 2. LOAD clause to extract load_fields
        // 3. GROUP BY groupby_fields
        // 4. Apply reduce_ops (COUNT/SUM/AVG/MIN/MAX)
        // 5. SORT BY sortby_fields with sortby_orders
        // 6. Apply LIMIT limit_offset/limit_count

        _ = self;
        _ = storage;
        _ = query;
        _ = load_fields;
        _ = groupby_fields;
        _ = reduce_ops;
        _ = sortby_fields;
        _ = sortby_orders;
        _ = limit_offset;
        _ = limit_count;

        // Stub: For now, return empty results

        const rows = try allocator.alloc(AggregateRow, 0);

        return AggregateResult{
            .total_count = 0,
            .rows = rows,
            .allocator = allocator,
        };
    }
};

/// Document result from search
pub const Document = struct {
    id: []const u8,
    fields: std.StringHashMap([]const u8),
    allocator: Allocator,

    pub fn init(allocator: Allocator, id: []const u8) !Document {
        return Document{
            .id = try allocator.dupe(u8, id),
            .fields = std.StringHashMap([]const u8).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Document, allocator: Allocator) void {
        allocator.free(self.id);
        var it = self.fields.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            allocator.free(entry.value_ptr.*);
        }
        self.fields.deinit();
    }

    /// Sets a field in the document with automatic memory duplication.
    ///
    /// Ownership: Caller retains ownership of name and value.
    /// This function duplicates both before storing.
    ///
    /// Arguments:
    ///   - name: Field name (will be duplicated)
    ///   - value: Field value (will be duplicated)
    ///
    /// Returns error if allocation fails or HashMap put fails.
    pub fn setField(self: *Document, name: []const u8, value: []const u8) !void {
        const name_copy = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(name_copy);

        const value_copy = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(value_copy);

        try self.fields.put(name_copy, value_copy);
    }
};

/// Search result with total count and documents
pub const SearchResult = struct {
    total_count: usize,
    documents: []Document,
    allocator: Allocator,

    pub fn deinit(self: *SearchResult) void {
        for (self.documents) |*doc| {
            doc.deinit(self.allocator);
        }
        self.allocator.free(self.documents);
    }
};

/// Reduce operation type for aggregation
pub const ReduceType = enum {
    count,
    sum,
    min,
    max,
    avg,

    pub fn fromString(s: []const u8) !ReduceType {
        if (std.mem.eql(u8, s, "COUNT")) return .count;
        if (std.mem.eql(u8, s, "SUM")) return .sum;
        if (std.mem.eql(u8, s, "MIN")) return .min;
        if (std.mem.eql(u8, s, "MAX")) return .max;
        if (std.mem.eql(u8, s, "AVG")) return .avg;
        return error.InvalidReduceType;
    }
};

/// Reduce operation definition
pub const ReduceOp = struct {
    reduce_type: ReduceType,
    args: []const []const u8,
    as_name: ?[]const u8,
};

/// Aggregation row (group result)
pub const AggregateRow = struct {
    fields: std.StringHashMap([]const u8),
    allocator: Allocator,

    pub fn init(allocator: Allocator) AggregateRow {
        return AggregateRow{
            .fields = std.StringHashMap([]const u8).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *AggregateRow) void {
        var it = self.fields.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.fields.deinit();
    }

    /// Sets a field in the aggregation row with automatic memory duplication.
    ///
    /// Ownership: Caller retains ownership of name and value.
    /// This function duplicates both before storing.
    ///
    /// Arguments:
    ///   - name: Field name (will be duplicated)
    ///   - value: Field value (will be duplicated)
    ///
    /// Returns error if allocation fails or HashMap put fails.
    pub fn setField(self: *AggregateRow, name: []const u8, value: []const u8) !void {
        const name_copy = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(name_copy);

        const value_copy = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(value_copy);

        try self.fields.put(name_copy, value_copy);
    }
};

/// Aggregation result
pub const AggregateResult = struct {
    total_count: usize,
    rows: []AggregateRow,
    allocator: Allocator,

    pub fn deinit(self: *AggregateResult) void {
        for (self.rows) |*row| {
            row.deinit();
        }
        self.allocator.free(self.rows);
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

test "SearchIndex: basic search returns empty results (stub)" {
    const allocator = std.testing.allocator;

    var index = try SearchIndex.init(allocator, "testIndex", .hash);
    defer index.deinit();

    // Stub search should return empty results
    var result = try index.search(null, allocator, "hello", 0, 10, false, null, null, false);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.total_count);
    try std.testing.expectEqual(@as(usize, 0), result.documents.len);
}

test "SearchIndex: wildcard search returns empty results (stub)" {
    const allocator = std.testing.allocator;

    var index = try SearchIndex.init(allocator, "testIndex", .hash);
    defer index.deinit();

    // Wildcard search should also return empty (stub)
    var result = try index.search(null, allocator, "*", 0, 10, false, null, null, false);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.total_count);
    try std.testing.expectEqual(@as(usize, 0), result.documents.len);
}

test "Document: init and deinit" {
    const allocator = std.testing.allocator;

    var doc = try Document.init(allocator, "doc:1");
    defer doc.deinit(allocator);

    try std.testing.expectEqualStrings("doc:1", doc.id);
    try std.testing.expectEqual(@as(usize, 0), doc.fields.count());
}

test "Document: setField" {
    const allocator = std.testing.allocator;

    var doc = try Document.init(allocator, "doc:1");
    defer doc.deinit(allocator);

    try doc.setField("title", "Hello World");
    try doc.setField("body", "Test document");

    try std.testing.expectEqual(@as(usize, 2), doc.fields.count());

    const title = doc.fields.get("title");
    try std.testing.expect(title != null);
    try std.testing.expectEqualStrings("Hello World", title.?);

    const body = doc.fields.get("body");
    try std.testing.expect(body != null);
    try std.testing.expectEqualStrings("Test document", body.?);
}
