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

    /// Performs spell checking on query terms (stub implementation)
    ///
    /// Returns empty suggestions for all terms. Real implementation will:
    /// 1. Parse query into terms
    /// 2. For each term, calculate Levenshtein distance to indexed terms
    /// 3. Filter suggestions by distance threshold
    /// 4. Apply include/exclude dictionaries
    /// 5. Calculate scores based on document frequency
    /// 6. Sort suggestions by score descending
    ///
    /// Arguments:
    ///   storage: pointer to Storage for accessing data
    ///   allocator: allocator for results
    ///   query: query string to spell check
    ///   distance: maximum Levenshtein distance (1-4)
    ///   include_dicts: list of dictionaries to include
    ///   exclude_dicts: list of dictionaries to exclude
    ///
    /// Returns:
    ///   SpellCheckResult with terms and suggestions
    pub fn spellCheck(
        self: *SearchIndex,
        storage: anytype,
        allocator: Allocator,
        query: []const u8,
        distance: u32,
        include_dicts: []const []const u8,
        exclude_dicts: []const []const u8,
    ) !SpellCheckResult {
        _ = self;
        _ = storage;
        _ = distance;
        _ = include_dicts;
        _ = exclude_dicts;
        _ = query;

        // Stub: Parse query into terms (simple whitespace split)
        var terms = try std.ArrayList(SpellCheckTermResult).initCapacity(allocator, 0);
        errdefer {
            for (terms.items) |*term| {
                term.deinit();
            }
            terms.deinit(allocator);
        }

        // TODO: Real implementation will:
        // 1. Parse query into terms
        // 2. For each term, calculate Levenshtein distance to indexed terms
        // 3. Filter suggestions by distance threshold
        // 4. Apply include/exclude dictionaries
        // 5. Calculate scores based on document frequency
        // 6. Sort suggestions by score descending

        const terms_slice = try terms.toOwnedSlice(allocator);
        return SpellCheckResult{
            .terms = terms_slice,
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

/// Spell check suggestion result
pub const SpellCheckSuggestion = struct {
    term: []const u8,
    score: f64,
};

/// Spell check term result
pub const SpellCheckTermResult = struct {
    original_term: []const u8,
    suggestions: []SpellCheckSuggestion,
    allocator: Allocator,

    pub fn deinit(self: *SpellCheckTermResult) void {
        for (self.suggestions) |*suggestion| {
            self.allocator.free(suggestion.term);
        }
        self.allocator.free(self.suggestions);
        self.allocator.free(self.original_term);
    }
};

/// Spell check result
pub const SpellCheckResult = struct {
    terms: []SpellCheckTermResult,
    allocator: Allocator,

    pub fn deinit(self: *SpellCheckResult) void {
        for (self.terms) |*term| {
            term.deinit();
        }
        self.allocator.free(self.terms);
    }
};

/// Search cursor for pagination
pub const SearchCursor = struct {
    /// Cursor ID (unique integer)
    id: u64,
    /// Index name
    index_name: []const u8,
    /// Search query
    query: []const u8,
    /// Current offset in results
    offset: usize,
    /// Total result count
    total_count: usize,
    /// Default page size (COUNT parameter)
    default_count: usize,
    /// Last access timestamp (for idle timeout)
    last_access: i64,
    /// Search parameters
    nocontent: bool,
    return_fields: ?[]const []const u8,
    sortby_field: ?[]const u8,
    sortby_desc: bool,

    allocator: Allocator,

    pub fn init(
        allocator: Allocator,
        id: u64,
        index_name: []const u8,
        query: []const u8,
        total_count: usize,
        default_count: usize,
        nocontent: bool,
        return_fields: ?[]const []const u8,
        sortby_field: ?[]const u8,
        sortby_desc: bool,
    ) !SearchCursor {
        const index_name_copy = try allocator.dupe(u8, index_name);
        errdefer allocator.free(index_name_copy);

        const query_copy = try allocator.dupe(u8, query);
        errdefer allocator.free(query_copy);

        const return_fields_copy = if (return_fields) |fields| blk: {
            const fields_copy = try allocator.alloc([]const u8, fields.len);
            var allocated_count: usize = 0;
            errdefer {
                for (fields_copy[0..allocated_count]) |field| {
                    allocator.free(field);
                }
                allocator.free(fields_copy);
            }
            for (fields, 0..) |field, i| {
                fields_copy[i] = try allocator.dupe(u8, field);
                allocated_count = i + 1;
            }
            break :blk fields_copy;
        } else null;
        errdefer if (return_fields_copy) |fields| {
            for (fields) |field| allocator.free(field);
            allocator.free(fields);
        };

        const sortby_field_copy = if (sortby_field) |field| try allocator.dupe(u8, field) else null;
        errdefer if (sortby_field_copy) |field| allocator.free(field);

        return SearchCursor{
            .id = id,
            .index_name = index_name_copy,
            .query = query_copy,
            .offset = 0,
            .total_count = total_count,
            .default_count = default_count,
            .last_access = std.time.timestamp(),
            .nocontent = nocontent,
            .return_fields = return_fields_copy,
            .sortby_field = sortby_field_copy,
            .sortby_desc = sortby_desc,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SearchCursor) void {
        self.allocator.free(self.index_name);
        self.allocator.free(self.query);
        if (self.return_fields) |fields| {
            for (fields) |field| {
                self.allocator.free(field);
            }
            self.allocator.free(fields);
        }
        if (self.sortby_field) |field| {
            self.allocator.free(field);
        }
    }
};

/// Search index store — manages all indices
pub const SearchStore = struct {
    /// Map: index_name -> SearchIndex
    indices: std.StringHashMap(SearchIndex),
    /// Map: cursor_id -> SearchCursor (for pagination)
    cursors: std.AutoHashMap(u64, SearchCursor),
    /// Next cursor ID
    next_cursor_id: u64,
    /// Maximum cursor idle time in seconds (default 300)
    cursor_max_idle: i64,
    allocator: Allocator,

    pub fn init(allocator: Allocator) SearchStore {
        return SearchStore{
            .indices = std.StringHashMap(SearchIndex).init(allocator),
            .cursors = std.AutoHashMap(u64, SearchCursor).init(allocator),
            .next_cursor_id = 1,
            .cursor_max_idle = 300, // 300 seconds = 5 minutes
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

        var cursor_it = self.cursors.iterator();
        while (cursor_it.next()) |entry| {
            var cursor = entry.value_ptr;
            cursor.deinit();
        }
        self.cursors.deinit();
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

    /// Create a new cursor for pagination
    ///
    /// Arguments:
    ///   index_name: name of the index
    ///   query: search query
    ///   total_count: total number of results
    ///   default_count: default page size
    ///   nocontent: whether to return content
    ///   return_fields: optional fields to return
    ///   sortby_field: optional sort field
    ///   sortby_desc: sort descending
    ///
    /// Returns:
    ///   cursor_id for subsequent FT.CURSOR READ calls
    pub fn createCursor(
        self: *SearchStore,
        index_name: []const u8,
        query: []const u8,
        total_count: usize,
        default_count: usize,
        nocontent: bool,
        return_fields: ?[]const []const u8,
        sortby_field: ?[]const u8,
        sortby_desc: bool,
    ) !u64 {
        const cursor_id = self.next_cursor_id;
        self.next_cursor_id += 1;

        const cursor = try SearchCursor.init(
            self.allocator,
            cursor_id,
            index_name,
            query,
            total_count,
            default_count,
            nocontent,
            return_fields,
            sortby_field,
            sortby_desc,
        );
        errdefer {
            var c = cursor;
            c.deinit();
        }

        try self.cursors.put(cursor_id, cursor);
        return cursor_id;
    }

    /// Get mutable cursor by ID
    ///
    /// Returns null if cursor not found or expired.
    pub fn getCursor(self: *SearchStore, cursor_id: u64) ?*SearchCursor {
        const cursor_ptr = self.cursors.getPtr(cursor_id) orelse return null;

        // Check if cursor expired
        const now = std.time.timestamp();
        if (now - cursor_ptr.last_access > self.cursor_max_idle) {
            // Cursor expired, remove it
            var cursor = self.cursors.fetchRemove(cursor_id).?.value;
            cursor.deinit();
            return null;
        }

        // Update last access time
        cursor_ptr.last_access = now;
        return cursor_ptr;
    }

    /// Delete cursor by ID
    ///
    /// Returns error if cursor doesn't exist.
    pub fn deleteCursor(self: *SearchStore, cursor_id: u64) !void {
        if (self.cursors.fetchRemove(cursor_id)) |kv| {
            var cursor = kv.value;
            cursor.deinit();
        } else {
            return error.CursorNotFound;
        }
    }

    /// Expire old cursors (cleanup task)
    ///
    /// Should be called periodically to clean up idle cursors.
    /// Returns number of cursors expired.
    pub fn expireOldCursors(self: *SearchStore) usize {
        const now = std.time.timestamp();
        var expired_count: usize = 0;

        var expired_ids = std.ArrayList(u64).initCapacity(self.allocator, 0) catch return 0;
        defer expired_ids.deinit(self.allocator);

        // Collect expired cursor IDs
        var it = self.cursors.iterator();
        while (it.next()) |entry| {
            const cursor = entry.value_ptr;
            if (now - cursor.last_access > self.cursor_max_idle) {
                expired_ids.append(self.allocator, entry.key_ptr.*) catch continue;
            }
        }

        // Remove expired cursors
        for (expired_ids.items) |cursor_id| {
            if (self.cursors.fetchRemove(cursor_id)) |kv| {
                var cursor = kv.value;
                cursor.deinit();
                expired_count += 1;
            }
        }

        return expired_count;
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

test "SearchStore: create cursor" {
    const allocator = std.testing.allocator;

    var store = SearchStore.init(allocator);
    defer store.deinit();

    const cursor_id = try store.createCursor("idx1", "test query", 100, 10, false, null, null, false);
    try std.testing.expectEqual(@as(u64, 1), cursor_id);

    const cursor = store.getCursor(cursor_id);
    try std.testing.expect(cursor != null);
    try std.testing.expectEqualStrings("idx1", cursor.?.index_name);
    try std.testing.expectEqualStrings("test query", cursor.?.query);
    try std.testing.expectEqual(@as(usize, 100), cursor.?.total_count);
    try std.testing.expectEqual(@as(usize, 10), cursor.?.default_count);
}

test "SearchStore: cursor ID increments" {
    const allocator = std.testing.allocator;

    var store = SearchStore.init(allocator);
    defer store.deinit();

    const cursor_id1 = try store.createCursor("idx1", "query1", 50, 5, false, null, null, false);
    const cursor_id2 = try store.createCursor("idx2", "query2", 100, 10, false, null, null, false);

    try std.testing.expectEqual(@as(u64, 1), cursor_id1);
    try std.testing.expectEqual(@as(u64, 2), cursor_id2);
}

test "SearchStore: delete cursor" {
    const allocator = std.testing.allocator;

    var store = SearchStore.init(allocator);
    defer store.deinit();

    const cursor_id = try store.createCursor("idx1", "query", 50, 5, false, null, null, false);
    try store.deleteCursor(cursor_id);

    const cursor = store.getCursor(cursor_id);
    try std.testing.expect(cursor == null);
}

test "SearchStore: delete nonexistent cursor" {
    const allocator = std.testing.allocator;

    var store = SearchStore.init(allocator);
    defer store.deinit();

    const result = store.deleteCursor(999);
    try std.testing.expectError(error.CursorNotFound, result);
}

test "SearchStore: cursor with return_fields" {
    const allocator = std.testing.allocator;

    var store = SearchStore.init(allocator);
    defer store.deinit();

    var fields = [_][]const u8{ "field1", "field2" };
    const cursor_id = try store.createCursor("idx1", "query", 50, 5, false, &fields, null, false);

    const cursor = store.getCursor(cursor_id);
    try std.testing.expect(cursor != null);
    try std.testing.expect(cursor.?.return_fields != null);
    try std.testing.expectEqual(@as(usize, 2), cursor.?.return_fields.?.len);
    try std.testing.expectEqualStrings("field1", cursor.?.return_fields.?[0]);
    try std.testing.expectEqualStrings("field2", cursor.?.return_fields.?[1]);
}

test "SearchStore: cursor offset tracking" {
    const allocator = std.testing.allocator;

    var store = SearchStore.init(allocator);
    defer store.deinit();

    const cursor_id = try store.createCursor("idx1", "query", 100, 10, false, null, null, false);

    const cursor = store.getCursor(cursor_id);
    try std.testing.expect(cursor != null);
    try std.testing.expectEqual(@as(usize, 0), cursor.?.offset);

    // Simulate reading a page
    cursor.?.offset += 10;
    try std.testing.expectEqual(@as(usize, 10), cursor.?.offset);
}

test "SearchStore: expire old cursors" {
    const allocator = std.testing.allocator;

    var store = SearchStore.init(allocator);
    defer store.deinit();

    // Create cursor and manually set old last_access time
    const cursor_id = try store.createCursor("idx1", "query", 50, 5, false, null, null, false);

    const cursor = store.getCursor(cursor_id);
    try std.testing.expect(cursor != null);

    // Set last_access to 400 seconds ago (beyond 300s timeout)
    cursor.?.last_access = std.time.timestamp() - 400;

    // Expire old cursors
    const expired_count = store.expireOldCursors();
    try std.testing.expectEqual(@as(usize, 1), expired_count);

    // Cursor should be gone
    const cursor_after = store.getCursor(cursor_id);
    try std.testing.expect(cursor_after == null);
}
