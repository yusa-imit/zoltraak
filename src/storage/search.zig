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

/// Dictionary for stop words and synonyms
pub const Dictionary = struct {
    /// Dictionary name
    name: []const u8,
    /// Map: term -> void (for O(1) deduplication)
    /// Keys are owned strings
    terms: std.StringHashMap(void),
    /// Insertion order list (references keys in terms map)
    order: std.ArrayList([]const u8),
    allocator: Allocator,

    /// Initialize a new dictionary
    pub fn init(allocator: Allocator, name: []const u8) !Dictionary {
        const name_copy = try allocator.dupe(u8, name);
        errdefer allocator.free(name_copy);

        return Dictionary{
            .name = name_copy,
            .terms = std.StringHashMap(void).init(allocator),
            .order = try std.ArrayList([]const u8).initCapacity(allocator, 0),
            .allocator = allocator,
        };
    }

    /// Free dictionary resources
    pub fn deinit(self: *Dictionary) void {
        // Free all term strings in the map
        var it = self.terms.keyIterator();
        while (it.next()) |term| {
            self.allocator.free(term.*);
        }
        self.terms.deinit();
        self.order.deinit(self.allocator);
        self.allocator.free(self.name);
    }

    /// Add a term to the dictionary
    /// Returns true if the term was newly added, false if it already existed
    pub fn addTerm(self: *Dictionary, term: []const u8) !bool {
        // Check if term already exists
        if (self.terms.contains(term)) {
            return false;
        }

        // Allocate new term string
        const term_copy = try self.allocator.dupe(u8, term);
        errdefer self.allocator.free(term_copy);

        // Add to terms map
        try self.terms.put(term_copy, {});

        // Add to order list
        try self.order.append(self.allocator, term_copy);

        return true;
    }

    /// Remove a term from the dictionary
    /// Returns true if the term existed and was removed, false otherwise
    pub fn removeTerm(self: *Dictionary, term: []const u8) bool {
        if (self.terms.fetchRemove(term)) |kv| {
            // Free the removed term string
            self.allocator.free(kv.key);

            // Remove from order list
            for (self.order.items, 0..) |item, i| {
                if (std.mem.eql(u8, item, term)) {
                    _ = self.order.orderedRemove(i);
                    break;
                }
            }

            return true;
        }
        return false;
    }

    /// Get terms in insertion order
    pub fn getTerms(self: *Dictionary) []const []const u8 {
        return self.order.items;
    }
};

/// Synonym group for search index
pub const SynonymGroup = struct {
    /// Synonym group ID (assigned by user via FT.SYNUPDATE)
    id: u64,
    /// List of synonym terms
    terms: std.ArrayList([]const u8),
    allocator: Allocator,

    pub fn init(allocator: Allocator, id: u64) !SynonymGroup {
        return SynonymGroup{
            .id = id,
            .terms = try std.ArrayList([]const u8).initCapacity(allocator, 0),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SynonymGroup) void {
        for (self.terms.items) |term| {
            self.allocator.free(term);
        }
        self.terms.deinit(self.allocator);
    }

    /// Add synonym term to group (no duplicates)
    pub fn addTerm(self: *SynonymGroup, term: []const u8) !void {
        // Check for duplicates
        for (self.terms.items) |existing| {
            if (std.mem.eql(u8, existing, term)) {
                return; // Already exists, skip
            }
        }

        const term_copy = try self.allocator.dupe(u8, term);
        errdefer self.allocator.free(term_copy);

        try self.terms.append(self.allocator, term_copy);
    }

    /// Get all terms in the group
    pub fn getTerms(self: *SynonymGroup) []const []const u8 {
        return self.terms.items;
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
    /// Map: synonym_group_id -> SynonymGroup
    synonym_groups: std.AutoHashMap(u64, SynonymGroup),

    allocator: Allocator,

    pub fn init(allocator: Allocator, name: []const u8, index_on: IndexOn) !SearchIndex {
        return SearchIndex{
            .name = try allocator.dupe(u8, name),
            .index_on = index_on,
            .prefix = null,
            .fields = try std.ArrayList(FieldSchema).initCapacity(allocator, 0),
            .created_at = std.time.timestamp(),
            .synonym_groups = std.AutoHashMap(u64, SynonymGroup).init(allocator),
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

        // Free synonym groups
        var syn_it = self.synonym_groups.valueIterator();
        while (syn_it.next()) |group| {
            var g = group;
            g.deinit();
        }
        self.synonym_groups.deinit();
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

    /// Add or update synonym group
    ///
    /// If the group already exists, it is replaced with the new terms.
    /// Arguments:
    ///   group_id: Synonym group ID
    ///   terms: Array of synonym terms
    pub fn updateSynonymGroup(self: *SearchIndex, group_id: u64, terms: []const []const u8) !void {
        // Remove existing group if present
        if (self.synonym_groups.fetchRemove(group_id)) |kv| {
            var old_group = kv.value;
            old_group.deinit();
        }

        // Create new group
        var group = try SynonymGroup.init(self.allocator, group_id);
        errdefer group.deinit();

        for (terms) |term| {
            try group.addTerm(term);
        }

        try self.synonym_groups.put(group_id, group);
    }

    /// Get all synonym groups in the index
    ///
    /// Returns array of group_id values sorted in ascending order.
    /// Caller must free the returned array.
    pub fn getSynonymGroupIds(self: *SearchIndex, allocator: Allocator) ![]u64 {
        var ids = try std.ArrayList(u64).initCapacity(allocator, self.synonym_groups.count());
        errdefer ids.deinit(allocator);

        var it = self.synonym_groups.keyIterator();
        while (it.next()) |key| {
            try ids.append(allocator, key.*);
        }

        // Sort in ascending order
        std.mem.sort(u64, ids.items, {}, std.sort.asc(u64));

        return try ids.toOwnedSlice(allocator);
    }

    /// Get synonym group by ID
    pub fn getSynonymGroup(self: *SearchIndex, group_id: u64) ?*SynonymGroup {
        return self.synonym_groups.getPtr(group_id);
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
    /// Map: alias_name -> target_index_name
    aliases: std.StringHashMap([]const u8),
    /// Map: dictionary_name -> Dictionary
    dictionaries: std.StringHashMap(Dictionary),
    allocator: Allocator,

    pub fn init(allocator: Allocator) SearchStore {
        return SearchStore{
            .indices = std.StringHashMap(SearchIndex).init(allocator),
            .cursors = std.AutoHashMap(u64, SearchCursor).init(allocator),
            .next_cursor_id = 1,
            .cursor_max_idle = 300, // 300 seconds = 5 minutes
            .aliases = std.StringHashMap([]const u8).init(allocator),
            .dictionaries = std.StringHashMap(Dictionary).init(allocator),
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

        // Free aliases: both key and value strings
        var alias_it = self.aliases.iterator();
        while (alias_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.aliases.deinit();

        // Free dictionaries
        var dict_it = self.dictionaries.iterator();
        while (dict_it.next()) |entry| {
            var dict = entry.value_ptr;
            dict.deinit();
        }
        self.dictionaries.deinit();
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

    /// Drop index by name and remove all aliases pointing to it
    pub fn dropIndex(self: *SearchStore, name: []const u8) !void {
        if (self.indices.fetchRemove(name)) |kv| {
            var index = kv.value;
            index.deinit();
        } else {
            return error.IndexNotFound;
        }

        // Remove all aliases pointing to this index
        var alias_keys_to_remove = std.ArrayList([]const u8).initCapacity(self.allocator, 0) catch return;
        errdefer alias_keys_to_remove.deinit(self.allocator);

        var alias_it = self.aliases.iterator();
        while (alias_it.next()) |entry| {
            if (std.mem.eql(u8, entry.value_ptr.*, name)) {
                alias_keys_to_remove.append(self.allocator, entry.key_ptr.*) catch return;
            }
        }

        for (alias_keys_to_remove.items) |alias_key| {
            if (self.aliases.fetchRemove(alias_key)) |kv| {
                self.allocator.free(kv.key);
                self.allocator.free(kv.value);
            }
        }
        alias_keys_to_remove.deinit(self.allocator);
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

    /// Add an alias for an index
    ///
    /// Arguments:
    ///   alias: alias name
    ///   index_name: target index name
    ///
    /// Returns error if:
    ///   - index doesn't exist (IndexNotFound)
    ///   - alias already exists (AliasAlreadyExists)
    ///   - alias name equals index name (AliasEqualsIndexName)
    pub fn addAlias(self: *SearchStore, alias: []const u8, index_name: []const u8) !void {
        // Validate index exists
        if (!self.indices.contains(index_name)) {
            return error.IndexNotFound;
        }

        // Validate alias != index_name
        if (std.mem.eql(u8, alias, index_name)) {
            return error.AliasEqualsIndexName;
        }

        // Validate alias not already in aliases map
        if (self.aliases.contains(alias)) {
            return error.AliasAlreadyExists;
        }

        // Duplicate alias and index_name strings
        const alias_copy = try self.allocator.dupe(u8, alias);
        errdefer self.allocator.free(alias_copy);

        const index_copy = try self.allocator.dupe(u8, index_name);
        errdefer self.allocator.free(index_copy);

        // Put into aliases map
        try self.aliases.put(alias_copy, index_copy);
    }

    /// Delete an alias
    ///
    /// Arguments:
    ///   alias: alias name to delete
    ///
    /// Returns error if alias doesn't exist (AliasNotFound)
    pub fn deleteAlias(self: *SearchStore, alias: []const u8) !void {
        if (self.aliases.fetchRemove(alias)) |kv| {
            self.allocator.free(kv.key);
            self.allocator.free(kv.value);
        } else {
            return error.AliasNotFound;
        }
    }

    /// Update an existing alias to point to a different index
    ///
    /// Arguments:
    ///   alias: existing alias name
    ///   new_index_name: new target index name
    ///
    /// Returns error if:
    ///   - alias doesn't exist (AliasNotFound)
    ///   - new_index doesn't exist (IndexNotFound)
    pub fn updateAlias(self: *SearchStore, alias: []const u8, new_index_name: []const u8) !void {
        // Check alias exists
        if (!self.aliases.contains(alias)) {
            return error.AliasNotFound;
        }

        // Validate new_index exists
        if (!self.indices.contains(new_index_name)) {
            return error.IndexNotFound;
        }

        // Duplicate new_index_name
        const index_copy = try self.allocator.dupe(u8, new_index_name);
        errdefer self.allocator.free(index_copy);

        // Update map value
        if (self.aliases.fetchRemove(alias)) |kv| {
            self.allocator.free(kv.value);
            try self.aliases.put(kv.key, index_copy);
        }
    }

    /// Resolve a name to an index name
    ///
    /// If name is an index, returns the index name.
    /// If name is an alias, returns the target index name.
    /// Otherwise returns IndexNotFound.
    pub fn getIndexByAlias(self: *SearchStore, name_or_alias: []const u8) ![]const u8 {
        // Check if it's an index name
        if (self.indices.contains(name_or_alias)) {
            return name_or_alias;
        }

        // Check if it's an alias
        if (self.aliases.get(name_or_alias)) |target_name| {
            return target_name;
        }

        return error.IndexNotFound;
    }

    /// Add multiple terms to a dictionary
    ///
    /// Creates the dictionary if it doesn't exist.
    /// Returns the count of newly added terms (duplicates are ignored).
    pub fn addTermsToDictionary(self: *SearchStore, dict_name: []const u8, terms: []const []const u8) !u64 {
        // Get or create dictionary
        if (!self.dictionaries.contains(dict_name)) {
            const dict = try Dictionary.init(self.allocator, dict_name);
            errdefer {
                var d = dict;
                d.deinit();
            }
            try self.dictionaries.put(dict.name, dict);
        }

        // Get mutable dictionary pointer
        var dict = self.dictionaries.getPtr(dict_name).?;

        // Add terms and count newly added ones
        var added_count: u64 = 0;
        for (terms) |term| {
            const is_new = try dict.addTerm(term);
            if (is_new) {
                added_count += 1;
            }
        }

        return added_count;
    }

    /// Remove multiple terms from a dictionary
    ///
    /// Returns the count of removed terms.
    /// Returns 0 if the dictionary doesn't exist.
    pub fn removeTermsFromDictionary(self: *SearchStore, dict_name: []const u8, terms: []const []const u8) !u64 {
        // If dictionary doesn't exist, return 0
        if (!self.dictionaries.contains(dict_name)) {
            return 0;
        }

        // Get mutable dictionary pointer
        var dict = self.dictionaries.getPtr(dict_name).?;

        // Remove terms and count removed ones
        var removed_count: u64 = 0;
        for (terms) |term| {
            const was_removed = dict.removeTerm(term);
            if (was_removed) {
                removed_count += 1;
            }
        }

        return removed_count;
    }

    /// Dump all terms from a dictionary in insertion order
    ///
    /// Returns empty array if dictionary doesn't exist.
    /// Returned array and strings must be freed by caller.
    pub fn dumpDictionary(self: *SearchStore, allocator: Allocator, dict_name: []const u8) ![][]const u8 {
        // If dictionary doesn't exist, return empty array
        if (!self.dictionaries.contains(dict_name)) {
            return try allocator.alloc([]const u8, 0);
        }

        const dict = self.dictionaries.getPtr(dict_name).?;
        const terms = dict.getTerms();

        // Allocate result array and copy term strings
        var result = try allocator.alloc([]const u8, terms.len);
        errdefer allocator.free(result);

        for (terms, 0..) |term, i| {
            result[i] = try allocator.dupe(u8, term);
            errdefer {
                for (result[0..i]) |term_copy| {
                    allocator.free(term_copy);
                }
            }
        }

        return result;
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

// Dictionary tests
test "Dictionary: init and deinit" {
    const allocator = std.testing.allocator;

    var dict = try Dictionary.init(allocator, "mydict");
    defer dict.deinit();

    try std.testing.expectEqualStrings("mydict", dict.name);
    try std.testing.expectEqual(@as(usize, 0), dict.terms.count());
}

test "Dictionary: add single term" {
    const allocator = std.testing.allocator;

    var dict = try Dictionary.init(allocator, "mydict");
    defer dict.deinit();

    const is_new = try dict.addTerm("hello");
    try std.testing.expect(is_new);
    try std.testing.expectEqual(@as(usize, 1), dict.terms.count());
}

test "Dictionary: add duplicate term returns false" {
    const allocator = std.testing.allocator;

    var dict = try Dictionary.init(allocator, "mydict");
    defer dict.deinit();

    const is_new1 = try dict.addTerm("hello");
    const is_new2 = try dict.addTerm("hello");

    try std.testing.expect(is_new1);
    try std.testing.expect(!is_new2);
    try std.testing.expectEqual(@as(usize, 1), dict.terms.count());
}

test "Dictionary: insertion order preserved" {
    const allocator = std.testing.allocator;

    var dict = try Dictionary.init(allocator, "mydict");
    defer dict.deinit();

    _ = try dict.addTerm("zebra");
    _ = try dict.addTerm("apple");
    _ = try dict.addTerm("middle");

    const terms = dict.getTerms();
    try std.testing.expectEqual(@as(usize, 3), terms.len);
    try std.testing.expectEqualStrings("zebra", terms[0]);
    try std.testing.expectEqualStrings("apple", terms[1]);
    try std.testing.expectEqualStrings("middle", terms[2]);
}

test "Dictionary: remove term" {
    const allocator = std.testing.allocator;

    var dict = try Dictionary.init(allocator, "mydict");
    defer dict.deinit();

    _ = try dict.addTerm("hello");
    _ = try dict.addTerm("world");

    const was_removed = dict.removeTerm("hello");
    try std.testing.expect(was_removed);
    try std.testing.expectEqual(@as(usize, 1), dict.terms.count());
    try std.testing.expectEqual(@as(usize, 1), dict.getTerms().len);
}

test "Dictionary: remove nonexistent term" {
    const allocator = std.testing.allocator;

    var dict = try Dictionary.init(allocator, "mydict");
    defer dict.deinit();

    const was_removed = dict.removeTerm("nonexistent");
    try std.testing.expect(!was_removed);
}

test "SearchStore: addTermsToDictionary creates dict" {
    const allocator = std.testing.allocator;

    var store = SearchStore.init(allocator);
    defer store.deinit();

    var terms = [_][]const u8{ "hello", "world" };
    const count = try store.addTermsToDictionary("mydict", &terms);

    try std.testing.expectEqual(@as(u64, 2), count);
    try std.testing.expect(store.dictionaries.contains("mydict"));
}

test "SearchStore: addTermsToDictionary ignores duplicates" {
    const allocator = std.testing.allocator;

    var store = SearchStore.init(allocator);
    defer store.deinit();

    var terms1 = [_][]const u8{ "hello", "world" };
    const count1 = try store.addTermsToDictionary("mydict", &terms1);
    try std.testing.expectEqual(@as(u64, 2), count1);

    var terms2 = [_][]const u8{ "hello", "foo" };
    const count2 = try store.addTermsToDictionary("mydict", &terms2);
    try std.testing.expectEqual(@as(u64, 1), count2);
}

test "SearchStore: removeTermsFromDictionary" {
    const allocator = std.testing.allocator;

    var store = SearchStore.init(allocator);
    defer store.deinit();

    var add_terms = [_][]const u8{ "a", "b", "c" };
    _ = try store.addTermsToDictionary("mydict", &add_terms);

    var remove_terms = [_][]const u8{ "a", "c" };
    const count = try store.removeTermsFromDictionary("mydict", &remove_terms);

    try std.testing.expectEqual(@as(u64, 2), count);
}

test "SearchStore: removeTermsFromDictionary returns 0 for nonexistent dict" {
    const allocator = std.testing.allocator;

    var store = SearchStore.init(allocator);
    defer store.deinit();

    var terms = [_][]const u8{ "hello" };
    const count = try store.removeTermsFromDictionary("nonexistent", &terms);

    try std.testing.expectEqual(@as(u64, 0), count);
}

test "SearchStore: dumpDictionary returns terms in order" {
    const allocator = std.testing.allocator;

    var store = SearchStore.init(allocator);
    defer store.deinit();

    var add_terms = [_][]const u8{ "zebra", "apple", "middle" };
    _ = try store.addTermsToDictionary("mydict", &add_terms);

    const terms = try store.dumpDictionary(allocator, "mydict");
    defer {
        for (terms) |term| {
            allocator.free(term);
        }
        allocator.free(terms);
    }

    try std.testing.expectEqual(@as(usize, 3), terms.len);
    try std.testing.expectEqualStrings("zebra", terms[0]);
    try std.testing.expectEqualStrings("apple", terms[1]);
    try std.testing.expectEqualStrings("middle", terms[2]);
}

test "SearchStore: dumpDictionary returns empty for nonexistent dict" {
    const allocator = std.testing.allocator;

    var store = SearchStore.init(allocator);
    defer store.deinit();

    const terms = try store.dumpDictionary(allocator, "nonexistent");
    defer allocator.free(terms);

    try std.testing.expectEqual(@as(usize, 0), terms.len);
}
