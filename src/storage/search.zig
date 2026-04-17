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

/// Node in the suggestion trie
pub const SuggestionNode = struct {
    /// True if this node represents a complete suggestion
    is_terminal: bool,
    /// Score for this suggestion (if terminal)
    score: f64,
    /// Optional payload (if terminal)
    payload: ?[]const u8,
    /// Children map: character -> child node
    children: std.AutoHashMap(u8, *SuggestionNode),
    allocator: Allocator,

    /// Initialize a new non-terminal node
    pub fn init(allocator: Allocator) !SuggestionNode {
        return SuggestionNode{
            .is_terminal = false,
            .score = 0.0,
            .payload = null,
            .children = std.AutoHashMap(u8, *SuggestionNode).init(allocator),
            .allocator = allocator,
        };
    }

    /// Recursively free node and all children
    pub fn deinit(self: *SuggestionNode) void {
        // Free children recursively
        var it = self.children.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.children.deinit();

        // Free payload if exists
        if (self.payload) |payload| {
            self.allocator.free(payload);
        }
    }

    /// Mark node as terminal with score and optional payload
    pub fn markTerminal(self: *SuggestionNode, score: f64, payload: ?[]const u8) !void {
        self.is_terminal = true;
        self.score = score;

        // Free old payload if exists
        if (self.payload) |old_payload| {
            self.allocator.free(old_payload);
        }

        // Copy new payload if provided
        self.payload = if (payload) |p| try self.allocator.dupe(u8, p) else null;
    }

    /// Get or create child node for character
    pub fn getOrCreateChild(self: *SuggestionNode, char: u8) !*SuggestionNode {
        if (self.children.get(char)) |child| {
            return child;
        }

        // Create new child
        const child = try self.allocator.create(SuggestionNode);
        errdefer self.allocator.destroy(child);

        child.* = try SuggestionNode.init(self.allocator);
        errdefer child.deinit();

        try self.children.put(char, child);
        return child;
    }

    /// Get child node for character (returns null if not exists)
    pub fn getChild(self: *SuggestionNode, char: u8) ?*SuggestionNode {
        return self.children.get(char);
    }
};

/// Suggestion result with score and optional payload
pub const Suggestion = struct {
    string: []const u8,
    score: f64,
    payload: ?[]const u8,
};

/// Auto-complete suggestion dictionary (trie-based)
pub const SuggestionDictionary = struct {
    /// Root trie node
    root: SuggestionNode,
    /// Total number of suggestions
    count: u64,
    allocator: Allocator,

    /// Initialize empty dictionary
    pub fn init(allocator: Allocator) !SuggestionDictionary {
        return SuggestionDictionary{
            .root = try SuggestionNode.init(allocator),
            .count = 0,
            .allocator = allocator,
        };
    }

    /// Free all nodes and payloads
    pub fn deinit(self: *SuggestionDictionary) void {
        self.root.deinit();
    }

    /// Add or update suggestion with score
    /// If INCR is true, adds score to existing. Otherwise replaces.
    /// Returns true if this is a new insertion (affects count)
    pub fn addSuggestion(self: *SuggestionDictionary, string: []const u8, score: f64, payload: ?[]const u8, incr: bool) !bool {
        var node = &self.root;

        // Traverse/create path
        for (string) |char| {
            node = try node.getOrCreateChild(char);
        }

        const is_new = !node.is_terminal;

        // Update score
        const new_score = if (incr and node.is_terminal) node.score + score else score;
        try node.markTerminal(new_score, payload);

        if (is_new) {
            self.count += 1;
        }

        return is_new;
    }

    /// Get suggestions matching prefix, sorted by score descending
    /// Returns owned array of Suggestion (caller must free strings and payload)
    pub fn getSuggestions(self: *SuggestionDictionary, prefix: []const u8, max: usize) !std.ArrayList(Suggestion) {
        var results = try std.ArrayList(Suggestion).initCapacity(self.allocator, 0);
        errdefer {
            for (results.items) |item| {
                self.allocator.free(item.string);
                if (item.payload) |p| self.allocator.free(p);
            }
            results.deinit(self.allocator);
        }

        // Navigate to prefix node
        var node = &self.root;
        for (prefix) |char| {
            if (node.getChild(char)) |child| {
                node = child;
            } else {
                // Prefix not found, return empty
                return results;
            }
        }

        // Collect all suggestions under this prefix
        // Note: collectSuggestions uses dynamic allocation per recursion to avoid buffer overflow
        const prefix_copy = try self.allocator.dupe(u8, prefix);
        defer self.allocator.free(prefix_copy);

        try self.collectSuggestions(node, prefix_copy, &results);

        // Sort by score descending
        std.mem.sort(Suggestion, results.items, {}, compareByScore);

        // Limit results
        if (results.items.len > max) {
            // Free extra items
            for (results.items[max..]) |item| {
                self.allocator.free(item.string);
                if (item.payload) |p| self.allocator.free(p);
            }
            results.items.len = max;
        }

        return results;
    }

    /// Collect all terminal nodes recursively using dynamic allocation
    /// This avoids buffer overflow by allocating a new buffer per recursion level
    fn collectSuggestions(self: *SuggestionDictionary, node: *SuggestionNode, current_prefix: []const u8, results: *std.ArrayList(Suggestion)) !void {
        if (node.is_terminal) {
            const string_copy = try self.allocator.dupe(u8, current_prefix);
            errdefer self.allocator.free(string_copy);

            const payload_copy = if (node.payload) |p| try self.allocator.dupe(u8, p) else null;
            errdefer if (payload_copy) |p| self.allocator.free(p);

            try results.append(self.allocator, Suggestion{
                .string = string_copy,
                .score = node.score,
                .payload = payload_copy,
            });
        }

        // Recurse into children with dynamically extended prefix
        var it = node.children.iterator();
        while (it.next()) |entry| {
            const char = entry.key_ptr.*;
            const child = entry.value_ptr.*;

            // Allocate new buffer for extended prefix (safe, no overflow risk)
            var extended = try self.allocator.alloc(u8, current_prefix.len + 1);
            defer self.allocator.free(extended);

            @memcpy(extended[0..current_prefix.len], current_prefix);
            extended[current_prefix.len] = char;

            try self.collectSuggestions(child, extended, results);
        }
    }

    /// Get fuzzy suggestions (Levenshtein distance = 1)
    /// Applies score penalty: distance-0 = 1.0, distance-1 = 0.707
    pub fn getFuzzySuggestions(self: *SuggestionDictionary, prefix: []const u8, max: usize) !std.ArrayList(Suggestion) {
        var results = try std.ArrayList(Suggestion).initCapacity(self.allocator, 0);
        errdefer {
            for (results.items) |item| {
                self.allocator.free(item.string);
                if (item.payload) |p| self.allocator.free(p);
            }
            results.deinit(self.allocator);
        }

        // Strategy: collect exact matches + all distance-1 variants
        // Distance-1 operations: insertion, deletion, substitution

        // 1. Exact matches (no penalty)
        var exact = try self.getSuggestions(prefix, max * 2);
        defer {
            for (exact.items) |item| {
                self.allocator.free(item.string);
                if (item.payload) |p| self.allocator.free(p);
            }
            exact.deinit(self.allocator);
        }

        for (exact.items) |item| {
            const string_copy = try self.allocator.dupe(u8, item.string);
            errdefer self.allocator.free(string_copy);

            const payload_copy = if (item.payload) |p| try self.allocator.dupe(u8, p) else null;
            errdefer if (payload_copy) |p| self.allocator.free(p);

            try results.append(self.allocator, Suggestion{
                .string = string_copy,
                .score = item.score, // No penalty
                .payload = payload_copy,
            });
        }

        // 2. Distance-1 variants (0.707 penalty)
        var variant_buf = try self.allocator.alloc(u8, prefix.len + 1);
        defer self.allocator.free(variant_buf);

        // Deletion: remove each character
        for (0..prefix.len) |i| {
            const variant_len = prefix.len - 1;
            @memcpy(variant_buf[0..i], prefix[0..i]);
            @memcpy(variant_buf[i..variant_len], prefix[i + 1 ..]);

            var matches = try self.getSuggestions(variant_buf[0..variant_len], max);
            defer {
                for (matches.items) |item| {
                    self.allocator.free(item.string);
                    if (item.payload) |p| self.allocator.free(p);
                }
                matches.deinit(self.allocator);
            }

            for (matches.items) |item| {
                try self.addFuzzyResult(&results, item, 0.707);
            }
        }

        // Substitution: replace each character with all possible chars
        for (0..prefix.len) |i| {
            @memcpy(variant_buf[0..prefix.len], prefix);
            const original_char = variant_buf[i];

            for (0..256) |c| {
                const char: u8 = @intCast(c);
                if (char == original_char) continue;

                variant_buf[i] = char;

                var matches = try self.getSuggestions(variant_buf[0..prefix.len], max);
                defer {
                    for (matches.items) |item| {
                        self.allocator.free(item.string);
                        if (item.payload) |p| self.allocator.free(p);
                    }
                    matches.deinit(self.allocator);
                }

                for (matches.items) |item| {
                    try self.addFuzzyResult(&results, item, 0.707);
                }
            }
        }

        // Insertion: insert each possible character at each position
        if (prefix.len + 1 <= variant_buf.len) {
            for (0..prefix.len + 1) |i| {
                for (0..256) |c| {
                    const char: u8 = @intCast(c);

                    @memcpy(variant_buf[0..i], prefix[0..i]);
                    variant_buf[i] = char;
                    @memcpy(variant_buf[i + 1 .. prefix.len + 1], prefix[i..]);

                    var matches = try self.getSuggestions(variant_buf[0 .. prefix.len + 1], max);
                    defer {
                        for (matches.items) |item| {
                            self.allocator.free(item.string);
                            if (item.payload) |p| self.allocator.free(p);
                        }
                        matches.deinit(self.allocator);
                    }

                    for (matches.items) |item| {
                        try self.addFuzzyResult(&results, item, 0.707);
                    }
                }
            }
        }

        // Remove duplicates (keep highest score)
        var seen = std.StringHashMap(usize).init(self.allocator);
        defer seen.deinit();

        var i: usize = 0;
        while (i < results.items.len) {
            const item = results.items[i];
            if (seen.get(item.string)) |existing_idx| {
                // Keep higher score
                if (item.score > results.items[existing_idx].score) {
                    // Free old entry
                    self.allocator.free(results.items[existing_idx].string);
                    if (results.items[existing_idx].payload) |p| self.allocator.free(p);
                    // Replace
                    results.items[existing_idx] = item;
                }
                // Free current duplicate
                self.allocator.free(item.string);
                if (item.payload) |p| self.allocator.free(p);
                // Remove from list
                _ = results.swapRemove(i);
            } else {
                try seen.put(item.string, i);
                i += 1;
            }
        }

        // Sort by adjusted score descending
        std.mem.sort(Suggestion, results.items, {}, compareByScore);

        // Limit results
        if (results.items.len > max) {
            for (results.items[max..]) |item| {
                self.allocator.free(item.string);
                if (item.payload) |p| self.allocator.free(p);
            }
            results.items.len = max;
        }

        return results;
    }

    /// Helper to add fuzzy result with penalty
    fn addFuzzyResult(self: *SuggestionDictionary, results: *std.ArrayList(Suggestion), item: Suggestion, penalty: f64) !void {
        const string_copy = try self.allocator.dupe(u8, item.string);
        errdefer self.allocator.free(string_copy);

        const payload_copy = if (item.payload) |p| try self.allocator.dupe(u8, p) else null;
        errdefer if (payload_copy) |p| self.allocator.free(p);

        try results.append(self.allocator, Suggestion{
            .string = string_copy,
            .score = item.score * penalty,
            .payload = payload_copy,
        });
    }

    /// Delete suggestion by string
    /// Returns true if found and deleted, false otherwise
    pub fn deleteSuggestion(self: *SuggestionDictionary, string: []const u8) !bool {
        if (string.len == 0) {
            // Empty string edge case
            if (self.root.is_terminal) {
                if (self.root.payload) |payload| {
                    self.allocator.free(payload);
                }
                self.root.is_terminal = false;
                self.root.score = 0.0;
                self.root.payload = null;
                self.count -= 1;
                return true;
            }
            return false;
        }

        // Navigate to parent of terminal node
        var node = &self.root;
        for (string[0 .. string.len - 1]) |char| {
            if (node.getChild(char)) |child| {
                node = child;
            } else {
                return false; // Path doesn't exist
            }
        }

        const last_char = string[string.len - 1];
        if (node.getChild(last_char)) |terminal_node| {
            if (terminal_node.is_terminal) {
                // Mark as non-terminal (don't remove node to preserve children)
                if (terminal_node.payload) |payload| {
                    self.allocator.free(payload);
                }
                terminal_node.is_terminal = false;
                terminal_node.score = 0.0;
                terminal_node.payload = null;
                self.count -= 1;
                return true;
            }
        }

        return false;
    }

    /// Get count of suggestions in dictionary
    pub fn getCount(self: *SuggestionDictionary) u64 {
        return self.count;
    }
};

/// Compare suggestions by score (descending)
fn compareByScore(_: void, a: Suggestion, b: Suggestion) bool {
    return a.score > b.score;
}

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
    /// TAG field inverted index: field_name -> tag_value -> doc_ids
    /// Structure: StringHashMap(field_name -> StringHashMap(tag_value -> ArrayList(doc_id)))
    tag_inverted_index: std.StringHashMap(std.StringHashMap(std.ArrayList([]const u8))),

    allocator: Allocator,

    pub fn init(allocator: Allocator, name: []const u8, index_on: IndexOn) !SearchIndex {
        return SearchIndex{
            .name = try allocator.dupe(u8, name),
            .index_on = index_on,
            .prefix = null,
            .fields = try std.ArrayList(FieldSchema).initCapacity(allocator, 0),
            .created_at = std.time.timestamp(),
            .synonym_groups = std.AutoHashMap(u64, SynonymGroup).init(allocator),
            .tag_inverted_index = std.StringHashMap(std.StringHashMap(std.ArrayList([]const u8))).init(allocator),
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

        // Free tag inverted index
        var tag_it = self.tag_inverted_index.iterator();
        while (tag_it.next()) |field_entry| {
            var field_index = field_entry.value_ptr;
            var value_it = field_index.iterator();
            while (value_it.next()) |value_entry| {
                // Free each ArrayList of doc_ids
                var doc_list = value_entry.value_ptr;
                for (doc_list.items) |doc_id| {
                    self.allocator.free(doc_id);
                }
                doc_list.deinit(self.allocator);
            }
            field_index.deinit();
        }
        self.tag_inverted_index.deinit();
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

    /// Add a tag value to the inverted index for a field
    /// Automatically normalizes the tag (lowercase, trimmed)
    /// Arguments:
    ///   field_name: name of the TAG field
    ///   tag_value: the tag value to add (will be normalized)
    ///   doc_id: document ID that contains this tag
    pub fn addTagValue(self: *SearchIndex, field_name: []const u8, tag_value: []const u8, doc_id: []const u8) !void {
        // Normalize tag value: lowercase and trim whitespace
        const trimmed = std.mem.trim(u8, tag_value, " \t\n\r");
        const normalized = try self.allocator.alloc(u8, trimmed.len);
        defer self.allocator.free(normalized);

        for (trimmed, 0..) |char, i| {
            normalized[i] = std.ascii.toLower(char);
        }

        // Get or create field index map
        const field_index_ptr = try self.tag_inverted_index.getOrPutValue(field_name, std.StringHashMap(std.ArrayList([]const u8)).init(self.allocator));
        var field_index = field_index_ptr.value_ptr;

        // Allocate normalized tag copy for the map key
        const tag_key = try self.allocator.dupe(u8, normalized);
        errdefer self.allocator.free(tag_key);

        // Get or create doc_id list for this tag value
        const doc_list_ptr = try field_index.getOrPutValue(tag_key, try std.ArrayList([]const u8).initCapacity(self.allocator, 0));
        var doc_list = doc_list_ptr.value_ptr;

        // If tag already existed (found_existing), free our allocated tag_key
        // because the HashMap uses its own stored key
        if (doc_list_ptr.found_existing) {
            self.allocator.free(tag_key);
        }

        // Check if doc_id already exists in list (avoid duplicates)
        for (doc_list.items) |existing_doc_id| {
            if (std.mem.eql(u8, existing_doc_id, doc_id)) {
                return; // Already exists, skip (safe now - tag_key freed above if duplicate)
            }
        }

        // Allocate doc_id copy and add to list
        const doc_id_copy = try self.allocator.dupe(u8, doc_id);
        errdefer self.allocator.free(doc_id_copy);

        try doc_list.append(self.allocator, doc_id_copy);
    }

    /// Get all distinct tag values for a field
    /// Returns owned array of strings (caller must free)
    /// Arguments:
    ///   allocator: allocator for returned array
    ///   field_name: name of the TAG field
    pub fn getDistinctTagValues(self: *SearchIndex, allocator: Allocator, field_name: []const u8) ![][]const u8 {
        // Get field index or return empty array
        const field_index = self.tag_inverted_index.get(field_name) orelse {
            return try allocator.alloc([]const u8, 0);
        };

        // Collect all distinct tag values
        var result = try std.ArrayList([]const u8).initCapacity(allocator, field_index.count());
        errdefer {
            for (result.items) |tag| allocator.free(tag);
            result.deinit(allocator);
        }

        var it = field_index.keyIterator();
        while (it.next()) |tag_key| {
            const tag_copy = try allocator.dupe(u8, tag_key.*);
            errdefer allocator.free(tag_copy);
            try result.append(allocator, tag_copy);
        }

        return try result.toOwnedSlice(allocator);
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

/// Search engine runtime configuration
pub const SearchConfig = struct {
    /// Query timeout in milliseconds (default: 500ms)
    timeout: u64,
    /// Action when query times out: "fail" or "return" (default: "return")
    on_timeout: []const u8,
    /// Maximum wildcard expansions (default: 200)
    max_expansions: u64,
    /// Maximum prefix expansions (default: 200)
    max_prefix_expansions: u64,

    allocator: Allocator,

    /// Initialize with default values
    pub fn init(allocator: Allocator) !SearchConfig {
        return SearchConfig{
            .timeout = 500,
            .on_timeout = try allocator.dupe(u8, "return"),
            .max_expansions = 200,
            .max_prefix_expansions = 200,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SearchConfig) void {
        self.allocator.free(self.on_timeout);
    }

    /// Get configuration option value as string
    /// Caller owns the returned string
    pub fn get(self: *const SearchConfig, allocator: Allocator, option: []const u8) ![]const u8 {
        const lower_option = try allocator.alloc(u8, option.len);
        defer allocator.free(lower_option);
        for (option, 0..) |char, i| {
            lower_option[i] = std.ascii.toLower(char);
        }

        if (std.mem.eql(u8, lower_option, "timeout")) {
            return std.fmt.allocPrint(allocator, "{d}", .{self.timeout});
        } else if (std.mem.eql(u8, lower_option, "on_timeout")) {
            return allocator.dupe(u8, self.on_timeout);
        } else if (std.mem.eql(u8, lower_option, "maxexpansions")) {
            return std.fmt.allocPrint(allocator, "{d}", .{self.max_expansions});
        } else if (std.mem.eql(u8, lower_option, "maxprefixexpansions")) {
            return std.fmt.allocPrint(allocator, "{d}", .{self.max_prefix_expansions});
        }
        return error.UnknownConfigOption;
    }

    /// Set configuration option value
    /// Returns error if option is unknown or value is invalid
    pub fn set(self: *SearchConfig, option: []const u8, value: []const u8) !void {
        const lower_option = try self.allocator.alloc(u8, option.len);
        defer self.allocator.free(lower_option);
        for (option, 0..) |char, i| {
            lower_option[i] = std.ascii.toLower(char);
        }

        if (std.mem.eql(u8, lower_option, "timeout")) {
            self.timeout = try std.fmt.parseInt(u64, value, 10);
        } else if (std.mem.eql(u8, lower_option, "on_timeout")) {
            const lower_value = try self.allocator.alloc(u8, value.len);
            defer self.allocator.free(lower_value);
            for (value, 0..) |char, i| {
                lower_value[i] = std.ascii.toLower(char);
            }
            if (!std.mem.eql(u8, lower_value, "fail") and !std.mem.eql(u8, lower_value, "return")) {
                return error.InvalidConfigValue;
            }
            self.allocator.free(self.on_timeout);
            self.on_timeout = try self.allocator.dupe(u8, lower_value);
        } else if (std.mem.eql(u8, lower_option, "maxexpansions")) {
            self.max_expansions = try std.fmt.parseInt(u64, value, 10);
        } else if (std.mem.eql(u8, lower_option, "maxprefixexpansions")) {
            self.max_prefix_expansions = try std.fmt.parseInt(u64, value, 10);
        } else {
            return error.UnknownConfigOption;
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
    /// Map: key -> SuggestionDictionary (for auto-complete)
    suggestion_dictionaries: std.StringHashMap(SuggestionDictionary),
    /// Runtime configuration
    config: SearchConfig,
    allocator: Allocator,

    pub fn init(allocator: Allocator) !SearchStore {
        const config = try SearchConfig.init(allocator);
        errdefer {
            var cfg = config;
            cfg.deinit();
        }

        return SearchStore{
            .indices = std.StringHashMap(SearchIndex).init(allocator),
            .cursors = std.AutoHashMap(u64, SearchCursor).init(allocator),
            .next_cursor_id = 1,
            .cursor_max_idle = 300, // 300 seconds = 5 minutes
            .aliases = std.StringHashMap([]const u8).init(allocator),
            .dictionaries = std.StringHashMap(Dictionary).init(allocator),
            .suggestion_dictionaries = std.StringHashMap(SuggestionDictionary).init(allocator),
            .config = config,
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

        // Free suggestion dictionaries
        var sug_it = self.suggestion_dictionaries.iterator();
        while (sug_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*); // Free key
            var dict = entry.value_ptr;
            dict.deinit();
        }
        self.suggestion_dictionaries.deinit();

        // Free config
        self.config.deinit();
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

// ============================================================================
// FT.SUGADD/SUGDEL/SUGGET/SUGLEN Storage Layer Tests (Iteration 198)
// ============================================================================
// These tests will fail initially since SuggestionNode, SuggestionDictionary,
// and Suggestion types are not yet implemented. This follows TDD principles:
// write failing tests first, then implement to make them pass.
// ============================================================================

test "SuggestionNode: init creates non-terminal node" {
    const allocator = std.testing.allocator;
    var node = try SuggestionNode.init(allocator);
    defer node.deinit();

    try std.testing.expect(!node.is_terminal);
    try std.testing.expectEqual(@as(f64, 0.0), node.score);
    try std.testing.expect(node.payload == null);
    try std.testing.expectEqual(@as(usize, 0), node.children.count());
}

test "SuggestionNode: markTerminal sets terminal flag and score" {
    const allocator = std.testing.allocator;
    var node = try SuggestionNode.init(allocator);
    defer node.deinit();

    try node.markTerminal(100.5, null);

    try std.testing.expect(node.is_terminal);
    try std.testing.expectEqual(@as(f64, 100.5), node.score);
    try std.testing.expect(node.payload == null);
}

test "SuggestionNode: markTerminal with payload stores payload string" {
    const allocator = std.testing.allocator;
    var node = try SuggestionNode.init(allocator);
    defer node.deinit();

    try node.markTerminal(200.0, "metadata");

    try std.testing.expect(node.is_terminal);
    try std.testing.expectEqual(@as(f64, 200.0), node.score);
    try std.testing.expect(node.payload != null);
    try std.testing.expectEqualStrings("metadata", node.payload.?);
}

test "SuggestionNode: addChild creates new child for character" {
    const allocator = std.testing.allocator;
    var parent = try SuggestionNode.init(allocator);
    defer parent.deinit();

    const child = try parent.addChild('a');

    try std.testing.expectEqual(@as(usize, 1), parent.children.count());
    try std.testing.expect(parent.children.contains('a'));
    try std.testing.expect(!child.is_terminal);
}

test "SuggestionNode: addChild returns existing child if present" {
    const allocator = std.testing.allocator;
    var parent = try SuggestionNode.init(allocator);
    defer parent.deinit();

    const child1 = try parent.addChild('b');
    try child1.markTerminal(50.0, null);

    const child2 = try parent.addChild('b');

    try std.testing.expectEqual(@as(usize, 1), parent.children.count());
    try std.testing.expect(child2.is_terminal); // Should return the same node
    try std.testing.expectEqual(@as(f64, 50.0), child2.score);
}

test "SuggestionNode: getChild returns child if exists" {
    const allocator = std.testing.allocator;
    var parent = try SuggestionNode.init(allocator);
    defer parent.deinit();

    _ = try parent.addChild('c');
    const child = parent.getChild('c');

    try std.testing.expect(child != null);
}

test "SuggestionNode: getChild returns null if child does not exist" {
    const allocator = std.testing.allocator;
    var parent = try SuggestionNode.init(allocator);
    defer parent.deinit();

    const child = parent.getChild('z');

    try std.testing.expect(child == null);
}

test "SuggestionNode: deinit cleans up children recursively" {
    const allocator = std.testing.allocator;
    var root = try SuggestionNode.init(allocator);
    defer root.deinit();

    // Build a small trie: h -> e -> l -> l -> o
    const h = try root.addChild('h');
    const e = try h.addChild('e');
    const l1 = try e.addChild('l');
    const l2 = try l1.addChild('l');
    _ = try l2.addChild('o');

    // deinit() should clean up all nodes without leaking memory
    // This test verifies no leaks via testing.allocator
}

test "SuggestionDictionary: init creates empty dictionary" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    try std.testing.expectEqual(@as(u64, 0), dict.count);
}

test "SuggestionDictionary: addSuggestion inserts new suggestion" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    const count = try dict.addSuggestion("hello", 100.0, false, null);

    try std.testing.expectEqual(@as(u64, 1), count);
    try std.testing.expectEqual(@as(u64, 1), dict.count);
}

test "SuggestionDictionary: addSuggestion with multiple unique strings increments count" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("hello", 100.0, false, null);
    _ = try dict.addSuggestion("world", 90.0, false, null);
    const count = try dict.addSuggestion("test", 80.0, false, null);

    try std.testing.expectEqual(@as(u64, 3), count);
    try std.testing.expectEqual(@as(u64, 3), dict.count);
}

test "SuggestionDictionary: addSuggestion with duplicate replaces score (no INCR)" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("hello", 100.0, false, null);
    const count = try dict.addSuggestion("hello", 200.0, false, null);

    // Count should not increase for duplicate
    try std.testing.expectEqual(@as(u64, 1), count);
    try std.testing.expectEqual(@as(u64, 1), dict.count);

    // Verify score was replaced
    const suggestions = try dict.getSuggestions("hello", 1, false);
    defer allocator.free(suggestions);
    try std.testing.expectEqual(@as(usize, 1), suggestions.len);
    try std.testing.expectEqual(@as(f64, 200.0), suggestions[0].score);
}

test "SuggestionDictionary: addSuggestion with INCR increments score" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("hello", 100.0, false, null);
    const count = try dict.addSuggestion("hello", 50.0, true, null); // INCR=true

    // Count should not increase
    try std.testing.expectEqual(@as(u64, 1), count);
    try std.testing.expectEqual(@as(u64, 1), dict.count);

    // Verify score was incremented (100 + 50 = 150)
    const suggestions = try dict.getSuggestions("hello", 1, false);
    defer allocator.free(suggestions);
    try std.testing.expectEqual(@as(usize, 1), suggestions.len);
    try std.testing.expectEqual(@as(f64, 150.0), suggestions[0].score);
}

test "SuggestionDictionary: addSuggestion with payload stores payload" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("laptop", 300.0, false, "electronics");

    const suggestions = try dict.getSuggestions("laptop", 1, false);
    defer allocator.free(suggestions);
    try std.testing.expectEqual(@as(usize, 1), suggestions.len);
    try std.testing.expect(suggestions[0].payload != null);
    try std.testing.expectEqualStrings("electronics", suggestions[0].payload.?);
}

test "SuggestionDictionary: addSuggestion with empty string" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    const count = try dict.addSuggestion("", 100.0, false, null);

    try std.testing.expectEqual(@as(u64, 1), count);
    try std.testing.expectEqual(@as(u64, 1), dict.count);

    // Empty string should be retrievable
    const suggestions = try dict.getSuggestions("", 1, false);
    defer allocator.free(suggestions);
    try std.testing.expectEqual(@as(usize, 1), suggestions.len);
    try std.testing.expectEqualStrings("", suggestions[0].string);
}

test "SuggestionDictionary: addSuggestion with Unicode strings" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("café", 100.0, false, null);
    _ = try dict.addSuggestion("你好", 90.0, false, null);
    _ = try dict.addSuggestion("🚀rocket", 80.0, false, null);

    try std.testing.expectEqual(@as(u64, 3), dict.count);

    // Verify Unicode retrieval works
    const cafe = try dict.getSuggestions("café", 1, false);
    defer allocator.free(cafe);
    try std.testing.expectEqual(@as(usize, 1), cafe.len);
    try std.testing.expectEqualStrings("café", cafe[0].string);
}

test "SuggestionDictionary: addSuggestion with negative score" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("negative", -50.0, false, null);

    const suggestions = try dict.getSuggestions("negative", 1, false);
    defer allocator.free(suggestions);
    try std.testing.expectEqual(@as(usize, 1), suggestions.len);
    try std.testing.expectEqual(@as(f64, -50.0), suggestions[0].score);
}

test "SuggestionDictionary: getSuggestions returns exact matches" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("hello", 100.0, false, null);
    _ = try dict.addSuggestion("help", 90.0, false, null);
    _ = try dict.addSuggestion("hero", 80.0, false, null);

    const suggestions = try dict.getSuggestions("hel", 10, false);
    defer allocator.free(suggestions);

    // Should match "hello" and "help" but not "hero"
    try std.testing.expectEqual(@as(usize, 2), suggestions.len);
}

test "SuggestionDictionary: getSuggestions returns results sorted by score descending" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("apple", 50.0, false, null);
    _ = try dict.addSuggestion("application", 200.0, false, null);
    _ = try dict.addSuggestion("apply", 100.0, false, null);

    const suggestions = try dict.getSuggestions("app", 10, false);
    defer allocator.free(suggestions);

    try std.testing.expectEqual(@as(usize, 3), suggestions.len);
    // Verify descending order: 200 > 100 > 50
    try std.testing.expectEqual(@as(f64, 200.0), suggestions[0].score);
    try std.testing.expectEqualStrings("application", suggestions[0].string);
    try std.testing.expectEqual(@as(f64, 100.0), suggestions[1].score);
    try std.testing.expectEqualStrings("apply", suggestions[1].string);
    try std.testing.expectEqual(@as(f64, 50.0), suggestions[2].score);
    try std.testing.expectEqualStrings("apple", suggestions[2].string);
}

test "SuggestionDictionary: getSuggestions with MAX limits results" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("test1", 100.0, false, null);
    _ = try dict.addSuggestion("test2", 90.0, false, null);
    _ = try dict.addSuggestion("test3", 80.0, false, null);
    _ = try dict.addSuggestion("test4", 70.0, false, null);
    _ = try dict.addSuggestion("test5", 60.0, false, null);

    const suggestions = try dict.getSuggestions("test", 3, false);
    defer allocator.free(suggestions);

    try std.testing.expectEqual(@as(usize, 3), suggestions.len);
    try std.testing.expectEqualStrings("test1", suggestions[0].string);
    try std.testing.expectEqualStrings("test2", suggestions[1].string);
    try std.testing.expectEqualStrings("test3", suggestions[2].string);
}

test "SuggestionDictionary: getSuggestions with empty prefix returns all suggestions" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("alpha", 100.0, false, null);
    _ = try dict.addSuggestion("beta", 90.0, false, null);
    _ = try dict.addSuggestion("gamma", 80.0, false, null);

    const suggestions = try dict.getSuggestions("", 10, false);
    defer allocator.free(suggestions);

    // Empty prefix matches everything
    try std.testing.expectEqual(@as(usize, 3), suggestions.len);
    // Verify sorted by score
    try std.testing.expectEqual(@as(f64, 100.0), suggestions[0].score);
    try std.testing.expectEqual(@as(f64, 90.0), suggestions[1].score);
    try std.testing.expectEqual(@as(f64, 80.0), suggestions[2].score);
}

test "SuggestionDictionary: getSuggestions with no matches returns empty array" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("hello", 100.0, false, null);

    const suggestions = try dict.getSuggestions("xyz", 10, false);
    defer allocator.free(suggestions);

    try std.testing.expectEqual(@as(usize, 0), suggestions.len);
}

test "SuggestionDictionary: getSuggestions with MAX larger than available returns all" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("one", 100.0, false, null);
    _ = try dict.addSuggestion("two", 90.0, false, null);

    const suggestions = try dict.getSuggestions("", 1000, false);
    defer allocator.free(suggestions);

    try std.testing.expectEqual(@as(usize, 2), suggestions.len);
}

test "SuggestionDictionary: getFuzzySuggestions matches with insertion (distance 1)" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("hello", 100.0, false, null);

    // "helo" -> "hello" (missing 'l', insertion needed)
    const suggestions = try dict.getSuggestions("helo", 10, true); // fuzzy=true
    defer allocator.free(suggestions);

    try std.testing.expectEqual(@as(usize, 1), suggestions.len);
    try std.testing.expectEqualStrings("hello", suggestions[0].string);
}

test "SuggestionDictionary: getFuzzySuggestions matches with deletion (distance 1)" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("hello", 100.0, false, null);

    // "helllo" -> "hello" (extra 'l', deletion needed)
    const suggestions = try dict.getSuggestions("helllo", 10, true);
    defer allocator.free(suggestions);

    try std.testing.expectEqual(@as(usize, 1), suggestions.len);
    try std.testing.expectEqualStrings("hello", suggestions[0].string);
}

test "SuggestionDictionary: getFuzzySuggestions matches with substitution (distance 1)" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("hello", 100.0, false, null);

    // "hallo" -> "hello" (substitution 'a' -> 'e')
    const suggestions = try dict.getSuggestions("hallo", 10, true);
    defer allocator.free(suggestions);

    try std.testing.expectEqual(@as(usize, 1), suggestions.len);
    try std.testing.expectEqualStrings("hello", suggestions[0].string);
}

test "SuggestionDictionary: getFuzzySuggestions applies score penalty" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("hello", 100.0, false, null);
    _ = try dict.addSuggestion("help", 90.0, false, null);

    // "helo" matches "hello" with distance 1
    const suggestions = try dict.getSuggestions("helo", 10, true);
    defer allocator.free(suggestions);

    try std.testing.expectEqual(@as(usize, 1), suggestions.len);
    // Score should be penalized: 100 * 0.707... ≈ 70.71
    try std.testing.expect(suggestions[0].score < 100.0);
    try std.testing.expect(suggestions[0].score > 70.0);
    try std.testing.expect(suggestions[0].score < 72.0);
}

test "SuggestionDictionary: getFuzzySuggestions returns multiple matches sorted by score" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("hello", 100.0, false, null);
    _ = try dict.addSuggestion("help", 90.0, false, null);
    _ = try dict.addSuggestion("hell", 80.0, false, null);

    const suggestions = try dict.getSuggestions("helo", 10, true);
    defer allocator.free(suggestions);

    // Should match "hello" and "hell" (both distance 1 from "helo")
    try std.testing.expect(suggestions.len >= 2);
    // Verify sorted by adjusted score (descending)
    for (0..suggestions.len - 1) |i| {
        try std.testing.expect(suggestions[i].score >= suggestions[i + 1].score);
    }
}

test "SuggestionDictionary: getFuzzySuggestions does not match distance > 1" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("hello", 100.0, false, null);

    // "hlo" -> "hello" (distance 2: missing 'e' and 'l')
    const suggestions = try dict.getSuggestions("hlo", 10, true);
    defer allocator.free(suggestions);

    try std.testing.expectEqual(@as(usize, 0), suggestions.len);
}

test "SuggestionDictionary: deleteSuggestion removes existing suggestion" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("hello", 100.0, false, null);

    const deleted = try dict.deleteSuggestion("hello");

    try std.testing.expect(deleted);
    try std.testing.expectEqual(@as(u64, 0), dict.count);
}

test "SuggestionDictionary: deleteSuggestion returns false for non-existent suggestion" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    const deleted = try dict.deleteSuggestion("nonexistent");

    try std.testing.expect(!deleted);
    try std.testing.expectEqual(@as(u64, 0), dict.count);
}

test "SuggestionDictionary: deleteSuggestion is idempotent" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("test", 100.0, false, null);

    const deleted1 = try dict.deleteSuggestion("test");
    const deleted2 = try dict.deleteSuggestion("test");

    try std.testing.expect(deleted1);
    try std.testing.expect(!deleted2);
    try std.testing.expectEqual(@as(u64, 0), dict.count);
}

test "SuggestionDictionary: deleteSuggestion decrements count" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("one", 100.0, false, null);
    _ = try dict.addSuggestion("two", 90.0, false, null);
    _ = try dict.addSuggestion("three", 80.0, false, null);

    _ = try dict.deleteSuggestion("two");

    try std.testing.expectEqual(@as(u64, 2), dict.count);

    // Verify remaining suggestions
    const suggestions = try dict.getSuggestions("", 10, false);
    defer allocator.free(suggestions);
    try std.testing.expectEqual(@as(usize, 2), suggestions.len);
}

test "SuggestionDictionary: deleteSuggestion with shared prefix preserves other suggestions" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("hello", 100.0, false, null);
    _ = try dict.addSuggestion("help", 90.0, false, null);
    _ = try dict.addSuggestion("hero", 80.0, false, null);

    _ = try dict.deleteSuggestion("help");

    try std.testing.expectEqual(@as(u64, 2), dict.count);

    // Verify "hello" still exists
    const hello_suggestions = try dict.getSuggestions("hello", 1, false);
    defer allocator.free(hello_suggestions);
    try std.testing.expectEqual(@as(usize, 1), hello_suggestions.len);
    try std.testing.expectEqualStrings("hello", hello_suggestions[0].string);

    // Verify "help" is gone
    const help_suggestions = try dict.getSuggestions("help", 1, false);
    defer allocator.free(help_suggestions);
    try std.testing.expectEqual(@as(usize, 0), help_suggestions.len);
}

test "SuggestionDictionary: deleteSuggestion cleans up payload memory" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("laptop", 100.0, false, "electronics");

    const deleted = try dict.deleteSuggestion("laptop");

    try std.testing.expect(deleted);
    // Memory leak check via testing.allocator verifies payload was freed
}

test "SuggestionDictionary: getCount returns 0 for empty dictionary" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    const count = dict.getCount();

    try std.testing.expectEqual(@as(u64, 0), count);
}

test "SuggestionDictionary: getCount reflects additions" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("one", 100.0, false, null);
    try std.testing.expectEqual(@as(u64, 1), dict.getCount());

    _ = try dict.addSuggestion("two", 90.0, false, null);
    try std.testing.expectEqual(@as(u64, 2), dict.getCount());

    _ = try dict.addSuggestion("three", 80.0, false, null);
    try std.testing.expectEqual(@as(u64, 3), dict.getCount());
}

test "SuggestionDictionary: getCount unchanged by score updates" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("test", 100.0, false, null);
    const count1 = dict.getCount();

    _ = try dict.addSuggestion("test", 200.0, false, null); // Replace score
    const count2 = dict.getCount();

    _ = try dict.addSuggestion("test", 50.0, true, null); // INCR score
    const count3 = dict.getCount();

    try std.testing.expectEqual(count1, count2);
    try std.testing.expectEqual(count2, count3);
    try std.testing.expectEqual(@as(u64, 1), count3);
}

test "SuggestionDictionary: getCount reflects deletions" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("alpha", 100.0, false, null);
    _ = try dict.addSuggestion("beta", 90.0, false, null);
    _ = try dict.addSuggestion("gamma", 80.0, false, null);

    _ = try dict.deleteSuggestion("beta");
    try std.testing.expectEqual(@as(u64, 2), dict.getCount());

    _ = try dict.deleteSuggestion("alpha");
    try std.testing.expectEqual(@as(u64, 1), dict.getCount());

    _ = try dict.deleteSuggestion("gamma");
    try std.testing.expectEqual(@as(u64, 0), dict.getCount());
}

test "SuggestionDictionary: handles very long strings" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    // Create a 1000-character string
    const long_string = try allocator.alloc(u8, 1000);
    defer allocator.free(long_string);
    @memset(long_string, 'a');

    _ = try dict.addSuggestion(long_string, 100.0, false, null);

    const suggestions = try dict.getSuggestions(long_string[0..500], 1, false);
    defer allocator.free(suggestions);

    try std.testing.expectEqual(@as(usize, 1), suggestions.len);
}

test "SuggestionDictionary: handles many suggestions with shared prefix" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    // Add 100 suggestions all starting with "test"
    var buf: [20]u8 = undefined;
    for (0..100) |i| {
        const suggestion = try std.fmt.bufPrint(&buf, "test{d}", .{i});
        _ = try dict.addSuggestion(suggestion, @as(f64, @floatFromInt(100 - i)), false, null);
    }

    try std.testing.expectEqual(@as(u64, 100), dict.count);

    const suggestions = try dict.getSuggestions("test", 50, false);
    defer allocator.free(suggestions);

    try std.testing.expectEqual(@as(usize, 50), suggestions.len);
    // Verify sorted by score
    try std.testing.expectEqual(@as(f64, 100.0), suggestions[0].score);
}

test "SuggestionDictionary: handles zero score" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("zero", 0.0, false, null);

    const suggestions = try dict.getSuggestions("zero", 1, false);
    defer allocator.free(suggestions);

    try std.testing.expectEqual(@as(usize, 1), suggestions.len);
    try std.testing.expectEqual(@as(f64, 0.0), suggestions[0].score);
}

test "SuggestionDictionary: handles very large scores" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    const large_score = 1.0e100;
    _ = try dict.addSuggestion("large", large_score, false, null);

    const suggestions = try dict.getSuggestions("large", 1, false);
    defer allocator.free(suggestions);

    try std.testing.expectEqual(@as(usize, 1), suggestions.len);
    try std.testing.expectEqual(large_score, suggestions[0].score);
}

test "SuggestionDictionary: preserves score precision with INCR" {
    const allocator = std.testing.allocator;
    var dict = try SuggestionDictionary.init(allocator);
    defer dict.deinit();

    _ = try dict.addSuggestion("precise", 0.1, false, null);
    _ = try dict.addSuggestion("precise", 0.2, true, null);
    _ = try dict.addSuggestion("precise", 0.3, true, null);

    const suggestions = try dict.getSuggestions("precise", 1, false);
    defer allocator.free(suggestions);

    try std.testing.expectEqual(@as(usize, 1), suggestions.len);
    // 0.1 + 0.2 + 0.3 = 0.6
    try std.testing.expectApproxEqAbs(@as(f64, 0.6), suggestions[0].score, 0.0001);
}

// ============================================================================
// TAG FIELD INDEX TESTS — FT.TAGVALS Command
// ============================================================================

test "TagIndex: addTagValue stores tag in inverted index" {
    const allocator = std.testing.allocator;
    var index = try SearchIndex.init(allocator, "idx", .hash);
    defer index.deinit();

    // Test that addTagValue method exists and properly stores tags
    // This will initially fail because method doesn't exist yet
    try index.addTagValue("category", "electronics", "doc1");

    // Retrieve tags and verify
    const tags = try index.getDistinctTagValues(allocator, "category");
    defer {
        for (tags) |tag| allocator.free(tag);
        allocator.free(tags);
    }

    try std.testing.expectEqual(@as(usize, 1), tags.len);
    try std.testing.expect(std.mem.eql(u8, tags[0], "electronics"));
}

test "TagIndex: multiple tags from single field" {
    const allocator = std.testing.allocator;
    var index = try SearchIndex.init(allocator, "idx", .hash);
    defer index.deinit();

    try index.addTagValue("tags", "apple", "doc1");
    try index.addTagValue("tags", "banana", "doc1");
    try index.addTagValue("tags", "cherry", "doc1");

    const tags = try index.getDistinctTagValues(allocator, "tags");
    defer {
        for (tags) |tag| allocator.free(tag);
        allocator.free(tags);
    }

    try std.testing.expectEqual(@as(usize, 3), tags.len);
}

test "TagIndex: deduplication across documents" {
    const allocator = std.testing.allocator;
    var index = try SearchIndex.init(allocator, "idx", .hash);
    defer index.deinit();

    // Same tag in multiple documents
    try index.addTagValue("category", "electronics", "doc1");
    try index.addTagValue("category", "electronics", "doc2");
    try index.addTagValue("category", "electronics", "doc3");

    const tags = try index.getDistinctTagValues(allocator, "category");
    defer {
        for (tags) |tag| allocator.free(tag);
        allocator.free(tags);
    }

    // Should only have one distinct value
    try std.testing.expectEqual(@as(usize, 1), tags.len);
    try std.testing.expect(std.mem.eql(u8, tags[0], "electronics"));
}

test "TagIndex: case normalization (lowercase)" {
    const allocator = std.testing.allocator;
    var index = try SearchIndex.init(allocator, "idx", .hash);
    defer index.deinit();

    // Add tags with mixed case
    try index.addTagValue("brand", "Apple", "doc1");
    try index.addTagValue("brand", "SAMSUNG", "doc2");
    try index.addTagValue("brand", "Sony", "doc3");

    const tags = try index.getDistinctTagValues(allocator, "brand");
    defer {
        for (tags) |tag| allocator.free(tag);
        allocator.free(tags);
    }

    try std.testing.expectEqual(@as(usize, 3), tags.len);
    // All should be lowercase
    for (tags) |tag| {
        for (tag) |char| {
            try std.testing.expect(char == std.ascii.toLower(char));
        }
    }
}

test "TagIndex: whitespace trimming" {
    const allocator = std.testing.allocator;
    var index = try SearchIndex.init(allocator, "idx", .hash);
    defer index.deinit();

    try index.addTagValue("tags", "  electronics  ", "doc1");
    try index.addTagValue("tags", "\tgaming\n", "doc2");

    const tags = try index.getDistinctTagValues(allocator, "tags");
    defer {
        for (tags) |tag| allocator.free(tag);
        allocator.free(tags);
    }

    try std.testing.expectEqual(@as(usize, 2), tags.len);
    // Should be trimmed
    for (tags) |tag| {
        try std.testing.expect(tag.len == 0 or tag[0] != ' ');
        try std.testing.expect(tag.len == 0 or tag[tag.len - 1] != ' ');
    }
}

test "TagIndex: empty field returns empty array" {
    const allocator = std.testing.allocator;
    var index = try SearchIndex.init(allocator, "idx", .hash);
    defer index.deinit();

    // Don't add any tags to "category" field

    const tags = try index.getDistinctTagValues(allocator, "category");
    defer {
        for (tags) |tag| allocator.free(tag);
        allocator.free(tags);
    }

    try std.testing.expectEqual(@as(usize, 0), tags.len);
}

test "TagIndex: nonexistent field returns empty array" {
    const allocator = std.testing.allocator;
    var index = try SearchIndex.init(allocator, "idx", .hash);
    defer index.deinit();

    try index.addTagValue("existing_field", "tag1", "doc1");

    // Query different field
    const tags = try index.getDistinctTagValues(allocator, "nonexistent_field");
    defer {
        for (tags) |tag| allocator.free(tag);
        allocator.free(tags);
    }

    try std.testing.expectEqual(@as(usize, 0), tags.len);
}

test "TagIndex: complex tag values" {
    const allocator = std.testing.allocator;
    var index = try SearchIndex.init(allocator, "idx", .hash);
    defer index.deinit();

    try index.addTagValue("tags", "c++", "doc1");
    try index.addTagValue("tags", "node.js", "doc2");
    try index.addTagValue("tags", "rust-lang", "doc3");
    try index.addTagValue("tags", "python@3.9", "doc4");

    const tags = try index.getDistinctTagValues(allocator, "tags");
    defer {
        for (tags) |tag| allocator.free(tag);
        allocator.free(tags);
    }

    try std.testing.expectEqual(@as(usize, 4), tags.len);
}

test "TagIndex: many documents same tag" {
    const allocator = std.testing.allocator;
    var index = try SearchIndex.init(allocator, "idx", .hash);
    defer index.deinit();

    // Add same tag to 100 documents
    for (0..100) |i| {
        const doc_id = try std.fmt.allocPrint(allocator, "doc{}", .{i});
        defer allocator.free(doc_id);
        try index.addTagValue("category", "popular", doc_id);
    }

    const tags = try index.getDistinctTagValues(allocator, "category");
    defer {
        for (tags) |tag| allocator.free(tag);
        allocator.free(tags);
    }

    try std.testing.expectEqual(@as(usize, 1), tags.len);
}

test "TagIndex: multiple fields independent" {
    const allocator = std.testing.allocator;
    var index = try SearchIndex.init(allocator, "idx", .hash);
    defer index.deinit();

    try index.addTagValue("category", "electronics", "doc1");
    try index.addTagValue("category", "gaming", "doc1");
    try index.addTagValue("brand", "apple", "doc1");
    try index.addTagValue("brand", "samsung", "doc1");

    const cat_tags = try index.getDistinctTagValues(allocator, "category");
    defer {
        for (cat_tags) |tag| allocator.free(tag);
        allocator.free(cat_tags);
    }

    const brand_tags = try index.getDistinctTagValues(allocator, "brand");
    defer {
        for (brand_tags) |tag| allocator.free(tag);
        allocator.free(brand_tags);
    }

    try std.testing.expectEqual(@as(usize, 2), cat_tags.len);
    try std.testing.expectEqual(@as(usize, 2), brand_tags.len);
}

test "TagIndex: unicode tags" {
    const allocator = std.testing.allocator;
    var index = try SearchIndex.init(allocator, "idx", .hash);
    defer index.deinit();

    try index.addTagValue("tags", "日本語", "doc1");
    try index.addTagValue("tags", "français", "doc2");
    try index.addTagValue("tags", "español", "doc3");

    const tags = try index.getDistinctTagValues(allocator, "tags");
    defer {
        for (tags) |tag| allocator.free(tag);
        allocator.free(tags);
    }

    try std.testing.expectEqual(@as(usize, 3), tags.len);
}

test "TagIndex: very long tag values" {
    const allocator = std.testing.allocator;
    var index = try SearchIndex.init(allocator, "idx", .hash);
    defer index.deinit();

    // Create a long tag (1000 chars)
    const long_tag = try allocator.alloc(u8, 1000);
    defer allocator.free(long_tag);
    for (long_tag, 0..) |*c, i| {
        c.* = @as(u8, @intCast('a' + (i % 26)));
    }

    try index.addTagValue("tags", long_tag, "doc1");

    const tags = try index.getDistinctTagValues(allocator, "tags");
    defer {
        for (tags) |tag| allocator.free(tag);
        allocator.free(tags);
    }

    try std.testing.expectEqual(@as(usize, 1), tags.len);
    try std.testing.expectEqual(@as(usize, 1000), tags[0].len);
}

test "TagIndex: many distinct tags" {
    const allocator = std.testing.allocator;
    var index = try SearchIndex.init(allocator, "idx", .hash);
    defer index.deinit();

    // Add 100 distinct tags
    for (0..100) |i| {
        const tag = try std.fmt.allocPrint(allocator, "tag{}", .{i});
        defer allocator.free(tag);
        try index.addTagValue("tags", tag, "doc1");
    }

    const tags = try index.getDistinctTagValues(allocator, "tags");
    defer {
        for (tags) |tag| allocator.free(tag);
        allocator.free(tags);
    }

    try std.testing.expectEqual(@as(usize, 100), tags.len);
}
