# FT.SPELLCHECK Command Specification — Iteration 193

## Overview

FT.SPELLCHECK performs spell checking on a search query by comparing query terms against indexed terms in a Redis Search index. It returns spelling suggestions for misspelled terms using Levenshtein distance-based matching with configurable custom dictionaries.

**Implementation Phase**: Phase 13.10 — Search Engine
**Redis Version**: Available since Redis Search 1.4.0
**ACL Category**: `@search`
**Command Flags**: `readonly`
**Time Complexity**: O(1) — stub implementation (future: O(N*M) where N=query terms, M=indexed terms)

---

## Command Syntax

```
FT.SPELLCHECK index query
  [DISTANCE distance]
  [TERMS INCLUDE | EXCLUDE dictionary [terms ...]]
  [DIALECT dialect]
```

### Required Arguments

| Argument | Type | Description |
|----------|------|-------------|
| `index` | string | Name of the search index to check spelling against |
| `query` | string | Search query to perform spelling correction on |

### Optional Arguments

| Argument | Type | Description | Default | Valid Range |
|----------|------|-------------|---------|-------------|
| `DISTANCE` | integer | Maximum Levenshtein distance for suggestions | 1 | 1-4 |
| `TERMS INCLUDE/EXCLUDE` | flag + dict | Include or exclude custom dictionary terms | N/A | Multiple allowed |
| `DIALECT` | integer | Query dialect version | Module default | 1-3 |

#### DISTANCE

- Controls the maximum Levenshtein distance between query terms and suggestions
- Higher values allow more character differences (more lenient matching)
- Range: 1 (exact matches only) to 4 (very lenient)
- Default: 1

#### TERMS INCLUDE/EXCLUDE

- **INCLUDE**: Include terms from a custom dictionary when generating suggestions
- **EXCLUDE**: Exclude terms from a custom dictionary from suggestions
- Multiple TERMS clauses can be specified
- Each TERMS clause can specify multiple terms after the dictionary name
- Syntax: `TERMS INCLUDE <dict_name> [term1 term2 ...]`
- Related commands: FT.DICTADD, FT.DICTDEL, FT.DICTDUMP

#### DIALECT

- Selects query parsing dialect version
- Available since Redis Search 2.4.3
- Default: Module-level default (set via FT.CONFIG SET or module loading)
- Valid values: 1, 2, 3

---

## Return Value Format

Returns an **array reply** where each element represents a misspelled term from the query:

### Structure

```
1) 1) "TERM"                   # Constant marker
   2) "misspelled_term"        # Original term from query
   3) 1) 1) "score"            # Score (string representation)
         2) "suggestion1"      # First suggestion
      2) 1) "score"
         2) "suggestion2"
      ...
2) 1) "TERM"
   2) "another_misspelled"
   3) ...
```

### Format Details

- **Outer array**: One element per misspelled term (ordered by appearance in query)
- **Per-term structure**: 3-element array
  1. **Marker**: Constant string `"TERM"`
  2. **Original term**: The misspelled term from the query
  3. **Suggestions array**: Array of score/suggestion pairs
- **Suggestions ordering**: Descending by score (highest score first)

### Score Calculation

```
score = (documents containing suggestion) / (total documents in index)
```

- Score represents the frequency/popularity of the suggestion term
- Range: 0.0 (term in no documents) to 1.0 (term in all documents)
- Returned as floating-point string (e.g., "0.66666666666666663")
- Results can be normalized by dividing by the highest score

### Empty Results

- If no misspelled terms found: return empty array `[]`
- If term has no suggestions: return term with empty suggestions array
- If index is empty: return empty suggestions for all terms

---

## RESP Protocol Examples

### Example 1: Basic Spellcheck

**Command:**
```
FT.SPELLCHECK idx held
```

**Response:**
```
*1
*3
$4
TERM
$4
held
*2
*2
$19
0.66666666666666663
$5
hello
*2
$19
0.33333333333333331
$4
help
```

**Interpretation:**
- Query term "held" is misspelled
- Two suggestions: "hello" (score 0.667) and "help" (score 0.333)

### Example 2: No Misspellings

**Command:**
```
FT.SPELLCHECK idx hello
```

**Response:**
```
*0
```

**Interpretation:**
- "hello" is correctly spelled (exists in index)
- Empty array returned

### Example 3: With DISTANCE and TERMS

**Command:**
```
FT.SPELLCHECK idx helo DISTANCE 2 TERMS INCLUDE custom_dict hello world
```

**Response:**
```
*1
*3
$4
TERM
$4
helo
*3
*2
$3
1.0
$5
hello
*2
$3
0.5
$4
help
*2
$3
0.3
$5
world
```

**Interpretation:**
- DISTANCE 2 allows up to 2 character differences
- TERMS INCLUDE adds "hello" and "world" to suggestion pool
- "world" included despite larger distance due to custom dictionary

### Example 4: Multiple Misspellings

**Command:**
```
FT.SPELLCHECK idx helo wrld
```

**Response:**
```
*2
*3
$4
TERM
$4
helo
*1
*2
$3
0.9
$5
hello
*3
$4
TERM
$4
wrld
*1
*2
$3
0.7
$5
world
```

**Interpretation:**
- Two misspelled terms: "helo" and "wrld"
- Each has its own TERM entry with suggestions

---

## Error Conditions

### Error: Index Not Found

**Command:**
```
FT.SPELLCHECK nonexistent hello
```

**Response:**
```
-ERR Unknown index name
```

### Error: Invalid Distance

**Command:**
```
FT.SPELLCHECK idx hello DISTANCE 5
```

**Response:**
```
-ERR DISTANCE must be between 1 and 4
```

### Error: Wrong Number of Arguments

**Command:**
```
FT.SPELLCHECK idx
```

**Response:**
```
-ERR wrong number of arguments for 'FT.SPELLCHECK' command
```

### Error: Invalid DIALECT

**Command:**
```
FT.SPELLCHECK idx hello DIALECT abc
```

**Response:**
```
-ERR DIALECT must be an integer
```

### Error: Missing TERMS Dictionary Name

**Command:**
```
FT.SPELLCHECK idx hello TERMS INCLUDE
```

**Response:**
```
-ERR TERMS requires dictionary name
```

---

## Levenshtein Distance Algorithm

### Overview

Levenshtein distance measures the minimum number of single-character edits (insertions, deletions, substitutions) required to transform one string into another.

### Algorithm Outline

```
function levenshteinDistance(s1, s2):
    m = length(s1)
    n = length(s2)

    // Initialize matrix (m+1) x (n+1)
    matrix[0..m][0..n]

    // Base cases
    for i = 0 to m:
        matrix[i][0] = i
    for j = 0 to n:
        matrix[0][j] = j

    // Fill matrix
    for i = 1 to m:
        for j = 1 to n:
            if s1[i-1] == s2[j-1]:
                cost = 0
            else:
                cost = 1

            matrix[i][j] = min(
                matrix[i-1][j] + 1,      // deletion
                matrix[i][j-1] + 1,      // insertion
                matrix[i-1][j-1] + cost  // substitution
            )

    return matrix[m][n]
```

### Examples

- `levenshtein("held", "hello")` = 2 (substitute 'd'→'l', insert 'o')
- `levenshtein("helo", "hello")` = 1 (insert 'l')
- `levenshtein("world", "wrld")` = 1 (delete 'o')
- `levenshtein("test", "best")` = 1 (substitute 't'→'b')

### Complexity

- **Time**: O(m * n) where m, n are string lengths
- **Space**: O(m * n) — can be optimized to O(min(m, n)) using two-row buffer

### Stub Implementation Note

**For Iteration 193**, the Levenshtein distance algorithm is **deferred to future iterations**. The stub implementation will:
- Accept the DISTANCE parameter for syntax validation
- Store the distance value (default: 1)
- Return empty suggestions arrays (no actual distance calculations)
- Provide extension point for future implementation

---

## Dictionary Management

### Overview

Custom dictionaries allow users to provide domain-specific terms for inclusion or exclusion from spell checking suggestions.

### Dictionary Structure

```zig
pub const SpellCheckDict = struct {
    name: []const u8,
    terms: std.StringHashMap(void),
    allocator: Allocator,

    pub fn init(allocator: Allocator, name: []const u8) !SpellCheckDict {
        return SpellCheckDict{
            .name = try allocator.dupe(u8, name),
            .terms = std.StringHashMap(void).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SpellCheckDict) void {
        var it = self.terms.keyIterator();
        while (it.next()) |key| {
            self.allocator.free(key.*);
        }
        self.terms.deinit();
        self.allocator.free(self.name);
    }

    pub fn addTerm(self: *SpellCheckDict, term: []const u8) !void {
        const owned = try self.allocator.dupe(u8, term);
        try self.terms.put(owned, {});
    }

    pub fn hasTerm(self: *SpellCheckDict, term: []const u8) bool {
        return self.terms.contains(term);
    }
};
```

### Related Commands

- **FT.DICTADD**: Add terms to a dictionary
- **FT.DICTDEL**: Remove terms from a dictionary
- **FT.DICTDUMP**: List all terms in a dictionary

### Stub Implementation Note

**For Iteration 193**, dictionary management is **deferred to future iterations**. The stub implementation will:
- Parse TERMS INCLUDE/EXCLUDE syntax
- Store dictionary names and terms in command arguments
- Not perform actual inclusion/exclusion (return empty suggestions)
- Provide extension point for FT.DICTADD/DEL/DUMP commands

---

## Implementation Architecture

### Storage Layer (`src/storage/search.zig`)

```zig
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

/// Perform spell checking on query terms (stub implementation)
pub fn spellCheck(
    self: *SearchIndex,
    storage: *Storage,
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
```

### Command Layer (`src/commands/search.zig`)

```zig
/// FT.SPELLCHECK command — Performs spell checking on search query (stub)
///
/// Syntax: FT.SPELLCHECK index query [DISTANCE distance] [TERMS INCLUDE|EXCLUDE dict [terms ...]] [DIALECT dialect]
///
/// Returns: Array of term/suggestions pairs
///
/// Errors:
///   "ERR wrong number of arguments for 'FT.SPELLCHECK' command" if args < 2
///   "ERR Unknown index name" if index doesn't exist
///   "ERR DISTANCE must be between 1 and 4" if invalid distance
///   "ERR DISTANCE must be an integer" if distance not numeric
///   "ERR DIALECT must be an integer" if dialect not numeric
///   "ERR TERMS requires dictionary name" if TERMS without dict name
///   "ERR syntax error" if invalid keyword
pub fn cmdFtSpellcheck(storage: *Storage, arena: std.mem.Allocator, args: []const []const u8) !RespValue {
    if (args.len < 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.SPELLCHECK' command" };
    }

    const index_name = args[0];
    const query = args[1];

    // Parse optional arguments
    var distance: u32 = 1; // Default distance
    var dialect: u32 = 1; // Default dialect
    var include_dicts = std.ArrayList([]const u8).init(arena);
    var exclude_dicts = std.ArrayList([]const u8).init(arena);

    var i: usize = 2;
    while (i < args.len) {
        const keyword = args[i];

        if (std.mem.eql(u8, keyword, "DISTANCE")) {
            i += 1;
            if (i >= args.len) {
                return RespValue{ .error_string = "ERR DISTANCE requires an integer argument" };
            }

            distance = std.fmt.parseInt(u32, args[i], 10) catch {
                return RespValue{ .error_string = "ERR DISTANCE must be an integer" };
            };

            if (distance < 1 or distance > 4) {
                return RespValue{ .error_string = "ERR DISTANCE must be between 1 and 4" };
            }

            i += 1;
        } else if (std.mem.eql(u8, keyword, "TERMS")) {
            i += 1;
            if (i >= args.len) {
                return RespValue{ .error_string = "ERR TERMS requires INCLUDE or EXCLUDE keyword" };
            }

            const mode = args[i];
            const is_include = std.mem.eql(u8, mode, "INCLUDE");
            const is_exclude = std.mem.eql(u8, mode, "EXCLUDE");

            if (!is_include and !is_exclude) {
                return RespValue{ .error_string = "ERR TERMS requires INCLUDE or EXCLUDE keyword" };
            }

            i += 1;
            if (i >= args.len) {
                return RespValue{ .error_string = "ERR TERMS requires dictionary name" };
            }

            const dict_name = args[i];
            if (is_include) {
                try include_dicts.append(dict_name);
            } else {
                try exclude_dicts.append(dict_name);
            }

            i += 1;

            // Consume optional terms after dictionary name (until next keyword)
            while (i < args.len) {
                const next_arg = args[i];
                if (std.mem.eql(u8, next_arg, "DISTANCE") or
                    std.mem.eql(u8, next_arg, "TERMS") or
                    std.mem.eql(u8, next_arg, "DIALECT")) {
                    break;
                }
                // Terms after dict name are currently ignored in stub
                i += 1;
            }
        } else if (std.mem.eql(u8, keyword, "DIALECT")) {
            i += 1;
            if (i >= args.len) {
                return RespValue{ .error_string = "ERR DIALECT requires an integer argument" };
            }

            dialect = std.fmt.parseInt(u32, args[i], 10) catch {
                return RespValue{ .error_string = "ERR DIALECT must be an integer" };
            };

            i += 1;
        } else {
            return RespValue{ .error_string = "ERR syntax error" };
        }
    }

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Verify index exists
    const index = storage.search.getIndex(index_name) orelse {
        return RespValue{ .error_string = "ERR Unknown index name" };
    };

    // Perform spell checking (stub)
    var result = try index.spellCheck(
        storage,
        arena,
        query,
        distance,
        include_dicts.items,
        exclude_dicts.items,
    );
    defer result.deinit();

    // Format response
    var outer_array = try std.ArrayList(RespValue).initCapacity(arena, result.terms.len);
    errdefer outer_array.deinit(arena);

    for (result.terms) |*term_result| {
        // Build term entry: ["TERM", <original_term>, [suggestions]]
        var term_array = try std.ArrayList(RespValue).initCapacity(arena, 3);
        errdefer term_array.deinit(arena);

        // 1. "TERM" marker
        try term_array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "TERM") });

        // 2. Original term
        const term_copy = try arena.dupe(u8, term_result.original_term);
        try term_array.append(arena, RespValue{ .bulk_string = term_copy });

        // 3. Suggestions array
        var suggestions_array = try std.ArrayList(RespValue).initCapacity(arena, term_result.suggestions.len);
        errdefer suggestions_array.deinit(arena);

        for (term_result.suggestions) |*suggestion| {
            // Each suggestion is [score, term]
            var suggestion_pair = try std.ArrayList(RespValue).initCapacity(arena, 2);
            errdefer suggestion_pair.deinit(arena);

            // Score as bulk string
            var score_buf: [32]u8 = undefined;
            const score_str = try std.fmt.bufPrint(&score_buf, "{d}", .{suggestion.score});
            try suggestion_pair.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, score_str) });

            // Suggestion term
            try suggestion_pair.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, suggestion.term) });

            const pair_slice = try suggestion_pair.toOwnedSlice(arena);
            errdefer arena.free(pair_slice);
            try suggestions_array.append(arena, RespValue{ .array = pair_slice });
        }

        const suggestions_slice = try suggestions_array.toOwnedSlice(arena);
        errdefer arena.free(suggestions_slice);
        try term_array.append(arena, RespValue{ .array = suggestions_slice });

        const term_slice = try term_array.toOwnedSlice(arena);
        errdefer arena.free(term_slice);
        try outer_array.append(arena, RespValue{ .array = term_slice });
    }

    const outer_slice = try outer_array.toOwnedSlice(arena);
    return RespValue{ .array = outer_slice };
}
```

### Dispatcher Integration (`src/commands/strings.zig`)

```zig
// In FT dispatcher
} else if (std.mem.eql(u8, subcmd, "SPELLCHECK")) {
    return try search_mod.cmdFtSpellcheck(storage, arena, args[2..]);
```

---

## Testing Strategy

### Unit Tests (Storage Layer)

File: `tests/test_search.zig`

```zig
test "FT.SPELLCHECK stub: empty result for empty query" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    // Create index
    const index_name = "test_idx";
    try storage.search.createIndex(index_name, .hash);

    const index = storage.search.getIndex(index_name).?;

    // Spell check empty query
    var result = try index.spellCheck(&storage, testing.allocator, "", 1, &[_][]const u8{}, &[_][]const u8{});
    defer result.deinit();

    try testing.expectEqual(@as(usize, 0), result.terms.len);
}

test "FT.SPELLCHECK stub: validates distance parameter" {
    // Validated at command layer (not storage layer)
    // See integration tests
}
```

### Integration Tests

File: `tests/test_ft_spellcheck.zig`

```zig
const std = @import("std");
const testing = std.testing;
const Storage = @import("../src/storage/memory.zig").Storage;
const RespValue = @import("../src/protocol/parser.zig").RespValue;
const search_mod = @import("../src/commands/search.zig");

test "FT.SPELLCHECK: basic command (stub returns empty)" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    try storage.search.createIndex("idx", .hash);

    const args = [_][]const u8{ "idx", "hello" };
    const result = try search_mod.cmdFtSpellcheck(&storage, testing.allocator, &args);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .array);
    try testing.expectEqual(@as(usize, 0), result.array.len); // Stub returns empty
}

test "FT.SPELLCHECK: validates DISTANCE range" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    try storage.search.createIndex("idx", .hash);

    // Valid distance (1-4)
    const args1 = [_][]const u8{ "idx", "hello", "DISTANCE", "2" };
    const result1 = try search_mod.cmdFtSpellcheck(&storage, testing.allocator, &args1);
    defer result1.deinit(testing.allocator);
    try testing.expect(result1 == .array);

    // Invalid distance (5)
    const args2 = [_][]const u8{ "idx", "hello", "DISTANCE", "5" };
    const result2 = try search_mod.cmdFtSpellcheck(&storage, testing.allocator, &args2);
    try testing.expect(result2 == .error_string);
    try testing.expect(std.mem.indexOf(u8, result2.error_string, "DISTANCE must be between 1 and 4") != null);

    // Invalid distance (0)
    const args3 = [_][]const u8{ "idx", "hello", "DISTANCE", "0" };
    const result3 = try search_mod.cmdFtSpellcheck(&storage, testing.allocator, &args3);
    try testing.expect(result3 == .error_string);
}

test "FT.SPELLCHECK: TERMS INCLUDE syntax" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    try storage.search.createIndex("idx", .hash);

    const args = [_][]const u8{ "idx", "hello", "TERMS", "INCLUDE", "mydict" };
    const result = try search_mod.cmdFtSpellcheck(&storage, testing.allocator, &args);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .array);
}

test "FT.SPELLCHECK: TERMS EXCLUDE syntax" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    try storage.search.createIndex("idx", .hash);

    const args = [_][]const u8{ "idx", "world", "TERMS", "EXCLUDE", "stopwords" };
    const result = try search_mod.cmdFtSpellcheck(&storage, testing.allocator, &args);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .array);
}

test "FT.SPELLCHECK: TERMS with multiple terms after dict name" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    try storage.search.createIndex("idx", .hash);

    const args = [_][]const u8{ "idx", "helo", "TERMS", "INCLUDE", "custom", "hello", "help", "world" };
    const result = try search_mod.cmdFtSpellcheck(&storage, testing.allocator, &args);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .array);
}

test "FT.SPELLCHECK: DIALECT argument" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    try storage.search.createIndex("idx", .hash);

    const args = [_][]const u8{ "idx", "test", "DIALECT", "2" };
    const result = try search_mod.cmdFtSpellcheck(&storage, testing.allocator, &args);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .array);
}

test "FT.SPELLCHECK: combined options" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    try storage.search.createIndex("idx", .hash);

    const args = [_][]const u8{
        "idx", "helo wrld",
        "DISTANCE", "2",
        "TERMS", "INCLUDE", "dict1", "hello",
        "TERMS", "EXCLUDE", "dict2", "bad",
        "DIALECT", "3"
    };
    const result = try search_mod.cmdFtSpellcheck(&storage, testing.allocator, &args);
    defer result.deinit(testing.allocator);

    try testing.expect(result == .array);
}

test "FT.SPELLCHECK: error - index not found" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    const args = [_][]const u8{ "nonexistent", "hello" };
    const result = try search_mod.cmdFtSpellcheck(&storage, testing.allocator, &args);

    try testing.expect(result == .error_string);
    try testing.expect(std.mem.indexOf(u8, result.error_string, "Unknown index name") != null);
}

test "FT.SPELLCHECK: error - wrong number of arguments" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    const args1 = [_][]const u8{"idx"};
    const result1 = try search_mod.cmdFtSpellcheck(&storage, testing.allocator, &args1);

    try testing.expect(result1 == .error_string);
    try testing.expect(std.mem.indexOf(u8, result1.error_string, "wrong number of arguments") != null);
}

test "FT.SPELLCHECK: error - DISTANCE not integer" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    try storage.search.createIndex("idx", .hash);

    const args = [_][]const u8{ "idx", "hello", "DISTANCE", "abc" };
    const result = try search_mod.cmdFtSpellcheck(&storage, testing.allocator, &args);

    try testing.expect(result == .error_string);
    try testing.expect(std.mem.indexOf(u8, result.error_string, "DISTANCE must be an integer") != null);
}

test "FT.SPELLCHECK: error - DIALECT not integer" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    try storage.search.createIndex("idx", .hash);

    const args = [_][]const u8{ "idx", "hello", "DIALECT", "xyz" };
    const result = try search_mod.cmdFtSpellcheck(&storage, testing.allocator, &args);

    try testing.expect(result == .error_string);
    try testing.expect(std.mem.indexOf(u8, result.error_string, "DIALECT must be an integer") != null);
}

test "FT.SPELLCHECK: error - TERMS without mode" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    try storage.search.createIndex("idx", .hash);

    const args = [_][]const u8{ "idx", "hello", "TERMS" };
    const result = try search_mod.cmdFtSpellcheck(&storage, testing.allocator, &args);

    try testing.expect(result == .error_string);
    try testing.expect(std.mem.indexOf(u8, result.error_string, "TERMS requires INCLUDE or EXCLUDE") != null);
}

test "FT.SPELLCHECK: error - TERMS without dict name" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    try storage.search.createIndex("idx", .hash);

    const args = [_][]const u8{ "idx", "hello", "TERMS", "INCLUDE" };
    const result = try search_mod.cmdFtSpellcheck(&storage, testing.allocator, &args);

    try testing.expect(result == .error_string);
    try testing.expect(std.mem.indexOf(u8, result.error_string, "TERMS requires dictionary name") != null);
}

test "FT.SPELLCHECK: error - invalid keyword" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    try storage.search.createIndex("idx", .hash);

    const args = [_][]const u8{ "idx", "hello", "INVALID", "arg" };
    const result = try search_mod.cmdFtSpellcheck(&storage, testing.allocator, &args);

    try testing.expect(result == .error_string);
    try testing.expect(std.mem.indexOf(u8, result.error_string, "syntax error") != null);
}
```

### Manual Testing (redis-cli)

```bash
# Create index
redis-cli FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA title TEXT content TEXT

# Add test documents
redis-cli HSET doc:1 title "hello world" content "test document"
redis-cli HSET doc:2 title "help guide" content "another document"

# Basic spell check (stub returns empty)
redis-cli FT.SPELLCHECK idx "helo"

# With DISTANCE
redis-cli FT.SPELLCHECK idx "helo" DISTANCE 2

# With TERMS INCLUDE
redis-cli FT.SPELLCHECK idx "helo" TERMS INCLUDE mydict hello world

# With TERMS EXCLUDE
redis-cli FT.SPELLCHECK idx "helo" TERMS EXCLUDE stopwords the a an

# Multiple options
redis-cli FT.SPELLCHECK idx "helo wrld" DISTANCE 2 TERMS INCLUDE dict1 hello DIALECT 2

# Error cases
redis-cli FT.SPELLCHECK nonexistent "hello"  # Unknown index
redis-cli FT.SPELLCHECK idx "hello" DISTANCE 5  # Invalid distance
redis-cli FT.SPELLCHECK idx  # Wrong number of arguments
```

---

## Stub Implementation Summary

### What Gets Implemented in Iteration 193

1. ✅ **Command parsing**: Full argument parsing with all options
2. ✅ **Validation**: Index existence, DISTANCE range (1-4), argument counts
3. ✅ **RESP formatting**: Correct 3-element array structure per term
4. ✅ **Error handling**: All error conditions properly handled
5. ✅ **TERMS syntax**: Parse INCLUDE/EXCLUDE with dictionary names and optional terms
6. ✅ **DIALECT support**: Parse and validate DIALECT argument
7. ✅ **Empty results**: Return empty array (no suggestions) for all queries

### What Gets Deferred to Future Iterations

1. ⏸️ **Levenshtein distance**: Algorithm implementation deferred
2. ⏸️ **Query parsing**: Tokenization and term extraction
3. ⏸️ **Index term matching**: Actual spell checking against indexed terms
4. ⏸️ **Score calculation**: Document frequency-based scoring
5. ⏸️ **Dictionary operations**: FT.DICTADD/DEL/DUMP commands
6. ⏸️ **Dictionary filtering**: Include/exclude logic
7. ⏸️ **Suggestion ranking**: Sort by score descending

### Extension Points

```zig
// Future: Add to SearchIndex struct
pub const SearchIndex = struct {
    // ... existing fields ...

    /// Indexed terms for spell checking (future)
    /// Maps term → document count
    indexed_terms: std.StringHashMap(u64),
};

// Future: Real implementation
pub fn spellCheck(
    self: *SearchIndex,
    storage: *Storage,
    allocator: Allocator,
    query: []const u8,
    distance: u32,
    include_dicts: []const []const u8,
    exclude_dicts: []const []const u8,
) !SpellCheckResult {
    // Parse query into terms
    var query_terms = try tokenizeQuery(allocator, query);
    defer query_terms.deinit();

    var results = try std.ArrayList(SpellCheckTermResult).initCapacity(allocator, query_terms.items.len);

    for (query_terms.items) |term| {
        // Check if term exists in index
        if (self.indexed_terms.get(term)) |_| {
            // Term is correct, skip
            continue;
        }

        // Find suggestions using Levenshtein distance
        var suggestions = try std.ArrayList(SpellCheckSuggestion).initCapacity(allocator, 10);

        var it = self.indexed_terms.iterator();
        while (it.next()) |entry| {
            const indexed_term = entry.key_ptr.*;
            const doc_count = entry.value_ptr.*;

            const dist = levenshteinDistance(term, indexed_term);
            if (dist <= distance) {
                const score = @intToFloat(f64, doc_count) / @intToFloat(f64, self.total_documents);
                try suggestions.append(SpellCheckSuggestion{
                    .term = try allocator.dupe(u8, indexed_term),
                    .score = score,
                });
            }
        }

        // Apply dictionary filters
        try applyDictionaryFilters(&suggestions, include_dicts, exclude_dicts);

        // Sort by score descending
        std.sort.sort(SpellCheckSuggestion, suggestions.items, {}, compareScoreDesc);

        try results.append(SpellCheckTermResult{
            .original_term = try allocator.dupe(u8, term),
            .suggestions = try suggestions.toOwnedSlice(),
            .allocator = allocator,
        });
    }

    return SpellCheckResult{
        .terms = try results.toOwnedSlice(),
        .allocator = allocator,
    };
}
```

---

## Success Criteria

### Phase Gate Requirements

- ✅ All tests pass (`zig build test`)
- ✅ Zero memory leaks (`std.testing.allocator`)
- ✅ Command registered in dispatcher
- ✅ Correct RESP format returned (even if empty)
- ✅ All error conditions handled
- ✅ Integration tests cover all argument combinations
- ✅ Manual testing with redis-cli confirms syntax acceptance
- ✅ Code quality review passes (memory safety, error handling, docs)
- ✅ Architecture review passes (separation of concerns, extension points)

### Behavioral Expectations

**Current (Stub)**:
```
redis-cli> FT.SPELLCHECK idx "helo wrld"
(empty array)
```

**Future (Full Implementation)**:
```
redis-cli> FT.SPELLCHECK idx "helo wrld" DISTANCE 2
1) 1) "TERM"
   2) "helo"
   3) 1) 1) "0.9"
         2) "hello"
      2) 1) "0.1"
         2) "help"
2) 1) "TERM"
   2) "wrld"
   3) 1) 1) "1.0"
         2) "world"
```

---

## References

- [Redis FT.SPELLCHECK Documentation](https://redis.io/docs/latest/commands/ft.spellcheck/)
- [Redis Search Spellchecking Guide](https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/spellcheck/)
- [Levenshtein Distance Algorithm](https://en.wikipedia.org/wiki/Levenshtein_distance)
- [FT.DICTADD Command](https://redis.io/docs/latest/commands/ft.dictadd/)
- [FT.DICTDEL Command](https://redis.io/docs/latest/commands/ft.dictdel/)
- [FT.DICTDUMP Command](https://redis.io/docs/latest/commands/ft.dictdump/)

---

## Notes

1. **Stub scope**: This iteration focuses on command infrastructure (parsing, validation, RESP format). Real spell checking logic is deferred to future iterations when FT.DICTADD/DEL/DUMP and index term tracking are implemented.

2. **Dictionary commands**: FT.DICTADD/DEL/DUMP are separate commands in the PRD and will be implemented in future iterations.

3. **Levenshtein distance**: Well-defined algorithm with O(m*n) complexity. Can be optimized with space-efficient two-row buffer.

4. **Score normalization**: Redis returns raw scores (doc_count / total_docs). Clients can normalize by dividing by max score if needed.

5. **Ordering guarantees**: Misspelled terms ordered by appearance in query, suggestions ordered by score descending.

6. **DIALECT parameter**: Parsed and validated but not used in stub (extension point for future query parser).

7. **Extension strategy**: All data structures and function signatures designed to support future real implementation without breaking changes.
