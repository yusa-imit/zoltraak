# Phase 12 — JSON Data Type Implementation Specification

**Author**: redis-spec-analyzer agent
**Date**: 2026-04-06
**Status**: Planning
**Target Iterations**: 169-174 (6 iterations)
**Commands**: 26 JSON.* commands
**Estimated LOC**: ~3,500-4,500 (storage layer + parser + commands + tests)

---

## Executive Summary

Phase 12 implements Redis 8.0's built-in JSON data type with full JSONPath query support (both legacy `.path` and RFC 9535 `$..path` syntax). This phase adds 26 JSON.* commands for document storage, retrieval, and atomic sub-document operations. The implementation prioritizes Redis compatibility, memory efficiency, and integration with existing persistence layers (RDB/AOF).

**Key Milestones**:
1. JSONPath parser supporting RFC 9535 syntax + legacy path syntax
2. JSON value representation in storage layer (new ValueType variant)
3. Core document operations (SET/GET/DEL/TYPE)
4. Atomic numeric/string/boolean operations
5. Array manipulation commands (8 commands)
6. Object introspection commands
7. Advanced operations (MERGE, DEBUG, MGET/MSET)

---

## 1. Specification Reference

### Primary Sources
- [Redis JSON Commands Documentation](https://redis.io/docs/latest/commands/?group=json)
- [Redis JSON Data Type Guide](https://redis.io/docs/latest/develop/data-types/json/)
- [JSONPath Syntax Guide](https://redis.io/docs/latest/develop/data-types/json/path/)
- [RFC 9535 - JSONPath: Query Expressions for JSON](https://datatracker.ietf.org/doc/rfc9535/)
- [RFC 7386 - JSON Merge Patch](https://datatracker.ietf.org/doc/html/rfc7386)

### Command List (26 commands)

| Category | Commands | Count |
|----------|----------|-------|
| Core Operations | JSON.SET, JSON.GET, JSON.MGET, JSON.MSET, JSON.DEL, JSON.FORGET, JSON.TYPE | 7 |
| Numeric Operations | JSON.NUMINCRBY, JSON.NUMMULTBY | 2 |
| String Operations | JSON.STRAPPEND, JSON.STRLEN | 2 |
| Boolean Operations | JSON.TOGGLE, JSON.CLEAR | 2 |
| Array Operations | JSON.ARRAPPEND, JSON.ARRINDEX, JSON.ARRINSERT, JSON.ARRLEN, JSON.ARRPOP, JSON.ARRTRIM | 6 |
| Object Operations | JSON.OBJKEYS, JSON.OBJLEN | 2 |
| Advanced Operations | JSON.RESP, JSON.MERGE | 2 |
| Debug Operations | JSON.DEBUG, JSON.DEBUG HELP, JSON.DEBUG MEMORY | 3 |

---

## 2. Current State Analysis

### Existing Infrastructure (Can Leverage)
✅ **RESP2/RESP3 Protocol**: Full support in `src/protocol/parser.zig` and `src/protocol/writer.zig`
✅ **Tagged Union Storage**: `Value` enum in `src/storage/memory.zig` supports extensibility
✅ **RDB/AOF Persistence**: Framework in `src/storage/persistence.zig` and `src/storage/aof.zig`
✅ **Expiration System**: TTL tracking already implemented for all value types
✅ **ACL System**: Command categorization via `@json` category
✅ **Command Routing**: Dispatcher pattern in `src/commands/strings.zig`

### Missing Infrastructure (Must Build)
❌ **JSONPath Parser**: No RFC 9535 query parser exists
❌ **JSON Value Representation**: No internal JSON document model
❌ **Path Evaluator**: No query execution engine for `$..path` syntax
❌ **JSON Serialization**: No efficient JSON encode/decode
❌ **Sub-document Operations**: No atomic in-place modification

### Dependencies
- **No blocking dependencies**: Phase 12 is independent (Phase 13 Search will depend on this)
- **Optional enhancement**: zuda may provide JSON parser in future (check after Phase 12.1)

---

## 3. Architecture Design

### 3.1 Storage Layer Integration

**File**: `src/storage/memory.zig`

Add new variant to `Value` tagged union:

```zig
pub const ValueType = enum {
    string,
    list,
    set,
    hash,
    sorted_set,
    stream,
    hyperloglog,
    json, // NEW
};

pub const Value = union(ValueType) {
    // ... existing variants ...
    json: JsonValue,

    pub const JsonValue = struct {
        root: *JsonNode, // Root node of the JSON tree
        expires_at: ?i64,
        allocator: std.mem.Allocator,

        pub fn deinit(self: *JsonValue) void {
            self.root.deinit(self.allocator);
            self.allocator.destroy(self.root);
        }
    };
};
```

### 3.2 JSON Node Representation

**File**: `src/storage/json_value.zig` (new)

```zig
/// Internal JSON document representation
/// Optimized for sub-document queries and atomic modifications
pub const JsonNode = union(enum) {
    null: void,
    bool: bool,
    number: f64,  // All numbers stored as f64 (Redis behavior)
    string: []const u8,
    array: std.ArrayList(*JsonNode),
    object: std.StringHashMap(*JsonNode),

    /// Parse JSON string into JsonNode tree
    pub fn parse(allocator: std.mem.Allocator, json_str: []const u8) !*JsonNode;

    /// Serialize JsonNode tree to JSON string
    pub fn stringify(self: *const JsonNode, allocator: std.mem.Allocator) ![]const u8;

    /// Deep clone a node
    pub fn clone(self: *const JsonNode, allocator: std.mem.Allocator) !*JsonNode;

    /// Recursively free all child nodes
    pub fn deinit(self: *JsonNode, allocator: std.mem.Allocator) void;

    /// Get type name as string ("null", "boolean", "number", "string", "array", "object")
    pub fn typeName(self: *const JsonNode) []const u8;
};
```

**Design Rationale**:
- Uses tagged union for type-safe variant handling (idiomatic Zig)
- All numbers stored as `f64` to match Redis behavior (no integer/float distinction in JSON)
- Pointers (`*JsonNode`) enable structural sharing and efficient tree operations
- String keys stored directly in HashMap (no wrapper structs)

### 3.3 JSONPath Parser & Evaluator

**File**: `src/storage/jsonpath.zig` (new)

```zig
/// JSONPath query parser supporting both legacy and RFC 9535 syntax
pub const JsonPath = struct {
    segments: std.ArrayList(PathSegment),
    is_legacy: bool, // false if starts with $, true if starts with .

    pub const PathSegment = union(enum) {
        root,                              // $
        child: []const u8,                 // .key or $.key
        wildcard,                          // .* or $.*
        recursive: []const u8,             // ..key or $..key
        recursive_wildcard,                // .. or $..[*]
        array_index: i64,                  // [0], [1], [-1] (negative = from end)
        array_slice: ArraySlice,           // [start:end:step]
        array_wildcard,                    // [*]
        filter: FilterExpr,                // [?(@.price < 100)]

        pub const ArraySlice = struct {
            start: ?i64,
            end: ?i64,
            step: i64 = 1,
        };

        pub const FilterExpr = struct {
            // Simplified filter for Phase 12 - full filter expressions in Phase 13
            operator: FilterOp,
            field: []const u8,
            value: []const u8,
        };

        pub const FilterOp = enum { eq, ne, lt, gt, le, ge, regex };
    };

    /// Parse JSONPath query string into segments
    pub fn parse(allocator: std.mem.Allocator, query: []const u8) !JsonPath;

    /// Evaluate path against a JSON document, returns all matching nodes
    pub fn evaluate(self: *const JsonPath, root: *const JsonNode) !std.ArrayList(*JsonNode);

    /// Check if path can create new keys (only root paths $ or . can create)
    pub fn canCreate(self: *const JsonPath) bool;
};
```

**Path Syntax Examples**:
- Legacy: `.`, `.a`, `.a.b`, `..a`, `.a[0]`, `.a[*]`
- JSONPath: `$`, `$.a`, `$.a.b`, `$..a`, `$.a[0]`, `$.a[*]`, `$[?(@.x>5)]`

**Evaluation Algorithm**:
1. Parse query into segments
2. Start with root node in results set
3. For each segment, apply to all nodes in current results:
   - **child**: Get object member by key
   - **wildcard**: Get all object members or array elements
   - **recursive**: Depth-first search for matching keys at any level
   - **array_index**: Get array element at index (negative = from end)
   - **array_slice**: Get array elements in range [start:end:step]
   - **filter**: Evaluate predicate on each node
4. Return all matching nodes

**Performance**:
- Simple paths (`.a.b[0]`): O(depth)
- Recursive paths (`$..a`): O(N) where N = total nodes in document
- Filter expressions: O(N × filter complexity)

### 3.4 Command Handler Structure

**File**: `src/commands/json.zig` (new)

```zig
const std = @import("std");
const Storage = @import("../storage/memory.zig").Storage;
const RespValue = @import("../protocol/parser.zig").RespValue;
const JsonNode = @import("../storage/json_value.zig").JsonNode;
const JsonPath = @import("../storage/jsonpath.zig").JsonPath;

/// JSON.SET key path value [NX | XX]
pub fn cmdJsonSet(storage: *Storage, args: []const RespValue, allocator: std.mem.Allocator) !RespValue;

/// JSON.GET key [INDENT indent] [NEWLINE newline] [SPACE space] [path ...]
pub fn cmdJsonGet(storage: *Storage, args: []const RespValue, allocator: std.mem.Allocator) !RespValue;

/// JSON.DEL key [path]
pub fn cmdJsonDel(storage: *Storage, args: []const RespValue, allocator: std.mem.Allocator) !RespValue;

/// JSON.TYPE key [path]
pub fn cmdJsonType(storage: *Storage, args: []const RespValue, allocator: std.mem.Allocator) !RespValue;

// ... (22 more command handlers)
```

**Dispatcher Integration** (in `src/commands/strings.zig`):

```zig
// Add JSON.* command routing
if (std.ascii.startsWithIgnoreCase(cmd_name, "JSON.")) {
    const subcmd = cmd_name[5..];
    return try routeJsonCommand(storage, subcmd, args, allocator, client);
}
```

### 3.5 Persistence Integration

**RDB Format** (in `src/storage/persistence.zig`):

```
JSON value encoding:
- Type byte: 0x0F (new RDB type for JSON)
- JSON string length (varint)
- JSON string data (UTF-8)
- Expiration info (existing TTL encoding)
```

**AOF Format** (in `src/storage/aof.zig`):

```
Replicate JSON commands exactly as received:
*4\r\n$8\r\nJSON.SET\r\n$3\r\nkey\r\n$1\r\n$\r\n$15\r\n{"a":1,"b":2}\r\n
```

---

## 4. Command Specifications

### 4.1 Core Operations

#### JSON.SET

**Syntax**: `JSON.SET key path value [NX | XX]`

**Arguments**:
- `key`: Redis key (creates if not exists for root paths)
- `path`: JSONPath or legacy path (default: `$`)
- `value`: JSON value as string (scalar or compound)
- `NX`: Set only if path doesn't exist
- `XX`: Set only if path exists

**Behavior**:
- Root path (`$` or `.`) creates new JSON document
- Non-root path requires parent to exist
- Multiple matches: updates all matching locations
- Object member creation: adds new field if parent is object
- Array index: error if out of bounds
- NX/XX: operates on path existence, not key existence

**Return**:
- `OK` on success
- `(nil)` if NX/XX condition not met or path cannot be created
- Error if invalid JSON, invalid path, or new key with non-root path

**Time Complexity**:
- Single path: O(M+N) where M = old value size, N = new value size
- Multiple paths: O(M+N) where M = document size, N = new value size × match count

**Examples**:
```redis
JSON.SET doc $ '{"a":2}'          # Create document, returns OK
JSON.SET doc $.a '3'              # Replace value, returns OK
JSON.SET doc $.b '8'              # Add new field, returns OK
JSON.SET doc $..a 3               # Update all 'a' fields
JSON.SET doc $.x.y 2              # Error: parent doesn't exist
JSON.SET new $ '5' NX             # Create if not exists
JSON.SET new $ '10' NX            # Returns (nil) - already exists
```

**ACL Category**: `@json`, `@write`, `@slow`

---

#### JSON.GET

**Syntax**: `JSON.GET key [INDENT indent] [NEWLINE newline] [SPACE space] [path ...]`

**Arguments**:
- `key`: Redis key
- `INDENT`: Indentation string for formatting (optional)
- `NEWLINE`: Line ending string (optional)
- `SPACE`: String between key and value (optional)
- `path`: One or more JSONPath queries (default: `$`)

**Behavior**:
- Single path: Returns JSON array of matching values
- Multiple paths: Returns JSON object with path as key, array of values as value
- Formatting options apply to output JSON
- Missing key: returns `(nil)`
- No matches for path: returns empty array `[]`

**Return**:
- Bulk string: JSON-encoded result
- `(nil)` if key doesn't exist

**Time Complexity**:
- Single path: O(N) where N = result size
- Multiple paths: O(N) where N = document size

**Examples**:
```redis
JSON.GET doc $                          # Get entire document
JSON.GET doc $.a                        # Get single field: "[2]"
JSON.GET doc $..a                       # Get all 'a' fields: "[2,4,6]"
JSON.GET doc $.a $.b                    # Multiple paths: '{"$.a":[2],"$.b":[8]}'
JSON.GET doc INDENT "\t" NEWLINE "\n" $ # Formatted output
```

**ACL Category**: `@json`, `@read`, `@slow`

---

#### JSON.MGET / JSON.MSET

**MGET Syntax**: `JSON.MGET key [key ...] path`

**Behavior**:
- Gets value at `path` from multiple keys
- Returns array of values (one per key)
- Missing keys or paths return `null` in array

**Return**: Array of bulk strings (JSON values or null)

**MSET Syntax**: `JSON.MSET key path value [key path value ...]`

**Behavior**:
- Sets multiple key-path-value triplets atomically
- All operations must succeed or all fail
- Creates keys if needed (path must be root)

**Return**: `OK` or error

**Examples**:
```redis
JSON.MGET doc1 doc2 doc3 $.a     # Get $.a from 3 documents
JSON.MSET k1 $ '1' k2 $ '2'      # Set multiple documents
```

**ACL Category**: `@json`, `@read/@write`, `@slow`

---

#### JSON.DEL / JSON.FORGET

**Syntax**: `JSON.DEL key [path]`

**Behavior**:
- Deletes values at matching paths
- Root path deletion (`$`) deletes the entire key
- Returns count of deleted values
- Missing paths silently ignored (returns 0)

**Return**: Integer (count of deleted values)

**Note**: `JSON.FORGET` is an alias for `JSON.DEL`

**Examples**:
```redis
JSON.DEL doc $..a                # Delete all 'a' fields, returns 2
JSON.DEL doc $                   # Delete entire document
JSON.DEL doc $.nonexistent       # Returns 0
```

**ACL Category**: `@json`, `@write`, `@slow`

---

#### JSON.TYPE

**Syntax**: `JSON.TYPE key [path]`

**Behavior**:
- Returns type name(s) of value(s) at path
- Type names: `null`, `boolean`, `number`, `string`, `array`, `object`
- Legacy path (`.`): returns single bulk string
- JSONPath (`$`): returns array of bulk strings (one per match)

**Return**:
- Legacy: Bulk string or `(nil)`
- JSONPath: Array of bulk strings or nulls

**Examples**:
```redis
JSON.TYPE doc $               # ["object"]
JSON.TYPE doc $.a             # ["number"]
JSON.TYPE doc $..a            # ["number", "number"]
```

**ACL Category**: `@json`, `@read`, `@fast`

---

### 4.2 Numeric Operations

#### JSON.NUMINCRBY

**Syntax**: `JSON.NUMINCRBY key path value`

**Behavior**:
- Increments numeric value(s) at path by `value`
- `value` can be negative (decrement)
- Multiple matches: increments all
- Returns new value(s)
- Error if value is not a number

**Return**:
- Legacy path: Bulk string (JSON number)
- JSONPath: Array of bulk strings (JSON numbers or nulls)

**Examples**:
```redis
JSON.NUMINCRBY doc $.count 1        # Increment by 1
JSON.NUMINCRBY doc $.price -10.5    # Decrement by 10.5
JSON.NUMINCRBY doc $..qty 5         # Increment all 'qty' fields
```

**ACL Category**: `@json`, `@write`, `@slow`

---

#### JSON.NUMMULTBY

**Syntax**: `JSON.NUMMULTBY key path value`

**Behavior**: Same as NUMINCRBY but multiplies instead of adds

**Examples**:
```redis
JSON.NUMMULTBY doc $.price 1.1      # Increase price by 10%
JSON.NUMMULTBY doc $..score 2       # Double all scores
```

**ACL Category**: `@json`, `@write`, `@slow`

---

### 4.3 String Operations

#### JSON.STRAPPEND

**Syntax**: `JSON.STRAPPEND key [path] value`

**Behavior**:
- Appends `value` to string(s) at path
- Multiple matches: appends to all
- Error if value is not a string
- Returns new string length(s)

**Return**:
- Legacy path: Integer (new length)
- JSONPath: Array of integers or nulls

**Examples**:
```redis
JSON.STRAPPEND doc $.name ' Jr.'    # Append to name
JSON.STRAPPEND doc $..label '!'     # Append to all labels
```

**ACL Category**: `@json`, `@write`, `@slow`

---

#### JSON.STRLEN

**Syntax**: `JSON.STRLEN key [path]`

**Behavior**:
- Returns length of string(s) at path
- Returns null for non-string values

**Return**:
- Legacy path: Integer or `(nil)`
- JSONPath: Array of integers or nulls

**Examples**:
```redis
JSON.STRLEN doc $.name              # Get name length
JSON.STRLEN doc $..label            # Get all label lengths
```

**ACL Category**: `@json`, `@read`, `@fast`

---

### 4.4 Boolean Operations

#### JSON.TOGGLE

**Syntax**: `JSON.TOGGLE key path`

**Behavior**:
- Toggles boolean value(s): `true` ↔ `false`
- Returns new value: 0 (false) or 1 (true)
- Returns null for non-boolean values
- Multiple matches: toggles all

**Return**:
- Legacy path: Integer (0 or 1) or `(nil)`
- JSONPath: Array of integers or nulls

**Examples**:
```redis
JSON.TOGGLE doc $.active            # Toggle flag: [1]
JSON.TOGGLE doc $..enabled          # Toggle all enabled flags
```

**ACL Category**: `@json`, `@write`, `@fast`

---

#### JSON.CLEAR

**Syntax**: `JSON.CLEAR key [path]`

**Behavior**:
- Clears value at path to default:
  - `number` → `0`
  - `string` → `""`
  - `boolean` → `false`
  - `array` → `[]`
  - `object` → `{}`
  - `null` → remains `null`
- Returns count of cleared values

**Return**: Integer (count of cleared values)

**Examples**:
```redis
JSON.CLEAR doc $.arr                # Clear array: returns 1
JSON.CLEAR doc $..obj               # Clear all objects
```

**ACL Category**: `@json`, `@write`, `@slow`

---

### 4.5 Array Operations

#### JSON.ARRAPPEND

**Syntax**: `JSON.ARRAPPEND key [path] value [value ...]`

**Behavior**:
- Appends one or more values to array(s) at path
- Multiple matches: appends to all arrays
- Returns new array length(s)
- Error if value is not an array
- String values must be quoted: `'"string"'`

**Return**:
- Legacy path: Integer (new length) or `(nil)`
- JSONPath: Array of integers or nulls

**Examples**:
```redis
JSON.ARRAPPEND doc $.arr 1 2 3      # Append 3 numbers: [3]
JSON.ARRAPPEND doc $.arr '"x"'      # Append string: [4]
JSON.ARRAPPEND doc $..arr 99        # Append to all arrays
```

**Time Complexity**: O(1) per array (single path), O(N) for multiple paths

**ACL Category**: `@json`, `@write`, `@slow`

---

#### JSON.ARRINDEX

**Syntax**: `JSON.ARRINDEX key path value [start [stop]]`

**Behavior**:
- Finds first index of `value` in array(s)
- `start`: Starting index (default: 0)
- `stop`: Ending index (default: 0 = end of array)
- Returns -1 if not found
- Negative indices count from end

**Return**:
- Legacy path: Integer (index or -1)
- JSONPath: Array of integers

**Examples**:
```redis
JSON.ARRINDEX doc $.arr 5           # Find 5 in array: [2]
JSON.ARRINDEX doc $.arr 5 0 10      # Search in range [0,10)
JSON.ARRINDEX doc $..arr 99         # Search all arrays
```

**Time Complexity**: O(N) per array where N = array length

**ACL Category**: `@json`, `@read`, `@slow`

---

#### JSON.ARRINSERT

**Syntax**: `JSON.ARRINSERT key path index value [value ...]`

**Behavior**:
- Inserts values at `index` in array(s)
- Negative index counts from end
- Index out of bounds: error
- Returns new array length(s)

**Return**:
- Legacy path: Integer (new length) or `(nil)`
- JSONPath: Array of integers or nulls

**Examples**:
```redis
JSON.ARRINSERT doc $.arr 0 1 2      # Insert at start
JSON.ARRINSERT doc $.arr -1 99      # Insert before last
```

**Time Complexity**: O(N) per array where N = array length

**ACL Category**: `@json`, `@write`, `@slow`

---

#### JSON.ARRLEN

**Syntax**: `JSON.ARRLEN key [path]`

**Behavior**:
- Returns length of array(s) at path
- Returns null for non-array values

**Return**:
- Legacy path: Integer or `(nil)`
- JSONPath: Array of integers or nulls

**Examples**:
```redis
JSON.ARRLEN doc $.arr               # Get array length: [5]
JSON.ARRLEN doc $..arr              # Get all array lengths
```

**Time Complexity**: O(1) per array

**ACL Category**: `@json`, `@read`, `@fast`

---

#### JSON.ARRPOP

**Syntax**: `JSON.ARRPOP key [path [index]]`

**Behavior**:
- Removes and returns element at `index` (default: -1 = last)
- Negative index counts from end
- Returns null if array is empty or index out of bounds

**Return**:
- Legacy path: Bulk string (JSON value) or `(nil)`
- JSONPath: Array of bulk strings or nulls

**Examples**:
```redis
JSON.ARRPOP doc $.arr               # Pop last: ["5"]
JSON.ARRPOP doc $.arr 0             # Pop first: ["1"]
JSON.ARRPOP doc $..arr -1           # Pop last from all arrays
```

**Time Complexity**: O(N) per array where N = array length

**ACL Category**: `@json`, `@write`, `@slow`

---

#### JSON.ARRTRIM

**Syntax**: `JSON.ARRTRIM key path start stop`

**Behavior**:
- Trims array(s) to range [start, stop] inclusive
- Negative indices count from end
- Out of bounds indices clamped to array bounds
- Returns new array length(s)

**Return**:
- Legacy path: Integer (new length) or `(nil)`
- JSONPath: Array of integers or nulls

**Examples**:
```redis
JSON.ARRTRIM doc $.arr 0 2          # Keep first 3 elements: [3]
JSON.ARRTRIM doc $.arr -3 -1        # Keep last 3 elements: [3]
JSON.ARRTRIM doc $..arr 1 -2        # Trim all arrays
```

**Time Complexity**: O(N) per array where N = array length

**ACL Category**: `@json`, `@write`, `@slow`

---

### 4.6 Object Operations

#### JSON.OBJKEYS

**Syntax**: `JSON.OBJKEYS key [path]`

**Behavior**:
- Returns key names in object(s) at path
- Returns null for non-object values
- Key order is implementation-defined (no guaranteed order)

**Return**:
- Legacy path: Array of bulk strings or `(nil)`
- JSONPath: Array of arrays or nulls

**Examples**:
```redis
JSON.OBJKEYS doc $                  # [["a","b","c"]]
JSON.OBJKEYS doc $.obj              # [["x","y","z"]]
JSON.OBJKEYS doc $..obj             # Multiple object key lists
```

**Time Complexity**: O(N) per object where N = key count

**ACL Category**: `@json`, `@read`, `@slow`

---

#### JSON.OBJLEN

**Syntax**: `JSON.OBJLEN key [path]`

**Behavior**:
- Returns number of keys in object(s) at path
- Returns null for non-object values

**Return**:
- Legacy path: Integer or `(nil)`
- JSONPath: Array of integers or nulls

**Examples**:
```redis
JSON.OBJLEN doc $                   # [3]
JSON.OBJLEN doc $.obj               # [2]
JSON.OBJLEN doc $..obj              # [2, 5, 1]
```

**Time Complexity**: O(1) per object

**ACL Category**: `@json`, `@read`, `@fast`

---

### 4.7 Advanced Operations

#### JSON.RESP

**Syntax**: `JSON.RESP key [path]`

**Behavior**:
- Returns JSON value at path as RESP2 native types:
  - `null` → Null reply
  - `boolean` → Simple string "true"/"false"
  - `number` → Bulk string with number
  - `string` → Bulk string
  - `array` → Array reply (recursive)
  - `object` → Array reply: `[key1, val1, key2, val2, ...]`

**Return**: RESP2 encoded value (mixed types)

**Examples**:
```redis
JSON.RESP doc $.num                 # "123"
JSON.RESP doc $.arr                 # ["a", "b", "c"]
JSON.RESP doc $.obj                 # ["x", 1, "y", 2]
```

**Time Complexity**: O(N) where N = value size

**ACL Category**: `@json`, `@read`, `@slow`

---

#### JSON.MERGE

**Syntax**: `JSON.MERGE key path value`

**Behavior**:
- Merges `value` into JSON at path using RFC 7386 JSON Merge Patch
- Merge rules:
  - `null` in patch: Delete key from object
  - Non-null value: Update or create key
  - Array: Replace entire array (not merge elements)
  - Primitive: Replace value
- Path must exist (creates if path is root `$`)

**Return**: `OK` or error

**Examples**:
```redis
JSON.MERGE doc $ '{"a":null}'       # Delete 'a' field
JSON.MERGE doc $.obj '{"x":10}'     # Add/update 'x' field
JSON.MERGE doc $.arr '[1,2,3]'      # Replace array
```

**Time Complexity**: O(M+N) where M = old size, N = new size

**ACL Category**: `@json`, `@write`, `@slow`

---

### 4.8 Debug Operations

#### JSON.DEBUG

**Syntax**: `JSON.DEBUG subcommand key [path]`

**Subcommands**:
- `MEMORY key [path]`: Returns memory usage in bytes
- `HELP`: Returns help text

**MEMORY Behavior**:
- Returns approximate memory usage of JSON value(s) at path
- Includes overhead for pointers, metadata, etc.

**Return**:
- `MEMORY`: Integer or array of integers
- `HELP`: Array of bulk strings (help lines)

**Examples**:
```redis
JSON.DEBUG MEMORY doc $             # [512]
JSON.DEBUG MEMORY doc $.arr         # [128]
JSON.DEBUG HELP                     # ["MEMORY <key> [path]", ...]
```

**Time Complexity**: O(N) where N = value size (for MEMORY)

**ACL Category**: `@json`, `@read`, `@slow`

---

## 5. Gap Analysis

### What's Already Implemented
✅ RESP2/RESP3 protocol support
✅ Tagged union storage model
✅ RDB/AOF persistence framework
✅ Expiration system
✅ ACL command routing
✅ Memory tracking infrastructure

### What Must Be Built
❌ JSON parser (std.json can be used as starting point)
❌ JSONPath parser (RFC 9535 grammar)
❌ JSONPath evaluator (query execution)
❌ JSON value representation (tree structure)
❌ 26 command handlers
❌ RDB encoding for JSON type
❌ Integration tests for all commands

### Complexity Estimate

| Component | LOC | Complexity | Dependencies |
|-----------|-----|------------|--------------|
| `json_value.zig` | 400-500 | Medium | std.json, std.mem |
| `jsonpath.zig` | 600-800 | High | Parsing, regex |
| `json.zig` (commands) | 1800-2200 | Medium | json_value, jsonpath |
| RDB/AOF integration | 200-300 | Low | persistence.zig, aof.zig |
| Tests | 500-700 | Medium | All above |
| **Total** | **3500-4500** | **High** | - |

---

## 6. Phased Implementation Plan (6 Iterations)

### Iteration 169 — Foundation & Core Operations (4 commands)

**Goal**: JSON value representation + JSONPath parser + basic SET/GET/DEL/TYPE

**Scope**:
1. Create `src/storage/json_value.zig`:
   - `JsonNode` tagged union (null/bool/number/string/array/object)
   - `parse()` — JSON string → tree (leverage std.json.parseFromSlice)
   - `stringify()` — tree → JSON string
   - `clone()` — deep copy
   - `deinit()` — recursive cleanup
   - `typeName()` — type introspection
2. Create `src/storage/jsonpath.zig`:
   - `JsonPath` struct with segments
   - `parse()` — query string → segments (support basic syntax: `$`, `$.a`, `.a`, `$[0]`, `$.*`, `$..a`)
   - `evaluate()` — segments + tree → matching nodes
   - `canCreate()` — check if path is root
3. Add `json: JsonValue` to `Value` union in `memory.zig`
4. Create `src/commands/json.zig`:
   - `cmdJsonSet()` — JSON.SET with NX/XX options
   - `cmdJsonGet()` — JSON.GET with single path (defer formatting options)
   - `cmdJsonDel()` — JSON.DEL
   - `cmdJsonType()` — JSON.TYPE
5. Register commands in `strings.zig` dispatcher
6. Unit tests: 20+ tests (parser, evaluator, commands)
7. Integration tests: 10+ RESP protocol tests

**Deliverables**:
- JSON storage foundation ✅
- JSONPath parser (basic syntax) ✅
- 4 core commands working ✅
- Zero memory leaks ✅

**Estimated LOC**: 1200-1500 (foundation + 4 commands)

**Risk**: JSONPath parser complexity → Mitigate by starting with basic syntax only

---

### Iteration 170 — Extended Core + Numeric Operations (4 commands)

**Goal**: Multi-path support + MGET/MSET + numeric operations

**Scope**:
1. Enhance `cmdJsonGet()`:
   - Multiple path support (returns object with paths as keys)
   - Formatting options: INDENT, NEWLINE, SPACE
2. Implement `cmdJsonMget()` — JSON.MGET key [key ...] path
3. Implement `cmdJsonMset()` — JSON.MSET key path value [...]
4. Implement `cmdJsonNumincrby()` — JSON.NUMINCRBY key path value
5. Implement `cmdJsonNummultby()` — JSON.NUMMULTBY key path value
6. Enhance JSONPath evaluator:
   - Array slicing: `$[0:5]`, `$[-2:]`
   - Negative indices: `$[-1]`
7. Unit tests: 15+ tests (multi-path, numeric ops)
8. Integration tests: 12+ RESP protocol tests

**Deliverables**:
- Multi-path GET ✅
- Atomic multi-SET ✅
- Numeric operations ✅
- Advanced array indexing ✅

**Estimated LOC**: 500-600 (4 commands + enhancements)

---

### Iteration 171 — String, Boolean, Object Operations (6 commands)

**Goal**: String/boolean manipulation + object introspection

**Scope**:
1. Implement `cmdJsonStrappend()` — JSON.STRAPPEND key [path] value
2. Implement `cmdJsonStrlen()` — JSON.STRLEN key [path]
3. Implement `cmdJsonToggle()` — JSON.TOGGLE key path
4. Implement `cmdJsonClear()` — JSON.CLEAR key [path]
5. Implement `cmdJsonObjkeys()` — JSON.OBJKEYS key [path]
6. Implement `cmdJsonObjlen()` — JSON.OBJLEN key [path]
7. Unit tests: 18+ tests (string/bool/object ops)
8. Integration tests: 15+ RESP protocol tests

**Deliverables**:
- String manipulation ✅
- Boolean toggle ✅
- Object introspection ✅

**Estimated LOC**: 600-700 (6 commands)

---

### Iteration 172 — Array Operations Part 1 (4 commands)

**Goal**: Array manipulation (append/insert/index/length)

**Scope**:
1. Implement `cmdJsonArrappend()` — JSON.ARRAPPEND key [path] value [value ...]
2. Implement `cmdJsonArrinsert()` — JSON.ARRINSERT key path index value [value ...]
3. Implement `cmdJsonArrindex()` — JSON.ARRINDEX key path value [start [stop]]
4. Implement `cmdJsonArrlen()` — JSON.ARRLEN key [path]
5. Unit tests: 16+ tests (array operations)
6. Integration tests: 12+ RESP protocol tests

**Deliverables**:
- Array append/insert ✅
- Array search ✅
- Array length ✅

**Estimated LOC**: 500-600 (4 commands)

---

### Iteration 173 — Array Operations Part 2 + Advanced (4 commands)

**Goal**: Array pop/trim + RESP + MERGE

**Scope**:
1. Implement `cmdJsonArrpop()` — JSON.ARRPOP key [path [index]]
2. Implement `cmdJsonArrtrim()` — JSON.ARRTRIM key path start stop
3. Implement `cmdJsonResp()` — JSON.RESP key [path]
4. Implement `cmdJsonMerge()` — JSON.MERGE key path value (RFC 7386)
5. Unit tests: 16+ tests (pop/trim/resp/merge)
6. Integration tests: 12+ RESP protocol tests

**Deliverables**:
- Array pop/trim ✅
- RESP native type conversion ✅
- JSON Merge Patch ✅

**Estimated LOC**: 500-600 (4 commands)

---

### Iteration 174 — Debug, Persistence, Polish (3 commands + integration)

**Goal**: Debug commands + RDB/AOF + comprehensive validation

**Scope**:
1. Implement `cmdJsonDebug()` — JSON.DEBUG MEMORY/HELP
2. Implement `cmdJsonForget()` — alias for JSON.DEL
3. RDB integration:
   - Add JSON type encoding (0x0F) in `persistence.zig`
   - Serialize JSON as string via `stringify()`
   - Deserialize via `parse()`
4. AOF integration:
   - Verify JSON commands replicate correctly
5. Memory profiling:
   - Implement `calculateMemoryUsage()` for JSON trees
6. JSONPath enhancements:
   - Recursive descent optimization (`$..a`)
   - Filter expressions (basic: `$[?(@.x>5)]`)
7. Comprehensive integration testing:
   - Cross-command scenarios (SET + GET + MERGE)
   - Large documents (10KB+ JSON)
   - Persistence round-trip (RDB save/load)
   - Edge cases (deeply nested, empty arrays/objects)
8. Performance validation:
   - Benchmark vs redis-server (JSON.SET/GET throughput)
   - Memory efficiency check (comparable to Redis)

**Deliverables**:
- Debug commands ✅
- RDB/AOF persistence ✅
- Filter expressions (basic) ✅
- Comprehensive validation ✅

**Estimated LOC**: 400-500 (3 commands + persistence + polish)

---

## 7. Testing Strategy

### Unit Tests (per iteration)
- **Parsing tests**: Valid/invalid JSON, valid/invalid JSONPath
- **Evaluation tests**: All path types (child/wildcard/recursive/array/filter)
- **Command tests**: All argument combinations, error conditions
- **Memory tests**: No leaks detected by `std.testing.allocator`

**Target**: 90+ unit tests across 6 iterations

### Integration Tests
- **RESP protocol tests**: Exact byte-by-byte response matching
- **Multi-command scenarios**: SET → GET → MERGE → GET
- **Persistence tests**: RDB save/load, AOF replay
- **Edge cases**: Empty documents, deeply nested (100+ levels), large arrays (10K+ elements)
- **Error cases**: Invalid JSON, invalid path, type mismatches

**Target**: 70+ integration tests across 6 iterations

### Redis Compatibility Validation
- **Differential testing**: Compare Zoltraak vs Redis for every command
- **Test corpus**: Official Redis JSON test suite (if available)
- **Compatibility target**: >= 95% byte-identical responses

### Performance Benchmarks
- **redis-benchmark**: JSON.SET/GET throughput
- **Target**: >= 70% of Redis throughput (Phase 12 baseline)
- **Memory**: <= 1.2x Redis memory for equivalent JSON documents

---

## 8. Implementation Notes

### 8.1 Zig-Specific Considerations

**Memory Management**:
- Use `std.mem.Allocator` for all JSON tree allocations
- JSON trees owned by `JsonValue.allocator` (long-lived data)
- Command handlers use arena allocator for temporary data
- Always use `errdefer` after allocations for cleanup

**Error Handling**:
- Define specific error types: `JsonParseError`, `JsonPathError`, `JsonTypeError`
- Avoid `anyerror` — be explicit
- Return errors, don't panic (except for programming errors)

**JSON Parsing**:
- Leverage `std.json.parseFromSlice()` for initial parse
- Convert `std.json.Value` to `JsonNode` tree (our internal representation)
- Why custom representation? Enables efficient in-place updates without full re-parse

**String Handling**:
- JSON strings are UTF-8 (Redis requirement)
- Use `std.unicode.utf8ValidateSlice()` for validation
- Object keys stored as owned strings (allocated, must be freed)

**ArrayList vs ArrayListUnmanaged**:
- Use `std.ArrayList(T)` for convenience (embedded allocator) in Zig 0.15.2
- All mutation methods take allocator as first argument in 0.15.2

### 8.2 JSONPath Implementation Strategy

**Parsing Approach**:
1. Tokenize query string (split on `.`, `[`, `]`, etc.)
2. Identify segment types (child/wildcard/recursive/array/filter)
3. Build `PathSegment` array
4. Validate syntax (balanced brackets, valid operators)

**Evaluation Approach**:
1. Start with `[root_node]` as current results
2. For each segment:
   - Apply segment to all nodes in current results
   - Collect matching nodes into new results list
3. Return final results list

**Optimization Tips**:
- **Cache parsed paths**: Store parsed `JsonPath` in command context (future optimization)
- **Lazy evaluation**: Don't materialize full result set if only need count/existence
- **Recursive descent**: Use iterative DFS instead of recursive (avoid stack overflow)

**Simplified Filter Expressions (Phase 12)**:
- Only support basic comparisons: `==`, `!=`, `<`, `>`, `<=`, `>=`
- Field access: `@.field` (current node property)
- Literals: `5`, `"string"`, `true`, `false`, `null`
- No logical operators (AND/OR) in Phase 12 — defer to Phase 13

### 8.3 RESP3 Considerations

**Native Type Mapping**:
- JSON array → RESP3 array
- JSON object → RESP3 map (when HELLO 3 is active)
- JSON null → RESP3 null
- JSON boolean → RESP3 boolean (HELLO 3) or simple string (HELLO 2)
- JSON number → Bulk string (Redis behavior: no native number type in RESP2/RESP3 for JSON)

**JSON.GET with RESP3**:
- Single path: Returns array of values
- Multiple paths: Returns map with paths as keys
- Check `client.protocol_version` in command handler

### 8.4 Persistence Considerations

**RDB Encoding**:
- Type byte: `0x0F` (new JSON type)
- Length-prefixed JSON string (use existing varint encoding)
- Expiration: Use existing TTL encoding
- Estimate: ~30 LOC addition to `persistence.zig`

**AOF Encoding**:
- Replicate commands exactly as received
- No special handling needed (JSON.SET writes JSON string as bulk string)
- Verify with round-trip test (SET → save → load → GET)

**Serialization Format**:
- Use `stringify()` to convert tree → JSON string
- Use `parse()` to convert JSON string → tree
- Trade-off: Not most compact (compared to binary), but human-readable and compatible with existing JSON tools

### 8.5 Extension Points for Phase 13

**Search Integration** (Future):
- JSON fields can be indexed by FT.CREATE with JSON type
- `JsonNode` tree enables efficient path evaluation without re-parsing
- Filter expressions in JSONPath will align with FT.SEARCH filter syntax

**Vector Embeddings** (Future):
- JSON arrays of numbers can be interpreted as vectors
- `JSON.GET $.embedding` → numeric array → vector search input

---

## 9. Known Edge Cases & Limitations

### Phase 12 Limitations (Acceptable)
❌ **No regex filters**: `$[?(@.name =~ ".*pattern.*")]` → defer to Phase 13
❌ **No script operators**: `@.price + @.tax` → defer to Phase 13
❌ **No function calls**: `length(@.arr)`, `min(@.numbers)` → defer to Phase 13
❌ **No union operator**: `$['a','b']` → can be emulated with multiple GET calls

### Edge Cases to Handle
✅ **Deeply nested documents**: Max depth = 1000 (prevent stack overflow)
✅ **Large arrays**: Support 1M+ elements (use iterative algorithms)
✅ **Unicode in keys**: Full UTF-8 support (validate with `utf8ValidateSlice`)
✅ **Numeric precision**: All numbers as `f64` (matches Redis behavior)
✅ **Empty paths**: `""` returns error, `$` is root
✅ **Path doesn't exist**: Commands return null/empty array (no error)

### Redis Compatibility Notes
- **Number representation**: Redis stores as `f64`, not int vs float distinction
- **Key order in objects**: Undefined order (implementation can choose)
- **Path syntax**: Both `.path` and `$..path` must work
- **Legacy vs JSONPath return values**: Different structures (document clearly)

---

## 10. Acceptance Criteria (Phase 12 Complete)

### Functionality
✅ All 26 JSON.* commands implemented
✅ Both legacy (`.path`) and JSONPath (`$..path`) syntax work
✅ All JSONPath features documented as supported work correctly
✅ RESP2/RESP3 responses match Redis exactly
✅ RDB save/load preserves JSON values
✅ AOF replay correctly reconstructs JSON state

### Quality
✅ Zero memory leaks (validated by `std.testing.allocator`)
✅ All unit tests pass (90+ tests)
✅ All integration tests pass (70+ tests)
✅ No compiler warnings (Zig 0.15.2)
✅ Code review approved by `zig-quality-reviewer` + `code-reviewer`

### Performance
✅ JSON.SET throughput >= 70% of Redis
✅ JSON.GET throughput >= 70% of Redis
✅ Memory usage <= 1.2x Redis for equivalent documents
✅ No performance regression on existing commands

### Compatibility
✅ Redis compatibility validation >= 95% (byte-identical responses)
✅ All Redis JSON examples from documentation work identically
✅ Cross-command scenarios match Redis behavior (SET → MERGE → GET)

### Documentation
✅ All 26 commands documented in README.md
✅ JSONPath syntax guide in docs/
✅ CLAUDE.md updated with Phase 12 completion status
✅ Migration notes for users (if any breaking changes)

---

## 11. Dependencies & Risks

### Dependencies
✅ **No blocking dependencies**: Phase 12 is independent
⚠ **Optional**: zuda JSON parser (check if available after Iteration 169)
⚠ **Follows**: Phase 11 (Functions) — complete ✅
⚠ **Enables**: Phase 13 (Search) — JSON indexing depends on this

### Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| JSONPath parser complexity | High | Medium | Start with basic syntax, iterate |
| Performance below target | Medium | Medium | Profile early, optimize hot paths |
| Memory leaks in tree operations | Medium | High | Use `std.testing.allocator`, extensive tests |
| Incompatibility with Redis edge cases | Low | High | Differential testing vs Redis, extensive integration tests |
| Recursive depth stack overflow | Low | Medium | Use iterative algorithms, depth limit |

### Assumptions
- `std.json` parser is sufficient for initial JSON parsing (can be replaced with custom parser if needed)
- JSONPath filter expressions (basic) can be implemented with simple predicate evaluation
- RDB/AOF frameworks support new type encoding without major refactor (confirmed by reviewing code)
- Existing test infrastructure (unit-test-writer, integration-test-orchestrator) works for JSON commands

---

## 12. Success Metrics

### Quantitative
- **26/26 commands** implemented ✅
- **90+ unit tests** passing ✅
- **70+ integration tests** passing ✅
- **>= 95% compatibility** with Redis (differential testing) ✅
- **>= 70% throughput** of Redis (redis-benchmark) ✅
- **<= 1.2x memory** usage vs Redis ✅
- **0 memory leaks** (std.testing.allocator) ✅

### Qualitative
- Code reviewed and approved by quality reviewers ✅
- All examples from Redis documentation work identically ✅
- No open bugs related to Phase 12 ✅
- Community validation (if deployed) shows no issues ✅

---

## 13. References

### Primary Documentation
1. [Redis JSON Commands](https://redis.io/docs/latest/commands/?group=json)
2. [Redis JSON Data Type](https://redis.io/docs/latest/develop/data-types/json/)
3. [JSONPath Syntax](https://redis.io/docs/latest/develop/data-types/json/path/)
4. [RFC 9535 - JSONPath](https://datatracker.ietf.org/doc/rfc9535/)
5. [RFC 7386 - JSON Merge Patch](https://datatracker.ietf.org/doc/html/rfc7386)

### Implementation References
- [redis/redis: JSON type implementation](https://github.com/redis/redis) (if available in source)
- [redis/redis-py: JSON client examples](https://github.com/redis/redis-py/blob/master/redis/commands/json/commands.py)
- [Goessner's JSONPath article](https://goessner.net/articles/JsonPath/) (legacy syntax)

### Related Zoltraak Docs
- `/Users/fn/codespace/zoltraak/docs/PRD.md` — Phase 12 requirements
- `/Users/fn/codespace/zoltraak/docs/milestones.md` — Iteration tracking
- `/Users/fn/codespace/zoltraak/CLAUDE.md` — Development cycle protocol
- `/Users/fn/codespace/zoltraak/src/storage/memory.zig` — Storage layer reference
- `/Users/fn/codespace/zoltraak/src/protocol/parser.zig` — RESP parser reference

---

## 14. Appendix: Command Quick Reference

| Command | Syntax | Returns | Complexity |
|---------|--------|---------|------------|
| JSON.SET | `key path value [NX\|XX]` | OK / (nil) / error | O(M+N) |
| JSON.GET | `key [formatting] [path ...]` | Bulk string | O(N) |
| JSON.MGET | `key [...] path` | Array | O(N×K) |
| JSON.MSET | `key path value [...]` | OK | O(N×K) |
| JSON.DEL | `key [path]` | Integer | O(N) |
| JSON.FORGET | `key [path]` | Integer | O(N) |
| JSON.TYPE | `key [path]` | Bulk string / Array | O(1) |
| JSON.NUMINCRBY | `key path value` | Bulk string / Array | O(1) |
| JSON.NUMMULTBY | `key path value` | Bulk string / Array | O(1) |
| JSON.STRAPPEND | `key [path] value` | Integer / Array | O(N) |
| JSON.STRLEN | `key [path]` | Integer / Array | O(1) |
| JSON.TOGGLE | `key path` | Integer / Array | O(1) |
| JSON.CLEAR | `key [path]` | Integer | O(N) |
| JSON.ARRAPPEND | `key [path] value [...]` | Integer / Array | O(1) |
| JSON.ARRINDEX | `key path value [start [stop]]` | Integer / Array | O(N) |
| JSON.ARRINSERT | `key path index value [...]` | Integer / Array | O(N) |
| JSON.ARRLEN | `key [path]` | Integer / Array | O(1) |
| JSON.ARRPOP | `key [path [index]]` | Bulk string / Array | O(N) |
| JSON.ARRTRIM | `key path start stop` | Integer / Array | O(N) |
| JSON.OBJKEYS | `key [path]` | Array / Array of arrays | O(N) |
| JSON.OBJLEN | `key [path]` | Integer / Array | O(1) |
| JSON.RESP | `key [path]` | Mixed RESP types | O(N) |
| JSON.MERGE | `key path value` | OK | O(M+N) |
| JSON.DEBUG | `MEMORY key [path]` | Integer / Array | O(N) |
| JSON.DEBUG | `HELP` | Array | O(1) |

**Legend**:
- N = size of JSON value(s)
- M = size of old value (for replacements)
- K = number of keys (for multi-key operations)

---

## 15. Next Steps (Post-Phase 12)

### Phase 13 Dependencies
Phase 13 (Search Engine) will build on JSON foundation:
- Full-text indexing of JSON documents
- JSONPath filter expressions in FT.SEARCH queries
- JSON field types in FT.CREATE (TEXT, NUMERIC, TAG, GEO, VECTOR)

### Potential Optimizations (Future)
- **JIT-compiled JSONPath queries**: Cache parsed paths, compile to bytecode
- **Memory-mapped JSON**: For large documents, avoid full in-memory tree
- **Compressed storage**: Use zstd for large JSON blobs in RDB
- **SIMD acceleration**: Use SIMD for JSON parsing (Zig 0.16+ has better SIMD support)

---

**End of Specification**

This document provides a comprehensive blueprint for implementing Phase 12 (JSON Data Type) across 6 iterations (169-174). All command specifications, architecture decisions, and acceptance criteria are defined. The implementation team can proceed with Iteration 169 immediately.

**Estimated Total Effort**: 6 iterations × 1 day/iteration = 6 days (autonomous execution)
**Complexity**: High (JSONPath parser) but manageable with incremental approach
**Risk Level**: Medium — mitigated by extensive testing and differential validation

**Approval Status**: Ready for implementation ✅

---

**Document Metadata**:
- Version: 1.0
- Last Updated: 2026-04-06
- Author: redis-spec-analyzer (Claude Code Agent)
- Review Status: Initial draft for team review
