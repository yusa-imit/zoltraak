const std = @import("std");
const Storage = @import("../storage/memory.zig").Storage;
const parser = @import("../protocol/parser.zig");
const RespValue = parser.RespValue;
const search_mod = @import("../storage/search.zig");
const search_agg = @import("search_aggregate.zig");

/// FT.CREATE index_name ON HASH|JSON [PREFIX count prefix [prefix ...]] SCHEMA field_name field_type [options ...]
///
/// Creates a search index on HASH or JSON keys.
///
/// Arguments:
///   args[0] = index_name
///   args[1] = "ON"
///   args[2] = "HASH" or "JSON"
///   args[3..] = optional PREFIX clause + SCHEMA clause
///
/// Returns:
///   +OK on success
///   Error if index already exists or syntax error
pub fn cmdFtCreate(storage: *Storage, _: std.mem.Allocator, args: []const []const u8) !RespValue {
    if (args.len < 4) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.CREATE' command" };
    }

    const index_name = args[0];

    // Validate "ON" keyword
    if (!std.mem.eql(u8, args[1], "ON")) {
        return RespValue{ .error_string = "ERR syntax error, expected ON after index name" };
    }

    // Parse index type (HASH or JSON)
    const index_on = search_mod.IndexOn.fromString(args[2]) catch {
        return RespValue{ .error_string = "ERR syntax error, expected HASH or JSON after ON" };
    };

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Create index in search store
    storage.search.createIndex(index_name, index_on) catch |err| {
        if (err == error.IndexAlreadyExists) {
            return RespValue{ .error_string = "ERR Index already exists" };
        }
        return err;
    };

    // Get mutable index for further configuration
    var index = storage.search.getIndex(index_name).?;

    // Parse optional PREFIX and SCHEMA clauses
    var i: usize = 3;
    var in_schema = false;

    while (i < args.len) {
        const keyword = args[i];

        if (std.mem.eql(u8, keyword, "PREFIX")) {
            if (in_schema) {
                return RespValue{ .error_string = "ERR PREFIX must come before SCHEMA" };
            }
            i += 1;
            if (i >= args.len) {
                return RespValue{ .error_string = "ERR PREFIX requires count argument" };
            }

            const count = std.fmt.parseInt(usize, args[i], 10) catch {
                return RespValue{ .error_string = "ERR PREFIX count must be a valid integer" };
            };

            i += 1;
            if (i + count > args.len) {
                return RespValue{ .error_string = "ERR not enough prefix arguments" };
            }

            // For now, only support single prefix
            if (count != 1) {
                return RespValue{ .error_string = "ERR only single PREFIX supported in this version" };
            }

            const prefix = args[i];
            try index.setPrefix(prefix);
            i += 1;
        } else if (std.mem.eql(u8, keyword, "SCHEMA")) {
            in_schema = true;
            i += 1;

            // Parse field definitions
            while (i < args.len) {
                if (i + 1 >= args.len) {
                    return RespValue{ .error_string = "ERR SCHEMA field requires type" };
                }

                const field_name = args[i];
                const field_type_str = args[i + 1];
                i += 2;

                const field_type = search_mod.FieldType.fromString(field_type_str) catch {
                    return RespValue{ .error_string = "ERR invalid field type" };
                };

                var field = try search_mod.FieldSchema.init(storage.allocator, field_name, field_type);
                errdefer field.deinit();

                // Parse field options (SORTABLE, NOINDEX, NOSTEM, AS, etc.)
                while (i < args.len) {
                    const option = args[i];

                    if (std.mem.eql(u8, option, "SORTABLE")) {
                        field.sortable = true;
                        i += 1;
                    } else if (std.mem.eql(u8, option, "NOINDEX")) {
                        field.noindex = true;
                        i += 1;
                    } else if (std.mem.eql(u8, option, "NOSTEM")) {
                        field.nostem = true;
                        i += 1;
                    } else if (std.mem.eql(u8, option, "AS")) {
                        i += 1;
                        if (i >= args.len) {
                            return RespValue{ .error_string = "ERR AS requires alias argument" };
                        }
                        const alias = args[i];
                        if (field.alias) |old| {
                            storage.allocator.free(old);
                        }
                        field.alias = try storage.allocator.dupe(u8, alias);
                        i += 1;
                    } else {
                        // Not a field option, must be next field or end
                        break;
                    }
                }

                try index.addField(field);
            }
        } else {
            return RespValue{ .error_string = "ERR syntax error, expected PREFIX or SCHEMA" };
        }
    }

    return RespValue{ .simple_string = "OK" };
}

/// FT._LIST
///
/// Returns array of all index names.
///
/// Arguments:
///   None (args is empty)
///
/// Returns:
///   Array of bulk strings (index names)
pub fn cmdFtList(storage: *Storage, arena: std.mem.Allocator, args: []const []const u8) !RespValue {
    if (args.len != 0) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT._LIST' command" };
    }

    storage.mutex.lock();
    defer storage.mutex.unlock();

    const names = try storage.search.listIndices(arena);
    defer {
        for (names) |name| {
            arena.free(name);
        }
        arena.free(names);
    }

    var array = try std.ArrayList(RespValue).initCapacity(arena, names.len);
    errdefer array.deinit(arena);

    for (names) |name| {
        const name_copy = try arena.dupe(u8, name);
        try array.append(arena, RespValue{ .bulk_string = name_copy });
    }

    return RespValue{ .array = try array.toOwnedSlice(arena) };
}

/// FT.DROPINDEX index_name [DD]
///
/// Drops an index. If DD flag is given, also deletes all documents.
///
/// Arguments:
///   args[0] = index_name
///   args[1] = optional "DD" flag
///
/// Returns:
///   +OK on success
///   Error if index doesn't exist
pub fn cmdFtDropindex(storage: *Storage, arena: std.mem.Allocator, args: []const []const u8) !RespValue {
    _ = arena;

    if (args.len < 1 or args.len > 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.DROPINDEX' command" };
    }

    const index_name = args[0];

    // Check for DD flag
    var delete_docs = false;
    if (args.len == 2) {
        if (!std.mem.eql(u8, args[1], "DD")) {
            return RespValue{ .error_string = "ERR syntax error, expected DD flag" };
        }
        delete_docs = true;
    }

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Drop index
    storage.search.dropIndex(index_name) catch |err| {
        if (err == error.IndexNotFound) {
            return RespValue{ .error_string = "ERR Unknown index name" };
        }
        return err;
    };

    // TODO: If DD flag set, also delete documents matching prefix
    if (delete_docs) {
        // Stub for now - would iterate storage.data and delete matching keys
    }

    return RespValue{ .simple_string = "OK" };
}

/// FT.INFO index_name
///
/// Returns metadata about an index.
///
/// Arguments:
///   args[0] = index_name
///
/// Returns:
///   Flat array of key-value pairs (index_name, index_options, index_definition, attributes, statistics)
///   Error if index doesn't exist
pub fn cmdFtInfo(storage: *Storage, arena: std.mem.Allocator, args: []const []const u8) !RespValue {
    if (args.len != 1) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.INFO' command" };
    }

    const index_name = args[0];

    storage.mutex.lock();
    defer storage.mutex.unlock();

    const index = storage.search.getIndex(index_name) orelse {
        return RespValue{ .error_string = "ERR Unknown index name" };
    };

    // Build flat array of key-value pairs
    var array = try std.ArrayList(RespValue).initCapacity(arena, 0);
    errdefer array.deinit(arena);

    // index_name
    try array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "index_name") });
    try array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, index.name) });

    // index_options (empty for now)
    try array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "index_options") });
    var opts = try std.ArrayList(RespValue).initCapacity(arena, 0);
    try array.append(arena, RespValue{ .array = try opts.toOwnedSlice(arena) });

    // index_definition
    try array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "index_definition") });
    var def = try std.ArrayList(RespValue).initCapacity(arena, 0);
    errdefer def.deinit(arena);

    try def.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "key_type") });
    const key_type = switch (index.index_on) {
        .hash => "HASH",
        .json => "JSON",
    };
    try def.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, key_type) });

    if (index.prefix) |prefix| {
        try def.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "prefixes") });
        var prefixes = try std.ArrayList(RespValue).initCapacity(arena, 1);
        try prefixes.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, prefix) });
        try def.append(arena, RespValue{ .array = try prefixes.toOwnedSlice(arena) });
    }

    try def.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "default_score") });
    try def.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "1.0") });

    try array.append(arena, RespValue{ .array = try def.toOwnedSlice(arena) });

    // attributes (field schemas)
    try array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "attributes") });
    var attrs = try std.ArrayList(RespValue).initCapacity(arena, 0);
    errdefer attrs.deinit(arena);

    for (index.fields.items) |field| {
        var field_arr = try std.ArrayList(RespValue).initCapacity(arena, 0);
        errdefer field_arr.deinit(arena);

        try field_arr.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "identifier") });
        try field_arr.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, field.name) });

        try field_arr.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "attribute") });
        const attr_name = if (field.alias) |alias| alias else field.name;
        try field_arr.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, attr_name) });

        try field_arr.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "type") });
        const type_str = switch (field.field_type) {
            .text => "TEXT",
            .tag => "TAG",
            .numeric => "NUMERIC",
            .geo => "GEO",
            .vector => "VECTOR",
            .geoshape => "GEOSHAPE",
        };
        try field_arr.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, type_str) });

        // Add flags
        if (field.sortable) {
            try field_arr.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "SORTABLE") });
        }
        if (field.noindex) {
            try field_arr.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "NOINDEX") });
        }
        if (field.nostem) {
            try field_arr.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "NOSTEM") });
        }

        try attrs.append(arena, RespValue{ .array = try field_arr.toOwnedSlice(arena) });
    }

    try array.append(arena, RespValue{ .array = try attrs.toOwnedSlice(arena) });

    // num_docs, max_doc_id, num_terms, num_records, inverted_sz_mb, percent_indexed
    try array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "num_docs") });
    try array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "0") });

    try array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "max_doc_id") });
    try array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "0") });

    try array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "num_terms") });
    try array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "0") });

    try array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "num_records") });
    try array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "0") });

    try array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "inverted_sz_mb") });
    try array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "0") });

    try array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "percent_indexed") });
    try array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, "1") });

    return RespValue{ .array = try array.toOwnedSlice(arena) };
}

/// FT.ALTER index_name SCHEMA ADD field_name field_type [options]
///
/// Adds a new field to an existing index.
///
/// Arguments:
///   args[0] = index_name
///   args[1] = "SCHEMA"
///   args[2] = "ADD"
///   args[3] = field_name
///   args[4] = field_type
///   args[5..] = optional field options (SORTABLE, NOINDEX, NOSTEM, AS, etc.)
///
/// Returns:
///   +OK on success
///   "ERR Unknown index name" if index doesn't exist
///   "ERR wrong number of arguments" if too few arguments
///   "ERR syntax error" if SCHEMA or ADD keyword missing
///   "ERR invalid field type" if field_type is not valid
pub fn cmdFtAlter(storage: *Storage, arena: std.mem.Allocator, args: []const []const u8) !RespValue {
    _ = arena;

    if (args.len < 5) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.ALTER' command" };
    }

    const index_name = args[0];

    // Validate "SCHEMA" keyword
    if (!std.mem.eql(u8, args[1], "SCHEMA")) {
        return RespValue{ .error_string = "ERR syntax error, expected SCHEMA after index name" };
    }

    // Validate "ADD" keyword
    if (!std.mem.eql(u8, args[2], "ADD")) {
        return RespValue{ .error_string = "ERR syntax error, expected ADD after SCHEMA" };
    }

    const field_name = args[3];
    const field_type_str = args[4];

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Get mutable index
    var index = storage.search.getIndex(index_name) orelse {
        return RespValue{ .error_string = "ERR Unknown index name" };
    };

    // Parse field type
    const field_type = search_mod.FieldType.fromString(field_type_str) catch {
        return RespValue{ .error_string = "ERR invalid field type" };
    };

    // Create new field
    var field = try search_mod.FieldSchema.init(storage.allocator, field_name, field_type);
    errdefer field.deinit();

    // Parse field options (SORTABLE, NOINDEX, NOSTEM, AS, etc.)
    var i: usize = 5;
    while (i < args.len) {
        const option = args[i];

        if (std.mem.eql(u8, option, "SORTABLE")) {
            field.sortable = true;
            i += 1;
        } else if (std.mem.eql(u8, option, "NOINDEX")) {
            field.noindex = true;
            i += 1;
        } else if (std.mem.eql(u8, option, "NOSTEM")) {
            field.nostem = true;
            i += 1;
        } else if (std.mem.eql(u8, option, "AS")) {
            i += 1;
            if (i >= args.len) {
                return RespValue{ .error_string = "ERR AS requires alias argument" };
            }
            const alias = args[i];
            if (field.alias) |old| {
                storage.allocator.free(old);
            }
            field.alias = try storage.allocator.dupe(u8, alias);
            i += 1;
        } else {
            // Unknown option
            break;
        }
    }

    // Add field to index
    try index.addField(field);

    return RespValue{ .simple_string = "OK" };
}

/// FT.EXPLAIN index query [DIALECT dialect]
///
/// Returns the execution plan for a query without executing it.
///
/// This is a stub implementation that:
/// - Validates the index exists
/// - Parses DIALECT argument if present (stored but not used yet)
/// - Returns a simple execution plan showing the query as a TERM
///
/// Arguments:
///   args[0] = index_name
///   args[1] = query string
///   args[2..] = optional DIALECT argument
///
/// Returns:
///   Bulk string containing the execution plan
///   Error if index doesn't exist or wrong number of arguments
pub fn cmdFtExplain(storage: *Storage, arena: std.mem.Allocator, args: []const []const u8) !RespValue {
    if (args.len < 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.EXPLAIN' command" };
    }

    if (args.len > 4) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.EXPLAIN' command" };
    }

    const index_name = args[0];
    const query = args[1];

    // Parse optional DIALECT argument
    var dialect: u32 = 1; // Default dialect
    var i: usize = 2;
    while (i < args.len) {
        const keyword = args[i];

        if (std.mem.eql(u8, keyword, "DIALECT")) {
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
    if (storage.search.getIndex(index_name) == null) {
        return RespValue{ .error_string = "ERR Unknown index name" };
    }

    // Generate simple execution plan (stub)
    // For now, just return the query as a single TERM
    // Future iterations will add full query parsing
    // Note: dialect parsed but not used in this stub implementation

    // Build execution plan string
    var plan = try std.ArrayList(u8).initCapacity(arena, 100);
    errdefer plan.deinit(arena);

    try plan.appendSlice(arena, "TERM {\n");
    try plan.appendSlice(arena, "  ");
    try plan.appendSlice(arena, query);
    try plan.appendSlice(arena, "\n");
    try plan.appendSlice(arena, "}\n");

    return RespValue{ .bulk_string = try plan.toOwnedSlice(arena) };
}

/// FT.EXPLAINCLI index query [DIALECT dialect]
///
/// Returns the query execution plan with CLI-formatted output (ANSI color codes).
/// Similar to FT.EXPLAIN but returns an array of strings for CLI display.
///
/// Arguments:
///   args[0]: index_name
///   args[1]: query string
///   args[2-3]: optional DIALECT <dialect_version>
///
/// Returns:
///   Array of strings with color-coded query plan (stub: simple array representation)
///
/// Error:
///   "ERR Unknown index name" if index doesn't exist
///   "ERR wrong number of arguments" for invalid arity
///   "ERR DIALECT requires an integer argument" if DIALECT without value
///   "ERR DIALECT must be an integer" if non-numeric dialect
///   "ERR syntax error" for invalid arguments
pub fn cmdFtExplaincli(storage: *Storage, arena: std.mem.Allocator, args: []const []const u8) !RespValue {
    if (args.len < 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.EXPLAINCLI' command" };
    }

    if (args.len > 4) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.EXPLAINCLI' command" };
    }

    const index_name = args[0];
    const query = args[1];

    // Parse optional DIALECT argument
    var dialect: u32 = 1; // Default dialect
    var i: usize = 2;
    while (i < args.len) {
        const keyword = args[i];

        if (std.mem.eql(u8, keyword, "DIALECT")) {
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
    if (storage.search.getIndex(index_name) == null) {
        return RespValue{ .error_string = "ERR Unknown index name" };
    }

    // Generate CLI-formatted execution plan (stub)
    // FT.EXPLAINCLI returns an array of strings with ANSI color codes
    // For this stub implementation, we'll return a simple array representation
    // Future iterations will add full query parsing and color coding
    // Note: dialect parsed but not used in this stub implementation

    // Build CLI-formatted plan as array of strings
    var result_array = try std.ArrayList(RespValue).initCapacity(arena, 3);
    errdefer {
        for (result_array.items) |item| {
            if (item == .bulk_string) arena.free(item.bulk_string);
        }
        result_array.deinit(arena);
    }

    // Line 1: Opening with TERM keyword (color code stub)
    const line1 = try std.fmt.allocPrint(arena, "TERM {{", .{});
    try result_array.append(arena, RespValue{ .bulk_string = line1 });

    // Line 2: Query text indented
    const line2 = try std.fmt.allocPrint(arena, "  {s}", .{query});
    try result_array.append(arena, RespValue{ .bulk_string = line2 });

    // Line 3: Closing brace
    const line3 = try std.fmt.allocPrint(arena, "}}", .{});
    try result_array.append(arena, RespValue{ .bulk_string = line3 });

    return RespValue{ .array = try result_array.toOwnedSlice(arena) };
}

/// FT.SEARCH index query [NOCONTENT] [LIMIT offset count] [RETURN num field ...] [SORTBY field [ASC|DESC]]
///
/// Searches an index for documents matching the query.
///
/// Arguments:
///   args[0]: index_name
///   args[1]: query string
///   args[2..]: optional flags (NOCONTENT, LIMIT, RETURN, SORTBY)
///
/// Returns:
///   Array with [total_count, doc_id1, fields1, doc_id2, fields2, ...]
///   If NOCONTENT: [total_count, doc_id1, doc_id2, ...]
///
/// Error:
///   "ERR Unknown index name" if index doesn't exist
///   "ERR wrong number of arguments" if < 2 args
///   "ERR Syntax error" for invalid options
pub fn cmdFtSearch(storage: *Storage, arena: std.mem.Allocator, args: []const []const u8) !RespValue {
    if (args.len < 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.SEARCH' command" };
    }

    const index_name = args[0];
    const query = args[1];

    // Parse optional flags
    var nocontent = false;
    var limit_offset: usize = 0;
    var limit_count: usize = 10;
    var return_fields: ?[]const []const u8 = null;
    var sortby_field: ?[]const u8 = null;
    var sortby_desc = false;

    var i: usize = 2;
    while (i < args.len) {
        const flag = args[i];

        if (std.mem.eql(u8, flag, "NOCONTENT")) {
            nocontent = true;
            i += 1;
        } else if (std.mem.eql(u8, flag, "LIMIT")) {
            i += 1;
            if (i + 1 >= args.len) {
                return RespValue{ .error_string = "ERR LIMIT requires two integer arguments" };
            }

            limit_offset = std.fmt.parseInt(usize, args[i], 10) catch {
                return RespValue{ .error_string = "ERR LIMIT offset must be an integer" };
            };
            i += 1;

            limit_count = std.fmt.parseInt(usize, args[i], 10) catch {
                return RespValue{ .error_string = "ERR LIMIT count must be an integer" };
            };
            i += 1;
        } else if (std.mem.eql(u8, flag, "RETURN")) {
            i += 1;
            if (i >= args.len) {
                return RespValue{ .error_string = "ERR RETURN requires count followed by field names" };
            }

            const num_fields = std.fmt.parseInt(usize, args[i], 10) catch {
                return RespValue{ .error_string = "ERR RETURN count must be an integer" };
            };
            i += 1;

            if (num_fields == 0) {
                // RETURN 0 is equivalent to NOCONTENT
                nocontent = true;
            } else {
                if (i + num_fields > args.len) {
                    return RespValue{ .error_string = "ERR not enough field names for RETURN" };
                }

                return_fields = args[i .. i + num_fields];
                i += num_fields;
            }
        } else if (std.mem.eql(u8, flag, "SORTBY")) {
            i += 1;
            if (i >= args.len) {
                return RespValue{ .error_string = "ERR SORTBY requires field name" };
            }

            sortby_field = args[i];
            i += 1;

            // Check for optional ASC/DESC
            if (i < args.len) {
                if (std.mem.eql(u8, args[i], "DESC")) {
                    sortby_desc = true;
                    i += 1;
                } else if (std.mem.eql(u8, args[i], "ASC")) {
                    i += 1;
                }
            }
        } else {
            return RespValue{ .error_string = "ERR Syntax error" };
        }
    }

    storage.mutex.lock();
    defer storage.mutex.unlock();

    const index = storage.search.getIndex(index_name) orelse {
        return RespValue{ .error_string = "ERR Unknown index name" };
    };

    // Perform search
    var result = try index.search(
        storage,
        arena,
        query,
        limit_offset,
        limit_count,
        nocontent,
        return_fields,
        sortby_field,
        sortby_desc,
    );
    defer result.deinit();

    // Format response
    var array = try std.ArrayList(RespValue).initCapacity(arena, 0);
    errdefer array.deinit(arena);

    // First element: total count
    try array.append(arena, RespValue{ .integer = @intCast(result.total_count) });

    // Subsequent elements: document ID + fields (or just ID if NOCONTENT)
    for (result.documents) |*doc| {
        // Document ID
        const id_copy = try arena.dupe(u8, doc.id);
        try array.append(arena, RespValue{ .bulk_string = id_copy });

        if (!nocontent) {
            // Document fields as flat array
            var fields_arr = try std.ArrayList(RespValue).initCapacity(arena, 0);
            errdefer fields_arr.deinit(arena);

            var it = doc.fields.iterator();
            while (it.next()) |entry| {
                const field_name = try arena.dupe(u8, entry.key_ptr.*);
                const field_value = try arena.dupe(u8, entry.value_ptr.*);

                try fields_arr.append(arena, RespValue{ .bulk_string = field_name });
                try fields_arr.append(arena, RespValue{ .bulk_string = field_value });
            }

            try array.append(arena, RespValue{ .array = try fields_arr.toOwnedSlice(arena) });
        }
    }

    return RespValue{ .array = try array.toOwnedSlice(arena) };
}

/// FT.PROFILE index SEARCH|AGGREGATE [LIMITED] QUERY query [options...]
///
/// Profiles an FT.SEARCH or FT.AGGREGATE command and returns both the query results
/// and detailed performance metrics.
///
/// Arguments:
///   args[0] = index_name
///   args[1] = "SEARCH" or "AGGREGATE"
///   args[2] = optional "LIMITED" flag
///   args[next] = "QUERY"
///   args[next+1] = query string
///   args[next+2..] = optional options (NOCONTENT, LIMIT, DIALECT, etc.)
///
/// Returns:
///   2-element array:
///     1) Query results (identical to FT.SEARCH or FT.AGGREGATE)
///     2) Profile data array with metrics:
///        - "Total profile time" => milliseconds
///        - "Parsing time" => milliseconds
///        - "Pipeline creation time" => milliseconds
///        - "Warning" => warning message (empty if none)
///        - "Iterators profile" => iterator tree structure
///        - "Result processors profile" => processor metrics array
///
/// Errors:
///   "ERR wrong number of arguments for 'FT.PROFILE' command" if too few args
///   "ERR Unknown index name" if index doesn't exist
///   "ERR syntax error, expected SEARCH or AGGREGATE" if invalid query type
///   "ERR syntax error, expected QUERY keyword" if QUERY keyword missing
pub fn cmdFtProfile(storage: *Storage, allocator: std.mem.Allocator, args: []const []const u8) !RespValue {
    if (args.len < 4) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.PROFILE' command" };
    }

    const index_name = args[0];
    const query_type_str = args[1];

    // Validate query type (SEARCH or AGGREGATE)
    const is_search = std.mem.eql(u8, query_type_str, "SEARCH");
    const is_aggregate = std.mem.eql(u8, query_type_str, "AGGREGATE");

    if (!is_search and !is_aggregate) {
        return RespValue{ .error_string = "ERR syntax error, expected SEARCH or AGGREGATE" };
    }

    // Parse LIMITED flag if present (when set, omits "Child iterators" field from iterator tree to reduce output size)
    var query_start_idx: usize = 2;
    var limited = false;
    if (args.len > 2 and std.mem.eql(u8, args[2], "LIMITED")) {
        limited = true;
        query_start_idx = 3;
    }

    // Validate QUERY keyword
    if (query_start_idx >= args.len or !std.mem.eql(u8, args[query_start_idx], "QUERY")) {
        return RespValue{ .error_string = "ERR syntax error, expected QUERY keyword" };
    }

    if (query_start_idx + 1 >= args.len) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.PROFILE' command" };
    }

    // Verify index exists
    storage.mutex.lock();
    defer storage.mutex.unlock();

    if (storage.search.getIndex(index_name) == null) {
        return RespValue{ .error_string = "ERR Unknown index name" };
    }

    // Build arguments for FT.SEARCH or FT.AGGREGATE (skip "QUERY" keyword)
    const query_args = args[query_start_idx + 1..];

    // Time the query execution
    const start_time_ns = std.time.nanoTimestamp();

    // Execute query (delegates to FT.SEARCH or FT.AGGREGATE based on query type)
    const query_result = if (is_search)
        try cmdFtSearch(storage, allocator, query_args)
    else
        try search_agg.cmdFtAggregate(storage, allocator, query_args);

    const end_time_ns = std.time.nanoTimestamp();
    const total_time_ms = @as(f64, @floatFromInt(end_time_ns - start_time_ns)) / 1_000_000.0;

    // Build profile data structure
    var profile_array = try std.ArrayList(RespValue).initCapacity(allocator, 6);
    errdefer profile_array.deinit(allocator);

    // "Total profile time"
    var total_pair = try std.ArrayList(RespValue).initCapacity(allocator, 2);
    errdefer total_pair.deinit(allocator);
    try total_pair.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "Total profile time") });
    var time_buf: [32]u8 = undefined;
    const time_str = try std.fmt.bufPrint(&time_buf, "{d:.3}", .{total_time_ms});
    try total_pair.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, time_str) });
    const total_slice = try total_pair.toOwnedSlice(allocator);
    errdefer allocator.free(total_slice);
    try profile_array.append(allocator, RespValue{ .array = total_slice });

    // "Parsing time" (stub: 0.0)
    var parsing_pair = try std.ArrayList(RespValue).initCapacity(allocator, 2);
    errdefer parsing_pair.deinit(allocator);
    try parsing_pair.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "Parsing time") });
    try parsing_pair.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "0.0") });
    const parsing_slice = try parsing_pair.toOwnedSlice(allocator);
    errdefer allocator.free(parsing_slice);
    try profile_array.append(allocator, RespValue{ .array = parsing_slice });

    // "Pipeline creation time" (stub: 0.0)
    var pipeline_pair = try std.ArrayList(RespValue).initCapacity(allocator, 2);
    errdefer pipeline_pair.deinit(allocator);
    try pipeline_pair.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "Pipeline creation time") });
    try pipeline_pair.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "0.0") });
    const pipeline_slice = try pipeline_pair.toOwnedSlice(allocator);
    errdefer allocator.free(pipeline_slice);
    try profile_array.append(allocator, RespValue{ .array = pipeline_slice });

    // "Warning" (stub: empty)
    var warning_pair = try std.ArrayList(RespValue).initCapacity(allocator, 2);
    errdefer warning_pair.deinit(allocator);
    try warning_pair.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "Warning") });
    try warning_pair.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "") });
    const warning_slice = try warning_pair.toOwnedSlice(allocator);
    errdefer allocator.free(warning_slice);
    try profile_array.append(allocator, RespValue{ .array = warning_slice });

    // "Iterators profile" (stub: WILDCARD iterator tree)
    var iterators_pair = try std.ArrayList(RespValue).initCapacity(allocator, 2);
    errdefer iterators_pair.deinit(allocator);
    try iterators_pair.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "Iterators profile") });

    var iterator_tree = try std.ArrayList(RespValue).initCapacity(allocator, 8);
    errdefer iterator_tree.deinit(allocator);

    try iterator_tree.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "Type") });
    try iterator_tree.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "WILDCARD") });

    try iterator_tree.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "Query type") });
    try iterator_tree.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "WILDCARD") });

    try iterator_tree.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "Time") });
    try iterator_tree.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "0.0") });

    try iterator_tree.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "Number of reading operations") });
    try iterator_tree.append(allocator, RespValue{ .integer = 0 });

    // Add child iterators only if not LIMITED
    if (!limited) {
        try iterator_tree.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "Child iterators") });
        var children = try std.ArrayList(RespValue).initCapacity(allocator, 0);
        const children_slice = try children.toOwnedSlice(allocator);
        errdefer allocator.free(children_slice);
        try iterator_tree.append(allocator, RespValue{ .array = children_slice });
    }

    const iterator_tree_slice = try iterator_tree.toOwnedSlice(allocator);
    errdefer allocator.free(iterator_tree_slice);
    try iterators_pair.append(allocator, RespValue{ .array = iterator_tree_slice });
    const iterators_pair_slice = try iterators_pair.toOwnedSlice(allocator);
    errdefer allocator.free(iterators_pair_slice);
    try profile_array.append(allocator, RespValue{ .array = iterators_pair_slice });

    // "Result processors profile" (stub: basic processors)
    var processors_pair = try std.ArrayList(RespValue).initCapacity(allocator, 2);
    errdefer processors_pair.deinit(allocator);
    try processors_pair.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "Result processors profile") });

    var processors_list = try std.ArrayList(RespValue).initCapacity(allocator, 2);
    errdefer processors_list.deinit(allocator);

    // Index processor
    var index_proc = try std.ArrayList(RespValue).initCapacity(allocator, 6);
    errdefer index_proc.deinit(allocator);
    try index_proc.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "Type") });
    try index_proc.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "Index") });
    try index_proc.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "Time") });
    try index_proc.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "0.0") });
    try index_proc.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "Results processed") });
    try index_proc.append(allocator, RespValue{ .integer = 0 });
    const index_proc_slice = try index_proc.toOwnedSlice(allocator);
    errdefer allocator.free(index_proc_slice);
    try processors_list.append(allocator, RespValue{ .array = index_proc_slice });

    // Processor based on query type
    const processor_type = if (is_search) "Counter" else "Grouper";
    var typed_proc = try std.ArrayList(RespValue).initCapacity(allocator, 6);
    errdefer typed_proc.deinit(allocator);
    try typed_proc.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "Type") });
    try typed_proc.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, processor_type) });
    try typed_proc.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "Time") });
    try typed_proc.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "0.0") });
    try typed_proc.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "Results processed") });
    try typed_proc.append(allocator, RespValue{ .integer = 0 });
    const typed_proc_slice = try typed_proc.toOwnedSlice(allocator);
    errdefer allocator.free(typed_proc_slice);
    try processors_list.append(allocator, RespValue{ .array = typed_proc_slice });

    const processors_list_slice = try processors_list.toOwnedSlice(allocator);
    errdefer allocator.free(processors_list_slice);
    try processors_pair.append(allocator, RespValue{ .array = processors_list_slice });
    const processors_pair_slice = try processors_pair.toOwnedSlice(allocator);
    errdefer allocator.free(processors_pair_slice);
    try profile_array.append(allocator, RespValue{ .array = processors_pair_slice });

    // Build final 2-element response array
    var response = try std.ArrayList(RespValue).initCapacity(allocator, 2);
    errdefer response.deinit(allocator);

    try response.append(allocator, query_result);
    try response.append(allocator, RespValue{ .array = try profile_array.toOwnedSlice(allocator) });

    return RespValue{ .array = try response.toOwnedSlice(allocator) };
}

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
    var include_dicts = try std.ArrayList([]const u8).initCapacity(arena, 0);
    var exclude_dicts = try std.ArrayList([]const u8).initCapacity(arena, 0);

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
                try include_dicts.append(arena, dict_name);
            } else {
                try exclude_dicts.append(arena, dict_name);
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

/// FT.CURSOR READ index cursor_id [COUNT read_size]
///
/// Reads from a cursor created by FT.AGGREGATE ... WITHCURSOR.
/// Advances the cursor offset by the number of documents returned.
///
/// Arguments:
///   args[0] = index_name
///   args[1] = cursor_id (integer)
///   args[2..] = optional COUNT read_size
///
/// Returns:
///   2-element array: [results_array, cursor_id or 0 if exhausted]
///   Error if cursor not found or index doesn't exist
///
/// Side Effects:
///   Mutates cursor.offset to track pagination position
pub fn cmdFtCursorRead(storage: *Storage, arena: std.mem.Allocator, args: []const []const u8) !RespValue {
    if (args.len < 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.CURSOR READ' command" };
    }

    const index_name = args[0];
    const cursor_id = std.fmt.parseInt(u64, args[1], 10) catch {
        return RespValue{ .error_string = "ERR invalid cursor ID" };
    };

    // Parse optional COUNT parameter
    var read_count: ?usize = null;
    if (args.len >= 4 and std.mem.eql(u8, args[2], "COUNT")) {
        read_count = std.fmt.parseInt(usize, args[3], 10) catch {
            return RespValue{ .error_string = "ERR invalid COUNT value" };
        };
    }

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Verify index exists
    const index = storage.search.getIndex(index_name) orelse {
        return RespValue{ .error_string = "ERR Unknown index name" };
    };

    // Get cursor
    const cursor = storage.search.getCursor(cursor_id) orelse {
        return RespValue{ .error_string = "ERR Cursor does not exist" };
    };

    // Verify cursor belongs to this index
    if (!std.mem.eql(u8, cursor.index_name, index_name)) {
        return RespValue{ .error_string = "ERR Cursor belongs to different index" };
    }

    // Determine page size (use COUNT parameter or cursor default)
    const page_size = read_count orelse cursor.default_count;

    // Execute search with current offset
    var result = try index.search(
        storage,
        arena,
        cursor.query,
        cursor.offset,
        page_size,
        cursor.nocontent,
        cursor.return_fields,
        cursor.sortby_field,
        cursor.sortby_desc,
    );
    defer result.deinit();

    // Update cursor offset
    cursor.offset += result.documents.len;

    // Check if cursor exhausted
    const next_cursor_id = if (cursor.offset >= cursor.total_count) 0 else cursor_id;

    // Build result array: [results, cursor_id]
    var response_array = try std.ArrayList(RespValue).initCapacity(arena, 2);
    errdefer response_array.deinit(arena);

    // Results array
    var docs_array = try std.ArrayList(RespValue).initCapacity(arena, result.documents.len);
    errdefer docs_array.deinit(arena);

    for (result.documents) |doc| {
        // Build document result (same as FT.SEARCH format)
        var doc_array = try std.ArrayList(RespValue).initCapacity(arena, 2);
        errdefer doc_array.deinit(arena);

        try doc_array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, doc.id) });

        if (!cursor.nocontent) {
            var field_array = try std.ArrayList(RespValue).initCapacity(arena, doc.fields.count() * 2);
            errdefer field_array.deinit(arena);

            var field_it = doc.fields.iterator();
            while (field_it.next()) |entry| {
                try field_array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, entry.key_ptr.*) });
                try field_array.append(arena, RespValue{ .bulk_string = try arena.dupe(u8, entry.value_ptr.*) });
            }

            const field_slice = try field_array.toOwnedSlice(arena);
            errdefer arena.free(field_slice);
            try doc_array.append(arena, RespValue{ .array = field_slice });
        }

        const doc_slice = try doc_array.toOwnedSlice(arena);
        errdefer arena.free(doc_slice);
        try docs_array.append(arena, RespValue{ .array = doc_slice });
    }

    const docs_slice = try docs_array.toOwnedSlice(arena);
    errdefer arena.free(docs_slice);
    try response_array.append(arena, RespValue{ .array = docs_slice });

    // Cursor ID (0 if exhausted)
    try response_array.append(arena, RespValue{ .integer = @intCast(next_cursor_id) });

    const response_slice = try response_array.toOwnedSlice(arena);
    return RespValue{ .array = response_slice };
}

/// FT.CURSOR DEL index cursor_id
///
/// Deletes a cursor to free resources.
///
/// Arguments:
///   args[0] = index_name
///   args[1] = cursor_id (integer)
///
/// Returns:
///   +OK on success
///   Error if cursor doesn't exist
pub fn cmdFtCursorDel(storage: *Storage, _: std.mem.Allocator, args: []const []const u8) !RespValue {
    if (args.len != 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.CURSOR DEL' command" };
    }

    const index_name = args[0];
    const cursor_id = std.fmt.parseInt(u64, args[1], 10) catch {
        return RespValue{ .error_string = "ERR invalid cursor ID" };
    };

    storage.mutex.lock();
    defer storage.mutex.unlock();

    // Verify index exists
    if (storage.search.getIndex(index_name) == null) {
        return RespValue{ .error_string = "ERR Unknown index name" };
    }

    // Delete cursor
    storage.search.deleteCursor(cursor_id) catch {
        return RespValue{ .error_string = "ERR Cursor does not exist" };
    };

    return RespValue{ .simple_string = "OK" };
}

/// FT.ALIASADD alias index
///
/// Creates an alias for a search index.
///
/// Returns +OK on success.
/// Errors:
/// - wrong number of arguments
/// - Unknown index name (if index doesn't exist)
/// - Alias already exists
pub fn cmdFtAliasadd(storage: *Storage, _: std.mem.Allocator, args: []const []const u8) !RespValue {
    if (args.len != 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.ALIASADD' command" };
    }

    const alias = args[0];
    const index_name = args[1];

    storage.mutex.lock();
    defer storage.mutex.unlock();

    storage.search.addAlias(alias, index_name) catch |err| {
        return switch (err) {
            error.IndexNotFound => RespValue{ .error_string = "ERR Unknown index name" },
            error.AliasAlreadyExists => RespValue{ .error_string = "ERR Alias already exists" },
            error.AliasEqualsIndexName => RespValue{ .error_string = "ERR Alias name equals index name" },
            else => return err,
        };
    };

    return RespValue{ .simple_string = "OK" };
}

/// FT.ALIASDEL alias
///
/// Deletes an alias.
///
/// Returns +OK on success.
/// Errors:
/// - wrong number of arguments
/// - Alias does not exist
pub fn cmdFtAliasdel(storage: *Storage, _: std.mem.Allocator, args: []const []const u8) !RespValue {
    if (args.len != 1) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.ALIASDEL' command" };
    }

    const alias = args[0];

    storage.mutex.lock();
    defer storage.mutex.unlock();

    storage.search.deleteAlias(alias) catch |err| {
        return switch (err) {
            error.AliasNotFound => RespValue{ .error_string = "ERR Alias does not exist" },
            else => return err,
        };
    };

    return RespValue{ .simple_string = "OK" };
}

/// FT.ALIASUPDATE alias index
///
/// Updates an existing alias to point to a different index.
///
/// Returns +OK on success.
/// Errors:
/// - wrong number of arguments
/// - Alias does not exist
/// - Unknown index name (if new index doesn't exist)
pub fn cmdFtAliasupdate(storage: *Storage, _: std.mem.Allocator, args: []const []const u8) !RespValue {
    if (args.len != 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.ALIASUPDATE' command" };
    }

    const alias = args[0];
    const new_index_name = args[1];

    storage.mutex.lock();
    defer storage.mutex.unlock();

    storage.search.updateAlias(alias, new_index_name) catch |err| {
        return switch (err) {
            error.AliasNotFound => RespValue{ .error_string = "ERR Alias does not exist" },
            error.IndexNotFound => RespValue{ .error_string = "ERR Unknown index name" },
            else => return err,
        };
    };

    return RespValue{ .simple_string = "OK" };
}

/// FT.DICTADD dict_name term [term ...]
///
/// Add terms to a dictionary (for stop words, synonyms).
///
/// Returns the number of newly added terms (ignoring duplicates).
pub fn cmdFtDictadd(storage: *Storage, _: std.mem.Allocator, args: []const []const u8) !RespValue {
    if (args.len < 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.DICTADD' command" };
    }

    const dict_name = args[0];
    const terms = args[1..];

    storage.mutex.lock();
    defer storage.mutex.unlock();

    const count = try storage.search.addTermsToDictionary(dict_name, terms);

    return RespValue{ .integer = @as(i64, @intCast(count)) };
}

/// FT.DICTDEL dict_name term [term ...]
///
/// Remove terms from a dictionary.
///
/// Returns the number of removed terms.
/// Returns 0 if dictionary doesn't exist.
pub fn cmdFtDictdel(storage: *Storage, _: std.mem.Allocator, args: []const []const u8) !RespValue {
    if (args.len < 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.DICTDEL' command" };
    }

    const dict_name = args[0];
    const terms = args[1..];

    storage.mutex.lock();
    defer storage.mutex.unlock();

    const count = try storage.search.removeTermsFromDictionary(dict_name, terms);

    return RespValue{ .integer = @as(i64, @intCast(count)) };
}

/// FT.DICTDUMP dict_name
///
/// Return all terms in a dictionary in insertion order.
///
/// Returns empty array if dictionary doesn't exist.
pub fn cmdFtDictdump(storage: *Storage, allocator: std.mem.Allocator, args: []const []const u8) !RespValue {
    if (args.len != 1) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.DICTDUMP' command" };
    }

    const dict_name = args[0];

    storage.mutex.lock();
    defer storage.mutex.unlock();

    const terms = try storage.search.dumpDictionary(allocator, dict_name);
    defer {
        for (terms) |term| {
            allocator.free(term);
        }
        allocator.free(terms);
    }

    // Build RESP array response
    var resp_array = try std.ArrayList(RespValue).initCapacity(allocator, terms.len);
    errdefer resp_array.deinit(allocator);

    for (terms) |term| {
        const term_copy = try allocator.dupe(u8, term);
        errdefer allocator.free(term_copy);
        try resp_array.append(allocator, RespValue{ .bulk_string = term_copy });
    }

    return RespValue{ .array = try resp_array.toOwnedSlice(allocator) };
}
