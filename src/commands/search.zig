const std = @import("std");
const Storage = @import("../storage/memory.zig").Storage;
const parser = @import("../protocol/parser.zig");
const RespValue = parser.RespValue;
const search_mod = @import("../storage/search.zig");

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
