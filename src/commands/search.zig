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
pub fn cmdFtCreate(storage: *Storage, arena: std.mem.Allocator, args: []const []const u8) !RespValue {
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

                // Parse field options (SORTABLE, NOINDEX, etc.)
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
                        field.alias = try storage.allocator.dupe(u8, args[i]);
                        i += 1;
                    } else {
                        // Not a field option, break to parse next field
                        break;
                    }
                }

                try index.addField(field);
            }
        } else {
            // Unknown clause
            const err_msg = try std.fmt.allocPrint(arena, "ERR unknown clause: {s}", .{keyword});
            return RespValue{ .error_string = err_msg };
        }
    }

    return RespValue{ .simple_string = "OK" };
}

/// FT._LIST
///
/// Lists all index names in the search store.
///
/// Returns:
///   Array of index names
pub fn cmdFtList(storage: *Storage, arena: std.mem.Allocator, args: []const []const u8) !RespValue {
    _ = args;

    storage.mutex.lock();
    defer storage.mutex.unlock();

    const names = try storage.search.listIndices(arena);
    // No need to free names - arena will be cleared

    var values = try arena.alloc(RespValue, names.len);
    for (names, 0..) |name, i| {
        values[i] = RespValue{ .bulk_string = name };
    }

    return RespValue{ .array = values };
}

/// FT.DROPINDEX index_name [DD]
///
/// Drops a search index. If DD flag is provided, also deletes indexed documents.
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

    if (args.len < 1) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.DROPINDEX' command" };
    }

    const index_name = args[0];
    const delete_docs = if (args.len >= 2) std.mem.eql(u8, args[1], "DD") else false;

    _ = delete_docs; // TODO: implement document deletion

    storage.mutex.lock();
    defer storage.mutex.unlock();

    storage.search.dropIndex(index_name) catch |err| {
        if (err == error.IndexNotFound) {
            return RespValue{ .error_string = "ERR Unknown Index name" };
        }
        return err;
    };

    return RespValue{ .simple_string = "OK" };
}

/// FT.INFO index_name
///
/// Returns information and statistics about an index.
///
/// Arguments:
///   args[0] = index_name
///
/// Returns:
///   Array of key-value pairs containing index metadata:
///   - index_name: string
///   - index_options: array (empty for now)
///   - index_definition: nested array with key_type, prefixes, default_score
///   - attributes: array of field definitions
///   - num_docs: integer (0 for stub)
///   - max_doc_id: integer (0 for stub)
///   - num_terms: integer (0 for stub)
///   - num_records: integer (0 for stub)
///   - inverted_sz_mb: float (0.0 for stub)
///   - percent_indexed: float (1.0 = fully indexed)
///
/// Error:
///   "ERR Unknown index name" if index doesn't exist
pub fn cmdFtInfo(storage: *Storage, arena: std.mem.Allocator, args: []const []const u8) !RespValue {
    if (args.len < 1) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.INFO' command" };
    }

    const index_name = args[0];

    storage.mutex.lock();
    defer storage.mutex.unlock();

    const index = storage.search.getIndex(index_name) orelse {
        return RespValue{ .error_string = "ERR Unknown index name" };
    };

    // Build response as flat array of key-value pairs (RESP2 format)
    // Total fields: 10 base fields * 2 = 20 elements
    var result = try std.ArrayList(RespValue).initCapacity(arena, 0);
    errdefer result.deinit(arena);

    // Field 1: index_name
    try result.append(arena, RespValue{ .bulk_string = "index_name" });
    try result.append(arena, RespValue{ .bulk_string = index.name });

    // Field 2: index_options (empty array for now)
    try result.append(arena, RespValue{ .bulk_string = "index_options" });
    const empty_array = try arena.alloc(RespValue, 0);
    try result.append(arena, RespValue{ .array = empty_array });

    // Field 3: index_definition (nested array with key_type, prefixes, default_score)
    try result.append(arena, RespValue{ .bulk_string = "index_definition" });
    var def = try std.ArrayList(RespValue).initCapacity(arena, 0);
    errdefer def.deinit(arena);

    // key_type
    try def.append(arena, RespValue{ .bulk_string = "key_type" });
    const key_type_str = switch (index.index_on) {
        .hash => "HASH",
        .json => "JSON",
    };
    try def.append(arena, RespValue{ .bulk_string = key_type_str });

    // prefixes
    try def.append(arena, RespValue{ .bulk_string = "prefixes" });
    if (index.prefix) |prefix| {
        var prefix_array = try arena.alloc(RespValue, 1);
        prefix_array[0] = RespValue{ .bulk_string = prefix };
        try def.append(arena, RespValue{ .array = prefix_array });
    } else {
        const empty_prefix_array = try arena.alloc(RespValue, 0);
        try def.append(arena, RespValue{ .array = empty_prefix_array });
    }

    // default_score
    try def.append(arena, RespValue{ .bulk_string = "default_score" });
    try def.append(arena, RespValue{ .bulk_string = "1" });

    const def_slice = try def.toOwnedSlice(arena);
    try result.append(arena, RespValue{ .array = def_slice });

    // Field 4: attributes (field definitions)
    try result.append(arena, RespValue{ .bulk_string = "attributes" });
    var attrs = try std.ArrayList(RespValue).initCapacity(arena, 0);
    errdefer attrs.deinit(arena);

    for (index.fields.items) |field| {
        // Each field is an array of key-value pairs
        var field_info = try std.ArrayList(RespValue).initCapacity(arena, 0);
        errdefer field_info.deinit(arena);

        // identifier (field name or alias)
        try field_info.append(arena, RespValue{ .bulk_string = "identifier" });
        const identifier = field.alias orelse field.name;
        try field_info.append(arena, RespValue{ .bulk_string = identifier });

        // attribute (original field name)
        try field_info.append(arena, RespValue{ .bulk_string = "attribute" });
        try field_info.append(arena, RespValue{ .bulk_string = field.name });

        // type
        try field_info.append(arena, RespValue{ .bulk_string = "type" });
        const type_str = switch (field.field_type) {
            .text => "TEXT",
            .tag => "TAG",
            .numeric => "NUMERIC",
            .geo => "GEO",
            .vector => "VECTOR",
            .geoshape => "GEOSHAPE",
        };
        try field_info.append(arena, RespValue{ .bulk_string = type_str });

        // SORTABLE flag
        if (field.sortable) {
            try field_info.append(arena, RespValue{ .bulk_string = "SORTABLE" });
        }

        // NOINDEX flag
        if (field.noindex) {
            try field_info.append(arena, RespValue{ .bulk_string = "NOINDEX" });
        }

        // NOSTEM flag (TEXT only)
        if (field.nostem and field.field_type == .text) {
            try field_info.append(arena, RespValue{ .bulk_string = "NOSTEM" });
        }

        const field_slice = try field_info.toOwnedSlice(arena);
        try attrs.append(arena, RespValue{ .array = field_slice });
    }

    const attrs_slice = try attrs.toOwnedSlice(arena);
    try result.append(arena, RespValue{ .array = attrs_slice });

    // Field 5-10: Statistics (all stubs for now)
    // num_docs
    try result.append(arena, RespValue{ .bulk_string = "num_docs" });
    try result.append(arena, RespValue{ .bulk_string = "0" });

    // max_doc_id
    try result.append(arena, RespValue{ .bulk_string = "max_doc_id" });
    try result.append(arena, RespValue{ .bulk_string = "0" });

    // num_terms
    try result.append(arena, RespValue{ .bulk_string = "num_terms" });
    try result.append(arena, RespValue{ .bulk_string = "0" });

    // num_records
    try result.append(arena, RespValue{ .bulk_string = "num_records" });
    try result.append(arena, RespValue{ .bulk_string = "0" });

    // inverted_sz_mb
    try result.append(arena, RespValue{ .bulk_string = "inverted_sz_mb" });
    try result.append(arena, RespValue{ .bulk_string = "0" });

    // percent_indexed (1.0 = 100% indexed)
    try result.append(arena, RespValue{ .bulk_string = "percent_indexed" });
    try result.append(arena, RespValue{ .bulk_string = "1" });

    const final_slice = try result.toOwnedSlice(arena);
    return RespValue{ .array = final_slice };
}

/// FT.ALTER index_name SCHEMA ADD field_name field_type [options...]
///
/// Adds a new field to an existing index schema.
///
/// Arguments:
///   args[0]: index_name - name of the index to alter
///   args[1]: "SCHEMA" keyword
///   args[2]: "ADD" keyword
///   args[3]: field_name - name of the field to add
///   args[4]: field_type - type of field (TEXT, TAG, NUMERIC, GEO, VECTOR, GEOSHAPE)
///   args[5..]: optional field options (SORTABLE, NOINDEX, NOSTEM, AS alias, etc.)
///
/// Returns:
///   Simple string "+OK" on success
///
/// Error:
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
            // Unknown option - stop parsing
            break;
        }
    }

    // Add field to index
    try index.addField(field);

    return RespValue{ .simple_string = "OK" };
}
