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
