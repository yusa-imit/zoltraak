const std = @import("std");
const Storage = @import("../storage/memory.zig").Storage;
const parser = @import("../protocol/parser.zig");
const RespValue = parser.RespValue;
const search_mod = @import("../storage/search.zig");

/// FT.AGGREGATE index_name query [LOAD count field [field ...]]
///              [GROUPBY count field [field ...] REDUCE func nargs arg [arg ...] [AS name] ...]
///              [SORTBY count field [ASC|DESC] ...]
///              [LIMIT offset count]
///
/// Runs an aggregation pipeline on search results.
///
/// Arguments:
///   args[0] = index_name
///   args[1] = query (supports "*" wildcard or search terms)
///   args[2..] = optional LOAD/GROUPBY/SORTBY/LIMIT clauses
///
/// Returns:
///   Array of aggregated results with row count
///
/// Example:
///   FT.AGGREGATE idx "*" GROUPBY 1 @category REDUCE COUNT 0 AS cnt SORTBY 2 @cnt DESC
pub fn cmdFtAggregate(storage: *Storage, arena: std.mem.Allocator, args: []const []const u8) !RespValue {
    if (args.len < 2) {
        return RespValue{ .error_string = "ERR wrong number of arguments for 'FT.AGGREGATE'" };
    }

    const index_name = args[0];
    const query = args[1];

    storage.mutex.lock();
    defer storage.mutex.unlock();

    const index = storage.search.getIndex(index_name) orelse {
        return RespValue{ .error_string = "ERR Unknown index name" };
    };

    // Parse aggregation pipeline clauses
    var load_fields: ?[]const []const u8 = null;
    var groupby_fields: ?[]const []const u8 = null;
    var reduce_ops = try std.ArrayList(search_mod.ReduceOp).initCapacity(arena, 0);
    defer reduce_ops.deinit(arena);
    var sortby_fields: ?[]const []const u8 = null;
    var sortby_orders: ?[]bool = null; // true = DESC, false = ASC
    var limit_offset: usize = 0;
    var limit_count: usize = 10;

    var i: usize = 2;
    while (i < args.len) {
        const keyword = args[i];

        if (std.mem.eql(u8, keyword, "LOAD")) {
            i += 1;
            if (i >= args.len) {
                return RespValue{ .error_string = "ERR LOAD requires count argument" };
            }

            const count = std.fmt.parseInt(usize, args[i], 10) catch {
                return RespValue{ .error_string = "ERR LOAD count must be a valid integer" };
            };
            i += 1;

            if (i + count > args.len) {
                return RespValue{ .error_string = "ERR not enough LOAD field arguments" };
            }

            load_fields = args[i .. i + count];
            i += count;
        } else if (std.mem.eql(u8, keyword, "GROUPBY")) {
            i += 1;
            if (i >= args.len) {
                return RespValue{ .error_string = "ERR GROUPBY requires count argument" };
            }

            const count = std.fmt.parseInt(usize, args[i], 10) catch {
                return RespValue{ .error_string = "ERR GROUPBY count must be a valid integer" };
            };
            i += 1;

            if (i + count > args.len) {
                return RespValue{ .error_string = "ERR not enough GROUPBY field arguments" };
            }

            groupby_fields = args[i .. i + count];
            i += count;

            // Parse REDUCE operations
            while (i < args.len and std.mem.eql(u8, args[i], "REDUCE")) {
                i += 1;
                if (i + 2 > args.len) {
                    return RespValue{ .error_string = "ERR REDUCE requires function and nargs" };
                }

                const func_name = args[i];
                i += 1;

                const nargs = std.fmt.parseInt(usize, args[i], 10) catch {
                    return RespValue{ .error_string = "ERR REDUCE nargs must be a valid integer" };
                };
                i += 1;

                if (i + nargs > args.len) {
                    return RespValue{ .error_string = "ERR not enough REDUCE arguments" };
                }

                const reduce_args = args[i .. i + nargs];
                i += nargs;

                // Optional AS clause
                var as_name: ?[]const u8 = null;
                if (i + 1 < args.len and std.mem.eql(u8, args[i], "AS")) {
                    i += 1;
                    as_name = args[i];
                    i += 1;
                }

                const reduce_type = search_mod.ReduceType.fromString(func_name) catch {
                    return RespValue{ .error_string = "ERR unsupported REDUCE function" };
                };

                try reduce_ops.append(arena, search_mod.ReduceOp{
                    .reduce_type = reduce_type,
                    .args = reduce_args,
                    .as_name = as_name,
                });
            }
        } else if (std.mem.eql(u8, keyword, "SORTBY")) {
            i += 1;
            if (i >= args.len) {
                return RespValue{ .error_string = "ERR SORTBY requires count argument" };
            }

            const count = std.fmt.parseInt(usize, args[i], 10) catch {
                return RespValue{ .error_string = "ERR SORTBY count must be a valid integer" };
            };
            i += 1;

            if (count % 2 != 0) {
                return RespValue{ .error_string = "ERR SORTBY count must be even (field/order pairs)" };
            }

            if (i + count > args.len) {
                return RespValue{ .error_string = "ERR not enough SORTBY arguments" };
            }

            var fields = try arena.alloc([]const u8, count / 2);
            var orders = try arena.alloc(bool, count / 2);

            var j: usize = 0;
            while (j < count / 2) : (j += 1) {
                fields[j] = args[i + j * 2];

                const order = args[i + j * 2 + 1];
                if (std.mem.eql(u8, order, "DESC")) {
                    orders[j] = true;
                } else if (std.mem.eql(u8, order, "ASC")) {
                    orders[j] = false;
                } else {
                    return RespValue{ .error_string = "ERR SORTBY order must be ASC or DESC" };
                }
            }

            sortby_fields = fields;
            sortby_orders = orders;
            i += count;
        } else if (std.mem.eql(u8, keyword, "LIMIT")) {
            i += 1;
            if (i + 1 >= args.len) {
                return RespValue{ .error_string = "ERR LIMIT requires offset and count" };
            }

            limit_offset = std.fmt.parseInt(usize, args[i], 10) catch {
                return RespValue{ .error_string = "ERR LIMIT offset must be a valid integer" };
            };
            i += 1;

            limit_count = std.fmt.parseInt(usize, args[i], 10) catch {
                return RespValue{ .error_string = "ERR LIMIT count must be a valid integer" };
            };
            i += 1;
        } else {
            return RespValue{ .error_string = "ERR unknown clause in aggregation pipeline" };
        }
    }

    // Run aggregation
    var agg_result = try index.aggregate(
        storage,
        arena,
        query,
        load_fields,
        groupby_fields,
        reduce_ops.items,
        sortby_fields,
        sortby_orders,
        limit_offset,
        limit_count,
    );
    defer agg_result.deinit();

    // Format response: [count, row1, row2, ...]
    var array = try std.ArrayList(RespValue).initCapacity(arena, 1 + agg_result.total_count);
    errdefer array.deinit(arena);

    try array.append(arena, RespValue{ .integer = @intCast(agg_result.total_count) });

    for (agg_result.rows) |*row| {
        var row_arr = try std.ArrayList(RespValue).initCapacity(arena, 0);
        errdefer row_arr.deinit(arena);

        var field_iter = row.fields.iterator();
        while (field_iter.next()) |entry| {
            const field_name = try arena.dupe(u8, entry.key_ptr.*);
            errdefer arena.free(field_name);

            const field_value = try arena.dupe(u8, entry.value_ptr.*);
            errdefer arena.free(field_value);

            try row_arr.append(arena, RespValue{ .bulk_string = field_name });
            try row_arr.append(arena, RespValue{ .bulk_string = field_value });
        }

        try array.append(arena, RespValue{ .array = try row_arr.toOwnedSlice(arena) });
    }

    return RespValue{ .array = try array.toOwnedSlice(arena) };
}
