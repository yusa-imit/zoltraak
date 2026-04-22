const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const Storage = @import("../storage/memory.zig").Storage;
const Value = @import("../storage/memory.zig").Value;
const TopKValue = @import("../storage/topk.zig").TopKValue;
const RespProtocol = @import("client.zig").RespProtocol;

/// TOPK.RESERVE key topk [width depth decay]
/// Create an empty Top-K with specified parameters
/// Default values: width=8, depth=7, decay=0.9
pub fn cmdTopkReserve(allocator: std.mem.Allocator, storage: *Storage, args: []protocol.RespValue) !protocol.RespValue {
    if (args.len < 3 or args.len > 6) {
        return protocol.RespValue{ .error_string = "ERR wrong number of arguments for 'TOPK.RESERVE' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid key" },
    };

    const topk_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid topk parameter" },
    };

    const k = std.fmt.parseInt(u32, topk_str, 10) catch {
        return protocol.RespValue{ .error_string = "ERR invalid topk parameter" };
    };

    if (k == 0) {
        return protocol.RespValue{ .error_string = "ERR topk must be greater than 0" };
    }

    // Parse optional parameters
    var width: u32 = 8;
    var depth: u32 = 7;
    var decay: f64 = 0.9;

    if (args.len >= 4) {
        const width_str = switch (args[3]) {
            .bulk_string => |s| s,
            else => return protocol.RespValue{ .error_string = "ERR invalid width parameter" },
        };
        width = std.fmt.parseInt(u32, width_str, 10) catch {
            return protocol.RespValue{ .error_string = "ERR invalid width parameter" };
        };
    }

    if (args.len >= 5) {
        const depth_str = switch (args[4]) {
            .bulk_string => |s| s,
            else => return protocol.RespValue{ .error_string = "ERR invalid depth parameter" },
        };
        depth = std.fmt.parseInt(u32, depth_str, 10) catch {
            return protocol.RespValue{ .error_string = "ERR invalid depth parameter" };
        };
    }

    if (args.len >= 6) {
        const decay_str = switch (args[5]) {
            .bulk_string => |s| s,
            else => return protocol.RespValue{ .error_string = "ERR invalid decay parameter" },
        };
        decay = std.fmt.parseFloat(f64, decay_str) catch {
            return protocol.RespValue{ .error_string = "ERR invalid decay parameter" };
        };
    }

    // Validate decay range
    if (decay <= 0.0 or decay >= 1.0) {
        return protocol.RespValue{ .error_string = "ERR decay must be between 0 and 1" };
    }

    // Create Top-K value
    var topk = TopKValue.init(allocator, k, width, depth, decay) catch |err| {
        return switch (err) {
            error.InvalidK => protocol.RespValue{ .error_string = "ERR topk must be greater than 0" },
            error.InvalidWidth => protocol.RespValue{ .error_string = "ERR width must be greater than 0" },
            error.InvalidDepth => protocol.RespValue{ .error_string = "ERR depth must be greater than 0" },
            error.InvalidDecay => protocol.RespValue{ .error_string = "ERR decay must be between 0 and 1" },
            else => protocol.RespValue{ .error_string = "ERR failed to create Top-K" },
        };
    };

    // Check if key already exists
    if (storage.data.contains(key)) {
        topk.deinit();
        return protocol.RespValue{ .error_string = "ERR key already exists" };
    }

    // Duplicate key for HashMap ownership
    const key_copy = try allocator.dupe(u8, key);
    errdefer allocator.free(key_copy);
    try storage.data.put(key_copy, Value{ .top_k = topk });

    return protocol.RespValue{ .simple_string = "OK" };
}

/// TOPK.ADD key item [item ...]
/// Add one or more items to the Top-K
/// Returns array of expelled items (null if none)
pub fn cmdTopkAdd(allocator: std.mem.Allocator, storage: *Storage, args: []protocol.RespValue) !protocol.RespValue {
    if (args.len < 3) {
        return protocol.RespValue{ .error_string = "ERR wrong number of arguments for 'TOPK.ADD' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid key" },
    };

    // Get Top-K value
    const entry = storage.data.getEntry(key) orelse {
        return protocol.RespValue{ .error_string = "ERR key does not exist" };
    };

    if (entry.value_ptr.* != .top_k) {
        return protocol.RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    var topk = &entry.value_ptr.top_k;

    // Process each item
    var results = std.ArrayList(protocol.RespValue){};
    errdefer {
        for (results.items) |*item| {
            if (item.* == .bulk_string) {
                allocator.free(item.bulk_string);
            }
        }
        results.deinit(allocator);
    }

    for (args[2..]) |arg| {
        const item = switch (arg) {
            .bulk_string => |s| s,
            else => {
                for (results.items) |*r| {
                    if (r.* == .bulk_string) {
                        allocator.free(r.bulk_string);
                    }
                }
                results.deinit(allocator);
                return protocol.RespValue{ .error_string = "ERR invalid item" };
            },
        };

        const expelled = try topk.add(item);
        if (expelled) |exp| {
            // Caller owns expelled string, must free on error
            errdefer allocator.free(exp);
            try results.append(allocator, protocol.RespValue{ .bulk_string = exp });
        } else {
            try results.append(allocator, protocol.RespValue{ .null_bulk_string = {} });
        }
    }

    const results_slice = try results.toOwnedSlice(allocator);
    return protocol.RespValue{ .array = results_slice };
}

/// TOPK.QUERY key item [item ...]
/// Check if one or more items are in the Top-K
/// Returns array of integers (1=in, 0=not in) for RESP2
/// Returns array of booleans (true/false) for RESP3
pub fn cmdTopkQuery(allocator: std.mem.Allocator, storage: *Storage, args: []protocol.RespValue, protocol_version: RespProtocol) !protocol.RespValue {
    if (args.len < 3) {
        return protocol.RespValue{ .error_string = "ERR wrong number of arguments for 'TOPK.QUERY' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid key" },
    };

    // Get Top-K value
    const entry = storage.data.getEntry(key) orelse {
        return protocol.RespValue{ .error_string = "ERR key does not exist" };
    };

    const topk = switch (entry.value_ptr.*) {
        .top_k => |*tk| tk,
        else => return protocol.RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" },
    };

    // Process each item
    var results = std.ArrayList(protocol.RespValue){};
    errdefer results.deinit(allocator);

    for (args[2..]) |arg| {
        const item = switch (arg) {
            .bulk_string => |s| s,
            else => {
                results.deinit(allocator);
                return protocol.RespValue{ .error_string = "ERR invalid item" };
            },
        };

        const in_topk = topk.query(item);

        if (protocol_version == .RESP3) {
            // RESP3: return boolean
            try results.append(allocator, protocol.RespValue{ .boolean = in_topk });
        } else {
            // RESP2: return integer (1 or 0)
            const int_val: i64 = if (in_topk) 1 else 0;
            try results.append(allocator, protocol.RespValue{ .integer = int_val });
        }
    }

    const results_slice = try results.toOwnedSlice(allocator);
    return protocol.RespValue{ .array = results_slice };
}

/// TOPK.COUNT key item [item ...]
/// Get estimated counts for one or more items
/// Returns array of integers (estimated counts)
pub fn cmdTopkCount(allocator: std.mem.Allocator, storage: *Storage, args: []protocol.RespValue) !protocol.RespValue {
    if (args.len < 3) {
        return protocol.RespValue{ .error_string = "ERR wrong number of arguments for 'TOPK.COUNT' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid key" },
    };

    // Get Top-K value
    const entry = storage.data.getEntry(key) orelse {
        return protocol.RespValue{ .error_string = "ERR key does not exist" };
    };

    const topk = switch (entry.value_ptr.*) {
        .top_k => |*tk| tk,
        else => return protocol.RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" },
    };

    // Process each item
    var results = std.ArrayList(protocol.RespValue){};
    errdefer results.deinit(allocator);

    for (args[2..]) |arg| {
        const item = switch (arg) {
            .bulk_string => |s| s,
            else => {
                results.deinit(allocator);
                return protocol.RespValue{ .error_string = "ERR invalid item" };
            },
        };

        const cnt = topk.count(item);
        try results.append(allocator, protocol.RespValue{ .integer = @intCast(cnt) });
    }

    const results_slice = try results.toOwnedSlice(allocator);
    return protocol.RespValue{ .array = results_slice };
}
