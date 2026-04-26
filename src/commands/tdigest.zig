const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const Storage = @import("../storage/memory.zig").Storage;
const Value = @import("../storage/memory.zig").Value;
const TDigestValue = @import("../storage/tdigest.zig").TDigestValue;
const RespProtocol = @import("client.zig").RespProtocol;

const RespValue = protocol.RespValue;

/// Helper to recursively free RespValue
fn deinitRespValue(value: *const RespValue, allocator: std.mem.Allocator) void {
    switch (value.*) {
        .array => |arr| {
            for (arr) |item| {
                deinitRespValue(&item, allocator);
            }
            allocator.free(@constCast(arr));
        },
        .bulk_string => |s| allocator.free(@constCast(s)),
        .simple_string => |s| allocator.free(@constCast(s)),
        .error_string => |s| allocator.free(@constCast(s)),
        else => {},
    }
}

/// TDIGEST.CREATE key [COMPRESSION compression]
/// Create an empty T-Digest with specified compression parameter
/// Default compression: 100
pub fn cmdTdigestCreate(allocator: std.mem.Allocator, storage: *Storage, args: []protocol.RespValue) !protocol.RespValue {
    if (args.len < 2 or args.len > 4) {
        return protocol.RespValue{ .error_string = "ERR wrong number of arguments for 'TDIGEST.CREATE' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid key" },
    };

    // Check if key already exists
    if (storage.data.get(key)) |_| {
        return protocol.RespValue{ .error_string = "BUSYKEY Target key name already exists" };
    }

    // Parse optional COMPRESSION parameter
    var compression: u32 = 100;

    if (args.len >= 4) {
        const opt_name = switch (args[2]) {
            .bulk_string => |s| s,
            else => return protocol.RespValue{ .error_string = "ERR invalid option" },
        };

        const opt_lower = try std.ascii.allocLowerString(allocator, opt_name);
        defer allocator.free(opt_lower);

        if (!std.mem.eql(u8, opt_lower, "compression")) {
            return protocol.RespValue{ .error_string = "ERR unknown option" };
        }

        const compression_str = switch (args[3]) {
            .bulk_string => |s| s,
            else => return protocol.RespValue{ .error_string = "ERR invalid compression value" },
        };

        compression = std.fmt.parseInt(u32, compression_str, 10) catch {
            return protocol.RespValue{ .error_string = "ERR compression must be an integer" };
        };
    }

    // Create T-Digest value
    var td = TDigestValue.init(allocator, compression) catch |err| {
        return switch (err) {
            error.InvalidCompression => protocol.RespValue{ .error_string = "ERR compression must be greater than 0" },
            error.InvalidValue => protocol.RespValue{ .error_string = "ERR invalid value" }, // Never returned by init, but required for exhaustive switch
            error.InvalidQuantile => protocol.RespValue{ .error_string = "ERR internal error" }, // Never returned by init
            error.EmptySketch => protocol.RespValue{ .error_string = "ERR internal error" }, // Never returned by init
            error.OutOfMemory => protocol.RespValue{ .error_string = "ERR out of memory" },
        };
    };
    errdefer td.deinit(); // CRITICAL: Cleanup on error

    // Store in hash map
    const key_copy = try allocator.dupe(u8, key);
    errdefer allocator.free(key_copy);

    try storage.data.put(key_copy, Value{ .t_digest = td });

    return protocol.RespValue{ .simple_string = "OK" };
}

/// TDIGEST.ADD key value [value ...]
/// Add values to T-Digest. Does NOT auto-create.
pub fn cmdTdigestAdd(allocator: std.mem.Allocator, storage: *Storage, args: []protocol.RespValue) !protocol.RespValue {
    _ = allocator; // Mark as unused for consistency with other commands
    if (args.len < 3) {
        return protocol.RespValue{ .error_string = "ERR wrong number of arguments for 'TDIGEST.ADD' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid key" },
    };

    // Look up key (MUST exist)
    const value_ptr = storage.data.getPtr(key) orelse {
        return protocol.RespValue{ .error_string = "ERR no such key" };
    };

    // Validate type
    if (value_ptr.* != .t_digest) {
        return protocol.RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    var td = &value_ptr.t_digest;

    // Parse and add values
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const value_str = switch (args[i]) {
            .bulk_string => |s| s,
            else => return protocol.RespValue{ .error_string = "ERR value must be a number" },
        };

        const value = std.fmt.parseFloat(f64, value_str) catch {
            return protocol.RespValue{ .error_string = "ERR value must be a valid float" };
        };

        td.add(value) catch {
            return protocol.RespValue{ .error_string = "ERR failed to add value" };
        };
    }

    return protocol.RespValue{ .simple_string = "OK" };
}

/// TDIGEST.RESET key
/// Clear all centroids but preserve compression parameter
pub fn cmdTdigestReset(allocator: std.mem.Allocator, storage: *Storage, args: []protocol.RespValue) !protocol.RespValue {
    _ = allocator;

    if (args.len != 2) {
        return protocol.RespValue{ .error_string = "ERR wrong number of arguments for 'TDIGEST.RESET' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid key" },
    };

    // Look up key
    const value_ptr = storage.data.getPtr(key) orelse {
        return protocol.RespValue{ .error_string = "ERR no such key" };
    };

    // Validate type
    if (value_ptr.* != .t_digest) {
        return protocol.RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    var td = &value_ptr.t_digest;
    td.reset();

    return protocol.RespValue{ .simple_string = "OK" };
}

/// TDIGEST.MERGE destkey numkeys sourcekey [sourcekey ...] [COMPRESSION compression] [OVERRIDE]
/// Merge multiple T-Digest sketches into a destination sketch
pub fn cmdTdigestMerge(allocator: std.mem.Allocator, storage: *Storage, args: []protocol.RespValue) !protocol.RespValue {
    if (args.len < 4) {
        return protocol.RespValue{ .error_string = "ERR wrong number of arguments for 'TDIGEST.MERGE' command" };
    }

    // Parse destkey
    const destkey = switch (args[1]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid destkey" },
    };

    // Parse numkeys
    const numkeys_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid numkeys" },
    };

    const numkeys = std.fmt.parseInt(u32, numkeys_str, 10) catch {
        return protocol.RespValue{ .error_string = "ERR numkeys must be an integer" };
    };

    if (numkeys == 0) {
        return protocol.RespValue{ .error_string = "ERR numkeys must be greater than 0" };
    }

    // Parse source keys
    if (args.len < 3 + numkeys) {
        return protocol.RespValue{ .error_string = "ERR numkeys does not match number of source keys" };
    }

    // Parse optional flags: COMPRESSION <value>, OVERRIDE
    var compression_override: ?u32 = null;
    var override_flag = false;
    var idx: usize = 3 + numkeys;

    while (idx < args.len) {
        const opt_name = switch (args[idx]) {
            .bulk_string => |s| s,
            else => return protocol.RespValue{ .error_string = "ERR invalid option" },
        };

        const opt_lower = try std.ascii.allocLowerString(allocator, opt_name);
        defer allocator.free(opt_lower);

        if (std.mem.eql(u8, opt_lower, "compression")) {
            idx += 1;
            if (idx >= args.len) {
                return protocol.RespValue{ .error_string = "ERR COMPRESSION requires a value" };
            }

            const compression_str = switch (args[idx]) {
                .bulk_string => |s| s,
                else => return protocol.RespValue{ .error_string = "ERR invalid compression value" },
            };

            const compression = std.fmt.parseInt(u32, compression_str, 10) catch {
                return protocol.RespValue{ .error_string = "ERR compression must be an integer" };
            };

            if (compression == 0) {
                return protocol.RespValue{ .error_string = "ERR compression must be greater than 0" };
            }

            compression_override = compression;
        } else if (std.mem.eql(u8, opt_lower, "override")) {
            override_flag = true;
        } else {
            return protocol.RespValue{ .error_string = "ERR unknown option" };
        }

        idx += 1;
    }

    // Check if destkey exists
    const dest_exists = storage.data.get(destkey) != null;

    if (dest_exists and !override_flag) {
        return protocol.RespValue{ .error_string = "BUSYKEY Target key name already exists" };
    }

    // Collect source sketches
    const sources = try allocator.alloc(*TDigestValue, numkeys);
    defer allocator.free(sources);

    // Validate all source keys and collect pointers
    var i: usize = 0;
    while (i < numkeys) : (i += 1) {
        const source_key = switch (args[3 + i]) {
            .bulk_string => |s| s,
            else => return protocol.RespValue{ .error_string = "ERR invalid source key" },
        };

        const source_value_ptr = storage.data.getPtr(source_key) orelse {
            return protocol.RespValue{ .error_string = "ERR no such key" };
        };

        if (source_value_ptr.* != .t_digest) {
            return protocol.RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
        }

        sources[i] = &source_value_ptr.t_digest;
    }

    // Create or get destination sketch
    var dest_td: TDigestValue = undefined;
    var need_to_store = false;

    if (dest_exists and override_flag) {
        // Create new dest BEFORE removing old one (atomicity)
        const new_td = try TDigestValue.init(allocator, compression_override orelse 100);
        errdefer new_td.deinit();

        // Now safe to remove old dest
        const old_kv = storage.data.fetchRemove(destkey).?;
        allocator.free(old_kv.key);
        var old_val = old_kv.value;
        old_val.deinit(allocator);

        dest_td = new_td;
        need_to_store = true;
    } else if (!dest_exists) {
        // Create new dest
        dest_td = try TDigestValue.init(allocator, compression_override orelse 100);
        errdefer dest_td.deinit();
        need_to_store = true;
    }

    // Perform merge
    try dest_td.merge(sources, compression_override);

    // Store destination if needed
    if (need_to_store) {
        const key_copy = try allocator.dupe(u8, destkey);
        errdefer allocator.free(key_copy);

        try storage.data.put(key_copy, Value{ .t_digest = dest_td });
    }

    return protocol.RespValue{ .simple_string = "OK" };
}

/// TDIGEST.QUANTILE key quantile [quantile ...]
/// Get estimated values at given quantiles (0.0-1.0)
pub fn cmdTdigestQuantile(allocator: std.mem.Allocator, storage: *Storage, args: []protocol.RespValue) !protocol.RespValue {
    if (args.len < 3) {
        return protocol.RespValue{ .error_string = "ERR wrong number of arguments for 'TDIGEST.QUANTILE' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid key" },
    };

    // Look up key
    const value_ptr = storage.data.getPtr(key) orelse {
        return protocol.RespValue{ .error_string = "ERR no such key" };
    };

    // Validate type
    if (value_ptr.* != .t_digest) {
        return protocol.RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    const td = &value_ptr.t_digest;

    // Parse quantiles and compute results
    var results = try std.ArrayList(protocol.RespValue).initCapacity(allocator, args.len - 2);
    errdefer {
        for (results.items) |item| {
            deinitRespValue(&item, allocator);
        }
        results.deinit(allocator);
    }

    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const quantile_str = switch (args[i]) {
            .bulk_string => |s| s,
            else => {
                for (results.items) |item| {
                    deinitRespValue(&item, allocator);
                }
                results.deinit(allocator);
                return protocol.RespValue{ .error_string = "ERR quantile must be a number" };
            },
        };

        const q = std.fmt.parseFloat(f64, quantile_str) catch {
            for (results.items) |item| {
                deinitRespValue(&item, allocator);
            }
            results.deinit(allocator);
            return protocol.RespValue{ .error_string = "ERR quantile must be a valid float" };
        };

        const result_value = td.quantile(q) catch |err| {
            for (results.items) |item| {
                deinitRespValue(&item, allocator);
            }
            results.deinit(allocator);
            return switch (err) {
                error.InvalidQuantile => protocol.RespValue{ .error_string = "ERR quantile must be in range [0.0, 1.0]" },
                error.EmptySketch => protocol.RespValue{ .error_string = "ERR sketch is empty" },
                else => protocol.RespValue{ .error_string = "ERR failed to compute quantile" },
            };
        };

        // Format as bulk string
        const buf = try allocator.alloc(u8, 32);
        errdefer allocator.free(buf);
        const formatted = try std.fmt.bufPrint(buf, "{d}", .{result_value});
        const owned = try allocator.dupe(u8, formatted);
        allocator.free(buf);

        try results.append(allocator, protocol.RespValue{ .bulk_string = owned });
    }

    return protocol.RespValue{ .array = try results.toOwnedSlice(allocator) };
}

/// TDIGEST.CDF key value [value ...]
/// Get cumulative distribution function values
pub fn cmdTdigestCdf(allocator: std.mem.Allocator, storage: *Storage, args: []protocol.RespValue) !protocol.RespValue {
    if (args.len < 3) {
        return protocol.RespValue{ .error_string = "ERR wrong number of arguments for 'TDIGEST.CDF' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid key" },
    };

    // Look up key
    const value_ptr = storage.data.getPtr(key) orelse {
        return protocol.RespValue{ .error_string = "ERR no such key" };
    };

    // Validate type
    if (value_ptr.* != .t_digest) {
        return protocol.RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    const td = &value_ptr.t_digest;

    // Parse values and compute CDF results
    var results = try std.ArrayList(protocol.RespValue).initCapacity(allocator, args.len - 2);
    errdefer {
        for (results.items) |item| {
            deinitRespValue(&item, allocator);
        }
        results.deinit(allocator);
    }

    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const value_str = switch (args[i]) {
            .bulk_string => |s| s,
            else => {
                for (results.items) |item| {
                    deinitRespValue(&item, allocator);
                }
                results.deinit(allocator);
                return protocol.RespValue{ .error_string = "ERR value must be a number" };
            },
        };

        const value = std.fmt.parseFloat(f64, value_str) catch {
            for (results.items) |item| {
                deinitRespValue(&item, allocator);
            }
            results.deinit(allocator);
            return protocol.RespValue{ .error_string = "ERR value must be a valid float" };
        };

        const cdf_value = td.cdf(value) catch |err| {
            for (results.items) |item| {
                deinitRespValue(&item, allocator);
            }
            results.deinit(allocator);
            return switch (err) {
                error.EmptySketch => protocol.RespValue{ .error_string = "ERR sketch is empty" },
                else => protocol.RespValue{ .error_string = "ERR failed to compute CDF" },
            };
        };

        // Format as bulk string
        const buf = try allocator.alloc(u8, 32);
        errdefer allocator.free(buf);
        const formatted = try std.fmt.bufPrint(buf, "{d}", .{cdf_value});
        const owned = try allocator.dupe(u8, formatted);
        allocator.free(buf);

        try results.append(allocator, protocol.RespValue{ .bulk_string = owned });
    }

    return protocol.RespValue{ .array = try results.toOwnedSlice(allocator) };
}

/// TDIGEST.MIN key
/// Get minimum value in sketch
pub fn cmdTdigestMin(allocator: std.mem.Allocator, storage: *Storage, args: []protocol.RespValue) !protocol.RespValue {
    _ = allocator;

    if (args.len != 2) {
        return protocol.RespValue{ .error_string = "ERR wrong number of arguments for 'TDIGEST.MIN' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid key" },
    };

    // Look up key
    const value_ptr = storage.data.getPtr(key) orelse {
        return protocol.RespValue{ .error_string = "ERR no such key" };
    };

    // Validate type
    if (value_ptr.* != .t_digest) {
        return protocol.RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    const td = &value_ptr.t_digest;

    // Empty sketch
    if (td.total_count == 0) {
        return protocol.RespValue{ .error_string = "ERR sketch is empty" };
    }

    // Format min value as bulk string
    var buf: [32]u8 = undefined;
    const formatted = try std.fmt.bufPrint(&buf, "{d}", .{td.min});
    const allocator_for_resp = storage.allocator;
    const owned = try allocator_for_resp.dupe(u8, formatted);

    return protocol.RespValue{ .bulk_string = owned };
}

/// TDIGEST.MAX key
/// Get maximum value in sketch
pub fn cmdTdigestMax(allocator: std.mem.Allocator, storage: *Storage, args: []protocol.RespValue) !protocol.RespValue {
    _ = allocator;

    if (args.len != 2) {
        return protocol.RespValue{ .error_string = "ERR wrong number of arguments for 'TDIGEST.MAX' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid key" },
    };

    // Look up key
    const value_ptr = storage.data.getPtr(key) orelse {
        return protocol.RespValue{ .error_string = "ERR no such key" };
    };

    // Validate type
    if (value_ptr.* != .t_digest) {
        return protocol.RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    const td = &value_ptr.t_digest;

    // Empty sketch
    if (td.total_count == 0) {
        return protocol.RespValue{ .error_string = "ERR sketch is empty" };
    }

    // Format max value as bulk string
    var buf: [32]u8 = undefined;
    const formatted = try std.fmt.bufPrint(&buf, "{d}", .{td.max});
    const allocator_for_resp = storage.allocator;
    const owned = try allocator_for_resp.dupe(u8, formatted);

    return protocol.RespValue{ .bulk_string = owned };
}

/// TDIGEST.RANK key value [value ...]
/// Returns the estimated rank (number of values less than the given value) for each value.
/// Rank is in range [0, total_count-1].
pub fn cmdTdigestRank(allocator: std.mem.Allocator, storage: *Storage, args: []protocol.RespValue) !protocol.RespValue {
    if (args.len < 3) {
        return protocol.RespValue{ .error_string = "ERR wrong number of arguments for 'TDIGEST.RANK' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid key" },
    };

    // Look up key
    const value_ptr = storage.data.getPtr(key) orelse {
        return protocol.RespValue{ .error_string = "ERR no such key" };
    };

    // Validate type
    if (value_ptr.* != .t_digest) {
        return protocol.RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    const td = &value_ptr.t_digest;

    // Parse all values and compute ranks
    const num_values = args.len - 2;
    var results = try std.ArrayList(protocol.RespValue).initCapacity(allocator, num_values);
    errdefer {
        for (results.items) |item| {
            deinitRespValue(&item, allocator);
        }
        results.deinit(allocator);
    }

    for (args[2..]) |arg| {
        const value_str = switch (arg) {
            .bulk_string => |s| s,
            else => {
                for (results.items) |item| {
                    deinitRespValue(&item, allocator);
                }
                results.deinit(allocator);
                return protocol.RespValue{ .error_string = "ERR invalid value" };
            },
        };

        const value = std.fmt.parseFloat(f64, value_str) catch {
            for (results.items) |item| {
                deinitRespValue(&item, allocator);
            }
            results.deinit(allocator);
            return protocol.RespValue{ .error_string = "ERR value is not a valid float" };
        };

        const rank_value = td.rank(value) catch {
            for (results.items) |item| {
                deinitRespValue(&item, allocator);
            }
            results.deinit(allocator);
            return protocol.RespValue{ .error_string = "ERR sketch is empty" };
        };

        try results.append(allocator, protocol.RespValue{ .integer = rank_value });
    }

    // Single result returns integer directly, multiple results return array
    if (num_values == 1) {
        const result = results.items[0];
        results.deinit(allocator);
        return result;
    } else {
        return protocol.RespValue{ .array = try results.toOwnedSlice(allocator) };
    }
}

/// TDIGEST.REVRANK key value [value ...]
/// Returns the estimated reverse rank (number of values greater than the given value) for each value.
/// Reverse rank is in range [0, total_count-1].
pub fn cmdTdigestRevrank(allocator: std.mem.Allocator, storage: *Storage, args: []protocol.RespValue) !protocol.RespValue {
    if (args.len < 3) {
        return protocol.RespValue{ .error_string = "ERR wrong number of arguments for 'TDIGEST.REVRANK' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid key" },
    };

    // Look up key
    const value_ptr = storage.data.getPtr(key) orelse {
        return protocol.RespValue{ .error_string = "ERR no such key" };
    };

    // Validate type
    if (value_ptr.* != .t_digest) {
        return protocol.RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    const td = &value_ptr.t_digest;

    // Parse all values and compute revranks
    const num_values = args.len - 2;
    var results = try std.ArrayList(protocol.RespValue).initCapacity(allocator, num_values);
    errdefer {
        for (results.items) |item| {
            deinitRespValue(&item, allocator);
        }
        results.deinit(allocator);
    }

    for (args[2..]) |arg| {
        const value_str = switch (arg) {
            .bulk_string => |s| s,
            else => {
                for (results.items) |item| {
                    deinitRespValue(&item, allocator);
                }
                results.deinit(allocator);
                return protocol.RespValue{ .error_string = "ERR invalid value" };
            },
        };

        const value = std.fmt.parseFloat(f64, value_str) catch {
            for (results.items) |item| {
                deinitRespValue(&item, allocator);
            }
            results.deinit(allocator);
            return protocol.RespValue{ .error_string = "ERR value is not a valid float" };
        };

        const revrank_value = td.revrank(value) catch {
            for (results.items) |item| {
                deinitRespValue(&item, allocator);
            }
            results.deinit(allocator);
            return protocol.RespValue{ .error_string = "ERR sketch is empty" };
        };

        try results.append(allocator, protocol.RespValue{ .integer = revrank_value });
    }

    // Single result returns integer directly, multiple results return array
    if (num_values == 1) {
        const result = results.items[0];
        results.deinit(allocator);
        return result;
    } else {
        return protocol.RespValue{ .array = try results.toOwnedSlice(allocator) };
    }
}

/// TDIGEST.BYRANK key rank [rank ...]
/// Returns the estimated value at the given rank position for each rank.
/// Rank must be in range [0, total_count-1].
pub fn cmdTdigestByrank(allocator: std.mem.Allocator, storage: *Storage, args: []protocol.RespValue) !protocol.RespValue {
    if (args.len < 3) {
        return protocol.RespValue{ .error_string = "ERR wrong number of arguments for 'TDIGEST.BYRANK' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid key" },
    };

    // Look up key
    const value_ptr = storage.data.getPtr(key) orelse {
        return protocol.RespValue{ .error_string = "ERR no such key" };
    };

    // Validate type
    if (value_ptr.* != .t_digest) {
        return protocol.RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    const td = &value_ptr.t_digest;

    // Parse all rank positions and get values
    const num_ranks = args.len - 2;
    var results = try std.ArrayList(protocol.RespValue).initCapacity(allocator, num_ranks);
    errdefer {
        for (results.items) |item| {
            deinitRespValue(&item, allocator);
        }
        results.deinit(allocator);
    }

    for (args[2..]) |arg| {
        const rank_str = switch (arg) {
            .bulk_string => |s| s,
            else => {
                for (results.items) |item| {
                    deinitRespValue(&item, allocator);
                }
                results.deinit(allocator);
                return protocol.RespValue{ .error_string = "ERR invalid rank" };
            },
        };

        const rank_pos = std.fmt.parseInt(i64, rank_str, 10) catch {
            for (results.items) |item| {
                deinitRespValue(&item, allocator);
            }
            results.deinit(allocator);
            return protocol.RespValue{ .error_string = "ERR rank must be an integer" };
        };

        const value = td.byrank(rank_pos) catch |err| {
            for (results.items) |item| {
                deinitRespValue(&item, allocator);
            }
            results.deinit(allocator);
            if (err == error.EmptySketch) {
                return protocol.RespValue{ .error_string = "ERR sketch is empty" };
            } else {
                return protocol.RespValue{ .error_string = "ERR rank out of range" };
            }
        };

        // Format as bulk string
        var buf: [32]u8 = undefined;
        const formatted = try std.fmt.bufPrint(&buf, "{d}", .{value});
        const owned = try allocator.dupe(u8, formatted);
        errdefer allocator.free(owned);

        try results.append(allocator, protocol.RespValue{ .bulk_string = owned });
    }

    // Single result returns bulk string directly, multiple results return array
    if (num_ranks == 1) {
        const result = results.items[0];
        results.deinit(allocator);
        return result;
    } else {
        return protocol.RespValue{ .array = try results.toOwnedSlice(allocator) };
    }
}

/// TDIGEST.BYREVRANK key rank [rank ...]
/// Returns the estimated value at the given reverse rank position for each rank.
/// Reverse rank must be in range [0, total_count-1].
pub fn cmdTdigestByrevrank(allocator: std.mem.Allocator, storage: *Storage, args: []protocol.RespValue) !protocol.RespValue {
    if (args.len < 3) {
        return protocol.RespValue{ .error_string = "ERR wrong number of arguments for 'TDIGEST.BYREVRANK' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid key" },
    };

    // Look up key
    const value_ptr = storage.data.getPtr(key) orelse {
        return protocol.RespValue{ .error_string = "ERR no such key" };
    };

    // Validate type
    if (value_ptr.* != .t_digest) {
        return protocol.RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    const td = &value_ptr.t_digest;

    // Parse all reverse rank positions and get values
    const num_ranks = args.len - 2;
    var results = try std.ArrayList(protocol.RespValue).initCapacity(allocator, num_ranks);
    errdefer {
        for (results.items) |item| {
            deinitRespValue(&item, allocator);
        }
        results.deinit(allocator);
    }

    for (args[2..]) |arg| {
        const revrank_str = switch (arg) {
            .bulk_string => |s| s,
            else => {
                for (results.items) |item| {
                    deinitRespValue(&item, allocator);
                }
                results.deinit(allocator);
                return protocol.RespValue{ .error_string = "ERR invalid rank" };
            },
        };

        const revrank_pos = std.fmt.parseInt(i64, revrank_str, 10) catch {
            for (results.items) |item| {
                deinitRespValue(&item, allocator);
            }
            results.deinit(allocator);
            return protocol.RespValue{ .error_string = "ERR rank must be an integer" };
        };

        const value = td.byrevrank(revrank_pos) catch |err| {
            for (results.items) |item| {
                deinitRespValue(&item, allocator);
            }
            results.deinit(allocator);
            if (err == error.EmptySketch) {
                return protocol.RespValue{ .error_string = "ERR sketch is empty" };
            } else {
                return protocol.RespValue{ .error_string = "ERR rank out of range" };
            }
        };

        // Format as bulk string
        var buf: [32]u8 = undefined;
        const formatted = try std.fmt.bufPrint(&buf, "{d}", .{value});
        const owned = try allocator.dupe(u8, formatted);
        errdefer allocator.free(owned);

        try results.append(allocator, protocol.RespValue{ .bulk_string = owned });
    }

    // Single result returns bulk string directly, multiple results return array
    if (num_ranks == 1) {
        const result = results.items[0];
        results.deinit(allocator);
        return result;
    } else {
        return protocol.RespValue{ .array = try results.toOwnedSlice(allocator) };
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

test "cmdTdigestCreate basic" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
    };

    const result = try cmdTdigestCreate(allocator, &storage, &args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.simple_string, result);
    try std.testing.expectEqualStrings("OK", result.simple_string);

    // Verify stored
    const stored = storage.data.get("mydigest");
    try std.testing.expect(stored != null);
    try std.testing.expectEqual(100, stored.?.t_digest.compression);
}

test "cmdTdigestCreate with compression parameter" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
        protocol.RespValue{ .bulk_string = "COMPRESSION" },
        protocol.RespValue{ .bulk_string = "250" },
    };

    const result = try cmdTdigestCreate(allocator, &storage, &args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.simple_string, result);

    const stored = storage.data.get("mydigest");
    try std.testing.expect(stored != null);
    try std.testing.expectEqual(250, stored.?.t_digest.compression);
}

test "cmdTdigestCreate rejects duplicate key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args1 = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
    };

    _ = try cmdTdigestCreate(allocator, &storage, &args1);

    const args2 = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
    };

    const result = try cmdTdigestCreate(allocator, &storage, &args2);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.error_string, result);
}

test "cmdTdigestCreate invalid compression" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
        protocol.RespValue{ .bulk_string = "COMPRESSION" },
        protocol.RespValue{ .bulk_string = "0" },
    };

    const result = try cmdTdigestCreate(allocator, &storage, &args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.error_string, result);
}

test "cmdTdigestAdd single value" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create first
    const create_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create_args);

    // Add value
    const add_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.ADD" },
        protocol.RespValue{ .bulk_string = "mydigest" },
        protocol.RespValue{ .bulk_string = "42.5" },
    };

    const result = try cmdTdigestAdd(allocator, &storage, &add_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.simple_string, result);
    try std.testing.expectEqualStrings("OK", result.simple_string);

    const stored = storage.data.get("mydigest").?.t_digest;
    try std.testing.expectEqual(1, stored.total_count);
}

test "cmdTdigestAdd multiple values" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const create_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create_args);

    const add_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.ADD" },
        protocol.RespValue{ .bulk_string = "mydigest" },
        protocol.RespValue{ .bulk_string = "1.0" },
        protocol.RespValue{ .bulk_string = "2.0" },
        protocol.RespValue{ .bulk_string = "3.0" },
    };

    const result = try cmdTdigestAdd(allocator, &storage, &add_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.simple_string, result);

    const stored = storage.data.get("mydigest").?.t_digest;
    try std.testing.expectEqual(3, stored.total_count);
}

test "cmdTdigestAdd no auto-create" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const add_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.ADD" },
        protocol.RespValue{ .bulk_string = "nonexistent" },
        protocol.RespValue{ .bulk_string = "1.0" },
    };

    const result = try cmdTdigestAdd(allocator, &storage, &add_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.error_string, result);
}

test "cmdTdigestAdd wrong type" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create a string instead
    const key_copy = try allocator.dupe(u8, "mykey");
    try storage.data.put(key_copy, Value{ .string = .{
        .data = try allocator.dupe(u8, "hello"),
        .expires_at = null,
    } });

    const add_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.ADD" },
        protocol.RespValue{ .bulk_string = "mykey" },
        protocol.RespValue{ .bulk_string = "1.0" },
    };

    const result = try cmdTdigestAdd(allocator, &storage, &add_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.error_string, result);
}

test "cmdTdigestReset" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create
    const create_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
        protocol.RespValue{ .bulk_string = "COMPRESSION" },
        protocol.RespValue{ .bulk_string = "250" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create_args);

    // Add values
    const add_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.ADD" },
        protocol.RespValue{ .bulk_string = "mydigest" },
        protocol.RespValue{ .bulk_string = "1.0" },
        protocol.RespValue{ .bulk_string = "2.0" },
    };
    _ = try cmdTdigestAdd(allocator, &storage, &add_args);

    var stored = storage.data.get("mydigest").?.t_digest;
    try std.testing.expectEqual(2, stored.total_count);

    // Reset
    const reset_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.RESET" },
        protocol.RespValue{ .bulk_string = "mydigest" },
    };

    const result = try cmdTdigestReset(allocator, &storage, &reset_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.simple_string, result);

    stored = storage.data.get("mydigest").?.t_digest;
    try std.testing.expectEqual(0, stored.total_count);
    try std.testing.expectEqual(250, stored.compression); // Preserved
}

test "cmdTdigestReset nonexistent key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const reset_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.RESET" },
        protocol.RespValue{ .bulk_string = "nonexistent" },
    };

    const result = try cmdTdigestReset(allocator, &storage, &reset_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.error_string, result);
}

// ============================================================================
// TDIGEST.MERGE Tests (Iteration 227)
// ============================================================================

test "cmdTdigestMerge basic two sources" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create source sketches
    const create1 = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "src1" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create1);

    const create2 = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "src2" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create2);

    // Add values to sources
    const add1 = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.ADD" },
        protocol.RespValue{ .bulk_string = "src1" },
        protocol.RespValue{ .bulk_string = "10.0" },
        protocol.RespValue{ .bulk_string = "20.0" },
    };
    _ = try cmdTdigestAdd(allocator, &storage, &add1);

    const add2 = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.ADD" },
        protocol.RespValue{ .bulk_string = "src2" },
        protocol.RespValue{ .bulk_string = "30.0" },
    };
    _ = try cmdTdigestAdd(allocator, &storage, &add2);

    // Merge into dest
    const merge_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.MERGE" },
        protocol.RespValue{ .bulk_string = "dest" },
        protocol.RespValue{ .bulk_string = "2" },
        protocol.RespValue{ .bulk_string = "src1" },
        protocol.RespValue{ .bulk_string = "src2" },
    };

    const result = try cmdTdigestMerge(allocator, &storage, &merge_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.simple_string, result);
    try std.testing.expectEqualStrings("OK", result.simple_string);

    // Verify merged result
    const merged = storage.data.get("dest").?.t_digest;
    try std.testing.expectEqual(3, merged.total_count);
}

test "cmdTdigestMerge with COMPRESSION override" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create source
    const create = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "src" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create);

    const add = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.ADD" },
        protocol.RespValue{ .bulk_string = "src" },
        protocol.RespValue{ .bulk_string = "42.0" },
    };
    _ = try cmdTdigestAdd(allocator, &storage, &add);

    // Merge with COMPRESSION override
    const merge_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.MERGE" },
        protocol.RespValue{ .bulk_string = "dest" },
        protocol.RespValue{ .bulk_string = "1" },
        protocol.RespValue{ .bulk_string = "src" },
        protocol.RespValue{ .bulk_string = "COMPRESSION" },
        protocol.RespValue{ .bulk_string = "250" },
    };

    const result = try cmdTdigestMerge(allocator, &storage, &merge_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.simple_string, result);

    const merged = storage.data.get("dest").?.t_digest;
    try std.testing.expectEqual(250, merged.compression);
}

test "cmdTdigestMerge with OVERRIDE flag" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create dest and source
    const create_dest = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "dest" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create_dest);

    const add_dest = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.ADD" },
        protocol.RespValue{ .bulk_string = "dest" },
        protocol.RespValue{ .bulk_string = "999.0" },
    };
    _ = try cmdTdigestAdd(allocator, &storage, &add_dest);

    const create_src = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "src" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create_src);

    const add_src = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.ADD" },
        protocol.RespValue{ .bulk_string = "src" },
        protocol.RespValue{ .bulk_string = "42.0" },
    };
    _ = try cmdTdigestAdd(allocator, &storage, &add_src);

    // Merge with OVERRIDE should replace dest
    const merge_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.MERGE" },
        protocol.RespValue{ .bulk_string = "dest" },
        protocol.RespValue{ .bulk_string = "1" },
        protocol.RespValue{ .bulk_string = "src" },
        protocol.RespValue{ .bulk_string = "OVERRIDE" },
    };

    const result = try cmdTdigestMerge(allocator, &storage, &merge_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.simple_string, result);

    // Should only have src values, not old dest value
    const merged = storage.data.get("dest").?.t_digest;
    try std.testing.expectEqual(1, merged.total_count);
    try std.testing.expectEqual(42.0, merged.min);
}

test "cmdTdigestMerge error when dest exists without OVERRIDE" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create dest
    const create_dest = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "dest" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create_dest);

    const create_src = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "src" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create_src);

    // Merge without OVERRIDE should fail
    const merge_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.MERGE" },
        protocol.RespValue{ .bulk_string = "dest" },
        protocol.RespValue{ .bulk_string = "1" },
        protocol.RespValue{ .bulk_string = "src" },
    };

    const result = try cmdTdigestMerge(allocator, &storage, &merge_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.error_string, result);
}

test "cmdTdigestMerge numkeys mismatch" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const create = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "src" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create);

    // numkeys=2 but only 1 source key provided
    const merge_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.MERGE" },
        protocol.RespValue{ .bulk_string = "dest" },
        protocol.RespValue{ .bulk_string = "2" },
        protocol.RespValue{ .bulk_string = "src" },
    };

    const result = try cmdTdigestMerge(allocator, &storage, &merge_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.error_string, result);
}

test "cmdTdigestMerge invalid numkeys" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const merge_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.MERGE" },
        protocol.RespValue{ .bulk_string = "dest" },
        protocol.RespValue{ .bulk_string = "notanumber" },
    };

    const result = try cmdTdigestMerge(allocator, &storage, &merge_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.error_string, result);
}

test "cmdTdigestMerge zero numkeys" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const merge_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.MERGE" },
        protocol.RespValue{ .bulk_string = "dest" },
        protocol.RespValue{ .bulk_string = "0" },
    };

    const result = try cmdTdigestMerge(allocator, &storage, &merge_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.error_string, result);
}

test "cmdTdigestMerge nonexistent source key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const merge_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.MERGE" },
        protocol.RespValue{ .bulk_string = "dest" },
        protocol.RespValue{ .bulk_string = "1" },
        protocol.RespValue{ .bulk_string = "nonexistent" },
    };

    const result = try cmdTdigestMerge(allocator, &storage, &merge_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.error_string, result);
}

test "cmdTdigestMerge source wrong type" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create a string instead of T-Digest
    const key_copy = try allocator.dupe(u8, "notdigest");
    try storage.data.put(key_copy, Value{ .string = .{
        .data = try allocator.dupe(u8, "hello"),
        .expires_at = null,
    } });

    const merge_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.MERGE" },
        protocol.RespValue{ .bulk_string = "dest" },
        protocol.RespValue{ .bulk_string = "1" },
        protocol.RespValue{ .bulk_string = "notdigest" },
    };

    const result = try cmdTdigestMerge(allocator, &storage, &merge_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.error_string, result);
}

test "cmdTdigestMerge invalid compression value" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const create = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "src" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create);

    const merge_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.MERGE" },
        protocol.RespValue{ .bulk_string = "dest" },
        protocol.RespValue{ .bulk_string = "1" },
        protocol.RespValue{ .bulk_string = "src" },
        protocol.RespValue{ .bulk_string = "COMPRESSION" },
        protocol.RespValue{ .bulk_string = "0" },
    };

    const result = try cmdTdigestMerge(allocator, &storage, &merge_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.error_string, result);
}

test "cmdTdigestMerge three sources" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create three sources
    var i: u8 = 1;
    while (i <= 3) : (i += 1) {
        const key_buf = try std.fmt.allocPrint(allocator, "src{d}", .{i});
        defer allocator.free(key_buf);

        const create = [_]protocol.RespValue{
            protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
            protocol.RespValue{ .bulk_string = key_buf },
        };
        _ = try cmdTdigestCreate(allocator, &storage, &create);

        const value_buf = try std.fmt.allocPrint(allocator, "{d}.0", .{i * 10});
        defer allocator.free(value_buf);

        const add = [_]protocol.RespValue{
            protocol.RespValue{ .bulk_string = "TDIGEST.ADD" },
            protocol.RespValue{ .bulk_string = key_buf },
            protocol.RespValue{ .bulk_string = value_buf },
        };
        _ = try cmdTdigestAdd(allocator, &storage, &add);
    }

    const merge_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.MERGE" },
        protocol.RespValue{ .bulk_string = "dest" },
        protocol.RespValue{ .bulk_string = "3" },
        protocol.RespValue{ .bulk_string = "src1" },
        protocol.RespValue{ .bulk_string = "src2" },
        protocol.RespValue{ .bulk_string = "src3" },
    };

    const result = try cmdTdigestMerge(allocator, &storage, &merge_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.simple_string, result);

    const merged = storage.data.get("dest").?.t_digest;
    try std.testing.expectEqual(3, merged.total_count);
}

test "cmdTdigestMerge too few arguments" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const merge_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.MERGE" },
        protocol.RespValue{ .bulk_string = "dest" },
    };

    const result = try cmdTdigestMerge(allocator, &storage, &merge_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.error_string, result);
}

// ============================================================================
// TDIGEST.QUANTILE/CDF/MIN/MAX Tests (Iteration 228)
// ============================================================================

test "cmdTdigestQuantile single quantile" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    // Create and populate
    const create_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create_args);

    const add_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.ADD" },
        protocol.RespValue{ .bulk_string = "mydigest" },
        protocol.RespValue{ .bulk_string = "10.0" },
        protocol.RespValue{ .bulk_string = "20.0" },
        protocol.RespValue{ .bulk_string = "30.0" },
    };
    _ = try cmdTdigestAdd(allocator, &storage, &add_args);

    // Query quantile
    const quantile_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.QUANTILE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
        protocol.RespValue{ .bulk_string = "0.5" },
    };

    const result = try cmdTdigestQuantile(allocator, &storage, &quantile_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.array, result);
    try std.testing.expectEqual(1, result.array.len);
}

test "cmdTdigestQuantile multiple quantiles" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const create_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create_args);

    const add_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.ADD" },
        protocol.RespValue{ .bulk_string = "mydigest" },
        protocol.RespValue{ .bulk_string = "10.0" },
        protocol.RespValue{ .bulk_string = "20.0" },
    };
    _ = try cmdTdigestAdd(allocator, &storage, &add_args);

    const quantile_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.QUANTILE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
        protocol.RespValue{ .bulk_string = "0.0" },
        protocol.RespValue{ .bulk_string = "0.5" },
        protocol.RespValue{ .bulk_string = "1.0" },
    };

    const result = try cmdTdigestQuantile(allocator, &storage, &quantile_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.array, result);
    try std.testing.expectEqual(3, result.array.len);
}

test "cmdTdigestQuantile nonexistent key" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const quantile_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.QUANTILE" },
        protocol.RespValue{ .bulk_string = "nonexistent" },
        protocol.RespValue{ .bulk_string = "0.5" },
    };

    const result = try cmdTdigestQuantile(allocator, &storage, &quantile_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.error_string, result);
}

test "cmdTdigestQuantile invalid quantile" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const create_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create_args);

    const add_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.ADD" },
        protocol.RespValue{ .bulk_string = "mydigest" },
        protocol.RespValue{ .bulk_string = "42.0" },
    };
    _ = try cmdTdigestAdd(allocator, &storage, &add_args);

    const quantile_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.QUANTILE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
        protocol.RespValue{ .bulk_string = "1.5" },
    };

    const result = try cmdTdigestQuantile(allocator, &storage, &quantile_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.error_string, result);
}

test "cmdTdigestCdf single value" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const create_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create_args);

    const add_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.ADD" },
        protocol.RespValue{ .bulk_string = "mydigest" },
        protocol.RespValue{ .bulk_string = "10.0" },
        protocol.RespValue{ .bulk_string = "20.0" },
        protocol.RespValue{ .bulk_string = "30.0" },
    };
    _ = try cmdTdigestAdd(allocator, &storage, &add_args);

    const cdf_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CDF" },
        protocol.RespValue{ .bulk_string = "mydigest" },
        protocol.RespValue{ .bulk_string = "20.0" },
    };

    const result = try cmdTdigestCdf(allocator, &storage, &cdf_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.array, result);
    try std.testing.expectEqual(1, result.array.len);
}

test "cmdTdigestCdf multiple values" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const create_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create_args);

    const add_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.ADD" },
        protocol.RespValue{ .bulk_string = "mydigest" },
        protocol.RespValue{ .bulk_string = "10.0" },
        protocol.RespValue{ .bulk_string = "50.0" },
    };
    _ = try cmdTdigestAdd(allocator, &storage, &add_args);

    const cdf_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CDF" },
        protocol.RespValue{ .bulk_string = "mydigest" },
        protocol.RespValue{ .bulk_string = "5.0" },
        protocol.RespValue{ .bulk_string = "30.0" },
        protocol.RespValue{ .bulk_string = "100.0" },
    };

    const result = try cmdTdigestCdf(allocator, &storage, &cdf_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.array, result);
    try std.testing.expectEqual(3, result.array.len);
}

test "cmdTdigestMin basic" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const create_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create_args);

    const add_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.ADD" },
        protocol.RespValue{ .bulk_string = "mydigest" },
        protocol.RespValue{ .bulk_string = "10.0" },
        protocol.RespValue{ .bulk_string = "20.0" },
        protocol.RespValue{ .bulk_string = "5.0" },
    };
    _ = try cmdTdigestAdd(allocator, &storage, &add_args);

    const min_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.MIN" },
        protocol.RespValue{ .bulk_string = "mydigest" },
    };

    const result = try cmdTdigestMin(allocator, &storage, &min_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.bulk_string, result);
}

test "cmdTdigestMax basic" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const create_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create_args);

    const add_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.ADD" },
        protocol.RespValue{ .bulk_string = "mydigest" },
        protocol.RespValue{ .bulk_string = "10.0" },
        protocol.RespValue{ .bulk_string = "20.0" },
        protocol.RespValue{ .bulk_string = "50.0" },
    };
    _ = try cmdTdigestAdd(allocator, &storage, &add_args);

    const max_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.MAX" },
        protocol.RespValue{ .bulk_string = "mydigest" },
    };

    const result = try cmdTdigestMax(allocator, &storage, &max_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.bulk_string, result);
}

test "cmdTdigestMin empty sketch" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const create_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.CREATE" },
        protocol.RespValue{ .bulk_string = "mydigest" },
    };
    _ = try cmdTdigestCreate(allocator, &storage, &create_args);

    const min_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.MIN" },
        protocol.RespValue{ .bulk_string = "mydigest" },
    };

    const result = try cmdTdigestMin(allocator, &storage, &min_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.error_string, result);
}

test "cmdTdigestMax wrong type" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const key_copy = try allocator.dupe(u8, "notdigest");
    try storage.data.put(key_copy, Value{ .string = .{
        .data = try allocator.dupe(u8, "hello"),
        .expires_at = null,
    } });

    const max_args = [_]protocol.RespValue{
        protocol.RespValue{ .bulk_string = "TDIGEST.MAX" },
        protocol.RespValue{ .bulk_string = "notdigest" },
    };

    const result = try cmdTdigestMax(allocator, &storage, &max_args);
    defer deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.error_string, result);
}

// ============================================================================
// TDIGEST.INFO & TDIGEST.TRIMMED_MEAN (Iteration 230)
// ============================================================================

/// TDIGEST.INFO key
/// Returns information about the T-Digest sketch including compression, capacity, nodes, weight, memory usage.
pub fn cmdTdigestInfo(allocator: std.mem.Allocator, storage: *Storage, args: []protocol.RespValue) !protocol.RespValue {
    if (args.len != 2) {
        return protocol.RespValue{ .error_string = "ERR wrong number of arguments for 'TDIGEST.INFO' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid key" },
    };

    // Look up key
    const value_ptr = storage.data.getPtr(key) orelse {
        return protocol.RespValue{ .error_string = "ERR no such key" };
    };

    // Validate type
    if (value_ptr.* != .t_digest) {
        return protocol.RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    const td = &value_ptr.t_digest;
    const info = td.getInfo();

    // Build RESP2 array response: alternating field names and values
    // [Compression, 100, Capacity, 610, Merged nodes, 0, Unmerged nodes, 5, ...]
    var results = try std.ArrayList(protocol.RespValue).initCapacity(allocator, 18);
    errdefer results.deinit(allocator);

    // Compression
    try results.append(allocator, protocol.RespValue{ .bulk_string = try allocator.dupe(u8, "Compression") });
    try results.append(allocator, protocol.RespValue{ .integer = @as(i64, @intCast(info.compression)) });

    // Capacity
    try results.append(allocator, protocol.RespValue{ .bulk_string = try allocator.dupe(u8, "Capacity") });
    try results.append(allocator, protocol.RespValue{ .integer = @as(i64, @intCast(info.capacity)) });

    // Merged nodes
    try results.append(allocator, protocol.RespValue{ .bulk_string = try allocator.dupe(u8, "Merged nodes") });
    try results.append(allocator, protocol.RespValue{ .integer = @as(i64, @intCast(info.merged_nodes)) });

    // Unmerged nodes
    try results.append(allocator, protocol.RespValue{ .bulk_string = try allocator.dupe(u8, "Unmerged nodes") });
    try results.append(allocator, protocol.RespValue{ .integer = @as(i64, @intCast(info.unmerged_nodes)) });

    // Merged weight
    try results.append(allocator, protocol.RespValue{ .bulk_string = try allocator.dupe(u8, "Merged weight") });
    try results.append(allocator, protocol.RespValue{ .integer = @as(i64, @intCast(info.merged_weight)) });

    // Unmerged weight
    try results.append(allocator, protocol.RespValue{ .bulk_string = try allocator.dupe(u8, "Unmerged weight") });
    try results.append(allocator, protocol.RespValue{ .integer = @as(i64, @intCast(info.unmerged_weight)) });

    // Observations (same as unmerged_weight in simplified implementation)
    try results.append(allocator, protocol.RespValue{ .bulk_string = try allocator.dupe(u8, "Observations") });
    try results.append(allocator, protocol.RespValue{ .integer = @as(i64, @intCast(info.unmerged_weight)) });

    // Total compressions
    try results.append(allocator, protocol.RespValue{ .bulk_string = try allocator.dupe(u8, "Total compressions") });
    try results.append(allocator, protocol.RespValue{ .integer = @as(i64, @intCast(info.total_compressions)) });

    // Memory usage
    try results.append(allocator, protocol.RespValue{ .bulk_string = try allocator.dupe(u8, "Memory usage") });
    try results.append(allocator, protocol.RespValue{ .integer = @as(i64, @intCast(info.memory_usage)) });

    return protocol.RespValue{ .array = try results.toOwnedSlice(allocator) };
}

/// TDIGEST.TRIMMED_MEAN key low_cut_quantile high_cut_quantile
/// Returns the mean value excluding observations outside the specified quantile range.
pub fn cmdTdigestTrimmedMean(allocator: std.mem.Allocator, storage: *Storage, args: []protocol.RespValue) !protocol.RespValue {
    if (args.len != 4) {
        return protocol.RespValue{ .error_string = "ERR wrong number of arguments for 'TDIGEST.TRIMMED_MEAN' command" };
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid key" },
    };

    // Look up key
    const value_ptr = storage.data.getPtr(key) orelse {
        return protocol.RespValue{ .error_string = "ERR no such key" };
    };

    // Validate type
    if (value_ptr.* != .t_digest) {
        return protocol.RespValue{ .error_string = "WRONGTYPE Operation against a key holding the wrong kind of value" };
    }

    const td = &value_ptr.t_digest;

    // Parse low_cut_quantile
    const low_cut_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid low_cut_quantile" },
    };

    const low_cut = std.fmt.parseFloat(f64, low_cut_str) catch {
        return protocol.RespValue{ .error_string = "ERR low_cut_quantile must be a valid float" };
    };

    // Parse high_cut_quantile
    const high_cut_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return protocol.RespValue{ .error_string = "ERR invalid high_cut_quantile" },
    };

    const high_cut = std.fmt.parseFloat(f64, high_cut_str) catch {
        return protocol.RespValue{ .error_string = "ERR high_cut_quantile must be a valid float" };
    };

    // Calculate trimmed mean
    const mean = td.trimmedMean(low_cut, high_cut) catch |err| {
        return switch (err) {
            error.EmptySketch => protocol.RespValue{ .bulk_string = try allocator.dupe(u8, "nan") },
            error.InvalidQuantile => protocol.RespValue{ .error_string = "ERR quantiles must be in range [0, 1] and low_cut < high_cut" },
            error.InvalidCompression, error.InvalidValue => protocol.RespValue{ .error_string = "ERR internal error" },
            error.OutOfMemory => protocol.RespValue{ .error_string = "ERR out of memory" },
        };
    };

    // Return as bulk string (Redis spec for TRIMMED_MEAN)
    // Handle NaN case (all values trimmed)
    if (std.math.isNan(mean)) {
        return protocol.RespValue{ .bulk_string = try allocator.dupe(u8, "nan") };
    }

    var buf: [64]u8 = undefined;
    const mean_str = try std.fmt.bufPrint(&buf, "{d}", .{mean});
    return protocol.RespValue{ .bulk_string = try allocator.dupe(u8, mean_str) };
}
