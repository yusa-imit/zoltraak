const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const Storage = @import("../storage/memory.zig").Storage;
const Value = @import("../storage/memory.zig").Value;
const TDigestValue = @import("../storage/tdigest.zig").TDigestValue;
const RespProtocol = @import("client.zig").RespProtocol;

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

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
    defer protocol.deinitRespValue(result, allocator);

    try std.testing.expectEqual(protocol.RespValueType.error_string, result);
}
