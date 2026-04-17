const std = @import("std");
const Storage = @import("../storage/memory.zig").Storage;
const Value = @import("../storage/memory.zig").Value;
const TimeSeriesValue = @import("../storage/memory.zig").TimeSeriesValue;
const timeseries_mod = @import("../storage/timeseries.zig");
const DuplicatePolicy = timeseries_mod.DuplicatePolicy;
const Encoding = timeseries_mod.Encoding;

/// Parse a timestamp argument, handling special "*" for current time
fn parseTimestamp(arg: []const u8) !i64 {
    if (std.mem.eql(u8, arg, "*")) {
        return std.time.milliTimestamp();
    }
    return std.fmt.parseInt(i64, arg, 10) catch return error.InvalidTimestamp;
}

/// Parse a floating-point value for a data point
fn parseValue(arg: []const u8) !f64 {
    const value = std.fmt.parseFloat(f64, arg) catch return error.InvalidValue;
    if (std.math.isNan(value)) return error.InvalidValue;
    return value;
}

/// TS.CREATE key [RETENTION retentionPeriod] [ENCODING <COMPRESSED|UNCOMPRESSED>] [CHUNK_SIZE size] [DUPLICATE_POLICY <BLOCK|FIRST|LAST|MIN|MAX|SUM>] [LABELS label value [label value ...]]
///
/// Create a new time series with optional configuration.
/// Returns: OK on success, error if key already exists
///
/// Example:
/// TS.CREATE sensor:temp RETENTION 86400000 ENCODING COMPRESSED DUPLICATE_POLICY LAST LABELS sensor temp location room1
pub fn cmdTsCreate(
    storage: *Storage,
    args: []const []const u8,
    arena: std.mem.Allocator,
) ![]const u8 {
    _ = arena; // Not needed for this command

    if (args.len < 2) {
        return "ERR wrong number of arguments for 'TS.CREATE' command\r\n";
    }

    const key = args[1];

    // Parse optional arguments
    var retention_ms: i64 = 0;
    var encoding: Encoding = .uncompressed;
    var chunk_size: u32 = 4096;
    var duplicate_policy: DuplicatePolicy = .last;
    var labels = try std.ArrayList(struct { key: []const u8, value: []const u8 }).initCapacity(storage.allocator, 10);
    defer labels.deinit(storage.allocator);

    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const option = args[i];

        if (std.ascii.eqlIgnoreCase(option, "RETENTION")) {
            i += 1;
            if (i >= args.len) return "ERR RETENTION requires a value\r\n";
            retention_ms = std.fmt.parseInt(i64, args[i], 10) catch {
                return "ERR invalid RETENTION value\r\n";
            };
            if (retention_ms < 0) return "ERR RETENTION must be non-negative\r\n";
        } else if (std.ascii.eqlIgnoreCase(option, "ENCODING")) {
            i += 1;
            if (i >= args.len) return "ERR ENCODING requires a value\r\n";
            encoding = Encoding.fromString(args[i]) orelse {
                return "ERR invalid ENCODING value (must be COMPRESSED or UNCOMPRESSED)\r\n";
            };
        } else if (std.ascii.eqlIgnoreCase(option, "CHUNK_SIZE")) {
            i += 1;
            if (i >= args.len) return "ERR CHUNK_SIZE requires a value\r\n";
            chunk_size = std.fmt.parseInt(u32, args[i], 10) catch {
                return "ERR invalid CHUNK_SIZE value\r\n";
            };
            if (chunk_size == 0) return "ERR CHUNK_SIZE must be positive\r\n";
        } else if (std.ascii.eqlIgnoreCase(option, "DUPLICATE_POLICY")) {
            i += 1;
            if (i >= args.len) return "ERR DUPLICATE_POLICY requires a value\r\n";
            duplicate_policy = DuplicatePolicy.fromString(args[i]) orelse {
                return "ERR invalid DUPLICATE_POLICY value\r\n";
            };
        } else if (std.ascii.eqlIgnoreCase(option, "LABELS")) {
            i += 1;
            // Parse label key-value pairs
            while (i + 1 < args.len) {
                // Check if next arg is a known option
                const next = args[i];
                if (std.ascii.eqlIgnoreCase(next, "RETENTION") or
                    std.ascii.eqlIgnoreCase(next, "ENCODING") or
                    std.ascii.eqlIgnoreCase(next, "CHUNK_SIZE") or
                    std.ascii.eqlIgnoreCase(next, "DUPLICATE_POLICY"))
                {
                    i -= 1; // Back up so outer loop can process this option
                    break;
                }

                const label_key = args[i];
                i += 1;
                if (i >= args.len) return "ERR LABELS requires key-value pairs\r\n";
                const label_value = args[i];
                try labels.append(storage.allocator, .{ .key = label_key, .value = label_value });
                i += 1;
            }
            i -= 1; // Adjust for outer loop increment
        } else {
            return "ERR unknown option for TS.CREATE\r\n";
        }
    }

    // Check if key already exists
    if (storage.get(key)) |_| {
        return "ERR key already exists\r\n";
    }

    // Create time series value
    var ts = try TimeSeriesValue.init(storage.allocator);
    errdefer ts.deinit();

    ts.info.retention_ms = retention_ms;
    ts.info.encoding = encoding;
    ts.info.chunk_size = chunk_size;
    ts.info.duplicate_policy = duplicate_policy;

    // Add labels
    for (labels.items) |label| {
        try ts.info.setLabel(storage.allocator, label.key, label.value);
    }

    // Store in database
    const key_owned = try storage.allocator.dupe(u8, key);
    errdefer storage.allocator.free(key_owned);
    try storage.data.put(key_owned, Value{ .timeseries = ts });

    return "+OK\r\n";
}

/// TS.INFO key
///
/// Get metadata and configuration for a time series.
/// Returns array of key-value pairs with time series information.
pub fn cmdTsInfo(
    storage: *Storage,
    args: []const []const u8,
    arena: std.mem.Allocator,
) ![]const u8 {
    _ = arena;

    if (args.len != 2) {
        return "ERR wrong number of arguments for 'TS.INFO' command\r\n";
    }

    const key = args[1];

    const val = storage.data.get(key) orelse {
        return "ERR key does not exist\r\n";
    };

    if (val != .timeseries) {
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    const ts = &val.timeseries;

    // Format response as RESP array
    var buf = try std.ArrayList(u8).initCapacity(storage.allocator, 512);
    defer buf.deinit(storage.allocator);

    const writer = buf.writer(storage.allocator);

    // Array of 18 elements (9 key-value pairs)
    try writer.writeAll("*18\r\n");

    // totalSamples
    try writer.writeAll("$12\r\ntotalSamples\r\n");
    const samples_str = try std.fmt.allocPrint(storage.allocator, ":{d}\r\n", .{ts.info.total_samples});
    defer storage.allocator.free(samples_str);
    try writer.writeAll(samples_str);

    // memoryUsage
    try writer.writeAll("$11\r\nmemoryUsage\r\n");
    const memory_str = try std.fmt.allocPrint(storage.allocator, ":{d}\r\n", .{ts.info.memory_bytes});
    defer storage.allocator.free(memory_str);
    try writer.writeAll(memory_str);

    // firstTimestamp
    try writer.writeAll("$14\r\nfirstTimestamp\r\n");
    if (ts.info.first_timestamp) |first_ts| {
        const first_str = try std.fmt.allocPrint(storage.allocator, ":{d}\r\n", .{first_ts});
        defer storage.allocator.free(first_str);
        try writer.writeAll(first_str);
    } else {
        try writer.writeAll("$-1\r\n"); // null
    }

    // lastTimestamp
    try writer.writeAll("$13\r\nlastTimestamp\r\n");
    if (ts.info.last_timestamp) |last_ts| {
        const last_str = try std.fmt.allocPrint(storage.allocator, ":{d}\r\n", .{last_ts});
        defer storage.allocator.free(last_str);
        try writer.writeAll(last_str);
    } else {
        try writer.writeAll("$-1\r\n"); // null
    }

    // retentionTime
    try writer.writeAll("$13\r\nretentionTime\r\n");
    const retention_str = try std.fmt.allocPrint(storage.allocator, ":{d}\r\n", .{ts.info.retention_ms});
    defer storage.allocator.free(retention_str);
    try writer.writeAll(retention_str);

    // chunkSize
    try writer.writeAll("$9\r\nchunkSize\r\n");
    const chunk_str = try std.fmt.allocPrint(storage.allocator, ":{d}\r\n", .{ts.info.chunk_size});
    defer storage.allocator.free(chunk_str);
    try writer.writeAll(chunk_str);

    // duplicatePolicy
    try writer.writeAll("$15\r\nduplicatePolicy\r\n");
    const policy_name = ts.info.duplicate_policy.toString();
    const policy_str = try std.fmt.allocPrint(storage.allocator, "${d}\r\n{s}\r\n", .{ policy_name.len, policy_name });
    defer storage.allocator.free(policy_str);
    try writer.writeAll(policy_str);

    // encoding
    try writer.writeAll("$8\r\nencoding\r\n");
    const encoding_name = ts.info.encoding.toString();
    const encoding_str = try std.fmt.allocPrint(storage.allocator, "${d}\r\n{s}\r\n", .{ encoding_name.len, encoding_name });
    defer storage.allocator.free(encoding_str);
    try writer.writeAll(encoding_str);

    // labels (array of label key-value pairs)
    try writer.writeAll("$6\r\nlabels\r\n");
    const label_count = ts.info.labels.count();
    const labels_array_str = try std.fmt.allocPrint(storage.allocator, "*{d}\r\n", .{label_count * 2});
    defer storage.allocator.free(labels_array_str);
    try writer.writeAll(labels_array_str);

    var it = ts.info.labels.iterator();
    while (it.next()) |entry| {
        const key_str = try std.fmt.allocPrint(storage.allocator, "${d}\r\n{s}\r\n", .{ entry.key_ptr.*.len, entry.key_ptr.* });
        defer storage.allocator.free(key_str);
        try writer.writeAll(key_str);

        const val_str = try std.fmt.allocPrint(storage.allocator, "${d}\r\n{s}\r\n", .{ entry.value_ptr.*.len, entry.value_ptr.* });
        defer storage.allocator.free(val_str);
        try writer.writeAll(val_str);
    }

    return try buf.toOwnedSlice(storage.allocator);
}

/// TS.ADD key timestamp value [RETENTION ms] [ENCODING type] [CHUNK_SIZE size]
///        [DUPLICATE_POLICY policy] [ON_DUPLICATE policy] [LABELS label value ...]
///
/// Add a data point to a time series. Auto-creates the key if it doesn't exist.
/// Returns: Integer timestamp of the added sample (`:timestamp\r\n`)
pub fn cmdTsAdd(
    storage: *Storage,
    args: []const []const u8,
    arena: std.mem.Allocator,
) ![]const u8 {
    if (args.len < 4) {
        return "ERR wrong number of arguments for 'TS.ADD' command\r\n";
    }

    const key = args[1];
    const timestamp_arg = args[2];
    const value_arg = args[3];

    // Parse timestamp and value
    const timestamp = parseTimestamp(timestamp_arg) catch {
        return "ERR invalid timestamp\r\n";
    };

    const value = parseValue(value_arg) catch {
        return "ERR invalid value\r\n";
    };

    // Parse optional arguments
    var retention_ms: i64 = 0;
    var encoding: Encoding = .uncompressed;
    var chunk_size: u32 = 4096;
    var duplicate_policy: DuplicatePolicy = .last;
    var on_duplicate_policy: ?DuplicatePolicy = null;
    var labels = try std.ArrayList(struct { key: []const u8, value: []const u8 }).initCapacity(arena, 8);
    defer labels.deinit(arena);

    var i: usize = 4;
    var is_creation = false; // Track if we're parsing creation-only options

    while (i < args.len) : (i += 1) {
        const option = args[i];

        if (std.ascii.eqlIgnoreCase(option, "RETENTION")) {
            is_creation = true;
            i += 1;
            if (i >= args.len) return "ERR RETENTION requires a value\r\n";
            retention_ms = std.fmt.parseInt(i64, args[i], 10) catch {
                return "ERR invalid RETENTION value\r\n";
            };
            if (retention_ms < 0) return "ERR RETENTION must be non-negative\r\n";
        } else if (std.ascii.eqlIgnoreCase(option, "ENCODING")) {
            is_creation = true;
            i += 1;
            if (i >= args.len) return "ERR ENCODING requires a value\r\n";
            encoding = Encoding.fromString(args[i]) orelse {
                return "ERR invalid ENCODING value (must be COMPRESSED or UNCOMPRESSED)\r\n";
            };
        } else if (std.ascii.eqlIgnoreCase(option, "CHUNK_SIZE")) {
            is_creation = true;
            i += 1;
            if (i >= args.len) return "ERR CHUNK_SIZE requires a value\r\n";
            chunk_size = std.fmt.parseInt(u32, args[i], 10) catch {
                return "ERR invalid CHUNK_SIZE value\r\n";
            };
            if (chunk_size == 0) return "ERR CHUNK_SIZE must be positive\r\n";
        } else if (std.ascii.eqlIgnoreCase(option, "DUPLICATE_POLICY")) {
            is_creation = true;
            i += 1;
            if (i >= args.len) return "ERR DUPLICATE_POLICY requires a value\r\n";
            duplicate_policy = DuplicatePolicy.fromString(args[i]) orelse {
                return "ERR invalid DUPLICATE_POLICY value\r\n";
            };
        } else if (std.ascii.eqlIgnoreCase(option, "ON_DUPLICATE")) {
            i += 1;
            if (i >= args.len) return "ERR ON_DUPLICATE requires a value\r\n";
            on_duplicate_policy = DuplicatePolicy.fromString(args[i]) orelse {
                return "ERR invalid ON_DUPLICATE value\r\n";
            };
        } else if (std.ascii.eqlIgnoreCase(option, "LABELS")) {
            is_creation = true;
            i += 1;
            // Parse label key-value pairs
            while (i + 1 < args.len) {
                const next = args[i];
                if (std.ascii.eqlIgnoreCase(next, "RETENTION") or
                    std.ascii.eqlIgnoreCase(next, "ENCODING") or
                    std.ascii.eqlIgnoreCase(next, "CHUNK_SIZE") or
                    std.ascii.eqlIgnoreCase(next, "DUPLICATE_POLICY") or
                    std.ascii.eqlIgnoreCase(next, "ON_DUPLICATE"))
                {
                    i -= 1;
                    break;
                }

                const label_key = args[i];
                i += 1;
                if (i >= args.len) return "ERR LABELS requires key-value pairs\r\n";
                const label_value = args[i];
                try labels.append(arena, .{ .key = label_key, .value = label_value });
                i += 1;
            }
            i -= 1;
        } else {
            return "ERR unknown option for TS.ADD\r\n";
        }
    }

    // Check if key exists
    const entry = try storage.data.getOrPut(key);

    if (entry.found_existing) {
        // Key exists - must be a time series
        if (entry.value_ptr.* != .timeseries) {
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }

        var ts = &entry.value_ptr.timeseries;

        // Add the sample (with ON_DUPLICATE override if specified)
        ts.add(timestamp, value, on_duplicate_policy) catch |err| {
            if (err == error.DuplicateTimestamp) {
                return "ERR DUPLICATE_POLICY is BLOCK and timestamp already exists\r\n";
            }
            return "ERR failed to add sample\r\n";
        };
    } else {
        // Key doesn't exist - create time series with creation params
        var ts = try TimeSeriesValue.init(storage.allocator);
        errdefer ts.deinit();

        ts.info.retention_ms = retention_ms;
        ts.info.encoding = encoding;
        ts.info.chunk_size = chunk_size;
        ts.info.duplicate_policy = duplicate_policy;

        // Add labels
        for (labels.items) |label| {
            try ts.info.setLabel(storage.allocator, label.key, label.value);
        }

        // Add the sample (ON_DUPLICATE override for new keys)
        try ts.add(timestamp, value, on_duplicate_policy);

        // Store in database (getOrPut allocated key already)
        const key_copy = try storage.allocator.dupe(u8, key);
        errdefer storage.allocator.free(key_copy);
        entry.key_ptr.* = key_copy;
        entry.value_ptr.* = Value{ .timeseries = ts };
    }

    // Return the timestamp as integer
    var buf = try std.ArrayList(u8).initCapacity(arena, 256);
    defer buf.deinit(arena);

    try buf.writer(arena).print(":{d}\r\n", .{timestamp});
    return try buf.toOwnedSlice(arena);
}

/// TS.MADD key timestamp value [key timestamp value ...]
///
/// Add multiple data points to multiple time series in a single command.
/// All keys must exist (no auto-creation).
/// Returns: Array of timestamps (`:ts1\r\n:ts2\r\n...`)
pub fn cmdTsMadd(
    storage: *Storage,
    args: []const []const u8,
    arena: std.mem.Allocator,
) ![]const u8 {
    // Validate arity: must have 1 + 3N arguments
    if (args.len < 4 or ((args.len - 1) % 3) != 0) {
        return "ERR wrong number of arguments for 'TS.MADD' command\r\n";
    }

    var timestamps = try std.ArrayList(i64).initCapacity(arena, 32);
    defer timestamps.deinit(arena);

    // Process each key-timestamp-value triplet
    var i: usize = 1;
    while (i < args.len) : (i += 3) {
        const key = args[i];
        const timestamp_arg = args[i + 1];
        const value_arg = args[i + 2];

        // Check key exists and get mutable pointer
        const val_ptr = storage.data.getPtr(key) orelse {
            return "ERR key does not exist\r\n";
        };

        // Check type
        if (val_ptr.* != .timeseries) {
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }

        // Parse timestamp and value
        const timestamp = parseTimestamp(timestamp_arg) catch {
            return "ERR invalid timestamp\r\n";
        };

        const value = parseValue(value_arg) catch {
            return "ERR invalid value\r\n";
        };

        // Add sample (no ON_DUPLICATE override for MADD)
        var ts = &val_ptr.timeseries;
        ts.add(timestamp, value, null) catch |err| {
            if (err == error.DuplicateTimestamp) {
                return "ERR DUPLICATE_POLICY is BLOCK and timestamp already exists\r\n";
            }
            return "ERR failed to add sample\r\n";
        };

        try timestamps.append(arena, timestamp);
    }

    // Build response array
    var buf = try std.ArrayList(u8).initCapacity(arena, 256);
    defer buf.deinit(arena);

    const writer = buf.writer(arena);
    try writer.print("*{d}\r\n", .{timestamps.items.len});

    for (timestamps.items) |ts| {
        try writer.print(":{d}\r\n", .{ts});
    }

    return try buf.toOwnedSlice(arena);
}

// ============================================================================
// Tests for TS.ADD
// ============================================================================

test "TS.ADD basic auto-create" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.ADD myts 1000 42.5
    const add_cmd = [_][]const u8{ "TS.ADD", "myts", "1000", "42.5" };

    var buf = try std.ArrayList(u8).initCapacity(allocator, 256);
    defer buf.deinit(allocator);

    const result = try cmdTsAdd(&storage, &add_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1000\r\n", result);

    // Verify key exists
    const value = storage.get("myts");
    try std.testing.expect(value != null);
    try std.testing.expect(value.?.* == .timeseries);
    try std.testing.expectEqual(@as(usize, 1), value.?.timeseries.samples.items.len);
    try std.testing.expectEqual(@as(f64, 42.5), value.?.timeseries.samples.items[0].value);
}

test "TS.ADD to existing time series" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series first
    const create_cmd = [_][]const u8{ "TS.CREATE", "myts" };
    const create_result = try cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    // Add sample
    const add_cmd = [_][]const u8{ "TS.ADD", "myts", "1000", "42.5" };
    const add_result = try cmdTsAdd(&storage, &add_cmd, allocator);
    defer allocator.free(add_result);

    try std.testing.expectEqualStrings(":1000\r\n", add_result);

    // Verify sample was added
    const value = storage.get("myts").?;
    try std.testing.expectEqual(@as(usize, 1), value.timeseries.samples.items.len);
}

test "TS.ADD with wildcard timestamp" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.ADD myts * 42.5
    const add_cmd = [_][]const u8{ "TS.ADD", "myts", "*", "42.5" };
    const result = try cmdTsAdd(&storage, &add_cmd, allocator);
    defer allocator.free(result);

    // Verify it returned a valid timestamp
    try std.testing.expect(std.mem.startsWith(u8, result, ":"));
    try std.testing.expect(std.mem.endsWith(u8, result, "\r\n"));

    // Verify sample was added
    const value = storage.get("myts").?;
    try std.testing.expectEqual(@as(usize, 1), value.timeseries.samples.items.len);
}

test "TS.ADD with RETENTION" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.ADD myts 1000 42.5 RETENTION 86400000
    const add_cmd = [_][]const u8{ "TS.ADD", "myts", "1000", "42.5", "RETENTION", "86400000" };
    const result = try cmdTsAdd(&storage, &add_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1000\r\n", result);

    // Verify retention was set
    const value = storage.get("myts").?;
    try std.testing.expectEqual(@as(i64, 86400000), value.timeseries.info.retention_ms);
}

test "TS.ADD with DUPLICATE_POLICY" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.ADD myts 1000 42.5 DUPLICATE_POLICY SUM
    const add_cmd = [_][]const u8{ "TS.ADD", "myts", "1000", "42.5", "DUPLICATE_POLICY", "SUM" };
    const result = try cmdTsAdd(&storage, &add_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1000\r\n", result);

    // Verify policy was set
    const value = storage.get("myts").?;
    try std.testing.expect(value.timeseries.info.duplicate_policy == .sum);
}

test "TS.ADD with LABELS" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.ADD myts 1000 42.5 LABELS sensor temp location room1
    const add_cmd = [_][]const u8{ "TS.ADD", "myts", "1000", "42.5", "LABELS", "sensor", "temp", "location", "room1" };
    const result = try cmdTsAdd(&storage, &add_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1000\r\n", result);

    // Verify labels
    const value = storage.get("myts").?;
    try std.testing.expectEqual(@as(usize, 2), value.timeseries.info.labels.count());
}

test "TS.ADD with ON_DUPLICATE override" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create with BLOCK policy
    const create_cmd = [_][]const u8{ "TS.CREATE", "myts", "DUPLICATE_POLICY", "BLOCK" };
    const create_result = try cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    // First add
    const add_cmd1 = [_][]const u8{ "TS.ADD", "myts", "1000", "10.0" };
    const result1 = try cmdTsAdd(&storage, &add_cmd1, allocator);
    defer allocator.free(result1);

    // Second add with same timestamp but ON_DUPLICATE LAST
    const add_cmd2 = [_][]const u8{ "TS.ADD", "myts", "1000", "20.0", "ON_DUPLICATE", "LAST" };
    const result2 = try cmdTsAdd(&storage, &add_cmd2, allocator);
    defer allocator.free(result2);

    try std.testing.expectEqualStrings(":1000\r\n", result2);

    // Verify value was replaced (LAST policy)
    const value = storage.get("myts").?;
    try std.testing.expectEqual(@as(f64, 20.0), value.timeseries.samples.items[0].value);
}

test "TS.ADD WRONGTYPE error" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Set a string key
    const set_result = try @import("../commands/strings.zig").handleCommand(allocator, &storage, &[_]@import("../protocol/parser.zig").RespValue{
        .{ .bulk_string = "SET" },
        .{ .bulk_string = "mystring" },
        .{ .bulk_string = "value" },
    }, 0, null, null, null);
    defer allocator.free(set_result);

    // Try to TS.ADD to string key
    const add_cmd = [_][]const u8{ "TS.ADD", "mystring", "1000", "42.5" };
    const result = try cmdTsAdd(&storage, &add_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-WRONGTYPE"));
}

test "TS.ADD duplicate BLOCK error" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create with BLOCK policy
    const create_cmd = [_][]const u8{ "TS.CREATE", "myts", "DUPLICATE_POLICY", "BLOCK" };
    const create_result = try cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    // First add
    const add_cmd1 = [_][]const u8{ "TS.ADD", "myts", "1000", "10.0" };
    const result1 = try cmdTsAdd(&storage, &add_cmd1, allocator);
    defer allocator.free(result1);

    // Try to add duplicate with BLOCK policy
    const add_cmd2 = [_][]const u8{ "TS.ADD", "myts", "1000", "20.0" };
    const result2 = try cmdTsAdd(&storage, &add_cmd2, allocator);
    defer allocator.free(result2);

    try std.testing.expect(std.mem.startsWith(u8, result2, "ERR DUPLICATE_POLICY is BLOCK"));
}

test "TS.ADD invalid timestamp" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    const add_cmd = [_][]const u8{ "TS.ADD", "myts", "invalid", "42.5" };
    const result = try cmdTsAdd(&storage, &add_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "ERR invalid timestamp"));
}

test "TS.ADD invalid value" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    const add_cmd = [_][]const u8{ "TS.ADD", "myts", "1000", "notanumber" };
    const result = try cmdTsAdd(&storage, &add_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "ERR invalid value"));
}

test "TS.ADD arity error" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    const add_cmd = [_][]const u8{ "TS.ADD", "myts" };
    const result = try cmdTsAdd(&storage, &add_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "ERR wrong number"));
}

// ============================================================================
// Tests for TS.MADD
// ============================================================================

test "TS.MADD basic multiple keys" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create two time series
    const create_cmd1 = [_][]const u8{ "TS.CREATE", "ts1" };
    const create_result1 = try cmdTsCreate(&storage, &create_cmd1, allocator);
    defer allocator.free(create_result1);

    const create_cmd2 = [_][]const u8{ "TS.CREATE", "ts2" };
    const create_result2 = try cmdTsCreate(&storage, &create_cmd2, allocator);
    defer allocator.free(create_result2);

    // TS.MADD ts1 1000 10.0 ts2 2000 20.0
    const madd_cmd = [_][]const u8{ "TS.MADD", "ts1", "1000", "10.0", "ts2", "2000", "20.0" };
    const result = try cmdTsMadd(&storage, &madd_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("*2\r\n:1000\r\n:2000\r\n", result);

    // Verify both samples were added
    const val1 = storage.get("ts1").?;
    const val2 = storage.get("ts2").?;
    try std.testing.expectEqual(@as(usize, 1), val1.timeseries.samples.items.len);
    try std.testing.expectEqual(@as(usize, 1), val2.timeseries.samples.items.len);
}

test "TS.MADD same key multiple times" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create one time series
    const create_cmd = [_][]const u8{ "TS.CREATE", "myts" };
    const create_result = try cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    // TS.MADD myts 1000 10.0 myts 2000 20.0
    const madd_cmd = [_][]const u8{ "TS.MADD", "myts", "1000", "10.0", "myts", "2000", "20.0" };
    const result = try cmdTsMadd(&storage, &madd_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("*2\r\n:1000\r\n:2000\r\n", result);

    // Verify both samples were added to same series
    const value = storage.get("myts").?;
    try std.testing.expectEqual(@as(usize, 2), value.timeseries.samples.items.len);
}

test "TS.MADD with wildcard timestamps" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series
    const create_cmd = [_][]const u8{ "TS.CREATE", "myts" };
    const create_result = try cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    // TS.MADD myts * 10.0 myts * 20.0
    const madd_cmd = [_][]const u8{ "TS.MADD", "myts", "*", "10.0", "myts", "*", "20.0" };
    const result = try cmdTsMadd(&storage, &madd_cmd, allocator);
    defer allocator.free(result);

    // Verify it returned array of 2 timestamps
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));

    // Verify both samples were added
    const value = storage.get("myts").?;
    try std.testing.expectEqual(@as(usize, 2), value.timeseries.samples.items.len);
}

test "TS.MADD nonexistent key error" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // TS.MADD nonexistent 1000 10.0
    const madd_cmd = [_][]const u8{ "TS.MADD", "nonexistent", "1000", "10.0" };
    const result = try cmdTsMadd(&storage, &madd_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "ERR key does not exist"));
}

test "TS.MADD WRONGTYPE error" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Set a string key
    const set_result = try @import("../commands/strings.zig").handleCommand(allocator, &storage, &[_]@import("../protocol/parser.zig").RespValue{
        .{ .bulk_string = "SET" },
        .{ .bulk_string = "mystring" },
        .{ .bulk_string = "value" },
    }, 0, null, null, null);
    defer allocator.free(set_result);

    // TS.MADD to string key
    const madd_cmd = [_][]const u8{ "TS.MADD", "mystring", "1000", "10.0" };
    const result = try cmdTsMadd(&storage, &madd_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-WRONGTYPE"));
}

test "TS.MADD arity error" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Wrong arity (1 + 2 args instead of 1 + 3N)
    const madd_cmd = [_][]const u8{ "TS.MADD", "myts", "1000" };
    const result = try cmdTsMadd(&storage, &madd_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "ERR wrong number"));
}

test "TS.MADD invalid timestamp in triplet" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series
    const create_cmd = [_][]const u8{ "TS.CREATE", "myts" };
    const create_result = try cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    // TS.MADD myts invalid 10.0
    const madd_cmd = [_][]const u8{ "TS.MADD", "myts", "invalid", "10.0" };
    const result = try cmdTsMadd(&storage, &madd_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "ERR invalid timestamp"));
}

test "TS.MADD duplicate BLOCK error" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create with BLOCK policy
    const create_cmd = [_][]const u8{ "TS.CREATE", "myts", "DUPLICATE_POLICY", "BLOCK" };
    const create_result = try cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    // First add
    const add_cmd = [_][]const u8{ "TS.ADD", "myts", "1000", "10.0" };
    const add_result = try cmdTsAdd(&storage, &add_cmd, allocator);
    defer allocator.free(add_result);

    // Try MADD with duplicate timestamp
    const madd_cmd = [_][]const u8{ "TS.MADD", "myts", "1000", "20.0" };
    const result = try cmdTsMadd(&storage, &madd_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "ERR DUPLICATE_POLICY is BLOCK"));
}
