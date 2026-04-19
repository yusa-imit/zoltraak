const std = @import("std");
const Storage = @import("../storage/memory.zig").Storage;
const Value = @import("../storage/memory.zig").Value;
const TimeSeriesValue = @import("../storage/memory.zig").TimeSeriesValue;
const timeseries_mod = @import("../storage/timeseries.zig");
const DuplicatePolicy = timeseries_mod.DuplicatePolicy;
const Encoding = timeseries_mod.Encoding;
const AggregationType = timeseries_mod.AggregationType;
const CompactionRule = timeseries_mod.CompactionRule;

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

/// TS.INCRBY key timestamp delta [RETENTION retentionPeriod] [ENCODING <COMPRESSED|UNCOMPRESSED>] [CHUNK_SIZE size] [DUPLICATE_POLICY <BLOCK|FIRST|LAST|MIN|MAX|SUM>] [IGNORE] [LABELS label value [label value ...]]
///
/// Increment a time series value by delta at specified timestamp.
/// Auto-creates the time series if key doesn't exist.
/// Returns: timestamp as integer
///
/// Example:
/// TS.INCRBY counter 1000 5 RETENTION 86400000 LABELS env prod
pub fn cmdTsIncrby(
    storage: *Storage,
    args: []const []const u8,
    arena: std.mem.Allocator,
) ![]const u8 {
    if (args.len < 4) {
        return "ERR wrong number of arguments for 'TS.INCRBY' command\r\n";
    }

    const key = args[1];
    const timestamp_arg = args[2];
    const delta_arg = args[3];

    // Parse timestamp and delta
    const timestamp = parseTimestamp(timestamp_arg) catch {
        return "ERR invalid timestamp\r\n";
    };

    const delta = parseValue(delta_arg) catch {
        return "ERR invalid delta value\r\n";
    };

    // Parse optional arguments
    var retention_ms: i64 = 0;
    var encoding: Encoding = .uncompressed;
    var chunk_size: u32 = 4096;
    var duplicate_policy: DuplicatePolicy = .last;
    var labels = try std.ArrayList(struct { key: []const u8, value: []const u8 }).initCapacity(arena, 8);
    defer labels.deinit(arena);
    // Note: IGNORE option is parsed but not used (stub implementation)
    var _ignore = false;

    var i: usize = 4;
    var is_creation = false;

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
        } else if (std.ascii.eqlIgnoreCase(option, "IGNORE")) {
            // Parse but don't use (stub for filtering based on duplicate policy)
            _ignore = true;
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
                    std.ascii.eqlIgnoreCase(next, "IGNORE"))
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
            return "ERR unknown option for TS.INCRBY\r\n";
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

        // Increment the sample
        ts.incrementBy(timestamp, delta) catch |err| {
            if (err == error.DuplicateTimestamp) {
                return "ERR DUPLICATE_POLICY is BLOCK and timestamp already exists\r\n";
            }
            return "ERR failed to increment sample\r\n";
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

        // Increment the sample
        try ts.incrementBy(timestamp, delta);

        // Store in database
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

/// TS.DECRBY key timestamp delta [RETENTION retentionPeriod] [ENCODING <COMPRESSED|UNCOMPRESSED>] [CHUNK_SIZE size] [DUPLICATE_POLICY <BLOCK|FIRST|LAST|MIN|MAX|SUM>] [IGNORE] [LABELS label value [label value ...]]
///
/// Decrement a time series value by delta at specified timestamp.
/// Auto-creates the time series if key doesn't exist.
/// Returns: timestamp as integer
///
/// Example:
/// TS.DECRBY counter 1000 5 RETENTION 86400000
pub fn cmdTsDecrby(
    storage: *Storage,
    args: []const []const u8,
    arena: std.mem.Allocator,
) ![]const u8 {
    if (args.len < 4) {
        return "ERR wrong number of arguments for 'TS.DECRBY' command\r\n";
    }

    const key = args[1];
    const timestamp_arg = args[2];
    const delta_arg = args[3];

    // Parse timestamp and delta
    const timestamp = parseTimestamp(timestamp_arg) catch {
        return "ERR invalid timestamp\r\n";
    };

    const delta = parseValue(delta_arg) catch {
        return "ERR invalid delta value\r\n";
    };

    // Parse optional arguments
    var retention_ms: i64 = 0;
    var encoding: Encoding = .uncompressed;
    var chunk_size: u32 = 4096;
    var duplicate_policy: DuplicatePolicy = .last;
    var labels = try std.ArrayList(struct { key: []const u8, value: []const u8 }).initCapacity(arena, 8);
    defer labels.deinit(arena);
    // Note: IGNORE option is parsed but not used (stub implementation)
    var _ignore = false;

    var i: usize = 4;
    var is_creation = false;

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
        } else if (std.ascii.eqlIgnoreCase(option, "IGNORE")) {
            // Parse but don't use (stub for filtering based on duplicate policy)
            _ignore = true;
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
                    std.ascii.eqlIgnoreCase(next, "IGNORE"))
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
            return "ERR unknown option for TS.DECRBY\r\n";
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

        // Decrement the sample (negate delta)
        ts.decrementBy(timestamp, delta) catch |err| {
            if (err == error.DuplicateTimestamp) {
                return "ERR DUPLICATE_POLICY is BLOCK and timestamp already exists\r\n";
            }
            return "ERR failed to decrement sample\r\n";
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

        // Decrement the sample (negate delta)
        try ts.decrementBy(timestamp, delta);

        // Store in database
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

/// TS.DEL key fromTimestamp toTimestamp
///
/// Delete all samples in the time range [fromTimestamp, toTimestamp] inclusive.
/// Returns the number of samples deleted.
///
/// Example:
/// TS.DEL sensor:temp 1000 2000
pub fn cmdTsDel(
    storage: *Storage,
    args: []const []const u8,
    arena: std.mem.Allocator,
) ![]const u8 {
    _ = arena; // Not needed for this command

    if (args.len != 4) {
        return "ERR wrong number of arguments for 'TS.DEL' command\r\n";
    }

    const key = args[1];
    const from_ts = parseTimestamp(args[2]) catch {
        return "ERR invalid fromTimestamp\r\n";
    };
    const to_ts = parseTimestamp(args[3]) catch {
        return "ERR invalid toTimestamp\r\n";
    };

    // Validate range
    if (from_ts > to_ts) {
        return "ERR fromTimestamp must be <= toTimestamp\r\n";
    }

    // Get mutable pointer to the existing time series
    const val_ptr = storage.data.getPtr(key) orelse {
        return "ERR key does not exist\r\n";
    };

    if (val_ptr.* != .timeseries) {
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    var ts = &val_ptr.timeseries;
    const deleted_count = ts.deleteRange(from_ts, to_ts);

    // Return integer count
    var buf: [32]u8 = undefined;
    const num_str = try std.fmt.bufPrint(&buf, ":{d}\r\n", .{deleted_count});
    return try storage.allocator.dupe(u8, num_str);
}

/// TS.GET key [LATEST]
///
/// Get the most recent sample from the time series.
/// LATEST flag is optional (default behavior is already latest).
/// Returns [timestamp, value] array or null if no samples exist.
///
/// Example:
/// TS.GET sensor:temp
/// TS.GET sensor:temp LATEST
pub fn cmdTsGet(
    storage: *Storage,
    args: []const []const u8,
    arena: std.mem.Allocator,
) ![]const u8 {
    _ = arena; // Not needed for this command

    if (args.len < 2 or args.len > 3) {
        return "ERR wrong number of arguments for 'TS.GET' command\r\n";
    }

    const key = args[1];

    // Parse optional LATEST flag (default is latest anyway)
    if (args.len == 3) {
        if (!std.ascii.eqlIgnoreCase(args[2], "LATEST")) {
            return "ERR unknown option for TS.GET\r\n";
        }
    }

    // Get existing time series
    const val = storage.data.get(key) orelse {
        return "ERR key does not exist\r\n";
    };

    if (val != .timeseries) {
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    const ts = &val.timeseries;
    const latest = ts.getLatest();

    if (latest == null) {
        // No samples exist
        return "$-1\r\n";
    }

    // Return [timestamp, value] array
    const sample = latest.?;
    var buf: [128]u8 = undefined;
    const result_str = try std.fmt.bufPrint(&buf, "*2\r\n:{d}\r\n+{d}\r\n", .{ sample.timestamp, sample.value });
    return try storage.allocator.dupe(u8, result_str);
}

/// TS.ALTER key [RETENTION ms] [CHUNK_SIZE size] [DUPLICATE_POLICY policy] [LABELS label value ...]
///
/// Alter configuration of an existing time series.
/// ENCODING is immutable and cannot be changed.
/// LABELS provided completely replace all existing labels (if specified, old labels are cleared first).
/// Returns: +OK on success
///
/// Example:
/// TS.ALTER sensor:temp RETENTION 86400000 LABELS type temperature location room1
pub fn cmdTsAlter(
    storage: *Storage,
    args: []const []const u8,
    arena: std.mem.Allocator,
) ![]const u8 {
    if (args.len < 2) {
        return "ERR wrong number of arguments for 'TS.ALTER' command\r\n";
    }

    const key = args[1];

    // Get mutable pointer to existing time series
    const val_ptr = storage.data.getPtr(key) orelse {
        return "ERR key does not exist\r\n";
    };

    if (val_ptr.* != .timeseries) {
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    var ts = &val_ptr.timeseries;

    // Parse optional arguments
    var retention_ms: ?i64 = null;
    var chunk_size: ?u32 = null;
    var duplicate_policy: ?DuplicatePolicy = null;
    var labels = try std.ArrayList(struct { key: []const u8, value: []const u8 }).initCapacity(arena, 8);
    defer labels.deinit(arena);

    // Parse optional arguments (all are update-only - no creation mode like TS.ADD)
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const option = args[i];

        if (std.ascii.eqlIgnoreCase(option, "RETENTION")) {
            i += 1;
            if (i >= args.len) return "ERR RETENTION requires a value\r\n";
            retention_ms = std.fmt.parseInt(i64, args[i], 10) catch {
                return "ERR invalid RETENTION value\r\n";
            };
            if (retention_ms.? < 0) return "ERR RETENTION must be non-negative\r\n";
        } else if (std.ascii.eqlIgnoreCase(option, "CHUNK_SIZE")) {
            i += 1;
            if (i >= args.len) return "ERR CHUNK_SIZE requires a value\r\n";
            chunk_size = std.fmt.parseInt(u32, args[i], 10) catch {
                return "ERR invalid CHUNK_SIZE value\r\n";
            };
            if (chunk_size.? == 0) return "ERR CHUNK_SIZE must be positive\r\n";
        } else if (std.ascii.eqlIgnoreCase(option, "DUPLICATE_POLICY")) {
            i += 1;
            if (i >= args.len) return "ERR DUPLICATE_POLICY requires a value\r\n";
            duplicate_policy = DuplicatePolicy.fromString(args[i]) orelse {
                return "ERR invalid DUPLICATE_POLICY value\r\n";
            };
        } else if (std.ascii.eqlIgnoreCase(option, "ENCODING")) {
            return "ERR ENCODING cannot be altered after creation\r\n";
        } else if (std.ascii.eqlIgnoreCase(option, "LABELS")) {
            i += 1;
            // Parse label key-value pairs
            while (i + 1 < args.len) {
                const next = args[i];
                if (std.ascii.eqlIgnoreCase(next, "RETENTION") or
                    std.ascii.eqlIgnoreCase(next, "CHUNK_SIZE") or
                    std.ascii.eqlIgnoreCase(next, "DUPLICATE_POLICY") or
                    std.ascii.eqlIgnoreCase(next, "ENCODING"))
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
            return "ERR unknown option for TS.ALTER\r\n";
        }
    }

    // Apply alterations
    // Note: Pass null for labels to avoid Zig anonymous struct type issues
    try ts.alter(storage.allocator, retention_ms, chunk_size, duplicate_policy, null);

    // If labels were provided, manually update them after alter() call
    if (labels.items.len > 0) {
        // Clear existing labels first
        var iter = ts.info.labels.iterator();
        while (iter.next()) |entry| {
            storage.allocator.free(entry.key_ptr.*);
            storage.allocator.free(entry.value_ptr.*);
        }
        ts.info.labels.clearRetainingCapacity();

        // Add new labels
        for (labels.items) |label| {
            try ts.info.setLabel(storage.allocator, label.key, label.value);
        }
    }

    return "+OK\r\n";
}

/// TS.MGET [LATEST] [WITHLABELS] [SELECTED_LABELS label1 label2 ...] FILTER filter1 [FILTER filter2 ...]
///
/// Get latest samples from multiple time series matching label filters.
/// Returns array of [key, labels, [timestamp, value]] for each matching time series.
/// Filters use label=value, label=(v1,v2), label!=value, label=, label!= syntax.
/// At least one positive filter (equals or in_list) is required.
///
/// Example:
/// TS.MGET WITHLABELS FILTER type=sensor FILTER location=(room1,room2)
pub fn cmdTsMget(
    storage: *Storage,
    args: []const []const u8,
    arena: std.mem.Allocator,
) ![]const u8 {
    if (args.len < 3) {
        return "ERR wrong number of arguments for 'TS.MGET' command\r\n";
    }

    const TimeSeriesFilter = timeseries_mod.TimeSeriesFilter;

    // Parse optional flags and filter expressions
    var with_latest = false;
    var with_labels = false;
    var selected_labels = try std.ArrayList([]const u8).initCapacity(arena, 8);
    defer selected_labels.deinit(arena);

    var filters = try std.ArrayList(TimeSeriesFilter).initCapacity(arena, 8);
    defer {
        for (filters.items) |*f| {
            f.deinit();
        }
        filters.deinit(arena);
    }

    var has_positive_filter = false;

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];

        if (std.ascii.eqlIgnoreCase(arg, "LATEST")) {
            with_latest = true;
        } else if (std.ascii.eqlIgnoreCase(arg, "WITHLABELS")) {
            with_labels = true;
        } else if (std.ascii.eqlIgnoreCase(arg, "SELECTED_LABELS")) {
            i += 1;
            // Parse label names until next flag
            while (i < args.len) : (i += 1) {
                const next = args[i];
                if (std.ascii.eqlIgnoreCase(next, "FILTER") or
                    std.ascii.eqlIgnoreCase(next, "LATEST") or
                    std.ascii.eqlIgnoreCase(next, "WITHLABELS"))
                {
                    i -= 1;
                    break;
                }
                try selected_labels.append(arena, next);
            }
        } else if (std.ascii.eqlIgnoreCase(arg, "FILTER")) {
            i += 1;
            if (i >= args.len) return "ERR FILTER requires an expression\r\n";

            const filter_expr = args[i];
            const filter_opt = try TimeSeriesFilter.parse(filter_expr, arena);

            if (filter_opt) |filter| {
                // Check if this is a positive filter
                if (filter.filter_type == .equals or filter.filter_type == .in_list) {
                    has_positive_filter = true;
                }
                try filters.append(arena, filter);
            } else {
                return "ERR invalid FILTER expression\r\n";
            }
        } else {
            return "ERR unknown option for TS.MGET\r\n";
        }
    }

    // Validate: at least one positive filter is required
    if (!has_positive_filter) {
        return "ERR MGET requires at least one positive filter (label=value or label=(v1,v2))\r\n";
    }

    // Validate: at least one filter is required
    if (filters.items.len == 0) {
        return "ERR MGET requires at least one FILTER\r\n";
    }

    // Scan storage for all timeseries keys matching all filters
    var results = try std.ArrayList(struct { key: []const u8, ts: *TimeSeriesValue }).initCapacity(arena, 16);
    defer results.deinit(arena);

    var iter = storage.data.iterator();
    while (iter.next()) |entry| {
        const key = entry.key_ptr.*;
        const val = entry.value_ptr;

        if (val.* != .timeseries) continue;

        var ts = &val.timeseries;

        // Check all filters - all must match
        var all_match = true;
        for (filters.items) |*filter| {
            if (!filter.matches(&ts.info)) {
                all_match = false;
                break;
            }
        }

        if (all_match) {
            try results.append(arena, .{ .key = key, .ts = ts });
        }
    }

    // Build RESP response
    var buf = try std.ArrayList(u8).initCapacity(arena, 512);
    defer buf.deinit(arena);

    const writer = buf.writer(arena);

    // Array of results
    try writer.print("*{d}\r\n", .{results.items.len});

    for (results.items) |result| {
        const key = result.key;
        var ts = result.ts;

        // Get latest sample
        const latest = ts.getLatest();

        if (latest != null) {
            const sample = latest.?;

            // For each match, format: [key, labels, [timestamp, value]]
            try writer.writeAll("*3\r\n");

            // Key
            try writer.print("${d}\r\n{s}\r\n", .{ key.len, key });

            // Labels array
            if (with_labels or selected_labels.items.len > 0) {
                if (selected_labels.items.len > 0) {
                    // SELECTED_LABELS: return only specified labels
                    var label_count: usize = 0;
                    for (selected_labels.items) |label_name| {
                        if (ts.info.labels.get(label_name)) |_| {
                            label_count += 1;
                        }
                    }

                    try writer.print("*{d}\r\n", .{label_count * 2});

                    for (selected_labels.items) |label_name| {
                        if (ts.info.labels.get(label_name)) |label_value| {
                            try writer.print("${d}\r\n{s}\r\n", .{ label_name.len, label_name });
                            try writer.print("${d}\r\n{s}\r\n", .{ label_value.len, label_value });
                        }
                    }
                } else {
                    // WITHLABELS: return all labels
                    const label_count = ts.info.labels.count();
                    try writer.print("*{d}\r\n", .{label_count * 2});

                    var label_iter = ts.info.labels.iterator();
                    while (label_iter.next()) |entry| {
                        const label_key = entry.key_ptr.*;
                        const label_value = entry.value_ptr.*;

                        try writer.print("${d}\r\n{s}\r\n", .{ label_key.len, label_key });
                        try writer.print("${d}\r\n{s}\r\n", .{ label_value.len, label_value });
                    }
                }
            } else {
                // No labels
                try writer.writeAll("*0\r\n");
            }

            // Sample [timestamp, value]
            try writer.print("*2\r\n:{d}\r\n+{d}\r\n", .{ sample.timestamp, sample.value });
        }
    }

    return try buf.toOwnedSlice(arena);
}

/// TS.RANGE key fromTimestamp toTimestamp [FILTER_BY_TS ts [ts ...]] [FILTER_BY_VALUE min max] [COUNT count]
///
/// Get samples in a time range with optional value and timestamp filters.
/// Returns array of [timestamp, value] pairs for samples in the range.
/// Supports special timestamps: "-" for earliest, "+" for latest.
/// Filters are applied in order: timestamp range → FILTER_BY_TS → FILTER_BY_VALUE → COUNT limit.
///
/// Example:
/// TS.RANGE sensor:temp 1000 3000
/// TS.RANGE sensor:temp - + FILTER_BY_VALUE 10.5 30.5
/// TS.RANGE sensor:temp 1000 3000 FILTER_BY_TS 1500 2500 COUNT 10
pub fn cmdTsRange(
    storage: *Storage,
    args: []const []const u8,
    arena: std.mem.Allocator,
) ![]const u8 {
    if (args.len < 4) {
        return "ERR wrong number of arguments for 'TS.RANGE' command\r\n";
    }

    const key = args[1];

    // Check if key exists
    const val_ptr = storage.data.getPtr(key) orelse {
        return "ERR key does not exist\r\n";
    };

    if (val_ptr.* != .timeseries) {
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    const ts = &val_ptr.timeseries;

    // Parse from_timestamp (support "-" for earliest)
    const from_ts = if (std.mem.eql(u8, args[2], "-"))
        if (ts.info.first_timestamp) |ft| ft else std.math.minInt(i64)
    else
        parseTimestamp(args[2]) catch {
            return "ERR invalid fromTimestamp\r\n";
        };

    // Parse to_timestamp (support "+" for latest)
    const to_ts = if (std.mem.eql(u8, args[3], "+"))
        if (ts.info.last_timestamp) |lt| lt else std.math.maxInt(i64)
    else
        parseTimestamp(args[3]) catch {
            return "ERR invalid toTimestamp\r\n";
        };

    // Validate timestamp range
    if (from_ts > to_ts) {
        return "ERR fromTimestamp must be <= toTimestamp\r\n";
    }

    // Parse optional arguments
    var filter_by_ts = try std.ArrayList(i64).initCapacity(arena, 4);
    defer filter_by_ts.deinit(arena);

    var filter_by_value: ?struct { min: f64, max: f64 } = null;
    var count_limit: ?usize = null;

    var i: usize = 4;
    while (i < args.len) : (i += 1) {
        const option = args[i];

        if (std.ascii.eqlIgnoreCase(option, "FILTER_BY_TS")) {
            i += 1;
            // Parse all following timestamps until next option or end
            while (i < args.len) : (i += 1) {
                const next = args[i];
                if (std.ascii.eqlIgnoreCase(next, "FILTER_BY_VALUE") or
                    std.ascii.eqlIgnoreCase(next, "COUNT"))
                {
                    i -= 1;
                    break;
                }

                const ts_val = parseTimestamp(next) catch {
                    return "ERR invalid timestamp in FILTER_BY_TS\r\n";
                };
                try filter_by_ts.append(arena, ts_val);
            }
        } else if (std.ascii.eqlIgnoreCase(option, "FILTER_BY_VALUE")) {
            i += 1;
            if (i + 1 >= args.len) {
                return "ERR FILTER_BY_VALUE requires min and max values\r\n";
            }

            const min = parseValue(args[i]) catch {
                return "ERR invalid min value for FILTER_BY_VALUE\r\n";
            };
            i += 1;
            const max = parseValue(args[i]) catch {
                return "ERR invalid max value for FILTER_BY_VALUE\r\n";
            };

            if (min > max) {
                return "ERR min must be <= max for FILTER_BY_VALUE\r\n";
            }

            filter_by_value = .{ .min = min, .max = max };
        } else if (std.ascii.eqlIgnoreCase(option, "COUNT")) {
            i += 1;
            if (i >= args.len) {
                return "ERR COUNT requires a value\r\n";
            }

            count_limit = std.fmt.parseInt(usize, args[i], 10) catch {
                return "ERR invalid COUNT value\r\n";
            };
        } else {
            return "ERR unknown option for TS.RANGE\r\n";
        }
    }

    // Get samples in range
    const samples = ts.getRange(from_ts, to_ts);

    // Apply filters in order: FILTER_BY_TS → FILTER_BY_VALUE → COUNT limit
    var filtered_samples = try std.ArrayList(timeseries_mod.DataPoint).initCapacity(arena, 64);
    defer filtered_samples.deinit(arena);

    for (samples) |sample| {
        // Apply FILTER_BY_TS (exact match check)
        if (filter_by_ts.items.len > 0) {
            var found = false;
            for (filter_by_ts.items) |ts_filter| {
                if (sample.timestamp == ts_filter) {
                    found = true;
                    break;
                }
            }
            if (!found) continue;
        }

        // Apply FILTER_BY_VALUE (range check)
        if (filter_by_value) |fbv| {
            if (sample.value < fbv.min or sample.value > fbv.max) {
                continue;
            }
        }

        try filtered_samples.append(arena, sample);

        // Apply COUNT limit
        if (count_limit) |limit| {
            if (filtered_samples.items.len >= limit) {
                break;
            }
        }
    }

    // Build RESP response: *N\r\n followed by *2\r\n:timestamp\r\n+value\r\n pairs
    var buf = try std.ArrayList(u8).initCapacity(arena, 512);
    defer buf.deinit(arena);

    const writer = buf.writer(arena);

    try writer.print("*{d}\r\n", .{filtered_samples.items.len});

    for (filtered_samples.items) |sample| {
        try writer.writeAll("*2\r\n");
        try writer.print(":{d}\r\n", .{sample.timestamp});
        try writer.print("+{d}\r\n", .{sample.value});
    }

    return try buf.toOwnedSlice(arena);
}

/// TS.REVRANGE key fromTimestamp toTimestamp [FILTER_BY_TS ts [ts ...]] [FILTER_BY_VALUE min max] [COUNT count]
///
/// Get samples in a time range in reverse order.
/// Same syntax and behavior as TS.RANGE, but results are returned in descending timestamp order.
///
/// Example:
/// TS.REVRANGE sensor:temp 1000 3000
/// TS.REVRANGE sensor:temp - + COUNT 10
pub fn cmdTsRevrange(
    storage: *Storage,
    args: []const []const u8,
    arena: std.mem.Allocator,
) ![]const u8 {
    // Define a named struct type to avoid anonymous struct type issues in Zig
    const DataPoint = struct { timestamp: i64, value: f64 };

    // Use TS.RANGE with modified command name for error messages
    const modified_args = try arena.alloc([]const u8, args.len);
    defer arena.free(modified_args);

    for (args, 0..) |arg, idx| {
        modified_args[idx] = arg;
    }

    // Call TS.RANGE logic
    const result = try cmdTsRange(storage, modified_args, arena);

    // Parse the RESP result and reverse the data points
    // Expected format: *N\r\n followed by *2\r\n:ts\r\n+value\r\n pairs

    if (result.len == 0) {
        return result;
    }

    // Check if it's an error (starts with - or +ERR)
    if (std.mem.startsWith(u8, result, "-") or std.mem.startsWith(u8, result, "+ERR")) {
        return result;
    }

    // Parse the array count from first line
    const first_crlf_pos = std.mem.indexOf(u8, result, "\r\n") orelse return result;
    const count_str = result[1..first_crlf_pos];
    const count = std.fmt.parseInt(usize, count_str, 10) catch return result;

    if (count == 0) {
        return result; // Empty array, nothing to reverse
    }

    // Parse data points
    var data_points = try std.ArrayList(DataPoint).initCapacity(arena, count);
    defer data_points.deinit(arena);

    var line_start: usize = first_crlf_pos + 2; // Skip "*N\r\n"
    for (0..count) |_| {
        // Skip "*2\r\n"
        while (line_start < result.len and result[line_start] != '\r') : (line_start += 1) {}
        line_start += 2; // Skip "\r\n"

        // Parse timestamp line ":TIMESTAMP\r\n"
        const ts_line_start = line_start;
        while (line_start < result.len and result[line_start] != '\r') : (line_start += 1) {}
        const ts_str = result[ts_line_start + 1 .. line_start];
        const timestamp = std.fmt.parseInt(i64, ts_str, 10) catch break;
        line_start += 2; // Skip "\r\n"

        // Parse value line "+VALUE\r\n"
        const val_line_start = line_start;
        while (line_start < result.len and result[line_start] != '\r') : (line_start += 1) {}
        const val_str = result[val_line_start + 1 .. line_start];
        const value = std.fmt.parseFloat(f64, val_str) catch break;
        line_start += 2; // Skip "\r\n"

        try data_points.append(arena, .{ .timestamp = timestamp, .value = value });
    }

    // Reverse the data points (use the named DataPoint type defined at function start)
    std.mem.reverse(DataPoint, data_points.items);

    // Build reversed RESP response
    var buf = try std.ArrayList(u8).initCapacity(arena, 512);
    defer buf.deinit(arena);

    const writer = buf.writer(arena);

    try writer.print("*{d}\r\n", .{data_points.items.len});

    for (data_points.items) |dp| {
        try writer.writeAll("*2\r\n");
        try writer.print(":{d}\r\n", .{dp.timestamp});
        try writer.print("+{d}\r\n", .{dp.value});
    }

    return try buf.toOwnedSlice(arena);
}

/// TS.MRANGE fromTimestamp toTimestamp [LATEST] [FILTER_BY_TS ts [ts ...]] [FILTER_BY_VALUE min max] [COUNT count] [WITHLABELS | SELECTED_LABELS label [label ...]] FILTER filterExpr [filterExpr ...]
///
/// Query a range across multiple time series using label-based filtering.
/// Combines label filtering (like TS.MGET) with range query logic (like TS.RANGE).
///
/// Example:
/// TS.MRANGE 1000 3000 WITHLABELS FILTER sensor=temp
/// TS.MRANGE - + COUNT 10 FILTER location=room1
pub fn cmdTsMrange(
    storage: *Storage,
    args: []const []const u8,
    arena: std.mem.Allocator,
) ![]const u8 {
    if (args.len < 5) {
        return "ERR wrong number of arguments for 'TS.MRANGE' command\r\n";
    }

    const TimeSeriesFilter = timeseries_mod.TimeSeriesFilter;

    // Parse fromTimestamp (support "-" for earliest)
    const from_ts = if (std.mem.eql(u8, args[1], "-"))
        std.math.minInt(i64)
    else
        parseTimestamp(args[1]) catch {
            return "ERR invalid fromTimestamp\r\n";
        };

    // Parse toTimestamp (support "+" for latest)
    const to_ts = if (std.mem.eql(u8, args[2], "+"))
        std.math.maxInt(i64)
    else
        parseTimestamp(args[2]) catch {
            return "ERR invalid toTimestamp\r\n";
        };

    // Validate timestamp range
    if (from_ts > to_ts) {
        return "ERR fromTimestamp must be <= toTimestamp\r\n";
    }

    // Parse optional arguments
    var with_latest = false;
    var with_labels = false;
    var selected_labels = try std.ArrayList([]const u8).initCapacity(arena, 8);
    defer selected_labels.deinit(arena);

    var filter_by_ts = try std.ArrayList(i64).initCapacity(arena, 4);
    defer filter_by_ts.deinit(arena);

    var filter_by_value: ?struct { min: f64, max: f64 } = null;
    var count_limit: ?usize = null;

    var filters = try std.ArrayList(TimeSeriesFilter).initCapacity(arena, 8);
    defer {
        for (filters.items) |*f| {
            f.deinit();
        }
        filters.deinit(arena);
    }

    var has_positive_filter = false;

    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const option = args[i];

        if (std.ascii.eqlIgnoreCase(option, "LATEST")) {
            with_latest = true;
        } else if (std.ascii.eqlIgnoreCase(option, "WITHLABELS")) {
            with_labels = true;
        } else if (std.ascii.eqlIgnoreCase(option, "SELECTED_LABELS")) {
            i += 1;
            // Parse label names until next flag
            while (i < args.len) : (i += 1) {
                const next = args[i];
                if (std.ascii.eqlIgnoreCase(next, "FILTER") or
                    std.ascii.eqlIgnoreCase(next, "FILTER_BY_TS") or
                    std.ascii.eqlIgnoreCase(next, "FILTER_BY_VALUE") or
                    std.ascii.eqlIgnoreCase(next, "COUNT") or
                    std.ascii.eqlIgnoreCase(next, "LATEST") or
                    std.ascii.eqlIgnoreCase(next, "WITHLABELS"))
                {
                    i -= 1;
                    break;
                }
                try selected_labels.append(arena, next);
            }
        } else if (std.ascii.eqlIgnoreCase(option, "FILTER_BY_TS")) {
            i += 1;
            // Parse all following timestamps until next option or end
            while (i < args.len) : (i += 1) {
                const next = args[i];
                if (std.ascii.eqlIgnoreCase(next, "FILTER_BY_VALUE") or
                    std.ascii.eqlIgnoreCase(next, "COUNT") or
                    std.ascii.eqlIgnoreCase(next, "FILTER") or
                    std.ascii.eqlIgnoreCase(next, "LATEST") or
                    std.ascii.eqlIgnoreCase(next, "WITHLABELS") or
                    std.ascii.eqlIgnoreCase(next, "SELECTED_LABELS"))
                {
                    i -= 1;
                    break;
                }

                const ts_val = parseTimestamp(next) catch {
                    return "ERR invalid timestamp in FILTER_BY_TS\r\n";
                };
                try filter_by_ts.append(arena, ts_val);
            }
        } else if (std.ascii.eqlIgnoreCase(option, "FILTER_BY_VALUE")) {
            i += 1;
            if (i + 1 >= args.len) {
                return "ERR FILTER_BY_VALUE requires min and max values\r\n";
            }

            const min = parseValue(args[i]) catch {
                return "ERR invalid min value for FILTER_BY_VALUE\r\n";
            };
            i += 1;
            const max = parseValue(args[i]) catch {
                return "ERR invalid max value for FILTER_BY_VALUE\r\n";
            };

            if (min > max) {
                return "ERR min must be <= max for FILTER_BY_VALUE\r\n";
            }

            filter_by_value = .{ .min = min, .max = max };
        } else if (std.ascii.eqlIgnoreCase(option, "COUNT")) {
            i += 1;
            if (i >= args.len) {
                return "ERR COUNT requires a value\r\n";
            }

            count_limit = std.fmt.parseInt(usize, args[i], 10) catch {
                return "ERR invalid COUNT value\r\n";
            };
        } else if (std.ascii.eqlIgnoreCase(option, "FILTER")) {
            i += 1;
            if (i >= args.len) return "ERR FILTER requires an expression\r\n";

            const filter_expr = args[i];
            const filter_opt = try TimeSeriesFilter.parse(filter_expr, arena);

            if (filter_opt) |filter| {
                // Check if this is a positive filter
                if (filter.filter_type == .equals or filter.filter_type == .in_list) {
                    has_positive_filter = true;
                }
                try filters.append(arena, filter);
            } else {
                return "ERR invalid FILTER expression\r\n";
            }
        } else {
            return "ERR unknown option for TS.MRANGE\r\n";
        }
    }

    // Validate: at least one positive filter is required
    if (!has_positive_filter) {
        return "ERR MRANGE requires at least one positive filter (label=value or label=(v1,v2))\r\n";
    }

    // Validate: at least one filter is required
    if (filters.items.len == 0) {
        return "ERR MRANGE requires at least one FILTER\r\n";
    }

    // Scan storage for all timeseries keys matching all filters
    var results = try std.ArrayList(struct { key: []const u8, ts: *TimeSeriesValue }).initCapacity(arena, 16);
    defer results.deinit(arena);

    var iter = storage.data.iterator();
    while (iter.next()) |entry| {
        const key = entry.key_ptr.*;
        const val = entry.value_ptr;

        if (val.* != .timeseries) continue;

        var ts = &val.timeseries;

        // Check all filters - all must match
        var all_match = true;
        for (filters.items) |*filter| {
            if (!filter.matches(&ts.info)) {
                all_match = false;
                break;
            }
        }

        if (all_match) {
            try results.append(arena, .{ .key = key, .ts = ts });
        }
    }

    // Build RESP response
    var buf = try std.ArrayList(u8).initCapacity(arena, 2048);
    defer buf.deinit(arena);

    const writer = buf.writer(arena);

    // Array of results
    try writer.print("*{d}\r\n", .{results.items.len});

    for (results.items) |result| {
        const key = result.key;
        var ts = result.ts;

        // Get samples in range
        const samples = ts.getRange(from_ts, to_ts);

        // Apply filters in order: FILTER_BY_TS → FILTER_BY_VALUE → COUNT limit
        var filtered_samples = try std.ArrayList(timeseries_mod.DataPoint).initCapacity(arena, 64);
        defer filtered_samples.deinit(arena);

        for (samples) |sample| {
            // Apply FILTER_BY_TS (exact match check)
            if (filter_by_ts.items.len > 0) {
                var found = false;
                for (filter_by_ts.items) |ts_filter| {
                    if (sample.timestamp == ts_filter) {
                        found = true;
                        break;
                    }
                }
                if (!found) continue;
            }

            // Apply FILTER_BY_VALUE (range check)
            if (filter_by_value) |fbv| {
                if (sample.value < fbv.min or sample.value > fbv.max) {
                    continue;
                }
            }

            try filtered_samples.append(arena, sample);

            // Apply COUNT limit
            if (count_limit) |limit| {
                if (filtered_samples.items.len >= limit) {
                    break;
                }
            }
        }

        // For each match, format: [key, labels, [[timestamp, value], ...]]
        try writer.writeAll("*3\r\n");

        // Key
        try writer.print("${d}\r\n{s}\r\n", .{ key.len, key });

        // Labels array
        if (with_labels or selected_labels.items.len > 0) {
            if (selected_labels.items.len > 0) {
                // SELECTED_LABELS: return only specified labels
                var label_count: usize = 0;
                for (selected_labels.items) |label_name| {
                    if (ts.info.labels.get(label_name)) |_| {
                        label_count += 1;
                    }
                }

                try writer.print("*{d}\r\n", .{label_count * 2});

                for (selected_labels.items) |label_name| {
                    if (ts.info.labels.get(label_name)) |label_value| {
                        try writer.print("${d}\r\n{s}\r\n", .{ label_name.len, label_name });
                        try writer.print("${d}\r\n{s}\r\n", .{ label_value.len, label_value });
                    }
                }
            } else {
                // WITHLABELS: return all labels
                const label_count = ts.info.labels.count();
                try writer.print("*{d}\r\n", .{label_count * 2});

                var label_iter = ts.info.labels.iterator();
                while (label_iter.next()) |entry| {
                    const label_key = entry.key_ptr.*;
                    const label_value = entry.value_ptr.*;

                    try writer.print("${d}\r\n{s}\r\n", .{ label_key.len, label_key });
                    try writer.print("${d}\r\n{s}\r\n", .{ label_value.len, label_value });
                }
            }
        } else {
            // No labels
            try writer.writeAll("*0\r\n");
        }

        // Samples array [[timestamp, value], ...]
        try writer.print("*{d}\r\n", .{filtered_samples.items.len});

        for (filtered_samples.items) |sample| {
            try writer.writeAll("*2\r\n");
            try writer.print(":{d}\r\n", .{sample.timestamp});
            try writer.print("+{d}\r\n", .{sample.value});
        }
    }

    return try buf.toOwnedSlice(arena);
}

/// TS.MREVRANGE fromTimestamp toTimestamp [LATEST] [FILTER_BY_TS ts [ts ...]] [FILTER_BY_VALUE min max] [COUNT count] [WITHLABELS | SELECTED_LABELS label [label ...]] FILTER filterExpr [filterExpr ...]
///
/// Query a range across multiple time series in reverse order using label-based filtering.
/// Same syntax and behavior as TS.MRANGE, but results within each time series are returned in descending timestamp order.
///
/// Example:
/// TS.MREVRANGE 1000 3000 WITHLABELS FILTER sensor=temp
/// TS.MREVRANGE - + COUNT 10 FILTER location=room1
pub fn cmdTsMrevrange(
    storage: *Storage,
    args: []const []const u8,
    arena: std.mem.Allocator,
) ![]const u8 {
    // Call TS.MRANGE to get the results
    const result = try cmdTsMrange(storage, args, arena);

    // Check if it's an error (starts with - or +ERR)
    if (std.mem.startsWith(u8, result, "-") or std.mem.startsWith(u8, result, "+ERR")) {
        return result;
    }

    // Parse the RESP result and reverse the samples within each time series
    // Expected format: *N\r\n followed by [key, labels, [[ts, val], ...]] entries

    if (result.len == 0) {
        return result;
    }

    // Parse the array count from first line
    const first_crlf_pos = std.mem.indexOf(u8, result, "\r\n") orelse return result;
    const count_str = result[1..first_crlf_pos];
    const num_series = std.fmt.parseInt(usize, count_str, 10) catch return result;

    if (num_series == 0) {
        return result; // Empty array, nothing to reverse
    }

    // We need to parse each series and reverse its samples
    // This is complex because we need to:
    // 1. Parse the entire RESP structure
    // 2. Reverse samples within each series
    // 3. Re-serialize to RESP

    // For simplicity, we'll build a new response
    var buf = try std.ArrayList(u8).initCapacity(arena, result.len);
    defer buf.deinit(arena);

    const writer = buf.writer(arena);

    // Start parsing
    var pos: usize = first_crlf_pos + 2; // Skip "*N\r\n"

    try writer.print("*{d}\r\n", .{num_series});

    for (0..num_series) |_| {
        // Each series is: *3\r\n $key\r\nKEY\r\n *labels\r\n... *samples\r\n...

        // Skip "*3\r\n"
        while (pos < result.len and result[pos] != '\r') : (pos += 1) {}
        pos += 2;

        try writer.writeAll("*3\r\n");

        // Copy key (bulk string)
        const key_marker_pos = pos;
        while (pos < result.len and result[pos] != '\r') : (pos += 1) {}
        const key_len_str = result[key_marker_pos + 1 .. pos];
        const key_len = std.fmt.parseInt(usize, key_len_str, 10) catch continue;
        pos += 2; // Skip "\r\n"

        // Copy key data
        try writer.print("${d}\r\n", .{key_len});
        try writer.writeAll(result[pos .. pos + key_len]);
        try writer.writeAll("\r\n");
        pos += key_len + 2;

        // Copy labels array (entire array as-is)
        const labels_array_start = pos;
        while (pos < result.len and result[pos] != '\r') : (pos += 1) {}
        const labels_count_str = result[labels_array_start + 1 .. pos];
        const labels_count = std.fmt.parseInt(usize, labels_count_str, 10) catch continue;
        pos += 2;

        try writer.print("*{d}\r\n", .{labels_count});

        // Copy all label elements
        for (0..labels_count) |_| {
            // Each label is a bulk string
            const label_marker_pos = pos;
            while (pos < result.len and result[pos] != '\r') : (pos += 1) {}
            const label_len_str = result[label_marker_pos + 1 .. pos];
            const label_len = std.fmt.parseInt(usize, label_len_str, 10) catch continue;
            pos += 2;

            try writer.print("${d}\r\n", .{label_len});
            try writer.writeAll(result[pos .. pos + label_len]);
            try writer.writeAll("\r\n");
            pos += label_len + 2;
        }

        // Parse samples array and reverse
        const samples_array_start = pos;
        while (pos < result.len and result[pos] != '\r') : (pos += 1) {}
        const samples_count_str = result[samples_array_start + 1 .. pos];
        const samples_count = std.fmt.parseInt(usize, samples_count_str, 10) catch continue;
        pos += 2;

        // Store samples
        const DataPoint = struct { timestamp: i64, value: f64 };
        var samples = try std.ArrayList(DataPoint).initCapacity(arena, samples_count);
        defer samples.deinit(arena);

        for (0..samples_count) |_| {
            // Skip "*2\r\n"
            while (pos < result.len and result[pos] != '\r') : (pos += 1) {}
            pos += 2;

            // Parse timestamp ":TS\r\n"
            const ts_start = pos + 1;
            while (pos < result.len and result[pos] != '\r') : (pos += 1) {}
            const ts_str = result[ts_start..pos];
            const timestamp = std.fmt.parseInt(i64, ts_str, 10) catch continue;
            pos += 2;

            // Parse value "+VAL\r\n"
            const val_start = pos + 1;
            while (pos < result.len and result[pos] != '\r') : (pos += 1) {}
            const val_str = result[val_start..pos];
            const value = std.fmt.parseFloat(f64, val_str) catch continue;
            pos += 2;

            try samples.append(arena, .{ .timestamp = timestamp, .value = value });
        }

        // Reverse samples
        std.mem.reverse(DataPoint, samples.items);

        // Write reversed samples
        try writer.print("*{d}\r\n", .{samples.items.len});

        for (samples.items) |sample| {
            try writer.writeAll("*2\r\n");
            try writer.print(":{d}\r\n", .{sample.timestamp});
            try writer.print("+{d}\r\n", .{sample.value});
        }
    }

    return try buf.toOwnedSlice(arena);
}

/// TS.QUERYINDEX filterExpr [filterExpr ...]
///
/// Retrieve time series keys matching label filter expressions (keys only, no data).
/// Similar to TS.MGET but returns only the keys, not the data or labels.
/// Requires at least one positive filter (label=value or label=(v1,v2)).
/// All filters are combined with AND logic.
/// Returns: RESP array of matching key strings
///
/// Example:
/// TS.QUERYINDEX type=temp
/// TS.QUERYINDEX type=temp room=kitchen
/// TS.QUERYINDEX room=(kitchen,living) sensor=temp
pub fn cmdTsQueryindex(
    storage: *Storage,
    args: []const []const u8,
    arena: std.mem.Allocator,
) ![]const u8 {
    if (args.len < 2) {
        return "ERR wrong number of arguments for 'TS.QUERYINDEX' command\r\n";
    }

    const TimeSeriesFilter = timeseries_mod.TimeSeriesFilter;

    // Parse filter expressions from args[1..]
    var filters = try std.ArrayList(TimeSeriesFilter).initCapacity(arena, 8);
    defer {
        for (filters.items) |*f| {
            f.deinit();
        }
        filters.deinit(arena);
    }

    var has_positive_filter = false;

    for (args[1..]) |filter_expr| {
        const filter_opt = try TimeSeriesFilter.parse(filter_expr, arena);

        if (filter_opt) |filter| {
            // Check if this is a positive filter
            if (filter.filter_type == .equals or filter.filter_type == .in_list) {
                has_positive_filter = true;
            }
            try filters.append(arena, filter);
        } else {
            return "ERR invalid FILTER expression\r\n";
        }
    }

    // Validate: at least one positive filter is required
    if (!has_positive_filter) {
        return "ERR QUERYINDEX requires at least one positive filter (label=value or label=(v1,v2))\r\n";
    }

    // Validate: at least one filter is required
    if (filters.items.len == 0) {
        return "ERR QUERYINDEX requires at least one filter\r\n";
    }

    // Scan storage for all timeseries keys matching all filters
    var matching_keys = try std.ArrayList([]const u8).initCapacity(arena, 16);
    defer matching_keys.deinit(arena);

    var iter = storage.data.iterator();
    while (iter.next()) |entry| {
        const key = entry.key_ptr.*;
        const val = entry.value_ptr;

        if (val.* != .timeseries) continue;

        var ts = &val.timeseries;

        // Check all filters - all must match
        var all_match = true;
        for (filters.items) |*filter| {
            if (!filter.matches(&ts.info)) {
                all_match = false;
                break;
            }
        }

        if (all_match) {
            try matching_keys.append(arena, key);
        }
    }

    // Build RESP response
    var buf = try std.ArrayList(u8).initCapacity(arena, 512);
    defer buf.deinit(arena);

    const writer = buf.writer(arena);

    // Array of keys
    try writer.print("*{d}\r\n", .{matching_keys.items.len});

    for (matching_keys.items) |key| {
        try writer.print("${d}\r\n{s}\r\n", .{ key.len, key });
    }

    return try buf.toOwnedSlice(arena);
}

/// TS.CREATERULE sourceKey destKey AGGREGATION aggregator bucketDuration
///
/// Create a compaction rule to downsample data from source to destination time series.
/// The aggregator specifies how to aggregate samples within each bucket.
/// Bucket duration is in milliseconds.
///
/// Returns: OK on success, error if source/dest keys don't exist or rule already exists
///
/// Example:
/// TS.CREATERULE sensor:temp:raw sensor:temp:avg AGGREGATION avg 60000
pub fn cmdTsCreaterule(
    storage: *Storage,
    args: []const []const u8,
    arena: std.mem.Allocator,
) ![]const u8 {
    _ = arena; // Not needed for this command

    if (args.len != 6) {
        return "ERR wrong number of arguments for 'TS.CREATERULE' command\r\n";
    }

    const source_key = args[1];
    const dest_key = args[2];

    // Validate AGGREGATION keyword
    if (!std.ascii.eqlIgnoreCase(args[3], "AGGREGATION")) {
        return "ERR syntax error, expected AGGREGATION keyword\r\n";
    }

    // Parse aggregation type
    const aggregator_str = args[4];
    const aggregation = AggregationType.fromString(aggregator_str) orelse {
        return "ERR invalid aggregation type (must be AVG, SUM, MIN, MAX, RANGE, COUNT, FIRST, LAST, STD.P, STD.S, VAR.P, VAR.S, or TWA)\r\n";
    };

    // Parse bucket duration
    const bucket_duration_ms = std.fmt.parseInt(i64, args[5], 10) catch {
        return "ERR invalid bucket duration (must be positive integer in milliseconds)\r\n";
    };
    if (bucket_duration_ms <= 0) {
        return "ERR bucket duration must be positive\r\n";
    }

    // Validate that source key exists and is a time series
    const source_entry = storage.data.getPtr(source_key) orelse {
        return "ERR source key does not exist\r\n";
    };
    if (source_entry.* != .timeseries) {
        return "ERR source key is not a time series\r\n";
    }

    // Validate that destination key exists and is a time series
    const dest_entry = storage.data.getPtr(dest_key) orelse {
        return "ERR destination key does not exist\r\n";
    };
    if (dest_entry.* != .timeseries) {
        return "ERR destination key is not a time series\r\n";
    }

    // Check if rule already exists
    var source_ts = &source_entry.timeseries;
    for (source_ts.info.rules.items) |*rule| {
        if (std.mem.eql(u8, rule.dest_key, dest_key)) {
            return "ERR compaction rule already exists for this destination key\r\n";
        }
    }

    // Create the compaction rule
    var rule = try CompactionRule.init(storage.allocator, dest_key, aggregation, bucket_duration_ms);
    errdefer rule.deinit();

    try source_ts.info.rules.append(storage.allocator, rule);

    return "+OK\r\n";
}

/// TS.DELETERULE sourceKey destKey
///
/// Delete a compaction rule from source to destination time series.
///
/// Returns: OK on success, error if source key doesn't exist or rule not found
///
/// Example:
/// TS.DELETERULE sensor:temp:raw sensor:temp:avg
pub fn cmdTsDeleterule(
    storage: *Storage,
    args: []const []const u8,
    arena: std.mem.Allocator,
) ![]const u8 {
    _ = arena; // Not needed for this command

    if (args.len != 3) {
        return "ERR wrong number of arguments for 'TS.DELETERULE' command\r\n";
    }

    const source_key = args[1];
    const dest_key = args[2];

    // Validate that source key exists and is a time series
    const source_entry = storage.data.getPtr(source_key) orelse {
        return "ERR source key does not exist\r\n";
    };
    if (source_entry.* != .timeseries) {
        return "ERR source key is not a time series\r\n";
    }

    var source_ts = &source_entry.timeseries;

    // Find and remove the rule
    var found_index: ?usize = null;
    for (source_ts.info.rules.items, 0..) |*rule, i| {
        if (std.mem.eql(u8, rule.dest_key, dest_key)) {
            found_index = i;
            break;
        }
    }

    if (found_index == null) {
        return "ERR compaction rule not found\r\n";
    }

    // Remove the rule and clean up
    var removed_rule = source_ts.info.rules.orderedRemove(found_index.?);
    removed_rule.deinit();

    return "+OK\r\n";
}

//
// Unit tests
//

test "TS.DEL basic" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series with samples
    const create_cmd = [_][]const u8{ "TS.CREATE", "myts" };
    const create_result = try cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    _ = try cmdTsAdd(&storage, &[_][]const u8{ "TS.ADD", "myts", "1000", "10.0" }, allocator);
    _ = try cmdTsAdd(&storage, &[_][]const u8{ "TS.ADD", "myts", "2000", "20.0" }, allocator);
    _ = try cmdTsAdd(&storage, &[_][]const u8{ "TS.ADD", "myts", "3000", "30.0" }, allocator);
    _ = try cmdTsAdd(&storage, &[_][]const u8{ "TS.ADD", "myts", "4000", "40.0" }, allocator);

    // Delete range
    const del_cmd = [_][]const u8{ "TS.DEL", "myts", "1500", "3500" };
    const result = try cmdTsDel(&storage, &del_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":2\r\n", result);

    // Verify samples were deleted
    const value = storage.get("myts").?;
    try std.testing.expectEqual(@as(usize, 2), value.timeseries.samples.items.len);
}

test "TS.DEL nonexistent key error" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    const del_cmd = [_][]const u8{ "TS.DEL", "nonexistent", "1000", "2000" };
    const result = try cmdTsDel(&storage, &del_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "ERR key does not exist"));
}

test "TS.DEL WRONGTYPE error" {
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

    const del_cmd = [_][]const u8{ "TS.DEL", "mystring", "1000", "2000" };
    const result = try cmdTsDel(&storage, &del_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-WRONGTYPE"));
}

test "TS.DEL invalid range error" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    const create_cmd = [_][]const u8{ "TS.CREATE", "myts" };
    const create_result = try cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    // from > to
    const del_cmd = [_][]const u8{ "TS.DEL", "myts", "5000", "3000" };
    const result = try cmdTsDel(&storage, &del_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "ERR fromTimestamp must be"));
}

test "TS.GET basic" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series with samples
    const create_cmd = [_][]const u8{ "TS.CREATE", "myts" };
    const create_result = try cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    _ = try cmdTsAdd(&storage, &[_][]const u8{ "TS.ADD", "myts", "1000", "10.5" }, allocator);
    _ = try cmdTsAdd(&storage, &[_][]const u8{ "TS.ADD", "myts", "2000", "20.5" }, allocator);

    const get_cmd = [_][]const u8{ "TS.GET", "myts" };
    const result = try cmdTsGet(&storage, &get_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("*2\r\n:2000\r\n+20.5\r\n", result);
}

test "TS.GET with LATEST flag" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    const create_cmd = [_][]const u8{ "TS.CREATE", "myts" };
    const create_result = try cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    _ = try cmdTsAdd(&storage, &[_][]const u8{ "TS.ADD", "myts", "1000", "10.0" }, allocator);

    const get_cmd = [_][]const u8{ "TS.GET", "myts", "LATEST" };
    const result = try cmdTsGet(&storage, &get_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("*2\r\n:1000\r\n+10\r\n", result);
}

test "TS.GET empty series" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    const create_cmd = [_][]const u8{ "TS.CREATE", "myts" };
    const create_result = try cmdTsCreate(&storage, &create_cmd, allocator);
    defer allocator.free(create_result);

    const get_cmd = [_][]const u8{ "TS.GET", "myts" };
    const result = try cmdTsGet(&storage, &get_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "TS.GET nonexistent key error" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    const get_cmd = [_][]const u8{ "TS.GET", "nonexistent" };
    const result = try cmdTsGet(&storage, &get_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "ERR key does not exist"));
}

test "TS.GET WRONGTYPE error" {
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

    const get_cmd = [_][]const u8{ "TS.GET", "mystring" };
    const result = try cmdTsGet(&storage, &get_cmd, allocator);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-WRONGTYPE"));
}

test "TS.QUERYINDEX basic single filter" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series with labels
    const create1 = [_][]const u8{ "TS.CREATE", "sensor:temp:room1", "LABELS", "type", "temp", "room", "room1" };
    _ = try cmdTsCreate(&storage, &create1, allocator);

    const create2 = [_][]const u8{ "TS.CREATE", "sensor:temp:room2", "LABELS", "type", "temp", "room", "room2" };
    _ = try cmdTsCreate(&storage, &create2, allocator);

    const create3 = [_][]const u8{ "TS.CREATE", "sensor:humidity:room1", "LABELS", "type", "humidity", "room", "room1" };
    _ = try cmdTsCreate(&storage, &create3, allocator);

    // Query for type=temp
    const query_cmd = [_][]const u8{ "TS.QUERYINDEX", "type=temp" };
    const result = try cmdTsQueryindex(&storage, &query_cmd, allocator);
    defer allocator.free(result);

    // Should return 2 keys: sensor:temp:room1 and sensor:temp:room2
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try std.testing.expect(std.mem.containsAtLeast(u8, result, 1, "sensor:temp:room1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, result, 1, "sensor:temp:room2"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, result, 1, "sensor:humidity:room1"));
}

test "TS.QUERYINDEX multiple filters AND logic" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series with labels
    const create1 = [_][]const u8{ "TS.CREATE", "sensor:1", "LABELS", "type", "temp", "room", "kitchen" };
    _ = try cmdTsCreate(&storage, &create1, allocator);

    const create2 = [_][]const u8{ "TS.CREATE", "sensor:2", "LABELS", "type", "temp", "room", "living" };
    _ = try cmdTsCreate(&storage, &create2, allocator);

    const create3 = [_][]const u8{ "TS.CREATE", "sensor:3", "LABELS", "type", "humidity", "room", "kitchen" };
    _ = try cmdTsCreate(&storage, &create3, allocator);

    // Query for type=temp AND room=kitchen
    const query_cmd = [_][]const u8{ "TS.QUERYINDEX", "type=temp", "room=kitchen" };
    const result = try cmdTsQueryindex(&storage, &query_cmd, allocator);
    defer allocator.free(result);

    // Should return only sensor:1 (matches both filters)
    try std.testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    try std.testing.expect(std.mem.containsAtLeast(u8, result, 1, "sensor:1"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, result, 1, "sensor:2"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, result, 1, "sensor:3"));
}

test "TS.QUERYINDEX list filter" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series with labels
    const create1 = [_][]const u8{ "TS.CREATE", "sensor:1", "LABELS", "room", "kitchen" };
    _ = try cmdTsCreate(&storage, &create1, allocator);

    const create2 = [_][]const u8{ "TS.CREATE", "sensor:2", "LABELS", "room", "living" };
    _ = try cmdTsCreate(&storage, &create2, allocator);

    const create3 = [_][]const u8{ "TS.CREATE", "sensor:3", "LABELS", "room", "basement" };
    _ = try cmdTsCreate(&storage, &create3, allocator);

    // Query for room in (kitchen, living)
    const query_cmd = [_][]const u8{ "TS.QUERYINDEX", "room=(kitchen,living)" };
    const result = try cmdTsQueryindex(&storage, &query_cmd, allocator);
    defer allocator.free(result);

    // Should return sensor:1 and sensor:2
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try std.testing.expect(std.mem.containsAtLeast(u8, result, 1, "sensor:1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, result, 1, "sensor:2"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, result, 1, "sensor:3"));
}

test "TS.QUERYINDEX negative filter" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series with labels
    const create1 = [_][]const u8{ "TS.CREATE", "sensor:1", "LABELS", "type", "temp", "room", "basement" };
    _ = try cmdTsCreate(&storage, &create1, allocator);

    const create2 = [_][]const u8{ "TS.CREATE", "sensor:2", "LABELS", "type", "temp", "room", "kitchen" };
    _ = try cmdTsCreate(&storage, &create2, allocator);

    const create3 = [_][]const u8{ "TS.CREATE", "sensor:3", "LABELS", "type", "humidity", "room", "kitchen" };
    _ = try cmdTsCreate(&storage, &create3, allocator);

    // Query for type=temp AND room!=basement
    const query_cmd = [_][]const u8{ "TS.QUERYINDEX", "type=temp", "room!=basement" };
    const result = try cmdTsQueryindex(&storage, &query_cmd, allocator);
    defer allocator.free(result);

    // Should return only sensor:2
    try std.testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    try std.testing.expect(std.mem.containsAtLeast(u8, result, 1, "sensor:2"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, result, 1, "sensor:1"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, result, 1, "sensor:3"));
}

test "TS.QUERYINDEX exists filter" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series with different labels
    const create1 = [_][]const u8{ "TS.CREATE", "sensor:1", "LABELS", "type", "temp", "location", "room1" };
    _ = try cmdTsCreate(&storage, &create1, allocator);

    const create2 = [_][]const u8{ "TS.CREATE", "sensor:2", "LABELS", "type", "humidity" };
    _ = try cmdTsCreate(&storage, &create2, allocator);

    const create3 = [_][]const u8{ "TS.CREATE", "sensor:3", "LABELS", "type", "pressure", "location", "room2" };
    _ = try cmdTsCreate(&storage, &create3, allocator);

    // Query for keys that have location label (exists)
    const query_cmd = [_][]const u8{ "TS.QUERYINDEX", "location=" };
    const result = try cmdTsQueryindex(&storage, &query_cmd, allocator);
    defer allocator.free(result);

    // Should return sensor:1 and sensor:3 (have location label)
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    try std.testing.expect(std.mem.containsAtLeast(u8, result, 1, "sensor:1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, result, 1, "sensor:3"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, result, 1, "sensor:2"));
}

test "TS.QUERYINDEX not_exists filter" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series with different labels
    const create1 = [_][]const u8{ "TS.CREATE", "sensor:1", "LABELS", "type", "temp", "location", "room1" };
    _ = try cmdTsCreate(&storage, &create1, allocator);

    const create2 = [_][]const u8{ "TS.CREATE", "sensor:2", "LABELS", "type", "humidity" };
    _ = try cmdTsCreate(&storage, &create2, allocator);

    const create3 = [_][]const u8{ "TS.CREATE", "sensor:3", "LABELS", "type", "pressure", "location", "room2" };
    _ = try cmdTsCreate(&storage, &create3, allocator);

    // Query for keys that don't have location label (not_exists)
    const query_cmd = [_][]const u8{ "TS.QUERYINDEX", "location!=" };
    const result = try cmdTsQueryindex(&storage, &query_cmd, allocator);
    defer allocator.free(result);

    // Should return sensor:2 (doesn't have location label)
    try std.testing.expect(std.mem.startsWith(u8, result, "*1\r\n"));
    try std.testing.expect(std.mem.containsAtLeast(u8, result, 1, "sensor:2"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, result, 1, "sensor:1"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, result, 1, "sensor:3"));
}

test "TS.QUERYINDEX empty result set" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Create time series with labels
    const create1 = [_][]const u8{ "TS.CREATE", "sensor:1", "LABELS", "type", "temp" };
    _ = try cmdTsCreate(&storage, &create1, allocator);

    // Query for type=humidity (doesn't exist)
    const query_cmd = [_][]const u8{ "TS.QUERYINDEX", "type=humidity" };
    const result = try cmdTsQueryindex(&storage, &query_cmd, allocator);
    defer allocator.free(result);

    // Should return empty array
    try std.testing.expectEqualStrings("*0\r\n", result);
}

test "TS.QUERYINDEX no positive filter error" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Query with only negative filter (no positive filter)
    const query_cmd = [_][]const u8{ "TS.QUERYINDEX", "type!=temp" };
    const result = try cmdTsQueryindex(&storage, &query_cmd, allocator);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.startsWith(u8, result, "ERR"));
    try std.testing.expect(std.mem.containsAtLeast(u8, result, 1, "positive filter"));
}

test "TS.QUERYINDEX invalid filter syntax error" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Query with invalid filter syntax (no = or !=)
    const query_cmd = [_][]const u8{ "TS.QUERYINDEX", "invalidfilter" };
    const result = try cmdTsQueryindex(&storage, &query_cmd, allocator);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.startsWith(u8, result, "ERR invalid FILTER"));
}

test "TS.QUERYINDEX arity error" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    // Query with no filters
    const query_cmd = [_][]const u8{ "TS.QUERYINDEX" };
    const result = try cmdTsQueryindex(&storage, &query_cmd, allocator);
    defer allocator.free(result);

    // Should return arity error
    try std.testing.expect(std.mem.startsWith(u8, result, "ERR wrong number"));
}

test "TS.CREATERULE basic" {
    const allocator = std.testing.allocator;

    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();

    var arena_allocator = std.heap.ArenaAllocator.init(allocator);
    defer arena_allocator.deinit();
    const arena = arena_allocator.allocator();

    // Create source and destination time series
    const create_args = [_][]const u8{ "TS.CREATE", "sensor:temp:raw" };
    _ = try cmdTsCreate(&storage, &create_args, arena);

    const create_dest_args = [_][]const u8{ "TS.CREATE", "sensor:temp:avg" };
    _ = try cmdTsCreate(&storage, &create_dest_args, arena);

    // Create compaction rule
    const args = [_][]const u8{ "TS.CREATERULE", "sensor:temp:raw", "sensor:temp:avg", "AGGREGATION", "avg", "60000" };
    const result = try cmdTsCreaterule(&storage, &args, arena);

    try std.testing.expect(std.mem.eql(u8, result, "+OK\r\n"));

    // Verify rule was added
    const entry = storage.data.get("sensor:temp:raw").?;
    try std.testing.expectEqual(@as(usize, 1), entry.timeseries.info.rules.items.len);
    const rule = &entry.timeseries.info.rules.items[0];
    try std.testing.expect(std.mem.eql(u8, rule.dest_key, "sensor:temp:avg"));
    try std.testing.expectEqual(AggregationType.avg, rule.aggregation);
    try std.testing.expectEqual(@as(i64, 60000), rule.bucket_duration_ms);
}

test "TS.CREATERULE source key not exists" {
    const allocator = std.testing.allocator;
    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();
    var arena_allocator = std.heap.ArenaAllocator.init(allocator);
    defer arena_allocator.deinit();
    const arena = arena_allocator.allocator();
    const args = [_][]const u8{ "TS.CREATERULE", "nonexistent", "dest", "AGGREGATION", "avg", "60000" };
    const result = try cmdTsCreaterule(&storage, &args, arena);
    try std.testing.expect(std.mem.startsWith(u8, result, "ERR source key does not exist"));
}

test "TS.CREATERULE dest key not exists" {
    const allocator = std.testing.allocator;
    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();
    var arena_allocator = std.heap.ArenaAllocator.init(allocator);
    defer arena_allocator.deinit();
    const arena = arena_allocator.allocator();
    const create_args = [_][]const u8{ "TS.CREATE", "source" };
    _ = try cmdTsCreate(&storage, &create_args, arena);
    const args = [_][]const u8{ "TS.CREATERULE", "source", "nonexistent", "AGGREGATION", "avg", "60000" };
    const result = try cmdTsCreaterule(&storage, &args, arena);
    try std.testing.expect(std.mem.startsWith(u8, result, "ERR destination key does not exist"));
}

test "TS.CREATERULE source not time series" {
    const allocator = std.testing.allocator;
    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();
    var arena_allocator = std.heap.ArenaAllocator.init(allocator);
    defer arena_allocator.deinit();
    const arena = arena_allocator.allocator();
    const set_args = [_][]const u8{ "SET", "source", "value" };
    _ = try @import("strings.zig").cmdSet(&storage, &set_args, arena);
    const create_dest_args = [_][]const u8{ "TS.CREATE", "dest" };
    _ = try cmdTsCreate(&storage, &create_dest_args, arena);
    const args = [_][]const u8{ "TS.CREATERULE", "source", "dest", "AGGREGATION", "avg", "60000" };
    const result = try cmdTsCreaterule(&storage, &args, arena);
    try std.testing.expect(std.mem.startsWith(u8, result, "ERR source key is not a time series"));
}

test "TS.CREATERULE duplicate rule" {
    const allocator = std.testing.allocator;
    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();
    var arena_allocator = std.heap.ArenaAllocator.init(allocator);
    defer arena_allocator.deinit();
    const arena = arena_allocator.allocator();
    const create_args = [_][]const u8{ "TS.CREATE", "source" };
    _ = try cmdTsCreate(&storage, &create_args, arena);
    const create_dest_args = [_][]const u8{ "TS.CREATE", "dest" };
    _ = try cmdTsCreate(&storage, &create_dest_args, arena);
    const args = [_][]const u8{ "TS.CREATERULE", "source", "dest", "AGGREGATION", "avg", "60000" };
    _ = try cmdTsCreaterule(&storage, &args, arena);
    const result = try cmdTsCreaterule(&storage, &args, arena);
    try std.testing.expect(std.mem.startsWith(u8, result, "ERR compaction rule already exists"));
}

test "TS.DELETERULE basic" {
    const allocator = std.testing.allocator;
    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();
    var arena_allocator = std.heap.ArenaAllocator.init(allocator);
    defer arena_allocator.deinit();
    const arena = arena_allocator.allocator();
    const create_args = [_][]const u8{ "TS.CREATE", "source" };
    _ = try cmdTsCreate(&storage, &create_args, arena);
    const create_dest_args = [_][]const u8{ "TS.CREATE", "dest" };
    _ = try cmdTsCreate(&storage, &create_dest_args, arena);
    const create_rule_args = [_][]const u8{ "TS.CREATERULE", "source", "dest", "AGGREGATION", "avg", "60000" };
    _ = try cmdTsCreaterule(&storage, &create_rule_args, arena);
    const args = [_][]const u8{ "TS.DELETERULE", "source", "dest" };
    const result = try cmdTsDeleterule(&storage, &args, arena);
    try std.testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
    const entry = storage.data.get("source").?;
    try std.testing.expectEqual(@as(usize, 0), entry.timeseries.info.rules.items.len);
}

test "TS.DELETERULE source not exists" {
    const allocator = std.testing.allocator;
    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();
    var arena_allocator = std.heap.ArenaAllocator.init(allocator);
    defer arena_allocator.deinit();
    const arena = arena_allocator.allocator();
    const args = [_][]const u8{ "TS.DELETERULE", "nonexistent", "dest" };
    const result = try cmdTsDeleterule(&storage, &args, arena);
    try std.testing.expect(std.mem.startsWith(u8, result, "ERR source key does not exist"));
}

test "TS.DELETERULE rule not found" {
    const allocator = std.testing.allocator;
    var config = @import("../storage/memory.zig").Config.default();
    var storage = try @import("../storage/memory.zig").Storage.init(allocator, &config);
    defer storage.deinit();
    var arena_allocator = std.heap.ArenaAllocator.init(allocator);
    defer arena_allocator.deinit();
    const arena = arena_allocator.allocator();
    const create_args = [_][]const u8{ "TS.CREATE", "source" };
    _ = try cmdTsCreate(&storage, &create_args, arena);
    const args = [_][]const u8{ "TS.DELETERULE", "source", "nonexistent" };
    const result = try cmdTsDeleterule(&storage, &args, arena);
    try std.testing.expect(std.mem.startsWith(u8, result, "ERR compaction rule not found"));
}
