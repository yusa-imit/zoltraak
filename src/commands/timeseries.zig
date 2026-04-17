const std = @import("std");
const Storage = @import("../storage/memory.zig").Storage;
const Value = @import("../storage/memory.zig").Value;
const TimeSeriesValue = @import("../storage/memory.zig").TimeSeriesValue;
const timeseries_mod = @import("../storage/timeseries.zig");
const DuplicatePolicy = timeseries_mod.DuplicatePolicy;
const Encoding = timeseries_mod.Encoding;

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
    var labels = std.ArrayList(struct { key: []const u8, value: []const u8 }).init(storage.allocator);
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
    try storage.put(key, Value{ .timeseries = ts });

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

    const value = storage.get(key) orelse {
        return "ERR key does not exist\r\n";
    };

    if (value.* != .timeseries) {
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    const ts = &value.timeseries;

    // Format response as RESP array
    var buf = std.ArrayList(u8).init(storage.allocator);
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
