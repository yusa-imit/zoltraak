const std = @import("std");
const storage_mod = @import("../storage/memory.zig");
const writer_mod = @import("../protocol/writer.zig");
const parser_mod = @import("../protocol/parser.zig");

const Storage = storage_mod.Storage;
const Writer = writer_mod.Writer;
const RespValue = parser_mod.RespValue;

/// Handle SENTINEL PING command
/// Returns a simple string +PONG\r\n
pub fn cmdSentinelPing(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    _ = storage;

    // SENTINEL PING takes no additional arguments (only SENTINEL and PING in args[0] and args[1])
    if (args.len > 2) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'sentinel|ping' command");
    }

    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeSimpleString("PONG");
}

/// Helper to free a RespValue and its contents
fn deinitRespValue(allocator: std.mem.Allocator, value: RespValue) void {
    switch (value) {
        .bulk_string => |s| allocator.free(s),
        .array => |arr| {
            for (arr) |item| {
                deinitRespValue(allocator, item);
            }
            allocator.free(arr);
        },
        else => {},
    }
}

/// Handle SENTINEL MASTERS command
/// Returns array of all monitored masters with their info
pub fn cmdSentinelMasters(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // SENTINEL MASTERS takes no additional arguments
    if (args.len > 2) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'sentinel|masters' command");
    }

    // Check if Sentinel mode is enabled
    if (!storage.sentinel.enabled) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR This instance has sentinel mode disabled");
    }

    const masters = try storage.sentinel.getMasters();
    defer allocator.free(masters);

    // Build array of master info arrays
    var master_arrays = try std.ArrayList(RespValue).initCapacity(allocator, masters.len);
    defer {
        for (master_arrays.items) |item| {
            deinitRespValue(allocator, item);
        }
        master_arrays.deinit(allocator);
    }

    for (masters) |master| {
        // Build array of key-value pairs for this master
        var fields = try std.ArrayList(RespValue).initCapacity(allocator, 22);
        errdefer {
            for (fields.items) |item| {
                deinitRespValue(allocator, item);
            }
            fields.deinit(allocator);
        }

        // name
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "name") });
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, master.name) });

        // ip
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "ip") });
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, master.ip) });

        // port
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "port") });
        const port_str = try std.fmt.allocPrint(allocator, "{d}", .{master.port});
        try fields.append(allocator, RespValue{ .bulk_string = port_str });

        // quorum
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "quorum") });
        const quorum_str = try std.fmt.allocPrint(allocator, "{d}", .{master.quorum});
        try fields.append(allocator, RespValue{ .bulk_string = quorum_str });

        // down-after-milliseconds
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "down-after-milliseconds") });
        const down_after_str = try std.fmt.allocPrint(allocator, "{d}", .{master.down_after_milliseconds});
        try fields.append(allocator, RespValue{ .bulk_string = down_after_str });

        // last-ping-sent
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "last-ping-sent") });
        const last_ping_str = try std.fmt.allocPrint(allocator, "{d}", .{master.last_ping_time});
        try fields.append(allocator, RespValue{ .bulk_string = last_ping_str });

        // last-ok-ping-reply
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "last-ok-ping-reply") });
        const last_pong_str = try std.fmt.allocPrint(allocator, "{d}", .{master.last_pong_time});
        try fields.append(allocator, RespValue{ .bulk_string = last_pong_str });

        // flags
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "flags") });
        if (master.is_down) {
            try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "master,s_down") });
        } else {
            try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "master") });
        }

        // num-slaves
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "num-slaves") });
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "0") });

        // num-other-sentinels
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "num-other-sentinels") });
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "0") });

        // parallel-syncs
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "parallel-syncs") });
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "1") });

        const fields_array = try fields.toOwnedSlice(allocator);
        try master_arrays.append(allocator, RespValue{ .array = fields_array });
    }

    var w = Writer.init(allocator);
    defer w.deinit();

    const master_arrays_slice = try master_arrays.toOwnedSlice(allocator);
    defer {
        for (master_arrays_slice) |item| {
            deinitRespValue(allocator, item);
        }
        allocator.free(master_arrays_slice);
    }

    return try w.writeArray(master_arrays_slice);
}

/// Handle SENTINEL MONITOR command
/// Adds a master to the monitoring list
pub fn cmdSentinelMonitor(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // SENTINEL MONITOR <name> <ip> <port> <quorum>
    if (args.len != 6) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'sentinel|monitor' command");
    }

    // Check if Sentinel mode is enabled
    if (!storage.sentinel.enabled) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR This instance has sentinel mode disabled");
    }

    const name = args[2];
    const ip = args[3];
    const port_str = args[4];
    const quorum_str = args[5];

    // Parse port
    const port = std.fmt.parseInt(u16, port_str, 10) catch {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR Invalid port number");
    };

    // Parse quorum
    const quorum = std.fmt.parseInt(u8, quorum_str, 10) catch {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR Invalid quorum value");
    };

    // Validate quorum is at least 1
    if (quorum == 0) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR Quorum must be 1 or greater");
    }

    // Add master to monitoring
    storage.sentinel.monitorMaster(name, ip, port, quorum) catch |err| {
        var w = Writer.init(allocator);
        defer w.deinit();
        if (err == error.MasterAlreadyExists) {
            return try w.writeError("ERR Duplicated master name");
        }
        return try w.writeError("ERR Failed to monitor master");
    };

    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeSimpleString("OK");
}

/// Handle SENTINEL REMOVE command
/// Removes a master from the monitoring list
pub fn cmdSentinelRemove(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // SENTINEL REMOVE <name>
    if (args.len != 3) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'sentinel|remove' command");
    }

    // Check if Sentinel mode is enabled
    if (!storage.sentinel.enabled) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR This instance has sentinel mode disabled");
    }

    const name = args[2];

    // Remove master from monitoring
    storage.sentinel.removeMaster(name) catch |err| {
        var w = Writer.init(allocator);
        defer w.deinit();
        if (err == error.MasterNotFound) {
            return try w.writeError("ERR No such master with that name");
        }
        return try w.writeError("ERR Failed to remove master");
    };

    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeSimpleString("OK");
}

/// Handle SENTINEL MASTER command
/// Returns info for a specific monitored master
pub fn cmdSentinelMaster(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // SENTINEL MASTER <name>
    if (args.len != 3) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'sentinel|master' command");
    }

    // Check if Sentinel mode is enabled
    if (!storage.sentinel.enabled) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR This instance has sentinel mode disabled");
    }

    const name = args[2];

    // Get master by name
    const master = storage.sentinel.getMaster(name) orelse {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR No such master with that name");
    };

    // Build array of key-value pairs (same format as SENTINEL MASTERS but single item)
    var fields = try std.ArrayList(RespValue).initCapacity(allocator, 22);
    defer {
        for (fields.items) |item| {
            deinitRespValue(allocator, item);
        }
        fields.deinit(allocator);
    }

    // name
    try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "name") });
    try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, master.name) });

    // ip
    try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "ip") });
    try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, master.ip) });

    // port
    try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "port") });
    const port_str = try std.fmt.allocPrint(allocator, "{d}", .{master.port});
    try fields.append(allocator, RespValue{ .bulk_string = port_str });

    // quorum
    try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "quorum") });
    const quorum_str = try std.fmt.allocPrint(allocator, "{d}", .{master.quorum});
    try fields.append(allocator, RespValue{ .bulk_string = quorum_str });

    // down-after-milliseconds
    try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "down-after-milliseconds") });
    const down_after_str = try std.fmt.allocPrint(allocator, "{d}", .{master.down_after_milliseconds});
    try fields.append(allocator, RespValue{ .bulk_string = down_after_str });

    // last-ping-sent
    try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "last-ping-sent") });
    const last_ping_str = try std.fmt.allocPrint(allocator, "{d}", .{master.last_ping_time});
    try fields.append(allocator, RespValue{ .bulk_string = last_ping_str });

    // last-ok-ping-reply
    try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "last-ok-ping-reply") });
    const last_pong_str = try std.fmt.allocPrint(allocator, "{d}", .{master.last_pong_time});
    try fields.append(allocator, RespValue{ .bulk_string = last_pong_str });

    // flags
    try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "flags") });
    if (master.is_down) {
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "master,s_down") });
    } else {
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "master") });
    }

    // num-slaves
    try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "num-slaves") });
    try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "0") });

    // num-other-sentinels
    try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "num-other-sentinels") });
    try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "0") });

    // parallel-syncs
    try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "parallel-syncs") });
    try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "1") });

    var w = Writer.init(allocator);
    defer w.deinit();

    const fields_array = try fields.toOwnedSlice(allocator);
    defer {
        for (fields_array) |item| {
            deinitRespValue(allocator, item);
        }
        allocator.free(fields_array);
    }

    return try w.writeArray(fields_array);
}

/// Handle SENTINEL REPLICAS command
/// Returns array of all replicas for a specific master
/// NOTE: Replica tracking not yet implemented, returns empty array
pub fn cmdSentinelReplicas(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // SENTINEL REPLICAS <name>
    if (args.len != 3) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'sentinel|replicas' command");
    }

    // Check if Sentinel mode is enabled
    if (!storage.sentinel.enabled) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR This instance has sentinel mode disabled");
    }

    const name = args[2];

    // Verify master exists
    _ = storage.sentinel.getMaster(name) orelse {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR No such master with that name");
    };

    // TODO: Implement replica tracking in future iterations
    // For now, return empty array (no replicas tracked)
    var w = Writer.init(allocator);
    defer w.deinit();
    const empty_array: []const RespValue = &[_]RespValue{};
    return try w.writeArray(empty_array);
}

/// Handle SENTINEL GET-MASTER-ADDR-BY-NAME command
/// Returns [ip, port] for a specific master
pub fn cmdSentinelGetMasterAddrByName(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // SENTINEL GET-MASTER-ADDR-BY-NAME <name>
    if (args.len != 3) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'sentinel|get-master-addr-by-name' command");
    }

    // Check if Sentinel mode is enabled
    if (!storage.sentinel.enabled) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR This instance has sentinel mode disabled");
    }

    const name = args[2];

    // Get master by name
    const master = storage.sentinel.getMaster(name) orelse {
        // Return nil when master doesn't exist (Redis behavior)
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeNull();
    };

    // Build [ip, port] array
    var addr_array = [_]RespValue{
        RespValue{ .bulk_string = try allocator.dupe(u8, master.ip) },
        RespValue{ .bulk_string = try std.fmt.allocPrint(allocator, "{d}", .{master.port}) },
    };
    defer {
        for (addr_array) |item| {
            deinitRespValue(allocator, item);
        }
    }

    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeArray(&addr_array);
}
