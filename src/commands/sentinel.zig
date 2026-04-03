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

/// Handle SENTINEL SENTINELS command
/// Returns array of all other Sentinels monitoring a specific master
pub fn cmdSentinelSentinels(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // SENTINEL SENTINELS <name>
    if (args.len != 3) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'sentinel|sentinels' command");
    }

    // Check if Sentinel mode is enabled
    if (!storage.sentinel.enabled) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR This instance has sentinel mode disabled");
    }

    const name = args[2];

    // Get sentinels for this master
    const sentinels_opt = try storage.sentinel.getSentinels(name);
    if (sentinels_opt == null) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR No such master with that name");
    }

    const sentinels = sentinels_opt.?;
    defer allocator.free(sentinels);

    // Build array of sentinel info arrays
    var sentinel_arrays = try std.ArrayList(RespValue).initCapacity(allocator, sentinels.len);
    defer {
        for (sentinel_arrays.items) |item| {
            deinitRespValue(allocator, item);
        }
        sentinel_arrays.deinit(allocator);
    }

    for (sentinels) |sentinel| {
        // Build array of key-value pairs for this sentinel
        var fields = try std.ArrayList(RespValue).initCapacity(allocator, 14);
        errdefer {
            for (fields.items) |item| {
                deinitRespValue(allocator, item);
            }
            fields.deinit(allocator);
        }

        // id
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "id") });
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, sentinel.id) });

        // ip
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "ip") });
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, sentinel.ip) });

        // port
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "port") });
        const port_str = try std.fmt.allocPrint(allocator, "{d}", .{sentinel.port});
        try fields.append(allocator, RespValue{ .bulk_string = port_str });

        // last-hello-time
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "last-hello-time") });
        const last_hello_str = try std.fmt.allocPrint(allocator, "{d}", .{sentinel.last_hello_time});
        try fields.append(allocator, RespValue{ .bulk_string = last_hello_str });

        // flags
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "flags") });
        if (sentinel.is_master_down) {
            try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "sentinel,s_down") });
        } else {
            try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "sentinel") });
        }

        // link-pending-commands
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "link-pending-commands") });
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "0") });

        // link-refcount
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "link-refcount") });
        try fields.append(allocator, RespValue{ .bulk_string = try allocator.dupe(u8, "1") });

        const fields_array = try fields.toOwnedSlice(allocator);
        try sentinel_arrays.append(allocator, RespValue{ .array = fields_array });
    }

    var w = Writer.init(allocator);
    defer w.deinit();

    const sentinel_arrays_slice = try sentinel_arrays.toOwnedSlice(allocator);
    defer {
        for (sentinel_arrays_slice) |item| {
            deinitRespValue(allocator, item);
        }
        allocator.free(sentinel_arrays_slice);
    }

    return try w.writeArray(sentinel_arrays_slice);
}

/// Handle SENTINEL IS-MASTER-DOWN-BY-ADDR command
/// Returns [is_down, leader_runid] for a master at specific IP:port
pub fn cmdSentinelIsMasterDownByAddr(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // SENTINEL IS-MASTER-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid>
    if (args.len != 6) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'sentinel|is-master-down-by-addr' command");
    }

    // Check if Sentinel mode is enabled
    if (!storage.sentinel.enabled) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR This instance has sentinel mode disabled");
    }

    const ip = args[2];
    const port_str = args[3];
    // args[4] is current-epoch (not used in this implementation)
    // args[5] is runid (not used in this implementation)

    // Parse port
    const port = std.fmt.parseInt(u16, port_str, 10) catch {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR Invalid port number");
    };

    // Check if master is down
    const result = storage.sentinel.isMasterDownByAddr(ip, port);

    // Build response: [is_down_integer, leader_runid or nil]
    // is_down_integer: 1 if master is known and down, 0 otherwise
    const is_down_int = if (result.is_known and result.is_down) @as(i64, 1) else @as(i64, 0);

    var response_array: [2]RespValue = undefined;
    response_array[0] = RespValue{ .integer = is_down_int };

    if (result.leader_runid) |runid| {
        response_array[1] = RespValue{ .bulk_string = try allocator.dupe(u8, runid) };
    } else {
        response_array[1] = RespValue{ .null_bulk_string = {} };
    }

    defer {
        if (result.leader_runid != null) {
            deinitRespValue(allocator, response_array[1]);
        }
    }

    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeArray(&response_array);
}

/// Handle SENTINEL RESET command
/// Resets master(s) by glob pattern, clears sentinels and state
pub fn cmdSentinelReset(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // SENTINEL RESET <pattern>
    if (args.len != 3) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'sentinel|reset' command");
    }

    // Check if Sentinel mode is enabled
    if (!storage.sentinel.enabled) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR This instance has sentinel mode disabled");
    }

    const pattern = args[2];

    // Reset masters matching pattern
    const count = try storage.sentinel.resetMaster(pattern);

    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeInteger(@intCast(count));
}

/// Handle SENTINEL FAILOVER command
/// Forces a failover for a specific master
pub fn cmdSentinelFailover(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // SENTINEL FAILOVER <master-name>
    if (args.len != 3) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'sentinel|failover' command");
    }

    // Check if Sentinel mode is enabled
    if (!storage.sentinel.enabled) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR This instance has sentinel mode disabled");
    }

    const master_name = args[2];

    // Force failover for this master
    storage.sentinel.forceFailover(master_name) catch |err| {
        var w = Writer.init(allocator);
        defer w.deinit();
        if (err == error.MasterNotFound) {
            return try w.writeError("ERR No such master with that name");
        }
        return try w.writeError("ERR Failed to force failover");
    };

    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeSimpleString("OK");
}

/// Handle SENTINEL CKQUORUM command
/// Check if the current Sentinel configuration can reach quorum for a master
pub fn cmdSentinelCkquorum(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // SENTINEL CKQUORUM <master-name>
    if (args.len != 3) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'sentinel|ckquorum' command");
    }

    // Check if Sentinel mode is enabled
    if (!storage.sentinel.enabled) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR This instance has sentinel mode disabled");
    }

    const master_name = args[2];

    // Check quorum
    const result = storage.sentinel.checkQuorum(master_name) catch |err| {
        var w = Writer.init(allocator);
        defer w.deinit();
        if (err == error.MasterNotFound) {
            return try w.writeError("ERR No such master with that name");
        }
        return try w.writeError("ERR Failed to check quorum");
    };

    var w = Writer.init(allocator);
    defer w.deinit();

    if (result.can_reach) {
        // Success case: can reach quorum
        // Format: "OK <known_sentinels> usable Sentinels. Quorum and failover authorization can be reached"
        const msg = try std.fmt.allocPrint(
            allocator,
            "OK {d} usable Sentinels. Quorum and failover authorization can be reached",
            .{result.known_sentinels + 1}, // Include this Sentinel
        );
        defer allocator.free(msg);
        return try w.writeSimpleString(msg);
    } else {
        // Failure case: cannot reach quorum
        // Format: "NOQUORUM <known_sentinels> usable Sentinels. Not enough available Sentinels to reach the specified quorum for this master"
        const msg = try std.fmt.allocPrint(
            allocator,
            "NOQUORUM {d} usable Sentinels. Not enough available Sentinels to reach the specified quorum for this master",
            .{result.known_sentinels + 1}, // Include this Sentinel
        );
        defer allocator.free(msg);
        return try w.writeSimpleString(msg);
    }
}

/// Handle SENTINEL FLUSHCONFIG command
/// Force Sentinel to rewrite its configuration file
pub fn cmdSentinelFlushconfig(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // SENTINEL FLUSHCONFIG takes no additional arguments
    if (args.len != 2) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'sentinel|flushconfig' command");
    }

    // Check if Sentinel mode is enabled
    if (!storage.sentinel.enabled) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR This instance has sentinel mode disabled");
    }

    // Use sentinel_config_path from storage (default: "sentinel.conf")
    const config_path = storage.sentinel_config_path;

    // Flush config to disk
    storage.sentinel.flushConfig(config_path) catch |err| {
        var w = Writer.init(allocator);
        defer w.deinit();
        if (err == error.InvalidPath) {
            return try w.writeError("ERR Invalid configuration path");
        }
        return try w.writeError("ERR Failed to flush configuration");
    };

    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeSimpleString("OK");
}

/// Handle SENTINEL SET command
/// Set configuration parameters for a master at runtime
pub fn cmdSentinelSet(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // SENTINEL SET <master-name> <option> <value>
    if (args.len != 5) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'sentinel|set' command");
    }

    // Check if Sentinel mode is enabled
    if (!storage.sentinel.enabled) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR This instance has sentinel mode disabled");
    }

    const master_name = args[2];
    const option = args[3];
    const value = args[4];

    // Set master option
    storage.sentinel.setMasterOption(master_name, option, value) catch |err| {
        var w = Writer.init(allocator);
        defer w.deinit();
        if (err == error.MasterNotFound) {
            return try w.writeError("ERR No such master with that name");
        } else if (err == error.InvalidValue) {
            return try w.writeError("ERR Invalid argument");
        } else if (err == error.UnsupportedOption) {
            return try w.writeError("ERR Unsupported option");
        }
        return try w.writeError("ERR Failed to set option");
    };

    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeSimpleString("OK");
}

/// Handle SENTINEL MYID command
/// Returns the Sentinel's unique 40-character ID
pub fn cmdSentinelMyid(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // SENTINEL MYID (no additional arguments)
    if (args.len != 2) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'sentinel|myid' command");
    }

    // Check if Sentinel mode is enabled
    if (!storage.sentinel.enabled) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR This instance has sentinel mode disabled");
    }

    // Return the Sentinel ID as a bulk string
    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeBulkString(&storage.sentinel.myid);
}

/// Handle SENTINEL CONFIG GET command
/// Returns configuration parameter value
pub fn cmdSentinelConfigGet(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // SENTINEL CONFIG GET <param>
    if (args.len != 4) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'sentinel|config|get' command");
    }

    // Check if Sentinel mode is enabled
    if (!storage.sentinel.enabled) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR This instance has sentinel mode disabled");
    }

    const param = args[3];

    // Supported parameters (for now, just return sentinel.conf path)
    // Redis Sentinel CONFIG GET is used for internal parameters
    // We'll implement a minimal subset for compatibility
    if (std.mem.eql(u8, param, "sentinel-config-file")) {
        // Return the sentinel config file path
        var result = [_]RespValue{
            RespValue{ .bulk_string = try allocator.dupe(u8, "sentinel-config-file") },
            RespValue{ .bulk_string = try allocator.dupe(u8, storage.sentinel_config_path) },
        };
        defer {
            for (result) |item| {
                deinitRespValue(allocator, item);
            }
        }

        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeArray(&result);
    }

    // Unknown parameter - return empty array (Redis behavior)
    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeArray(&[_]RespValue{});
}

/// Handle SENTINEL CONFIG SET command
/// Sets configuration parameter value
pub fn cmdSentinelConfigSet(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    // SENTINEL CONFIG SET <param> <value>
    if (args.len != 5) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'sentinel|config|set' command");
    }

    // Check if Sentinel mode is enabled
    if (!storage.sentinel.enabled) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR This instance has sentinel mode disabled");
    }

    const param = args[3];
    const value = args[4];

    // Supported parameters (minimal subset for compatibility)
    if (std.mem.eql(u8, param, "sentinel-config-file")) {
        // This would normally update the config file path
        // For now, we don't support runtime config file path changes
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR Cannot change sentinel-config-file at runtime");
    }

    // Unknown parameter
    _ = value; // suppress unused warning
    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeError("ERR Unsupported CONFIG parameter");
}
