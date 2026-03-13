const std = @import("std");
const writer_mod = @import("../protocol/writer.zig");
const Writer = writer_mod.Writer;
const Storage = @import("../storage/memory.zig").Storage;
const PubSub = @import("../storage/pubsub.zig").PubSub;
const TxState = @import("transactions.zig").TxState;
const ClientRegistry = @import("client.zig").ClientRegistry;
const ServerConfig = @import("../storage/config.zig").Config;

/// ECHO command - returns the message
pub fn cmdEcho(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    _: *Storage,
    _: *PubSub,
    _: ?*TxState,
    _: *ClientRegistry,
    _: u64,
    _: *ServerConfig,
    _: u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return try w.writeError("ERR wrong number of arguments for 'echo' command");
    }
    return try w.writeBulkString(args[1]);
}

/// QUIT command - signals to close the connection
pub fn cmdQuit(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    _: *Storage,
    _: *PubSub,
    _: ?*TxState,
    _: *ClientRegistry,
    _: u64,
    _: *ServerConfig,
    _: u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 1) {
        return try w.writeError("ERR wrong number of arguments for 'quit' command");
    }
    // Return OK - the server will handle closing the connection
    return try w.writeSimpleString("OK");
}

/// RESET command - reset the connection state
///
/// Resets the connection to its initial state:
/// - Discards any pending MULTI transaction
/// - Unsubscribes from all pub/sub channels
/// - Removes the connection name
/// - Switches back to RESP2 protocol (default)
/// - Returns to database 0 (if multi-DB support exists)
///
/// Returns "RESET" on success
pub fn cmdReset(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    _: *Storage,
    ps: *PubSub,
    tx_state: *TxState,
    client_registry: *ClientRegistry,
    client_id: u64,
    _: *ServerConfig,
    _: u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 1) {
        return try w.writeError("ERR wrong number of arguments for 'reset' command");
    }

    // 1. Discard MULTI transaction if active
    tx_state.reset();

    // 2. Unsubscribe from all channels (regular + pattern + sharded)
    // Note: PubSub.unsubscribeAll will handle all subscription types
    try ps.unsubscribeAll(client_id);

    // 3. Remove connection name
    try client_registry.setClientName(client_id, "");

    // 4. Reset protocol to RESP2
    client_registry.setProtocol(client_id, .RESP2);

    // 5. Switch to database 0 (no-op in current single-DB implementation)

    // Return "RESET" as simple string
    return try w.writeSimpleString("RESET");
}

/// TIME command - returns current Unix timestamp and microseconds
pub fn cmdTime(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    _: *Storage,
    _: *PubSub,
    _: ?*TxState,
    _: *ClientRegistry,
    _: u64,
    _: *ServerConfig,
    protocol_version: u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 1) {
        return try w.writeError("ERR wrong number of arguments for 'time' command");
    }

    const now = std.time.milliTimestamp();
    const seconds = @divFloor(now, 1000);
    const microseconds = @mod(now, 1000) * 1000;

    var buf = std.ArrayList(u8){};
    defer buf.deinit(allocator);
    const buf_writer = buf.writer(allocator);

    // Format seconds as string
    var seconds_buf: [32]u8 = undefined;
    const seconds_str = try std.fmt.bufPrint(&seconds_buf, "{d}", .{seconds});

    // Format microseconds as string
    var micros_buf: [32]u8 = undefined;
    const micros_str = try std.fmt.bufPrint(&micros_buf, "{d}", .{microseconds});

    // Return as array of two elements
    try buf_writer.print("*2\r\n", .{});
    try buf_writer.print("${d}\r\n{s}\r\n", .{ seconds_str.len, seconds_str });
    try buf_writer.print("${d}\r\n{s}\r\n", .{ micros_str.len, micros_str });

    _ = protocol_version;
    return try buf.toOwnedSlice(allocator);
}

/// LASTSAVE command - returns Unix timestamp of last successful RDB save
pub fn cmdLastsave(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: *PubSub,
    _: ?*TxState,
    _: *ClientRegistry,
    _: u64,
    _: *ServerConfig,
    _: u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 1) {
        return try w.writeError("ERR wrong number of arguments for 'lastsave' command");
    }

    const last_save_time = storage.getLastSaveTime();
    return try w.writeInteger(@intCast(last_save_time));
}

/// SHUTDOWN command - signals graceful server shutdown
pub fn cmdShutdown(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    _: *Storage,
    _: *PubSub,
    _: ?*TxState,
    _: *ClientRegistry,
    _: u64,
    _: *ServerConfig,
    _: u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Optional: NOSAVE or SAVE modifier
    if (args.len > 2) {
        return try w.writeError("ERR wrong number of arguments for 'shutdown' command");
    }

    // Parse optional SAVE/NOSAVE modifier (validated but not used in stub)
    if (args.len == 2) {
        const modifier = args[1];
        if (!std.ascii.eqlIgnoreCase(modifier, "NOSAVE") and
            !std.ascii.eqlIgnoreCase(modifier, "SAVE"))
        {
            return try w.writeError("ERR syntax error");
        }
    }

    // Note: The actual shutdown logic will be handled by the server
    // This command just acknowledges the request

    // Return OK to acknowledge - server will handle shutdown
    return try w.writeSimpleString("OK");
}

/// MONITOR command - enables real-time command monitoring
pub fn cmdMonitor(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    _: *Storage,
    _: *PubSub,
    _: ?*TxState,
    client_registry: *ClientRegistry,
    client_id: u64,
    _: *ServerConfig,
    _: u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 1) {
        return try w.writeError("ERR wrong number of arguments for 'monitor' command");
    }

    // Enable monitor mode for this client
    client_registry.setMonitorMode(client_id, true);

    // Return OK to acknowledge - server will stream commands to this client
    return try w.writeSimpleString("OK");
}

/// SELECT command - select database by index (stub for single-DB mode)
pub fn cmdSelect(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    _: *Storage,
    _: *PubSub,
    _: ?*TxState,
    _: *ClientRegistry,
    _: u64,
    _: *ServerConfig,
    _: u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return try w.writeError("ERR wrong number of arguments for 'select' command");
    }

    // Parse database index
    const db_index = std.fmt.parseInt(i64, args[1], 10) catch {
        return try w.writeError("ERR invalid DB index");
    };

    // Zoltraak only supports database 0 (single database mode)
    if (db_index < 0) {
        return try w.writeError("ERR DB index is out of range");
    }

    if (db_index != 0) {
        return try w.writeError("ERR DB index is out of range");
    }

    return try w.writeSimpleString("OK");
}

/// DEBUG command - debugging utilities
pub fn cmdDebug(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: *PubSub,
    _: ?*TxState,
    _: *ClientRegistry,
    _: u64,
    config: *ServerConfig,
    _: u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return try w.writeError("ERR wrong number of arguments for 'debug' command");
    }

    const subcommand = args[1];

    if (std.ascii.eqlIgnoreCase(subcommand, "OBJECT")) {
        // DEBUG OBJECT key - show object info
        if (args.len != 3) {
            return try w.writeError("ERR wrong number of arguments for 'debug object' command");
        }

        const key = args[2];
        if (storage.getType(key)) |value_type| {
            var buf = std.ArrayList(u8){};
            defer buf.deinit(allocator);
            const buf_writer = buf.writer(allocator);

            const type_str = switch (value_type) {
                .string => "string",
                .list => "list",
                .set => "set",
                .hash => "hash",
                .sorted_set => "zset",
                .stream => "stream",
                .hyperloglog => "hyperloglog",
            };

            try buf_writer.print("Value at:{s} refcount:1 encoding:raw serializedlength:0 lru:0 lru_seconds_idle:0", .{type_str});
            const result = try buf.toOwnedSlice(allocator);
            defer allocator.free(result);
            return try w.writeBulkString(result);
        } else {
            return try w.writeError("ERR no such key");
        }
    } else if (std.ascii.eqlIgnoreCase(subcommand, "SET-ACTIVE-EXPIRE")) {
        // DEBUG SET-ACTIVE-EXPIRE 0|1 - toggle active expiration
        if (args.len != 3) {
            return try w.writeError("ERR wrong number of arguments for 'debug set-active-expire' command");
        }

        const value = std.fmt.parseInt(i64, args[2], 10) catch {
            return try w.writeError("ERR value must be 0 or 1");
        };

        if (value != 0 and value != 1) {
            return try w.writeError("ERR value must be 0 or 1");
        }

        storage.mutex.lock();
        storage.active_expire_enabled = (value == 1);
        storage.mutex.unlock();

        return try w.writeInteger(value);
    } else if (std.ascii.eqlIgnoreCase(subcommand, "SLEEP")) {
        // DEBUG SLEEP <seconds> - sleep for N seconds
        if (args.len != 3) {
            return try w.writeError("ERR wrong number of arguments for 'debug sleep' command");
        }

        const seconds = std.fmt.parseFloat(f64, args[2]) catch {
            return try w.writeError("ERR value is not a float or out of range");
        };

        if (seconds < 0) {
            return try w.writeError("ERR sleep time must be non-negative");
        }

        const nanoseconds = @as(u64, @intFromFloat(seconds * 1_000_000_000));
        std.Thread.sleep(nanoseconds);

        return try w.writeSimpleString("OK");
    } else if (std.ascii.eqlIgnoreCase(subcommand, "RELOAD")) {
        // DEBUG RELOAD - save RDB + reload from disk
        if (args.len != 2) {
            return try w.writeError("ERR wrong number of arguments for 'debug reload' command");
        }

        // Save current state to RDB
        const Persistence = @import("../storage/persistence.zig").Persistence;
        const rdb_path_opt = try config.get("dir");
        if (rdb_path_opt == null) {
            return try w.writeError("ERR config parameter 'dir' not found");
        }
        const rdb_path = rdb_path_opt.?;
        defer allocator.free(rdb_path);

        const dbfilename_opt = try config.get("dbfilename");
        if (dbfilename_opt == null) {
            return try w.writeError("ERR config parameter 'dbfilename' not found");
        }
        const dbfilename = dbfilename_opt.?;
        defer allocator.free(dbfilename);

        var path_buf: [4096]u8 = undefined;
        const full_path = try std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ rdb_path, dbfilename });

        // Save current data
        try Persistence.save(storage, full_path, allocator);

        // Clear current data
        storage.mutex.lock();
        var it = storage.data.iterator();
        while (it.next()) |entry| {
            const val = entry.value_ptr;
            val.deinit(storage.allocator);
        }
        storage.data.clearRetainingCapacity();
        storage.mutex.unlock();

        // Reload from disk
        _ = try Persistence.load(storage, full_path, allocator);

        return try w.writeSimpleString("OK");
    } else if (std.ascii.eqlIgnoreCase(subcommand, "CHANGE-REPL-ID")) {
        // DEBUG CHANGE-REPL-ID - force new replication ID
        // Note: This is a stub since ReplicationState is owned by Server,
        // not accessible from command handlers. In a real implementation,
        // this would require passing server context through the command chain.
        if (args.len != 2) {
            return try w.writeError("ERR wrong number of arguments for 'debug change-repl-id' command");
        }

        // Stub implementation - returns OK but doesn't actually change replid
        // TODO: Implement when server context is available to commands
        return try w.writeSimpleString("OK");
    } else if (std.ascii.eqlIgnoreCase(subcommand, "POPULATE")) {
        // DEBUG POPULATE <count> [<prefix>] [<size>] - fill DB with test keys
        if (args.len < 3 or args.len > 5) {
            return try w.writeError("ERR wrong number of arguments for 'debug populate' command");
        }

        const count = std.fmt.parseInt(u64, args[2], 10) catch {
            return try w.writeError("ERR value is not an integer or out of range");
        };

        const prefix = if (args.len >= 4) args[3] else "key:";
        const size = if (args.len >= 5) blk: {
            break :blk std.fmt.parseInt(u64, args[4], 10) catch {
                return try w.writeError("ERR value is not an integer or out of range");
            };
        } else 64;

        // Create test value of specified size
        const value_data = try allocator.alloc(u8, size);
        defer allocator.free(value_data);
        @memset(value_data, 'x');

        // Generate keys
        var i: u64 = 0;
        while (i < count) : (i += 1) {
            var key_buf: [256]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "{s}{d}", .{ prefix, i });

            const key_copy = try allocator.dupe(u8, key);
            errdefer allocator.free(key_copy);

            const val_copy = try allocator.dupe(u8, value_data);
            errdefer allocator.free(val_copy);

            try storage.set(key_copy, val_copy, null);
        }

        return try w.writeSimpleString("OK");
    } else if (std.ascii.eqlIgnoreCase(subcommand, "HELP")) {
        const help_text =
            \\DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:
            \\OBJECT <key>
            \\    Show low-level info about key and associated value.
            \\SET-ACTIVE-EXPIRE <0|1>
            \\    Toggle active key expiration on/off.
            \\SLEEP <seconds>
            \\    Sleep for the specified number of seconds.
            \\RELOAD
            \\    Save RDB snapshot and reload from disk.
            \\CHANGE-REPL-ID
            \\    Generate a new replication ID.
            \\POPULATE <count> [<prefix>] [<size>]
            \\    Create <count> test keys with optional prefix and value size.
            \\HELP
            \\    Print this help.
        ;
        return try w.writeBulkString(help_text);
    } else {
        return try w.writeError("ERR unknown subcommand or wrong number of arguments");
    }
}

/// SWAPDB command - swap two databases (stub for single-DB mode)
pub fn cmdSwapdb(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    _: *Storage,
    _: *PubSub,
    _: ?*TxState,
    _: *ClientRegistry,
    _: u64,
    _: *ServerConfig,
    _: u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return try w.writeError("ERR wrong number of arguments for 'swapdb' command");
    }

    // Parse database indices
    const index1 = std.fmt.parseInt(i64, args[1], 10) catch {
        return try w.writeError("ERR invalid first DB index");
    };

    const index2 = std.fmt.parseInt(i64, args[2], 10) catch {
        return try w.writeError("ERR invalid second DB index");
    };

    // Validate indices are non-negative
    if (index1 < 0 or index2 < 0) {
        return try w.writeError("ERR DB index is out of range");
    }

    // Zoltraak only supports database 0 (single database mode)
    // SWAPDB 0 0 is allowed (no-op), but any other index is rejected
    if (index1 != 0 or index2 != 0) {
        return try w.writeError("ERR DB index is out of range");
    }

    // SWAPDB 0 0 is a no-op but returns OK
    return try w.writeSimpleString("OK");
}

// Unit tests
test "cmdEcho - basic echo" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    const args = [_][]const u8{ "ECHO", "Hello World" };
    const result = try cmdEcho(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$11\r\nHello World\r\n", result);
}

test "cmdEcho - wrong number of arguments" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    const args = [_][]const u8{"ECHO"};
    const result = try cmdEcho(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR wrong number of arguments"));
}

test "cmdQuit - returns OK" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    const args = [_][]const u8{"QUIT"};
    const result = try cmdQuit(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "cmdTime - returns array of two elements" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    const args = [_][]const u8{"TIME"};
    const result = try cmdTime(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
    defer allocator.free(result);

    // Should start with *2\r\n (array of 2 elements)
    try std.testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
}

test "cmdLastsave - returns integer timestamp" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    const args = [_][]const u8{"LASTSAVE"};
    const result = try cmdLastsave(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
    defer allocator.free(result);

    // Should be integer response
    try std.testing.expect(std.mem.startsWith(u8, result, ":"));
}

test "cmdShutdown - accepts SAVE and NOSAVE" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    // Test plain SHUTDOWN
    {
        const args = [_][]const u8{"SHUTDOWN"};
        const result = try cmdShutdown(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
        defer allocator.free(result);
        try std.testing.expectEqualStrings("+OK\r\n", result);
    }

    // Test SHUTDOWN SAVE
    {
        const args = [_][]const u8{ "SHUTDOWN", "SAVE" };
        const result = try cmdShutdown(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
        defer allocator.free(result);
        try std.testing.expectEqualStrings("+OK\r\n", result);
    }

    // Test SHUTDOWN NOSAVE
    {
        const args = [_][]const u8{ "SHUTDOWN", "NOSAVE" };
        const result = try cmdShutdown(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
        defer allocator.free(result);
        try std.testing.expectEqualStrings("+OK\r\n", result);
    }
}

test "cmdMonitor - enables monitor mode and returns OK" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    // Register a client
    const client_id = try client_registry.registerClient("127.0.0.1:12345", 10);

    // Monitor mode should be off initially
    try std.testing.expect(!client_registry.isMonitoring(client_id));

    // Execute MONITOR command
    const args = [_][]const u8{"MONITOR"};
    const result = try cmdMonitor(allocator, &args, &storage, &pubsub, null, &client_registry, client_id, &config, 2);
    defer allocator.free(result);

    // Should return OK
    try std.testing.expectEqualStrings("+OK\r\n", result);

    // Monitor mode should now be enabled
    try std.testing.expect(client_registry.isMonitoring(client_id));
}

test "cmdMonitor - wrong number of arguments" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    const args = [_][]const u8{ "MONITOR", "extra" };
    const result = try cmdMonitor(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

test "cmdDebug - OBJECT subcommand" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    // Set a key
    _ = try storage.set("testkey", "testvalue", null);

    const args = [_][]const u8{ "DEBUG", "OBJECT", "testkey" };
    const result = try cmdDebug(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
    defer allocator.free(result);

    // Should contain type info
    try std.testing.expect(std.mem.indexOf(u8, result, "string") != null);
}

test "cmdDebug - HELP subcommand" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    const args = [_][]const u8{ "DEBUG", "HELP" };
    const result = try cmdDebug(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
    defer allocator.free(result);

    // Should contain help text
    try std.testing.expect(std.mem.indexOf(u8, result, "OBJECT") != null);
}

test "cmdSelect - database 0 is accepted" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    const args = [_][]const u8{ "SELECT", "0" };
    const result = try cmdSelect(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "cmdSelect - other databases are rejected" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    const args = [_][]const u8{ "SELECT", "1" };
    const result = try cmdSelect(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR DB index is out of range"));
}

test "cmdSelect - negative index rejected" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    const args = [_][]const u8{ "SELECT", "-1" };
    const result = try cmdSelect(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR DB index is out of range"));
}

test "cmdSelect - invalid argument" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    const args = [_][]const u8{ "SELECT", "abc" };
    const result = try cmdSelect(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR invalid DB index"));
}

test "cmdSelect - wrong number of arguments" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    const args = [_][]const u8{"SELECT"};
    const result = try cmdSelect(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR wrong number of arguments"));
}

test "cmdSwapdb - SWAPDB 0 0 returns OK" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    const args = [_][]const u8{ "SWAPDB", "0", "0" };
    const result = try cmdSwapdb(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "cmdSwapdb - non-zero database rejected" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    // Test SWAPDB 0 1
    {
        const args = [_][]const u8{ "SWAPDB", "0", "1" };
        const result = try cmdSwapdb(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
        defer allocator.free(result);
        try std.testing.expect(std.mem.startsWith(u8, result, "-ERR DB index is out of range"));
    }

    // Test SWAPDB 1 0
    {
        const args = [_][]const u8{ "SWAPDB", "1", "0" };
        const result = try cmdSwapdb(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
        defer allocator.free(result);
        try std.testing.expect(std.mem.startsWith(u8, result, "-ERR DB index is out of range"));
    }

    // Test SWAPDB 1 1
    {
        const args = [_][]const u8{ "SWAPDB", "1", "1" };
        const result = try cmdSwapdb(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
        defer allocator.free(result);
        try std.testing.expect(std.mem.startsWith(u8, result, "-ERR DB index is out of range"));
    }
}

test "cmdSwapdb - negative index rejected" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    const args = [_][]const u8{ "SWAPDB", "-1", "0" };
    const result = try cmdSwapdb(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR DB index is out of range"));
}

test "cmdSwapdb - invalid argument" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    // Test invalid first index
    {
        const args = [_][]const u8{ "SWAPDB", "abc", "0" };
        const result = try cmdSwapdb(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
        defer allocator.free(result);
        try std.testing.expect(std.mem.startsWith(u8, result, "-ERR invalid first DB index"));
    }

    // Test invalid second index
    {
        const args = [_][]const u8{ "SWAPDB", "0", "xyz" };
        const result = try cmdSwapdb(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
        defer allocator.free(result);
        try std.testing.expect(std.mem.startsWith(u8, result, "-ERR invalid second DB index"));
    }
}

test "cmdSwapdb - wrong number of arguments" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    // Test too few arguments
    {
        const args = [_][]const u8{ "SWAPDB", "0" };
        const result = try cmdSwapdb(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
        defer allocator.free(result);
        try std.testing.expect(std.mem.startsWith(u8, result, "-ERR wrong number of arguments"));
    }

    // Test too many arguments
    {
        const args = [_][]const u8{ "SWAPDB", "0", "0", "extra" };
        const result = try cmdSwapdb(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
        defer allocator.free(result);
        try std.testing.expect(std.mem.startsWith(u8, result, "-ERR wrong number of arguments"));
    }
}

test "cmdReset - basic reset" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();
    var tx_state = TxState.init();

    const client_id = try client_registry.registerClient("127.0.0.1:12345", 42);

    // Set some state
    try client_registry.setClientName(client_id, "test-client");
    client_registry.setProtocol(client_id, .RESP3);

    // Subscribe to a channel
    _ = try pubsub.subscribe(client_id, "test-channel");

    // Reset
    const args = [_][]const u8{"RESET"};
    const result = try cmdReset(allocator, &args, &storage, &pubsub, &tx_state, &client_registry, client_id, &config, 3);
    defer allocator.free(result);

    // Should return "RESET"
    try std.testing.expectEqualStrings("+RESET\r\n", result);

    // Protocol should be reset to RESP2
    try std.testing.expectEqual(@import("client.zig").RespProtocol.RESP2, client_registry.getProtocol(client_id));

    // Name should be reset to empty
    const name = try client_registry.getClientName(client_id, allocator);
    defer if (name) |n| allocator.free(n);
    if (name) |n| {
        try std.testing.expectEqualStrings("", n);
    }
}

test "cmdReset - wrong number of arguments" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();
    var tx_state = TxState.init();

    const args = [_][]const u8{ "RESET", "extra" };
    const result = try cmdReset(allocator, &args, &storage, &pubsub, &tx_state, &client_registry, 1, &config, 2);
    defer allocator.free(result);
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR wrong number of arguments"));
}

// ── DEBUG command tests ───────────────────────────────────────────────────────

test "cmdDebug - DEBUG OBJECT shows key info" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const config = storage.config;

    // Set a string key
    const key = try allocator.dupe(u8, "testkey");
    const value = try allocator.dupe(u8, "testvalue");
    try storage.set(key, value, null);

    const args = [_][]const u8{ "DEBUG", "OBJECT", "testkey" };
    const result = try cmdDebug(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "Value at:string") != null);
}

test "cmdDebug - DEBUG SET-ACTIVE-EXPIRE toggles flag" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const config = storage.config;

    // Initially enabled
    try std.testing.expect(storage.active_expire_enabled == true);

    // Disable
    {
        const args = [_][]const u8{ "DEBUG", "SET-ACTIVE-EXPIRE", "0" };
        const result = try cmdDebug(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
        defer allocator.free(result);
        try std.testing.expectEqualStrings(":0\r\n", result);
        try std.testing.expect(storage.active_expire_enabled == false);
    }

    // Enable
    {
        const args = [_][]const u8{ "DEBUG", "SET-ACTIVE-EXPIRE", "1" };
        const result = try cmdDebug(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
        defer allocator.free(result);
        try std.testing.expectEqualStrings(":1\r\n", result);
        try std.testing.expect(storage.active_expire_enabled == true);
    }
}

test "cmdDebug - DEBUG SLEEP sleeps for specified time" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const config = storage.config;

    const start = std.time.milliTimestamp();
    const args = [_][]const u8{ "DEBUG", "SLEEP", "0.1" }; // 100ms
    const result = try cmdDebug(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
    defer allocator.free(result);
    const elapsed = std.time.milliTimestamp() - start;

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expect(elapsed >= 100); // At least 100ms
}

test "cmdDebug - DEBUG POPULATE creates keys" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const config = storage.config;

    const args = [_][]const u8{ "DEBUG", "POPULATE", "10", "test:", "32" };
    const result = try cmdDebug(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqual(@as(usize, 10), storage.dbSize());

    // Check that keys exist
    const val = storage.get("test:0");
    try std.testing.expect(val != null);
    try std.testing.expectEqual(@as(usize, 32), val.?.len);
}

test "cmdDebug - DEBUG HELP returns help text" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const config = storage.config;

    const args = [_][]const u8{ "DEBUG", "HELP" };
    const result = try cmdDebug(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "SET-ACTIVE-EXPIRE") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "SLEEP") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "RELOAD") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "POPULATE") != null);
}

test "cmdDebug - DEBUG wrong subcommand returns error" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const config = storage.config;

    const args = [_][]const u8{ "DEBUG", "INVALID" };
    const result = try cmdDebug(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR unknown subcommand"));
}

test "cmdDebug - DEBUG CHANGE-REPL-ID returns OK (stub)" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const config = storage.config;

    const args = [_][]const u8{ "DEBUG", "CHANGE-REPL-ID" };
    const result = try cmdDebug(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}
