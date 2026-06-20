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

    // 5. Switch to database 0
    client_registry.setSelectedDb(client_id, 0);

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

/// SHUTDOWN command - signals graceful server shutdown with modifiers
/// Syntax: SHUTDOWN [NOSAVE|SAVE] [NOW] [FORCE] [ABORT]
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
    shutdown_state: ?*@import("../server.zig").ShutdownState,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Parse modifiers
    var save: ?bool = null; // null = default (save if configured), true = force save, false = no save
    var now = false;
    var force = false;
    var abort = false;

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const modifier = args[i];
        if (std.ascii.eqlIgnoreCase(modifier, "NOSAVE")) {
            if (save != null) {
                return try w.writeError("ERR SAVE and NOSAVE can't be set at the same time");
            }
            save = false;
        } else if (std.ascii.eqlIgnoreCase(modifier, "SAVE")) {
            if (save != null) {
                return try w.writeError("ERR SAVE and NOSAVE can't be set at the same time");
            }
            save = true;
        } else if (std.ascii.eqlIgnoreCase(modifier, "NOW")) {
            now = true;
        } else if (std.ascii.eqlIgnoreCase(modifier, "FORCE")) {
            force = true;
        } else if (std.ascii.eqlIgnoreCase(modifier, "ABORT")) {
            abort = true;
        } else {
            return try w.writeError("ERR syntax error");
        }
    }

    // ABORT modifier cancels a pending shutdown
    if (abort) {
        if (shutdown_state) |ss| {
            ss.mutex.lock();
            defer ss.mutex.unlock();
            if (ss.requested.load(.acquire)) {
                ss.request = null;
                ss.requested.store(false, .release);
                return try w.writeSimpleString("OK");
            } else {
                return try w.writeError("ERR No shutdown in progress");
            }
        }
        return try w.writeError("ERR shutdown state not available");
    }

    // Default: save if no modifier specified
    const should_save = save orelse true;

    // Request shutdown via shutdown state
    if (shutdown_state) |ss| {
        const ShutdownRequest = @import("../server.zig").ShutdownRequest;
        ss.requestShutdown(ShutdownRequest{
            .save = should_save,
            .now = now,
            .force = force,
        });
        // Return OK - actual shutdown happens in server loop
        return try w.writeSimpleString("OK");
    }

    // Fallback if shutdown state is not available (shouldn't happen in production)
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

/// SELECT command - select database by index
/// Syntax: SELECT db
/// Selects the database at the specified index for the current connection
pub fn cmdSelect(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: *PubSub,
    _: ?*TxState,
    client_registry: *ClientRegistry,
    client_id: u64,
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

    // Validate index range
    if (db_index < 0) {
        return try w.writeError("ERR DB index is out of range");
    }

    // Validate against configured number of databases (default 16)
    // TODO: Once storage refactoring is complete in Iteration 126, use storage.num_databases
    const num_databases = if (storage.config.getAsString("databases") catch null) |val_str| blk: {
        defer storage.config.allocator.free(val_str);
        const db_count = std.fmt.parseInt(i64, val_str, 10) catch 16;
        break :blk @as(u16, @intCast(@max(1, @min(16384, db_count))));
    } else 16;

    if (db_index >= num_databases) {
        return try w.writeError("ERR DB index is out of range");
    }

    // Set the selected database for this client
    client_registry.setSelectedDb(client_id, @as(u16, @intCast(db_index)));

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

            // Redis TYPE string (used in type: field — matches TYPE command output)
            const type_str: []const u8 = switch (value_type) {
                .string, .hyperloglog => "string",
                .list => "list",
                .set => "set",
                .hash => "hash",
                .sorted_set => "zset",
                .stream => "stream",
                else => "string",
            };

            // Actual encoding (simplified — avoids duplicating full OBJECT ENCODING logic)
            const encoding: []const u8 = switch (value_type) {
                .string => storage.peekStringEncoding(key) orelse "embstr",
                .list => "quicklist",
                .set => blk: {
                    const se = storage.getSetEncoding(key) orelse break :blk "hashtable";
                    break :blk switch (se) {
                        .intset => "intset",
                        .hashmap => "hashtable",
                    };
                },
                .hash => blk: {
                    const hl = storage.hlen(key) orelse 0;
                    break :blk if (hl <= 128) "listpack" else "hashtable";
                },
                .sorted_set => blk: {
                    const zc = storage.zcard(key) orelse 0;
                    break :blk if (zc <= 128) "listpack" else "skiplist";
                },
                .stream => "stream",
                .hyperloglog => "raw",
                else => "raw",
            };

            // Idle time in seconds (LRU-based; 0 if not tracked or LFU policy active)
            const idle_secs: u32 = storage.getObjectIdleTime(key) orelse 0;

            // Format: matches real Redis DEBUG OBJECT output (Redis 7.4+ includes type: field)
            try buf_writer.print(
                "Value at:0x0 refcount:1 encoding:{s} serializedlength:0 lru:0 lru_seconds_idle:{d} type:{s}",
                .{ encoding, idle_secs, type_str },
            );
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
        const rdb_path_opt = try config.getAsString("dir");
        if (rdb_path_opt == null) {
            return try w.writeError("ERR config parameter 'dir' not found");
        }
        const rdb_path = rdb_path_opt.?;
        defer allocator.free(rdb_path);

        const dbfilename_opt = try config.getAsString("dbfilename");
        if (dbfilename_opt == null) {
            return try w.writeError("ERR config parameter 'dbfilename' not found");
        }
        const dbfilename = dbfilename_opt.?;
        defer allocator.free(dbfilename);

        var path_buf: [4096]u8 = undefined;
        const full_path = try std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ rdb_path, dbfilename });

        // Save current data
        try Persistence.saveSingleDb(storage, full_path, allocator);

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
        _ = try Persistence.loadSingleDb(storage, full_path, allocator);

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
    } else if (std.ascii.eqlIgnoreCase(subcommand, "QUICKLIST-PACKED-THRESHOLD")) {
        // DEBUG QUICKLIST-PACKED-THRESHOLD <size>
        // Sets the maximum size (bytes) of a packed quicklist node.
        // 0 resets to default (4096). Affects OBJECT ENCODING for lists.
        if (args.len != 3) {
            return try w.writeError("ERR wrong number of arguments for 'debug quicklist-packed-threshold' command");
        }
        const threshold = std.fmt.parseInt(i64, args[2], 10) catch {
            return try w.writeError("ERR value is not an integer or out of range");
        };
        if (threshold < 0) {
            return try w.writeError("ERR value must be non-negative");
        }
        // 0 means reset to default (4096)
        const effective: i64 = if (threshold == 0) 4096 else threshold;
        config.setConfigValue("debug-quicklist-packed-threshold", .{ .int = effective }) catch |err| {
            if (err == error.UnknownParameter) {
                return try w.writeError("ERR internal error: missing config param");
            }
            return err;
        };
        return try w.writeSimpleString("OK");
    } else if (std.ascii.eqlIgnoreCase(subcommand, "JMAP")) {
        // DEBUG JMAP — no-op in non-JVM environments; returns OK for compatibility
        return try w.writeSimpleString("OK");
    } else if (std.ascii.eqlIgnoreCase(subcommand, "DISABLE-NEXT-AOF-FSYNC")) {
        // DEBUG DISABLE-NEXT-AOF-FSYNC — stub, returns OK
        return try w.writeSimpleString("OK");
    } else if (std.ascii.eqlIgnoreCase(subcommand, "SFLAGS")) {
        // DEBUG SFLAGS <value> — stub, returns OK
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
            \\QUICKLIST-PACKED-THRESHOLD <size>
            \\    Set quicklist packed-node size threshold. 0 resets to default.
            \\JMAP
            \\    No-op (Java heap dump — not applicable here).
            \\DISABLE-NEXT-AOF-FSYNC
            \\    Disable the next AOF fsync (stub).
            \\SFLAGS <value>
            \\    Set server flags (stub).
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
    databases: []Storage,
    num_databases: u16,
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

    // Validate indices are within range (use num_databases passed from server)
    if (index1 >= num_databases or index2 >= num_databases) {
        return try w.writeError("ERR DB index is out of range");
    }

    // SWAPDB i i is a no-op
    if (index1 == index2) {
        return try w.writeSimpleString("OK");
    }

    // Atomically swap the two databases by swapping their Storage structs
    const idx1 = @as(usize, @intCast(index1));
    const idx2 = @as(usize, @intCast(index2));

    const temp = databases[idx1];
    databases[idx1] = databases[idx2];
    databases[idx2] = temp;

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

test "cmdShutdown - accepts SAVE and NOSAVE and sets shutdown state" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    const ShutdownState = @import("../server.zig").ShutdownState;
    var shutdown_state = ShutdownState.init();

    // Test plain SHUTDOWN (default: save)
    {
        const args = [_][]const u8{"SHUTDOWN"};
        const result = try cmdShutdown(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2, &shutdown_state);
        defer allocator.free(result);
        try std.testing.expectEqualStrings("+OK\r\n", result);
        try std.testing.expect(shutdown_state.isRequested());
        const req = shutdown_state.getRequest().?;
        try std.testing.expect(req.save == true); // default is save
        try std.testing.expect(req.now == false);
        try std.testing.expect(req.force == false);
        // Reset for next test
        shutdown_state = ShutdownState.init();
    }

    // Test SHUTDOWN SAVE
    {
        const args = [_][]const u8{ "SHUTDOWN", "SAVE" };
        const result = try cmdShutdown(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2, &shutdown_state);
        defer allocator.free(result);
        try std.testing.expectEqualStrings("+OK\r\n", result);
        try std.testing.expect(shutdown_state.isRequested());
        const req = shutdown_state.getRequest().?;
        try std.testing.expect(req.save == true);
        shutdown_state = ShutdownState.init();
    }

    // Test SHUTDOWN NOSAVE
    {
        const args = [_][]const u8{ "SHUTDOWN", "NOSAVE" };
        const result = try cmdShutdown(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2, &shutdown_state);
        defer allocator.free(result);
        try std.testing.expectEqualStrings("+OK\r\n", result);
        try std.testing.expect(shutdown_state.isRequested());
        const req = shutdown_state.getRequest().?;
        try std.testing.expect(req.save == false);
        shutdown_state = ShutdownState.init();
    }

    // Test SHUTDOWN NOW
    {
        const args = [_][]const u8{ "SHUTDOWN", "NOW" };
        const result = try cmdShutdown(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2, &shutdown_state);
        defer allocator.free(result);
        try std.testing.expectEqualStrings("+OK\r\n", result);
        const req = shutdown_state.getRequest().?;
        try std.testing.expect(req.now == true);
        shutdown_state = ShutdownState.init();
    }

    // Test SHUTDOWN FORCE
    {
        const args = [_][]const u8{ "SHUTDOWN", "FORCE" };
        const result = try cmdShutdown(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2, &shutdown_state);
        defer allocator.free(result);
        try std.testing.expectEqualStrings("+OK\r\n", result);
        const req = shutdown_state.getRequest().?;
        try std.testing.expect(req.force == true);
        shutdown_state = ShutdownState.init();
    }

    // Test SHUTDOWN NOSAVE NOW FORCE (combined modifiers)
    {
        const args = [_][]const u8{ "SHUTDOWN", "NOSAVE", "NOW", "FORCE" };
        const result = try cmdShutdown(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2, &shutdown_state);
        defer allocator.free(result);
        try std.testing.expectEqualStrings("+OK\r\n", result);
        const req = shutdown_state.getRequest().?;
        try std.testing.expect(req.save == false);
        try std.testing.expect(req.now == true);
        try std.testing.expect(req.force == true);
        shutdown_state = ShutdownState.init();
    }

    // Test SHUTDOWN ABORT (cancels shutdown)
    {
        // First request shutdown
        {
            const args = [_][]const u8{"SHUTDOWN"};
            const result = try cmdShutdown(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2, &shutdown_state);
            defer allocator.free(result);
            try std.testing.expect(shutdown_state.isRequested());
        }
        // Then abort it
        {
            const args = [_][]const u8{ "SHUTDOWN", "ABORT" };
            const result = try cmdShutdown(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2, &shutdown_state);
            defer allocator.free(result);
            try std.testing.expectEqualStrings("+OK\r\n", result);
            try std.testing.expect(!shutdown_state.isRequested());
        }
    }

    // Test SHUTDOWN ABORT when no shutdown in progress (should error)
    {
        const args = [_][]const u8{ "SHUTDOWN", "ABORT" };
        const result = try cmdShutdown(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2, &shutdown_state);
        defer allocator.free(result);
        try std.testing.expect(std.mem.startsWith(u8, result, "-ERR No shutdown in progress"));
    }
}

test "cmdShutdown - rejects SAVE and NOSAVE together" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    const ShutdownState = @import("../server.zig").ShutdownState;
    var shutdown_state = ShutdownState.init();

    const args = [_][]const u8{ "SHUTDOWN", "SAVE", "NOSAVE" };
    const result = try cmdShutdown(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2, &shutdown_state);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR SAVE and NOSAVE can't be set at the same time"));
}

test "cmdShutdown - rejects invalid modifier" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    const ShutdownState = @import("../server.zig").ShutdownState;
    var shutdown_state = ShutdownState.init();

    const args = [_][]const u8{ "SHUTDOWN", "INVALID" };
    const result = try cmdShutdown(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2, &shutdown_state);
    defer allocator.free(result);

    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR syntax error"));
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
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const config = storage.config;

    // Set a key
    try storage.set("testkey", "testvalue", null);

    const args = [_][]const u8{ "DEBUG", "OBJECT", "testkey" };
    const result = try cmdDebug(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
    defer allocator.free(result);

    // Should contain proper format fields
    try std.testing.expect(std.mem.indexOf(u8, result, "Value at:0x0") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "type:string") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "encoding:") != null);
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

test "cmdDebug - DEBUG OBJECT shows key info with correct format" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const config = storage.config;

    // Set a short string key — should report embstr encoding
    try storage.set("testkey", "hello", null);

    const args = [_][]const u8{ "DEBUG", "OBJECT", "testkey" };
    const result = try cmdDebug(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
    defer allocator.free(result);

    // Verify correct Redis-compatible format (type: field, hex address, no type name in addr field)
    try std.testing.expect(std.mem.indexOf(u8, result, "Value at:0x0") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "type:string") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "encoding:embstr") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Value at:string") == null);
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

test "cmdDebug - QUICKLIST-PACKED-THRESHOLD sets threshold" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const config = storage.config;

    const args = [_][]const u8{ "DEBUG", "QUICKLIST-PACKED-THRESHOLD", "1" };
    const result = try cmdDebug(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);

    // Verify config was updated
    var cv = try config.get("debug-quicklist-packed-threshold");
    defer cv.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 1), cv.int);
}

test "cmdDebug - QUICKLIST-PACKED-THRESHOLD 0 resets to default" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const config = storage.config;

    // First set to 1
    {
        const args = [_][]const u8{ "DEBUG", "QUICKLIST-PACKED-THRESHOLD", "1" };
        const result = try cmdDebug(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
        defer allocator.free(result);
    }

    // Then reset to 0 (should restore to 4096)
    const args = [_][]const u8{ "DEBUG", "QUICKLIST-PACKED-THRESHOLD", "0" };
    const result = try cmdDebug(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    var cv = try config.get("debug-quicklist-packed-threshold");
    defer cv.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 4096), cv.int);
}

test "cmdDebug - JMAP returns OK" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const config = storage.config;

    const args = [_][]const u8{ "DEBUG", "JMAP" };
    const result = try cmdDebug(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "cmdDebug - DISABLE-NEXT-AOF-FSYNC returns OK" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const config = storage.config;

    const args = [_][]const u8{ "DEBUG", "DISABLE-NEXT-AOF-FSYNC" };
    const result = try cmdDebug(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

/// LOLWUT [VERSION version] - Display Redis version and computer art
pub fn cmdLolwut(
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

    // Parse VERSION parameter if provided
    var version: u32 = 0; // 0 = use Zoltraak version (0.1.0 -> version 1)

    if (args.len >= 3) {
        const version_key = args[1];

        if (!std.ascii.eqlIgnoreCase(version_key, "VERSION")) {
            return try w.writeError("ERR syntax error");
        }

        const version_str = args[2];

        version = std.fmt.parseInt(u32, version_str, 10) catch {
            return try w.writeError("ERR invalid version number");
        };
    }

    // Generate art based on version
    const art = try generateLolwutArt(allocator, version);
    defer allocator.free(art);

    return try w.writeBulkString(art);
}

fn generateLolwutArt(allocator: std.mem.Allocator, version: u32) ![]const u8 {
    // Use version 0 or 1 for Zoltraak v0.1.0 -> display our own art
    const effective_version = if (version == 0) 1 else version;

    return switch (effective_version) {
        1 => try generateZoltraakArt(allocator),
        5 => try generateRedis5Art(allocator),
        6 => try generateRedis6Art(allocator),
        7 => try generateRedis7Art(allocator),
        else => try generateGenericArt(allocator, effective_version),
    };
}

fn generateZoltraakArt(allocator: std.mem.Allocator) ![]const u8 {
    // Zoltraak's custom art - lightning bolt theme
    const art =
        \\
        \\    ___________    ________    __
        \\   |_  ___  __ \  |_   __ \  |  |
        \\     | |_  | ||  |   | |__) | |  |
        \\     |  _| | ||_ |   |  __ /  |  |
        \\    _| |_ |  ___|  _| |  \ \_ |  |___
        \\   |_____||_|     |____||____||______|
        \\
        \\         ⚡ Zoltraak v0.1.0 ⚡
        \\      Redis-compatible data store
        \\           Written in Zig
        \\
    ;
    return allocator.dupe(u8, art);
}

fn generateRedis5Art(allocator: std.mem.Allocator) ![]const u8 {
    // Simple geometric pattern for Redis 5.0
    const art =
        \\
        \\    ╔════════════════════╗
        \\    ║  ▓▒░ Redis 5 ░▒▓  ║
        \\    ║  ░▒▓ Classic ▓▒░  ║
        \\    ╚════════════════════╝
        \\
        \\      Redis 5.0.0+
        \\
    ;
    return allocator.dupe(u8, art);
}

fn generateRedis6Art(allocator: std.mem.Allocator) ![]const u8 {
    // ASCII pattern for Redis 6.0
    const art =
        \\
        \\    ┌──────────────────┐
        \\    │ ▄▄ ▄▄▄ ▄▄ ▄▄▄ ▄▄ │
        \\    │ ██ ███ ██ ███ ██ │
        \\    │ ▀▀ ▀▀▀ ▀▀ ▀▀▀ ▀▀ │
        \\    └──────────────────┘
        \\
        \\      Redis 6.0.0+
        \\
    ;
    return allocator.dupe(u8, art);
}

fn generateRedis7Art(allocator: std.mem.Allocator) ![]const u8 {
    // Wave pattern for Redis 7.0
    const art =
        \\
        \\    ╭──────────────────╮
        \\    │ ≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈≈ │
        \\    │ ～～～～～～～～ │
        \\    │ ≋≋≋≋≋≋≋≋≋≋≋≋≋≋≋≋ │
        \\    ╰──────────────────╯
        \\
        \\      Redis 7.0.0+
        \\
    ;
    return allocator.dupe(u8, art);
}

fn generateGenericArt(allocator: std.mem.Allocator, version: u32) ![]const u8 {
    // Generic art for unknown versions
    var buf = try std.ArrayList(u8).initCapacity(allocator, 128);
    defer buf.deinit(allocator);

    const writer = buf.writer(allocator);

    try writer.writeAll("\n");
    try writer.writeAll("    ┌──────────────────┐\n");
    try writer.print("    │  Redis {d}.0.0+    │\n", .{version});
    try writer.writeAll("    │  (Unknown ver)   │\n");
    try writer.writeAll("    └──────────────────┘\n");
    try writer.writeAll("\n");

    return try buf.toOwnedSlice(allocator);
}

// ── LOLWUT Unit Tests ────────────────────────────────────────────────────────

test "cmdLolwut - default version shows Zoltraak art" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const config = storage.config;

    const args = [_][]const u8{"LOLWUT"};
    const result = try cmdLolwut(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
    defer allocator.free(result);

    // Should contain Zoltraak version art
    try std.testing.expect(std.mem.indexOf(u8, result, "Zoltraak") != null);
}

test "cmdLolwut - version 5 shows Redis 5 art" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const config = storage.config;

    const args = [_][]const u8{ "LOLWUT", "VERSION", "5" };
    const result = try cmdLolwut(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
    defer allocator.free(result);

    // Should contain Redis 5 art
    try std.testing.expect(std.mem.indexOf(u8, result, "Redis 5") != null);
}

test "cmdLolwut - version 7 shows Redis 7 art" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const config = storage.config;

    const args = [_][]const u8{ "LOLWUT", "VERSION", "7" };
    const result = try cmdLolwut(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
    defer allocator.free(result);

    // Should contain Redis 7 reference
    try std.testing.expect(std.mem.indexOf(u8, result, "Redis 7") != null);
}

test "cmdLolwut - invalid version returns error" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const config = storage.config;

    const args = [_][]const u8{ "LOLWUT", "VERSION", "notanumber" };
    const result = try cmdLolwut(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

test "cmdLolwut - unknown version returns generic art" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    const config = storage.config;

    const args = [_][]const u8{ "LOLWUT", "VERSION", "99" };
    const result = try cmdLolwut(allocator, &args, storage, &pubsub, null, &client_registry, 1, config, 2);
    defer allocator.free(result);

    // Should contain version 99 in generic art
    try std.testing.expect(std.mem.indexOf(u8, result, "99") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "Unknown ver") != null);
}
