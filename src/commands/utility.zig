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

/// MONITOR command - enables real-time command monitoring (stub)
pub fn cmdMonitor(
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
        return try w.writeError("ERR wrong number of arguments for 'monitor' command");
    }

    // Stub implementation - in a real implementation, this would enable monitoring mode
    // and stream all commands to this client
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

/// DEBUG command - debugging utilities (stub)
pub fn cmdDebug(
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
    } else if (std.ascii.eqlIgnoreCase(subcommand, "HELP")) {
        const help_text =
            \\DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:
            \\OBJECT <key>
            \\    Show low-level info about key and associated value.
            \\HELP
            \\    Print this help.
        ;
        return try w.writeBulkString(help_text);
    } else {
        return try w.writeError("ERR unknown subcommand or wrong number of arguments");
    }
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

test "cmdMonitor - stub returns OK" {
    const allocator = std.testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();
    var pubsub = PubSub.init(allocator);
    defer pubsub.deinit();
    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();
    var config = ServerConfig.init();

    const args = [_][]const u8{"MONITOR"};
    const result = try cmdMonitor(allocator, &args, &storage, &pubsub, null, &client_registry, 1, &config, 2);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
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
