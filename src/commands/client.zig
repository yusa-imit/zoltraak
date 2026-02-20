const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;

/// Metadata for a single client connection
pub const ClientInfo = struct {
    /// Unique connection ID
    id: u64,
    /// Connection name (set via CLIENT SETNAME)
    name: ?[]const u8,
    /// Address string (e.g., "127.0.0.1:54321")
    addr: []const u8,
    /// File descriptor (for display purposes)
    fd: i32,
    /// Timestamp when connection was established (milliseconds)
    connected_at: i64,
    /// Timestamp of last command (milliseconds)
    last_cmd_at: i64,
    /// Last command name executed
    last_cmd: []const u8,
    /// Client flags (e.g., "N" = normal)
    flags: []const u8,

    /// Deinitialize and free resources
    pub fn deinit(self: *ClientInfo, allocator: std.mem.Allocator) void {
        if (self.name) |n| {
            allocator.free(n);
        }
        allocator.free(self.addr);
        allocator.free(self.last_cmd);
        allocator.free(self.flags);
    }
};

/// Thread-safe registry of all active client connections
pub const ClientRegistry = struct {
    allocator: std.mem.Allocator,
    /// Map: client_id -> ClientInfo
    clients: std.AutoHashMap(u64, ClientInfo),
    /// Monotonically increasing client ID counter
    next_id: u64,
    mutex: std.Thread.Mutex,

    /// Initialize a new client registry
    pub fn init(allocator: std.mem.Allocator) ClientRegistry {
        return ClientRegistry{
            .allocator = allocator,
            .clients = std.AutoHashMap(u64, ClientInfo).init(allocator),
            .next_id = 1,
            .mutex = std.Thread.Mutex{},
        };
    }

    /// Deinitialize and free all resources
    pub fn deinit(self: *ClientRegistry) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var it = self.clients.valueIterator();
        while (it.next()) |client| {
            var c = client.*;
            c.deinit(self.allocator);
        }
        self.clients.deinit();
    }

    /// Register a new client connection and return its ID
    pub fn registerClient(
        self: *ClientRegistry,
        addr: []const u8,
        fd: i32,
    ) !u64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const client_id = self.next_id;
        self.next_id += 1;

        const now = std.time.milliTimestamp();
        const addr_copy = try self.allocator.dupe(u8, addr);
        errdefer self.allocator.free(addr_copy);

        const default_cmd = try self.allocator.dupe(u8, "");
        errdefer self.allocator.free(default_cmd);

        const flags = try self.allocator.dupe(u8, "N");
        errdefer self.allocator.free(flags);

        const info = ClientInfo{
            .id = client_id,
            .name = null,
            .addr = addr_copy,
            .fd = fd,
            .connected_at = now,
            .last_cmd_at = now,
            .last_cmd = default_cmd,
            .flags = flags,
        };

        try self.clients.put(client_id, info);
        return client_id;
    }

    /// Unregister a client connection
    pub fn unregisterClient(self: *ClientRegistry, client_id: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.fetchRemove(client_id)) |kv| {
            var info = kv.value;
            info.deinit(self.allocator);
        }
    }

    /// Update the name for a client connection
    pub fn setClientName(self: *ClientRegistry, client_id: u64, name: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.getPtr(client_id)) |info| {
            if (info.name) |old_name| {
                self.allocator.free(old_name);
            }
            info.name = try self.allocator.dupe(u8, name);
        }
    }

    /// Get the name for a client connection (returns null if not set)
    pub fn getClientName(self: *ClientRegistry, client_id: u64, allocator: std.mem.Allocator) !?[]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.get(client_id)) |info| {
            if (info.name) |name| {
                return try allocator.dupe(u8, name);
            }
        }
        return null;
    }

    /// Update the last command timestamp and name for a client
    pub fn updateLastCommand(self: *ClientRegistry, client_id: u64, cmd_name: []const u8) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.getPtr(client_id)) |info| {
            // Allocate new command string first to avoid use-after-free
            const new_cmd = self.allocator.dupe(u8, cmd_name) catch {
                std.log.warn("CLIENT: failed to allocate memory for last_cmd update (client_id={d})", .{client_id});
                return; // Keep old value intact
            };
            // Now safe to free old value and update
            self.allocator.free(info.last_cmd);
            info.last_cmd = new_cmd;
            info.last_cmd_at = std.time.milliTimestamp();
        }
    }

    /// Format client list output matching Redis format
    pub fn formatClientList(
        self: *ClientRegistry,
        allocator: std.mem.Allocator,
        filter_type: ?[]const u8,
    ) ![]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var buf = std.ArrayList(u8){};
        defer buf.deinit(allocator);

        const now = std.time.milliTimestamp();

        var it = self.clients.valueIterator();
        while (it.next()) |info| {
            // Apply filters if specified
            if (filter_type) |ft| {
                if (std.mem.eql(u8, ft, "normal")) {
                    // Only show normal clients (all our clients for now)
                } else {
                    // Skip clients that don't match filter
                    continue;
                }
            }

            const age_sec = @divFloor(now - info.connected_at, 1000);
            const idle_sec = @divFloor(now - info.last_cmd_at, 1000);

            // Format: id=<id> addr=<addr> fd=<fd> name=<name> age=<age> idle=<idle> flags=<flags> db=0 sub=0 psub=0 cmd=<cmd>
            const name_str = info.name orelse "";
            try buf.writer(allocator).print("id={d} addr={s} fd={d} name={s} age={d} idle={d} flags={s} db=0 sub=0 psub=0 cmd={s}\n", .{
                info.id,
                info.addr,
                info.fd,
                name_str,
                age_sec,
                idle_sec,
                info.flags,
                info.last_cmd,
            });
        }

        return buf.toOwnedSlice(allocator);
    }
};

/// CLIENT ID - Return the current connection ID
fn cmdClientId(allocator: std.mem.Allocator, client_id: u64, args: []const RespValue) ![]const u8 {
    if (args.len != 1) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'client|id' command");
    }

    var w = Writer.init(allocator);
    defer w.deinit();
    return w.writeInteger(@intCast(client_id));
}

/// CLIENT GETNAME - Get the current connection name
fn cmdClientGetname(
    allocator: std.mem.Allocator,
    registry: *ClientRegistry,
    client_id: u64,
    args: []const RespValue,
) ![]const u8 {
    if (args.len != 1) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'client|getname' command");
    }

    const name = try registry.getClientName(client_id, allocator);
    defer if (name) |n| allocator.free(n);

    var w = Writer.init(allocator);
    defer w.deinit();

    if (name) |n| {
        return w.writeBulkString(n);
    } else {
        return w.writeNull();
    }
}

/// CLIENT SETNAME - Set the current connection name
fn cmdClientSetname(
    allocator: std.mem.Allocator,
    registry: *ClientRegistry,
    client_id: u64,
    args: []const RespValue,
) ![]const u8 {
    if (args.len != 2) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'client|setname' command");
    }

    const name = switch (args[1]) {
        .bulk_string => |s| s,
        else => {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR invalid name format");
        },
    };

    // Validate: name must not contain spaces
    for (name) |c| {
        if (c == ' ') {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR Client names cannot contain spaces, newlines or special characters.");
        }
    }

    try registry.setClientName(client_id, name);

    var w = Writer.init(allocator);
    defer w.deinit();
    return w.writeSimpleString("OK");
}

/// CLIENT LIST - List all client connections
fn cmdClientList(
    allocator: std.mem.Allocator,
    registry: *ClientRegistry,
    args: []const RespValue,
) ![]const u8 {
    // Parse optional TYPE filter
    var filter_type: ?[]const u8 = null;

    var i: usize = 1;
    while (i < args.len) : (i += 2) {
        const option = switch (args[i]) {
            .bulk_string => |s| s,
            else => {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            },
        };

        if (std.ascii.eqlIgnoreCase(option, "TYPE")) {
            if (i + 1 >= args.len) {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            }

            const type_value = switch (args[i + 1]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    return w.writeError("ERR syntax error");
                },
            };

            // Validate TYPE value - only "normal", "master", "replica", "pubsub" are valid
            if (!std.ascii.eqlIgnoreCase(type_value, "normal") and
                !std.ascii.eqlIgnoreCase(type_value, "master") and
                !std.ascii.eqlIgnoreCase(type_value, "replica") and
                !std.ascii.eqlIgnoreCase(type_value, "pubsub"))
            {
                var w = Writer.init(allocator);
                defer w.deinit();
                var buf = std.ArrayList(u8){};
                try buf.writer(allocator).print("ERR Unknown client type '{s}'", .{type_value});
                const msg = try buf.toOwnedSlice(allocator);
                defer allocator.free(msg);
                return w.writeError(msg);
            }

            filter_type = type_value;
        } else {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR syntax error");
        }
    }

    const list_output = try registry.formatClientList(allocator, filter_type);
    defer allocator.free(list_output);

    var w = Writer.init(allocator);
    defer w.deinit();
    return w.writeBulkString(list_output);
}

/// CLIENT command dispatcher
pub fn cmdClient(
    allocator: std.mem.Allocator,
    registry: *ClientRegistry,
    client_id: u64,
    args: []const RespValue,
) ![]const u8 {
    if (args.len < 1) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'client' command");
    }

    // Get subcommand
    const subcmd = switch (args[0]) {
        .bulk_string => |s| s,
        else => {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR invalid command format");
        },
    };

    // Route to appropriate handler
    if (std.ascii.eqlIgnoreCase(subcmd, "ID")) {
        return cmdClientId(allocator, client_id, args);
    } else if (std.ascii.eqlIgnoreCase(subcmd, "GETNAME")) {
        return cmdClientGetname(allocator, registry, client_id, args);
    } else if (std.ascii.eqlIgnoreCase(subcmd, "SETNAME")) {
        return cmdClientSetname(allocator, registry, client_id, args);
    } else if (std.ascii.eqlIgnoreCase(subcmd, "LIST")) {
        return cmdClientList(allocator, registry, args);
    } else {
        var w = Writer.init(allocator);
        defer w.deinit();
        var buf = std.ArrayList(u8){};
        // No defer buf.deinit() needed - toOwnedSlice handles it
        try buf.writer(allocator).print("ERR unknown subcommand '{s}'. Try CLIENT HELP.", .{subcmd});
        const msg = try buf.toOwnedSlice(allocator);
        defer allocator.free(msg);
        return w.writeError(msg);
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Unit Tests
// ────────────────────────────────────────────────────────────────────────────

test "ClientRegistry: register and unregister" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client1 = try registry.registerClient("127.0.0.1:12345", 42);
    const client2 = try registry.registerClient("127.0.0.1:54321", 43);

    try std.testing.expectEqual(@as(u64, 1), client1);
    try std.testing.expectEqual(@as(u64, 2), client2);
    try std.testing.expectEqual(@as(usize, 2), registry.clients.count());

    registry.unregisterClient(client1);
    try std.testing.expectEqual(@as(usize, 1), registry.clients.count());
}

test "ClientRegistry: set and get client name" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // Initially no name
    const name1 = try registry.getClientName(client_id, allocator);
    try std.testing.expectEqual(@as(?[]const u8, null), name1);

    // Set name
    try registry.setClientName(client_id, "test-client");

    // Get name
    const name2 = try registry.getClientName(client_id, allocator);
    try std.testing.expect(name2 != null);
    defer allocator.free(name2.?);
    try std.testing.expectEqualStrings("test-client", name2.?);

    // Update name
    try registry.setClientName(client_id, "updated-name");

    const name3 = try registry.getClientName(client_id, allocator);
    try std.testing.expect(name3 != null);
    defer allocator.free(name3.?);
    try std.testing.expectEqualStrings("updated-name", name3.?);
}

test "ClientRegistry: update last command" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    registry.updateLastCommand(client_id, "SET");
    registry.updateLastCommand(client_id, "GET");

    // Verify last_cmd is updated (check via formatClientList)
    const list = try registry.formatClientList(allocator, null);
    defer allocator.free(list);

    try std.testing.expect(std.mem.indexOf(u8, list, "cmd=GET") != null);
}

test "ClientRegistry: format client list" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client1 = try registry.registerClient("127.0.0.1:12345", 42);
    _ = try registry.registerClient("127.0.0.1:54321", 43);

    try registry.setClientName(client1, "client-one");
    registry.updateLastCommand(client1, "PING");

    const list = try registry.formatClientList(allocator, null);
    defer allocator.free(list);

    // Should contain both clients
    try std.testing.expect(std.mem.indexOf(u8, list, "id=1") != null);
    try std.testing.expect(std.mem.indexOf(u8, list, "id=2") != null);
    try std.testing.expect(std.mem.indexOf(u8, list, "name=client-one") != null);
    try std.testing.expect(std.mem.indexOf(u8, list, "cmd=PING") != null);
    try std.testing.expect(std.mem.indexOf(u8, list, "127.0.0.1:12345") != null);
}

test "CLIENT ID command" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // Build command: CLIENT ID
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "ID" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice);
    defer allocator.free(response);

    // Should return ":1\r\n"
    try std.testing.expectEqualStrings(":1\r\n", response);
}

test "CLIENT GETNAME command - no name set" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "GETNAME" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice);
    defer allocator.free(response);

    // Should return null bulk string
    try std.testing.expectEqualStrings("$-1\r\n", response);
}

test "CLIENT SETNAME command - success" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "SETNAME" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "my-client" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);

    // Verify name was set
    const name = try registry.getClientName(client_id, allocator);
    try std.testing.expect(name != null);
    defer allocator.free(name.?);
    try std.testing.expectEqualStrings("my-client", name.?);
}

test "CLIENT SETNAME command - rejects spaces" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "SETNAME" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "my client" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "spaces") != null);
}

test "CLIENT LIST command - basic" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client1 = try registry.registerClient("127.0.0.1:12345", 42);
    const client2 = try registry.registerClient("192.168.1.100:9999", 43);

    try registry.setClientName(client1, "alice");
    registry.updateLastCommand(client1, "SET");
    registry.updateLastCommand(client2, "GET");

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "LIST" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client1, args_slice);
    defer allocator.free(response);

    // Should be bulk string containing both clients
    try std.testing.expect(std.mem.startsWith(u8, response, "$"));
    try std.testing.expect(std.mem.indexOf(u8, response, "id=1") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "id=2") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "name=alice") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "127.0.0.1:12345") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "192.168.1.100:9999") != null);
}

test "CLIENT LIST command - with TYPE filter" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    _ = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "LIST" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "TYPE" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "normal" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, 1, args_slice);
    defer allocator.free(response);

    // Should return list with normal clients
    try std.testing.expect(std.mem.startsWith(u8, response, "$"));
    try std.testing.expect(std.mem.indexOf(u8, response, "id=1") != null);
}

test "CLIENT unknown subcommand" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "UNKNOWN" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "unknown subcommand") != null);
}

test "CLIENT LIST command - invalid TYPE" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    _ = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "LIST" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "TYPE" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "invalid" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, 1, args_slice);
    defer allocator.free(response);

    // Should return error for unknown client type
    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "Unknown client type") != null);
}
