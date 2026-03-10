const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;

/// RESP protocol version
pub const RespProtocol = enum(u8) {
    RESP2 = 2,
    RESP3 = 3,
};

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
    /// RESP protocol version (2 or 3, defaults to 2)
    protocol: RespProtocol,

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
    /// Set of client IDs marked for killing
    killed_clients: std.AutoHashMap(u64, void),
    /// Monotonically increasing client ID counter
    next_id: u64,
    mutex: std.Thread.Mutex,
    /// Pause expiration timestamp in milliseconds (0 = not paused)
    pause_until_ms: i64,
    /// Pause mode: true = ALL, false = WRITE only
    pause_all: bool,

    /// Initialize a new client registry
    pub fn init(allocator: std.mem.Allocator) ClientRegistry {
        return ClientRegistry{
            .allocator = allocator,
            .clients = std.AutoHashMap(u64, ClientInfo).init(allocator),
            .killed_clients = std.AutoHashMap(u64, void).init(allocator),
            .next_id = 1,
            .mutex = std.Thread.Mutex{},
            .pause_until_ms = 0,
            .pause_all = false,
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
        self.killed_clients.deinit();
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
            .protocol = .RESP2, // Default to RESP2
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

    /// Set the protocol version for a client connection
    pub fn setProtocol(self: *ClientRegistry, client_id: u64, proto: RespProtocol) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.getPtr(client_id)) |info| {
            info.protocol = proto;
        }
    }

    /// Get the protocol version for a client connection
    pub fn getProtocol(self: *ClientRegistry, client_id: u64) RespProtocol {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.get(client_id)) |info| {
            return info.protocol;
        }
        return .RESP2; // Default to RESP2 if client not found
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

    /// Mark a client for killing by ID
    pub fn markClientForKill(self: *ClientRegistry, client_id: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.contains(client_id)) {
            try self.killed_clients.put(client_id, {});
        }
    }

    /// Check if a client is marked for killing
    pub fn isClientKilled(self: *ClientRegistry, client_id: u64) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.killed_clients.contains(client_id);
    }

    /// Clear killed status for a client
    pub fn clearKilledStatus(self: *ClientRegistry, client_id: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        _ = self.killed_clients.remove(client_id);
    }

    /// Pause clients for specified duration
    pub fn pauseClients(self: *ClientRegistry, timeout_ms: i64, pause_all: bool) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = std.time.milliTimestamp();
        self.pause_until_ms = now + timeout_ms;
        self.pause_all = pause_all;
    }

    /// Unpause all clients
    pub fn unpauseClients(self: *ClientRegistry) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.pause_until_ms = 0;
        self.pause_all = false;
    }

    /// Check if clients are currently paused (for a specific command type)
    /// is_write: true if the command is a write command
    pub fn isClientsPaused(self: *ClientRegistry, is_write: bool) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = std.time.milliTimestamp();
        if (self.pause_until_ms > now) {
            // Pause is active
            if (self.pause_all) {
                return true; // Pause all commands
            } else {
                return is_write; // Pause only write commands
            }
        }
        return false;
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
/// CLIENT INFO - Get current client connection info
fn cmdClientInfo(
    allocator: std.mem.Allocator,
    registry: *ClientRegistry,
    client_id: u64,
    args: []const RespValue,
) ![]const u8 {
    if (args.len != 1) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'client|info' command");
    }

    registry.mutex.lock();
    defer registry.mutex.unlock();

    const maybe_client = registry.clients.get(client_id);
    if (maybe_client == null) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR no such client");
    }

    const info = maybe_client.?;
    const now = std.time.milliTimestamp();
    const age_sec = @divFloor(now - info.connected_at, 1000);
    const idle_sec = @divFloor(now - info.last_cmd_at, 1000);
    const name_str = info.name orelse "";

    var buf = std.ArrayList(u8){};
    defer buf.deinit(allocator);

    // Format: id=<id> addr=<addr> fd=<fd> name=<name> age=<age> idle=<idle> flags=<flags> db=0 sub=0 psub=0 cmd=<cmd>
    try buf.writer(allocator).print("id={d} addr={s} laddr= fd={d} name={s} age={d} idle={d} flags={s} db=0 sub=0 psub=0 ssub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events= cmd={s} user=default redir=-1 resp={d}", .{
        info.id,
        info.addr,
        info.fd,
        name_str,
        age_sec,
        idle_sec,
        info.flags,
        info.last_cmd,
        @intFromEnum(info.protocol),
    });

    const result = try buf.toOwnedSlice(allocator);
    defer allocator.free(result);

    var w = Writer.init(allocator);
    defer w.deinit();
    return w.writeBulkString(result);
}

/// Filters for CLIENT KILL command
const KillFilter = struct {
    id: ?u64 = null,
    addr: ?[]const u8 = null,
    laddr: ?[]const u8 = null,
    user: ?[]const u8 = null,
    type: ?[]const u8 = null,
    skipme: bool = true, // Default: skip calling client
    maxage: ?i64 = null, // In seconds
};

/// CLIENT KILL - Kill client connections by filter
fn cmdClientKill(
    allocator: std.mem.Allocator,
    registry: *ClientRegistry,
    client_id: u64,
    args: []const RespValue,
) ![]const u8 {
    // Old format: CLIENT KILL addr:port
    if (args.len == 2) {
        const target = switch (args[1]) {
            .bulk_string => |s| s,
            else => {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            },
        };

        // Parse addr:port format
        registry.mutex.lock();
        defer registry.mutex.unlock();

        var killed: bool = false;
        var it = registry.clients.iterator();
        while (it.next()) |entry| {
            if (std.mem.eql(u8, entry.value_ptr.addr, target)) {
                try registry.killed_clients.put(entry.key_ptr.*, {});
                killed = true;
                break;
            }
        }

        var w = Writer.init(allocator);
        defer w.deinit();
        if (killed) {
            return w.writeSimpleString("OK");
        } else {
            return w.writeError("ERR No such client");
        }
    }

    // New format: CLIENT KILL <filter> <value> [<filter> <value> ...]
    var filter = KillFilter{};
    var i: usize = 1;

    while (i < args.len) {
        const filter_name = switch (args[i]) {
            .bulk_string => |s| s,
            else => {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            },
        };

        if (std.ascii.eqlIgnoreCase(filter_name, "ID")) {
            if (i + 1 >= args.len) {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            }
            i += 1;
            const id_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    return w.writeError("ERR syntax error");
                },
            };
            filter.id = std.fmt.parseInt(u64, id_str, 10) catch {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR invalid ID");
            };
        } else if (std.ascii.eqlIgnoreCase(filter_name, "ADDR")) {
            if (i + 1 >= args.len) {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            }
            i += 1;
            filter.addr = switch (args[i]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    return w.writeError("ERR syntax error");
                },
            };
        } else if (std.ascii.eqlIgnoreCase(filter_name, "LADDR")) {
            if (i + 1 >= args.len) {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            }
            i += 1;
            filter.laddr = switch (args[i]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    return w.writeError("ERR syntax error");
                },
            };
        } else if (std.ascii.eqlIgnoreCase(filter_name, "USER")) {
            if (i + 1 >= args.len) {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            }
            i += 1;
            filter.user = switch (args[i]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    return w.writeError("ERR syntax error");
                },
            };
        } else if (std.ascii.eqlIgnoreCase(filter_name, "TYPE")) {
            if (i + 1 >= args.len) {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            }
            i += 1;
            const type_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    return w.writeError("ERR syntax error");
                },
            };
            // Validate type: normal, master, replica, slave, pubsub
            if (!std.ascii.eqlIgnoreCase(type_str, "normal") and
                !std.ascii.eqlIgnoreCase(type_str, "master") and
                !std.ascii.eqlIgnoreCase(type_str, "replica") and
                !std.ascii.eqlIgnoreCase(type_str, "slave") and
                !std.ascii.eqlIgnoreCase(type_str, "pubsub"))
            {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR Unknown client type");
            }
            filter.type = type_str;
        } else if (std.ascii.eqlIgnoreCase(filter_name, "SKIPME")) {
            if (i + 1 >= args.len) {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            }
            i += 1;
            const skipme_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    return w.writeError("ERR syntax error");
                },
            };
            if (std.ascii.eqlIgnoreCase(skipme_str, "YES")) {
                filter.skipme = true;
            } else if (std.ascii.eqlIgnoreCase(skipme_str, "NO")) {
                filter.skipme = false;
            } else {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            }
        } else if (std.ascii.eqlIgnoreCase(filter_name, "MAXAGE")) {
            if (i + 1 >= args.len) {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            }
            i += 1;
            const age_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    return w.writeError("ERR syntax error");
                },
            };
            filter.maxage = std.fmt.parseInt(i64, age_str, 10) catch {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR invalid maxage");
            };
        } else {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR syntax error");
        }

        i += 1;
    }

    // Apply filters and count killed clients
    registry.mutex.lock();
    defer registry.mutex.unlock();

    const now = std.time.milliTimestamp();
    var killed_count: u64 = 0;

    var it = registry.clients.iterator();
    while (it.next()) |entry| {
        const cid = entry.key_ptr.*;
        const info = entry.value_ptr;

        // Skip calling client if SKIPME=YES
        if (filter.skipme and cid == client_id) {
            continue;
        }

        // Apply ID filter
        if (filter.id) |id| {
            if (cid != id) continue;
        }

        // Apply ADDR filter
        if (filter.addr) |addr| {
            if (!std.mem.eql(u8, info.addr, addr)) continue;
        }

        // Apply LADDR filter (local bind address - we don't track this, so skip)
        if (filter.laddr) |_| {
            // For now, we don't track local bind address, so no clients match
            continue;
        }

        // Apply USER filter (we don't have ACL yet, so only "default" user)
        if (filter.user) |user| {
            if (!std.mem.eql(u8, user, "default")) continue;
        }

        // Apply TYPE filter (currently all clients are "normal")
        if (filter.type) |t| {
            if (!std.ascii.eqlIgnoreCase(t, "normal")) continue;
        }

        // Apply MAXAGE filter
        if (filter.maxage) |maxage| {
            const age_ms = now - info.connected_at;
            const age_sec = @divFloor(age_ms, 1000);
            if (age_sec < maxage) continue;
        }

        // All filters passed - mark for killing
        try registry.killed_clients.put(cid, {});
        killed_count += 1;
    }

    var w = Writer.init(allocator);
    defer w.deinit();
    return w.writeInteger(@intCast(killed_count));
}

/// CLIENT PAUSE - Pause all clients for specified duration
fn cmdClientPause(
    allocator: std.mem.Allocator,
    registry: *ClientRegistry,
    args: []const RespValue,
) ![]const u8 {
    if (args.len < 2 or args.len > 3) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'client|pause' command");
    }

    // Parse timeout (milliseconds)
    const timeout_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR syntax error");
        },
    };

    const timeout_ms = std.fmt.parseInt(i64, timeout_str, 10) catch {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR timeout is not an integer or out of range");
    };

    if (timeout_ms < 0) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR timeout must be non-negative");
    }

    // Parse mode (optional, defaults to WRITE)
    var pause_all = false;
    if (args.len == 3) {
        const mode = switch (args[2]) {
            .bulk_string => |s| s,
            else => {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            },
        };

        if (std.ascii.eqlIgnoreCase(mode, "WRITE")) {
            pause_all = false;
        } else if (std.ascii.eqlIgnoreCase(mode, "ALL")) {
            pause_all = true;
        } else {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR CLIENT PAUSE mode must be WRITE or ALL");
        }
    }

    registry.pauseClients(timeout_ms, pause_all);

    var w = Writer.init(allocator);
    defer w.deinit();
    return w.writeSimpleString("OK");
}

/// CLIENT UNPAUSE - Resume all paused clients
fn cmdClientUnpause(
    allocator: std.mem.Allocator,
    registry: *ClientRegistry,
    args: []const RespValue,
) ![]const u8 {
    if (args.len != 1) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'client|unpause' command");
    }

    registry.unpauseClients();

    var w = Writer.init(allocator);
    defer w.deinit();
    return w.writeSimpleString("OK");
}

/// CLIENT HELP - Display help for CLIENT subcommands
fn cmdClientHelp(allocator: std.mem.Allocator, args: []const RespValue) ![]const u8 {
    if (args.len != 1) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'client|help' command");
    }

    const help_items = [_][]const u8{
        "CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "GETNAME",
        "    Return the name of the current connection.",
        "SETNAME <name>",
        "    Set the name of the current connection.",
        "ID",
        "    Return the ID of the current connection.",
        "INFO",
        "    Return information about the current client connection.",
        "LIST [TYPE <NORMAL|MASTER|REPLICA|PUBSUB>]",
        "    Return information about client connections. Options are:",
        "    * TYPE <type>: Return clients of specified type.",
        "KILL <ip:port | <filter> <value> [<filter> <value> ...]>",
        "    Kill client connections. Filters are:",
        "    * ID <client-id>: Kill by client ID.",
        "    * ADDR <ip:port>: Kill by address.",
        "    * LADDR <ip:port>: Kill by local address.",
        "    * USER <username>: Kill by username.",
        "    * TYPE <NORMAL|MASTER|REPLICA|PUBSUB>: Kill by type.",
        "    * SKIPME <YES|NO>: Skip caller (default: YES).",
        "    * MAXAGE <seconds>: Kill connections older than seconds.",
        "PAUSE <timeout> [WRITE|ALL]",
        "    Suspend all clients for <timeout> milliseconds.",
        "    Mode: WRITE (pause write commands, default) or ALL (pause all commands).",
        "UNPAUSE",
        "    Resume all paused clients.",
        "HELP",
        "    Print this help.",
    };

    var w = Writer.init(allocator);
    defer w.deinit();
    return w.writeArrayOfBulkStrings(&help_items);
}

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
    } else if (std.ascii.eqlIgnoreCase(subcmd, "INFO")) {
        return cmdClientInfo(allocator, registry, client_id, args);
    } else if (std.ascii.eqlIgnoreCase(subcmd, "KILL")) {
        return cmdClientKill(allocator, registry, client_id, args);
    } else if (std.ascii.eqlIgnoreCase(subcmd, "PAUSE")) {
        return cmdClientPause(allocator, registry, args);
    } else if (std.ascii.eqlIgnoreCase(subcmd, "UNPAUSE")) {
        return cmdClientUnpause(allocator, registry, args);
    } else if (std.ascii.eqlIgnoreCase(subcmd, "HELP")) {
        return cmdClientHelp(allocator, args);
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

test "ClientRegistry: set and get protocol version" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // Default protocol should be RESP2
    try std.testing.expectEqual(RespProtocol.RESP2, registry.getProtocol(client_id));

    // Set to RESP3
    registry.setProtocol(client_id, .RESP3);
    try std.testing.expectEqual(RespProtocol.RESP3, registry.getProtocol(client_id));

    // Set back to RESP2
    registry.setProtocol(client_id, .RESP2);
    try std.testing.expectEqual(RespProtocol.RESP2, registry.getProtocol(client_id));
}

test "ClientRegistry: get protocol for non-existent client" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    // Should return RESP2 for non-existent client
    try std.testing.expectEqual(RespProtocol.RESP2, registry.getProtocol(999));
}

test "CLIENT INFO command" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "INFO" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice);
    defer allocator.free(response);

    // Should return bulk string with client info
    try std.testing.expect(std.mem.startsWith(u8, response, "$"));
    try std.testing.expect(std.mem.indexOf(u8, response, "id=1") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "addr=127.0.0.1:12345") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "fd=42") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "resp=2") != null);
}

test "CLIENT HELP command" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "HELP" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice);
    defer allocator.free(response);

    // Should return array of help items
    try std.testing.expect(std.mem.startsWith(u8, response, "*"));
    try std.testing.expect(std.mem.indexOf(u8, response, "GETNAME") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "SETNAME") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "INFO") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "KILL") != null);
}

test "CLIENT KILL command - old format" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client1 = try registry.registerClient("127.0.0.1:12345", 42);
    const client2 = try registry.registerClient("192.168.1.100:9999", 43);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "KILL" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "192.168.1.100:9999" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client1, args_slice);
    defer allocator.free(response);

    // Should return OK
    try std.testing.expectEqualStrings("+OK\r\n", response);

    // Check that client2 is marked for killing
    try std.testing.expect(registry.isClientKilled(client2));
    try std.testing.expect(!registry.isClientKilled(client1));
}

test "CLIENT KILL command - by ID" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client1 = try registry.registerClient("127.0.0.1:12345", 42);
    const client2 = try registry.registerClient("192.168.1.100:9999", 43);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "KILL" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ID" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "2" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client1, args_slice);
    defer allocator.free(response);

    // Should return :1\r\n (killed count)
    try std.testing.expectEqualStrings(":1\r\n", response);

    // Check that client2 is marked for killing
    try std.testing.expect(registry.isClientKilled(client2));
    try std.testing.expect(!registry.isClientKilled(client1));
}

test "CLIENT KILL command - by ADDR" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client1 = try registry.registerClient("127.0.0.1:12345", 42);
    const client2 = try registry.registerClient("192.168.1.100:9999", 43);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "KILL" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ADDR" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "127.0.0.1:12345" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client2, args_slice);
    defer allocator.free(response);

    // Should return :1\r\n
    try std.testing.expectEqualStrings(":1\r\n", response);
    try std.testing.expect(registry.isClientKilled(client1));
}

test "CLIENT KILL command - SKIPME YES" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client1 = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "KILL" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ID" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "1" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "SKIPME" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "YES" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client1, args_slice);
    defer allocator.free(response);

    // Should return :0\r\n (caller skipped)
    try std.testing.expectEqualStrings(":0\r\n", response);
    try std.testing.expect(!registry.isClientKilled(client1));
}

test "CLIENT KILL command - SKIPME NO" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client1 = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "KILL" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ID" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "1" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "SKIPME" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "NO" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client1, args_slice);
    defer allocator.free(response);

    // Should return :1\r\n (caller included)
    try std.testing.expectEqualStrings(":1\r\n", response);
    try std.testing.expect(registry.isClientKilled(client1));
}

test "CLIENT KILL command - by TYPE" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client1 = try registry.registerClient("127.0.0.1:12345", 42);
    const client2 = try registry.registerClient("192.168.1.100:9999", 43);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "KILL" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "TYPE" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "normal" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "SKIPME" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "NO" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client1, args_slice);
    defer allocator.free(response);

    // Should return :2\r\n (both clients are normal type)
    try std.testing.expectEqualStrings(":2\r\n", response);
    try std.testing.expect(registry.isClientKilled(client1));
    try std.testing.expect(registry.isClientKilled(client2));
}

test "CLIENT KILL command - by MAXAGE" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client1 = try registry.registerClient("127.0.0.1:12345", 42);

    // Sleep to ensure client is older than 0 seconds
    std.Thread.sleep(1_100_000_000); // 1.1 seconds

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "KILL" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "MAXAGE" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "1" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "SKIPME" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "NO" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client1, args_slice);
    defer allocator.free(response);

    // Should kill client older than 1 second
    try std.testing.expectEqualStrings(":1\r\n", response);
    try std.testing.expect(registry.isClientKilled(client1));
}

test "CLIENT KILL command - multiple filters" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client1 = try registry.registerClient("127.0.0.1:12345", 42);
    const client2 = try registry.registerClient("192.168.1.100:9999", 43);
    const client3 = try registry.registerClient("127.0.0.1:54321", 44);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "KILL" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "TYPE" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "normal" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ID" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "2" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client1, args_slice);
    defer allocator.free(response);

    // Should only kill client2 (matches both filters)
    try std.testing.expectEqualStrings(":1\r\n", response);
    try std.testing.expect(!registry.isClientKilled(client1));
    try std.testing.expect(registry.isClientKilled(client2));
    try std.testing.expect(!registry.isClientKilled(client3));
}

test "ClientRegistry: mark and check killed status" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client1 = try registry.registerClient("127.0.0.1:12345", 42);
    const client2 = try registry.registerClient("192.168.1.100:9999", 43);

    // Initially no clients are killed
    try std.testing.expect(!registry.isClientKilled(client1));
    try std.testing.expect(!registry.isClientKilled(client2));

    // Mark client1 for killing
    try registry.markClientForKill(client1);
    try std.testing.expect(registry.isClientKilled(client1));
    try std.testing.expect(!registry.isClientKilled(client2));

    // Clear killed status
    registry.clearKilledStatus(client1);
    try std.testing.expect(!registry.isClientKilled(client1));
}

test "CLIENT PAUSE command - WRITE mode" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "PAUSE" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "1000" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "WRITE" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);

    // Check pause state
    try std.testing.expect(registry.isClientsPaused(true)); // Write commands paused
    try std.testing.expect(!registry.isClientsPaused(false)); // Read commands not paused
}

test "CLIENT PAUSE command - ALL mode" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "PAUSE" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "1000" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ALL" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);

    // Check pause state
    try std.testing.expect(registry.isClientsPaused(true)); // Write commands paused
    try std.testing.expect(registry.isClientsPaused(false)); // Read commands also paused
}

test "CLIENT PAUSE command - default WRITE mode" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "PAUSE" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "1000" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);

    // Check pause state (should default to WRITE mode)
    try std.testing.expect(registry.isClientsPaused(true)); // Write commands paused
    try std.testing.expect(!registry.isClientsPaused(false)); // Read commands not paused
}

test "CLIENT PAUSE command - zero timeout" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "PAUSE" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "0" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);

    // Zero timeout means pause expires immediately
    try std.testing.expect(!registry.isClientsPaused(true));
}

test "CLIENT PAUSE command - negative timeout rejected" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "PAUSE" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "-1" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "non-negative") != null);
}

test "CLIENT UNPAUSE command" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // First pause clients
    registry.pauseClients(10000, true);
    try std.testing.expect(registry.isClientsPaused(true));
    try std.testing.expect(registry.isClientsPaused(false));

    // Now unpause
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "UNPAUSE" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);

    // Check unpause state
    try std.testing.expect(!registry.isClientsPaused(true));
    try std.testing.expect(!registry.isClientsPaused(false));
}

test "CLIENT PAUSE command - pause expires" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    // Pause for 100ms
    registry.pauseClients(100, true);
    try std.testing.expect(registry.isClientsPaused(true));

    // Sleep for 150ms to let pause expire
    std.Thread.sleep(150_000_000); // 150 milliseconds

    // Pause should have expired
    try std.testing.expect(!registry.isClientsPaused(true));
}

test "CLIENT PAUSE command - invalid mode" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "PAUSE" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "1000" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "INVALID" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "WRITE or ALL") != null);
}
