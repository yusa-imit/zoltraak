const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const blocking_mod = @import("../storage/blocking.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const BlockingQueue = blocking_mod.BlockingQueue;

/// RESP protocol version
pub const RespProtocol = enum(u8) {
    RESP2 = 2,
    RESP3 = 3,
};

/// Reply mode for CLIENT REPLY command
pub const ReplyMode = enum {
    ON,   // Normal replies (default)
    OFF,  // Suppress all replies
    SKIP, // Skip next reply only, then revert to ON
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
    /// Reply mode (ON/OFF/SKIP) for CLIENT REPLY command
    reply_mode: ReplyMode,
    /// No-evict flag for CLIENT NO-EVICT command
    no_evict: bool,
    /// No-touch flag for CLIENT NO-TOUCH command (prevents LRU/LFU updates)
    no_touch: bool,
    /// Library name for CLIENT SETINFO (optional)
    lib_name: ?[]const u8,
    /// Library version for CLIENT SETINFO (optional)
    lib_ver: ?[]const u8,
    /// Tracking enabled flag (CLIENT TRACKING ON/OFF)
    tracking_enabled: bool,
    /// Tracking redirect client ID (0 = self, -1 = none)
    tracking_redirect: i64,
    /// Tracking broadcasting mode flag
    tracking_bcast: bool,
    /// Tracking OPTIN mode (don't track unless CLIENT CACHING yes)
    tracking_optin: bool,
    /// Tracking OPTOUT mode (track unless CLIENT CACHING no)
    tracking_optout: bool,
    /// Tracking NOLOOP mode (don't send notifications for self-modified keys)
    tracking_noloop: bool,
    /// Next command caching flag for OPTIN/OPTOUT (null = follow mode default, true/false = override)
    tracking_next_cache: ?bool,
    /// Tracking prefixes for broadcasting mode
    tracking_prefixes: std.ArrayList([]const u8),
    /// Monitor mode flag (CLIENT is in MONITOR mode)
    monitor_mode: bool,
    /// Replication offset of the last write command executed by this client (for WAIT command)
    client_repl_offset: i64,
    /// Currently authenticated ACL user (null = unauthenticated, defaults to "default" user)
    authenticated_user: ?[]const u8,
    /// Currently selected database (0-15, default 0)
    selected_db: u16,
    /// Pending RESP3 push invalidation messages (queued for delivery on next command)
    pending_invalidations: std.ArrayList([]u8),
    /// Number of active channel subscriptions (SUBSCRIBE)
    sub_count: u32,
    /// Number of active pattern subscriptions (PSUBSCRIBE)
    psub_count: u32,
    /// Number of active shard channel subscriptions (SSUBSCRIBE)
    ssub_count: u32,

    /// Deinitialize and free resources
    pub fn deinit(self: *ClientInfo, allocator: std.mem.Allocator) void {
        if (self.authenticated_user) |user| {
            allocator.free(user);
        }
        if (self.name) |n| {
            allocator.free(n);
        }
        if (self.lib_name) |ln| {
            allocator.free(ln);
        }
        if (self.lib_ver) |lv| {
            allocator.free(lv);
        }
        // Free tracking prefixes
        for (self.tracking_prefixes.items) |prefix| {
            allocator.free(prefix);
        }
        self.tracking_prefixes.deinit(allocator);
        // Free pending invalidation messages
        for (self.pending_invalidations.items) |msg| {
            allocator.free(msg);
        }
        self.pending_invalidations.deinit(allocator);
        allocator.free(self.addr);
        allocator.free(self.last_cmd);
        allocator.free(self.flags);
    }
};

/// Monitor message for broadcasting to monitoring clients
pub const MonitorMessage = struct {
    client_id: u64,
    message: []const u8,
};

/// Invalidation message for client-side caching
pub const InvalidationMessage = struct {
    client_id: u64,
    key: []const u8,

    pub fn deinit(self: *InvalidationMessage, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
    }
};

/// Set of client IDs tracking a key
const ClientSet = std.AutoHashMap(u64, void);

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
    /// Tracking table: key -> set of client IDs that accessed this key
    tracking_table: std.StringHashMap(ClientSet),
    /// Maximum tracking table size (configurable via CONFIG SET tracking-table-max-keys)
    tracking_table_max_keys: usize,

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
            .tracking_table = std.StringHashMap(ClientSet).init(allocator),
            .tracking_table_max_keys = 1_000_000, // Default 1M keys (same as Redis)
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

        // Clean up tracking table
        var tracking_it = self.tracking_table.iterator();
        while (tracking_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var client_set = entry.value_ptr.*;
            client_set.deinit();
        }
        self.tracking_table.deinit();
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
            .reply_mode = .ON, // Default to normal replies
            .no_evict = false, // Default to normal eviction
            .no_touch = false, // Default to normal LRU/LFU updates
            .lib_name = null, // No library name by default
            .lib_ver = null, // No library version by default
            .tracking_enabled = false, // Tracking off by default
            .tracking_redirect = -1, // No redirect by default
            .tracking_bcast = false, // Broadcasting off
            .tracking_optin = false, // OPTIN off
            .tracking_optout = false, // OPTOUT off
            .tracking_noloop = false, // NOLOOP off
            .tracking_next_cache = null, // No override
            .tracking_prefixes = std.ArrayList([]const u8){},
            .monitor_mode = false, // Monitor mode off by default
            .client_repl_offset = 0, // Start at offset 0
            .authenticated_user = null, // Unauthenticated by default (will use "default" user)
            .selected_db = 0, // Start at database 0
            .pending_invalidations = std.ArrayList([]u8){},
            .sub_count = 0,
            .psub_count = 0,
            .ssub_count = 0,
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

    /// Set the authenticated ACL user for a client connection
    pub fn setAuthenticatedUser(self: *ClientRegistry, client_id: u64, username: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.getPtr(client_id)) |info| {
            if (info.authenticated_user) |old_user| {
                self.allocator.free(old_user);
            }
            info.authenticated_user = try self.allocator.dupe(u8, username);
        }
    }

    /// Check if a client has explicitly authenticated (authenticated_user != null)
    pub fn isAuthenticated(self: *ClientRegistry, client_id: u64) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.get(client_id)) |info| {
            return info.authenticated_user != null;
        }
        return false;
    }

    /// Get client address string (e.g. "127.0.0.1:12345"). Returns "unknown" if not found.
    pub fn getClientAddr(self: *ClientRegistry, client_id: u64, allocator: std.mem.Allocator) ![]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.get(client_id)) |info| {
            return try allocator.dupe(u8, info.addr);
        }
        return try allocator.dupe(u8, "unknown");
    }

    /// Get the authenticated ACL user for a client connection (returns "default" if null)
    pub fn getAuthenticatedUser(self: *ClientRegistry, client_id: u64, allocator: std.mem.Allocator) ![]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.get(client_id)) |info| {
            if (info.authenticated_user) |user| {
                return try allocator.dupe(u8, user);
            }
        }
        // Return "default" user if no authentication has occurred
        return try allocator.dupe(u8, "default");
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

    /// Set the selected database for a client connection (SELECT command)
    pub fn setSelectedDb(self: *ClientRegistry, client_id: u64, db_index: u16) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.getPtr(client_id)) |info| {
            info.selected_db = db_index;
        }
    }

    /// Get the selected database for a client connection
    pub fn getSelectedDb(self: *ClientRegistry, client_id: u64) u16 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.get(client_id)) |info| {
            return info.selected_db;
        }
        return 0; // Default to DB 0 if client not found
    }

    /// Set reply mode for a client (CLIENT REPLY)
    pub fn setReplyMode(self: *ClientRegistry, client_id: u64, mode: ReplyMode) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.getPtr(client_id)) |info| {
            info.reply_mode = mode;
        }
    }

    /// Get reply mode for a client
    pub fn getReplyMode(self: *ClientRegistry, client_id: u64) ReplyMode {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.get(client_id)) |info| {
            return info.reply_mode;
        }
        return .ON; // Default to ON if client not found
    }

    /// Process reply for SKIP mode (converts SKIP to ON after one command)
    pub fn processReplySkip(self: *ClientRegistry, client_id: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.getPtr(client_id)) |info| {
            if (info.reply_mode == .SKIP) {
                info.reply_mode = .ON;
            }
        }
    }

    /// Set no-evict flag for a client (CLIENT NO-EVICT)
    pub fn setNoEvict(self: *ClientRegistry, client_id: u64, no_evict: bool) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.getPtr(client_id)) |info| {
            info.no_evict = no_evict;
        }
    }

    /// Get no-evict flag for a client
    pub fn getNoEvict(self: *ClientRegistry, client_id: u64) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.get(client_id)) |info| {
            return info.no_evict;
        }
        return false; // Default to false if client not found
    }

    /// Set no-touch flag for a client (prevents LRU/LFU updates)
    pub fn setNoTouch(self: *ClientRegistry, client_id: u64, no_touch: bool) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.getPtr(client_id)) |info| {
            info.no_touch = no_touch;
        }
    }

    /// Update subscription counts for a client (called after SUBSCRIBE/UNSUBSCRIBE/etc.)
    pub fn updateSubCounts(self: *ClientRegistry, client_id: u64, sub: u32, psub: u32, ssub: u32) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.getPtr(client_id)) |info| {
            info.sub_count = sub;
            info.psub_count = psub;
            info.ssub_count = ssub;
        }
    }

    /// Get no-touch flag for a client
    pub fn getNoTouch(self: *ClientRegistry, client_id: u64) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.get(client_id)) |info| {
            return info.no_touch;
        }
        return false; // Default to false if client not found
    }

    /// Set library name for a client (CLIENT SETINFO LIB-NAME)
    pub fn setLibName(self: *ClientRegistry, client_id: u64, lib_name: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.getPtr(client_id)) |info| {
            // Free old value if present
            if (info.lib_name) |old| {
                self.allocator.free(old);
            }
            // Duplicate and store new value
            info.lib_name = try self.allocator.dupe(u8, lib_name);
        }
    }

    /// Set library version for a client (CLIENT SETINFO LIB-VER)
    pub fn setLibVer(self: *ClientRegistry, client_id: u64, lib_ver: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.getPtr(client_id)) |info| {
            // Free old value if present
            if (info.lib_ver) |old| {
                self.allocator.free(old);
            }
            // Duplicate and store new value
            info.lib_ver = try self.allocator.dupe(u8, lib_ver);
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
            // Determine client type based on subscription state
            const is_pubsub = (info.sub_count + info.psub_count + info.ssub_count) > 0;
            const client_type = if (is_pubsub) "pubsub" else "normal";

            // Apply type filter: "normal" | "pubsub" | "replica" | "master"
            if (filter_type) |ft| {
                if (std.ascii.eqlIgnoreCase(ft, "normal")) {
                    // normal: non-pubsub clients
                    if (is_pubsub) continue;
                } else if (std.ascii.eqlIgnoreCase(ft, "pubsub")) {
                    // pubsub: clients in subscribe mode
                    if (!is_pubsub) continue;
                } else if (std.ascii.eqlIgnoreCase(ft, "replica") or std.ascii.eqlIgnoreCase(ft, "slave")) {
                    // replica: replication clients — none tracked here
                    continue;
                } else if (std.ascii.eqlIgnoreCase(ft, "master")) {
                    // master: outbound connections — none tracked here
                    continue;
                }
            }

            const age_sec = @divFloor(now - info.connected_at, 1000);
            const idle_sec = @divFloor(now - info.last_cmd_at, 1000);
            const name_str = info.name orelse "";
            const lib_name_str = info.lib_name orelse "";
            const lib_ver_str = info.lib_ver orelse "";
            const user_str = if (info.authenticated_user) |u| u else "default";
            const redir: i64 = if (info.tracking_enabled and info.tracking_redirect > 0)
                info.tracking_redirect
            else
                -1;

            // Redis 7.x full CLIENT LIST format
            try buf.writer(allocator).print(
                "id={d} addr={s} laddr= fd={d} name={s} age={d} idle={d} flags={s} db={d} sub={d} psub={d} ssub={d} multi=-1 watch=0 qbuf=0 qbuf-free=32768 argv-mem=10 multi-mem=0 tot-mem=20512 rbs=16384 rbp=0 obl=0 oll=0 omem=0 events=r cmd={s} user={s} library-name={s} library-ver={s} redir={d} resp={d} type={s}\n",
                .{
                    info.id,
                    info.addr,
                    info.fd,
                    name_str,
                    age_sec,
                    idle_sec,
                    info.flags,
                    info.selected_db,
                    info.sub_count,
                    info.psub_count,
                    info.ssub_count,
                    info.last_cmd,
                    user_str,
                    lib_name_str,
                    lib_ver_str,
                    redir,
                    @intFromEnum(info.protocol),
                    client_type,
                },
            );
        }

        return buf.toOwnedSlice(allocator);
    }

    /// Format CLIENT LIST output for specific client IDs only (Redis 7.0+ CLIENT LIST ID filter)
    pub fn formatClientListByIds(
        self: *ClientRegistry,
        allocator: std.mem.Allocator,
        ids: []const u64,
    ) ![]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var buf = std.ArrayList(u8){};
        defer buf.deinit(allocator);

        const now = std.time.milliTimestamp();

        for (ids) |target_id| {
            const info = self.clients.get(target_id) orelse continue;

            const is_pubsub = (info.sub_count + info.psub_count + info.ssub_count) > 0;
            const client_type = if (is_pubsub) "pubsub" else "normal";

            const age_sec = @divFloor(now - info.connected_at, 1000);
            const idle_sec = @divFloor(now - info.last_cmd_at, 1000);
            const name_str = info.name orelse "";
            const lib_name_str = info.lib_name orelse "";
            const lib_ver_str = info.lib_ver orelse "";
            const user_str = if (info.authenticated_user) |u| u else "default";
            const redir: i64 = if (info.tracking_enabled and info.tracking_redirect > 0)
                info.tracking_redirect
            else
                -1;

            try buf.writer(allocator).print(
                "id={d} addr={s} laddr= fd={d} name={s} age={d} idle={d} flags={s} db={d} sub={d} psub={d} ssub={d} multi=-1 watch=0 qbuf=0 qbuf-free=32768 argv-mem=10 multi-mem=0 tot-mem=20512 rbs=16384 rbp=0 obl=0 oll=0 omem=0 events=r cmd={s} user={s} library-name={s} library-ver={s} redir={d} resp={d} type={s}\n",
                .{
                    info.id,
                    info.addr,
                    info.fd,
                    name_str,
                    age_sec,
                    idle_sec,
                    info.flags,
                    info.selected_db,
                    info.sub_count,
                    info.psub_count,
                    info.ssub_count,
                    info.last_cmd,
                    user_str,
                    lib_name_str,
                    lib_ver_str,
                    redir,
                    @intFromEnum(info.protocol),
                    client_type,
                },
            );
        }

        return buf.toOwnedSlice(allocator);
    }

    /// Mark a client for killing by ID
    /// Return the number of currently connected clients (thread-safe)
    pub fn count(self: *ClientRegistry) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.clients.count();
    }

    /// Count clients with CLIENT TRACKING enabled (for INFO clients tracking_clients)
    pub fn countTrackingClients(self: *ClientRegistry) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        var n: usize = 0;
        var it = self.clients.valueIterator();
        while (it.next()) |info| {
            if (info.tracking_enabled) n += 1;
        }
        return n;
    }

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

    /// Enable/disable tracking for a client
    pub fn setTracking(
        self: *ClientRegistry,
        client_id: u64,
        enabled: bool,
        redirect: i64,
        bcast: bool,
        optin: bool,
        optout: bool,
        noloop: bool,
        prefixes: []const []const u8,
    ) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.getPtr(client_id)) |info| {
            // Validate redirect client ID exists if not -1 or 0
            if (redirect > 0 and !self.clients.contains(@intCast(redirect))) {
                return error.InvalidRedirect;
            }

            // Clear old prefixes
            for (info.tracking_prefixes.items) |prefix| {
                self.allocator.free(prefix);
            }
            info.tracking_prefixes.clearRetainingCapacity();

            // Set tracking state
            info.tracking_enabled = enabled;
            info.tracking_redirect = redirect;
            info.tracking_bcast = bcast;
            info.tracking_optin = optin;
            info.tracking_optout = optout;
            info.tracking_noloop = noloop;
            info.tracking_next_cache = null; // Reset override

            // Copy new prefixes
            if (enabled and bcast) {
                for (prefixes) |prefix| {
                    const prefix_copy = try self.allocator.dupe(u8, prefix);
                    errdefer self.allocator.free(prefix_copy);
                    try info.tracking_prefixes.append(self.allocator, prefix_copy);
                }
            }
        }
    }

    /// Get tracking info for a client (for CLIENT TRACKINGINFO)
    pub fn getTrackingInfo(self: *ClientRegistry, client_id: u64, allocator: std.mem.Allocator) !?struct {
        enabled: bool,
        redirect: i64,
        bcast: bool,
        optin: bool,
        optout: bool,
        noloop: bool,
        next_cache: ?bool,
        prefixes: []const []const u8,
    } {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.get(client_id)) |info| {
            // Copy prefixes
            const prefixes = try allocator.alloc([]const u8, info.tracking_prefixes.items.len);
            for (info.tracking_prefixes.items, 0..) |prefix, i| {
                prefixes[i] = try allocator.dupe(u8, prefix);
            }

            return .{
                .enabled = info.tracking_enabled,
                .redirect = info.tracking_redirect,
                .bcast = info.tracking_bcast,
                .optin = info.tracking_optin,
                .optout = info.tracking_optout,
                .noloop = info.tracking_noloop,
                .next_cache = info.tracking_next_cache,
                .prefixes = prefixes,
            };
        }
        return null;
    }

    /// Set next command caching flag (for CLIENT CACHING)
    pub fn setTrackingNextCache(self: *ClientRegistry, client_id: u64, cache: bool) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.getPtr(client_id)) |info| {
            info.tracking_next_cache = cache;
        }
    }

    /// Reset next command caching flag (after command execution)
    pub fn resetTrackingNextCache(self: *ClientRegistry, client_id: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.getPtr(client_id)) |info| {
            info.tracking_next_cache = null;
        }
    }

    /// Track key access for a client (for server-assisted client-side caching)
    /// This records that a client accessed a key, so we can send invalidation messages later.
    pub fn trackKeyAccess(self: *ClientRegistry, client_id: u64, key: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Get client info to check tracking settings
        const info = self.clients.get(client_id) orelse return;

        // Determine if we should track this key access
        const should_track = blk: {
            if (!info.tracking_enabled) break :blk false;

            // OPTIN mode: only track if next_cache is explicitly true
            if (info.tracking_optin) {
                break :blk info.tracking_next_cache orelse false;
            }

            // OPTOUT mode: track unless next_cache is explicitly false
            if (info.tracking_optout) {
                break :blk info.tracking_next_cache orelse true;
            }

            // Default mode (not OPTIN/OPTOUT): always track
            break :blk true;
        };

        if (!should_track) return;

        // BCAST mode: check if key matches any prefix
        if (info.tracking_bcast) {
            var matches = false;
            for (info.tracking_prefixes.items) |prefix| {
                if (std.mem.startsWith(u8, key, prefix)) {
                    matches = true;
                    break;
                }
            }
            if (!matches) return; // Key doesn't match any prefix
        }

        // Enforce tracking table size limit
        if (self.tracking_table.count() >= self.tracking_table_max_keys) {
            // Table is full - evict random entry (LRU would be more complex)
            var it = self.tracking_table.iterator();
            if (it.next()) |first| {
                const key_to_remove = first.key_ptr.*;
                if (self.tracking_table.fetchRemove(key_to_remove)) |kv| {
                    self.allocator.free(kv.key);
                    var removed_set = kv.value;
                    removed_set.deinit();
                }
            }
        }

        // Get or create client set for this key
        const key_copy = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_copy);

        const entry = try self.tracking_table.getOrPut(key_copy);
        if (entry.found_existing) {
            // Key already exists, free the duplicate
            self.allocator.free(key_copy);
        } else {
            // New entry, initialize client set
            entry.value_ptr.* = ClientSet.init(self.allocator);
        }
        errdefer if (!entry.found_existing) {
            entry.value_ptr.deinit();
            _ = self.tracking_table.remove(entry.key_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        };

        // Add this client to the set (if not already present)
        try entry.value_ptr.put(client_id, {});
    }

    /// Send invalidation message for a key to all tracking clients
    /// This is called when a key is modified (SET/DEL/EXPIRE/etc.)
    /// Returns a list of invalidation messages to send
    pub fn getInvalidationMessages(
        self: *ClientRegistry,
        key: []const u8,
        modifier_client_id: u64,
        allocator: std.mem.Allocator,
    ) ![]InvalidationMessage {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Look up clients tracking this key
        const client_set = self.tracking_table.get(key) orelse return &[_]InvalidationMessage{};

        var messages = std.ArrayList(InvalidationMessage){};
        defer messages.deinit(allocator);

        var it = client_set.keyIterator();
        while (it.next()) |tracked_client_id| {
            const tracked_id = tracked_client_id.*;
            const tracked_info = self.clients.get(tracked_id) orelse continue;

            // NOLOOP: skip if this is the client that modified the key
            if (tracked_info.tracking_noloop and tracked_id == modifier_client_id) {
                continue;
            }

            // BCAST mode: check if key matches prefix
            if (tracked_info.tracking_bcast) {
                var matches = false;
                for (tracked_info.tracking_prefixes.items) |prefix| {
                    if (std.mem.startsWith(u8, key, prefix)) {
                        matches = true;
                        break;
                    }
                }
                if (!matches) continue;
            }

            // Determine target client for invalidation
            const target_client_id = if (tracked_info.tracking_redirect > 0)
                @as(u64, @intCast(tracked_info.tracking_redirect))
            else
                tracked_id;

            const key_copy = try allocator.dupe(u8, key);
            errdefer allocator.free(key_copy);

            try messages.append(allocator, .{
                .client_id = target_client_id,
                .key = key_copy,
            });
        }

        return try messages.toOwnedSlice(allocator);
    }

    /// Remove key from tracking table (called when key is deleted)
    pub fn removeKeyFromTracking(self: *ClientRegistry, key: []const u8) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.tracking_table.fetchRemove(key)) |kv| {
            self.allocator.free(kv.key);
            var client_set = kv.value;
            client_set.deinit();
        }
    }

    /// Remove all tracking entries for a client (called when client disconnects or tracking is disabled)
    pub fn removeClientFromTracking(self: *ClientRegistry, client_id: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var it = self.tracking_table.iterator();
        while (it.next()) |entry| {
            _ = entry.value_ptr.remove(client_id);
        }
    }

    /// Queue a RESP3 push invalidation message for a client.
    /// The message bytes are owned by the caller and must have been allocated
    /// with the registry's allocator. The registry takes ownership.
    pub fn queuePushMessage(self: *ClientRegistry, client_id: u64, message: []u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.getPtr(client_id)) |info| {
            try info.pending_invalidations.append(self.allocator, message);
        } else {
            // Client gone — free the message
            self.allocator.free(message);
        }
    }

    /// Take all pending invalidation messages for a client.
    /// Returns an owned slice of message byte slices; caller must:
    ///   - free each inner slice with self.allocator.free(msg)
    ///   - free the outer slice with self.allocator.free(slice)
    /// Returns null if client not found or has no pending messages.
    pub fn takePendingInvalidations(self: *ClientRegistry, client_id: u64) ?[][]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const info = self.clients.getPtr(client_id) orelse return null;
        if (info.pending_invalidations.items.len == 0) return null;

        const owned = info.pending_invalidations.toOwnedSlice(self.allocator) catch return null;
        return owned;
    }

    /// Enable/disable monitor mode for a client
    pub fn setMonitorMode(self: *ClientRegistry, client_id: u64, enabled: bool) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.getPtr(client_id)) |info| {
            info.monitor_mode = enabled;
        }
    }

    /// Check if a client is in monitor mode
    pub fn isMonitoring(self: *ClientRegistry, client_id: u64) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.get(client_id)) |info| {
            return info.monitor_mode;
        }
        return false;
    }

    /// Get list of all monitoring client IDs
    pub fn getMonitoringClients(self: *ClientRegistry, allocator: std.mem.Allocator) ![]u64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var monitors = std.ArrayList(u64){};
        defer monitors.deinit(allocator);

        var it = self.clients.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.monitor_mode) {
                try monitors.append(allocator, entry.key_ptr.*);
            }
        }

        return try allocator.dupe(u64, monitors.items);
    }

    /// Broadcast a command to all monitoring clients
    /// Returns RESP bulk string for each monitor client
    pub fn broadcastToMonitors(
        self: *ClientRegistry,
        allocator: std.mem.Allocator,
        timestamp_sec: i64,
        timestamp_usec: i64,
        db: u8,
        client_addr: []const u8,
        command_args: []const []const u8,
    ) !std.ArrayList(MonitorMessage) {
        var messages = std.ArrayList(MonitorMessage){};
        errdefer {
            for (messages.items) |msg| {
                allocator.free(msg.message);
            }
            messages.deinit(allocator);
        }

        const monitor_ids = try self.getMonitoringClients(allocator);
        defer allocator.free(monitor_ids);

        // Format: +timestamp.microsec [db client_addr] "command" "arg1" "arg2" ...
        var buf = std.ArrayList(u8){};
        defer buf.deinit(allocator);

        const writer = buf.writer(allocator);
        try writer.print("+{d}.{d:0>6} [{d} {s}]", .{ timestamp_sec, timestamp_usec, db, client_addr });

        for (command_args) |arg| {
            try writer.print(" \"", .{});
            // Escape quotes in the argument
            for (arg) |c| {
                if (c == '"' or c == '\\') {
                    try writer.writeByte('\\');
                }
                try writer.writeByte(c);
            }
            try writer.print("\"", .{});
        }
        try writer.print("\r\n", .{});

        const message = try allocator.dupe(u8, buf.items);
        errdefer allocator.free(message);

        // Send to all monitoring clients
        for (monitor_ids) |monitor_id| {
            const msg_copy = try allocator.dupe(u8, message);
            try messages.append(allocator, .{ .client_id = monitor_id, .message = msg_copy });
        }

        allocator.free(message);
        return messages;
    }

    /// Update the client's replication offset (called after write commands)
    pub fn updateClientReplOffset(self: *ClientRegistry, client_id: u64, repl_offset: i64) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.getPtr(client_id)) |info| {
            info.client_repl_offset = repl_offset;
        }
    }

    /// Get the client's replication offset
    pub fn getClientReplOffset(self: *ClientRegistry, client_id: u64) i64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.get(client_id)) |info| {
            return info.client_repl_offset;
        }
        return 0; // Default to 0 if client not found
    }

    /// Check if a client is using RESP3 protocol
    /// This is needed to determine if invalidation push messages should be sent
    pub fn isResp3Client(self: *ClientRegistry, client_id: u64) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.get(client_id)) |info| {
            return info.protocol == .RESP3;
        }
        return false;
    }

    /// Check if a client is actively tracking keys
    /// Returns true if tracking is enabled and client is ready to receive invalidations
    pub fn isActivelyTracking(self: *ClientRegistry, client_id: u64) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.clients.get(client_id)) |info| {
            return info.tracking_enabled;
        }
        return false;
    }
};

/// Helper function to notify clients about key invalidation.
/// Generates RESP3 push messages and queues them for delivery in server.zig.
/// Removes the key from the tracking table so clients must re-read to re-track.
pub fn notifyInvalidation(
    registry: *ClientRegistry,
    key: []const u8,
    modifier_client_id: u64,
    allocator: std.mem.Allocator,
) !void {
    // Generate invalidation messages for all tracking clients
    const messages = try registry.getInvalidationMessages(key, modifier_client_id, allocator);
    defer {
        for (messages) |*msg| {
            msg.deinit(allocator);
        }
        allocator.free(messages);
    }

    // Queue a RESP3 push invalidation message for each target client.
    // The registry owns the allocated bytes; the client's pending queue will
    // free them when the connection drains or the client disconnects.
    for (messages) |msg| {
        var w = Writer.init(registry.allocator);
        defer w.deinit();
        const keys_slice = [_][]const u8{msg.key};
        const push_const = w.writePushInvalidation(&keys_slice) catch continue;
        // Cast to mutable slice — we own this allocation and need []u8 for the queue
        const push_bytes = @constCast(push_const);
        registry.queuePushMessage(msg.client_id, push_bytes) catch {
            registry.allocator.free(push_bytes);
        };
    }

    // Remove key from tracking table (clients must re-read to re-track)
    // This happens even if NOLOOP suppressed the message
    registry.removeKeyFromTracking(key);
}

/// Helper function to notify clients about multiple key invalidations
/// This generates invalidation messages for multiple keys
pub fn notifyInvalidationBatch(
    registry: *ClientRegistry,
    keys: []const []const u8,
    modifier_client_id: u64,
    allocator: std.mem.Allocator,
) !void {
    for (keys) |key| {
        try notifyInvalidation(registry, key, modifier_client_id, allocator);
    }
}

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

    // Validate: name must contain only printable ASCII chars (0x21-0x7E).
    // Redis rejects spaces (0x20), control characters (< 0x21), and DEL/high bytes (> 0x7E).
    for (name) |c| {
        if (c < 0x21 or c > 0x7E) {
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
/// Syntax: CLIENT LIST [TYPE normal|master|replica|pubsub] [ID id [id ...]]
fn cmdClientList(
    allocator: std.mem.Allocator,
    registry: *ClientRegistry,
    args: []const RespValue,
) ![]const u8 {
    var filter_type: ?[]const u8 = null;
    var id_filter = std.ArrayList(u64){};
    defer id_filter.deinit(allocator);

    var i: usize = 1;
    while (i < args.len) {
        const option = switch (args[i]) {
            .bulk_string => |s| s,
            else => {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            },
        };

        if (std.ascii.eqlIgnoreCase(option, "TYPE")) {
            i += 1;
            if (i >= args.len) {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            }

            const type_value = switch (args[i]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    return w.writeError("ERR syntax error");
                },
            };

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
            i += 1;
        } else if (std.ascii.eqlIgnoreCase(option, "ID")) {
            // Consume all following numeric IDs until next keyword or end
            i += 1;
            if (i >= args.len) {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            }
            while (i < args.len) {
                const id_str = switch (args[i]) {
                    .bulk_string => |s| s,
                    else => break,
                };
                // Stop if the token looks like a keyword (TYPE or ID)
                if (std.ascii.eqlIgnoreCase(id_str, "TYPE") or std.ascii.eqlIgnoreCase(id_str, "ID")) break;
                const id_val = std.fmt.parseInt(u64, id_str, 10) catch {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    return w.writeError("ERR value is not an integer or out of range");
                };
                try id_filter.append(allocator, id_val);
                i += 1;
            }
        } else {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR syntax error");
        }
    }

    const list_output = if (id_filter.items.len > 0)
        try registry.formatClientListByIds(allocator, id_filter.items)
    else
        try registry.formatClientList(allocator, filter_type);
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
    const lib_name_str = info.lib_name orelse "";
    const lib_ver_str = info.lib_ver orelse "";
    const user_str = if (info.authenticated_user) |u| u else "default";
    const redir: i64 = if (info.tracking_enabled and info.tracking_redirect > 0)
        info.tracking_redirect
    else
        -1;

    var buf = std.ArrayList(u8){};
    defer buf.deinit(allocator);

    const is_pubsub = (info.sub_count + info.psub_count + info.ssub_count) > 0;
    const client_type = if (is_pubsub) "pubsub" else "normal";

    // Redis 7.x full CLIENT INFO format (same as CLIENT LIST line for this client)
    try buf.writer(allocator).print("id={d} addr={s} laddr= fd={d} name={s} age={d} idle={d} flags={s} db={d} sub={d} psub={d} ssub={d} multi=-1 watch=0 qbuf=0 qbuf-free=32768 argv-mem=10 multi-mem=0 tot-mem=20512 rbs=16384 rbp=0 obl=0 oll=0 omem=0 events=r cmd={s} user={s} library-name={s} library-ver={s} redir={d} resp={d} type={s}", .{
        info.id,
        info.addr,
        info.fd,
        name_str,
        age_sec,
        idle_sec,
        info.flags,
        info.selected_db,
        info.sub_count,
        info.psub_count,
        info.ssub_count,
        info.last_cmd,
        user_str,
        lib_name_str,
        lib_ver_str,
        redir,
        @intFromEnum(info.protocol),
        client_type,
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

/// CLIENT UNBLOCK client-id [TIMEOUT|ERROR]
/// Unblock a client blocked in a blocking command from a different connection.
/// Returns 1 if client was found and unblocked, 0 if client not found or not blocked.
fn cmdClientUnblock(
    allocator: std.mem.Allocator,
    blocking_queue: *@import("../storage/blocking.zig").BlockingQueue,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2 or args.len > 3) {
        return w.writeError("ERR wrong number of arguments for 'client|unblock' command");
    }

    // Parse client_id
    const client_id_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid client ID"),
    };

    const client_id = std.fmt.parseInt(u64, client_id_str, 10) catch {
        return w.writeError("ERR invalid client ID");
    };

    // Parse unblock mode (default: TIMEOUT)
    const mode: @import("../storage/blocking.zig").UnblockMode = if (args.len == 3) blk: {
        const mode_str = switch (args[2]) {
            .bulk_string => |s| s,
            else => break :blk .timeout,
        };

        if (std.ascii.eqlIgnoreCase(mode_str, "TIMEOUT")) {
            break :blk .timeout;
        } else if (std.ascii.eqlIgnoreCase(mode_str, "ERROR")) {
            break :blk .error_mode;
        } else {
            return w.writeError("ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR");
        }
    } else .timeout;

    // Request unblock
    const found = try blocking_queue.requestUnblock(client_id, mode);

    return w.writeInteger(if (found) 1 else 0);
}

/// CLIENT NO-EVICT [ON|OFF]
/// Control whether the client's keys should be protected from eviction.
/// When enabled, keys created by this client won't be evicted when maxmemory is reached.
/// Returns current status if no argument provided.
fn cmdClientNoEvict(
    allocator: std.mem.Allocator,
    client_registry: *ClientRegistry,
    client_id: u64,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len > 2) {
        return w.writeError("ERR wrong number of arguments for 'client|no-evict' command");
    }

    // If no argument, return current status
    if (args.len == 1) {
        const no_evict = client_registry.getNoEvict(client_id);
        return w.writeSimpleString(if (no_evict) "on" else "off");
    }

    // Parse ON|OFF argument
    const status_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    const no_evict = if (std.ascii.eqlIgnoreCase(status_str, "ON"))
        true
    else if (std.ascii.eqlIgnoreCase(status_str, "OFF"))
        false
    else
        return w.writeError("ERR CLIENT NO-EVICT accepts either 'ON' or 'OFF'");

    client_registry.setNoEvict(client_id, no_evict);
    return w.writeSimpleString("OK");
}

/// CLIENT NO-TOUCH [ON|OFF]
/// Control whether commands sent by the client will alter LRU/LFU stats.
/// When ON, the client will not change LFU/LRU stats unless it sends TOUCH.
/// When OFF, the client touches LFU/LRU stats like normal (default).
/// Returns current status if no argument provided.
fn cmdClientNoTouch(
    allocator: std.mem.Allocator,
    client_registry: *ClientRegistry,
    client_id: u64,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len > 2) {
        return w.writeError("ERR wrong number of arguments for 'client|no-touch' command");
    }

    // If no argument, return current status
    if (args.len == 1) {
        const no_touch = client_registry.getNoTouch(client_id);
        return w.writeSimpleString(if (no_touch) "on" else "off");
    }

    // Parse ON|OFF argument
    const status_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    const no_touch = if (std.ascii.eqlIgnoreCase(status_str, "ON"))
        true
    else if (std.ascii.eqlIgnoreCase(status_str, "OFF"))
        false
    else
        return w.writeError("ERR CLIENT NO-TOUCH accepts either 'ON' or 'OFF'");

    client_registry.setNoTouch(client_id, no_touch);
    return w.writeSimpleString("OK");
}

/// CLIENT SETINFO LIB-NAME|LIB-VER <value>
/// Assign library name or version info to the current connection.
/// This info is displayed in CLIENT LIST and CLIENT INFO output.
/// Returns OK if the attribute was successfully set.
fn cmdClientSetinfo(
    allocator: std.mem.Allocator,
    client_registry: *ClientRegistry,
    client_id: u64,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'client|setinfo' command");
    }

    // Parse attribute name
    const attr_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    // Parse attribute value
    const value_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    // Validate no spaces, newlines, or non-printable characters
    for (value_str) |c| {
        if (c == ' ' or c == '\n' or c == '\r' or c == '\t' or c < 32 or c > 126) {
            return w.writeError("ERR CLIENT SETINFO value contains invalid characters (spaces, newlines, or non-printable)");
        }
    }

    // Set the appropriate attribute
    if (std.ascii.eqlIgnoreCase(attr_str, "LIB-NAME")) {
        try client_registry.setLibName(client_id, value_str);
        return w.writeSimpleString("OK");
    } else if (std.ascii.eqlIgnoreCase(attr_str, "LIB-VER")) {
        try client_registry.setLibVer(client_id, value_str);
        return w.writeSimpleString("OK");
    } else {
        return w.writeError("ERR CLIENT SETINFO accepts either 'LIB-NAME' or 'LIB-VER'");
    }
}

/// CLIENT REPLY ON|OFF|SKIP
/// Control client reply behavior.
/// ON: Normal replies (default)
/// OFF: Suppress all replies until turned back ON
/// SKIP: Skip reply for the next command only, then revert to ON
fn cmdClientReply(
    allocator: std.mem.Allocator,
    client_registry: *ClientRegistry,
    client_id: u64,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'client|reply' command");
    }

    const mode_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    const mode: ReplyMode = if (std.ascii.eqlIgnoreCase(mode_str, "ON"))
        .ON
    else if (std.ascii.eqlIgnoreCase(mode_str, "OFF"))
        .OFF
    else if (std.ascii.eqlIgnoreCase(mode_str, "SKIP"))
        .SKIP
    else
        return w.writeError("ERR syntax error");

    client_registry.setReplyMode(client_id, mode);
    return w.writeSimpleString("OK");
}

/// CLIENT TRACKING ON|OFF [REDIRECT client-id] [PREFIX prefix ...] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]
/// Enable/disable server-assisted client-side caching tracking on the current connection.
fn cmdClientTracking(
    allocator: std.mem.Allocator,
    client_registry: *ClientRegistry,
    client_id: u64,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'client|tracking' command");
    }

    // Parse ON/OFF
    const on_off_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    const enabled = if (std.ascii.eqlIgnoreCase(on_off_str, "ON"))
        true
    else if (std.ascii.eqlIgnoreCase(on_off_str, "OFF"))
        false
    else
        return w.writeError("ERR syntax error");

    // Parse options
    var redirect: i64 = -1;
    var bcast = false;
    var optin = false;
    var optout = false;
    var noloop = false;
    var prefixes = std.ArrayList([]const u8){};
    defer prefixes.deinit(allocator);

    var i: usize = 2;
    while (i < args.len) {
        const option = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        if (std.ascii.eqlIgnoreCase(option, "REDIRECT")) {
            if (i + 1 >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const redirect_str = switch (args[i + 1]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            redirect = std.fmt.parseInt(i64, redirect_str, 10) catch {
                return w.writeError("ERR invalid client ID");
            };
            i += 2;
        } else if (std.ascii.eqlIgnoreCase(option, "PREFIX")) {
            if (i + 1 >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const prefix = switch (args[i + 1]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            try prefixes.append(allocator, prefix);
            i += 2;
        } else if (std.ascii.eqlIgnoreCase(option, "BCAST")) {
            bcast = true;
            i += 1;
        } else if (std.ascii.eqlIgnoreCase(option, "OPTIN")) {
            optin = true;
            i += 1;
        } else if (std.ascii.eqlIgnoreCase(option, "OPTOUT")) {
            optout = true;
            i += 1;
        } else if (std.ascii.eqlIgnoreCase(option, "NOLOOP")) {
            noloop = true;
            i += 1;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Validate: OPTIN and OPTOUT are mutually exclusive
    if (optin and optout) {
        return w.writeError("ERR OPTIN and OPTOUT are mutually exclusive");
    }

    // Set tracking state
    client_registry.setTracking(client_id, enabled, redirect, bcast, optin, optout, noloop, prefixes.items) catch |err| {
        if (err == error.InvalidRedirect) {
            return w.writeError("ERR invalid redirect client ID");
        }
        return w.writeError("ERR failed to set tracking");
    };

    return w.writeSimpleString("OK");
}

/// CLIENT TRACKINGINFO
/// Return information about the current client's use of server-assisted client-side caching.
fn cmdClientTrackinginfo(
    allocator: std.mem.Allocator,
    client_registry: *ClientRegistry,
    client_id: u64,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 1) {
        return w.writeError("ERR wrong number of arguments for 'client|trackinginfo' command");
    }

    const tracking_info = (try client_registry.getTrackingInfo(client_id, allocator)) orelse {
        return w.writeError("ERR no such client");
    };
    defer {
        for (tracking_info.prefixes) |prefix| {
            allocator.free(prefix);
        }
        allocator.free(tracking_info.prefixes);
    }

    // Build flags array
    var flags = std.ArrayList([]const u8){};
    defer flags.deinit(allocator);

    if (!tracking_info.enabled) {
        try flags.append(allocator, "off");
    } else {
        try flags.append(allocator, "on");
    }

    if (tracking_info.bcast) {
        try flags.append(allocator, "bcast");
    }

    if (tracking_info.optin) {
        try flags.append(allocator, "optin");
        if (tracking_info.next_cache) |cache| {
            if (cache) {
                try flags.append(allocator, "caching-yes");
            }
        }
    }

    if (tracking_info.optout) {
        try flags.append(allocator, "optout");
        if (tracking_info.next_cache) |cache| {
            if (!cache) {
                try flags.append(allocator, "caching-no");
            }
        }
    }

    if (tracking_info.noloop) {
        try flags.append(allocator, "noloop");
    }

    // Check if redirect is valid (stub: we don't track broken redirects yet)
    // In a full implementation, we'd check if the redirect client still exists

    // Format output as RESP map (RESP3) or array (RESP2)
    // For simplicity, we'll use array format compatible with both
    var result = std.ArrayList(u8){};
    defer result.deinit(allocator);

    const proto = client_registry.getProtocol(client_id);
    if (proto == .RESP3) {
        // RESP3 map format
        try result.writer(allocator).print("%3\r\n", .{});

        // flags field
        try result.writer(allocator).print("$5\r\nflags\r\n", .{});
        try result.writer(allocator).print("*{d}\r\n", .{flags.items.len});
        for (flags.items) |flag| {
            try result.writer(allocator).print("${d}\r\n{s}\r\n", .{ flag.len, flag });
        }

        // redirect field
        try result.writer(allocator).print("$8\r\nredirect\r\n", .{});
        try result.writer(allocator).print(":{d}\r\n", .{tracking_info.redirect});

        // prefixes field
        try result.writer(allocator).print("$8\r\nprefixes\r\n", .{});
        try result.writer(allocator).print("*{d}\r\n", .{tracking_info.prefixes.len});
        for (tracking_info.prefixes) |prefix| {
            try result.writer(allocator).print("${d}\r\n{s}\r\n", .{ prefix.len, prefix });
        }
    } else {
        // RESP2 array format
        try result.writer(allocator).print("*6\r\n", .{});

        // "flags"
        try result.writer(allocator).print("$5\r\nflags\r\n", .{});
        // flags array
        try result.writer(allocator).print("*{d}\r\n", .{flags.items.len});
        for (flags.items) |flag| {
            try result.writer(allocator).print("${d}\r\n{s}\r\n", .{ flag.len, flag });
        }

        // "redirect"
        try result.writer(allocator).print("$8\r\nredirect\r\n", .{});
        // redirect value
        try result.writer(allocator).print(":{d}\r\n", .{tracking_info.redirect});

        // "prefixes"
        try result.writer(allocator).print("$8\r\nprefixes\r\n", .{});
        // prefixes array
        try result.writer(allocator).print("*{d}\r\n", .{tracking_info.prefixes.len});
        for (tracking_info.prefixes) |prefix| {
            try result.writer(allocator).print("${d}\r\n{s}\r\n", .{ prefix.len, prefix });
        }
    }

    return result.toOwnedSlice(allocator);
}

/// CLIENT GETREDIR
/// Return the ID of the client we're redirecting tracking invalidation messages to.
/// Returns -1 if tracking is OFF or not using REDIRECT mode.
fn cmdClientGetredir(
    allocator: std.mem.Allocator,
    client_registry: *ClientRegistry,
    client_id: u64,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 1) {
        return w.writeError("ERR wrong number of arguments for 'client|getredir' command");
    }

    const tracking_info = (try client_registry.getTrackingInfo(client_id, allocator)) orelse {
        return w.writeError("ERR no such client");
    };
    defer {
        for (tracking_info.prefixes) |prefix| {
            allocator.free(prefix);
        }
        allocator.free(tracking_info.prefixes);
    }

    // Return -1 if tracking is disabled or redirect is 0 (self), otherwise return redirect ID
    const redir = if (!tracking_info.enabled or tracking_info.redirect == 0)
        @as(i64, -1)
    else
        tracking_info.redirect;

    return w.writeInteger(redir);
}

/// CLIENT CACHING YES|NO
/// Control tracking of keys in the next command (for OPTIN/OPTOUT modes).
fn cmdClientCaching(
    allocator: std.mem.Allocator,
    client_registry: *ClientRegistry,
    client_id: u64,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'client|caching' command");
    }

    const yes_no_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    const cache = if (std.ascii.eqlIgnoreCase(yes_no_str, "YES"))
        true
    else if (std.ascii.eqlIgnoreCase(yes_no_str, "NO"))
        false
    else
        return w.writeError("ERR syntax error");

    client_registry.setTrackingNextCache(client_id, cache);
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
        "UNBLOCK <client-id> [TIMEOUT|ERROR]",
        "    Unblock a client blocked in a blocking operation.",
        "    Reason: TIMEOUT (default, unblock as if timeout occurred) or ERROR (return error).",
        "NO-EVICT [ON|OFF]",
        "    Control whether client's keys are protected from eviction.",
        "    ON: Keys created by this client won't be evicted when maxmemory is reached.",
        "    OFF: Normal eviction behavior (default).",
        "    Returns current status if no argument provided.",
        "NO-TOUCH [ON|OFF]",
        "    Control whether commands sent by the client alter LRU/LFU stats.",
        "    ON: Client will not change LFU/LRU stats unless it sends TOUCH.",
        "    OFF: Client touches LFU/LRU stats like normal (default).",
        "    Returns current status if no argument provided.",
        "REPLY ON|OFF|SKIP",
        "    Control client reply behavior.",
        "    ON: Normal replies (default).",
        "    OFF: Suppress all replies until turned back ON.",
        "    SKIP: Skip reply for the next command only, then revert to ON.",
        "SETINFO LIB-NAME|LIB-VER <value>",
        "    Assign library name or version info to the connection.",
        "    LIB-NAME: Set client library name (e.g., redis-py).",
        "    LIB-VER: Set client library version (e.g., 4.5.1).",
        "    Value cannot contain spaces, newlines, or non-printable characters.",
        "TRACKING ON|OFF [REDIRECT client-id] [PREFIX prefix ...] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]",
        "    Enable/disable server-assisted client-side caching tracking.",
        "    ON: Enable tracking. OFF: Disable tracking.",
        "    REDIRECT <client-id>: Send invalidation messages to different client.",
        "    PREFIX <prefix>: Register key prefix for broadcasting mode (can be repeated).",
        "    BCAST: Enable broadcasting mode (notifications for all prefixes).",
        "    OPTIN: Don't track keys unless preceded by CLIENT CACHING yes.",
        "    OPTOUT: Track keys unless preceded by CLIENT CACHING no.",
        "    NOLOOP: Don't send notifications for keys modified by this connection.",
        "TRACKINGINFO",
        "    Return tracking status and configuration for this connection.",
        "GETREDIR",
        "    Return the client ID we're redirecting tracking invalidation messages to.",
        "    Returns -1 if tracking is OFF or not using REDIRECT mode.",
        "CACHING YES|NO",
        "    Control tracking for the next command (OPTIN/OPTOUT modes).",
        "    YES: Enable tracking for next command (OPTIN mode).",
        "    NO: Disable tracking for next command (OPTOUT mode).",
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
    blocking_queue: *@import("../storage/blocking.zig").BlockingQueue,
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
    } else if (std.ascii.eqlIgnoreCase(subcmd, "UNBLOCK")) {
        return cmdClientUnblock(allocator, blocking_queue, args);
    } else if (std.ascii.eqlIgnoreCase(subcmd, "NO-EVICT")) {
        return cmdClientNoEvict(allocator, registry, client_id, args);
    } else if (std.ascii.eqlIgnoreCase(subcmd, "NO-TOUCH")) {
        return cmdClientNoTouch(allocator, registry, client_id, args);
    } else if (std.ascii.eqlIgnoreCase(subcmd, "REPLY")) {
        return cmdClientReply(allocator, registry, client_id, args);
    } else if (std.ascii.eqlIgnoreCase(subcmd, "SETINFO")) {
        return cmdClientSetinfo(allocator, registry, client_id, args);
    } else if (std.ascii.eqlIgnoreCase(subcmd, "TRACKING")) {
        return cmdClientTracking(allocator, registry, client_id, args);
    } else if (std.ascii.eqlIgnoreCase(subcmd, "TRACKINGINFO")) {
        return cmdClientTrackinginfo(allocator, registry, client_id, args);
    } else if (std.ascii.eqlIgnoreCase(subcmd, "GETREDIR")) {
        return cmdClientGetredir(allocator, registry, client_id, args);
    } else if (std.ascii.eqlIgnoreCase(subcmd, "CACHING")) {
        return cmdClientCaching(allocator, registry, client_id, args);
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
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // Build command: CLIENT ID
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "ID" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Should return ":1\r\n"
    try std.testing.expectEqualStrings(":1\r\n", response);
}

test "CLIENT GETNAME command - no name set" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "GETNAME" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Should return null bulk string
    try std.testing.expectEqualStrings("$-1\r\n", response);
}

test "CLIENT SETNAME command - success" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "SETNAME" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "my-client" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
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
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "SETNAME" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "my client" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "spaces") != null);
}

test "CLIENT LIST command - basic" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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

    const response = try cmdClient(allocator, &registry, client1, args_slice, &blocking_queue);
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
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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

    const response = try cmdClient(allocator, &registry, 1, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Should return list with normal clients
    try std.testing.expect(std.mem.startsWith(u8, response, "$"));
    try std.testing.expect(std.mem.indexOf(u8, response, "id=1") != null);
}

test "CLIENT unknown subcommand" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "UNKNOWN" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "unknown subcommand") != null);
}

test "CLIENT LIST command - invalid TYPE" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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

    const response = try cmdClient(allocator, &registry, 1, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Should return error for unknown client type
    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "Unknown client type") != null);
}

test "ClientRegistry: set and get protocol version" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    // Should return RESP2 for non-existent client
    try std.testing.expectEqual(RespProtocol.RESP2, registry.getProtocol(999));
}

test "CLIENT INFO command" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "INFO" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
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
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "HELP" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
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
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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

    const response = try cmdClient(allocator, &registry, client1, args_slice, &blocking_queue);
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
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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

    const response = try cmdClient(allocator, &registry, client1, args_slice, &blocking_queue);
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
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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

    const response = try cmdClient(allocator, &registry, client2, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Should return :1\r\n
    try std.testing.expectEqualStrings(":1\r\n", response);
    try std.testing.expect(registry.isClientKilled(client1));
}

test "CLIENT KILL command - SKIPME YES" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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

    const response = try cmdClient(allocator, &registry, client1, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Should return :0\r\n (caller skipped)
    try std.testing.expectEqualStrings(":0\r\n", response);
    try std.testing.expect(!registry.isClientKilled(client1));
}

test "CLIENT KILL command - SKIPME NO" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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

    const response = try cmdClient(allocator, &registry, client1, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Should return :1\r\n (caller included)
    try std.testing.expectEqualStrings(":1\r\n", response);
    try std.testing.expect(registry.isClientKilled(client1));
}

test "CLIENT KILL command - by TYPE" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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

    const response = try cmdClient(allocator, &registry, client1, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Should return :2\r\n (both clients are normal type)
    try std.testing.expectEqualStrings(":2\r\n", response);
    try std.testing.expect(registry.isClientKilled(client1));
    try std.testing.expect(registry.isClientKilled(client2));
}

test "CLIENT KILL command - by MAXAGE" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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

    const response = try cmdClient(allocator, &registry, client1, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Should kill client older than 1 second
    try std.testing.expectEqualStrings(":1\r\n", response);
    try std.testing.expect(registry.isClientKilled(client1));
}

test "CLIENT KILL command - multiple filters" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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

    const response = try cmdClient(allocator, &registry, client1, args_slice, &blocking_queue);
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
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);

    // Check pause state
    try std.testing.expect(registry.isClientsPaused(true)); // Write commands paused
    try std.testing.expect(!registry.isClientsPaused(false)); // Read commands not paused
}

test "CLIENT PAUSE command - ALL mode" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);

    // Check pause state
    try std.testing.expect(registry.isClientsPaused(true)); // Write commands paused
    try std.testing.expect(registry.isClientsPaused(false)); // Read commands also paused
}

test "CLIENT PAUSE command - default WRITE mode" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "PAUSE" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "1000" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);

    // Check pause state (should default to WRITE mode)
    try std.testing.expect(registry.isClientsPaused(true)); // Write commands paused
    try std.testing.expect(!registry.isClientsPaused(false)); // Read commands not paused
}

test "CLIENT PAUSE command - zero timeout" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "PAUSE" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "0" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);

    // Zero timeout means pause expires immediately
    try std.testing.expect(!registry.isClientsPaused(true));
}

test "CLIENT PAUSE command - negative timeout rejected" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "PAUSE" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "-1" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "non-negative") != null);
}

test "CLIENT UNPAUSE command" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);

    // Check unpause state
    try std.testing.expect(!registry.isClientsPaused(true));
    try std.testing.expect(!registry.isClientsPaused(false));
}

test "CLIENT PAUSE command - pause expires" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
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

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "WRITE or ALL") != null);
}

test "CLIENT UNBLOCK command - client not blocked" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "UNBLOCK" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "999" }); // Non-existent client
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Should return 0 (client not found or not blocked)
    try std.testing.expectEqualStrings(":0\r\n", response);
}

test "CLIENT UNBLOCK command - default TIMEOUT mode" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // Create a blocked client
    const keys = try allocator.alloc([]const u8, 1);
    keys[0] = try allocator.dupe(u8, "stream1");
    const start_ids = try allocator.alloc(blocking_mod.StreamId, 1);
    start_ids[0] = blocking_mod.StreamId{ .ms = 0, .seq = 0 };

    const blocked_client = blocking_mod.BlockedClient{
        .client_id = 999, // Different client
        .keys = keys,
        .start_ids = start_ids,
        .count = null,
        .timeout_ms = 5000,
        .start_time = std.time.milliTimestamp(),
        .allocator = allocator,
    };

    try blocking_queue.enqueueXreadClient("stream1", blocked_client);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "UNBLOCK" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "999" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Should return 1 (client found and unblock requested)
    try std.testing.expectEqualStrings(":1\r\n", response);

    // Check that unblock request was set with TIMEOUT mode
    const mode = blocking_queue.checkUnblockRequest(999);
    try std.testing.expect(mode != null);
    try std.testing.expectEqual(blocking_mod.UnblockMode.timeout, mode.?);
}

test "CLIENT UNBLOCK command - ERROR mode" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // Create a blocked client
    const keys = try allocator.alloc([]const u8, 1);
    keys[0] = try allocator.dupe(u8, "stream1");
    const start_ids = try allocator.alloc(blocking_mod.StreamId, 1);
    start_ids[0] = blocking_mod.StreamId{ .ms = 0, .seq = 0 };

    const blocked_client = blocking_mod.BlockedClient{
        .client_id = 999,
        .keys = keys,
        .start_ids = start_ids,
        .count = null,
        .timeout_ms = 5000,
        .start_time = std.time.milliTimestamp(),
        .allocator = allocator,
    };

    try blocking_queue.enqueueXreadClient("stream1", blocked_client);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "UNBLOCK" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "999" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ERROR" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Should return 1 (client found and unblock requested)
    try std.testing.expectEqualStrings(":1\r\n", response);

    // Check that unblock request was set with ERROR mode
    const mode = blocking_queue.checkUnblockRequest(999);
    try std.testing.expect(mode != null);
    try std.testing.expectEqual(blocking_mod.UnblockMode.error_mode, mode.?);
}

test "CLIENT UNBLOCK command - invalid mode" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "UNBLOCK" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "999" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "INVALID" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Should return error
    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "TIMEOUT or ERROR") != null);
}

test "CLIENT UNBLOCK command - invalid client ID" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "UNBLOCK" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "not-a-number" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Should return error
    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "invalid client ID") != null);
}

test "CLIENT NO-EVICT command - enable" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    // Test NO-EVICT ON
    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "NO-EVICT" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.eql(u8, response, "+OK\r\n"));
    try std.testing.expect(registry.getNoEvict(client_id) == true);
}

test "CLIENT NO-EVICT command - disable" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // Set to ON first
    registry.setNoEvict(client_id, true);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    // Test NO-EVICT OFF
    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "NO-EVICT" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "OFF" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.eql(u8, response, "+OK\r\n"));
    try std.testing.expect(registry.getNoEvict(client_id) == false);
}

test "CLIENT NO-EVICT command - get status" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);
    registry.setNoEvict(client_id, true);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    // Test NO-EVICT without argument (get status)
    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "NO-EVICT" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.eql(u8, response, "+on\r\n"));
}

test "CLIENT REPLY command - ON mode" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "REPLY" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.eql(u8, response, "+OK\r\n"));
    try std.testing.expect(registry.getReplyMode(client_id) == .ON);
}

test "CLIENT REPLY command - OFF mode" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "REPLY" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "OFF" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.eql(u8, response, "+OK\r\n"));
    try std.testing.expect(registry.getReplyMode(client_id) == .OFF);
}

test "CLIENT REPLY command - SKIP mode" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "REPLY" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "SKIP" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.eql(u8, response, "+OK\r\n"));
    try std.testing.expect(registry.getReplyMode(client_id) == .SKIP);

    // Test that SKIP reverts to ON after processing
    registry.processReplySkip(client_id);
    try std.testing.expect(registry.getReplyMode(client_id) == .ON);
}

test "CLIENT REPLY command - invalid mode" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "REPLY" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "INVALID" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "CLIENT NO-TOUCH command - enable" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "NO-TOUCH" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
    try std.testing.expect(registry.getNoTouch(client_id) == true);
}

test "CLIENT NO-TOUCH command - disable" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // First enable it
    registry.setNoTouch(client_id, true);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "NO-TOUCH" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "OFF" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
    try std.testing.expect(registry.getNoTouch(client_id) == false);
}

test "CLIENT NO-TOUCH command - get status" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    // Test default status (OFF)
    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "NO-TOUCH" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+off\r\n", response);
}

test "CLIENT NO-TOUCH command - invalid argument" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "NO-TOUCH" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "INVALID" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "CLIENT SETINFO command - LIB-NAME" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "SETINFO" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "LIB-NAME" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "redis-py" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT SETINFO command - LIB-VER" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "SETINFO" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "LIB-VER" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "4.5.1" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
}

test "CLIENT SETINFO command - invalid attribute" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "SETINFO" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "INVALID" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "value" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "LIB-NAME") != null);
}

test "CLIENT SETINFO command - value with space (rejected)" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "SETINFO" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "LIB-NAME" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "redis py" }); // space not allowed
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "invalid characters") != null);
}

test "CLIENT SETINFO command - wrong number of arguments" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "SETINFO" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "LIB-NAME" });
    // Missing value
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null);
}

test "CLIENT TRACKING - enable and disable" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    // Enable tracking
    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "+OK"));

    // Disable tracking
    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();
    const arena_allocator2 = arena2.allocator();

    var args2 = std.ArrayList(RespValue){};
    try args2.append(arena_allocator2, RespValue{ .bulk_string = "TRACKING" });
    try args2.append(arena_allocator2, RespValue{ .bulk_string = "OFF" });
    const args_slice2 = try args2.toOwnedSlice(arena_allocator2);

    const response2 = try cmdClient(allocator, &registry, client_id, args_slice2, &blocking_queue);
    defer allocator.free(response2);

    try std.testing.expect(std.mem.startsWith(u8, response2, "+OK"));
}

test "CLIENT TRACKING - with OPTIN and OPTOUT" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    // Enable tracking with OPTIN
    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "OPTIN" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "+OK"));
}

test "CLIENT TRACKING - OPTIN and OPTOUT mutually exclusive" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    // Try to enable both OPTIN and OPTOUT
    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "OPTIN" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "OPTOUT" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "mutually exclusive") != null);
}

test "CLIENT TRACKING - with PREFIX in BCAST mode" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    // Enable tracking with BCAST and PREFIX
    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "BCAST" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "PREFIX" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "user:" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "PREFIX" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "product:" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "+OK"));
}

test "CLIENT TRACKINGINFO - basic functionality" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // Enable tracking first
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Get tracking info
    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();
    const arena_allocator2 = arena2.allocator();

    var args2 = std.ArrayList(RespValue){};
    try args2.append(arena_allocator2, RespValue{ .bulk_string = "TRACKINGINFO" });
    const args_slice2 = try args2.toOwnedSlice(arena_allocator2);

    const response2 = try cmdClient(allocator, &registry, client_id, args_slice2, &blocking_queue);
    defer allocator.free(response2);

    // Should contain flags, redirect, and prefixes
    try std.testing.expect(std.mem.indexOf(u8, response2, "flags") != null);
    try std.testing.expect(std.mem.indexOf(u8, response2, "redirect") != null);
    try std.testing.expect(std.mem.indexOf(u8, response2, "prefixes") != null);
}

test "CLIENT TRACKINGINFO - with OPTIN mode" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // Enable tracking with OPTIN
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "OPTIN" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Get tracking info
    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();
    const arena_allocator2 = arena2.allocator();

    var args2 = std.ArrayList(RespValue){};
    try args2.append(arena_allocator2, RespValue{ .bulk_string = "TRACKINGINFO" });
    const args_slice2 = try args2.toOwnedSlice(arena_allocator2);

    const response2 = try cmdClient(allocator, &registry, client_id, args_slice2, &blocking_queue);
    defer allocator.free(response2);

    // Should contain "optin" flag
    try std.testing.expect(std.mem.indexOf(u8, response2, "optin") != null);
}

test "CLIENT GETREDIR - returns -1 when tracking disabled" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "GETREDIR" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Should return -1 (tracking disabled)
    try std.testing.expectEqualStrings(":-1\r\n", response);
}

test "CLIENT GETREDIR - returns redirect client ID when enabled" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);
    const redirect_id = try registry.registerClient("127.0.0.1:54321", 43);

    // Enable tracking with REDIRECT
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args_tracking = std.ArrayList(RespValue){};
    try args_tracking.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
    try args_tracking.append(arena_allocator, RespValue{ .bulk_string = "ON" });
    try args_tracking.append(arena_allocator, RespValue{ .bulk_string = "REDIRECT" });
    var buf: [32]u8 = undefined;
    const redirect_str = try std.fmt.bufPrint(&buf, "{d}", .{redirect_id});
    try args_tracking.append(arena_allocator, RespValue{ .bulk_string = redirect_str });
    const tracking_args = try args_tracking.toOwnedSlice(arena_allocator);

    const tracking_response = try cmdClient(allocator, &registry, client_id, tracking_args, &blocking_queue);
    defer allocator.free(tracking_response);

    // Call GETREDIR
    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();
    const arena_allocator2 = arena2.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator2, RespValue{ .bulk_string = "GETREDIR" });
    const args_slice = try args.toOwnedSlice(arena_allocator2);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Should return redirect_id
    var expected_buf: [32]u8 = undefined;
    const expected = try std.fmt.bufPrint(&expected_buf, ":{d}\r\n", .{redirect_id});
    try std.testing.expectEqualStrings(expected, response);
}

test "CLIENT GETREDIR - returns -1 when redirect is 0 (self)" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // Enable tracking without REDIRECT (defaults to self)
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args_tracking = std.ArrayList(RespValue){};
    try args_tracking.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
    try args_tracking.append(arena_allocator, RespValue{ .bulk_string = "ON" });
    const tracking_args = try args_tracking.toOwnedSlice(arena_allocator);

    const tracking_response = try cmdClient(allocator, &registry, client_id, tracking_args, &blocking_queue);
    defer allocator.free(tracking_response);

    // Call GETREDIR
    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();
    const arena_allocator2 = arena2.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator2, RespValue{ .bulk_string = "GETREDIR" });
    const args_slice = try args.toOwnedSlice(arena_allocator2);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Should return -1 (redirect is 0 = self)
    try std.testing.expectEqualStrings(":-1\r\n", response);
}

test "CLIENT CACHING - YES and NO" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    // CLIENT CACHING YES
    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "CACHING" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "YES" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "+OK"));

    // CLIENT CACHING NO
    var arena2 = std.heap.ArenaAllocator.init(allocator);
    defer arena2.deinit();
    const arena_allocator2 = arena2.allocator();

    var args2 = std.ArrayList(RespValue){};
    try args2.append(arena_allocator2, RespValue{ .bulk_string = "CACHING" });
    try args2.append(arena_allocator2, RespValue{ .bulk_string = "NO" });
    const args_slice2 = try args2.toOwnedSlice(arena_allocator2);

    const response2 = try cmdClient(allocator, &registry, client_id, args_slice2, &blocking_queue);
    defer allocator.free(response2);

    try std.testing.expect(std.mem.startsWith(u8, response2, "+OK"));
}

test "CLIENT CACHING - invalid argument" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    // CLIENT CACHING INVALID
    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "CACHING" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "INVALID" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "syntax error") != null);
}

test "CLIENT TRACKING - invalid redirect client" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    // Try to redirect to non-existent client 999
    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "REDIRECT" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "999" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "invalid redirect") != null);
}

test "CLIENT TRACKING - valid redirect to another client" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client1 = try registry.registerClient("127.0.0.1:12345", 42);
    const client2 = try registry.registerClient("127.0.0.1:12346", 43);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    // Enable tracking with REDIRECT to client2
    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "REDIRECT" });
    const redirect_str = try std.fmt.allocPrint(arena_allocator, "{d}", .{client2});
    try args.append(arena_allocator, RespValue{ .bulk_string = redirect_str });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client1, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "+OK"));
}

test "CLIENT TRACKING - with NOLOOP flag" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    // Enable tracking with NOLOOP
    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "NOLOOP" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "+OK"));
}

test "CLIENT TRACKING - missing ON/OFF argument" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    // Call TRACKING without ON/OFF
    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "wrong number of arguments") != null);
}

test "CLIENT TRACKING - combination BCAST NOLOOP PREFIX" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    // Enable tracking with multiple options
    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "BCAST" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "NOLOOP" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "PREFIX" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "user:" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "+OK"));
}

test "CLIENT TRACKING - OPTIN with NOLOOP" {
    const allocator = std.testing.allocator;

    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    // Enable tracking with OPTIN and NOLOOP
    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "OPTIN" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "NOLOOP" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "+OK"));
}

test "ClientRegistry - setMonitorMode and isMonitoring" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:54321", 10);

    // Initially not monitoring
    try std.testing.expect(!registry.isMonitoring(client_id));

    // Enable monitoring
    registry.setMonitorMode(client_id, true);
    try std.testing.expect(registry.isMonitoring(client_id));

    // Disable monitoring
    registry.setMonitorMode(client_id, false);
    try std.testing.expect(!registry.isMonitoring(client_id));
}

test "ClientRegistry - getMonitoringClients" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client1 = try registry.registerClient("127.0.0.1:1", 10);
    _ = try registry.registerClient("127.0.0.1:2", 11);
    const client3 = try registry.registerClient("127.0.0.1:3", 12);

    // Enable monitoring for client1 and client3
    registry.setMonitorMode(client1, true);
    registry.setMonitorMode(client3, true);

    const monitors = try registry.getMonitoringClients(allocator);
    defer allocator.free(monitors);

    // Should have 2 monitoring clients
    try std.testing.expectEqual(@as(usize, 2), monitors.len);

    // Check that client1 and client3 are in the list
    var found_client1 = false;
    var found_client3 = false;
    for (monitors) |id| {
        if (id == client1) found_client1 = true;
        if (id == client3) found_client3 = true;
    }
    try std.testing.expect(found_client1);
    try std.testing.expect(found_client3);
}

test "ClientRegistry - broadcastToMonitors" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    _ = try registry.registerClient("127.0.0.1:1", 10);
    const client2 = try registry.registerClient("127.0.0.1:2", 11);

    // Enable monitoring for client2
    registry.setMonitorMode(client2, true);

    const cmd_args = [_][]const u8{ "SET", "key", "value" };
    const messages = try registry.broadcastToMonitors(
        allocator,
        1234567890, // timestamp_sec
        123456, // timestamp_usec
        0, // db
        "127.0.0.1:54321", // client_addr
        &cmd_args,
    );
    defer {
        for (messages.items) |msg| {
            allocator.free(msg.message);
        }
        messages.deinit(allocator);
    }

    // Should have 1 message (only client2 is monitoring)
    try std.testing.expectEqual(@as(usize, 1), messages.items.len);
    try std.testing.expectEqual(client2, messages.items[0].client_id);

    // Check message format: +timestamp.usec [db addr] "cmd" "arg1" "arg2"
    const msg = messages.items[0].message;
    try std.testing.expect(std.mem.startsWith(u8, msg, "+1234567890.123456 [0 127.0.0.1:54321] \"SET\" \"key\" \"value\"\r\n"));
}

test "ClientRegistry - broadcastToMonitors with quote escaping" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client1 = try registry.registerClient("127.0.0.1:1", 10);
    registry.setMonitorMode(client1, true);

    const cmd_args = [_][]const u8{ "SET", "key", "val\"ue" };
    const messages = try registry.broadcastToMonitors(
        allocator,
        1234567890,
        123456,
        0,
        "127.0.0.1:54321",
        &cmd_args,
    );
    defer {
        for (messages.items) |msg| {
            allocator.free(msg.message);
        }
        messages.deinit(allocator);
    }

    // Check that quotes are escaped
    const msg = messages.items[0].message;
    try std.testing.expect(std.mem.indexOf(u8, msg, "val\\\"ue") != null);
}

test "ClientRegistry - queuePushMessage and takePendingInvalidations" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:1111", 1);
    defer registry.unregisterClient(client_id);

    // No pending messages initially
    const none = registry.takePendingInvalidations(client_id);
    try std.testing.expectEqual(@as(?[][]u8, null), none);

    // Queue a message
    const raw_msg = try allocator.dupe(u8, ">2\r\n$10\r\ninvalidate\r\n*1\r\n$3\r\nfoo\r\n");
    try registry.queuePushMessage(client_id, raw_msg);

    // Take pending messages
    const pending = registry.takePendingInvalidations(client_id);
    try std.testing.expect(pending != null);
    if (pending) |msgs| {
        defer allocator.free(msgs);
        try std.testing.expectEqual(@as(usize, 1), msgs.len);
        defer allocator.free(msgs[0]);
        try std.testing.expectEqualStrings(">2\r\n$10\r\ninvalidate\r\n*1\r\n$3\r\nfoo\r\n", msgs[0]);
    }

    // Queue is now empty
    const none2 = registry.takePendingInvalidations(client_id);
    try std.testing.expectEqual(@as(?[][]u8, null), none2);
}

test "ClientRegistry - queuePushMessage for non-existent client frees message" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    // Queue message for a non-existent client — should free without leak
    const raw_msg = try allocator.dupe(u8, ">2\r\n$10\r\ninvalidate\r\n*0\r\n");
    try registry.queuePushMessage(9999, raw_msg);
    // Testing allocator will catch if raw_msg is leaked
}

test "ClientRegistry - multiple pending invalidations drained in order" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:2222", 2);
    defer registry.unregisterClient(client_id);

    const msg1 = try allocator.dupe(u8, "msg1");
    const msg2 = try allocator.dupe(u8, "msg2");
    try registry.queuePushMessage(client_id, msg1);
    try registry.queuePushMessage(client_id, msg2);

    const pending = registry.takePendingInvalidations(client_id);
    try std.testing.expect(pending != null);
    if (pending) |msgs| {
        defer allocator.free(msgs);
        try std.testing.expectEqual(@as(usize, 2), msgs.len);
        defer allocator.free(msgs[0]);
        defer allocator.free(msgs[1]);
        try std.testing.expectEqualStrings("msg1", msgs[0]);
        try std.testing.expectEqualStrings("msg2", msgs[1]);
    }
}

test "CLIENT LIST - includes resp field" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);
    registry.setProtocol(client_id, .RESP3);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var args = std.ArrayList(RespValue){};
    try args.append(arena.allocator(), RespValue{ .bulk_string = "LIST" });
    const args_slice = try args.toOwnedSlice(arena.allocator());

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "resp=3") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "ssub=0") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "multi=-1") != null);
}

test "CLIENT LIST - includes user and library fields" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);
    try registry.setLibName(client_id, "redis-py");
    try registry.setLibVer(client_id, "4.5.1");

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var args = std.ArrayList(RespValue){};
    try args.append(arena.allocator(), RespValue{ .bulk_string = "LIST" });
    const args_slice = try args.toOwnedSlice(arena.allocator());

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "library-name=redis-py") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "library-ver=4.5.1") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "user=default") != null);
}

test "CLIENT INFO - includes library fields after SETINFO" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    // Set lib-name
    var set_args = std.ArrayList(RespValue){};
    try set_args.append(arena.allocator(), RespValue{ .bulk_string = "SETINFO" });
    try set_args.append(arena.allocator(), RespValue{ .bulk_string = "LIB-NAME" });
    try set_args.append(arena.allocator(), RespValue{ .bulk_string = "ioredis" });
    const set_slice = try set_args.toOwnedSlice(arena.allocator());
    const set_resp = try cmdClient(allocator, &registry, client_id, set_slice, &blocking_queue);
    defer allocator.free(set_resp);

    // Get CLIENT INFO
    var info_args = std.ArrayList(RespValue){};
    try info_args.append(arena.allocator(), RespValue{ .bulk_string = "INFO" });
    const info_slice = try info_args.toOwnedSlice(arena.allocator());

    const response = try cmdClient(allocator, &registry, client_id, info_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "library-name=ioredis") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "library-ver=") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "resp=2") != null);
}

test "CLIENT LIST - redir=-1 when tracking disabled" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var args = std.ArrayList(RespValue){};
    try args.append(arena.allocator(), RespValue{ .bulk_string = "LIST" });
    const args_slice = try args.toOwnedSlice(arena.allocator());

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "redir=-1") != null);
}

test "CLIENT LIST ID filter - returns only specified clients" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client1 = try registry.registerClient("127.0.0.1:11111", 41);
    const client2 = try registry.registerClient("127.0.0.1:22222", 42);
    const client3 = try registry.registerClient("127.0.0.1:33333", 43);
    _ = client3;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var id1_buf: [20]u8 = undefined;
    var id2_buf: [20]u8 = undefined;
    const id1_str = std.fmt.bufPrint(&id1_buf, "{d}", .{client1}) catch unreachable;
    const id2_str = std.fmt.bufPrint(&id2_buf, "{d}", .{client2}) catch unreachable;

    var args = std.ArrayList(RespValue){};
    try args.append(arena.allocator(), RespValue{ .bulk_string = "LIST" });
    try args.append(arena.allocator(), RespValue{ .bulk_string = "ID" });
    try args.append(arena.allocator(), RespValue{ .bulk_string = id1_str });
    try args.append(arena.allocator(), RespValue{ .bulk_string = id2_str });
    const args_slice = try args.toOwnedSlice(arena.allocator());

    const response = try cmdClient(allocator, &registry, client1, args_slice, &blocking_queue);
    defer allocator.free(response);

    // Should include client1 and client2 but NOT client3
    try std.testing.expect(std.mem.indexOf(u8, response, "127.0.0.1:11111") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "127.0.0.1:22222") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "127.0.0.1:33333") == null);
}

test "CLIENT LIST - includes watch=0 and type=normal fields (Redis 7.x compat)" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:55555", 55);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var args = std.ArrayList(RespValue){};
    try args.append(arena.allocator(), RespValue{ .bulk_string = "LIST" });
    const args_slice = try args.toOwnedSlice(arena.allocator());

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "watch=0") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "type=normal") != null);
}

test "CLIENT INFO - includes watch=0 and type=normal fields (Redis 7.x compat)" {
    const allocator = std.testing.allocator;
    var registry = ClientRegistry.init(allocator);
    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:66666", 66);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var args = std.ArrayList(RespValue){};
    try args.append(arena.allocator(), RespValue{ .bulk_string = "INFO" });
    const args_slice = try args.toOwnedSlice(arena.allocator());

    const response = try cmdClient(allocator, &registry, client_id, args_slice, &blocking_queue);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "watch=0") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "type=normal") != null);
}
