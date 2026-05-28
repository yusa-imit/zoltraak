const std = @import("std");
const glob = @import("../utils/glob.zig");

/// Parse a memory size value with optional unit suffix.
/// Supports: plain integers, and suffixes b, k/kb, m/mb, g/gb, t/tb (case-insensitive).
/// Returns bytes as i64. Returns error.InvalidValue for unparseable input.
pub fn parseMemoryValue(s: []const u8) !i64 {
    if (s.len == 0) return error.InvalidValue;
    const lower = blk: {
        var buf: [64]u8 = undefined;
        if (s.len > buf.len) break :blk s;
        break :blk std.ascii.lowerString(&buf, s);
    };
    // Find where digits end and suffix begins
    var i: usize = 0;
    if (i < lower.len and (lower[i] == '-' or lower[i] == '+')) i += 1;
    while (i < lower.len and lower[i] >= '0' and lower[i] <= '9') i += 1;
    const num_part = lower[0..i];
    const suf_part = lower[i..];
    const base = std.fmt.parseInt(i64, num_part, 10) catch return error.InvalidValue;
    const multiplier: i64 = if (suf_part.len == 0 or std.mem.eql(u8, suf_part, "b"))
        1
    else if (std.mem.eql(u8, suf_part, "k") or std.mem.eql(u8, suf_part, "kb"))
        1024
    else if (std.mem.eql(u8, suf_part, "m") or std.mem.eql(u8, suf_part, "mb"))
        1024 * 1024
    else if (std.mem.eql(u8, suf_part, "g") or std.mem.eql(u8, suf_part, "gb"))
        1024 * 1024 * 1024
    else if (std.mem.eql(u8, suf_part, "t") or std.mem.eql(u8, suf_part, "tb"))
        1024 * 1024 * 1024 * 1024
    else
        return error.InvalidValue;
    return std.math.mul(i64, base, multiplier) catch return error.InvalidValue;
}

/// Configuration parameter value types
pub const ConfigValue = union(enum) {
    string: []const u8,
    int: i64,
    bool: bool,

    pub fn deinit(self: *ConfigValue, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .string => |s| allocator.free(s),
            .int, .bool => {},
        }
    }

    pub fn clone(self: ConfigValue, allocator: std.mem.Allocator) !ConfigValue {
        return switch (self) {
            .string => |s| ConfigValue{ .string = try allocator.dupe(u8, s) },
            .int => |i| ConfigValue{ .int = i },
            .bool => |b| ConfigValue{ .bool = b },
        };
    }

    /// Format value as string for CONFIG GET response
    pub fn format(self: ConfigValue, allocator: std.mem.Allocator) ![]const u8 {
        return switch (self) {
            .string => |s| try allocator.dupe(u8, s),
            .int => |i| try std.fmt.allocPrint(allocator, "{d}", .{i}),
            .bool => |b| try allocator.dupe(u8, if (b) "yes" else "no"),
        };
    }
};

/// Configuration parameter metadata
pub const ConfigParam = struct {
    name: []const u8,
    default_value: ConfigValue,
    read_only: bool,

    pub fn deinit(self: *ConfigParam, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        var val = self.default_value;
        val.deinit(allocator);
    }
};

/// Runtime configuration for Zoltraak server
/// Thread-safe configuration management with parameter validation
pub const Config = struct {
    allocator: std.mem.Allocator,
    /// Parameter name (case-insensitive) -> current value
    params: std.StringHashMap(ConfigValue),
    /// Parameter metadata (name, default, read-only flag)
    metadata: std.ArrayList(ConfigParam),
    mutex: std.Thread.Mutex,

    /// Initialize configuration with Redis-compatible defaults
    pub fn init(allocator: std.mem.Allocator, port: u16, bind: []const u8) !*Config {
        const config = try allocator.create(Config);
        errdefer allocator.destroy(config);

        config.* = Config{
            .allocator = allocator,
            .params = std.StringHashMap(ConfigValue).init(allocator),
            .metadata = std.ArrayList(ConfigParam){},
            .mutex = std.Thread.Mutex{},
        };

        errdefer {
            config.params.deinit();
            for (config.metadata.items) |*meta| {
                meta.deinit(allocator);
            }
            config.metadata.deinit(allocator);
        }

        // Define default parameters
        const defaults = [_]struct {
            name: []const u8,
            value: ConfigValue,
            read_only: bool,
        }{
            // Memory management
            .{ .name = "maxmemory", .value = .{ .int = 0 }, .read_only = false }, // 0 = unlimited
            .{ .name = "maxmemory-policy", .value = .{ .string = "noeviction" }, .read_only = false },
            .{ .name = "maxmemory-samples", .value = .{ .int = 5 }, .read_only = false }, // Sample size for eviction (1-10)
            .{ .name = "lfu-log-factor", .value = .{ .int = 10 }, .read_only = false }, // LFU log factor (0-255)
            .{ .name = "lfu-decay-time", .value = .{ .int = 1 }, .read_only = false }, // LFU decay time in minutes (0-max)

            // Networking
            .{ .name = "timeout", .value = .{ .int = 0 }, .read_only = false }, // 0 = no timeout
            .{ .name = "tcp-keepalive", .value = .{ .int = 300 }, .read_only = false },
            .{ .name = "port", .value = .{ .int = @as(i64, port) }, .read_only = true },
            .{ .name = "bind", .value = .{ .string = bind }, .read_only = true },

            // Persistence
            .{ .name = "save", .value = .{ .string = "900 1 300 10 60 10000" }, .read_only = false },
            .{ .name = "appendonly", .value = .{ .bool = false }, .read_only = false },
            .{ .name = "appendfsync", .value = .{ .string = "everysec" }, .read_only = false },

            // Database
            .{ .name = "databases", .value = .{ .int = 16 }, .read_only = true }, // Default: 16 databases

            // Slowlog
            .{ .name = "slowlog-log-slower-than", .value = .{ .int = 10000 }, .read_only = false }, // microseconds, 10ms default
            .{ .name = "slowlog-max-len", .value = .{ .int = 128 }, .read_only = false }, // max entries in slowlog

            // Lua scripting
            .{ .name = "lua-time-limit", .value = .{ .int = 5000 }, .read_only = false }, // milliseconds, 5s default

            // Keyspace notifications
            .{ .name = "notify-keyspace-events", .value = .{ .string = "" }, .read_only = false }, // disabled by default

            // Lazy freeing
            .{ .name = "lazyfree-lazy-eviction", .value = .{ .bool = false }, .read_only = false }, // lazy free on eviction
            .{ .name = "lazyfree-lazy-expire", .value = .{ .bool = false }, .read_only = false }, // lazy free on expire
            .{ .name = "lazyfree-lazy-server-del", .value = .{ .bool = false }, .read_only = false }, // lazy free on implicit deletes (RENAME, etc.)
            .{ .name = "lazyfree-lazy-user-del", .value = .{ .bool = false }, .read_only = false }, // lazy free on DEL (not UNLINK)
            .{ .name = "lazyfree-lazy-user-flush", .value = .{ .bool = false }, .read_only = false }, // lazy free on FLUSHALL/FLUSHDB default
            .{ .name = "replica-lazy-flush", .value = .{ .bool = false }, .read_only = false }, // lazy flush on full resync

            // Active defragmentation
            .{ .name = "activedefrag", .value = .{ .bool = false }, .read_only = false }, // enable/disable active defragmentation
            .{ .name = "activedefrag-cycle-min", .value = .{ .int = 1 }, .read_only = false }, // min CPU % for defrag (1-75)
            .{ .name = "activedefrag-cycle-max", .value = .{ .int = 25 }, .read_only = false }, // max CPU % for defrag (1-75)
            .{ .name = "activedefrag-threshold-lower", .value = .{ .int = 10 }, .read_only = false }, // min fragmentation % to start defrag
            .{ .name = "activedefrag-threshold-upper", .value = .{ .int = 100 }, .read_only = false }, // max fragmentation % for aggressive defrag

            // Internal encoding optimizations (Redis 8.x defaults)
            .{ .name = "hash-max-listpack-entries", .value = .{ .int = 128 }, .read_only = false }, // max hash entries for listpack encoding
            .{ .name = "hash-max-listpack-value", .value = .{ .int = 64 }, .read_only = false }, // max hash field/value size (bytes) for listpack
            .{ .name = "list-max-listpack-entries", .value = .{ .int = 128 }, .read_only = false }, // max list entries for listpack encoding
            .{ .name = "list-max-listpack-value", .value = .{ .int = 64 }, .read_only = false }, // max list element size (bytes) for listpack
            .{ .name = "list-max-listpack-size", .value = .{ .int = -2 }, .read_only = false }, // max listpack node size (-2 = 8kb)
            .{ .name = "zset-max-listpack-entries", .value = .{ .int = 128 }, .read_only = false }, // max sorted set entries for listpack encoding
            .{ .name = "zset-max-listpack-value", .value = .{ .int = 64 }, .read_only = false }, // max sorted set member size (bytes) for listpack
            .{ .name = "set-max-intset-entries", .value = .{ .int = 512 }, .read_only = false }, // max set entries for intset encoding (integer-only)
            .{ .name = "set-max-listpack-entries", .value = .{ .int = 128 }, .read_only = false }, // max set entries for listpack encoding
            .{ .name = "set-max-listpack-value", .value = .{ .int = 64 }, .read_only = false }, // max set member size (bytes) for listpack
            // Deprecated ziplist aliases (kept for backward compatibility)
            .{ .name = "hash-max-ziplist-entries", .value = .{ .int = 128 }, .read_only = false }, // alias for hash-max-listpack-entries
            .{ .name = "hash-max-ziplist-value", .value = .{ .int = 64 }, .read_only = false }, // alias for hash-max-listpack-value
            .{ .name = "zset-max-ziplist-entries", .value = .{ .int = 128 }, .read_only = false }, // alias for zset-max-listpack-entries
            .{ .name = "zset-max-ziplist-value", .value = .{ .int = 64 }, .read_only = false }, // alias for zset-max-listpack-value

            // TLS/SSL configuration (Phase 10) - These are stored in Storage.tls_config but exposed via CONFIG GET/SET
            .{ .name = "tls-port", .value = .{ .int = 0 }, .read_only = false }, // TLS port (0 = disabled)
            .{ .name = "tls-cert-file", .value = .{ .string = "" }, .read_only = false }, // TLS certificate file path
            .{ .name = "tls-key-file", .value = .{ .string = "" }, .read_only = false }, // TLS private key file path
            .{ .name = "tls-key-file-pass", .value = .{ .string = "" }, .read_only = false }, // TLS key password
            .{ .name = "tls-ca-cert-file", .value = .{ .string = "" }, .read_only = false }, // CA certificate file
            .{ .name = "tls-ca-cert-dir", .value = .{ .string = "" }, .read_only = false }, // CA certificate directory
            .{ .name = "tls-auth-clients", .value = .{ .string = "yes" }, .read_only = false }, // Client authentication (yes/no/optional)
            .{ .name = "tls-protocols", .value = .{ .string = "TLSv1.2 TLSv1.3" }, .read_only = false }, // TLS protocol versions
            .{ .name = "tls-ciphers", .value = .{ .string = "" }, .read_only = false }, // TLS 1.2 cipher suites
            .{ .name = "tls-ciphersuites", .value = .{ .string = "" }, .read_only = false }, // TLS 1.3 cipher suites
            .{ .name = "tls-prefer-server-ciphers", .value = .{ .bool = true }, .read_only = false }, // Prefer server cipher order
            .{ .name = "tls-session-caching", .value = .{ .bool = true }, .read_only = false }, // Enable session caching
            .{ .name = "tls-session-cache-size", .value = .{ .int = 20480 }, .read_only = false }, // Session cache size
            .{ .name = "tls-session-cache-timeout", .value = .{ .int = 300 }, .read_only = false }, // Session cache timeout (seconds)
            .{ .name = "tls-cluster", .value = .{ .bool = false }, .read_only = false }, // Enable TLS for cluster bus
            .{ .name = "tls-replication", .value = .{ .bool = false }, .read_only = false }, // Enable TLS for replication
            .{ .name = "tls-client-cert-file", .value = .{ .string = "" }, .read_only = false }, // Client cert file (outbound)
            .{ .name = "tls-client-key-file", .value = .{ .string = "" }, .read_only = false }, // Client key file (outbound)
            .{ .name = "tls-client-key-file-pass", .value = .{ .string = "" }, .read_only = false }, // Client key password
            .{ .name = "tls-allowlisted-certs", .value = .{ .string = "" }, .read_only = false }, // Allowlisted cert paths (Redis 7.2+)

            // Server performance tuning (commonly used by clients and monitoring tools)
            .{ .name = "hz", .value = .{ .int = 10 }, .read_only = false }, // Server background task frequency (1-500)
            .{ .name = "dynamic-hz", .value = .{ .bool = true }, .read_only = false }, // Adaptive hz based on client count
            .{ .name = "aof-use-rdb-preamble", .value = .{ .bool = true }, .read_only = false }, // Use RDB preamble for AOF
            .{ .name = "activerehashing", .value = .{ .bool = true }, .read_only = false }, // Enable active rehashing of the main dictionaries
            .{ .name = "no-appendfsync-on-rewrite", .value = .{ .bool = false }, .read_only = false }, // Don't fsync during BGSAVE/BGREWRITEAOF
            .{ .name = "latency-tracking", .value = .{ .bool = true }, .read_only = false }, // Enable latency tracking
            .{ .name = "latency-tracking-info-percentiles", .value = .{ .string = "50 99 99.9" }, .read_only = false }, // Percentiles for latency tracking
            .{ .name = "proto-max-bulk-len", .value = .{ .int = 536870912 }, .read_only = false }, // Max bulk string length (512mb default)
            .{ .name = "client-query-buffer-limit", .value = .{ .int = 1073741824 }, .read_only = false }, // Client query buffer limit (1gb default)
            .{ .name = "maxclients", .value = .{ .int = 10000 }, .read_only = false }, // Max number of connected clients
            .{ .name = "tcp-backlog", .value = .{ .int = 511 }, .read_only = false }, // TCP listen backlog
            .{ .name = "bind-source-addr", .value = .{ .string = "" }, .read_only = false }, // Source bind address
            .{ .name = "repl-backlog-size", .value = .{ .int = 1048576 }, .read_only = false }, // Replication backlog size (1mb default)
            .{ .name = "repl-backlog-ttl", .value = .{ .int = 3600 }, .read_only = false }, // Replication backlog TTL (seconds)
            .{ .name = "min-replicas-to-write", .value = .{ .int = 0 }, .read_only = false }, // Min replicas for writes (0 = disabled)
            .{ .name = "min-slaves-to-write", .value = .{ .int = 0 }, .read_only = false }, // Alias for min-replicas-to-write
            .{ .name = "min-replicas-max-lag", .value = .{ .int = 10 }, .read_only = false }, // Max lag for replica writes (seconds)
            .{ .name = "min-slaves-max-lag", .value = .{ .int = 10 }, .read_only = false }, // Alias for min-replicas-max-lag
            .{ .name = "list-max-ziplist-size", .value = .{ .int = -2 }, .read_only = false }, // Alias for list-max-listpack-size
            .{ .name = "close-on-slave-write", .value = .{ .bool = true }, .read_only = false }, // Close on replica write (legacy param)
            .{ .name = "repl-diskless-sync", .value = .{ .bool = true }, .read_only = false }, // Diskless replication sync
            .{ .name = "repl-diskless-sync-delay", .value = .{ .int = 5 }, .read_only = false }, // Delay before diskless sync (seconds)
            .{ .name = "repl-diskless-sync-max-replicas", .value = .{ .int = 0 }, .read_only = false }, // Max replicas for diskless sync
            .{ .name = "repl-diskless-load", .value = .{ .string = "disabled" }, .read_only = false }, // Diskless replica load mode
            .{ .name = "repl-timeout", .value = .{ .int = 60 }, .read_only = false }, // Replication timeout (seconds)
            .{ .name = "repl-ping-replica-period", .value = .{ .int = 10 }, .read_only = false }, // Replica ping period (seconds)
            .{ .name = "repl-ping-slave-period", .value = .{ .int = 10 }, .read_only = false }, // Alias for repl-ping-replica-period
            .{ .name = "logfile", .value = .{ .string = "" }, .read_only = false }, // Log file path (empty = stdout)
            .{ .name = "loglevel", .value = .{ .string = "notice" }, .read_only = false }, // Log level (debug/verbose/notice/warning)
            .{ .name = "syslog-enabled", .value = .{ .bool = false }, .read_only = false }, // Enable syslog
            .{ .name = "crash-log-enabled", .value = .{ .bool = true }, .read_only = false }, // Enable crash log
            .{ .name = "crash-memlog-enabled", .value = .{ .bool = true }, .read_only = false }, // Enable memory log on crash
            .{ .name = "use-exit-on-panic", .value = .{ .bool = false }, .read_only = false }, // Use exit() on panic instead of abort()
            .{ .name = "disable-thp", .value = .{ .bool = true }, .read_only = false }, // Disable Transparent Huge Pages
            .{ .name = "lua-replicate-commands", .value = .{ .bool = true }, .read_only = false }, // Replicate Lua script effects
            .{ .name = "lazyfree-lazy-expire-on-snapshot", .value = .{ .bool = false }, .read_only = false }, // Lazy expire during snapshot
            .{ .name = "tracking-table-max-keys", .value = .{ .int = 0 }, .read_only = false }, // Max keys in client tracking table (0 = unlimited)
            .{ .name = "rdb-save-incremental-fsync", .value = .{ .bool = true }, .read_only = false }, // Incremental fsync during RDB save
            .{ .name = "aof-rewrite-incremental-fsync", .value = .{ .bool = true }, .read_only = false }, // Incremental fsync during AOF rewrite
            .{ .name = "jemalloc-bg-thread", .value = .{ .bool = true }, .read_only = false }, // Enable jemalloc background thread
            .{ .name = "io-threads", .value = .{ .int = 1 }, .read_only = false }, // Number of I/O threads
            .{ .name = "io-threads-do-reads", .value = .{ .bool = false }, .read_only = false }, // Allow I/O threads to read
        };

        for (defaults) |def| {
            const name_lower = try std.ascii.allocLowerString(allocator, def.name);
            errdefer allocator.free(name_lower);

            const value = try def.value.clone(allocator);
            errdefer {
                var v = value;
                v.deinit(allocator);
            }

            try config.params.put(name_lower, value);

            // Create a separate copy of name_lower for metadata
            const meta_name = try allocator.dupe(u8, name_lower);
            errdefer allocator.free(meta_name);

            const meta = ConfigParam{
                .name = meta_name,
                .default_value = try def.value.clone(allocator),
                .read_only = def.read_only,
            };
            try config.metadata.append(allocator, meta);
        }

        return config;
    }

    /// Deinitialize configuration and free all memory
    pub fn deinit(self: *Config) void {
        self.mutex.lock();

        // Free parameter values
        var it = self.params.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var value = entry.value_ptr.*;
            value.deinit(self.allocator);
        }
        self.params.deinit();

        // Free metadata
        for (self.metadata.items) |*meta| {
            meta.deinit(self.allocator);
        }
        self.metadata.deinit(self.allocator);

        const allocator = self.allocator;
        self.mutex.unlock();
        allocator.destroy(self);
    }

    /// Get configuration parameter value as typed ConfigValue
    /// Returns the value in its native type (bool, int, or string)
    /// Returns error if parameter not found
    pub fn get(self: *Config, param_name: []const u8) !ConfigValue {
        self.mutex.lock();
        defer self.mutex.unlock();

        const name_lower = try std.ascii.allocLowerString(self.allocator, param_name);
        defer self.allocator.free(name_lower);

        const value = self.params.get(name_lower) orelse {
            return error.UnknownParameter;
        };
        return try value.clone(self.allocator);
    }

    /// Get configuration parameter value as string
    /// Returns owned string representation, caller must free
    /// Returns null if parameter not found
    pub fn getAsString(self: *Config, param_name: []const u8) !?[]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const name_lower = try std.ascii.allocLowerString(self.allocator, param_name);
        defer self.allocator.free(name_lower);

        const value = self.params.get(name_lower) orelse return null;
        return try value.format(self.allocator);
    }

    /// Get configuration parameter value as typed ConfigValue (alias for get)
    /// Returns the value in its native type (bool, int, or string)
    pub fn getConfigValue(self: *Config, param_name: []const u8) !ConfigValue {
        return self.get(param_name);
    }

    /// Set configuration parameter value at runtime with typed ConfigValue
    /// Returns error if parameter is read-only or value is invalid
    pub fn setConfigValue(self: *Config, param_name: []const u8, value: ConfigValue) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const name_lower = try std.ascii.allocLowerString(self.allocator, param_name);
        defer self.allocator.free(name_lower);

        // Find metadata for validation
        var meta: ?*ConfigParam = null;
        for (self.metadata.items) |*m| {
            if (std.mem.eql(u8, m.name, name_lower)) {
                meta = m;
                break;
            }
        }

        const m = meta orelse return error.UnknownParameter;

        // Check read-only
        if (m.read_only) {
            return error.ReadOnlyParameter;
        }

        // Get current value to check type matches
        const current = self.params.get(name_lower) orelse return error.UnknownParameter;

        // Ensure types match
        const current_tag = @as(std.meta.Tag(ConfigValue), current);
        const value_tag = @as(std.meta.Tag(ConfigValue), value);
        if (current_tag != value_tag) {
            return error.InvalidValue;
        }

        // Create owned copy of the value
        const new_value = try value.clone(self.allocator);
        errdefer new_value.deinit(self.allocator);

        // Replace the value in the HashMap
        const gop = try self.params.getOrPut(name_lower);
        if (gop.found_existing) {
            // Free old value
            var old_val = gop.value_ptr.*;
            old_val.deinit(self.allocator);
            // Store new value
            gop.value_ptr.* = new_value;
        } else {
            return error.UnknownParameter;
        }
    }

    /// Set configuration parameter - overloaded to handle both string and ConfigValue
    /// When passed a string, parses it based on current parameter type
    /// When passed a ConfigValue, sets it directly (must match current type)
    pub fn set(self: *Config, param_name: []const u8, value: anytype) !void {
        const ValueType = @TypeOf(value);

        // Dispatch based on value type
        if (ValueType == []const u8 or ValueType == [:0]const u8) {
            // String version - use existing logic
            return self.setString(param_name, value);
        } else if (ValueType == ConfigValue) {
            // ConfigValue version - use typed setter
            return self.setConfigValue(param_name, value);
        } else {
            @compileError("set() expects either []const u8 or ConfigValue, got " ++ @typeName(ValueType));
        }
    }

    /// Set configuration parameter from string
    /// Parses string based on current parameter type
    pub fn setAsString(self: *Config, param_name: []const u8, value_str: []const u8) !void {
        return self.setString(param_name, value_str);
    }

    /// Internal: Set from string value
    /// Returns error if parameter is read-only or value is invalid
    fn setString(self: *Config, param_name: []const u8, value_str: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const name_lower = try std.ascii.allocLowerString(self.allocator, param_name);
        defer self.allocator.free(name_lower);

        // Find metadata for validation
        var meta: ?*ConfigParam = null;
        for (self.metadata.items) |*m| {
            if (std.mem.eql(u8, m.name, name_lower)) {
                meta = m;
                break;
            }
        }

        const m = meta orelse return error.UnknownParameter;

        // Check read-only
        if (m.read_only) {
            return error.ReadOnlyParameter;
        }

        // Get current value to determine type
        const current = self.params.get(name_lower) orelse return error.UnknownParameter;

        // Parse new value based on current type
        const new_value = switch (current) {
            .string => blk: {
                const dup = try self.allocator.dupe(u8, value_str);

                // Validate specific string parameters
                if (std.mem.eql(u8, name_lower, "maxmemory-policy")) {
                    const valid_policies = [_][]const u8{
                        "noeviction",   "allkeys-lru",    "volatile-lru",
                        "allkeys-lfu",  "volatile-lfu",   "allkeys-random",
                        "volatile-random", "volatile-ttl",
                    };
                    var valid = false;
                    for (valid_policies) |policy| {
                        if (std.mem.eql(u8, dup, policy)) {
                            valid = true;
                            break;
                        }
                    }
                    if (!valid) {
                        self.allocator.free(dup);
                        return error.InvalidValue;
                    }
                } else if (std.mem.eql(u8, name_lower, "appendfsync")) {
                    const valid_modes = [_][]const u8{ "always", "everysec", "no" };
                    var valid = false;
                    for (valid_modes) |mode| {
                        if (std.mem.eql(u8, dup, mode)) {
                            valid = true;
                            break;
                        }
                    }
                    if (!valid) {
                        self.allocator.free(dup);
                        return error.InvalidValue;
                    }
                }

                break :blk ConfigValue{ .string = dup };
            },
            .int => blk: {
                // Memory parameters accept size suffixes (kb, mb, gb, tb)
                const is_memory_param = std.mem.eql(u8, name_lower, "maxmemory") or
                    std.mem.eql(u8, name_lower, "proto-max-bulk-len") or
                    std.mem.eql(u8, name_lower, "client-query-buffer-limit") or
                    std.mem.eql(u8, name_lower, "repl-backlog-size");
                const parsed = if (is_memory_param)
                    parseMemoryValue(value_str) catch return error.InvalidValue
                else
                    std.fmt.parseInt(i64, value_str, 10) catch return error.InvalidValue;
                // Validate range for specific parameters
                if (std.mem.eql(u8, name_lower, "maxmemory") or
                    std.mem.eql(u8, name_lower, "timeout") or
                    std.mem.eql(u8, name_lower, "tcp-keepalive"))
                {
                    if (parsed < 0) return error.InvalidValue;
                } else if (std.mem.eql(u8, name_lower, "maxmemory-samples")) {
                    // Must be between 1 and 10
                    if (parsed < 1 or parsed > 10) return error.InvalidValue;
                } else if (std.mem.eql(u8, name_lower, "lfu-log-factor")) {
                    // Must be between 0 and 255
                    if (parsed < 0 or parsed > 255) return error.InvalidValue;
                } else if (std.mem.eql(u8, name_lower, "lfu-decay-time")) {
                    // Must be non-negative
                    if (parsed < 0) return error.InvalidValue;
                } else if (std.mem.eql(u8, name_lower, "hz")) {
                    // Hz must be between 1 and 500
                    if (parsed < 1 or parsed > 500) return error.InvalidValue;
                } else if (std.mem.eql(u8, name_lower, "maxclients")) {
                    // Maxclients must be at least 1
                    if (parsed < 1) return error.InvalidValue;
                }
                break :blk ConfigValue{ .int = parsed };
            },
            .bool => blk: {
                const lower = try std.ascii.allocLowerString(self.allocator, value_str);
                defer self.allocator.free(lower);

                const parsed = if (std.mem.eql(u8, lower, "yes") or std.mem.eql(u8, lower, "true") or std.mem.eql(u8, lower, "1"))
                    true
                else if (std.mem.eql(u8, lower, "no") or std.mem.eql(u8, lower, "false") or std.mem.eql(u8, lower, "0"))
                    false
                else
                    return error.InvalidValue;

                break :blk ConfigValue{ .bool = parsed };
            },
        };

        // Replace the value in the HashMap
        // The HashMap already has the key, so we just update the value
        const gop = try self.params.getOrPut(name_lower);
        if (gop.found_existing) {
            // Free old value
            var old_val = gop.value_ptr.*;
            old_val.deinit(self.allocator);
            // Store new value
            gop.value_ptr.* = new_value;
        } else {
            // This shouldn't happen since we validated the parameter exists
            return error.UnknownParameter;
        }
    }

    /// Get all parameter names matching glob pattern
    /// Returns owned array of owned strings, caller must free all
    pub fn getMatching(self: *Config, pattern: []const u8) ![][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var matches = std.ArrayList([]const u8){};
        errdefer {
            for (matches.items) |match| {
                self.allocator.free(match);
            }
            matches.deinit(self.allocator);
        }

        var it = self.params.keyIterator();
        while (it.next()) |key| {
            if (glob.matchGlob(pattern, key.*)) {
                const dup = try self.allocator.dupe(u8, key.*);
                try matches.append(self.allocator, dup);
            }
        }

        return matches.toOwnedSlice(self.allocator);
    }

    /// Reset all statistics (placeholder for future stats implementation)
    pub fn resetStats(self: *Config) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        // In a full implementation, this would reset INFO stats like:
        // - total_commands_processed
        // - total_connections_received
        // - keyspace_hits/misses
        // For now, this is a no-op as stats are tracked elsewhere
    }
};

// Glob pattern matching moved to utils/glob.zig and migrated to zuda.algorithms.string.globMatch

// ── Tests ────────────────────────────────────────────────────────────────────

test "Config.init creates default parameters" {
    const allocator = std.testing.allocator;

    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Check default values
    const maxmemory = try config.get("maxmemory");
    defer if (maxmemory) |m| allocator.free(m);
    try std.testing.expect(maxmemory != null);
    try std.testing.expectEqualStrings("0", maxmemory.?);

    const port = try config.get("port");
    defer if (port) |p| allocator.free(p);
    try std.testing.expect(port != null);
    try std.testing.expectEqualStrings("6379", port.?);

    const bind = try config.get("bind");
    defer if (bind) |b| allocator.free(b);
    try std.testing.expect(bind != null);
    try std.testing.expectEqualStrings("127.0.0.1", bind.?);
}

test "Config.set updates writable parameters" {
    const allocator = std.testing.allocator;

    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Set maxmemory
    try config.set("maxmemory", "1073741824");

    const maxmemory = try config.get("maxmemory");
    defer if (maxmemory) |m| allocator.free(m);
    try std.testing.expectEqualStrings("1073741824", maxmemory.?);

    // Set boolean parameter
    try config.set("appendonly", "yes");

    const appendonly = try config.get("appendonly");
    defer if (appendonly) |a| allocator.free(a);
    try std.testing.expectEqualStrings("yes", appendonly.?);
}

test "Config.set rejects read-only parameters" {
    const allocator = std.testing.allocator;

    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Attempt to set read-only port
    const result = config.set("port", "8080");
    try std.testing.expectError(error.ReadOnlyParameter, result);

    // Verify port unchanged
    const port = try config.get("port");
    defer if (port) |p| allocator.free(p);
    try std.testing.expectEqualStrings("6379", port.?);
}

test "Config.set validates parameter values" {
    const allocator = std.testing.allocator;

    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Invalid integer
    const bad_int = config.set("maxmemory", "not-a-number");
    try std.testing.expectError(error.InvalidValue, bad_int);

    // Invalid boolean
    const bad_bool = config.set("appendonly", "maybe");
    try std.testing.expectError(error.InvalidValue, bad_bool);

    // Invalid policy
    const bad_policy = config.set("maxmemory-policy", "delete-everything");
    try std.testing.expectError(error.InvalidValue, bad_policy);

    // Valid policy
    try config.set("maxmemory-policy", "allkeys-lru");
    const policy = try config.get("maxmemory-policy");
    defer if (policy) |p| allocator.free(p);
    try std.testing.expectEqualStrings("allkeys-lru", policy.?);
}

test "Config.getMatching with glob patterns" {
    const allocator = std.testing.allocator;

    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Match all
    {
        const matches = try config.getMatching("*");
        defer {
            for (matches) |match| {
                allocator.free(match);
            }
            allocator.free(matches);
        }
        try std.testing.expect(matches.len > 0);
    }

    // Match maxmemory*
    {
        const matches = try config.getMatching("maxmemory*");
        defer {
            for (matches) |match| {
                allocator.free(match);
            }
            allocator.free(matches);
        }
        try std.testing.expect(matches.len >= 2); // maxmemory, maxmemory-policy
    }

    // Match append*
    {
        const matches = try config.getMatching("append*");
        defer {
            for (matches) |match| {
                allocator.free(match);
            }
            allocator.free(matches);
        }
        try std.testing.expect(matches.len >= 2); // appendonly, appendfsync
    }

    // No match
    {
        const matches = try config.getMatching("nonexistent*");
        defer allocator.free(matches);
        try std.testing.expectEqual(@as(usize, 0), matches.len);
    }
}

test "glob patterns for CONFIG GET" {
    // Wildcard *
    try std.testing.expect(glob.matchGlob("*", "anything"));
    try std.testing.expect(glob.matchGlob("max*", "maxmemory"));
    try std.testing.expect(glob.matchGlob("*memory", "maxmemory"));
    try std.testing.expect(glob.matchGlob("max*policy", "maxmemory-policy"));

    // Single character ?
    try std.testing.expect(glob.matchGlob("p?rt", "port"));
    try std.testing.expect(glob.matchGlob("p?rt", "part")); // ? matches any single char
    try std.testing.expect(!glob.matchGlob("p?rt", "ports")); // too long
    try std.testing.expect(!glob.matchGlob("p?rt", "prt")); // too short

    // Character class []
    try std.testing.expect(glob.matchGlob("[abc]ind", "bind"));
    try std.testing.expect(!glob.matchGlob("[abc]ind", "find"));
    try std.testing.expect(glob.matchGlob("[pP]ort", "port"));
    try std.testing.expect(glob.matchGlob("[pP]ort", "Port"));

    // Combined patterns
    try std.testing.expect(glob.matchGlob("max*-*", "maxmemory-policy"));
    try std.testing.expect(!glob.matchGlob("max*-*", "maxmemory"));

    // Exact match
    try std.testing.expect(glob.matchGlob("port", "port"));
    try std.testing.expect(!glob.matchGlob("port", "ports"));
}

test "Config.set case insensitive" {
    const allocator = std.testing.allocator;

    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Set with mixed case
    try config.set("MaxMemory", "2048");

    // Get with different case
    const val1 = try config.get("maxmemory");
    defer if (val1) |v| allocator.free(v);
    try std.testing.expectEqualStrings("2048", val1.?);

    const val2 = try config.get("MAXMEMORY");
    defer if (val2) |v| allocator.free(v);
    try std.testing.expectEqualStrings("2048", val2.?);
}

test "Config.get returns null for unknown parameter" {
    const allocator = std.testing.allocator;

    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    const result = try config.get("nonexistent");
    try std.testing.expect(result == null);
}

test "ConfigValue.format conversions" {
    const allocator = std.testing.allocator;

    // String value
    {
        var val = ConfigValue{ .string = try allocator.dupe(u8, "test") };
        defer val.deinit(allocator);

        const formatted = try val.format(allocator);
        defer allocator.free(formatted);
        try std.testing.expectEqualStrings("test", formatted);
    }

    // Integer value
    {
        const val = ConfigValue{ .int = 12345 };

        const formatted = try val.format(allocator);
        defer allocator.free(formatted);
        try std.testing.expectEqualStrings("12345", formatted);
    }

    // Boolean values
    {
        const val_true = ConfigValue{ .bool = true };
        const val_false = ConfigValue{ .bool = false };

        const fmt_true = try val_true.format(allocator);
        defer allocator.free(fmt_true);
        try std.testing.expectEqualStrings("yes", fmt_true);

        const fmt_false = try val_false.format(allocator);
        defer allocator.free(fmt_false);
        try std.testing.expectEqualStrings("no", fmt_false);
    }
}

test "Config.set validates maxmemory-samples (1-10 range)" {
    const allocator = std.testing.allocator;

    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Valid: 1
    try config.set("maxmemory-samples", "1");
    var val = try config.get("maxmemory-samples");
    defer if (val) |v| allocator.free(v);
    try std.testing.expectEqualStrings("1", val.?);

    // Valid: 10
    try config.set("maxmemory-samples", "10");
    val = try config.get("maxmemory-samples");
    defer if (val) |v| allocator.free(v);
    try std.testing.expectEqualStrings("10", val.?);

    // Invalid: 0
    try std.testing.expectError(error.InvalidValue, config.set("maxmemory-samples", "0"));

    // Invalid: 11
    try std.testing.expectError(error.InvalidValue, config.set("maxmemory-samples", "11"));

    // Invalid: -1
    try std.testing.expectError(error.InvalidValue, config.set("maxmemory-samples", "-1"));
}

test "Config.set validates lfu-log-factor (0-255 range)" {
    const allocator = std.testing.allocator;

    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Valid: 0
    try config.set("lfu-log-factor", "0");
    var val = try config.get("lfu-log-factor");
    defer if (val) |v| allocator.free(v);
    try std.testing.expectEqualStrings("0", val.?);

    // Valid: 255
    try config.set("lfu-log-factor", "255");
    val = try config.get("lfu-log-factor");
    defer if (val) |v| allocator.free(v);
    try std.testing.expectEqualStrings("255", val.?);

    // Invalid: -1
    try std.testing.expectError(error.InvalidValue, config.set("lfu-log-factor", "-1"));

    // Invalid: 256
    try std.testing.expectError(error.InvalidValue, config.set("lfu-log-factor", "256"));
}

test "Config.set validates lfu-decay-time (non-negative)" {
    const allocator = std.testing.allocator;

    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Valid: 0
    try config.set("lfu-decay-time", "0");
    var val = try config.get("lfu-decay-time");
    defer if (val) |v| allocator.free(v);
    try std.testing.expectEqualStrings("0", val.?);

    // Valid: 60
    try config.set("lfu-decay-time", "60");
    val = try config.get("lfu-decay-time");
    defer if (val) |v| allocator.free(v);
    try std.testing.expectEqualStrings("60", val.?);

    // Invalid: -1
    try std.testing.expectError(error.InvalidValue, config.set("lfu-decay-time", "-1"));
}

test "parseMemoryValue - plain integers" {
    try std.testing.expectEqual(@as(i64, 0), try parseMemoryValue("0"));
    try std.testing.expectEqual(@as(i64, 1024), try parseMemoryValue("1024"));
    try std.testing.expectEqual(@as(i64, 536870912), try parseMemoryValue("536870912"));
}

test "parseMemoryValue - size suffixes" {
    try std.testing.expectEqual(@as(i64, 1024), try parseMemoryValue("1kb"));
    try std.testing.expectEqual(@as(i64, 1024), try parseMemoryValue("1KB"));
    try std.testing.expectEqual(@as(i64, 1024), try parseMemoryValue("1k"));
    try std.testing.expectEqual(@as(i64, 1024 * 1024), try parseMemoryValue("1mb"));
    try std.testing.expectEqual(@as(i64, 1024 * 1024), try parseMemoryValue("1MB"));
    try std.testing.expectEqual(@as(i64, 1024 * 1024), try parseMemoryValue("1m"));
    try std.testing.expectEqual(@as(i64, 1024 * 1024 * 1024), try parseMemoryValue("1gb"));
    try std.testing.expectEqual(@as(i64, 1024 * 1024 * 1024), try parseMemoryValue("1GB"));
    try std.testing.expectEqual(@as(i64, 1024 * 1024 * 1024), try parseMemoryValue("1g"));
    try std.testing.expectEqual(@as(i64, 100 * 1024 * 1024), try parseMemoryValue("100mb"));
    try std.testing.expectEqual(@as(i64, 512 * 1024 * 1024), try parseMemoryValue("512mb"));
}

test "parseMemoryValue - invalid input" {
    try std.testing.expectError(error.InvalidValue, parseMemoryValue(""));
    try std.testing.expectError(error.InvalidValue, parseMemoryValue("abc"));
    try std.testing.expectError(error.InvalidValue, parseMemoryValue("1xb"));
    try std.testing.expectError(error.InvalidValue, parseMemoryValue("mb"));
}

test "Config.set hz parameter" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Valid: 10 (default)
    const val = try config.get("hz");
    defer if (val) |v| allocator.free(v);
    try std.testing.expectEqualStrings("10", val.?);

    // Valid: set to 100
    try config.set("hz", "100");
    const val2 = try config.get("hz");
    defer if (val2) |v| allocator.free(v);
    try std.testing.expectEqualStrings("100", val2.?);

    // Invalid: 0 (below minimum)
    try std.testing.expectError(error.InvalidValue, config.set("hz", "0"));

    // Invalid: 501 (above maximum)
    try std.testing.expectError(error.InvalidValue, config.set("hz", "501"));
}

test "Config.set maxmemory with size suffixes" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Set with mb suffix
    try config.set("maxmemory", "100mb");
    const val = try config.get("maxmemory");
    defer if (val) |v| allocator.free(v);
    try std.testing.expectEqualStrings("104857600", val.?); // 100 * 1024 * 1024

    // Set with gb suffix
    try config.set("maxmemory", "1gb");
    const val2 = try config.get("maxmemory");
    defer if (val2) |v| allocator.free(v);
    try std.testing.expectEqualStrings("1073741824", val2.?); // 1 * 1024 * 1024 * 1024

    // Set with plain integer
    try config.set("maxmemory", "1073741824");
    const val3 = try config.get("maxmemory");
    defer if (val3) |v| allocator.free(v);
    try std.testing.expectEqualStrings("1073741824", val3.?);
}

test "Config.set dynamic-hz boolean parameter" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Default: yes
    const val = try config.get("dynamic-hz");
    defer if (val) |v| allocator.free(v);
    try std.testing.expectEqualStrings("yes", val.?);

    // Set to no
    try config.set("dynamic-hz", "no");
    const val2 = try config.get("dynamic-hz");
    defer if (val2) |v| allocator.free(v);
    try std.testing.expectEqualStrings("no", val2.?);
}
