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

/// Alias pairs for CONFIG parameters.
/// When setting one parameter, its peer is automatically synced.
/// Each entry is { alias, canonical } (both directions are synced).
const ALIAS_PAIRS = [_][2][]const u8{
    .{ "hash-max-ziplist-entries", "hash-max-listpack-entries" },
    .{ "hash-max-ziplist-value", "hash-max-listpack-value" },
    .{ "zset-max-ziplist-entries", "zset-max-listpack-entries" },
    .{ "zset-max-ziplist-value", "zset-max-listpack-value" },
    .{ "set-max-ziplist-entries", "set-max-listpack-entries" },
    .{ "set-max-ziplist-value", "set-max-listpack-value" },
    .{ "list-max-ziplist-size", "list-max-listpack-size" },
    .{ "slave-serve-stale-data", "replica-serve-stale-data" },
    .{ "slave-read-only", "replica-read-only" },
    .{ "slave-priority", "replica-priority" },
    .{ "slave-announced", "replica-announced" },
    .{ "cluster-slave-no-failover", "cluster-replica-no-failover" },
    .{ "cluster-slave-validity-factor", "cluster-replica-validity-factor" },
    .{ "repl-ping-slave-period", "repl-ping-replica-period" },
    .{ "min-slaves-to-write", "min-replicas-to-write" },
    .{ "min-slaves-max-lag", "min-replicas-max-lag" },
    .{ "repl-min-slaves-to-write", "min-replicas-to-write" },
    .{ "repl-min-slaves-max-lag", "min-replicas-max-lag" },
    .{ "slave-ignore-maxmemory", "replica-ignore-maxmemory" },
};

/// Return the alias peer for a parameter name, or null if none.
/// The returned string is a static literal (no allocation needed).
fn findAliasPeer(name: []const u8) ?[]const u8 {
    for (ALIAS_PAIRS) |pair| {
        if (std.mem.eql(u8, name, pair[0])) return pair[1];
        if (std.mem.eql(u8, name, pair[1])) return pair[0];
    }
    return null;
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

        // Resolve the working directory for the `dir` config param (matches Redis behavior).
        // Redis returns the absolute CWD when dir has not been explicitly set.
        var cwd_buf: [std.fs.max_path_bytes]u8 = undefined;
        const cwd_str = std.fs.cwd().realpath(".", &cwd_buf) catch "";

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
            .{ .name = "set-max-ziplist-entries", .value = .{ .int = 128 }, .read_only = false }, // alias for set-max-listpack-entries
            .{ .name = "set-max-ziplist-value", .value = .{ .int = 64 }, .read_only = false }, // alias for set-max-listpack-value

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

            // Cluster configuration parameters (commonly queried by client libraries)
            .{ .name = "cluster-enabled", .value = .{ .bool = false }, .read_only = false }, // Enable cluster mode (no for standalone)
            .{ .name = "cluster-config-file", .value = .{ .string = "nodes.conf" }, .read_only = false }, // Cluster node config file path
            .{ .name = "cluster-node-timeout", .value = .{ .int = 15000 }, .read_only = false }, // Cluster node failure timeout (ms)
            .{ .name = "cluster-require-full-coverage", .value = .{ .bool = true }, .read_only = false }, // Require all slots covered to accept writes
            .{ .name = "cluster-migration-barrier", .value = .{ .int = 1 }, .read_only = false }, // Min replicas to allow migration
            .{ .name = "cluster-allow-reads-when-down", .value = .{ .bool = false }, .read_only = false }, // Allow reads when cluster is down
            .{ .name = "cluster-allow-pubsubshard-when-down", .value = .{ .bool = true }, .read_only = false }, // Allow pub/sub when cluster down (Redis 7.0+)
            .{ .name = "cluster-link-sendbuf-limit", .value = .{ .int = 0 }, .read_only = false }, // Cluster link send buffer limit (Redis 7.0+)
            .{ .name = "cluster-announce-ip", .value = .{ .string = "" }, .read_only = false }, // IP address to announce to cluster
            .{ .name = "cluster-announce-port", .value = .{ .int = 0 }, .read_only = false }, // Port to announce to cluster (0 = auto)
            .{ .name = "cluster-announce-bus-port", .value = .{ .int = 0 }, .read_only = false }, // Bus port to announce (0 = auto)
            .{ .name = "cluster-slave-no-failover", .value = .{ .bool = false }, .read_only = false }, // Prevent replicas from starting failover
            .{ .name = "cluster-replica-no-failover", .value = .{ .bool = false }, .read_only = false }, // Alias for cluster-slave-no-failover
            .{ .name = "cluster-slave-validity-factor", .value = .{ .int = 10 }, .read_only = false }, // Validity factor for failover
            .{ .name = "cluster-replica-validity-factor", .value = .{ .int = 10 }, .read_only = false }, // Alias for cluster-slave-validity-factor
            .{ .name = "cluster-preferred-endpoint-type", .value = .{ .string = "ip" }, .read_only = false }, // Endpoint type (ip/hostname/unknown-endpoint)

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

            // Debug / runtime overrides (set only via DEBUG command, not CONFIG SET)
            .{ .name = "debug-quicklist-packed-threshold", .value = .{ .int = 4096 }, .read_only = false }, // Quicklist packed-node size threshold (DEBUG QUICKLIST-PACKED-THRESHOLD)

            // Authentication & security
            .{ .name = "requirepass", .value = .{ .string = "" }, .read_only = false }, // Password required for AUTH (empty = no password)
            .{ .name = "protected-mode", .value = .{ .bool = true }, .read_only = false }, // Protected mode (blocks connections from non-loopback when no password set)
            .{ .name = "masterauth", .value = .{ .string = "" }, .read_only = false }, // Password to authenticate against primary
            .{ .name = "aclfile", .value = .{ .string = "" }, .read_only = false }, // Path to ACL file (empty = inline ACL rules only)
            .{ .name = "acllog-max-len", .value = .{ .int = 128 }, .read_only = false }, // Max entries in ACL log
            .{ .name = "enable-protected-configs", .value = .{ .string = "no" }, .read_only = false }, // Allow setting protected configs via CONFIG SET
            .{ .name = "enable-debug-command", .value = .{ .string = "yes" }, .read_only = false }, // Allow DEBUG commands (yes/no/local)

            // Working directory (initialized to CWD at startup, matches Redis behavior)
            .{ .name = "dir", .value = .{ .string = cwd_str }, .read_only = false }, // Working directory for RDB/AOF files

            // Replication / replica settings
            .{ .name = "replica-serve-stale-data", .value = .{ .bool = true }, .read_only = false }, // Serve stale data while replica is syncing
            .{ .name = "replica-read-only", .value = .{ .bool = true }, .read_only = false }, // Replica is read-only
            .{ .name = "replica-priority", .value = .{ .int = 100 }, .read_only = false }, // Priority for Sentinel failover (lower = preferred; 0 = never promote)
            .{ .name = "replica-announced", .value = .{ .bool = true }, .read_only = false }, // Announce replica to Sentinel
            .{ .name = "slave-serve-stale-data", .value = .{ .bool = true }, .read_only = false }, // Deprecated alias for replica-serve-stale-data
            .{ .name = "slave-read-only", .value = .{ .bool = true }, .read_only = false }, // Deprecated alias for replica-read-only
            .{ .name = "slave-priority", .value = .{ .int = 100 }, .read_only = false }, // Deprecated alias for replica-priority
            .{ .name = "slave-announced", .value = .{ .bool = true }, .read_only = false }, // Deprecated alias for replica-announced
            .{ .name = "propagate-reads-enabled", .value = .{ .bool = false }, .read_only = false }, // Whether to propagate read commands to replicas

            // Latency monitoring
            .{ .name = "latency-monitor-threshold", .value = .{ .int = 0 }, .read_only = false }, // Minimum latency (ms) to record (0 = disabled)

            // Startup / runtime display
            .{ .name = "always-show-logo", .value = .{ .bool = false }, .read_only = false }, // Always show ASCII art logo on startup
            .{ .name = "set-proc-title", .value = .{ .bool = true }, .read_only = false }, // Set process title to show mode
            .{ .name = "proc-title-template", .value = .{ .string = "{title} {listen-addr} {server-mode}" }, .read_only = false }, // Process title template

            // Socket / network
            .{ .name = "socket-mark-id", .value = .{ .int = 0 }, .read_only = false }, // SO_MARK for outgoing connections (0 = disabled)
            // tls-replication and tls-cluster are defined in the TLS section above

            // RDB / AOF edge cases
            .{ .name = "rdb-del-sync-files", .value = .{ .bool = false }, .read_only = false }, // Delete RDB files used for replication after loading
            .{ .name = "aof-timestamp-enabled", .value = .{ .bool = false }, .read_only = false }, // Add timestamps to AOF entries (Redis 7.0+)

            // Active expiry / eviction tuning
            .{ .name = "active-expire-enabled", .value = .{ .bool = true }, .read_only = false }, // Enable active key expiration
            .{ .name = "active-expire-effort", .value = .{ .int = 1 }, .read_only = false }, // Aggressiveness of active expiry (1-10)
            .{ .name = "maxmemory-eviction-tenacity", .value = .{ .int = 10 }, .read_only = false }, // How aggressively to try to meet maxmemory (0-100)

            // Lua / scripting
            // lua-replicate-commands is defined in the Lua section above
            .{ .name = "busy-reply-threshold", .value = .{ .int = 5000 }, .read_only = false }, // ms before replying with BUSY during script
            .{ .name = "repl-min-slaves-to-write", .value = .{ .int = 0 }, .read_only = false }, // Alias for min-replicas-to-write
            .{ .name = "repl-min-slaves-max-lag", .value = .{ .int = 10 }, .read_only = false }, // Alias for min-replicas-max-lag

            // Quicklist / stream node encoding
            .{ .name = "list-compress-depth", .value = .{ .int = 0 }, .read_only = false }, // Number of quicklist node levels to leave uncompressed (0 = none)
            .{ .name = "stream-node-max-bytes", .value = .{ .int = 4096 }, .read_only = false }, // Max bytes per stream listpack node (0 = unlimited)
            .{ .name = "stream-node-max-entries", .value = .{ .int = 100 }, .read_only = false }, // Max entries per stream listpack node (0 = unlimited)

            // OOM killer adjustment
            .{ .name = "oom-score-adj", .value = .{ .string = "no" }, .read_only = false }, // OOM killer adjustment mode (no/yes/absolute)
            .{ .name = "oom-score-adj-values", .value = .{ .string = "0 200 800" }, .read_only = false }, // OOM score values per mode (server/slave/bgchild)

            // ACL / pub-sub defaults
            .{ .name = "acl-pubsub-default", .value = .{ .string = "resetchannels" }, .read_only = false }, // Default pub/sub ACL for new connections (resetchannels/allchannels)

            // Locale / collation
            .{ .name = "locale-collate", .value = .{ .string = "" }, .read_only = false }, // Locale used for string comparison (empty = POSIX)

            // Syslog metadata
            .{ .name = "syslog-ident", .value = .{ .string = "redis" }, .read_only = false }, // Syslog identity string
            .{ .name = "syslog-facility", .value = .{ .string = "local0" }, .read_only = false }, // Syslog facility

            // Active defragmentation (additional params)
            .{ .name = "activedefrag-ignore-bytes", .value = .{ .int = 104857600 }, .read_only = false }, // Min bytes of fragmentation to start defrag (default 100mb)
            .{ .name = "activedefrag-max-scan-fields", .value = .{ .int = 1000 }, .read_only = false }, // Max hash/set/zset/list fields per scan cycle for defrag

            // Client memory / eviction (Redis 7.x)
            .{ .name = "maxmemory-clients", .value = .{ .int = 0 }, .read_only = false }, // Client-side caching memory limit per client (0 = disabled)

            // Replica memory policy (Redis 7.x)
            .{ .name = "replica-ignore-maxmemory", .value = .{ .bool = true }, .read_only = false }, // Replica does not enforce maxmemory
            .{ .name = "slave-ignore-maxmemory", .value = .{ .bool = true }, .read_only = false }, // Deprecated alias for replica-ignore-maxmemory

            // RDB / loading throttle (Redis 7.x)
            .{ .name = "rdb-key-save-delay", .value = .{ .int = 0 }, .read_only = false }, // Microsecond delay between each key save (for throttling RDB save)
            .{ .name = "key-load-delay", .value = .{ .int = 0 }, .read_only = false }, // Microsecond delay between each key load (for throttling RDB load)
            .{ .name = "loading-process-events-interval-bytes", .value = .{ .int = 2097152 }, .read_only = false }, // How often to process events while loading (bytes, default 2mb)

            // Cluster announce (additional)
            .{ .name = "cluster-announce-hostname", .value = .{ .string = "" }, .read_only = false }, // Hostname that this node announces to cluster
            .{ .name = "cluster-announce-human-nodename", .value = .{ .string = "" }, .read_only = false }, // Human-readable node name for cluster topology

            // Server shutdown
            .{ .name = "shutdown-timeout", .value = .{ .int = 10 }, .read_only = false }, // Timeout (seconds) for graceful shutdown
            .{ .name = "shutdown-save-on-sigterm", .value = .{ .string = "default" }, .read_only = false }, // Save on SIGTERM (default/yes/no)

            // Client output buffer limits (Redis 6.0+)
            // Format: "<class> <hard-limit> <soft-limit> <soft-seconds> ..."
            // Redis default: normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60
            .{ .name = "client-output-buffer-limit", .value = .{ .string = "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60" }, .read_only = false },

            // ACL logging alias (acllog-max-len is the canonical name)
            .{ .name = "acllog-max-entries", .value = .{ .int = 128 }, .read_only = false }, // Alias for acllog-max-len

            // Cluster announce TLS port (Redis 6.2+)
            .{ .name = "cluster-announce-tls-port", .value = .{ .int = 0 }, .read_only = false }, // TLS port announced to cluster

            // Latency history flag (Redis 7.x)
            .{ .name = "latency-history-enabled", .value = .{ .bool = true }, .read_only = false }, // Enable per-event latency history tracking

            // Networking edge cases
            .{ .name = "close-on-oom-score-adj", .value = .{ .bool = false }, .read_only = false }, // Disconnect clients when process hits OOM score limit

            // RDB persistence options (commonly queried by clients)
            .{ .name = "dbfilename", .value = .{ .string = "dump.rdb" }, .read_only = false }, // RDB file name
            .{ .name = "rdbcompression", .value = .{ .bool = true }, .read_only = false }, // Compress RDB files with LZF
            .{ .name = "rdbchecksum", .value = .{ .bool = true }, .read_only = false }, // Add CRC64 checksums to RDB

            // Replication diskless sync timeout (Redis 7.x)
            .{ .name = "repl-diskless-sync-timeout", .value = .{ .int = 5 }, .read_only = false }, // Timeout for diskless replica sync (seconds)

            // Lua/scripting aliases (Redis 7.x)
            .{ .name = "script-time-limit", .value = .{ .int = 5000 }, .read_only = false }, // Max Lua script execution time (ms), alias for lua-time-limit

            // AOF persistence options (commonly queried)
            .{ .name = "aof-rewrite-min-size", .value = .{ .int = 67108864 }, .read_only = false }, // Min AOF file size (64mb) before rewrite
            .{ .name = "auto-aof-rewrite-percentage", .value = .{ .int = 100 }, .read_only = false }, // Rewrite when AOF is 100% larger than base
            .{ .name = "auto-aof-rewrite-min-size", .value = .{ .int = 67108864 }, .read_only = false }, // Min size for auto-rewrite (64mb)
            .{ .name = "aof-load-truncated", .value = .{ .bool = true }, .read_only = false }, // Load truncated AOF files
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
        var new_value = try value.clone(self.allocator);
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

        // Sync alias peer
        if (findAliasPeer(name_lower)) |peer_name| {
            if (self.params.getPtr(peer_name)) |peer_ptr| {
                if (gop.value_ptr.*.clone(self.allocator)) |peer_val| {
                    peer_ptr.*.deinit(self.allocator);
                    peer_ptr.* = peer_val;
                } else |_| {}
            }
        }
    }

    /// Set configuration parameter - overloaded to handle both string and ConfigValue
    /// When passed a string, parses it based on current parameter type
    /// When passed a ConfigValue, sets it directly (must match current type)
    pub fn set(self: *Config, param_name: []const u8, value: anytype) !void {
        const ValueType = @TypeOf(value);

        // Dispatch based on value type.
        // ConfigValue is the typed path; anything else is treated as a string
        // ([]const u8, [:0]const u8, and string literals *const[N:0]u8 all coerce).
        if (ValueType == ConfigValue) {
            return self.setConfigValue(param_name, value);
        } else {
            return self.setString(param_name, @as([]const u8, value));
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

        // Sync alias peer — keeps ziplist/listpack aliases and slave/replica aliases in sync.
        // We already hold the mutex and the value is stored; clone it for the peer.
        if (findAliasPeer(name_lower)) |peer_name| {
            if (self.params.getPtr(peer_name)) |peer_ptr| {
                if (gop.value_ptr.*.clone(self.allocator)) |peer_val| {
                    peer_ptr.*.deinit(self.allocator);
                    peer_ptr.* = peer_val;
                } else |_| {} // best-effort: ignore allocation failure for alias sync
            }
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

test "Config - requirepass default is empty" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    const val = try config.get("requirepass");
    defer if (val) |v| allocator.free(v);
    try std.testing.expectEqualStrings("", val.?);

    try config.set("requirepass", "secretpassword");
    const val2 = try config.get("requirepass");
    defer if (val2) |v| allocator.free(v);
    try std.testing.expectEqualStrings("secretpassword", val2.?);
}

test "Config - protected-mode default is yes" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    const val = try config.get("protected-mode");
    defer if (val) |v| allocator.free(v);
    try std.testing.expectEqualStrings("yes", val.?);
}

test "Config - dir defaults to non-empty working directory" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    const val = try config.get("dir");
    defer if (val) |v| allocator.free(v);
    // dir should be non-empty (initialized to CWD at startup)
    try std.testing.expect(val != null);
    try std.testing.expect(val.?.len > 0);
}

test "Config - replica params exist and have correct defaults" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    const serve = try config.get("replica-serve-stale-data");
    defer if (serve) |v| allocator.free(v);
    try std.testing.expectEqualStrings("yes", serve.?);

    const priority = try config.get("replica-priority");
    defer if (priority) |v| allocator.free(v);
    try std.testing.expectEqualStrings("100", priority.?);

    const ro = try config.get("replica-read-only");
    defer if (ro) |v| allocator.free(v);
    try std.testing.expectEqualStrings("yes", ro.?);
}

test "Config - latency-monitor-threshold default is 0" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    const val = try config.get("latency-monitor-threshold");
    defer if (val) |v| allocator.free(v);
    try std.testing.expectEqualStrings("0", val.?);

    try config.set("latency-monitor-threshold", "100");
    const val2 = try config.get("latency-monitor-threshold");
    defer if (val2) |v| allocator.free(v);
    try std.testing.expectEqualStrings("100", val2.?);
}

test "Config - slave aliases match replica defaults" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    const slave_serve = try config.get("slave-serve-stale-data");
    defer if (slave_serve) |v| allocator.free(v);
    try std.testing.expectEqualStrings("yes", slave_serve.?);

    const slave_prio = try config.get("slave-priority");
    defer if (slave_prio) |v| allocator.free(v);
    try std.testing.expectEqualStrings("100", slave_prio.?);
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

test "Config alias sync - setting ziplist alias updates listpack canonical" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Setting the ziplist alias should also update the listpack canonical
    try config.set("hash-max-ziplist-entries", "64");

    // Both should now be 64
    const canonical = try config.get("hash-max-listpack-entries");
    defer if (canonical) |v| allocator.free(v);
    try std.testing.expectEqualStrings("64", canonical.?);

    const alias = try config.get("hash-max-ziplist-entries");
    defer if (alias) |v| allocator.free(v);
    try std.testing.expectEqualStrings("64", alias.?);
}

test "Config alias sync - setting listpack canonical updates ziplist alias" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Setting the canonical should also update the alias
    try config.set("zset-max-listpack-entries", "32");

    const canonical = try config.get("zset-max-listpack-entries");
    defer if (canonical) |v| allocator.free(v);
    try std.testing.expectEqualStrings("32", canonical.?);

    const alias = try config.get("zset-max-ziplist-entries");
    defer if (alias) |v| allocator.free(v);
    try std.testing.expectEqualStrings("32", alias.?);
}

test "Config alias sync - slave/replica parameter bidirectional sync" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Setting slave-serve-stale-data should update replica-serve-stale-data
    try config.set("slave-serve-stale-data", "no");

    const replica_val = try config.get("replica-serve-stale-data");
    defer if (replica_val) |v| allocator.free(v);
    try std.testing.expectEqualStrings("no", replica_val.?);

    // Setting replica-serve-stale-data should update slave-serve-stale-data
    try config.set("replica-serve-stale-data", "yes");

    const slave_val = try config.get("slave-serve-stale-data");
    defer if (slave_val) |v| allocator.free(v);
    try std.testing.expectEqualStrings("yes", slave_val.?);
}

test "Config alias sync - min-replicas integer aliases stay in sync" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Setting the old slave alias updates the new canonical
    try config.set("min-slaves-to-write", "2");

    const canonical = try config.get("min-replicas-to-write");
    defer if (canonical) |v| allocator.free(v);
    try std.testing.expectEqualStrings("2", canonical.?);
}

test "Config alias sync - list-max-ziplist-size syncs with list-max-listpack-size" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    try config.set("list-max-ziplist-size", "-1");

    const listpack = try config.get("list-max-listpack-size");
    defer if (listpack) |v| allocator.free(v);
    try std.testing.expectEqualStrings("-1", listpack.?);
}

test "Config alias sync - zset-max-ziplist-value syncs with zset-max-listpack-value" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    try config.set("zset-max-ziplist-value", "32");

    const canonical = try config.get("zset-max-listpack-value");
    defer if (canonical) |v| allocator.free(v);
    try std.testing.expectEqualStrings("32", canonical.?);

    const alias = try config.get("zset-max-ziplist-value");
    defer if (alias) |v| allocator.free(v);
    try std.testing.expectEqualStrings("32", alias.?);
}

test "Config - new params have correct defaults" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    const compress_depth = try config.get("list-compress-depth");
    defer if (compress_depth) |v| allocator.free(v);
    try std.testing.expectEqualStrings("0", compress_depth.?);

    const stream_max_bytes = try config.get("stream-node-max-bytes");
    defer if (stream_max_bytes) |v| allocator.free(v);
    try std.testing.expectEqualStrings("4096", stream_max_bytes.?);

    const stream_max_entries = try config.get("stream-node-max-entries");
    defer if (stream_max_entries) |v| allocator.free(v);
    try std.testing.expectEqualStrings("100", stream_max_entries.?);

    const oom_adj = try config.get("oom-score-adj");
    defer if (oom_adj) |v| allocator.free(v);
    try std.testing.expectEqualStrings("no", oom_adj.?);

    const acl_pubsub = try config.get("acl-pubsub-default");
    defer if (acl_pubsub) |v| allocator.free(v);
    try std.testing.expectEqualStrings("resetchannels", acl_pubsub.?);
}

test "Config alias sync - set-max-ziplist-entries syncs with set-max-listpack-entries" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    try config.set("set-max-ziplist-entries", "64");

    const canonical = try config.get("set-max-listpack-entries");
    defer if (canonical) |v| allocator.free(v);
    try std.testing.expectEqualStrings("64", canonical.?);

    const alias = try config.get("set-max-ziplist-entries");
    defer if (alias) |v| allocator.free(v);
    try std.testing.expectEqualStrings("64", alias.?);
}

test "Config alias sync - set-max-ziplist-value syncs with set-max-listpack-value" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    try config.set("set-max-ziplist-value", "32");

    const canonical = try config.get("set-max-listpack-value");
    defer if (canonical) |v| allocator.free(v);
    try std.testing.expectEqualStrings("32", canonical.?);

    const alias = try config.get("set-max-ziplist-value");
    defer if (alias) |v| allocator.free(v);
    try std.testing.expectEqualStrings("32", alias.?);
}

test "Config - Iteration 335 new params have correct defaults" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    const defrag_ignore = try config.getAsString("activedefrag-ignore-bytes");
    defer if (defrag_ignore) |v| allocator.free(v);
    try std.testing.expectEqualStrings("104857600", defrag_ignore.?);

    const defrag_scan = try config.getAsString("activedefrag-max-scan-fields");
    defer if (defrag_scan) |v| allocator.free(v);
    try std.testing.expectEqualStrings("1000", defrag_scan.?);

    const maxmem_clients = try config.getAsString("maxmemory-clients");
    defer if (maxmem_clients) |v| allocator.free(v);
    try std.testing.expectEqualStrings("0", maxmem_clients.?);

    const replica_ignore = try config.getAsString("replica-ignore-maxmemory");
    defer if (replica_ignore) |v| allocator.free(v);
    try std.testing.expectEqualStrings("yes", replica_ignore.?);

    const shutdown_timeout = try config.getAsString("shutdown-timeout");
    defer if (shutdown_timeout) |v| allocator.free(v);
    try std.testing.expectEqualStrings("10", shutdown_timeout.?);
}

test "Config alias sync - slave-ignore-maxmemory syncs to replica-ignore-maxmemory" {
    const allocator = std.testing.allocator;
    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    try config.set("slave-ignore-maxmemory", @as([]const u8, "no"));

    const canonical = try config.getAsString("replica-ignore-maxmemory");
    defer if (canonical) |v| allocator.free(v);
    try std.testing.expectEqualStrings("no", canonical.?);
}
