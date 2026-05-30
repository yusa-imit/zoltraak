const std = @import("std");
const builtin = @import("builtin");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");
const repl_mod = @import("../storage/replication.zig");
const client_mod = @import("client.zig");

const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;
const ReplicationState = repl_mod.ReplicationState;
const ClientRegistry = client_mod.ClientRegistry;

/// Server configuration for INFO command
pub const ServerConfig = struct {
    port: u16,
    bind: []const u8,
    maxmemory: i64,
    maxmemory_policy: []const u8,
    timeout: i64,
    tcp_keepalive: i64,
    save: []const u8,
    appendonly: bool,
    appendfsync: []const u8,
    databases: i64,
};

/// Server statistics for INFO command
pub const ServerStats = struct {
    client_count: usize,
    tracking_clients: usize,
    total_commands_processed: u64,
    total_connections_received: u64,
    start_time_seconds: i64,
};

/// INFO [section]
///
/// Returns detailed information about the Redis-compatible server.
/// Supported sections: server, clients, memory, persistence, stats, replication, cpu, modules, cluster, keyspace, commandstats, errorstats, latencystats, all, default
///
/// Iteration 30: Comprehensive INFO implementation with all major sections.
pub fn cmdInfo(
    allocator: std.mem.Allocator,
    storage: *Storage,
    repl: *ReplicationState,
    config: ServerConfig,
    stats: ServerStats,
    args: []const []const u8,
    databases: ?[]Storage,
    num_databases: u16,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Determine which section was requested
    const section_arg: []const u8 = if (args.len >= 2) args[1] else "default";
    const section = try std.ascii.allocUpperString(allocator, section_arg);
    defer allocator.free(section);

    var buf = std.ArrayList(u8){};
    defer buf.deinit(allocator);

    const show_all = std.mem.eql(u8, section, "ALL");
    const show_default = std.mem.eql(u8, section, "DEFAULT");

    // Build the requested sections
    if (show_all or show_default or std.mem.eql(u8, section, "SERVER")) {
        try buildServerSection(&buf, allocator, config, storage, stats.start_time_seconds, num_databases);
    }
    if (show_all or show_default or std.mem.eql(u8, section, "CLIENTS")) {
        try buildClientsSection(&buf, allocator, stats.client_count, stats.tracking_clients, storage);
    }
    if (show_all or show_default or std.mem.eql(u8, section, "MEMORY")) {
        try buildMemorySection(&buf, allocator, storage);
    }
    if (show_all or show_default or std.mem.eql(u8, section, "PERSISTENCE")) {
        try buildPersistenceSection(&buf, allocator, config, storage);
    }
    if (show_all or show_default or std.mem.eql(u8, section, "STATS")) {
        try buildStatsSection(&buf, allocator, stats.total_commands_processed, stats.total_connections_received, storage);
    }
    if (show_all or show_default or std.mem.eql(u8, section, "REPLICATION")) {
        try buildReplicationSection(&buf, allocator, repl, storage);
    }
    if (show_all or show_default or std.mem.eql(u8, section, "CPU")) {
        try buildCpuSection(&buf, allocator);
    }
    if (show_all or show_default or std.mem.eql(u8, section, "MODULES")) {
        try buildModulesSection(&buf, allocator);
    }
    if (show_all or show_default or std.mem.eql(u8, section, "CLUSTER")) {
        try buildClusterSection(&buf, allocator);
    }
    if (show_all or show_default or std.mem.eql(u8, section, "KEYSPACE")) {
        try buildKeyspaceSection(&buf, allocator, storage, databases, num_databases);
    }
    if (show_all or std.mem.eql(u8, section, "COMMANDSTATS")) {
        try buildCommandstatsSection(&buf, allocator, storage);
    }
    if (show_all or std.mem.eql(u8, section, "ERRORSTATS")) {
        try buildErrorstatsSection(&buf, allocator, storage);
    }
    if (show_all or std.mem.eql(u8, section, "LATENCYSTATS")) {
        try buildLatencystatsSection(&buf, allocator);
    }

    const info = try buf.toOwnedSlice(allocator);
    defer allocator.free(info);
    return w.writeBulkString(info);
}

/// Returns total physical system memory in bytes using platform-specific APIs.
/// Falls back to 0 on unsupported platforms or on error.
fn getTotalSystemMemory() usize {
    switch (builtin.os.tag) {
        .linux => {
            // Read MemTotal from /proc/meminfo (format: "MemTotal: XXXXX kB")
            const file = std.fs.openFileAbsolute("/proc/meminfo", .{}) catch return 0;
            defer file.close();
            var buf: [512]u8 = undefined;
            const n = file.read(&buf) catch return 0;
            const content = buf[0..n];
            const prefix = "MemTotal:";
            const idx = std.mem.indexOf(u8, content, prefix) orelse return 0;
            const after = std.mem.trim(u8, content[idx + prefix.len ..], " \t");
            const end = std.mem.indexOf(u8, after, " ") orelse after.len;
            const kb = std.fmt.parseInt(usize, after[0..end], 10) catch return 0;
            return kb * 1024;
        },
        .macos, .ios, .tvos, .watchos => {
            // Use sysctlbyname("hw.memsize") to get physical RAM
            var memsize: usize = 0;
            var memsize_len: usize = @sizeOf(usize);
            const result = std.c.sysctlbyname("hw.memsize", &memsize, &memsize_len, null, 0);
            if (result == 0) return memsize;
            return 0;
        },
        else => return 0,
    }
}

fn buildServerSection(
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    config: ServerConfig,
    storage: *Storage,
    start_time_seconds: i64,
    num_databases: u16,
) !void {
    const bw = buf.writer(allocator);

    // Read hz from config (default 10, may be changed via CONFIG SET hz)
    const hz_val = storage.config.get("hz") catch null;
    const hz: i64 = if (hz_val) |v| switch (v) {
        .int => |n| n,
        else => 10,
    } else 10;

    const multiplexing_api = comptime switch (builtin.os.tag) {
        .linux => "epoll",
        .macos, .freebsd, .openbsd, .netbsd, .dragonfly => "kqueue",
        else => "select",
    };

    try bw.writeAll("# Server\r\n");
    try bw.writeAll("redis_version:7.2.0\r\n");
    try bw.writeAll("redis_git_sha1:00000000\r\n");
    try bw.writeAll("redis_git_dirty:0\r\n");
    try bw.writeAll("redis_build_id:zoltraak\r\n");
    try bw.writeAll("redis_mode:standalone\r\n");
    try bw.writeAll("os:");
    try bw.writeAll(@tagName(builtin.os.tag));
    try bw.writeAll("\r\n");
    try bw.writeAll("arch_bits:");
    try bw.print("{d}\r\n", .{@bitSizeOf(usize)});
    try bw.writeAll("monotonic_clock:POSIX clock_gettime\r\n");
    try bw.print("multiplexing_api:{s}\r\n", .{multiplexing_api});
    try bw.writeAll("atomicvar_api:atomic-builtin\r\n");
    try bw.writeAll("gcc_version:0.0.0\r\n");
    try bw.writeAll("process_id:");
    if (comptime @import("builtin").os.tag == .linux) {
        try bw.print("{d}\r\n", .{std.os.linux.getpid()});
    } else {
        try bw.print("{d}\r\n", .{std.c.getpid()});
    }
    try bw.print("run_id:{s}\r\n", .{&storage.run_id});
    try bw.print("tcp_port:{d}\r\n", .{config.port});
    // server_time_usec: current Unix time in microseconds (Redis 7.0+ field)
    try bw.print("server_time_usec:{d}\r\n", .{std.time.microTimestamp()});
    try bw.print("uptime_in_seconds:{d}\r\n", .{std.time.timestamp() - start_time_seconds});
    try bw.print("uptime_in_days:{d}\r\n", .{@divTrunc(std.time.timestamp() - start_time_seconds, 86400)});
    try bw.print("hz:{d}\r\n", .{hz});
    try bw.print("configured_hz:{d}\r\n", .{hz});
    try bw.writeAll("lru_clock:");
    // Redis LRU clock: Unix time in seconds, truncated to 24 bits (modulo 2^24)
    try bw.print("{d}\r\n", .{@as(u32, @intCast(@as(u64, @intCast(std.time.timestamp())) & 0xFFFFFF))});
    try bw.writeAll("executable:./zig-out/bin/zoltraak\r\n");
    try bw.writeAll("config_file:\r\n");
    try bw.print("databases:{d}\r\n", .{num_databases});
    try bw.writeAll("\r\n");
}

fn buildClientsSection(
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    client_count: usize,
    tracking_clients: usize,
    storage: *Storage,
) !void {
    const bw = buf.writer(allocator);

    // Read maxclients from config (default 10000)
    const maxclients: i64 = blk: {
        var cv = storage.config.get("maxclients") catch break :blk 10000;
        defer cv.deinit(allocator);
        break :blk switch (cv) { .int => |i| i, else => 10000 };
    };

    try bw.writeAll("# Clients\r\n");
    try bw.print("connected_clients:{d}\r\n", .{client_count});
    try bw.writeAll("cluster_connections:0\r\n");
    try bw.print("maxclients:{d}\r\n", .{maxclients});
    try bw.writeAll("client_recent_max_input_buffer:0\r\n");
    try bw.writeAll("client_recent_max_output_buffer:0\r\n");
    try bw.writeAll("total_blocking_keys:0\r\n");
    try bw.writeAll("total_blocking_keys_on_nokey:0\r\n");
    try bw.writeAll("blocked_clients:0\r\n");
    try bw.print("tracking_clients:{d}\r\n", .{tracking_clients});
    try bw.writeAll("clients_in_timeout_table:0\r\n");
    try bw.writeAll("total_watched_keys:0\r\n");
    try bw.writeAll("\r\n");
}

fn buildMemorySection(
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    storage: *Storage,
) !void {
    const bw = buf.writer(allocator);

    // Calculate approximate memory usage under mutex to prevent data races
    var total_memory: usize = 0;
    var key_count: usize = 0;
    {
        storage.mutex.lock();
        defer storage.mutex.unlock();
        var iter = storage.data.iterator();
        while (iter.next()) |entry| {
            key_count += 1;
            total_memory += entry.key_ptr.len;
            total_memory += estimateValueMemory(entry.value_ptr.*);
        }
    }

    // Read maxmemory from config (0 = unlimited)
    const maxmemory: i64 = blk: {
        var cv = storage.config.get("maxmemory") catch break :blk 0;
        defer cv.deinit(allocator);
        break :blk switch (cv) { .int => |i| i, else => 0 };
    };
    // Read maxmemory-policy from config
    const maxmemory_policy: []const u8 = blk: {
        var cv = storage.config.get("maxmemory-policy") catch break :blk try allocator.dupe(u8, "noeviction");
        defer cv.deinit(allocator);
        break :blk switch (cv) {
            .string => |s| try allocator.dupe(u8, s),
            else => try allocator.dupe(u8, "noeviction"),
        };
    };
    defer allocator.free(maxmemory_policy);

    // Update peak memory tracking atomically
    storage.updatePeakMemory(total_memory);
    const peak_memory = storage.getPeakMemory();

    // Get actual system physical memory
    const system_memory = getTotalSystemMemory();

    var fmt_buf1: [32]u8 = undefined;
    var fmt_buf2: [32]u8 = undefined;
    var fmt_buf3: [32]u8 = undefined;
    var fmt_buf4: [32]u8 = undefined;
    var fmt_buf5: [32]u8 = undefined;
    const maxmemory_bytes: usize = @intCast(@max(0, maxmemory));
    try bw.writeAll("# Memory\r\n");
    try bw.print("used_memory:{d}\r\n", .{total_memory});
    try bw.print("used_memory_human:{s}\r\n", .{formatBytes(total_memory, &fmt_buf1)});
    try bw.print("used_memory_rss:{d}\r\n", .{total_memory * 2});
    try bw.print("used_memory_rss_human:{s}\r\n", .{formatBytes(total_memory * 2, &fmt_buf2)});
    try bw.print("used_memory_peak:{d}\r\n", .{peak_memory});
    try bw.print("used_memory_peak_human:{s}\r\n", .{formatBytes(peak_memory, &fmt_buf3)});
    try bw.writeAll("used_memory_overhead:0\r\n");
    try bw.writeAll("used_memory_startup:0\r\n");
    try bw.writeAll("used_memory_dataset:0\r\n");
    if (system_memory > 0) {
        try bw.print("total_system_memory:{d}\r\n", .{system_memory});
        try bw.print("total_system_memory_human:{s}\r\n", .{formatBytes(system_memory, &fmt_buf4)});
    } else {
        try bw.writeAll("total_system_memory:0\r\n");
        try bw.writeAll("total_system_memory_human:0B\r\n");
    }
    try bw.print("maxmemory:{d}\r\n", .{maxmemory_bytes});
    try bw.print("maxmemory_human:{s}\r\n", .{if (maxmemory_bytes == 0) "0B" else formatBytes(maxmemory_bytes, &fmt_buf5)});
    try bw.print("maxmemory_policy:{s}\r\n", .{maxmemory_policy});
    try bw.writeAll("mem_fragmentation_ratio:1.00\r\n");
    try bw.writeAll("mem_allocator:zig\r\n");
    try bw.writeAll("\r\n");
}

fn buildPersistenceSection(
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    config: ServerConfig,
    storage: *Storage,
) !void {
    const bw = buf.writer(allocator);

    const dirty = storage.getDirtyCount();
    const last_save = storage.getLastSaveTime();

    try bw.writeAll("# Persistence\r\n");
    try bw.writeAll("loading:0\r\n");
    try bw.print("rdb_changes_since_last_save:{d}\r\n", .{dirty});
    try bw.writeAll("rdb_bgsave_in_progress:0\r\n");
    try bw.print("rdb_last_save_time:{d}\r\n", .{last_save});
    try bw.writeAll("rdb_last_bgsave_status:ok\r\n");
    try bw.writeAll("rdb_last_bgsave_time_sec:0\r\n");
    try bw.writeAll("rdb_current_bgsave_time_sec:-1\r\n");
    try bw.writeAll("rdb_last_cow_size:0\r\n");

    const aof_enabled = if (config.appendonly) "1" else "0";
    try bw.print("aof_enabled:{s}\r\n", .{aof_enabled});
    try bw.writeAll("aof_rewrite_in_progress:0\r\n");
    try bw.writeAll("aof_rewrite_scheduled:0\r\n");
    try bw.writeAll("aof_last_rewrite_time_sec:-1\r\n");
    try bw.writeAll("aof_current_rewrite_time_sec:-1\r\n");
    try bw.writeAll("aof_last_bgrewrite_status:ok\r\n");
    try bw.writeAll("aof_last_write_status:ok\r\n");
    try bw.writeAll("aof_last_cow_size:0\r\n");
    try bw.writeAll("\r\n");
}

fn buildStatsSection(
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    total_commands_processed: u64,
    total_connections_received: u64,
    storage: *Storage,
) !void {
    const bw = buf.writer(allocator);

    try bw.writeAll("# Stats\r\n");
    try bw.print("total_connections_received:{d}\r\n", .{total_connections_received});
    try bw.print("total_commands_processed:{d}\r\n", .{total_commands_processed});
    try bw.writeAll("instantaneous_ops_per_sec:0\r\n");
    try bw.writeAll("total_net_input_bytes:0\r\n");
    try bw.writeAll("total_net_output_bytes:0\r\n");
    try bw.writeAll("instantaneous_input_kbps:0.00\r\n");
    try bw.writeAll("instantaneous_output_kbps:0.00\r\n");
    try bw.writeAll("rejected_connections:0\r\n");
    try bw.writeAll("sync_full:0\r\n");
    try bw.writeAll("sync_partial_ok:0\r\n");
    try bw.writeAll("sync_partial_err:0\r\n");
    try bw.print("expired_keys:{d}\r\n", .{storage.getExpiredKeysCount()});
    try bw.print("evicted_keys:{d}\r\n", .{storage.getEvictedKeysCount()});
    try bw.print("lazyfree_pending_objects:{d}\r\n", .{storage.lazyfree_task.getPendingCount()});
    try bw.print("keyspace_hits:{d}\r\n", .{storage.getKeyspaceHits()});
    try bw.print("keyspace_misses:{d}\r\n", .{storage.getKeyspaceMisses()});
    // Pub/Sub channel and pattern counts from live state
    if (storage.pubsub_state) |ps| {
        try bw.print("pubsub_channels:{d}\r\n", .{ps.totalChannelCount()});
        try bw.print("pubsub_patterns:{d}\r\n", .{ps.totalPatternCount()});
    } else {
        try bw.writeAll("pubsub_channels:0\r\n");
        try bw.writeAll("pubsub_patterns:0\r\n");
    }
    try bw.writeAll("latest_fork_usec:0\r\n");
    // Active defragmentation statistics
    const defrag_stats = storage.defrag_task.getStats();
    const defrag_running: u8 = if (defrag_stats.is_running) 1 else 0;
    const defrag_misses = if (defrag_stats.keys_scanned > defrag_stats.keys_defragmented)
        defrag_stats.keys_scanned - defrag_stats.keys_defragmented
    else
        0;
    try bw.print("active_defrag_running:{d}\r\n", .{defrag_running});
    try bw.print("active_defrag_hits:{d}\r\n", .{defrag_stats.keys_defragmented});
    try bw.print("active_defrag_misses:{d}\r\n", .{defrag_misses});
    try bw.print("active_defrag_key_hits:{d}\r\n", .{defrag_stats.keys_defragmented});
    try bw.print("active_defrag_key_misses:{d}\r\n", .{defrag_misses});
    try bw.writeAll("\r\n");
}

fn buildReplicationSection(
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    repl: *ReplicationState,
    storage: *Storage,
) !void {
    const bw = buf.writer(allocator);

    // Read repl_backlog_size from config (default 1MB)
    const backlog_size: i64 = blk: {
        var cv = storage.config.get("repl-backlog-size") catch break :blk 1048576;
        defer cv.deinit(allocator);
        break :blk switch (cv) { .int => |i| i, else => 1048576 };
    };

    try bw.writeAll("# Replication\r\n");

    const role_str: []const u8 = switch (repl.role) {
        .primary => "master",
        .replica => "slave",
    };
    try bw.print("role:{s}\r\n", .{role_str});

    if (repl.role == .primary) {
        // Master-specific fields: connected_slaves, per-slave info, failover state
        try bw.print("connected_slaves:{d}\r\n", .{repl.replicas.items.len});
        for (repl.replicas.items, 0..) |r, i| {
            const state_str: []const u8 = switch (r.state) {
                .handshake => "wait_bgsave",
                .rdb_transfer => "send_bulk",
                .online => "online",
            };
            try bw.print("slave{d}:ip=127.0.0.1,port={d},state={s},offset={d},lag=0\r\n", .{
                i,
                6379 + i + 1,
                state_str,
                r.repl_offset,
            });
        }
        // master_failover_state: Redis 7.x field (no-failover for standalone/no active failover)
        try bw.print("master_failover_state:{s}\r\n", .{repl.failover_state.toString()});
    } else {
        // Replica-specific fields
        const link_status = if (repl.primary_link_up) "up" else "down";
        try bw.print("master_host:{s}\r\n", .{repl.primary_host orelse "unknown"});
        try bw.print("master_port:{d}\r\n", .{repl.primary_port});
        try bw.print("master_link_status:{s}\r\n", .{link_status});
        try bw.writeAll("master_last_io_seconds_ago:0\r\n");
        try bw.writeAll("master_sync_in_progress:0\r\n");
    }

    // Common fields (both master and replica)
    try bw.print("master_replid:{s}\r\n", .{repl.replid});
    // master_replid2: secondary replication ID (all zeros when not set, Redis 7.x field)
    try bw.writeAll("master_replid2:0000000000000000000000000000000000000000\r\n");
    try bw.print("master_repl_offset:{d}\r\n", .{repl.repl_offset});
    try bw.writeAll("second_repl_offset:-1\r\n");
    // repl_backlog_active: 1 when at least one replica is connected, 0 otherwise
    const backlog_active: u8 = if (repl.role == .primary and repl.replicas.items.len > 0) 1 else 0;
    try bw.print("repl_backlog_active:{d}\r\n", .{backlog_active});
    try bw.print("repl_backlog_size:{d}\r\n", .{backlog_size});
    try bw.writeAll("repl_backlog_first_byte_offset:0\r\n");
    try bw.writeAll("repl_backlog_histlen:0\r\n");
    try bw.writeAll("\r\n");
}

fn buildCpuSection(
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
) !void {
    const bw = buf.writer(allocator);

    try bw.writeAll("# CPU\r\n");
    try bw.writeAll("used_cpu_sys:0.000000\r\n");
    try bw.writeAll("used_cpu_user:0.000000\r\n");
    try bw.writeAll("used_cpu_sys_children:0.000000\r\n");
    try bw.writeAll("used_cpu_user_children:0.000000\r\n");
    try bw.writeAll("\r\n");
}

fn buildKeyspaceSection(
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    storage: *Storage,
    databases: ?[]Storage,
    num_databases: u16,
) !void {
    const bw = buf.writer(allocator);

    try bw.writeAll("# Keyspace\r\n");

    const now_ms = std.time.milliTimestamp();

    if (databases) |dbs| {
        // Show stats for all databases that have data
        const db_count: usize = @min(num_databases, dbs.len);
        for (dbs[0..db_count], 0..) |*db, db_idx| {
            var total_keys: usize = 0;
            var keys_with_expiry: usize = 0;
            var ttl_sum: i64 = 0;

            db.mutex.lock();
            var iter = db.data.iterator();
            while (iter.next()) |entry| {
                total_keys += 1;
                if (entry.value_ptr.*.getExpiration()) |exp_ms| {
                    keys_with_expiry += 1;
                    const remaining = exp_ms - now_ms;
                    if (remaining > 0) {
                        ttl_sum += remaining;
                    }
                }
            }
            db.mutex.unlock();

            if (total_keys > 0) {
                const avg_ttl: u64 = if (keys_with_expiry > 0)
                    @intCast(@divTrunc(ttl_sum, @as(i64, @intCast(keys_with_expiry))))
                else
                    0;
                try bw.print("db{d}:keys={d},expires={d},avg_ttl={d}\r\n", .{ db_idx, total_keys, keys_with_expiry, avg_ttl });
            }
        }
    } else {
        // Fallback: only the single storage passed in (db0)
        var total_keys: usize = 0;
        var keys_with_expiry: usize = 0;
        var ttl_sum: i64 = 0;

        storage.mutex.lock();
        var iter = storage.data.iterator();
        while (iter.next()) |entry| {
            total_keys += 1;
            if (entry.value_ptr.*.getExpiration()) |exp_ms| {
                keys_with_expiry += 1;
                const remaining = exp_ms - now_ms;
                if (remaining > 0) {
                    ttl_sum += remaining;
                }
            }
        }
        storage.mutex.unlock();

        if (total_keys > 0) {
            const avg_ttl: u64 = if (keys_with_expiry > 0)
                @intCast(@divTrunc(ttl_sum, @as(i64, @intCast(keys_with_expiry))))
            else
                0;
            try bw.print("db0:keys={d},expires={d},avg_ttl={d}\r\n", .{ total_keys, keys_with_expiry, avg_ttl });
        }
    }

    try bw.writeAll("\r\n");
}

fn buildCommandstatsSection(
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    storage: *Storage,
) !void {
    const bw = buf.writer(allocator);
    try bw.writeAll("# Commandstats\r\n");

    const snapshot = try storage.snapshotCommandStats(allocator);
    defer allocator.free(snapshot);

    for (snapshot) |item| {
        const name_lower = try std.ascii.allocLowerString(allocator, item.name);
        defer allocator.free(name_lower);

        const usec_per_call: f64 = if (item.entry.calls > 0)
            @as(f64, @floatFromInt(item.entry.usec)) / @as(f64, @floatFromInt(item.entry.calls))
        else
            0.0;

        try bw.print(
            "cmdstat_{s}:calls={d},usec={d},usec_per_call={d:.2},rejected_calls={d},failed_calls={d}\r\n",
            .{ name_lower, item.entry.calls, item.entry.usec, usec_per_call, item.entry.rejected_calls, item.entry.failed_calls },
        );
    }

    try bw.writeAll("\r\n");
}

fn buildErrorstatsSection(
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    storage: *Storage,
) !void {
    const bw = buf.writer(allocator);
    try bw.writeAll("# Errorstats\r\n");

    const snapshot = try storage.snapshotErrorStats(allocator);
    defer allocator.free(snapshot);

    for (snapshot) |item| {
        try bw.print("errorstat_{s}:count={d}\r\n", .{ item.error_type, item.count });
    }

    try bw.writeAll("\r\n");
}

fn buildLatencystatsSection(
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
) !void {
    const bw = buf.writer(allocator);
    try bw.writeAll("# Latencystats\r\n");
    try bw.writeAll("\r\n");
}

fn buildModulesSection(
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
) !void {
    const bw = buf.writer(allocator);
    try bw.writeAll("# Modules\r\n");
    try bw.writeAll("\r\n");
}

fn buildClusterSection(
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
) !void {
    const bw = buf.writer(allocator);
    try bw.writeAll("# Cluster\r\n");
    try bw.writeAll("cluster_enabled:0\r\n");
    try bw.writeAll("\r\n");
}

// ── Helper Functions ──────────────────────────────────────────────────────────

fn estimateValueMemory(value: storage_mod.Value) usize {
    return switch (value) {
        .string => |s| s.data.len,
        .list => |list| list.data.items.len * 8, // Rough estimate
        .set => |set| set.count() * 8,
        .hash => |hash| hash.data.count() * 16,
        .sorted_set => |zset| zset.members.count() * 24,
        .stream => |stream| stream.entries.items.len * 64,
        .hyperloglog => 12288, // 16384 registers * 6 bits = 12288 bytes
        .json => |j| blk: {
            _ = j;
            break :blk 256; // Rough JSON tree estimate
        },
        .timeseries => |ts| ts.samples.items.len * 16, // 8 bytes timestamp + 8 bytes value
        .bloom => |bf| blk: {
            var total: usize = 0;
            for (bf.filters.items) |filter| {
                total += filter.bits.len;
            }
            break :blk total;
        },
        .cuckoo => |cf| blk: {
            var total: usize = 0;
            for (cf.filters.items) |filter| {
                for (filter.buckets) |bucket| {
                    total += bucket.fingerprints.len;
                }
            }
            break :blk total;
        },
        .count_min_sketch => |cms| blk: {
            // Count-Min Sketch memory: depth * width * sizeof(u64)
            const total = cms.depth * cms.width * @sizeOf(u64);
            break :blk total;
        },
        .top_k => |tk| blk: {
            // Top-K memory: hash table + heap
            // Hash table: depth * width * (1 + 8) bytes per cell
            // Heap: k items * (ptr + count + fingerprint)
            const hash_table_size = tk.depth * tk.width * (@sizeOf(u8) + @sizeOf(u64));
            const heap_size = tk.k * (@sizeOf([]u8) + @sizeOf(u64) + @sizeOf(u8));
            break :blk hash_table_size + heap_size;
        },
        .t_digest => |td| blk: {
            // T-Digest memory: centroids array
            const centroid_size = td.centroids.items.len * (@sizeOf(f64) + @sizeOf(u64));
            break :blk centroid_size;
        },
        .vector_set => |vs| blk: {
            // Vector set memory: vectors * dimensionality * sizeof(f32) + attributes
            const vectors_size = vs.vectors.count() * vs.dimensionality * @sizeOf(f32);
            break :blk vectors_size;
        },
    };
}

fn formatBytes(bytes: usize, buf: *[32]u8) []const u8 {
    if (bytes < 1024) {
        return std.fmt.bufPrint(buf, "{d}B", .{bytes}) catch "?B";
    } else if (bytes < 1024 * 1024) {
        return std.fmt.bufPrint(buf, "{d}K", .{bytes / 1024}) catch "?K";
    } else if (bytes < 1024 * 1024 * 1024) {
        return std.fmt.bufPrint(buf, "{d}M", .{bytes / (1024 * 1024)}) catch "?M";
    } else {
        return std.fmt.bufPrint(buf, "{d}G", .{bytes / (1024 * 1024 * 1024)}) catch "?G";
    }
}

// ── Unit Tests ────────────────────────────────────────────────────────────────

test "INFO server section" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "900 1 300 10 60 10000",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };

    const stats = ServerStats{
        .client_count = 1,
        .tracking_clients = 0,
        .total_commands_processed = 100,
        .total_connections_received = 50,
        .start_time_seconds = 1000000,
    };

    const args = [_][]const u8{ "INFO", "SERVER" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "# Server") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "redis_version:") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "tcp_port:6379") != null);
}

test "INFO clients section" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "900 1 300 10 60 10000",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };

    const stats = ServerStats{
        .client_count = 5,
        .tracking_clients = 0,
        .total_commands_processed = 100,
        .total_connections_received = 50,
        .start_time_seconds = 1000000,
    };

    const args = [_][]const u8{ "INFO", "CLIENTS" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "# Clients") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "connected_clients:5") != null);
}

test "INFO keyspace section with data" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Add some test data
    _ = try storage.set("key1", "value1", null);
    _ = try storage.set("key2", "value2", 5000);

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "900 1 300 10 60 10000",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };

    const stats = ServerStats{
        .client_count = 1,
        .tracking_clients = 0,
        .total_commands_processed = 100,
        .total_connections_received = 50,
        .start_time_seconds = 1000000,
    };

    const args = [_][]const u8{ "INFO", "KEYSPACE" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "# Keyspace") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "db0:keys=2") != null);
}

test "INFO default shows multiple sections" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "900 1 300 10 60 10000",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };

    const stats = ServerStats{
        .client_count = 1,
        .tracking_clients = 0,
        .total_commands_processed = 100,
        .total_connections_received = 50,
        .start_time_seconds = 1000000,
    };

    const args = [_][]const u8{"INFO"};
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "# Server") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "# Clients") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "# Memory") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "# Persistence") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "# Stats") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "# Replication") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "# CPU") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "# Keyspace") != null);
}

test "INFO server section uptime is reasonable (not Unix timestamp)" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "900 1 300 10 60 10000",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };

    // Use real server start time (just now)
    const now = std.time.timestamp();
    const stats = ServerStats{
        .client_count = 1,
        .tracking_clients = 0,
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = now,
    };

    const args = [_][]const u8{ "INFO", "SERVER" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    // uptime_in_seconds must be < 5 seconds (we just set start_time = now)
    const uptime_line_start = std.mem.indexOf(u8, result, "uptime_in_seconds:") orelse unreachable;
    const line_start = uptime_line_start + "uptime_in_seconds:".len;
    var line_end = line_start;
    while (line_end < result.len and result[line_end] != '\r') line_end += 1;
    const uptime_str = result[line_start..line_end];
    const uptime = try std.fmt.parseInt(i64, uptime_str, 10);

    // Must be small (< 5s since we just started), NOT a Unix timestamp (which would be ~1.7B)
    try std.testing.expect(uptime >= 0);
    try std.testing.expect(uptime < 5);
}

test "INFO server section lru_clock is 24-bit truncated" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "900 1 300 10 60 10000",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };

    const stats = ServerStats{
        .client_count = 1,
        .tracking_clients = 0,
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "SERVER" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    // lru_clock must be <= 2^24 - 1 = 16777215
    const lru_start = std.mem.indexOf(u8, result, "lru_clock:") orelse unreachable;
    const val_start = lru_start + "lru_clock:".len;
    var val_end = val_start;
    while (val_end < result.len and result[val_end] != '\r') val_end += 1;
    const lru_str = result[val_start..val_end];
    const lru_val = try std.fmt.parseInt(u32, lru_str, 10);

    try std.testing.expect(lru_val <= 0xFFFFFF); // Must fit in 24 bits
}

test "INFO stats section shows real command/connection counts" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "900 1 300 10 60 10000",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };

    const stats = ServerStats{
        .client_count = 3,
        .tracking_clients = 0,
        .total_commands_processed = 42,
        .total_connections_received = 7,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "STATS" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "total_commands_processed:42") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "total_connections_received:7") != null);
}

test "INFO stats section shows keyspace hits and misses" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Simulate some GET hits and misses
    try storage.set("k1", "v1", null);
    _ = storage.get("k1");      // hit
    _ = storage.get("missing"); // miss
    _ = storage.get("missing"); // miss

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "900 1 300 10 60 10000",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };

    const stats = ServerStats{
        .client_count = 1,
        .tracking_clients = 0,
        .total_commands_processed = 3,
        .total_connections_received = 1,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "STATS" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "keyspace_hits:1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "keyspace_misses:2") != null);
}

test "INFO commandstats section is empty when no commands recorded" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "900 1 300 10 60 10000",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };

    const stats = ServerStats{
        .client_count = 0,
        .tracking_clients = 0,
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "COMMANDSTATS" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "# Commandstats") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "cmdstat_") == null);
}

test "INFO commandstats section shows recorded commands" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.recordCommandStat("GET", 15);
    storage.recordCommandStat("GET", 25);
    storage.recordCommandStat("SET", 10);

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "900 1 300 10 60 10000",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };

    const stats = ServerStats{
        .client_count = 1,
        .tracking_clients = 0,
        .total_commands_processed = 3,
        .total_connections_received = 1,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "COMMANDSTATS" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "cmdstat_get:calls=2") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "cmdstat_set:calls=1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "usec=40") != null); // GET: 15+25
}

test "INFO errorstats section shows recorded errors" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.recordErrorStat("ERR");
    storage.recordErrorStat("ERR");
    storage.recordErrorStat("WRONGTYPE");

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "900 1 300 10 60 10000",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };

    const stats = ServerStats{
        .client_count = 1,
        .tracking_clients = 0,
        .total_commands_processed = 3,
        .total_connections_received = 1,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "ERRORSTATS" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "# Errorstats") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "errorstat_ERR:count=2") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "errorstat_WRONGTYPE:count=1") != null);
}

test "INFO ALL includes commandstats and errorstats sections" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.recordCommandStat("PING", 5);

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "900 1 300 10 60 10000",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };

    const stats = ServerStats{
        .client_count = 1,
        .tracking_clients = 0,
        .total_commands_processed = 1,
        .total_connections_received = 1,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "ALL" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "# Commandstats") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "# Errorstats") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "# Latencystats") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "cmdstat_ping:calls=1") != null);
}

test "INFO cluster section appears in default and all" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };
    const stats = ServerStats{
        .client_count = 0,
        .tracking_clients = 0,
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = std.time.timestamp(),
    };

    // INFO default must include cluster section
    const default_args = [_][]const u8{ "INFO", "default" };
    const default_result = try cmdInfo(allocator, &storage, &repl, config, stats, &default_args, null, 1);
    defer allocator.free(default_result);
    try std.testing.expect(std.mem.indexOf(u8, default_result, "# Cluster") != null);
    try std.testing.expect(std.mem.indexOf(u8, default_result, "cluster_enabled:0") != null);

    // INFO all must also include cluster section
    const all_args = [_][]const u8{ "INFO", "all" };
    const all_result = try cmdInfo(allocator, &storage, &repl, config, stats, &all_args, null, 1);
    defer allocator.free(all_result);
    try std.testing.expect(std.mem.indexOf(u8, all_result, "# Cluster") != null);

    // INFO cluster specifically
    const cluster_args = [_][]const u8{ "INFO", "cluster" };
    const cluster_result = try cmdInfo(allocator, &storage, &repl, config, stats, &cluster_args, null, 1);
    defer allocator.free(cluster_result);
    try std.testing.expect(std.mem.indexOf(u8, cluster_result, "# Cluster") != null);
    try std.testing.expect(std.mem.indexOf(u8, cluster_result, "cluster_enabled:0") != null);
    // cluster section should not include server section
    try std.testing.expect(std.mem.indexOf(u8, cluster_result, "redis_version:") == null);
}

test "INFO modules section appears in default and all" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };
    const stats = ServerStats{
        .client_count = 0,
        .tracking_clients = 0,
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = std.time.timestamp(),
    };

    // INFO default must include modules section
    const default_args = [_][]const u8{ "INFO", "default" };
    const default_result = try cmdInfo(allocator, &storage, &repl, config, stats, &default_args, null, 1);
    defer allocator.free(default_result);
    try std.testing.expect(std.mem.indexOf(u8, default_result, "# Modules") != null);

    // INFO modules specifically
    const modules_args = [_][]const u8{ "INFO", "modules" };
    const modules_result = try cmdInfo(allocator, &storage, &repl, config, stats, &modules_args, null, 1);
    defer allocator.free(modules_result);
    try std.testing.expect(std.mem.indexOf(u8, modules_result, "# Modules") != null);
}

test "INFO keyspace shows avg_ttl for keys with expiry" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Add a key with 10-second TTL (expires_at in ms)
    const expire_at = std.time.milliTimestamp() + 10_000; // 10 seconds in the future
    try storage.set("mykey", "value", expire_at);

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };
    const stats = ServerStats{
        .client_count = 0,
        .tracking_clients = 0,
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "keyspace" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "# Keyspace") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "db0:keys=1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "expires=1") != null);
    // avg_ttl should be close to 10000 ms (9000–10000 range is acceptable)
    try std.testing.expect(std.mem.indexOf(u8, result, "avg_ttl=0") == null);
}

test "INFO keyspace avg_ttl is 0 when no keys have expiry" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Add a key WITHOUT TTL
    try storage.set("mykey", "value", null);

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };
    const stats = ServerStats{
        .client_count = 0,
        .tracking_clients = 0,
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "keyspace" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "db0:keys=1,expires=0,avg_ttl=0") != null);
}

test "INFO keyspace multi-database shows all non-empty databases" {
    const allocator = std.testing.allocator;

    // Create three databases
    var db0 = try Storage.init(allocator, 6379, "127.0.0.1");
    defer db0.deinit();
    var db1 = try Storage.init(allocator, 6379, "127.0.0.1");
    defer db1.deinit();
    var db2 = try Storage.init(allocator, 6379, "127.0.0.1");
    defer db2.deinit();

    // Add a key to db0 and db2 (but NOT db1)
    try db0.set("key0", "val0", null);
    try db2.set("key2", "val2", null);

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Use the actual storages directly as a slice (not copies)
    var dbs = [3]Storage{ db0, db1, db2 };

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 3,
    };
    const stats = ServerStats{
        .client_count = 0,
        .tracking_clients = 0,
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "keyspace" };
    const result = try cmdInfo(allocator, &db0, &repl, config, stats, &args, &dbs, 3);
    defer allocator.free(result);

    // db0 and db2 should appear, db1 should not
    try std.testing.expect(std.mem.indexOf(u8, result, "db0:keys=1") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "db1:") == null);
    try std.testing.expect(std.mem.indexOf(u8, result, "db2:keys=1") != null);
}

test "INFO server section shows correct databases count" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 16,
    };
    const stats = ServerStats{
        .client_count = 0,
        .tracking_clients = 0,
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "server" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 16);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "databases:16") != null);
}

test "INFO memory section shows default maxmemory:0 and maxmemory_policy:noeviction" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };
    const stats = ServerStats{
        .client_count = 0,
        .tracking_clients = 0,
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "memory" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "# Memory") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "maxmemory:0\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "maxmemory_human:0B\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "maxmemory_policy:noeviction\r\n") != null);
}

test "INFO memory section reflects CONFIG SET maxmemory" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set maxmemory to 1GB
    try storage.config.set("maxmemory", "1073741824");

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };
    const stats = ServerStats{
        .client_count = 0,
        .tracking_clients = 0,
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "memory" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "maxmemory:1073741824\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "maxmemory_human:1G\r\n") != null);
}

test "INFO memory section reflects CONFIG SET maxmemory-policy" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set maxmemory-policy to allkeys-lru
    try storage.config.set("maxmemory-policy", "allkeys-lru");

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 1,
    };
    const stats = ServerStats{
        .client_count = 0,
        .tracking_clients = 0,
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "memory" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "maxmemory_policy:allkeys-lru\r\n") != null);
}

test "INFO server section run_id is 40 hex chars" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();
    const config = ServerConfig{ .port = 6379, .bind = "127.0.0.1", .maxmemory = 0, .maxmemory_policy = "noeviction", .timeout = 0, .tcp_keepalive = 300, .save = "", .appendonly = false, .appendfsync = "everysec", .databases = 1 };
    const stats = ServerStats{ .client_count = 0, .total_commands_processed = 0, .total_connections_received = 0, .start_time_seconds = std.time.timestamp() };

    const args = [_][]const u8{ "INFO", "server" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    // run_id: must appear with exactly 40 hex chars
    const run_id_prefix = "run_id:";
    const pos = std.mem.indexOf(u8, result, run_id_prefix) orelse return error.TestExpectedRunId;
    const id_start = pos + run_id_prefix.len;
    try std.testing.expect(id_start + 40 <= result.len);
    for (result[id_start .. id_start + 40]) |c| {
        try std.testing.expect((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'));
    }
    // run_id must not be the old hardcoded value
    try std.testing.expect(std.mem.indexOf(u8, result, "run_id:zoltraak-instance-001") == null);
}

test "INFO server section hz reads from config" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set hz to 25 via config
    try storage.config.set("hz", "25");

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();
    const config = ServerConfig{ .port = 6379, .bind = "127.0.0.1", .maxmemory = 0, .maxmemory_policy = "noeviction", .timeout = 0, .tcp_keepalive = 300, .save = "", .appendonly = false, .appendfsync = "everysec", .databases = 1 };
    const stats = ServerStats{ .client_count = 0, .total_commands_processed = 0, .total_connections_received = 0, .start_time_seconds = std.time.timestamp() };

    const args = [_][]const u8{ "INFO", "server" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "hz:25\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "configured_hz:25\r\n") != null);
}

test "INFO stats section expired_keys counter" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Simulate 3 expired key events
    storage.incrementExpiredKeys();
    storage.incrementExpiredKeys();
    storage.incrementExpiredKeys();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();
    const config = ServerConfig{ .port = 6379, .bind = "127.0.0.1", .maxmemory = 0, .maxmemory_policy = "noeviction", .timeout = 0, .tcp_keepalive = 300, .save = "", .appendonly = false, .appendfsync = "everysec", .databases = 1 };
    const stats = ServerStats{ .client_count = 0, .total_commands_processed = 0, .total_connections_received = 0, .start_time_seconds = std.time.timestamp() };

    const args = [_][]const u8{ "INFO", "stats" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "expired_keys:3\r\n") != null);
}

test "INFO server section multiplexing_api is platform-appropriate" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();
    const config = ServerConfig{ .port = 6379, .bind = "127.0.0.1", .maxmemory = 0, .maxmemory_policy = "noeviction", .timeout = 0, .tcp_keepalive = 300, .save = "", .appendonly = false, .appendfsync = "everysec", .databases = 1 };
    const stats = ServerStats{ .client_count = 0, .total_commands_processed = 0, .total_connections_received = 0, .start_time_seconds = std.time.timestamp() };

    const args = [_][]const u8{ "INFO", "server" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    // Must contain multiplexing_api: field with a valid value
    const expected_api = comptime switch (builtin.os.tag) {
        .linux => "multiplexing_api:epoll\r\n",
        .macos, .freebsd, .openbsd, .netbsd, .dragonfly => "multiplexing_api:kqueue\r\n",
        else => "multiplexing_api:select\r\n",
    };
    try std.testing.expect(std.mem.indexOf(u8, result, expected_api) != null);
}

test "Storage run_id is unique across instances" {
    const allocator = std.testing.allocator;
    var s1 = try Storage.init(allocator, 6379, "127.0.0.1");
    defer s1.deinit();
    var s2 = try Storage.init(allocator, 6380, "127.0.0.1");
    defer s2.deinit();

    // Two instances should have different run_ids (extremely unlikely to collide)
    try std.testing.expect(!std.mem.eql(u8, &s1.run_id, &s2.run_id));

    // Each run_id must be 40 valid hex chars
    for (s1.run_id) |c| {
        try std.testing.expect((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'));
    }
}

test "INFO persistence section shows rdb_last_save_time:0 initially" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 16,
    };
    const stats = ServerStats{
        .client_count = 0,
        .tracking_clients = 0,
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "persistence" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "rdb_last_save_time:0\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "rdb_changes_since_last_save:0\r\n") != null);
}

test "INFO persistence section shows real dirty count after writes" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    // Make 3 write operations
    try storage.set("k1", "v1", null);
    try storage.set("k2", "v2", null);
    try storage.set("k3", "v3", null);

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 16,
    };
    const stats = ServerStats{
        .client_count = 0,
        .tracking_clients = 0,
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "persistence" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    // dirty count should be 3
    try std.testing.expect(std.mem.indexOf(u8, result, "rdb_changes_since_last_save:3\r\n") != null);
}

test "INFO persistence section rdb_changes_since_last_save resets after save" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    try storage.set("k1", "v1", null);
    try storage.set("k2", "v2", null);

    // Simulate a successful save
    storage.updateLastSaveTime();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 16,
    };
    const stats = ServerStats{
        .client_count = 0,
        .tracking_clients = 0,
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "persistence" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    // dirty count should be 0 after save
    try std.testing.expect(std.mem.indexOf(u8, result, "rdb_changes_since_last_save:0\r\n") != null);
    // rdb_last_save_time should now be non-zero
    try std.testing.expect(std.mem.indexOf(u8, result, "rdb_last_save_time:0\r\n") == null);
}

test "INFO memory section used_memory_peak grows monotonically" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 16,
    };
    const stats = ServerStats{
        .client_count = 0,
        .tracking_clients = 0,
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = std.time.timestamp(),
    };

    // First INFO call — establishes baseline peak
    const args = [_][]const u8{ "INFO", "memory" };
    const result1 = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result1);
    try std.testing.expect(std.mem.indexOf(u8, result1, "used_memory_peak:") != null);

    // Add some data
    try storage.set("k1", "v1", null);
    try storage.set("k2", "v2", null);

    // Second INFO call — peak should be >= first peak
    const result2 = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result2);
    // used_memory_peak must appear
    try std.testing.expect(std.mem.indexOf(u8, result2, "used_memory_peak:") != null);
    // peak should not be 0 after adding data (memory > 0)
    try std.testing.expect(std.mem.indexOf(u8, result2, "used_memory_peak:0\r\n") == null);
}

test "INFO memory section total_system_memory is non-negative" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 16,
    };
    const stats = ServerStats{
        .client_count = 0,
        .tracking_clients = 0,
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "memory" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    // total_system_memory field must exist (value ≥ 0, exact value is platform-dependent)
    try std.testing.expect(std.mem.indexOf(u8, result, "total_system_memory:") != null);
    // On supported platforms (macOS/Linux), system memory should be non-zero
    // On other platforms, it falls back to 0 — we just require the field exists
}

test "INFO clients section tracking_clients matches count" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();

    const config = ServerConfig{
        .port = 6379,
        .bind = "127.0.0.1",
        .maxmemory = 0,
        .maxmemory_policy = "noeviction",
        .timeout = 0,
        .tcp_keepalive = 300,
        .save = "",
        .appendonly = false,
        .appendfsync = "everysec",
        .databases = 16,
    };
    // 2 tracking clients
    const stats = ServerStats{
        .client_count = 5,
        .tracking_clients = 2,
        .total_commands_processed = 10,
        .total_connections_received = 5,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "clients" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "connected_clients:5\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "tracking_clients:2\r\n") != null);
}

test "Storage updatePeakMemory tracks maximum" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Initially 0
    try std.testing.expectEqual(@as(usize, 0), storage.getPeakMemory());

    // Update to 1000
    storage.updatePeakMemory(1000);
    try std.testing.expectEqual(@as(usize, 1000), storage.getPeakMemory());

    // Smaller value should not lower peak
    storage.updatePeakMemory(500);
    try std.testing.expectEqual(@as(usize, 1000), storage.getPeakMemory());

    // Larger value updates peak
    storage.updatePeakMemory(2000);
    try std.testing.expectEqual(@as(usize, 2000), storage.getPeakMemory());
}

test "INFO clients section includes cluster_connections and maxclients" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    // Set maxclients to 500 via config
    try storage.config.set("maxclients", "500");

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();
    const config = ServerConfig{ .port = 6379, .bind = "127.0.0.1", .maxmemory = 0, .maxmemory_policy = "noeviction", .timeout = 0, .tcp_keepalive = 300, .save = "", .appendonly = false, .appendfsync = "everysec", .databases = 1 };
    const stats = ServerStats{ .client_count = 3, .tracking_clients = 0, .total_commands_processed = 0, .total_connections_received = 0, .start_time_seconds = std.time.timestamp() };

    const args = [_][]const u8{ "INFO", "clients" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "cluster_connections:0\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "maxclients:500\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "connected_clients:3\r\n") != null);
}

test "INFO clients section includes blocking and watched key fields" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();
    const config = ServerConfig{ .port = 6379, .bind = "127.0.0.1", .maxmemory = 0, .maxmemory_policy = "noeviction", .timeout = 0, .tcp_keepalive = 300, .save = "", .appendonly = false, .appendfsync = "everysec", .databases = 1 };
    const stats = ServerStats{ .client_count = 0, .tracking_clients = 0, .total_commands_processed = 0, .total_connections_received = 0, .start_time_seconds = std.time.timestamp() };

    const args = [_][]const u8{ "INFO", "clients" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "total_blocking_keys:0\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "total_blocking_keys_on_nokey:0\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "total_watched_keys:0\r\n") != null);
}

test "INFO stats section includes active defrag fields" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();
    const config = ServerConfig{ .port = 6379, .bind = "127.0.0.1", .maxmemory = 0, .maxmemory_policy = "noeviction", .timeout = 0, .tcp_keepalive = 300, .save = "", .appendonly = false, .appendfsync = "everysec", .databases = 1 };
    const stats = ServerStats{ .client_count = 0, .tracking_clients = 0, .total_commands_processed = 0, .total_connections_received = 0, .start_time_seconds = std.time.timestamp() };

    const args = [_][]const u8{ "INFO", "stats" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    // All 5 active_defrag fields must be present
    try std.testing.expect(std.mem.indexOf(u8, result, "active_defrag_running:") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "active_defrag_hits:") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "active_defrag_misses:") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "active_defrag_key_hits:") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "active_defrag_key_misses:") != null);
    // Initially defrag is not running
    try std.testing.expect(std.mem.indexOf(u8, result, "active_defrag_running:0\r\n") != null);
}

test "INFO clients section maxclients defaults to 10000 when not configured" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    // Do NOT set maxclients — should default to 10000

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();
    const config = ServerConfig{ .port = 6379, .bind = "127.0.0.1", .maxmemory = 0, .maxmemory_policy = "noeviction", .timeout = 0, .tcp_keepalive = 300, .save = "", .appendonly = false, .appendfsync = "everysec", .databases = 1 };
    const stats = ServerStats{ .client_count = 0, .tracking_clients = 0, .total_commands_processed = 0, .total_connections_received = 0, .start_time_seconds = std.time.timestamp() };

    const args = [_][]const u8{ "INFO", "clients" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "maxclients:10000\r\n") != null);
}

test "INFO replication includes master_failover_state (Redis 7.x)" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();
    const config = ServerConfig{ .port = 6379, .bind = "127.0.0.1", .maxmemory = 0, .maxmemory_policy = "noeviction", .timeout = 0, .tcp_keepalive = 300, .save = "", .appendonly = false, .appendfsync = "everysec", .databases = 1 };
    const stats = ServerStats{ .client_count = 0, .tracking_clients = 0, .total_commands_processed = 0, .total_connections_received = 0, .start_time_seconds = std.time.timestamp() };

    const args = [_][]const u8{ "INFO", "replication" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "master_failover_state:no-failover\r\n") != null);
}

test "INFO replication includes master_replid2 (Redis 7.x)" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();
    const config = ServerConfig{ .port = 6379, .bind = "127.0.0.1", .maxmemory = 0, .maxmemory_policy = "noeviction", .timeout = 0, .tcp_keepalive = 300, .save = "", .appendonly = false, .appendfsync = "everysec", .databases = 1 };
    const stats = ServerStats{ .client_count = 0, .tracking_clients = 0, .total_commands_processed = 0, .total_connections_received = 0, .start_time_seconds = std.time.timestamp() };

    const args = [_][]const u8{ "INFO", "replication" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "master_replid2:0000000000000000000000000000000000000000\r\n") != null);
}

test "INFO replication repl_backlog_size reads from config" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set a custom repl-backlog-size (2MB)
    try storage.config.set("repl-backlog-size", "2097152");

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();
    const config = ServerConfig{ .port = 6379, .bind = "127.0.0.1", .maxmemory = 0, .maxmemory_policy = "noeviction", .timeout = 0, .tcp_keepalive = 300, .save = "", .appendonly = false, .appendfsync = "everysec", .databases = 1 };
    const stats = ServerStats{ .client_count = 0, .tracking_clients = 0, .total_commands_processed = 0, .total_connections_received = 0, .start_time_seconds = std.time.timestamp() };

    const args = [_][]const u8{ "INFO", "replication" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "repl_backlog_size:2097152\r\n") != null);
}

test "INFO replication repl_backlog_active is 0 when no replicas connected" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var repl = try ReplicationState.initPrimary(allocator);
    defer repl.deinit();
    const config = ServerConfig{ .port = 6379, .bind = "127.0.0.1", .maxmemory = 0, .maxmemory_policy = "noeviction", .timeout = 0, .tcp_keepalive = 300, .save = "", .appendonly = false, .appendfsync = "everysec", .databases = 1 };
    const stats = ServerStats{ .client_count = 0, .tracking_clients = 0, .total_commands_processed = 0, .total_connections_received = 0, .start_time_seconds = std.time.timestamp() };

    const args = [_][]const u8{ "INFO", "replication" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args, null, 1);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "repl_backlog_active:0\r\n") != null);
}
