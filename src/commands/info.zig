const std = @import("std");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");
const repl_mod = @import("../storage/replication.zig");

const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;
const ReplicationState = repl_mod.ReplicationState;

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
    total_commands_processed: u64,
    total_connections_received: u64,
    start_time_seconds: i64,
};

/// INFO [section]
///
/// Returns detailed information about the Redis-compatible server.
/// Supported sections: server, clients, memory, persistence, stats, replication, cpu, keyspace, all, default
///
/// Iteration 30: Comprehensive INFO implementation with all major sections.
pub fn cmdInfo(
    allocator: std.mem.Allocator,
    storage: *Storage,
    repl: *ReplicationState,
    config: ServerConfig,
    stats: ServerStats,
    args: []const []const u8,
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
        try buildServerSection(&buf, allocator, config, stats.start_time_seconds);
    }
    if (show_all or show_default or std.mem.eql(u8, section, "CLIENTS")) {
        try buildClientsSection(&buf, allocator, stats.client_count);
    }
    if (show_all or show_default or std.mem.eql(u8, section, "MEMORY")) {
        try buildMemorySection(&buf, allocator, storage);
    }
    if (show_all or show_default or std.mem.eql(u8, section, "PERSISTENCE")) {
        try buildPersistenceSection(&buf, allocator, config);
    }
    if (show_all or show_default or std.mem.eql(u8, section, "STATS")) {
        try buildStatsSection(&buf, allocator, stats.total_commands_processed, stats.total_connections_received, storage);
    }
    if (show_all or show_default or std.mem.eql(u8, section, "REPLICATION")) {
        try buildReplicationSection(&buf, allocator, repl);
    }
    if (show_all or show_default or std.mem.eql(u8, section, "CPU")) {
        try buildCpuSection(&buf, allocator);
    }
    if (show_all or show_default or std.mem.eql(u8, section, "KEYSPACE")) {
        try buildKeyspaceSection(&buf, allocator, storage);
    }

    const info = try buf.toOwnedSlice(allocator);
    defer allocator.free(info);
    return w.writeBulkString(info);
}

fn buildServerSection(
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    config: ServerConfig,
    start_time_seconds: i64,
) !void {
    const bw = buf.writer(allocator);

    try bw.writeAll("# Server\r\n");
    try bw.writeAll("redis_version:7.2.0\r\n");
    try bw.writeAll("redis_git_sha1:00000000\r\n");
    try bw.writeAll("redis_git_dirty:0\r\n");
    try bw.writeAll("redis_build_id:zoltraak\r\n");
    try bw.writeAll("redis_mode:standalone\r\n");
    try bw.writeAll("os:");
    try bw.writeAll(@tagName(@import("builtin").os.tag));
    try bw.writeAll("\r\n");
    try bw.writeAll("arch_bits:");
    try bw.print("{d}\r\n", .{@bitSizeOf(usize)});
    try bw.writeAll("multiplexing_api:kqueue\r\n"); // Simplified for macOS/BSD
    try bw.writeAll("gcc_version:0.0.0\r\n");
    try bw.writeAll("process_id:");
    if (comptime @import("builtin").os.tag == .linux) {
        try bw.print("{d}\r\n", .{std.os.linux.getpid()});
    } else {
        try bw.print("{d}\r\n", .{std.c.getpid()});
    }
    try bw.writeAll("run_id:zoltraak-instance-001\r\n");
    try bw.print("tcp_port:{d}\r\n", .{config.port});
    try bw.print("uptime_in_seconds:{d}\r\n", .{std.time.timestamp() - start_time_seconds});
    try bw.print("uptime_in_days:{d}\r\n", .{@divTrunc(std.time.timestamp() - start_time_seconds, 86400)});
    try bw.writeAll("hz:10\r\n");
    try bw.writeAll("configured_hz:10\r\n");
    try bw.writeAll("lru_clock:");
    // Redis LRU clock: Unix time in seconds, truncated to 24 bits (modulo 2^24)
    try bw.print("{d}\r\n", .{@as(u32, @intCast(@as(u64, @intCast(std.time.timestamp())) & 0xFFFFFF))});
    try bw.writeAll("executable:/Users/fn/Desktop/codespace/zoltraak/zig-out/bin/zoltraak\r\n");
    try bw.writeAll("config_file:\r\n");
    try bw.writeAll("\r\n");
}

fn buildClientsSection(
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    client_count: usize,
) !void {
    const bw = buf.writer(allocator);

    try bw.writeAll("# Clients\r\n");
    try bw.print("connected_clients:{d}\r\n", .{client_count});
    try bw.writeAll("client_recent_max_input_buffer:0\r\n");
    try bw.writeAll("client_recent_max_output_buffer:0\r\n");
    try bw.writeAll("blocked_clients:0\r\n");
    try bw.writeAll("tracking_clients:0\r\n");
    try bw.writeAll("clients_in_timeout_table:0\r\n");
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

    var fmt_buf1: [32]u8 = undefined;
    var fmt_buf2: [32]u8 = undefined;
    try bw.writeAll("# Memory\r\n");
    try bw.print("used_memory:{d}\r\n", .{total_memory});
    try bw.print("used_memory_human:{s}\r\n", .{formatBytes(total_memory, &fmt_buf1)});
    try bw.print("used_memory_rss:{d}\r\n", .{total_memory * 2});
    try bw.print("used_memory_rss_human:{s}\r\n", .{formatBytes(total_memory * 2, &fmt_buf2)});
    try bw.writeAll("used_memory_peak:0\r\n");
    try bw.writeAll("used_memory_peak_human:0B\r\n");
    try bw.writeAll("used_memory_overhead:0\r\n");
    try bw.writeAll("used_memory_startup:0\r\n");
    try bw.writeAll("used_memory_dataset:0\r\n");
    try bw.print("total_system_memory:{d}\r\n", .{1024 * 1024 * 1024}); // Placeholder: 1GB
    try bw.writeAll("total_system_memory_human:1G\r\n");
    try bw.writeAll("maxmemory:0\r\n");
    try bw.writeAll("maxmemory_human:0B\r\n");
    try bw.writeAll("maxmemory_policy:noeviction\r\n");
    try bw.writeAll("mem_fragmentation_ratio:1.00\r\n");
    try bw.writeAll("mem_allocator:zig\r\n");
    try bw.writeAll("\r\n");
}

fn buildPersistenceSection(
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    config: ServerConfig,
) !void {
    const bw = buf.writer(allocator);

    try bw.writeAll("# Persistence\r\n");
    try bw.writeAll("loading:0\r\n");
    try bw.writeAll("rdb_changes_since_last_save:0\r\n");
    try bw.writeAll("rdb_bgsave_in_progress:0\r\n");
    try bw.writeAll("rdb_last_save_time:0\r\n");
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
    try bw.writeAll("expired_keys:0\r\n");
    try bw.print("evicted_keys:{d}\r\n", .{storage.getEvictedKeysCount()});
    try bw.print("lazyfree_pending_objects:{d}\r\n", .{storage.lazyfree_task.getPendingCount()});
    try bw.writeAll("keyspace_hits:0\r\n");
    try bw.writeAll("keyspace_misses:0\r\n");
    try bw.writeAll("pubsub_channels:0\r\n");
    try bw.writeAll("pubsub_patterns:0\r\n");
    try bw.writeAll("latest_fork_usec:0\r\n");
    try bw.writeAll("\r\n");
}

fn buildReplicationSection(
    buf: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    repl: *ReplicationState,
) !void {
    const bw = buf.writer(allocator);

    try bw.writeAll("# Replication\r\n");

    const role_str: []const u8 = switch (repl.role) {
        .primary => "master",
        .replica => "slave",
    };
    try bw.print("role:{s}\r\n", .{role_str});
    try bw.print("master_replid:{s}\r\n", .{repl.replid});
    try bw.print("master_repl_offset:{d}\r\n", .{repl.repl_offset});

    if (repl.role == .primary) {
        try bw.print("connected_slaves:{d}\r\n", .{repl.replicas.items.len});
        for (repl.replicas.items, 0..) |r, i| {
            const state_str: []const u8 = switch (r.state) {
                .handshake => "wait_bgsave",
                .rdb_transfer => "send_bulk",
                .online => "online",
            };
            try bw.print("slave{d}:ip=127.0.0.1,port={d},state={s},offset={d},lag=0\r\n", .{
                i,
                6379 + i + 1, // Placeholder port
                state_str,
                r.repl_offset,
            });
        }
    } else {
        const link_status = if (repl.primary_link_up) "up" else "down";
        try bw.print("master_host:{s}\r\n", .{repl.primary_host orelse "unknown"});
        try bw.print("master_port:{d}\r\n", .{repl.primary_port});
        try bw.print("master_link_status:{s}\r\n", .{link_status});
        try bw.writeAll("master_last_io_seconds_ago:0\r\n");
        try bw.writeAll("master_sync_in_progress:0\r\n");
    }

    try bw.writeAll("second_repl_offset:-1\r\n");
    try bw.writeAll("repl_backlog_active:0\r\n");
    try bw.writeAll("repl_backlog_size:1048576\r\n");
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
) !void {
    const bw = buf.writer(allocator);

    try bw.writeAll("# Keyspace\r\n");

    // Count keys and keys with expiry under mutex to prevent data races
    var total_keys: usize = 0;
    var keys_with_expiry: usize = 0;
    {
        storage.mutex.lock();
        defer storage.mutex.unlock();
        var iter = storage.data.iterator();
        while (iter.next()) |entry| {
            total_keys += 1;
            if (entry.value_ptr.*.getExpiration()) |_| {
                keys_with_expiry += 1;
            }
        }
    }

    if (total_keys > 0) {
        try bw.print("db0:keys={d},expires={d},avg_ttl=0\r\n", .{ total_keys, keys_with_expiry });
    }

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
        .total_commands_processed = 100,
        .total_connections_received = 50,
        .start_time_seconds = 1000000,
    };

    const args = [_][]const u8{ "INFO", "SERVER" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args);
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
        .total_commands_processed = 100,
        .total_connections_received = 50,
        .start_time_seconds = 1000000,
    };

    const args = [_][]const u8{ "INFO", "CLIENTS" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args);
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
        .total_commands_processed = 100,
        .total_connections_received = 50,
        .start_time_seconds = 1000000,
    };

    const args = [_][]const u8{ "INFO", "KEYSPACE" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args);
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
        .total_commands_processed = 100,
        .total_connections_received = 50,
        .start_time_seconds = 1000000,
    };

    const args = [_][]const u8{"INFO"};
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args);
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
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = now,
    };

    const args = [_][]const u8{ "INFO", "SERVER" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args);
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
        .total_commands_processed = 0,
        .total_connections_received = 0,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "SERVER" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args);
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
        .total_commands_processed = 42,
        .total_connections_received = 7,
        .start_time_seconds = std.time.timestamp(),
    };

    const args = [_][]const u8{ "INFO", "STATS" };
    const result = try cmdInfo(allocator, &storage, &repl, config, stats, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "total_commands_processed:42") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "total_connections_received:7") != null);
}
