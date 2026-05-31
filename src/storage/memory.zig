const std = @import("std");
const zuda = @import("zuda");
const config_mod = @import("config.zig");
const blocking_mod = @import("blocking.zig");
const slowlog_mod = @import("slowlog.zig");
const latency_mod = @import("latency.zig");
const memory_tracker_mod = @import("memory_tracker.zig");
const heavykeeper_mod = @import("heavykeeper.zig");
const acl_mod = @import("acl.zig");
const cluster_mod = @import("cluster.zig");
const sentinel_mod = @import("sentinel.zig");
const functions_mod = @import("functions.zig");
const json_value_mod = @import("json_value.zig");
const search_mod = @import("search.zig");
const timeseries_mod = @import("timeseries.zig");
const bloom_mod = @import("bloom.zig");
const cuckoo_mod = @import("cuckoo.zig");
const cms_mod = @import("cms.zig");
const topk_mod = @import("topk.zig");
const tdigest_mod = @import("tdigest.zig");
const vector_mod = @import("vector.zig");
const notifications_mod = @import("notifications.zig");
const pubsub_mod = @import("pubsub.zig");
const eviction_mod = @import("eviction.zig");
const lazyfree_mod = @import("lazyfree.zig");
const defrag_mod = @import("defrag.zig");
const tls_config_mod = @import("tls_config.zig");
const modules_mod = @import("modules.zig");
const intset_mod = @import("intset.zig");

pub const Config = config_mod.Config;
pub const TlsConfig = tls_config_mod.TlsConfig;
pub const BlockingQueue = blocking_mod.BlockingQueue;
pub const BlockedClient = blocking_mod.BlockedClient;
pub const ACLStore = acl_mod.ACLStore;
pub const BlockedXreadgroupClient = blocking_mod.BlockedXreadgroupClient;
pub const SlowLog = slowlog_mod.SlowLog;
pub const LatencyMonitor = latency_mod.LatencyMonitor;
pub const MemoryTracker = memory_tracker_mod.MemoryTracker;
pub const ClusterState = cluster_mod.ClusterState;
pub const SentinelState = sentinel_mod.SentinelState;
pub const FunctionStore = functions_mod.FunctionStore;
pub const SearchStore = search_mod.SearchStore;
pub const TimeSeriesValue = timeseries_mod.TimeSeriesValue;
pub const BloomFilterValue = bloom_mod.BloomFilterValue;
pub const CuckooFilterValue = cuckoo_mod.CuckooFilterValue;
pub const CountMinSketchValue = cms_mod.CountMinSketchValue;
pub const TopKValue = topk_mod.TopKValue;
pub const TDigestValue = tdigest_mod.TDigestValue;
pub const VectorSetValue = vector_mod.VectorSetValue;
pub const LRUClock = eviction_mod.LRUClock;
pub const LFUCounter = eviction_mod.LFUCounter;
pub const EvictionPolicy = eviction_mod.EvictionPolicy;
pub const LazyFreeTask = lazyfree_mod.LazyFreeTask;
pub const LazyFreeWork = lazyfree_mod.LazyFreeWork;
pub const LazyFreeWorkType = lazyfree_mod.LazyFreeWorkType;
pub const DefragTask = defrag_mod.DefragTask;
pub const ModuleStore = modules_mod.ModuleStore;

/// Per-command call statistics tracked for INFO commandstats
pub const CommandStatEntry = struct {
    calls: u64 = 0,
    usec: u64 = 0,
    rejected_calls: u64 = 0,
    failed_calls: u64 = 0,
};

pub const CommandStatSnapshot = struct { name: []const u8, entry: CommandStatEntry };
pub const ErrorStatSnapshot = struct { error_type: []const u8, count: u64 };

/// Mode for XACKDEL and XDELEX commands
pub const XRefMode = enum {
    keepref, // Default: preserve PEL references
    delref, // Aggressive: remove all PEL references
    acked, // Safe: only delete if all groups acknowledged
};

/// Range unit for BITPOS command (Redis 7.0.0+)
pub const RangeUnit = enum {
    byte, // Default: start/end are byte indices
    bit, // Redis 7.0+: start/end are bit indices
};

/// Options for XADD command (Redis 7.x+)
pub const XAddOptions = struct {
    nomkstream: bool = false, // If true, return null instead of creating stream
    maxlen: ?usize = null, // Trim to at most this many entries
    minid_str: ?[]const u8 = null, // Trim entries with ID strictly less than this
    approx: bool = false, // ~ vs = (approximate/exact trimming)
    limit: ?usize = null, // Max entries to delete per trimming call
};

/// Type of value stored in the key-value store
pub const ValueType = enum {
    string,
    list,
    set,
    hash,
    sorted_set,
    stream,
    hyperloglog,
    json,
    timeseries,
    bloom,
    cuckoo,
    count_min_sketch,
    top_k,
    t_digest,
    vector_set,
};

/// Value stored in the key-value store with optional expiration
/// Tagged union supporting multiple Redis data types
pub const Value = union(ValueType) {
    string: StringValue,
    list: ListValue,
    set: SetValue,
    hash: HashValue,
    sorted_set: SortedSetValue,
    stream: StreamValue,
    hyperloglog: HyperLogLogValue,
    json: JsonValue,
    timeseries: TimeSeriesValue,
    bloom: BloomFilterValue,
    cuckoo: CuckooFilterValue,
    count_min_sketch: CountMinSketchValue,
    top_k: TopKValue,
    t_digest: TDigestValue,
    vector_set: VectorSetValue,

    /// String value with optional expiration
    pub const StringValue = struct {
        data: []const u8,
        expires_at: ?i64, // Unix timestamp in milliseconds, null = no expiration

        pub fn deinit(self: *StringValue, allocator: std.mem.Allocator) void {
            allocator.free(self.data);
        }
    };

    /// List value with optional expiration
    pub const ListValue = struct {
        data: std.ArrayList([]const u8),
        expires_at: ?i64,

        pub fn deinit(self: *ListValue, allocator: std.mem.Allocator) void {
            // Free all element strings
            for (self.data.items) |element| {
                allocator.free(element);
            }
            // Free the ArrayList
            self.data.deinit(allocator);
        }
    };

    /// Set encoding type
    pub const SetEncoding = enum {
        intset, // Compact sorted integer array (for small integer-only sets)
        hashmap, // String hashmap (for large sets or sets with non-integers)
    };

    /// Set value with optional expiration and automatic encoding optimization
    /// Uses intset for small integer-only sets, hashmap for larger or mixed sets
    pub const SetValue = struct {
        encoding: SetEncoding,
        data: union {
            intset: intset_mod.IntSet,
            hashmap: std.StringHashMap(void),
        },
        expires_at: ?i64,

        pub fn deinit(self: *SetValue, allocator: std.mem.Allocator) void {
            switch (self.encoding) {
                .intset => {
                    self.data.intset.deinit();
                },
                .hashmap => {
                    // Free all member strings (keys in the hash map)
                    var it = self.data.hashmap.keyIterator();
                    while (it.next()) |key| {
                        allocator.free(key.*);
                    }
                    self.data.hashmap.deinit();
                },
            }
        }

        /// Convert intset encoding to hashmap encoding
        /// Called when adding non-integer or exceeding threshold
        fn promoteToHashmap(self: *SetValue, allocator: std.mem.Allocator) !void {
            if (self.encoding == .hashmap) return;

            // Convert all integers to strings in hashmap
            var hashmap = std.StringHashMap(void).init(allocator);
            errdefer {
                var it = hashmap.keyIterator();
                while (it.next()) |key| {
                    allocator.free(key.*);
                }
                hashmap.deinit();
            }

            // Get all integers from intset
            const values = try self.data.intset.toSlice(allocator);
            defer allocator.free(values);

            // Convert each to string and add to hashmap
            for (values) |value| {
                const str = try std.fmt.allocPrint(allocator, "{d}", .{value});
                errdefer allocator.free(str);
                try hashmap.put(str, {});
            }

            // Free old intset
            self.data.intset.deinit();

            // Replace with hashmap
            self.encoding = .hashmap;
            self.data = .{ .hashmap = hashmap };
        }

        /// Check if member exists (handles both encodings)
        pub fn contains(self: *const SetValue, member: []const u8) !bool {
            return switch (self.encoding) {
                .intset => blk: {
                    const int_val = std.fmt.parseInt(i64, member, 10) catch break :blk false;
                    break :blk try self.data.intset.contains(int_val);
                },
                .hashmap => self.data.hashmap.contains(member),
            };
        }

        /// Get cardinality (handles both encodings)
        pub fn count(self: *const SetValue) usize {
            return switch (self.encoding) {
                .intset => self.data.intset.length,
                .hashmap => self.data.hashmap.count(),
            };
        }
    };

    /// Hash field value with optional per-field expiration
    pub const FieldValue = struct {
        data: []const u8,
        expires_at: ?i64, // Unix timestamp in milliseconds, null = no expiration

        pub fn deinit(self: *FieldValue, allocator: std.mem.Allocator) void {
            allocator.free(self.data);
        }
    };

    /// Hash value with optional key-level expiration and per-field expiration
    /// Maps field names to field values (data + optional TTL)
    pub const HashValue = struct {
        data: std.StringHashMap(FieldValue),
        expires_at: ?i64, // Key-level expiration

        pub fn deinit(self: *HashValue, allocator: std.mem.Allocator) void {
            // Free all field names (keys) and field values
            var it = self.data.iterator();
            while (it.next()) |entry| {
                allocator.free(entry.key_ptr.*);
                var field_val = entry.value_ptr.*;
                field_val.deinit(allocator);
            }
            self.data.deinit();
        }
    };

    /// A single scored member in a sorted set
    pub const ScoredMember = struct {
        score: f64,
        member: []const u8,
    };

    /// Sorted set value with optional expiration
    /// Uses dual-structure: hash map for O(1) member lookups, sorted list for range queries
    pub const SortedSetValue = struct {
        /// Member -> Score mapping for O(1) lookups
        members: std.StringHashMap(f64),
        /// Sorted list of (score, member) pairs, ordered by score then lexicographically
        sorted_list: std.ArrayList(ScoredMember),
        expires_at: ?i64,

        pub fn deinit(self: *SortedSetValue, allocator: std.mem.Allocator) void {
            // Free all member strings (keys in the hash map)
            var it = self.members.keyIterator();
            while (it.next()) |key| {
                allocator.free(key.*);
            }
            self.members.deinit();
            // sorted_list items are owned by the hash map, just free the list
            self.sorted_list.deinit(allocator);
        }
    };

    /// Stream entry ID in millisecondTimestamp-sequenceNumber format
    pub const StreamId = struct {
        ms: i64,
        seq: u64,

        /// Parse ID from string like "1234567890-0", "ms-*" (partial auto-seq), or "*" (full auto)
        pub fn parse(s: []const u8, last_id: ?StreamId) !StreamId {
            if (std.mem.eql(u8, s, "*")) {
                // Auto-generate ID based on current time
                const now = std.time.milliTimestamp();
                const ms = if (last_id) |lid| @max(now, lid.ms) else now;
                const seq = if (last_id) |lid| (if (ms == lid.ms) lid.seq + 1 else 0) else 0;
                return StreamId{ .ms = ms, .seq = seq };
            }

            // Parse "ms-seq" or "ms-*" (partial auto-seq) format
            const dash = std.mem.indexOf(u8, s, "-") orelse return error.InvalidStreamId;
            const ms = try std.fmt.parseInt(i64, s[0..dash], 10);
            const seq_str = s[dash + 1 ..];
            if (std.mem.eql(u8, seq_str, "*")) {
                // Partial ID: ms is given, seq is auto-generated
                const seq = if (last_id) |lid|
                    (if (ms == lid.ms) lid.seq + 1 else 0)
                else
                    0;
                return StreamId{ .ms = ms, .seq = seq };
            }
            const seq = try std.fmt.parseInt(u64, seq_str, 10);
            return StreamId{ .ms = ms, .seq = seq };
        }

        /// Compare two stream IDs
        pub fn lessThan(a: StreamId, b: StreamId) bool {
            if (a.ms != b.ms) return a.ms < b.ms;
            return a.seq < b.seq;
        }

        /// Check if two stream IDs are equal
        pub fn equals(a: StreamId, b: StreamId) bool {
            return a.ms == b.ms and a.seq == b.seq;
        }

        /// Format ID as "ms-seq" string
        pub fn format(self: StreamId, allocator: std.mem.Allocator) ![]const u8 {
            return std.fmt.allocPrint(allocator, "{d}-{d}", .{ self.ms, self.seq });
        }
    };

    /// Single entry in a stream
    pub const StreamEntry = struct {
        id: StreamId,
        /// Field-value pairs stored as flat array: [field1, value1, field2, value2, ...]
        fields: std.ArrayList([]const u8),

        pub fn deinit(self: *StreamEntry, allocator: std.mem.Allocator) void {
            for (self.fields.items) |item| {
                allocator.free(item);
            }
            self.fields.deinit(allocator);
        }
    };

    /// Pending entry in a consumer group
    pub const PendingEntry = struct {
        id: StreamId,
        consumer: []const u8,
        delivery_time: i64, // Unix timestamp in ms
        delivery_count: u64,

        pub fn deinit(self: *PendingEntry, allocator: std.mem.Allocator) void {
            allocator.free(self.consumer);
        }
    };

    /// Consumer in a consumer group
    pub const Consumer = struct {
        name: []const u8,
        pending: std.ArrayList(StreamId), // IDs of messages pending acknowledgment
        last_attempted_time: i64, // Milliseconds since epoch - last XREADGROUP/XCLAIM attempt (for 'idle')
        last_successful_time: i64, // Milliseconds since epoch - last successful operation (for 'inactive')
        creation_time: i64, // Milliseconds since epoch - consumer creation time

        pub fn deinit(self: *Consumer, allocator: std.mem.Allocator) void {
            allocator.free(self.name);
            self.pending.deinit(allocator);
        }
    };

    /// Consumer group for stream
    pub const ConsumerGroup = struct {
        name: []const u8,
        last_delivered_id: StreamId, // Last ID delivered to any consumer
        consumers: std.StringHashMap(Consumer), // consumer_name -> Consumer
        pending: std.ArrayList(PendingEntry), // All pending entries across all consumers
        entries_read: u64, // Logical read counter - total entries delivered to this group
        creation_time: i64, // Milliseconds since epoch - group creation time
        arbitrary_start: bool, // True if group started at arbitrary position (affects lag calculation)

        pub fn deinit(self: *ConsumerGroup, allocator: std.mem.Allocator) void {
            allocator.free(self.name);
            var it = self.consumers.valueIterator();
            while (it.next()) |consumer| {
                var copy = consumer.*;
                copy.deinit(allocator);
            }
            self.consumers.deinit();
            for (self.pending.items) |*entry| {
                entry.deinit(allocator);
            }
            self.pending.deinit(allocator);
        }
    };

    /// Stream value with optional expiration
    /// Ordered log of entries with unique IDs
    pub const StreamValue = struct {
        entries: std.ArrayList(StreamEntry),
        last_id: ?StreamId,
        expires_at: ?i64,
        consumer_groups: std.StringHashMap(ConsumerGroup), // group_name -> ConsumerGroup
        entries_added: u64, // Total entries ever added to this stream (for lag calculation)
        max_deleted_entry_id: StreamId, // Highest deleted entry ID (for compaction tracking)
        // IDMP (Idempotent Message Processing) configuration (Redis 8.6+)
        idmp_duration_sec: u32 = 100, // Default: 100 seconds (range: 1-86400)
        idmp_maxsize: u32 = 100, // Default: 100 entries per producer (range: 1-10000)
        // Note: IDMP map is not yet implemented (requires producer ID tracking in XADD)

        pub fn deinit(self: *StreamValue, allocator: std.mem.Allocator) void {
            for (self.entries.items) |*entry| {
                entry.deinit(allocator);
            }
            self.entries.deinit(allocator);

            var it = self.consumer_groups.valueIterator();
            while (it.next()) |group| {
                var copy = group.*;
                copy.deinit(allocator);
            }
            self.consumer_groups.deinit();
        }
    };

    /// HyperLogLog value for cardinality estimation
    /// Uses 16384 registers (14-bit precision, standard Redis configuration)
    /// Migrated to zuda.containers.probabilistic.HyperLogLog
    pub const HyperLogLogValue = struct {
        registers: [16384]u8, // Fixed array for Redis wire protocol compatibility (RDB/AOF)
        expires_at: ?i64,

        pub fn deinit(_: *HyperLogLogValue, _: std.mem.Allocator) void {
            // Registers are fixed-size array, no dynamic allocation to clean up
        }

        /// Initialize a new HyperLogLog with all registers set to 0
        pub fn init() HyperLogLogValue {
            return HyperLogLogValue{
                .registers = [_]u8{0} ** 16384,
                .expires_at = null,
            };
        }

        /// Hash function context for zuda HyperLogLog
        const HashContext = struct {};

        /// Hash function wrapper for zuda HyperLogLog
        fn hashFn(_: HashContext, element: []const u8) u64 {
            return std.hash.Murmur2_64.hash(element);
        }

        /// Internal type alias for zuda HyperLogLog
        const ZudaHLL = zuda.containers.probabilistic.HyperLogLog([]const u8, HashContext, hashFn);

        /// Add an element to the HyperLogLog
        /// Returns true if the register was updated
        pub fn add(self: *HyperLogLogValue, element: []const u8) bool {
            const hash = std.hash.Murmur2_64.hash(element);

            // Use first 14 bits for register index (0-16383)
            const register_index = @as(u16, @truncate(hash & 0x3FFF));

            // Count leading zeros in remaining 50 bits, add 1
            const remaining = hash >> 14;
            const leading_zeros = @clz(remaining | 1); // | 1 prevents all-zeros
            const rank = @as(u8, @intCast(50 - leading_zeros + 1));

            // Update register if new rank is higher (max 6-bit value = 63)
            const capped_rank = @min(rank, 63);
            if (capped_rank > self.registers[register_index]) {
                self.registers[register_index] = capped_rank;
                return true;
            }
            return false;
        }

        /// Estimate cardinality using zuda's HyperLogLog algorithm
        pub fn count(self: *const HyperLogLogValue) u64 {
            // Use zuda's cardinality estimation algorithm
            // We need to create a temporary zuda HLL and copy our registers
            // Since we maintain the same register layout, we can use zuda's count logic directly

            // Standard HyperLogLog cardinality estimation (using zuda's algorithm)
            const m: f64 = 16384.0; // number of registers
            const alpha: f64 = 0.7213 / (1.0 + 1.079 / m); // bias correction

            var raw_sum: f64 = 0.0;
            var zero_count: u32 = 0;

            for (self.registers) |register_val| {
                raw_sum += std.math.pow(f64, 2.0, -@as(f64, @floatFromInt(register_val)));
                if (register_val == 0) zero_count += 1;
            }

            var estimate = alpha * m * m / raw_sum;

            // Small range correction (LinearCounting) - matches zuda's algorithm
            if (estimate <= 2.5 * m) {
                if (zero_count > 0) {
                    const zeros_float: f64 = @floatFromInt(zero_count);
                    estimate = m * @log(m / zeros_float);
                }
            }
            // Large range correction (for hash collisions beyond 2^32) - matches zuda's algorithm
            else if (estimate > (1.0 / 30.0) * std.math.pow(f64, 2.0, 32.0)) {
                estimate = -std.math.pow(f64, 2.0, 32.0) * @log(1.0 - estimate / std.math.pow(f64, 2.0, 32.0));
            }

            return @intFromFloat(@max(0, @round(estimate)));
        }

        /// Merge another HyperLogLog into this one
        /// Takes the maximum value for each register
        pub fn merge(self: *HyperLogLogValue, other: *const HyperLogLogValue) void {
            for (&self.registers, other.registers) |*reg, other_reg| {
                reg.* = @max(reg.*, other_reg);
            }
        }
    };

    /// JSON value with optional expiration
    pub const JsonValue = struct {
        root: *json_value_mod.JsonNode,
        expires_at: ?i64,
        allocator: std.mem.Allocator,

        pub fn deinit(self: *JsonValue, _: std.mem.Allocator) void {
            self.root.deinit(self.allocator);
            self.allocator.destroy(self.root);
        }
    };

    /// Deinitialize value and free all associated memory
    pub fn deinit(self: *Value, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .string => |*s| s.deinit(allocator),
            .list => |*l| l.deinit(allocator),
            .set => |*s| s.deinit(allocator),
            .hash => |*h| h.deinit(allocator),
            .sorted_set => |*z| z.deinit(allocator),
            .stream => |*st| st.deinit(allocator),
            .hyperloglog => |*hll| hll.deinit(allocator),
            .json => |*j| j.deinit(allocator),
            .timeseries => |*ts| ts.deinit(),
            .bloom => |*b| b.deinit(),
            .cuckoo => |*c| c.deinit(),
            .count_min_sketch => |*cms| cms.deinit(),
            .top_k => |*tk| tk.deinit(),
            .t_digest => |*td| td.deinit(),
            .vector_set => |*vs| vs.deinit(),
        }
    }

    /// Get expiration timestamp for any value type
    pub fn getExpiration(self: Value) ?i64 {
        return switch (self) {
            .string => |s| s.expires_at,
            .list => |l| l.expires_at,
            .set => |s| s.expires_at,
            .hash => |h| h.expires_at,
            .sorted_set => |z| z.expires_at,
            .stream => |st| st.expires_at,
            .hyperloglog => |hll| hll.expires_at,
            .json => |j| j.expires_at,
            .timeseries => |ts| ts.expires_at,
            .bloom => |b| b.expires_at,
            .cuckoo => |c| c.expires_at,
            .count_min_sketch => null, // CMS doesn't support expiration
            .top_k => |tk| tk.expires_at,
            .t_digest => null, // T-Digest doesn't support expiration yet
            .vector_set => null, // Vector sets don't support expiration yet
        };
    }

    /// Check if value is expired
    pub fn isExpired(self: Value, now: i64) bool {
        const exp = self.getExpiration() orelse return false;
        return now >= exp;
    }
};

// ── Lexicographical range helpers for sorted sets ─────────────────────────

/// Lexicographical range boundary type
const LexRangeType = enum {
    neg_infinity, // "-" represents negative infinity
    pos_infinity, // "+" represents positive infinity
    inclusive, // "[value" inclusive boundary
    exclusive, // "(value" exclusive boundary
};

/// Parsed lexicographical range boundary
const LexRange = struct {
    type: LexRangeType,
    value: []const u8, // Empty for infinity types
};

/// Parse a lexicographical range string (e.g., "-", "+", "[abc", "(xyz")
fn parseLexRange(s: []const u8) !LexRange {
    if (s.len == 0) return error.InvalidLexRange;

    if (std.mem.eql(u8, s, "-")) {
        return LexRange{ .type = .neg_infinity, .value = "" };
    }
    if (std.mem.eql(u8, s, "+")) {
        return LexRange{ .type = .pos_infinity, .value = "" };
    }

    if (s[0] == '[' and s.len > 1) {
        return LexRange{ .type = .inclusive, .value = s[1..] };
    }
    if (s[0] == '(' and s.len > 1) {
        return LexRange{ .type = .exclusive, .value = s[1..] };
    }

    return error.InvalidLexRange;
}

/// Check if a member is within the lexicographical range [min, max]
fn inLexRange(member: []const u8, min: LexRange, max: LexRange) bool {
    // Check min boundary
    const min_ok = switch (min.type) {
        .neg_infinity => true,
        .pos_infinity => false,
        .inclusive => std.mem.order(u8, member, min.value) != .lt,
        .exclusive => std.mem.order(u8, member, min.value) == .gt,
    };

    if (!min_ok) return false;

    // Check max boundary
    const max_ok = switch (max.type) {
        .neg_infinity => false,
        .pos_infinity => true,
        .inclusive => std.mem.order(u8, member, max.value) != .gt,
        .exclusive => std.mem.order(u8, member, max.value) == .lt,
    };

    return max_ok;
}

/// HotkeyTracker tracks frequently accessed keys for monitoring
/// Phase 1: State management infrastructure (complete)
/// Phase 2: HeavyKeeper probabilistic top-K tracking (current)
pub const HotkeyTracker = struct {
    allocator: std.mem.Allocator,
    is_active: bool,
    metrics_count: u32, // Number of distinct metrics (usually 2: CPU + NET)
    track_cpu: bool,
    track_net: bool,
    top_k: u32, // Number of top keys to track (default 10)
    sample_ratio: u32, // Sampling ratio (1 to 100, default 100 = always sample)
    duration_ms: ?u64, // Duration in milliseconds (null = indefinite)
    start_time_ms: i64, // Timestamp when tracking started

    // Phase 2: HeavyKeeper for probabilistic top-K tracking
    heavy_keeper: heavykeeper_mod.HeavyKeeper, // Top-K frequency tracker

    // Aggregated metrics counters
    keys_sampled: u64, // Total number of key accesses sampled
    total_cpu_us: u64, // Total CPU microseconds tracked
    total_net_bytes: u64, // Total network bytes tracked

    /// Configuration for HotkeyTracker
    pub const TrackerConfig = struct {
        metrics_count: u32,
        track_cpu: bool,
        track_net: bool,
        top_k: u32,
        sample_ratio: u32,
        duration_ms: ?u64,
    };

    /// Initialize a new HotkeyTracker
    /// Takes ownership of allocator - caller must call deinit()
    pub fn init(allocator: std.mem.Allocator, config: TrackerConfig) !*HotkeyTracker {
        const tracker = try allocator.create(HotkeyTracker);
        errdefer allocator.destroy(tracker);

        // Initialize HeavyKeeper for top-K tracking
        // Use width=1024, depth=4, decay=0.9 (standard parameters)
        var heavy_keeper = try heavykeeper_mod.HeavyKeeper.init(
            allocator,
            config.top_k,
            1024, // width
            4, // depth
            0.9, // decay
        );
        errdefer heavy_keeper.deinit();

        tracker.* = HotkeyTracker{
            .allocator = allocator,
            .is_active = false,
            .metrics_count = config.metrics_count,
            .track_cpu = config.track_cpu,
            .track_net = config.track_net,
            .top_k = config.top_k,
            .sample_ratio = config.sample_ratio,
            .duration_ms = config.duration_ms,
            .start_time_ms = 0, // Will be set in start()
            .heavy_keeper = heavy_keeper,
            .keys_sampled = 0,
            .total_cpu_us = 0,
            .total_net_bytes = 0,
        };

        return tracker;
    }

    /// Free HotkeyTracker resources
    pub fn deinit(self: *HotkeyTracker) void {
        self.heavy_keeper.deinit();
        self.allocator.destroy(self);
    }

    /// Start tracking (mark as active and record start time)
    pub fn start(self: *HotkeyTracker) void {
        self.is_active = true;
        self.start_time_ms = std.time.milliTimestamp();
    }

    /// Stop tracking (mark as inactive but preserve data)
    pub fn stop(self: *HotkeyTracker) void {
        self.is_active = false;
    }

    /// Reset all counters and data
    pub fn reset(self: *HotkeyTracker) void {
        self.keys_sampled = 0;
        self.total_cpu_us = 0;
        self.total_net_bytes = 0;
        self.start_time_ms = 0;
    }

    /// Record a key access with metrics
    /// Updates HeavyKeeper for top-K tracking and accumulates aggregate counters
    /// key: the key being accessed
    /// cpu_us: CPU microseconds for this operation
    /// net_bytes: Network bytes for this operation
    pub fn recordAccess(self: *HotkeyTracker, key: []const u8, cpu_us: u64, net_bytes: u64) void {
        // Add key to HeavyKeeper with weight 1 (one access)
        _ = self.heavy_keeper.add(key, 1) catch |err| {
            // Log error but don't crash (monitoring shouldn't break commands)
            std.debug.print("HeavyKeeper.add error: {}\n", .{err});
            return;
        };

        if (self.track_cpu) {
            self.total_cpu_us += cpu_us;
        }
        if (self.track_net) {
            self.total_net_bytes += net_bytes;
        }
        self.keys_sampled += 1;
    }

    /// Check if tracking duration has expired
    /// Returns true if duration is set and elapsed time exceeds duration_ms
    pub fn isExpired(self: *HotkeyTracker) bool {
        if (!self.is_active or self.duration_ms == null) {
            return false;
        }

        const now_ms = std.time.milliTimestamp();
        const elapsed_ms = now_ms - self.start_time_ms;
        return elapsed_ms >= self.duration_ms.?;
    }

    /// Get top-K hotkeys (caller owns returned slice and must free with allocator)
    /// Returns error if HeavyKeeper list() fails
    pub fn getTopKeys(self: *HotkeyTracker, allocator: std.mem.Allocator) ![]heavykeeper_mod.HeavyKeeper.HotkeyItem {
        return try self.heavy_keeper.list(allocator);
    }

};

/// Thread-safe in-memory storage engine with TTL support
pub const Storage = struct {
    allocator: std.mem.Allocator,
    data: std.StringHashMap(Value),
    config: *Config,
    tls_config: TlsConfig, // TLS/SSL configuration (Phase 10)
    acl: ?*ACLStore, // ACL user management
    cluster: ClusterState, // Cluster state management
    cluster_config_path: []const u8, // Path to nodes.conf cluster config file
    sentinel: SentinelState, // Sentinel state management
    sentinel_config_path: []const u8, // Path to sentinel.conf Sentinel config file
    functions: FunctionStore, // Redis Functions library storage
    search: SearchStore, // Search index storage
    cuckoo_load_contexts: std.StringHashMap(*cuckoo_mod.CuckooFilterValue.LoadContext), // Load contexts for CF.LOADCHUNK
    mutex: std.Thread.Mutex,
    last_save_time: i64, // Unix timestamp in seconds of last successful RDB save
    blocking_queue: BlockingQueue, // Clients blocked on XREAD/XREADGROUP BLOCK
    slowlog: SlowLog, // Slow query log
    latency_monitor: LatencyMonitor, // Latency event tracking
    memory_tracker: MemoryTracker, // Memory usage tracking
    active_expire_enabled: bool, // Whether active expiration is enabled (default: true)
    notification_flags: std.atomic.Value(u16), // Keyspace notification flags (from notify-keyspace-events config)
    lru_clock: LRUClock, // LRU clock for eviction policies
    lfu_counter: LFUCounter, // LFU counter for eviction policies
    lfu_last_access: std.AutoHashMap(u64, i64), // LFU last access timestamps in milliseconds (key hash -> time_ms)
    evicted_keys: std.atomic.Value(u64), // Atomic counter for total evicted keys
    lazyfree_task: LazyFreeTask, // Background lazy freeing task
    defrag_task: DefragTask, // Background active defragmentation task
    module_store: ModuleStore, // Dynamically loaded modules (Phase 17)
    pubsub_state: ?*pubsub_mod.PubSub, // Optional pubsub state for firing notifications (set by server after init)
    hotkey_tracker: ?*HotkeyTracker, // Optional hotkeys tracking state (Phase 1)
    key_versions: std.StringHashMap(u64), // Per-key modification counters (OBJECT VERSION, Redis 7.4)
    server_start_time: i64, // Unix timestamp (seconds) when this server instance started
    total_commands_processed: std.atomic.Value(u64), // Total commands processed since start
    total_connections_received: std.atomic.Value(u64), // Total client connections received since start
    lazy_expire: std.atomic.Value(bool), // lazyfree-lazy-expire: async delete on expiry access
    lazy_eviction: std.atomic.Value(bool), // lazyfree-lazy-eviction: async delete on maxmemory eviction
    lazy_server_del: std.atomic.Value(bool), // lazyfree-lazy-server-del: async delete on key overwrite
    keyspace_hits: std.atomic.Value(u64), // Successful key lookups (used by INFO stats)
    keyspace_misses: std.atomic.Value(u64), // Failed key lookups (used by INFO stats)
    expired_keys: std.atomic.Value(u64), // Total keys expired (lazy + active) since start
    run_id: [40]u8, // Server instance run ID (40 hex chars, random at startup)
    command_stats_mutex: std.Thread.Mutex, // Protects command_stats
    command_stats: std.StringHashMapUnmanaged(CommandStatEntry), // Per-command call statistics
    error_stats_mutex: std.Thread.Mutex, // Protects error_stats
    error_stats: std.StringHashMapUnmanaged(u64), // Per-error-type occurrence counts
    dirty_count: std.atomic.Value(u64), // Write operations since last RDB save (rdb_changes_since_last_save)
    peak_memory: std.atomic.Value(usize), // Peak memory usage in bytes (used by INFO memory used_memory_peak)

    /// Initialize a new storage instance with runtime configuration.
    ///
    /// Arguments:
    ///   - allocator: Memory allocator for storage and config
    ///   - port: Server port for read-only CONFIG parameter
    ///   - bind: Server bind address for read-only CONFIG parameter
    ///
    /// Returns error.OutOfMemory if allocation fails.
    pub fn init(allocator: std.mem.Allocator, port: u16, bind: []const u8) !*Storage {
        const storage = try allocator.create(Storage);
        errdefer allocator.destroy(storage);

        const cfg = try Config.init(allocator, port, bind);
        errdefer cfg.deinit();

        var latency_mon = try LatencyMonitor.init(allocator);
        errdefer latency_mon.deinit();

        const mem_tracker = MemoryTracker.init();

        // Initialize ACL store with default user
        const acl_store = try allocator.create(ACLStore);
        errdefer allocator.destroy(acl_store);
        acl_store.* = try ACLStore.init(allocator);
        errdefer acl_store.deinit();

        // Initialize cluster state (single-node by default)
        var cluster_state = ClusterState.init(allocator);
        errdefer cluster_state.deinit();

        // Generate random IDs: server run_id + cluster node_id (both 40-char hex strings)
        var prng = std.Random.DefaultPrng.init(@as(u64, @intCast(std.time.microTimestamp())));
        var random = prng.random();

        var run_id: [40]u8 = undefined;
        for (0..20) |i| {
            const byte = random.int(u8);
            _ = std.fmt.bufPrint(run_id[i * 2 .. i * 2 + 2], "{x:0>2}", .{byte}) catch unreachable;
        }

        var node_id: [40]u8 = undefined;
        for (0..20) |i| {
            const byte = random.int(u8);
            _ = std.fmt.bufPrint(node_id[i * 2 .. i * 2 + 2], "{x:0>2}", .{byte}) catch unreachable;
        }

        // Create a single node for this instance
        const node = try allocator.create(cluster_mod.ClusterNode);
        errdefer allocator.destroy(node);

        node.* = try cluster_mod.ClusterNode.init(allocator, node_id, bind, port);
        errdefer node.deinit(allocator);

        // Set as self
        cluster_state.myself = node;
        cluster_state.enabled = false; // Cluster not enabled by default
        cluster_state.state = .ok;
        cluster_state.current_epoch = 1;

        // Add node to cluster nodes map (need to dup the key for the hashmap)
        const node_id_key = try allocator.dupe(u8, &node_id);
        errdefer allocator.free(node_id_key); // Free key if put fails
        try cluster_state.nodes.put(node_id_key, node);

        // Assign all slots to this node
        try cluster_state.assignSlots(node, 0, cluster_mod.CLUSTER_SLOTS - 1);

        // Initialize sentinel state (disabled by default)
        const sentinel_state = sentinel_mod.SentinelState.init(allocator);

        // Initialize function store (empty by default)
        const function_store = functions_mod.FunctionStore.init(allocator);

        // Initialize search store (empty by default)
        const search_store = try search_mod.SearchStore.init(allocator);

        // Initialize lazy free task
        var lazy_free = try LazyFreeTask.init(allocator);
        errdefer lazy_free.deinit();

        // Initialize defrag task
        var defrag_task = try DefragTask.init(allocator);
        errdefer defrag_task.deinit();

        // Initialize TLS config
        var tls_config = try TlsConfig.init(allocator);
        errdefer tls_config.deinit();

        storage.* = Storage{
            .allocator = allocator,
            .data = std.StringHashMap(Value).init(allocator),
            .config = cfg,
            .tls_config = tls_config,
            .acl = acl_store,
            .cluster = cluster_state,
            .cluster_config_path = "nodes.conf", // Default cluster config file
            .sentinel = sentinel_state,
            .sentinel_config_path = "sentinel.conf", // Default sentinel config file
            .functions = function_store,
            .search = search_store,
            .cuckoo_load_contexts = std.StringHashMap(*cuckoo_mod.CuckooFilterValue.LoadContext).init(allocator),
            .mutex = std.Thread.Mutex{},
            .last_save_time = 0, // Will be updated on first save
            .blocking_queue = BlockingQueue.init(allocator),
            .slowlog = SlowLog.init(allocator, 128, 10000), // max 128 entries, 10ms threshold
            .latency_monitor = latency_mon,
            .memory_tracker = mem_tracker,
            .active_expire_enabled = true, // Active expiration enabled by default
            .notification_flags = std.atomic.Value(u16).init(0), // Notifications disabled by default
            .lru_clock = LRUClock.init(allocator),
            .lfu_counter = LFUCounter.init(allocator),
            .lfu_last_access = std.AutoHashMap(u64, i64).init(allocator), // Track last access times for LFU decay
            .evicted_keys = std.atomic.Value(u64).init(0),
            .lazyfree_task = lazy_free,
            .defrag_task = defrag_task,
            .module_store = try ModuleStore.init(allocator),
            .pubsub_state = null, // Will be set by server after init
            .hotkey_tracker = null, // Will be created on HOTKEYS START
            .key_versions = std.StringHashMap(u64).init(allocator),
            .server_start_time = std.time.timestamp(),
            .total_commands_processed = std.atomic.Value(u64).init(0),
            .total_connections_received = std.atomic.Value(u64).init(0),
            .lazy_expire = std.atomic.Value(bool).init(false),
            .lazy_eviction = std.atomic.Value(bool).init(false),
            .lazy_server_del = std.atomic.Value(bool).init(false),
            .keyspace_hits = std.atomic.Value(u64).init(0),
            .keyspace_misses = std.atomic.Value(u64).init(0),
            .expired_keys = std.atomic.Value(u64).init(0),
            .run_id = run_id,
            .command_stats_mutex = std.Thread.Mutex{},
            .command_stats = std.StringHashMapUnmanaged(CommandStatEntry){},
            .error_stats_mutex = std.Thread.Mutex{},
            .error_stats = std.StringHashMapUnmanaged(u64){},
            .dirty_count = std.atomic.Value(u64).init(0),
            .peak_memory = std.atomic.Value(usize).init(0),
        };

        // Start background lazy free thread
        try storage.lazyfree_task.start();

        // Start background defrag thread if activedefrag is enabled
        const activedefrag_val = storage.config.getAsString("activedefrag") catch null;
        const activedefrag_enabled = if (activedefrag_val) |val|
            std.mem.eql(u8, val, "yes") or std.mem.eql(u8, val, "1") or std.mem.eql(u8, val, "true")
        else
            false;
        if (activedefrag_enabled) {
            try storage.defrag_task.start();
        }

        return storage;
    }

    /// Update notification flags from config string (e.g., "KEg", "KEA", "")
    /// This is called when CONFIG SET notify-keyspace-events is invoked
    pub fn updateNotificationFlags(self: *Storage, config_str: []const u8) void {
        const flags = notifications_mod.parseNotificationFlags(config_str);
        self.notification_flags.store(flags, .release);
    }

    /// Get current notification flags
    pub fn getNotificationFlags(self: *Storage) u16 {
        return self.notification_flags.load(.acquire);
    }

    /// Get the total count of evicted keys (thread-safe atomic read)
    pub fn getEvictedKeysCount(self: *Storage) u64 {
        return self.evicted_keys.load(.monotonic);
    }

    /// Increment the evicted keys counter atomically
    fn incrementEvictedKeys(self: *Storage) void {
        var current = self.evicted_keys.load(.monotonic);
        while (!self.evicted_keys.compareAndSwapWeak(current, current + 1, .monotonic, .monotonic)) |new_current| {
            current = new_current;
        }
    }

    /// Update lazyfree flags from config values.
    /// Called by CONFIG SET handlers for the 3 lazyfree-lazy-* parameters.
    pub fn updateLazyfreeFlags(self: *Storage, param: []const u8, enabled: bool) void {
        if (std.mem.eql(u8, param, "lazyfree-lazy-expire")) {
            self.lazy_expire.store(enabled, .release);
        } else if (std.mem.eql(u8, param, "lazyfree-lazy-eviction")) {
            self.lazy_eviction.store(enabled, .release);
        } else if (std.mem.eql(u8, param, "lazyfree-lazy-server-del")) {
            self.lazy_server_del.store(enabled, .release);
        }
    }

    /// Submit a key+value pair to the lazyfree background task.
    /// The key and value must already be removed from the hashmap.
    /// On success, background thread takes ownership of both.
    /// On failure, falls back to synchronous freeing.
    fn submitLazyfreeOrFree(self: *Storage, owned_key: []const u8, value: Value) void {
        const value_ptr = self.allocator.create(Value) catch {
            self.allocator.free(owned_key);
            var v = value;
            v.deinit(self.allocator);
            return;
        };
        value_ptr.* = value;
        const work = LazyFreeWork{
            .work_type = .free_key,
            .key = owned_key,
            .db_num = null,
            .value_ptr = value_ptr,
            .allocator = self.allocator,
        };
        self.lazyfree_task.submitWork(work) catch {
            value_ptr.deinit(self.allocator);
            self.allocator.destroy(value_ptr);
            self.allocator.free(owned_key);
        };
    }

    /// Submit a value (no key) to the lazyfree background task.
    /// Used for lazy-server-del where the key stays in the hashmap with a new value.
    /// On failure, falls back to synchronous freeing.
    fn submitLazyfreeValueOrFree(self: *Storage, value: Value) void {
        const value_ptr = self.allocator.create(Value) catch {
            var v = value;
            v.deinit(self.allocator);
            return;
        };
        value_ptr.* = value;
        const work = LazyFreeWork{
            .work_type = .free_key,
            .key = null,
            .db_num = null,
            .value_ptr = value_ptr,
            .allocator = self.allocator,
        };
        self.lazyfree_task.submitWork(work) catch {
            value_ptr.deinit(self.allocator);
            self.allocator.destroy(value_ptr);
        };
    }

    /// Increment the total commands processed counter atomically
    pub fn incrementCommandsProcessed(self: *Storage) void {
        _ = self.total_commands_processed.fetchAdd(1, .monotonic);
    }

    /// Increment the total connections received counter atomically
    pub fn incrementConnectionsReceived(self: *Storage) void {
        _ = self.total_connections_received.fetchAdd(1, .monotonic);
    }

    /// Get uptime in seconds since server start
    pub fn getUptimeSeconds(self: *Storage) i64 {
        return std.time.timestamp() - self.server_start_time;
    }

    /// Increment keyspace_hits counter (successful key lookup)
    pub fn incrementKeyspaceHits(self: *Storage) void {
        _ = self.keyspace_hits.fetchAdd(1, .monotonic);
    }

    /// Increment keyspace_misses counter (failed key lookup)
    pub fn incrementKeyspaceMisses(self: *Storage) void {
        _ = self.keyspace_misses.fetchAdd(1, .monotonic);
    }

    /// Get current keyspace_hits count
    pub fn getKeyspaceHits(self: *Storage) u64 {
        return self.keyspace_hits.load(.monotonic);
    }

    /// Get current keyspace_misses count
    pub fn getKeyspaceMisses(self: *Storage) u64 {
        return self.keyspace_misses.load(.monotonic);
    }

    /// Reset keyspace hit/miss statistics (called by CONFIG RESETSTAT)
    pub fn resetKeyspaceStats(self: *Storage) void {
        self.keyspace_hits.store(0, .monotonic);
        self.keyspace_misses.store(0, .monotonic);
    }

    /// Increment expired_keys counter (called on lazy and active expiration)
    pub fn incrementExpiredKeys(self: *Storage) void {
        _ = self.expired_keys.fetchAdd(1, .monotonic);
    }

    /// Get total expired keys count since server start
    pub fn getExpiredKeysCount(self: *Storage) u64 {
        return self.expired_keys.load(.monotonic);
    }

    /// Record a command invocation for INFO commandstats.
    /// Increments calls and usec for the named command.
    pub fn recordCommandStat(self: *Storage, name: []const u8, usec: u64) void {
        self.command_stats_mutex.lock();
        defer self.command_stats_mutex.unlock();

        const gop = self.command_stats.getOrPut(self.allocator, name) catch return;
        if (!gop.found_existing) {
            const owned = self.allocator.dupe(u8, name) catch {
                _ = self.command_stats.remove(name);
                return;
            };
            gop.key_ptr.* = owned;
            gop.value_ptr.* = CommandStatEntry{};
        }
        gop.value_ptr.calls += 1;
        gop.value_ptr.usec += usec;
    }

    /// Record an error response type for INFO errorstats.
    /// error_type is the prefix word from the RESP error string (e.g. "ERR", "WRONGTYPE").
    pub fn recordErrorStat(self: *Storage, error_type: []const u8) void {
        self.error_stats_mutex.lock();
        defer self.error_stats_mutex.unlock();

        const gop = self.error_stats.getOrPut(self.allocator, error_type) catch return;
        if (!gop.found_existing) {
            const owned = self.allocator.dupe(u8, error_type) catch {
                _ = self.error_stats.remove(error_type);
                return;
            };
            gop.key_ptr.* = owned;
            gop.value_ptr.* = 0;
        }
        gop.value_ptr.* += 1;
    }

    /// Reset command and error statistics (called by CONFIG RESETSTAT).
    pub fn resetCommandStats(self: *Storage) void {
        {
            self.command_stats_mutex.lock();
            defer self.command_stats_mutex.unlock();
            var it = self.command_stats.iterator();
            while (it.next()) |entry| {
                entry.value_ptr.calls = 0;
                entry.value_ptr.usec = 0;
                entry.value_ptr.rejected_calls = 0;
                entry.value_ptr.failed_calls = 0;
            }
        }
        {
            self.error_stats_mutex.lock();
            defer self.error_stats_mutex.unlock();
            var it = self.error_stats.valueIterator();
            while (it.next()) |v| {
                v.* = 0;
            }
        }
    }

    /// Snapshot command stats for INFO commandstats (caller must free the slice).
    pub fn snapshotCommandStats(self: *Storage, allocator: std.mem.Allocator) ![]CommandStatSnapshot {
        self.command_stats_mutex.lock();
        defer self.command_stats_mutex.unlock();

        var result = try std.ArrayList(CommandStatSnapshot).initCapacity(allocator, self.command_stats.count());
        var it = self.command_stats.iterator();
        while (it.next()) |kv| {
            try result.append(allocator, CommandStatSnapshot{ .name = kv.key_ptr.*, .entry = kv.value_ptr.* });
        }
        return result.toOwnedSlice(allocator);
    }

    /// Snapshot error stats for INFO errorstats (caller must free the slice).
    pub fn snapshotErrorStats(self: *Storage, allocator: std.mem.Allocator) ![]ErrorStatSnapshot {
        self.error_stats_mutex.lock();
        defer self.error_stats_mutex.unlock();

        var result = try std.ArrayList(ErrorStatSnapshot).initCapacity(allocator, self.error_stats.count());
        var it = self.error_stats.iterator();
        while (it.next()) |kv| {
            try result.append(allocator, ErrorStatSnapshot{ .error_type = kv.key_ptr.*, .count = kv.value_ptr.* });
        }
        return result.toOwnedSlice(allocator);
    }

    /// Record the last access time for a key (in milliseconds) for LFU decay tracking
    /// Should be called when a key is accessed
    pub fn setKeyAccessTime(self: *Storage, key: []const u8, time_ms: i64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const key_hash = std.hash_map.hashString(key);
        try self.lfu_last_access.put(key_hash, time_ms);
    }

    /// Apply LFU decay to a key's frequency counter based on elapsed time.
    /// Decay formula: counter -= (current_time_ms - last_access_time_ms) / (lfu_decay_time * 60000)
    /// Counter is clamped to minimum of 0
    pub fn applyLfuDecay(self: *Storage, key: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Get last access time for this key
        const key_hash = std.hash_map.hashString(key);
        const last_access_ms = self.lfu_last_access.get(key_hash) orelse {
            // If not tracked, just skip decay
            return;
        };

        const now_ms = getCurrentTimestamp();
        const time_elapsed_ms = now_ms - last_access_ms;

        // If time_elapsed is negative or zero, no decay
        if (time_elapsed_ms <= 0) {
            return;
        }

        // Get lfu-decay-time from config (in minutes, default 1)
        const decay_time_str = (self.config.getAsString("lfu-decay-time") catch null);
        defer if (decay_time_str) |s| self.allocator.free(s);

        const decay_time_minutes: i64 = if (decay_time_str) |s|
            std.fmt.parseInt(i64, s, 10) catch 1
        else
            1; // Default to 1 minute

        // Convert decay time to milliseconds
        const decay_time_ms = decay_time_minutes * 60 * 1000;
        if (decay_time_ms <= 0) {
            return; // Decay time not properly configured
        }

        // Get current counter
        const current_counter = self.lfu_counter.getCounter(key);

        // Calculate decay: counter_decrease = elapsed_ms / decay_time_ms
        const decay_amount: u8 = @intCast(std.math.min(
            current_counter,
            @as(u32, @intCast(time_elapsed_ms / decay_time_ms)),
        ));

        // Apply decay: never go below 0
        const new_counter: u8 = current_counter - decay_amount;

        try self.lfu_counter.counters.put(key, new_counter);
    }

    /// Deinitialize storage and free all keys and values
    pub fn deinit(self: *Storage) void {
        self.mutex.lock();

        // Free TLS config
        self.tls_config.deinit();

        // Free ACL store
        if (self.acl) |acl| {
            acl.deinit();
            self.allocator.destroy(acl);
        }

        // Free cluster state
        self.cluster.deinit();

        // Free sentinel state
        self.sentinel.deinit();

        // Free function store
        self.functions.deinit();

        // Free search store
        self.search.deinit();

        // Free cuckoo load contexts
        var ctx_it = self.cuckoo_load_contexts.iterator();
        while (ctx_it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.cuckoo_load_contexts.deinit();

        // Free all keys and values
        var it = self.data.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var value = entry.value_ptr.*;
            value.deinit(self.allocator);
        }
        self.data.deinit();

        self.blocking_queue.deinit();
        self.slowlog.deinit();
        self.latency_monitor.deinit();
        self.lru_clock.deinit();
        self.lfu_counter.deinit();
        self.lfu_last_access.deinit();
        self.lazyfree_task.deinit();
        self.defrag_task.deinit();
        self.module_store.deinit();

        // Free hotkey tracker if active
        if (self.hotkey_tracker) |tracker| {
            tracker.deinit();
        }

        // Free key version counters
        var kv_it = self.key_versions.iterator();
        while (kv_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.key_versions.deinit();

        // Free command stats map (keys are owned)
        {
            var cs_it = self.command_stats.iterator();
            while (cs_it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
            }
            self.command_stats.deinit(self.allocator);
        }

        // Free error stats map (keys are owned)
        {
            var es_it = self.error_stats.iterator();
            while (es_it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
            }
            self.error_stats.deinit(self.allocator);
        }

        self.config.deinit();

        const allocator = self.allocator;
        self.mutex.unlock();
        allocator.destroy(self);
    }

    /// Check if memory limit is exceeded and evict keys if needed
    /// Returns error.OOM if noeviction policy and memory limit reached
    /// This should be called before write commands that grow memory
    pub fn checkMemoryLimitAndEvict(self: *Storage, command_name: []const u8) !void {
        // Get maxmemory config (0 = unlimited)
        const maxmemory_str = self.config.getAsString("maxmemory") catch return; // Continue if not set
        defer if (maxmemory_str) |s| self.allocator.free(s);

        if (maxmemory_str == null) return; // No limit configured

        const maxmemory_i64 = std.fmt.parseInt(i64, maxmemory_str.?, 10) catch return;
        if (maxmemory_i64 <= 0) return; // 0 or negative = unlimited
        const maxmemory: usize = @intCast(maxmemory_i64);

        // Get current memory usage
        const current_memory = self.memory_tracker.current_allocated;
        if (current_memory <= maxmemory) {
            return; // Memory OK
        }

        // Get eviction policy
        const policy_str_owned = self.config.getAsString("maxmemory-policy") catch {
            return error.OOM; // Default to noeviction if not set
        };
        defer if (policy_str_owned) |s| self.allocator.free(s);

        const policy_str = policy_str_owned orelse {
            return error.OOM; // No policy configured, default to noeviction
        };

        const policy = EvictionPolicy.parse(policy_str) orelse {
            return error.OOM; // Invalid policy string
        };

        // Check if this command grows memory
        if (!eviction_mod.isMemoryGrowingCommand(command_name)) {
            return; // Commands that don't grow memory (GET, DEL, etc.) are always allowed
        }

        // Handle noeviction policy
        if (policy == .noeviction) {
            return error.OOM; // Return error immediately
        }

        // Attempt to evict keys until memory is under limit
        const max_evictions: usize = 100; // Safety limit to prevent infinite loop
        var evictions: usize = 0;

        while (evictions < max_evictions) {
            // Recheck memory after each eviction
            const current = self.memory_tracker.current_allocated;
            if (current <= maxmemory) {
                return; // Success!
            }

            // Try to evict one key
            const evicted = try self.evictOneKey(policy);
            if (!evicted) {
                // No more keys can be evicted
                return error.OOM;
            }

            evictions += 1;
        }

        // If we exhausted max_evictions and memory still high, return error
        return error.OOM;
    }

    /// Helper to remove LFU tracking for a key by its string value
    /// Must be called before key is freed
    inline fn cleanupLfuTracking(self: *Storage, key: []const u8) void {
        const key_hash = std.hash_map.hashString(key);
        _ = self.lfu_last_access.remove(key_hash);
    }

    /// Helper to remove a key and clean up all associated tracking
    /// Calls cleanupLfuTracking internally
    inline fn removeKeyCleanup(self: *Storage, key: []const u8) bool {
        self.cleanupLfuTracking(key);
        return self.data.remove(key);
    }

    /// Helper to delete a key and fire evicted notification
    /// Fires evicted event before deletion if notifications are enabled and pubsub available
    /// When lazyfree-lazy-eviction is enabled, the value is freed asynchronously.
    /// Returns true if key was deleted, false otherwise
    fn deleteKeyWithEvictionNotification(self: *Storage, key: []const u8) bool {
        if (self.data.fetchRemove(key)) |kv| {
            // Fire evicted event before deletion (if notifications enabled and pubsub available)
            if (self.pubsub_state) |pubsub| {
                const flags = self.notification_flags.load(.monotonic);
                if (flags != 0 and notifications_mod.shouldNotify(flags, .evicted)) {
                    notifications_mod.publishNotification(
                        self.allocator,
                        pubsub,
                        0, // db_index - currently always 0
                        key,
                        "evicted",
                        flags,
                    ) catch {}; // Fire-and-forget in eviction path
                }
            }

            self.cleanupLfuTracking(kv.key);
            self.incrementEvictedKeys(); // Increment evicted counter
            if (self.lazy_eviction.load(.acquire)) {
                // lazyfree-lazy-eviction: defer value freeing to background thread
                self.submitLazyfreeOrFree(kv.key, kv.value);
            } else {
                self.allocator.free(kv.key);
                var value = kv.value;
                value.deinit(self.allocator);
            }
            return true;
        }
        return false;
    }

    /// Evict a single key according to the eviction policy
    /// Returns true if a key was evicted, false if no candidates available
    fn evictOneKey(self: *Storage, policy: EvictionPolicy) !bool {
        // Get configurable sample size from maxmemory-samples config (default 5, range 1-10)
        const samples_str = (self.config.getAsString("maxmemory-samples") catch null);
        defer if (samples_str) |s| self.allocator.free(s);

        const sample_size: usize = if (samples_str) |s| std.fmt.parseInt(usize, s, 10) catch 5 else 5;

        switch (policy) {
            .noeviction => return false, // Should not be called with noeviction
            .allkeys_lru => {
                // Sample N keys and find LRU
                var candidates = std.ArrayList([]const u8){};
                defer candidates.deinit();

                var it = self.data.keyIterator();
                var count: usize = 0;
                while (it.next()) |key_ptr| : (count += 1) {
                    if (count >= sample_size) break;
                    try candidates.append(key_ptr.*);
                }

                if (candidates.items.len == 0) {
                    return false; // No keys to evict
                }

                // Find key with max idle time (oldest LRU)
                var oldest_key: ?[]const u8 = null;
                var max_idle: u32 = 0;

                for (candidates.items) |key| {
                    const idle_time = self.lru_clock.getIdleTime(key) orelse 0;
                    if (oldest_key == null or idle_time > max_idle) {
                        oldest_key = key;
                        max_idle = idle_time;
                    }
                }

                if (oldest_key) |key| {
                    // Delete the key with eviction notification
                    if (self.deleteKeyWithEvictionNotification(key)) {
                        self.lru_clock.remove(key);
                        return true;
                    }
                }

                return false;
            },
            .volatile_lru => {
                // Sample N keys with TTL and find LRU
                var candidates = std.ArrayList([]const u8){};
                defer candidates.deinit();

                var it = self.data.iterator();
                var count: usize = 0;
                while (it.next()) |entry| {
                    // Only consider keys with expiration
                    const has_expiry = switch (entry.value_ptr.*) {
                        .string => |s| s.expires_at != null,
                        .list => |l| l.expires_at != null,
                        .set => |s| s.expires_at != null,
                        .sorted_set => |ss| ss.expires_at != null,
                        .hash => |h| h.expires_at != null,
                        .stream => |s| s.expires_at != null,
                        .hyperloglog => |hll| hll.expires_at != null,
                        .json => |j| j.expires_at != null,
                        .timeseries => |ts| ts.expires_at != null,
                        .bloom_filter => |bf| bf.expires_at != null,
                        .cuckoo_filter => |cf| cf.expires_at != null,
                        .count_min_sketch => |cms| cms.expires_at != null,
                        .topk => |tk| tk.expires_at != null,
                        .tdigest => |td| td.expires_at != null,
                        .vector_set => |vs| vs.expires_at != null,
                    };

                    if (has_expiry) {
                        try candidates.append(entry.key_ptr.*);
                        count += 1;
                        if (count >= sample_size) break;
                    }
                }

                if (candidates.items.len == 0) {
                    return false; // No keys with TTL
                }

                // Find key with max idle time
                var oldest_key: ?[]const u8 = null;
                var max_idle: u32 = 0;

                for (candidates.items) |key| {
                    const idle_time = self.lru_clock.getIdleTime(key) orelse 0;
                    if (oldest_key == null or idle_time > max_idle) {
                        oldest_key = key;
                        max_idle = idle_time;
                    }
                }

                if (oldest_key) |key| {
                    if (self.deleteKeyWithEvictionNotification(key)) {
                        self.lru_clock.remove(key);
                        self.lfu_counter.remove(key);
                        return true;
                    }
                }

                return false;
            },
            .allkeys_lfu => {
                // Sample N keys and find LFU (lowest frequency)
                var candidates = std.ArrayList([]const u8){};
                defer candidates.deinit();

                var it = self.data.keyIterator();
                var count: usize = 0;
                while (it.next()) |key_ptr| : (count += 1) {
                    if (count >= sample_size) break;
                    try candidates.append(key_ptr.*);
                }

                if (candidates.items.len == 0) {
                    return false;
                }

                // Find key with lowest frequency
                var lfu_key: ?[]const u8 = null;
                var min_freq: u8 = 255;

                for (candidates.items) |key| {
                    const freq = self.lfu_counter.getCounter(key);
                    if (lfu_key == null or freq < min_freq) {
                        lfu_key = key;
                        min_freq = freq;
                    }
                }

                if (lfu_key) |key| {
                    if (self.deleteKeyWithEvictionNotification(key)) {
                        self.lru_clock.remove(key);
                        self.lfu_counter.remove(key);
                        return true;
                    }
                }

                return false;
            },
            .volatile_lfu => {
                // Sample N keys with TTL and find LFU
                var candidates = std.ArrayList([]const u8){};
                defer candidates.deinit();

                var it = self.data.iterator();
                var count: usize = 0;
                while (it.next()) |entry| {
                    const has_expiry = switch (entry.value_ptr.*) {
                        .string => |s| s.expires_at != null,
                        .list => |l| l.expires_at != null,
                        .set => |s| s.expires_at != null,
                        .sorted_set => |ss| ss.expires_at != null,
                        .hash => |h| h.expires_at != null,
                        .stream => |s| s.expires_at != null,
                        .hyperloglog => |hll| hll.expires_at != null,
                        .json => |j| j.expires_at != null,
                        .timeseries => |ts| ts.expires_at != null,
                        .bloom_filter => |bf| bf.expires_at != null,
                        .cuckoo_filter => |cf| cf.expires_at != null,
                        .count_min_sketch => |cms| cms.expires_at != null,
                        .topk => |tk| tk.expires_at != null,
                        .tdigest => |td| td.expires_at != null,
                        .vector_set => |vs| vs.expires_at != null,
                    };

                    if (has_expiry) {
                        try candidates.append(entry.key_ptr.*);
                        count += 1;
                        if (count >= sample_size) break;
                    }
                }

                if (candidates.items.len == 0) {
                    return false;
                }

                // Find key with lowest frequency
                var lfu_key: ?[]const u8 = null;
                var min_freq: u8 = 255;

                for (candidates.items) |key| {
                    const freq = self.lfu_counter.getCounter(key);
                    if (lfu_key == null or freq < min_freq) {
                        lfu_key = key;
                        min_freq = freq;
                    }
                }

                if (lfu_key) |key| {
                    if (self.deleteKeyWithEvictionNotification(key)) {
                        self.lru_clock.remove(key);
                        self.lfu_counter.remove(key);
                        return true;
                    }
                }

                return false;
            },
            .allkeys_random => {
                // Sample N keys and pick one randomly
                var candidates = std.ArrayList([]const u8){};
                defer candidates.deinit();

                var it = self.data.keyIterator();
                var count: usize = 0;
                while (it.next()) |key_ptr| : (count += 1) {
                    if (count >= sample_size) break;
                    try candidates.append(key_ptr.*);
                }

                if (candidates.items.len == 0) {
                    return false;
                }

                // Pick a random key
                var prng = std.Random.DefaultPrng.init(@bitCast(std.time.nanoTimestamp()));
                const rand = prng.random();
                const idx = rand.intRangeAtMost(usize, 0, candidates.items.len - 1);
                const key = candidates.items[idx];

                if (self.deleteKeyWithEvictionNotification(key)) {
                    self.lru_clock.remove(key);
                    self.lfu_counter.remove(key);
                    return true;
                }

                return false;
            },
            .volatile_random => {
                // Sample N keys with TTL and pick one randomly
                var candidates = std.ArrayList([]const u8){};
                defer candidates.deinit();

                var it = self.data.iterator();
                var count: usize = 0;
                while (it.next()) |entry| {
                    const has_expiry = switch (entry.value_ptr.*) {
                        .string => |s| s.expires_at != null,
                        .list => |l| l.expires_at != null,
                        .set => |s| s.expires_at != null,
                        .sorted_set => |ss| ss.expires_at != null,
                        .hash => |h| h.expires_at != null,
                        .stream => |s| s.expires_at != null,
                        .hyperloglog => |hll| hll.expires_at != null,
                        .json => |j| j.expires_at != null,
                        .timeseries => |ts| ts.expires_at != null,
                        .bloom_filter => |bf| bf.expires_at != null,
                        .cuckoo_filter => |cf| cf.expires_at != null,
                        .count_min_sketch => |cms| cms.expires_at != null,
                        .topk => |tk| tk.expires_at != null,
                        .tdigest => |td| td.expires_at != null,
                        .vector_set => |vs| vs.expires_at != null,
                    };

                    if (has_expiry) {
                        try candidates.append(entry.key_ptr.*);
                        count += 1;
                        if (count >= sample_size) break;
                    }
                }

                if (candidates.items.len == 0) {
                    return false;
                }

                // Pick a random key
                var prng = std.Random.DefaultPrng.init(@bitCast(std.time.nanoTimestamp()));
                const rand = prng.random();
                const idx = rand.intRangeAtMost(usize, 0, candidates.items.len - 1);
                const key = candidates.items[idx];

                if (self.deleteKeyWithEvictionNotification(key)) {
                    self.lru_clock.remove(key);
                    self.lfu_counter.remove(key);
                    return true;
                }

                return false;
            },
            .volatile_ttl => {
                // Sample N keys with TTL and evict one with soonest expiration
                var candidates = std.ArrayList(struct { key: []const u8, ttl: i64 }){};
                defer candidates.deinit();

                const now_ms = std.time.milliTimestamp();
                var it = self.data.iterator();
                var count: usize = 0;
                while (it.next()) |entry| {
                    const expires_at: ?i64 = switch (entry.value_ptr.*) {
                        .string => |s| s.expires_at,
                        .list => |l| l.expires_at,
                        .set => |s| s.expires_at,
                        .sorted_set => |ss| ss.expires_at,
                        .hash => |h| h.expires_at,
                        .stream => |s| s.expires_at,
                        .hyperloglog => |hll| hll.expires_at,
                        .json => |j| j.expires_at,
                        .timeseries => |ts| ts.expires_at,
                        .bloom_filter => |bf| bf.expires_at,
                        .cuckoo_filter => |cf| cf.expires_at,
                        .count_min_sketch => |cms| cms.expires_at,
                        .topk => |tk| tk.expires_at,
                        .tdigest => |td| td.expires_at,
                        .vector_set => |vs| vs.expires_at,
                    };

                    if (expires_at) |exp| {
                        try candidates.append(.{ .key = entry.key_ptr.*, .ttl = exp - now_ms });
                        count += 1;
                        if (count >= sample_size) break;
                    }
                }

                if (candidates.items.len == 0) {
                    return false;
                }

                // Find key with soonest expiration (lowest TTL)
                var soonest_key: ?[]const u8 = null;
                var min_ttl: i64 = std.math.maxInt(i64);

                for (candidates.items) |candidate| {
                    if (soonest_key == null or candidate.ttl < min_ttl) {
                        soonest_key = candidate.key;
                        min_ttl = candidate.ttl;
                    }
                }

                if (soonest_key) |key| {
                    if (self.deleteKeyWithEvictionNotification(key)) {
                        self.lru_clock.remove(key);
                        self.lfu_counter.remove(key);
                        return true;
                    }
                }

                return false;
            },
        }
    }

    /// Get type of value stored at key, or null if key doesn't exist
    /// Return the internal encoding of a set key: intset or hashmap.
    /// Returns null if key doesn't exist or is not a set.
    pub fn getSetEncoding(self: *Storage, key: []const u8) ?Value.SetEncoding {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) return null;

        return switch (entry.value_ptr.*) {
            .set => |sv| sv.encoding,
            else => null,
        };
    }

    pub fn getType(self: *Storage, key: []const u8) ?ValueType {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        return std.meta.activeTag(entry.value_ptr.*);
    }

    /// Peek at a string value's encoding category without updating keyspace stats or LRU.
    /// Returns "int" / "embstr" / "raw", or null if the key does not exist / is not a string.
    /// Used by OBJECT ENCODING to avoid spurious keyspace_hits increments.
    pub fn peekStringEncoding(self: *Storage, key: []const u8) ?[]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        return switch (entry.value_ptr.*) {
            .string => |s| {
                if (std.fmt.parseInt(i64, s.data, 10)) |_| return "int" else |_| {}
                return if (s.data.len <= 44) "embstr" else "raw";
            },
            else => null,
        };
    }

    /// Get the LFU frequency counter for a key (for OBJECT FREQ)
    /// Returns 0 if key doesn't exist or has no LFU data
    pub fn getObjectFreq(self: *Storage, key: []const u8) u8 {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.data.get(key) == null) return 0;
        return self.lfu_counter.getCounter(key);
    }

    /// Get the idle time (seconds since last access) for a key (for OBJECT IDLETIME)
    /// Returns null if key doesn't exist; returns 0 if key exists but has no LRU tracking
    pub fn getObjectIdleTime(self: *Storage, key: []const u8) ?u32 {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.data.get(key) == null) return null;
        return self.lru_clock.getIdleTime(key) orelse 0;
    }

    /// Return the maximum field name or field value byte length for a hash key.
    /// Used by OBJECT ENCODING to determine listpack vs hashtable encoding.
    /// Returns null if key does not exist or is not a hash.
    pub fn getHashMaxElementLength(self: *Storage, key: []const u8) ?usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) return null;

        const hv = switch (entry.value_ptr.*) {
            .hash => |*h| h,
            else => return null,
        };

        var max_len: usize = 0;
        var it = hv.data.iterator();
        while (it.next()) |kv| {
            if (kv.key_ptr.*.len > max_len) max_len = kv.key_ptr.*.len;
            if (kv.value_ptr.*.data.len > max_len) max_len = kv.value_ptr.*.data.len;
        }
        return max_len;
    }

    /// Return the maximum member byte length for a set key (hashmap encoding only).
    /// Used by OBJECT ENCODING to determine listpack vs hashtable encoding.
    /// Returns null if key does not exist, is not a set, or uses intset encoding.
    pub fn getSetMaxMemberLength(self: *Storage, key: []const u8) ?usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) return null;

        const sv = switch (entry.value_ptr.*) {
            .set => |*s| s,
            else => return null,
        };

        if (sv.encoding != .hashmap) return null;

        var max_len: usize = 0;
        var it = sv.data.hashmap.keyIterator();
        while (it.next()) |member| {
            if (member.*.len > max_len) max_len = member.*.len;
        }
        return max_len;
    }

    /// Return the maximum member byte length for a sorted set key.
    /// Used by OBJECT ENCODING to determine listpack vs skiplist encoding.
    /// Returns null if key does not exist or is not a sorted set.
    pub fn getZsetMaxMemberLength(self: *Storage, key: []const u8) ?usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) return null;

        const zv = switch (entry.value_ptr.*) {
            .sorted_set => |*z| z,
            else => return null,
        };

        var max_len: usize = 0;
        var it = zv.members.keyIterator();
        while (it.next()) |member| {
            if (member.*.len > max_len) max_len = member.*.len;
        }
        return max_len;
    }

    /// Return the maximum element byte length for a list key.
    /// Used by OBJECT ENCODING to determine listpack vs quicklist encoding.
    /// Returns null if key does not exist or is not a list.
    pub fn getListMaxElementLength(self: *Storage, key: []const u8) ?usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) return null;

        const lv = switch (entry.value_ptr.*) {
            .list => |*l| l,
            else => return null,
        };

        var max_len: usize = 0;
        for (lv.data.items) |elem| {
            if (elem.len > max_len) max_len = elem.len;
        }
        return max_len;
    }

    /// Return the modification version counter for a key (OBJECT VERSION).
    /// Starts at 1 on first write, increments on each subsequent write.
    /// Returns null if key does not exist or version is not tracked.
    pub fn getKeyVersion(self: *Storage, key: []const u8) ?u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.data.get(key) == null) return null;
        return self.key_versions.get(key);
    }

    /// Increment the modification counter for a key.
    /// Creates the counter starting at 1 if this is the first tracked write.
    /// Caller must NOT hold self.mutex (this method acquires it internally through the call path).
    /// This method is called while mutex is already held by the public write methods.
    fn bumpKeyVersionLocked(self: *Storage, key: []const u8) void {
        const gop = self.key_versions.getOrPut(key) catch return;
        if (gop.found_existing) {
            gop.value_ptr.* +%= 1; // wrapping add to avoid overflow
        } else {
            // New entry: duplicate key string for ownership
            const owned_key = self.allocator.dupe(u8, key) catch {
                // On OOM, remove the half-inserted entry
                _ = self.key_versions.remove(key);
                return;
            };
            gop.key_ptr.* = owned_key;
            gop.value_ptr.* = 1;
        }
    }

    /// Remove the version counter for a key (called on deletion).
    /// Caller must hold self.mutex.
    fn removeKeyVersionLocked(self: *Storage, key: []const u8) void {
        if (self.key_versions.fetchRemove(key)) |kv| {
            self.allocator.free(kv.key);
        }
    }

    /// Set key to string value with optional expiration
    /// Overwrites existing value if key exists
    /// expires_at: Unix timestamp in milliseconds, null = no expiration
    pub fn set(self: *Storage, key: []const u8, value: []const u8, expires_at: ?i64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Copy value
        const owned_value = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_value);

        const new_value = Value{
            .string = .{
                .data = owned_value,
                .expires_at = expires_at,
            },
        };

        // Check if key already exists
        if (self.data.getEntry(key)) |entry| {
            // Key exists - replace old value (lazily if lazy-server-del enabled)
            const old_value = entry.value_ptr.*;
            entry.value_ptr.* = new_value;
            if (self.lazy_server_del.load(.acquire)) {
                // lazyfree-lazy-server-del: defer old value freeing to background thread
                self.submitLazyfreeValueOrFree(old_value);
            } else {
                var v = old_value;
                v.deinit(self.allocator);
            }
        } else {
            // New key - copy key and insert
            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);

            try self.data.put(owned_key, new_value);
        }
        self.bumpKeyVersionLocked(key);
        self.incrementDirty(1);
    }

    /// Atomically set multiple keys with optional shared expiration
    /// If nx_flag is true, only set if NONE of the keys exist
    /// If xx_flag is true, only set if ALL of the keys exist
    /// If keepttl_flag is true, preserve existing TTLs
    /// Returns true if all keys were set, false otherwise
    pub fn msetex(
        self: *Storage,
        keys: []const []const u8,
        values: []const []const u8,
        expires_at: ?i64,
        nx_flag: bool,
        xx_flag: bool,
        keepttl_flag: bool,
    ) !bool {
        if (keys.len != values.len) return error.InvalidArgument;
        if (keys.len == 0) return false;

        self.mutex.lock();
        defer self.mutex.unlock();

        const now = getCurrentTimestamp();

        // Phase 1: Check conditions (NX or XX)
        if (nx_flag) {
            // NX: fail if ANY key exists
            for (keys) |key| {
                const entry = self.data.getEntry(key);
                if (entry) |e| {
                    // Key exists and not expired
                    if (!e.value_ptr.isExpired(now)) {
                        return false;
                    }
                }
            }
        } else if (xx_flag) {
            // XX: fail if ANY key doesn't exist
            for (keys) |key| {
                const entry = self.data.getEntry(key);
                if (entry) |e| {
                    if (e.value_ptr.isExpired(now)) {
                        return false; // Expired = doesn't exist
                    }
                } else {
                    return false; // Doesn't exist
                }
            }
        }

        // Phase 2: Set all keys atomically
        for (keys, values) |key, value| {
            const owned_value = try self.allocator.dupe(u8, value);
            errdefer self.allocator.free(owned_value);

            // Determine expiration time
            var final_expires_at: ?i64 = expires_at;
            if (keepttl_flag) {
                // Keep existing TTL if key exists
                if (self.data.getEntry(key)) |entry| {
                    if (!entry.value_ptr.isExpired(now)) {
                        final_expires_at = switch (entry.value_ptr.*) {
                            .string => |s| s.expires_at,
                            .list => |l| l.expires_at,
                            .set => |s| s.expires_at,
                            .hash => |h| h.expires_at,
                            .sorted_set => |z| z.expires_at,
                            .stream => |st| st.expires_at,
                            .hyperloglog => |hll| hll.expires_at,
                            .json => |j| j.expires_at,
                            .timeseries => |ts| ts.expires_at,
                            .bloom => |b| b.expires_at,
                            .cuckoo => |c| c.expires_at,
                            .count_min_sketch => null, // CMS doesn't support expiration
                            .top_k => |tk| tk.expires_at,
                            .t_digest => null, // T-Digest doesn't support expiration yet
                            .vector_set => null, // Vector sets don't support expiration yet
                        };
                    }
                }
            }

            const new_value = Value{
                .string = .{
                    .data = owned_value,
                    .expires_at = final_expires_at,
                },
            };

            // Check if key already exists
            if (self.data.getEntry(key)) |entry| {
                // Key exists - free old value and update
                var old_value = entry.value_ptr.*;
                old_value.deinit(self.allocator);
                entry.value_ptr.* = new_value;
            } else {
                // New key - copy key and insert
                const owned_key = try self.allocator.dupe(u8, key);
                errdefer self.allocator.free(owned_key);

                try self.data.put(owned_key, new_value);
            }
            self.bumpKeyVersionLocked(key);
        }

        return true;
    }

    /// Get string value for key
    /// Returns null if key doesn't exist, is expired, or is not a string
    /// Expired keys are lazily deleted.
    /// Tracks keyspace_hits/misses: hit when string found, miss when key absent or expired.
    pub fn get(self: *Storage, key: []const u8) ?[]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse {
            _ = self.keyspace_misses.fetchAdd(1, .monotonic);
            return null;
        };

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            // Fire expired event before deletion (if notifications enabled and pubsub available)
            if (self.pubsub_state) |pubsub| {
                const flags = self.notification_flags.load(.monotonic);
                if (flags != 0 and notifications_mod.shouldNotify(flags, .expired)) {
                    notifications_mod.publishNotification(
                        self.allocator,
                        pubsub,
                        0, // db_index - currently always 0
                        key,
                        "expired",
                        flags,
                    ) catch {}; // Fire-and-forget in hot path
                }
            }

            // Expired - remove from hashmap then free key+value (sync or async)
            const owned_key = entry.key_ptr.*;
            const value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            if (self.lazy_expire.load(.acquire)) {
                // lazyfree-lazy-expire: defer freeing to background thread
                self.submitLazyfreeOrFree(owned_key, value);
            } else {
                self.allocator.free(owned_key);
                var v = value;
                v.deinit(self.allocator);
            }
            _ = self.keyspace_misses.fetchAdd(1, .monotonic);
            _ = self.expired_keys.fetchAdd(1, .monotonic);
            return null;
        }

        // Check type — only strings count as hits for the GET command path
        return switch (entry.value_ptr.*) {
            .string => |s| blk: {
                _ = self.keyspace_hits.fetchAdd(1, .monotonic);
                break :blk s.data;
            },
            else => null, // WRONGTYPE — do not track as hit or miss (will return WRONGTYPE error)
        };
    }

    /// Get string value and expiration for SET GET/KEEPTTL options.
    /// Returns error.WrongType if the key exists and is not a string.
    /// Returns null if the key does not exist or has expired.
    pub fn getStringWithExpiry(self: *Storage, key: []const u8) error{WrongType}!?struct { value: []const u8, expires_at: ?i64 } {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            return null;
        }

        return switch (entry.value_ptr.*) {
            .string => |s| .{ .value = s.data, .expires_at = s.expires_at },
            else => error.WrongType,
        };
    }

    /// Delete one or more keys
    /// Returns count of keys actually deleted (non-existent keys are ignored)
    pub fn del(self: *Storage, keys: []const []const u8) usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        var count: usize = 0;
        for (keys) |key| {
            if (self.data.fetchRemove(key)) |kv| {
                self.cleanupLfuTracking(kv.key);
                self.removeKeyVersionLocked(kv.key);
                self.allocator.free(kv.key);
                var value = kv.value;
                value.deinit(self.allocator);
                count += 1;
            }
        }
        if (count > 0) self.incrementDirty(@intCast(count));
        return count;
    }

    /// Check if key exists (respects expiration)
    /// Expired keys count as non-existent and are lazily deleted
    pub fn exists(self: *Storage, key: []const u8) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse {
            _ = self.keyspace_misses.fetchAdd(1, .monotonic);
            return false;
        };

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            // Fire expired event before deletion (if notifications enabled and pubsub available)
            if (self.pubsub_state) |pubsub| {
                const flags = self.notification_flags.load(.monotonic);
                if (flags != 0 and notifications_mod.shouldNotify(flags, .expired)) {
                    notifications_mod.publishNotification(
                        self.allocator,
                        pubsub,
                        0, // db_index - currently always 0
                        key,
                        "expired",
                        flags,
                    ) catch {}; // Fire-and-forget in hot path
                }
            }

            // Expired - delete and return false
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            _ = self.keyspace_misses.fetchAdd(1, .monotonic);
            return false;
        }

        _ = self.keyspace_hits.fetchAdd(1, .monotonic);
        return true;
    }

    /// Remove all expired keys from storage
    /// Returns count of keys removed
    pub fn evictExpired(self: *Storage) usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = getCurrentTimestamp();
        var count: usize = 0;

        // Collect expired keys first (can't modify during iteration)
        var expired_keys: std.ArrayList([]const u8) = .{
            .items = &.{},
            .capacity = 0,
        };
        defer expired_keys.deinit(self.allocator);

        var it = self.data.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.isExpired(now)) {
                expired_keys.append(self.allocator, entry.key_ptr.*) catch continue;
            }
        }

        // Remove expired keys and fire notifications
        for (expired_keys.items) |key| {
            if (self.data.fetchRemove(key)) |kv| {
                // Fire expired event before deletion (if notifications enabled and pubsub available)
                if (self.pubsub_state) |pubsub| {
                    const flags = self.notification_flags.load(.monotonic);
                    if (flags != 0 and notifications_mod.shouldNotify(flags, .expired)) {
                        notifications_mod.publishNotification(
                            self.allocator,
                            pubsub,
                            0, // db_index - currently always 0
                            key,
                            "expired",
                            flags,
                        ) catch continue; // Skip notification on error, continue deletion
                    }
                }

                self.allocator.free(kv.key);
                var value = kv.value;
                value.deinit(self.allocator);
                _ = self.expired_keys.fetchAdd(1, .monotonic);
                count += 1;
            }
        }

        return count;
    }

    // List operations

    /// Resolve a potentially-negative list index to a concrete usize.
    /// Returns null if the index is out of bounds.
    inline fn resolveListIndex(list_len: usize, index: i64) ?usize {
        if (index >= 0) {
            const u: usize = @intCast(index);
            return if (u >= list_len) null else u;
        } else {
            const adjusted = @as(i64, @intCast(list_len)) + index;
            if (adjusted < 0) return null;
            return @intCast(adjusted);
        }
    }

    /// Push elements to the head of a list
    /// Creates list if it doesn't exist
    /// Returns length of list after push, or error if key exists and is not a list
    pub fn lpush(self: *Storage, key: []const u8, elements: []const []const u8, expires_at: ?i64) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.data.getEntry(key)) |entry| {
            // Key exists - verify it's a list
            switch (entry.value_ptr.*) {
                .list => |*list_val| {
                    // Insert elements at head one by one, left to right (Redis semantics)
                    for (elements) |elem| {
                        const owned_elem = try self.allocator.dupe(u8, elem);
                        errdefer self.allocator.free(owned_elem);
                        try list_val.data.insert(self.allocator, 0, owned_elem);
                    }
                    self.bumpKeyVersionLocked(key);
                    self.incrementDirty(1);
                    return list_val.data.items.len;
                },
                else => return error.WrongType,
            }
        } else {
            // Create new list
            var list: std.ArrayList([]const u8) = .{
                .items = &.{},
                .capacity = 0,
            };
            errdefer {
                for (list.items) |elem| {
                    self.allocator.free(elem);
                }
                list.deinit(self.allocator);
            }

            // Add elements in reverse order (LPUSH a b c -> [c, b, a])
            var i: usize = elements.len;
            while (i > 0) {
                i -= 1;
                const owned_elem = try self.allocator.dupe(u8, elements[i]);
                errdefer self.allocator.free(owned_elem);
                try list.append(self.allocator, owned_elem);
            }

            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);

            try self.data.put(owned_key, Value{
                .list = .{
                    .data = list,
                    .expires_at = expires_at,
                },
            });

            self.bumpKeyVersionLocked(key);
            self.incrementDirty(1);
            return list.items.len;
        }
    }

    /// Push elements to the tail of a list
    /// Creates list if it doesn't exist
    /// Returns length of list after push, or error if key exists and is not a list
    pub fn rpush(self: *Storage, key: []const u8, elements: []const []const u8, expires_at: ?i64) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.data.getEntry(key)) |entry| {
            // Key exists - verify it's a list
            switch (entry.value_ptr.*) {
                .list => |*list_val| {
                    // Append elements to tail
                    for (elements) |elem| {
                        const owned_elem = try self.allocator.dupe(u8, elem);
                        errdefer self.allocator.free(owned_elem);
                        try list_val.data.append(self.allocator, owned_elem);
                    }
                    self.bumpKeyVersionLocked(key);
                    self.incrementDirty(1);
                    return list_val.data.items.len;
                },
                else => return error.WrongType,
            }
        } else {
            // Create new list
            var list: std.ArrayList([]const u8) = .{
                .items = &.{},
                .capacity = 0,
            };
            errdefer {
                for (list.items) |elem| {
                    self.allocator.free(elem);
                }
                list.deinit(self.allocator);
            }

            // Add elements in order
            for (elements) |elem| {
                const owned_elem = try self.allocator.dupe(u8, elem);
                errdefer self.allocator.free(owned_elem);
                try list.append(self.allocator, owned_elem);
            }

            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);

            try self.data.put(owned_key, Value{
                .list = .{
                    .data = list,
                    .expires_at = expires_at,
                },
            });

            self.bumpKeyVersionLocked(key);
            self.incrementDirty(1);
            return list.items.len;
        }
    }

    /// Pop count elements from head of list
    /// Returns owned slice of elements (caller must free), or null if key doesn't exist or is not a list
    /// Automatically deletes key if list becomes empty
    pub fn lpop(self: *Storage, allocator: std.mem.Allocator, key: []const u8, count: usize) !?[][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        switch (entry.value_ptr.*) {
            .list => |*list_val| {
                const available = list_val.data.items.len;
                if (available == 0) return null;

                const to_pop = @min(count, available);
                var result = try allocator.alloc([]const u8, to_pop);
                errdefer allocator.free(result);

                // Pop from head
                for (0..to_pop) |i| {
                    result[i] = list_val.data.orderedRemove(0);
                }

                // Auto-delete if empty
                if (list_val.data.items.len == 0) {
                    const owned_key = entry.key_ptr.*;
                    var value = entry.value_ptr.*;
                    _ = self.removeKeyCleanup(key);
                    self.allocator.free(owned_key);
                    value.deinit(self.allocator);
                }

                return result;
            },
            else => return null,
        }
    }

    /// Pop count elements from tail of list
    /// Returns owned slice of elements (caller must free), or null if key doesn't exist or is not a list
    /// Automatically deletes key if list becomes empty
    pub fn rpop(self: *Storage, allocator: std.mem.Allocator, key: []const u8, count: usize) !?[][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        switch (entry.value_ptr.*) {
            .list => |*list_val| {
                const available = list_val.data.items.len;
                if (available == 0) return null;

                const to_pop = @min(count, available);
                var result = try allocator.alloc([]const u8, to_pop);
                errdefer allocator.free(result);

                // Pop from tail (in reverse order)
                for (0..to_pop) |i| {
                    result[i] = list_val.data.pop().?;
                }

                // Auto-delete if empty
                if (list_val.data.items.len == 0) {
                    const owned_key = entry.key_ptr.*;
                    var value = entry.value_ptr.*;
                    _ = self.removeKeyCleanup(key);
                    self.allocator.free(owned_key);
                    value.deinit(self.allocator);
                }

                return result;
            },
            else => return null,
        }
    }

    /// Get range of elements from list
    /// start and stop are inclusive, support negative indices (-1 = last element)
    /// Returns owned slice of elements (caller must free), or null if key doesn't exist or is not a list
    pub fn lrange(self: *Storage, allocator: std.mem.Allocator, key: []const u8, start: i64, stop: i64) !?[][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        switch (entry.value_ptr.*) {
            .list => |*list_val| {
                const len = list_val.data.items.len;
                if (len == 0) {
                    return try allocator.alloc([]const u8, 0);
                }

                // Normalize indices
                const norm_start: i64 = if (start < 0)
                    @max(0, @as(i64, @intCast(len)) + start)
                else
                    @min(start, @as(i64, @intCast(len)));

                const norm_stop: i64 = if (stop < 0)
                    @max(-1, @as(i64, @intCast(len)) + stop)
                else
                    @min(stop, @as(i64, @intCast(len)) - 1);

                // Handle invalid range
                if (norm_start > norm_stop or norm_start >= @as(i64, @intCast(len))) {
                    return try allocator.alloc([]const u8, 0);
                }

                const u_start: usize = @intCast(norm_start);
                const u_stop: usize = @intCast(norm_stop);
                const range_len = u_stop - u_start + 1;

                var result = try allocator.alloc([]const u8, range_len);
                errdefer allocator.free(result);

                for (0..range_len) |i| {
                    result[i] = list_val.data.items[u_start + i];
                }

                return result;
            },
            else => return null,
        }
    }

    /// Get length of list
    /// Returns null if key doesn't exist or is not a list
    pub fn llen(self: *Storage, key: []const u8) ?usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        switch (entry.value_ptr.*) {
            .list => |list_val| return list_val.data.items.len,
            else => return null,
        }
    }

    /// Get element at index in list (negative = from tail)
    /// Returns null if key doesn't exist or index is out of range
    /// Returns error.WrongType if key exists but is not a list
    pub fn lindex(self: *Storage, key: []const u8, index: i64) error{WrongType}!?[]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        switch (entry.value_ptr.*) {
            .list => |*list_val| {
                const len = list_val.data.items.len;
                if (len == 0) return null;
                const resolved = resolveListIndex(len, index) orelse return null;
                return list_val.data.items[resolved];
            },
            else => return error.WrongType,
        }
    }

    /// Set element at index in list (negative = from tail)
    /// Returns error.WrongType if not a list, error.NoSuchKey if missing,
    /// error.IndexOutOfRange if index is out of bounds
    pub fn lset(self: *Storage, key: []const u8, index: i64, element: []const u8) error{ WrongType, NoSuchKey, IndexOutOfRange, OutOfMemory }!void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return error.NoSuchKey;

        switch (entry.value_ptr.*) {
            .list => |*list_val| {
                const len = list_val.data.items.len;
                const resolved = resolveListIndex(len, index) orelse return error.IndexOutOfRange;
                const owned = try self.allocator.dupe(u8, element);
                self.allocator.free(list_val.data.items[resolved]);
                list_val.data.items[resolved] = owned;
            },
            else => return error.WrongType,
        }
    }

    /// Trim list to range [start, stop] in-place, removing elements outside
    /// Negative indices supported. Out-of-range indices are clamped.
    /// If result range is empty, deletes the key.
    /// Returns error.WrongType if key exists but is not a list; OK for missing key.
    pub fn ltrim(self: *Storage, key: []const u8, start: i64, stop: i64) error{WrongType}!void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return; // missing key is OK

        switch (entry.value_ptr.*) {
            .list => |*list_val| {
                const len = list_val.data.items.len;

                // Normalize start/stop the same way lrange does
                const norm_start: usize = blk: {
                    if (start < 0) {
                        const s = @as(i64, @intCast(len)) + start;
                        break :blk if (s < 0) 0 else @as(usize, @intCast(s));
                    } else {
                        break :blk @min(@as(usize, @intCast(start)), len);
                    }
                };

                const norm_stop: usize = blk: {
                    if (stop < 0) {
                        const s = @as(i64, @intCast(len)) + stop;
                        if (s < 0) {
                            // Entire list should be deleted
                            break :blk 0;
                        }
                        break :blk @as(usize, @intCast(s));
                    } else {
                        break :blk @min(@as(usize, @intCast(stop)), len -| 1);
                    }
                };

                // If range is invalid (start > stop or start >= len), delete whole list
                if (norm_start > norm_stop or norm_start >= len) {
                    const owned_key = entry.key_ptr.*;
                    var value = entry.value_ptr.*;
                    _ = self.removeKeyCleanup(key);
                    self.allocator.free(owned_key);
                    value.deinit(self.allocator);
                    return;
                }

                // Free elements before norm_start
                for (0..norm_start) |i| {
                    self.allocator.free(list_val.data.items[i]);
                }

                // Free elements after norm_stop
                for (norm_stop + 1..len) |i| {
                    self.allocator.free(list_val.data.items[i]);
                }

                // Shift kept elements to front
                const keep_len = norm_stop - norm_start + 1;
                for (0..keep_len) |i| {
                    list_val.data.items[i] = list_val.data.items[norm_start + i];
                }
                list_val.data.items.len = keep_len;

                // If list is now empty, delete the key
                if (list_val.data.items.len == 0) {
                    const owned_key = entry.key_ptr.*;
                    var value = entry.value_ptr.*;
                    _ = self.removeKeyCleanup(key);
                    self.allocator.free(owned_key);
                    value.deinit(self.allocator);
                }
            },
            else => return error.WrongType,
        }
    }

    /// Remove `count` occurrences of `element` from list.
    /// count > 0: remove from head; count < 0: remove from tail; count == 0: remove all
    /// Returns number of elements removed.
    /// Returns error.WrongType if key exists but is not a list.
    pub fn lrem(self: *Storage, key: []const u8, count: i64, element: []const u8) error{WrongType}!usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return 0;

        switch (entry.value_ptr.*) {
            .list => |*list_val| {
                const items = list_val.data.items;
                const limit: usize = if (count == 0) items.len else @as(usize, @intCast(if (count < 0) -count else count));
                var removed: usize = 0;

                if (count >= 0) {
                    // Remove from head
                    var i: usize = 0;
                    while (i < list_val.data.items.len and (count == 0 or removed < limit)) {
                        if (std.mem.eql(u8, list_val.data.items[i], element)) {
                            const elem = list_val.data.orderedRemove(i);
                            self.allocator.free(elem);
                            removed += 1;
                        } else {
                            i += 1;
                        }
                    }
                } else {
                    // Remove from tail
                    var i: usize = list_val.data.items.len;
                    while (i > 0 and removed < limit) {
                        i -= 1;
                        if (std.mem.eql(u8, list_val.data.items[i], element)) {
                            const elem = list_val.data.orderedRemove(i);
                            self.allocator.free(elem);
                            removed += 1;
                        }
                    }
                }

                // Auto-delete if empty
                if (list_val.data.items.len == 0) {
                    const owned_key = entry.key_ptr.*;
                    var value = entry.value_ptr.*;
                    _ = self.removeKeyCleanup(key);
                    self.allocator.free(owned_key);
                    value.deinit(self.allocator);
                }

                return removed;
            },
            else => return error.WrongType,
        }
    }

    /// Push elements to head only if key already exists as a list.
    /// Returns null if key doesn't exist, new length otherwise.
    /// Returns error.WrongType if key exists but is not a list.
    pub fn lpushx(self: *Storage, key: []const u8, elements: []const []const u8) error{ WrongType, OutOfMemory }!?usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        switch (entry.value_ptr.*) {
            .list => |*list_val| {
                // Pre-duplicate all elements before inserting to make the operation atomic:
                // if any dupe or insert fails, previously-duped elements are freed and the
                // list remains unmodified.
                var owned_buf: std.ArrayList([]const u8) = .{};
                defer owned_buf.deinit(self.allocator);
                // On error, free any successfully duped elements not yet inserted
                errdefer for (owned_buf.items) |o| self.allocator.free(o);

                for (elements) |elem| {
                    const owned_elem = try self.allocator.dupe(u8, elem);
                    try owned_buf.append(self.allocator, owned_elem);
                }
                // All dupes succeeded; now insert (list takes ownership)
                for (owned_buf.items) |o| {
                    try list_val.data.insert(self.allocator, 0, o);
                }
                // Clear owned_buf so errdefer does not double-free now-owned elements
                owned_buf.items.len = 0;
                return list_val.data.items.len;
            },
            else => return error.WrongType,
        }
    }

    /// Push elements to tail only if key already exists as a list.
    /// Returns null if key doesn't exist, new length otherwise.
    /// Returns error.WrongType if key exists but is not a list.
    pub fn rpushx(self: *Storage, key: []const u8, elements: []const []const u8) error{ WrongType, OutOfMemory }!?usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        switch (entry.value_ptr.*) {
            .list => |*list_val| {
                // Pre-duplicate all elements before appending for atomicity
                var owned_buf: std.ArrayList([]const u8) = .{};
                defer owned_buf.deinit(self.allocator);
                errdefer for (owned_buf.items) |o| self.allocator.free(o);

                for (elements) |elem| {
                    const owned_elem = try self.allocator.dupe(u8, elem);
                    try owned_buf.append(self.allocator, owned_elem);
                }
                for (owned_buf.items) |o| {
                    try list_val.data.append(self.allocator, o);
                }
                owned_buf.items.len = 0;
                return list_val.data.items.len;
            },
            else => return error.WrongType,
        }
    }

    /// Insert `element` BEFORE or AFTER the first occurrence of `pivot` in list.
    /// Returns new list length, -1 if pivot not found, 0 if key doesn't exist.
    /// Returns error.WrongType if key exists but is not a list.
    pub fn linsert(
        self: *Storage,
        key: []const u8,
        before: bool,
        pivot: []const u8,
        element: []const u8,
    ) error{ WrongType, OutOfMemory }!i64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return 0;

        switch (entry.value_ptr.*) {
            .list => |*list_val| {
                // Find pivot
                var pivot_idx: ?usize = null;
                for (list_val.data.items, 0..) |item, i| {
                    if (std.mem.eql(u8, item, pivot)) {
                        pivot_idx = i;
                        break;
                    }
                }

                const idx = pivot_idx orelse return -1;
                const insert_at = if (before) idx else idx + 1;

                const owned_elem = try self.allocator.dupe(u8, element);
                errdefer self.allocator.free(owned_elem);
                try list_val.data.insert(self.allocator, insert_at, owned_elem);

                return @intCast(list_val.data.items.len);
            },
            else => return error.WrongType,
        }
    }

    /// Find positions of `element` in list.
    /// rank: 1-based (negative = search from tail); count: 0 = all; maxlen: 0 = no limit.
    /// Returns owned slice of positions (caller must free). Slice is empty if no matches.
    /// Returns error.WrongType if key exists but is not a list.
    pub fn lpos(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        element: []const u8,
        rank: i64,
        count: usize,
        maxlen: usize,
    ) error{ WrongType, OutOfMemory }![]usize {
        // rank is 1-based; 0 is invalid and must be rejected before calling this function
        std.debug.assert(rank != 0);

        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse {
            return try allocator.alloc(usize, 0);
        };

        switch (entry.value_ptr.*) {
            .list => |*list_val| {
                const items = list_val.data.items;
                const len = items.len;
                var results = std.ArrayList(usize){};
                errdefer results.deinit(allocator);

                const abs_rank: usize = if (rank < 0) @as(usize, @intCast(-rank)) else @as(usize, @intCast(rank));
                const search_limit = if (maxlen == 0) len else @min(maxlen, len);

                if (rank >= 0) {
                    // Forward search (rank >= 1 means 1-based skip from head)
                    var matched: usize = 0;
                    for (0..search_limit) |i| {
                        if (std.mem.eql(u8, items[i], element)) {
                            matched += 1;
                            if (matched >= abs_rank) {
                                try results.append(allocator, i);
                                if (count != 0 and results.items.len >= count) break;
                            }
                        }
                    }
                } else {
                    // Backward search
                    var matched: usize = 0;
                    var i: usize = len;
                    const search_from = len -| search_limit;
                    while (i > search_from) {
                        i -= 1;
                        if (std.mem.eql(u8, items[i], element)) {
                            matched += 1;
                            if (matched >= abs_rank) {
                                try results.append(allocator, i);
                                if (count != 0 and results.items.len >= count) break;
                            }
                        }
                    }
                    // Reverse to return positions in ascending order
                    std.mem.reverse(usize, results.items);
                }

                return try results.toOwnedSlice(allocator);
            },
            else => return error.WrongType,
        }
    }

    /// Atomically pop from source and push to destination.
    /// src_from_left=true means pop from head; dst_to_left=true means push to head.
    /// Returns owned copy of the moved element, or null if source is empty/missing.
    /// Returns error.WrongType if source or destination exists but is not a list.
    pub fn lmove(
        self: *Storage,
        allocator: std.mem.Allocator,
        src: []const u8,
        dst: []const u8,
        src_from_left: bool,
        dst_to_left: bool,
    ) error{ WrongType, OutOfMemory }!?[]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Get source list
        const src_entry = self.data.getEntry(src) orelse return null;

        const popped: []const u8 = popped_blk: {
            switch (src_entry.value_ptr.*) {
                .list => |*src_list| {
                    if (src_list.data.items.len == 0) return null;
                    if (src_from_left) {
                        break :popped_blk src_list.data.orderedRemove(0);
                    } else {
                        break :popped_blk src_list.data.pop().?;
                    }
                },
                else => return error.WrongType,
            }
        };
        // popped is the actual string owned by the storage (was in the list)

        // Return copy to caller; storage manages inserting the original into dst
        const result_copy = try allocator.dupe(u8, popped);
        errdefer allocator.free(result_copy);

        // Now handle source auto-delete and destination push.
        // We have the popped element (owned by storage), need to push it to dst.
        // Special case: src == dst (rotation)
        const same_list = std.mem.eql(u8, src, dst);

        if (same_list) {
            // Re-insert into the same list.
            // Safe: same-list rotation pops then re-inserts without auto-deleting the key
            // (auto-delete only happens on the cross-list path below).
            const src_entry2 = self.data.getEntry(src) orelse unreachable;
            const list2 = &src_entry2.value_ptr.list;
            if (dst_to_left) {
                list2.data.insert(self.allocator, 0, popped) catch |err| {
                    self.allocator.free(popped);
                    return err;
                };
            } else {
                list2.data.append(self.allocator, popped) catch |err| {
                    self.allocator.free(popped);
                    return err;
                };
            }
            return result_copy;
        }

        // Check if source should be deleted (now empty)
        // Re-fetch since the remove may have happened above
        if (self.data.getEntry(src)) |src_entry2| {
            if (src_entry2.value_ptr.list.data.items.len == 0) {
                const owned_key = src_entry2.key_ptr.*;
                var value = src_entry2.value_ptr.*;
                _ = self.data.remove(src);
                self.allocator.free(owned_key);
                value.deinit(self.allocator);
            }
        }

        // Push popped element to destination
        if (self.data.getEntry(dst)) |dst_entry| {
            switch (dst_entry.value_ptr.*) {
                .list => |*dst_list| {
                    if (dst_to_left) {
                        dst_list.data.insert(self.allocator, 0, popped) catch |err| {
                            self.allocator.free(popped);
                            return err;
                        };
                    } else {
                        dst_list.data.append(self.allocator, popped) catch |err| {
                            self.allocator.free(popped);
                            return err;
                        };
                    }
                },
                else => {
                    self.allocator.free(popped);
                    return error.WrongType;
                },
            }
        } else {
            // Create new destination list
            var new_list: std.ArrayList([]const u8) = .{
                .items = &.{},
                .capacity = 0,
            };
            new_list.append(self.allocator, popped) catch |err| {
                self.allocator.free(popped);
                return err;
            };

            const owned_key = self.allocator.dupe(u8, dst) catch |err| {
                // new_list.deinit frees popped (which was appended into new_list above)
                new_list.deinit(self.allocator);
                return err;
            };
            errdefer self.allocator.free(owned_key);

            self.data.put(owned_key, Value{
                .list = .{
                    .data = new_list,
                    .expires_at = null,
                },
            }) catch |err| {
                new_list.deinit(self.allocator);
                return err;
            };
        }

        return result_copy;
    }

    // Set operations

    /// Add members to a set
    /// Returns count of members actually added (excluding duplicates)
    /// Returns error.WrongType if key exists and is not a set
    pub fn sadd(
        self: *Storage,
        key: []const u8,
        members: []const []const u8,
        expires_at: ?i64,
    ) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        var added_count: usize = 0;

        // Get intset threshold from config
        const max_intset_entries = blk: {
            const config_val = self.config.get("set-max-intset-entries") catch {
                break :blk @as(u32, 512); // default
            };
            var val = config_val;
            defer val.deinit(self.allocator);
            const parsed = switch (val) {
                .int => |i| @as(u32, @intCast(@min(@max(i, 0), 1000000))),
                else => 512,
            };
            break :blk parsed;
        };

        if (self.data.getEntry(key)) |entry| {
            // Key exists - verify it's a set
            switch (entry.value_ptr.*) {
                .set => |*set_val| {
                    switch (set_val.encoding) {
                        .intset => {
                            // Try to add to intset
                            for (members) |member| {
                                // Check if member is an integer
                                const int_val = std.fmt.parseInt(i64, member, 10) catch {
                                    // Non-integer - must promote to hashmap
                                    try set_val.promoteToHashmap(self.allocator);
                                    // Retry adding all remaining members as hashmap
                                    const result = try self.saddHashmap(set_val, members, &added_count, 0);
                                    if (result > 0) self.incrementDirty(1);
                                    return result;
                                };

                                // Add to intset
                                const was_added = try set_val.data.intset.add(int_val);
                                if (was_added) {
                                    added_count += 1;
                                    // Check if exceeded threshold
                                    if (set_val.data.intset.length > max_intset_entries) {
                                        try set_val.promoteToHashmap(self.allocator);
                                        // Continue with hashmap for remaining members
                                        const members_left = members[added_count..];
                                        if (members_left.len > 0) {
                                            const remaining_added = try self.saddHashmap(set_val, members_left, &added_count, added_count);
                                            added_count = remaining_added;
                                        }
                                        if (added_count > 0) self.incrementDirty(1);
                                        return added_count;
                                    }
                                }
                            }
                            if (added_count > 0) self.incrementDirty(1);
                            return added_count;
                        },
                        .hashmap => {
                            const result = try self.saddHashmap(set_val, members, &added_count, 0);
                            if (result > 0) self.incrementDirty(1);
                            return result;
                        },
                    }
                },
                else => return error.WrongType,
            }
        } else {
            // Create new set - decide encoding based on members
            // Check if all members are integers and count
            var all_integers = true;
            var unique_members = std.ArrayListUnmanaged(i64){};
            defer unique_members.deinit(self.allocator);

            for (members) |member| {
                const int_val = std.fmt.parseInt(i64, member, 10) catch {
                    all_integers = false;
                    break;
                };
                // Check for duplicates within command
                const already_exists = for (unique_members.items) |existing| {
                    if (existing == int_val) break true;
                } else false;
                if (!already_exists) {
                    try unique_members.append(self.allocator, int_val);
                }
            }

            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);

            if (all_integers and unique_members.items.len <= max_intset_entries) {
                // Use intset encoding
                var intset = intset_mod.IntSet.init(self.allocator);
                errdefer intset.deinit();

                for (unique_members.items) |int_val| {
                    _ = try intset.add(int_val);
                    added_count += 1;
                }

                try self.data.put(owned_key, Value{
                    .set = .{
                        .encoding = .intset,
                        .data = .{ .intset = intset },
                        .expires_at = expires_at,
                    },
                });
            } else {
                // Use hashmap encoding
                var set_map = std.StringHashMap(void).init(self.allocator);
                errdefer {
                    var it = set_map.keyIterator();
                    while (it.next()) |k| {
                        self.allocator.free(k.*);
                    }
                    set_map.deinit();
                }

                for (members) |member| {
                    if (!set_map.contains(member)) {
                        const owned_member = try self.allocator.dupe(u8, member);
                        errdefer self.allocator.free(owned_member);
                        try set_map.put(owned_member, {});
                        added_count += 1;
                    }
                }

                try self.data.put(owned_key, Value{
                    .set = .{
                        .encoding = .hashmap,
                        .data = .{ .hashmap = set_map },
                        .expires_at = expires_at,
                    },
                });
            }

            if (added_count > 0) self.incrementDirty(1);
            return added_count;
        }
    }

    /// Helper: Add members to hashmap-encoded set
    fn saddHashmap(
        self: *Storage,
        set_val: *Value.SetValue,
        members: []const []const u8,
        added_count: *usize,
        start_index: usize,
    ) !usize {
        _ = start_index; // Not used, kept for API compatibility
        for (members) |member| {
            if (!set_val.data.hashmap.contains(member)) {
                const owned_member = try self.allocator.dupe(u8, member);
                errdefer self.allocator.free(owned_member);
                try set_val.data.hashmap.put(owned_member, {});
                added_count.* += 1;
            }
        }
        return added_count.*;
    }

    /// Remove members from a set
    /// Returns count of members actually removed (non-existent members ignored)
    /// Returns 0 if key doesn't exist (treated as empty set)
    /// Auto-deletes key if set becomes empty
    pub fn srem(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        members: []const []const u8,
    ) !usize {
        _ = allocator; // Not used in this function
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return 0;

        switch (entry.value_ptr.*) {
            .set => |*set_val| {
                var removed_count: usize = 0;

                switch (set_val.encoding) {
                    .intset => {
                        for (members) |member| {
                            // Try to parse as integer
                            const int_val = std.fmt.parseInt(i64, member, 10) catch continue;
                            const was_removed = try set_val.data.intset.remove(int_val);
                            if (was_removed) removed_count += 1;
                        }

                        // Auto-delete if set becomes empty
                        if (set_val.data.intset.length == 0) {
                            const owned_key = entry.key_ptr.*;
                            var value = entry.value_ptr.*;
                            _ = self.removeKeyCleanup(key);
                            self.allocator.free(owned_key);
                            value.deinit(self.allocator);
                        }

                        return removed_count;
                    },
                    .hashmap => {
                        for (members) |member| {
                            if (set_val.data.hashmap.fetchRemove(member)) |kv| {
                                self.allocator.free(kv.key);
                                removed_count += 1;
                            }
                        }

                        // Auto-delete if set becomes empty
                        if (set_val.data.hashmap.count() == 0) {
                            const owned_key = entry.key_ptr.*;
                            var value = entry.value_ptr.*;
                            _ = self.removeKeyCleanup(key);
                            self.allocator.free(owned_key);
                            value.deinit(self.allocator);
                        }

                        return removed_count;
                    },
                }
            },
            else => return error.WrongType,
        }
    }

    /// Check if member exists in set
    /// Returns true if member is in set, false otherwise
    /// Returns false if key doesn't exist
    /// Returns error.WrongType if key is not a set
    pub fn sismember(
        self: *Storage,
        key: []const u8,
        member: []const u8,
    ) !bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return false;

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return false;
        }

        switch (entry.value_ptr.*) {
            .set => |*set_val| {
                switch (set_val.encoding) {
                    .intset => {
                        const int_val = std.fmt.parseInt(i64, member, 10) catch return false;
                        return try set_val.data.intset.contains(int_val);
                    },
                    .hashmap => {
                        return set_val.data.hashmap.contains(member);
                    },
                }
            },
            else => return error.WrongType,
        }
    }

    /// Get all members of a set
    /// Returns owned slice of members (caller must free outer slice, NOT member strings)
    /// Returns null if key doesn't exist or is not a set
    pub fn smembers(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
    ) !?[][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .set => |*set_val| {
                switch (set_val.encoding) {
                    .intset => {
                        const member_count = set_val.data.intset.length;
                        var result = try allocator.alloc([]const u8, member_count);
                        errdefer allocator.free(result);

                        // Convert integers to strings
                        var i: usize = 0;
                        while (i < member_count) : (i += 1) {
                            const int_val = try set_val.data.intset.getAt(i);
                            result[i] = try std.fmt.allocPrint(allocator, "{d}", .{int_val});
                        }

                        return result;
                    },
                    .hashmap => {
                        const member_count = set_val.data.hashmap.count();
                        var result = try allocator.alloc([]const u8, member_count);
                        errdefer allocator.free(result);

                        var it = set_val.data.hashmap.keyIterator();
                        var i: usize = 0;
                        while (it.next()) |member_ptr| : (i += 1) {
                            result[i] = member_ptr.*;
                        }

                        return result;
                    },
                }
            },
            else => return null,
        }
    }

    /// Get cardinality (size) of set
    /// Returns null if key doesn't exist or is not a set
    pub fn scard(
        self: *Storage,
        key: []const u8,
    ) ?usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .set => |set_val| {
                return switch (set_val.encoding) {
                    .intset => set_val.data.intset.length,
                    .hashmap => set_val.data.hashmap.count(),
                };
            },
            else => return null,
        }
    }

    // Hash operations

    /// Set field-value pairs in a hash
    /// Returns count of fields actually added (not updated)
    /// Returns error.WrongType if key exists and is not a hash
    pub fn hset(
        self: *Storage,
        key: []const u8,
        fields: []const []const u8,
        values: []const []const u8,
        expires_at: ?i64,
    ) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        var added_count: usize = 0;

        if (self.data.getEntry(key)) |entry| {
            // Key exists - verify it's a hash
            switch (entry.value_ptr.*) {
                .hash => |*hash_val| {
                    // Set fields in existing hash
                    for (fields, values) |field, value| {
                        const is_new = !hash_val.data.contains(field);

                        if (is_new) {
                            // New field - duplicate both
                            const owned_field = try self.allocator.dupe(u8, field);
                            errdefer self.allocator.free(owned_field);

                            const owned_value = try self.allocator.dupe(u8, value);
                            errdefer self.allocator.free(owned_value);

                            try hash_val.data.put(owned_field, Value.FieldValue{
                                .data = owned_value,
                                .expires_at = null, // No field-level expiration by default
                            });
                            added_count += 1;
                        } else {
                            // Existing field - free old value, set new one
                            const old_field_val = hash_val.data.get(field).?;
                            self.allocator.free(old_field_val.data);

                            const owned_value = try self.allocator.dupe(u8, value);
                            errdefer self.allocator.free(owned_value);

                            try hash_val.data.put(field, Value.FieldValue{
                                .data = owned_value,
                                .expires_at = old_field_val.expires_at, // Preserve field expiration
                            });
                        }
                    }
                    self.bumpKeyVersionLocked(key);
                    self.incrementDirty(1);
                    return added_count;
                },
                else => return error.WrongType,
            }
        } else {
            // Create new hash
            var hash_map = std.StringHashMap(Value.FieldValue).init(self.allocator);
            errdefer {
                var it = hash_map.iterator();
                while (it.next()) |entry| {
                    self.allocator.free(entry.key_ptr.*);
                    var field_val = entry.value_ptr.*;
                    field_val.deinit(self.allocator);
                }
                hash_map.deinit();
            }

            // Add all field-value pairs
            for (fields, values) |field, value| {
                const owned_field = try self.allocator.dupe(u8, field);
                errdefer self.allocator.free(owned_field);

                const owned_value = try self.allocator.dupe(u8, value);
                errdefer self.allocator.free(owned_value);

                try hash_map.put(owned_field, Value.FieldValue{
                    .data = owned_value,
                    .expires_at = null,
                });
                added_count += 1;
            }

            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);

            try self.data.put(owned_key, Value{
                .hash = .{
                    .data = hash_map,
                    .expires_at = expires_at,
                },
            });

            self.bumpKeyVersionLocked(key);
            self.incrementDirty(1);
            return added_count;
        }
    }

    /// Get value of a field in a hash
    /// Returns null if key doesn't exist, field doesn't exist, or key is not a hash
    pub fn hget(
        self: *Storage,
        key: []const u8,
        field: []const u8,
    ) ?[]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .hash => |*hash_val| {
                const field_val = hash_val.data.get(field) orelse return null;
                // Check field expiration
                if (field_val.expires_at) |expires_at| {
                    if (expires_at <= getCurrentTimestamp()) {
                        // Field expired - return null (lazy cleanup will remove it later)
                        return null;
                    }
                }
                return field_val.data;
            },
            else => return null,
        }
    }

    /// Delete fields from a hash
    /// Returns count of fields actually deleted
    /// Returns 0 if key doesn't exist
    /// Auto-deletes key if hash becomes empty
    pub fn hdel(
        self: *Storage,
        key: []const u8,
        fields: []const []const u8,
    ) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return 0;

        switch (entry.value_ptr.*) {
            .hash => |*hash_val| {
                var deleted_count: usize = 0;

                for (fields) |field| {
                    if (hash_val.data.fetchRemove(field)) |kv| {
                        self.allocator.free(kv.key);
                        var field_val = kv.value;
                        field_val.deinit(self.allocator);
                        deleted_count += 1;
                    }
                }

                // Auto-delete if hash becomes empty
                if (hash_val.data.count() == 0) {
                    const owned_key = entry.key_ptr.*;
                    var value = entry.value_ptr.*;
                    _ = self.removeKeyCleanup(key);
                    self.allocator.free(owned_key);
                    value.deinit(self.allocator);
                }

                return deleted_count;
            },
            else => return error.WrongType,
        }
    }

    /// Get all fields and values in a hash
    /// Returns array where even indices are fields, odd indices are values
    /// Returns null if key doesn't exist or is not a hash
    pub fn hgetall(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
    ) !?[][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .hash => |*hash_val| {
                const field_count = hash_val.data.count();
                var result = try allocator.alloc([]const u8, field_count * 2);
                errdefer allocator.free(result);

                var it = hash_val.data.iterator();
                var i: usize = 0;
                while (it.next()) |pair| {
                    // Skip expired fields
                    if (pair.value_ptr.*.expires_at) |expires_at| {
                        if (expires_at <= getCurrentTimestamp()) {
                            continue;
                        }
                    }
                    result[i] = pair.key_ptr.*;
                    result[i + 1] = pair.value_ptr.*.data;
                    i += 2;
                }

                // Resize if we skipped expired fields
                if (i < result.len) {
                    const resized = try allocator.realloc(result, i);
                    return resized;
                }
                return result;
            },
            else => return null,
        }
    }

    /// Get all field names in a hash
    /// Returns null if key doesn't exist or is not a hash
    pub fn hkeys(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
    ) !?[][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .hash => |*hash_val| {
                const field_count = hash_val.data.count();
                var result = try allocator.alloc([]const u8, field_count);
                errdefer allocator.free(result);

                var it = hash_val.data.keyIterator();
                var i: usize = 0;
                while (it.next()) |field_ptr| : (i += 1) {
                    result[i] = field_ptr.*;
                }

                return result;
            },
            else => return null,
        }
    }

    /// Get all values in a hash
    /// Returns null if key doesn't exist or is not a hash
    pub fn hvals(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
    ) !?[][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .hash => |*hash_val| {
                const field_count = hash_val.data.count();
                var result = try allocator.alloc([]const u8, field_count);
                errdefer allocator.free(result);

                var it = hash_val.data.valueIterator();
                var i: usize = 0;
                while (it.next()) |value_ptr| {
                    // Skip expired fields
                    if (value_ptr.*.expires_at) |expires_at| {
                        if (expires_at <= getCurrentTimestamp()) {
                            continue;
                        }
                    }
                    result[i] = value_ptr.*.data;
                    i += 1;
                }

                // Resize if we skipped expired fields
                if (i < result.len) {
                    const resized = try allocator.realloc(result, i);
                    return resized;
                }
                return result;
            },
            else => return null,
        }
    }

    /// Check if field exists in hash
    /// Returns true if field exists, false otherwise
    /// Returns false if key doesn't exist
    /// Returns error.WrongType if key is not a hash
    pub fn hexists(
        self: *Storage,
        key: []const u8,
        field: []const u8,
    ) !bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return false;

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return false;
        }

        switch (entry.value_ptr.*) {
            .hash => |*hash_val| {
                return hash_val.data.contains(field);
            },
            else => return error.WrongType,
        }
    }

    /// Get number of fields in a hash
    /// Returns null if key doesn't exist or is not a hash
    pub fn hlen(
        self: *Storage,
        key: []const u8,
    ) ?usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .hash => |hash_val| return hash_val.data.count(),
            else => return null,
        }
    }

    // Sorted set operations

    /// Insert a scored member into a sorted list at the correct position
    /// Sorted by score ascending, then lexicographically for equal scores
    fn insertSortedMember(list: *std.ArrayList(Value.ScoredMember), score: f64, member: []const u8, allocator: std.mem.Allocator) !void {
        var low: usize = 0;
        var high: usize = list.items.len;

        while (low < high) {
            const mid = low + (high - low) / 2;
            const mid_score = list.items[mid].score;

            if (score < mid_score) {
                high = mid;
            } else if (score > mid_score) {
                low = mid + 1;
            } else {
                // Same score - compare lexicographically
                if (std.mem.lessThan(u8, member, list.items[mid].member)) {
                    high = mid;
                } else {
                    low = mid + 1;
                }
            }
        }

        try list.insert(allocator, low, .{ .score = score, .member = member });
    }

    /// Add members with scores to a sorted set
    /// options: bit flags (NX=1, XX=2, CH=4)
    /// Returns count of new elements added (or changed elements if CH flag set)
    /// Returns error.WrongType if key exists and is not a sorted set
    pub fn zadd(
        self: *Storage,
        key: []const u8,
        scores: []const f64,
        members: []const []const u8,
        options: u8,
        expires_at: ?i64,
    ) !struct { added: usize, changed: usize } {
        self.mutex.lock();
        defer self.mutex.unlock();

        const nx_flag = (options & 1) != 0;
        const xx_flag = (options & 2) != 0;
        const gt_flag = (options & 8) != 0;
        const lt_flag = (options & 16) != 0;
        var added_count: usize = 0;
        var changed_count: usize = 0;

        if (self.data.getEntry(key)) |entry| {
            // Key exists - verify it's a sorted set
            switch (entry.value_ptr.*) {
                .sorted_set => |*zset| {
                    for (scores, members) |score, member| {
                        if (zset.members.get(member)) |old_score| {
                            // Member exists
                            if (nx_flag) continue; // NX: skip existing

                            // GT: only update if new score > old score
                            if (gt_flag and score <= old_score) continue;
                            // LT: only update if new score < old score
                            if (lt_flag and score >= old_score) continue;

                            if (old_score != score) {
                                // Remove old entry from sorted list
                                for (zset.sorted_list.items, 0..) |item, idx| {
                                    if (std.mem.eql(u8, item.member, member)) {
                                        _ = zset.sorted_list.orderedRemove(idx);
                                        break;
                                    }
                                }
                                // Update score in map
                                try zset.members.put(member, score);
                                // Re-insert at correct sorted position
                                // The member ptr now comes from the hashmap key
                                const stored_key = zset.members.getKey(member).?;
                                try insertSortedMember(&zset.sorted_list, score, stored_key, self.allocator);
                                changed_count += 1;
                            }
                        } else {
                            // Member does not exist
                            if (xx_flag) continue; // XX: skip new

                            const owned_member = try self.allocator.dupe(u8, member);
                            errdefer self.allocator.free(owned_member);

                            try zset.members.put(owned_member, score);
                            // Use the stored key pointer for the sorted list
                            const stored_key = zset.members.getKey(member).?;
                            try insertSortedMember(&zset.sorted_list, score, stored_key, self.allocator);
                            added_count += 1;
                            changed_count += 1;
                        }
                    }
                    self.bumpKeyVersionLocked(key);
                    if (added_count > 0 or changed_count > 0) self.incrementDirty(1);
                    return .{ .added = added_count, .changed = changed_count };
                },
                else => return error.WrongType,
            }
        } else {
            // Create new sorted set (unless XX flag)
            if (xx_flag) return .{ .added = 0, .changed = 0 };

            var member_map = std.StringHashMap(f64).init(self.allocator);
            errdefer {
                var it = member_map.keyIterator();
                while (it.next()) |k| self.allocator.free(k.*);
                member_map.deinit();
            }

            var sorted: std.ArrayList(Value.ScoredMember) = .{
                .items = &.{},
                .capacity = 0,
            };
            errdefer sorted.deinit(self.allocator);

            for (scores, members) |score, member| {
                if (member_map.contains(member)) {
                    // Duplicate in same command: update score
                    // Remove old entry from sorted list
                    for (sorted.items, 0..) |item, idx| {
                        if (std.mem.eql(u8, item.member, member)) {
                            _ = sorted.orderedRemove(idx);
                            break;
                        }
                    }
                    try member_map.put(member, score);
                    const stored_key = member_map.getKey(member).?;
                    try insertSortedMember(&sorted, score, stored_key, self.allocator);
                } else {
                    const owned_member = try self.allocator.dupe(u8, member);
                    errdefer self.allocator.free(owned_member);

                    try member_map.put(owned_member, score);
                    const stored_key = member_map.getKey(member).?;
                    try insertSortedMember(&sorted, score, stored_key, self.allocator);
                    added_count += 1;
                    changed_count += 1;
                }
            }

            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);

            try self.data.put(owned_key, Value{
                .sorted_set = .{
                    .members = member_map,
                    .sorted_list = sorted,
                    .expires_at = expires_at,
                },
            });

            self.bumpKeyVersionLocked(key);
            if (added_count > 0) self.incrementDirty(1);
            return .{ .added = added_count, .changed = changed_count };
        }
    }

    /// Remove members from a sorted set
    /// Returns count of members actually removed
    /// Returns 0 if key doesn't exist
    /// Returns error.WrongType if key is not a sorted set
    pub fn zrem(
        self: *Storage,
        key: []const u8,
        members: []const []const u8,
    ) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return 0;

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| {
                var removed_count: usize = 0;

                for (members) |member| {
                    if (zset.members.contains(member)) {
                        // Remove from sorted list first
                        for (zset.sorted_list.items, 0..) |item, idx| {
                            if (std.mem.eql(u8, item.member, member)) {
                                _ = zset.sorted_list.orderedRemove(idx);
                                break;
                            }
                        }
                        // Remove from hash map (frees the key)
                        if (zset.members.fetchRemove(member)) |kv| {
                            self.allocator.free(kv.key);
                        }
                        removed_count += 1;
                    }
                }

                // Auto-delete if sorted set becomes empty
                if (zset.members.count() == 0) {
                    const owned_key = entry.key_ptr.*;
                    var value = entry.value_ptr.*;
                    _ = self.removeKeyCleanup(key);
                    self.allocator.free(owned_key);
                    value.deinit(self.allocator);
                }

                return removed_count;
            },
            else => return error.WrongType,
        }
    }

    /// Get members in sorted set by rank range [start, stop]
    /// Supports negative indices (-1 = last element)
    /// If with_scores is true, returns interleaved [member, score, member, score, ...]
    /// Returns null if key doesn't exist or is not a sorted set
    pub fn zrange(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        start: i64,
        stop: i64,
        with_scores: bool,
    ) !?[][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| {
                const len = zset.sorted_list.items.len;
                if (len == 0) {
                    return try allocator.alloc([]const u8, 0);
                }

                // Normalize indices
                const norm_start: i64 = if (start < 0)
                    @max(0, @as(i64, @intCast(len)) + start)
                else
                    @min(start, @as(i64, @intCast(len)));

                const norm_stop: i64 = if (stop < 0)
                    @max(-1, @as(i64, @intCast(len)) + stop)
                else
                    @min(stop, @as(i64, @intCast(len)) - 1);

                if (norm_start > norm_stop or norm_start >= @as(i64, @intCast(len))) {
                    return try allocator.alloc([]const u8, 0);
                }

                const u_start: usize = @intCast(norm_start);
                const u_stop: usize = @intCast(norm_stop);
                const range_len = u_stop - u_start + 1;
                const result_len = if (with_scores) range_len * 2 else range_len;

                var result = try allocator.alloc([]const u8, result_len);
                errdefer allocator.free(result);

                if (with_scores) {
                    var i: usize = 0;
                    for (u_start..u_stop + 1) |idx| {
                        const item = zset.sorted_list.items[idx];
                        result[i] = item.member;
                        i += 1;
                        // Format score as string
                        var score_buf: [64]u8 = undefined;
                        const score_str = try std.fmt.bufPrint(&score_buf, "{d}", .{item.score});
                        const owned_score = try allocator.dupe(u8, score_str);
                        result[i] = owned_score;
                        i += 1;
                    }
                } else {
                    for (0..range_len) |i| {
                        result[i] = zset.sorted_list.items[u_start + i].member;
                    }
                }

                return result;
            },
            else => return null,
        }
    }

    /// Get members in sorted set with scores in range [min_score, max_score]
    /// Supports exclusive intervals via min_exclusive/max_exclusive flags
    /// If with_scores is true, returns interleaved [member, score, member, score, ...]
    /// LIMIT offset/count for pagination (null = no limit)
    /// Returns null if key doesn't exist or is not a sorted set
    pub fn zrangebyscore(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        min_score: f64,
        max_score: f64,
        min_exclusive: bool,
        max_exclusive: bool,
        with_scores: bool,
        limit_offset: ?usize,
        limit_count: ?usize,
    ) !?[][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| {
                var result_members: std.ArrayList(Value.ScoredMember) = .{
                    .items = &.{},
                    .capacity = 0,
                };
                defer result_members.deinit(allocator);

                for (zset.sorted_list.items) |item| {
                    // Check lower bound
                    const above_min = if (min_exclusive)
                        item.score > min_score
                    else
                        item.score >= min_score;

                    // Check upper bound
                    const below_max = if (max_exclusive)
                        item.score < max_score
                    else
                        item.score <= max_score;

                    if (above_min and below_max) {
                        try result_members.append(allocator, item);
                    } else if (item.score > max_score) {
                        break; // List is sorted, no need to continue
                    }
                }

                // Apply LIMIT
                var start_idx: usize = 0;
                var end_idx: usize = result_members.items.len;

                if (limit_offset) |offset| {
                    start_idx = @min(offset, end_idx);
                }
                if (limit_count) |count| {
                    end_idx = @min(start_idx + count, end_idx);
                }

                const effective_len = end_idx - start_idx;
                const result_len = if (with_scores) effective_len * 2 else effective_len;
                var result = try allocator.alloc([]const u8, result_len);
                errdefer allocator.free(result);

                if (with_scores) {
                    var i: usize = 0;
                    for (result_members.items[start_idx..end_idx]) |item| {
                        result[i] = item.member;
                        i += 1;
                        var score_buf: [64]u8 = undefined;
                        const score_str = try std.fmt.bufPrint(&score_buf, "{d}", .{item.score});
                        const owned_score = try allocator.dupe(u8, score_str);
                        result[i] = owned_score;
                        i += 1;
                    }
                } else {
                    for (result_members.items[start_idx..end_idx], 0..) |item, i| {
                        result[i] = item.member;
                    }
                }

                return result;
            },
            else => return null,
        }
    }

    /// Get score of a member in a sorted set
    /// Returns null if key doesn't exist or member doesn't exist
    pub fn zscore(
        self: *Storage,
        key: []const u8,
        member: []const u8,
    ) ?f64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| return zset.members.get(member),
            else => return null,
        }
    }

    /// Get cardinality (number of members) of a sorted set
    /// Returns null if key doesn't exist or is not a sorted set
    pub fn zcard(
        self: *Storage,
        key: []const u8,
    ) ?usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| return zset.members.count(),
            else => return null,
        }
    }

    /// Increment integer field in hash by increment
    /// Creates field with value 0 if it does not exist
    /// Returns new integer value after increment
    /// Returns error.WrongType if key is not a hash
    /// Returns error.InvalidValue if field value is not an integer
    /// Returns error.Overflow if increment would overflow i64 range
    pub fn hincrby(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        field: []const u8,
        increment: i64,
    ) !i64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Get or create the hash entry
        if (self.data.getEntry(key)) |entry| {
            switch (entry.value_ptr.*) {
                .hash => |*hash_val| {
                    const field_val = hash_val.data.get(field);
                    const current_str = if (field_val) |fv| fv.data else "0";
                    const current = std.fmt.parseInt(i64, current_str, 10) catch return error.InvalidValue;
                    const new_value = std.math.add(i64, current, increment) catch return error.Overflow;

                    // Format new value as string
                    var buf: [32]u8 = undefined;
                    const new_str = std.fmt.bufPrint(&buf, "{d}", .{new_value}) catch return error.InvalidValue;
                    const owned_new = try allocator.dupe(u8, new_str);
                    errdefer allocator.free(owned_new);

                    if (hash_val.data.contains(field)) {
                        // Free old value string
                        const old_val = hash_val.data.get(field).?;
                        self.allocator.free(old_val.data);
                        try hash_val.data.put(field, Value.FieldValue{
                            .data = owned_new,
                            .expires_at = old_val.expires_at, // Preserve field expiration
                        });
                    } else {
                        // New field: duplicate key
                        const owned_field = try allocator.dupe(u8, field);
                        errdefer allocator.free(owned_field);
                        try hash_val.data.put(owned_field, Value.FieldValue{
                            .data = owned_new,
                            .expires_at = null,
                        });
                    }
                    return new_value;
                },
                else => return error.WrongType,
            }
        } else {
            // Create new hash with single field
            var hash_map = std.StringHashMap(Value.FieldValue).init(self.allocator);
            errdefer {
                var it = hash_map.iterator();
                while (it.next()) |e| {
                    self.allocator.free(e.key_ptr.*);
                    var field_val = e.value_ptr.*;
                    field_val.deinit(self.allocator);
                }
                hash_map.deinit();
            }

            var buf: [32]u8 = undefined;
            const new_str = std.fmt.bufPrint(&buf, "{d}", .{increment}) catch return error.InvalidValue;
            const owned_field = try self.allocator.dupe(u8, field);
            errdefer self.allocator.free(owned_field);
            const owned_value = try self.allocator.dupe(u8, new_str);
            errdefer self.allocator.free(owned_value);
            try hash_map.put(owned_field, Value.FieldValue{
                .data = owned_value,
                .expires_at = null,
            });

            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);
            try self.data.put(owned_key, Value{
                .hash = .{ .data = hash_map, .expires_at = null },
            });
            return increment;
        }
    }

    /// Increment float field in hash by increment
    /// Creates field with value 0 if it does not exist
    /// Returns new float value after increment
    /// Returns error.WrongType if key is not a hash
    /// Returns error.InvalidValue if field value is not a float
    pub fn hincrbyfloat(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        field: []const u8,
        increment: f64,
    ) !f64 {
        if (std.math.isNan(increment) or std.math.isInf(increment)) {
            return error.NanOrInfinity;
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.data.getEntry(key)) |entry| {
            switch (entry.value_ptr.*) {
                .hash => |*hash_val| {
                    const field_val = hash_val.data.get(field);
                    const current_str = if (field_val) |fv| fv.data else "0";
                    const current = std.fmt.parseFloat(f64, current_str) catch return error.InvalidValue;
                    if (std.math.isNan(current) or std.math.isInf(current)) return error.InvalidValue;
                    const new_value = current + increment;
                    if (std.math.isNan(new_value) or std.math.isInf(new_value)) return error.NanOrInfinity;

                    // Format with enough precision, trim trailing zeros
                    var buf: [64]u8 = undefined;
                    const new_str = formatFloat(&buf, new_value);
                    const owned_new = try allocator.dupe(u8, new_str);
                    errdefer allocator.free(owned_new);

                    if (hash_val.data.contains(field)) {
                        const old_val = hash_val.data.get(field).?;
                        self.allocator.free(old_val.data);
                        try hash_val.data.put(field, Value.FieldValue{
                            .data = owned_new,
                            .expires_at = old_val.expires_at,
                        });
                    } else {
                        const owned_field = try allocator.dupe(u8, field);
                        errdefer allocator.free(owned_field);
                        try hash_val.data.put(owned_field, Value.FieldValue{
                            .data = owned_new,
                            .expires_at = null,
                        });
                    }
                    return new_value;
                },
                else => return error.WrongType,
            }
        } else {
            var hash_map = std.StringHashMap(Value.FieldValue).init(self.allocator);
            errdefer {
                var it = hash_map.iterator();
                while (it.next()) |e| {
                    self.allocator.free(e.key_ptr.*);
                    var field_val = e.value_ptr.*;
                    field_val.deinit(self.allocator);
                }
                hash_map.deinit();
            }

            var buf: [64]u8 = undefined;
            const new_str = formatFloat(&buf, increment);
            const owned_field = try self.allocator.dupe(u8, field);
            errdefer self.allocator.free(owned_field);
            const owned_value = try self.allocator.dupe(u8, new_str);
            errdefer self.allocator.free(owned_value);
            try hash_map.put(owned_field, Value.FieldValue{
                .data = owned_value,
                .expires_at = null,
            });

            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);
            try self.data.put(owned_key, Value{
                .hash = .{ .data = hash_map, .expires_at = null },
            });
            return increment;
        }
    }

    /// Set field in hash only if field does not exist
    /// Returns true if the field was set, false if it already existed
    /// Returns error.WrongType if key is not a hash
    pub fn hsetnx(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        field: []const u8,
        value: []const u8,
    ) !bool {
        _ = allocator;
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.data.getEntry(key)) |entry| {
            switch (entry.value_ptr.*) {
                .hash => |*hash_val| {
                    if (hash_val.data.contains(field)) return false;
                    const owned_field = try self.allocator.dupe(u8, field);
                    errdefer self.allocator.free(owned_field);
                    const owned_value = try self.allocator.dupe(u8, value);
                    errdefer self.allocator.free(owned_value);
                    try hash_val.data.put(owned_field, Value.FieldValue{
                        .data = owned_value,
                        .expires_at = null,
                    });
                    return true;
                },
                else => return error.WrongType,
            }
        } else {
            var hash_map = std.StringHashMap(Value.FieldValue).init(self.allocator);
            errdefer {
                var it = hash_map.iterator();
                while (it.next()) |e| {
                    self.allocator.free(e.key_ptr.*);
                    var field_val = e.value_ptr.*;
                    field_val.deinit(self.allocator);
                }
                hash_map.deinit();
            }
            const owned_field = try self.allocator.dupe(u8, field);
            errdefer self.allocator.free(owned_field);
            const owned_value = try self.allocator.dupe(u8, value);
            errdefer self.allocator.free(owned_value);
            try hash_map.put(owned_field, Value.FieldValue{
                .data = owned_value,
                .expires_at = null,
            });

            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);
            try self.data.put(owned_key, Value{
                .hash = .{ .data = hash_map, .expires_at = null },
            });
            return true;
        }
    }

    /// Get the string length of the value associated with field in the hash.
    /// Returns 0 if the field does not exist.
    /// Returns error.WrongType if key is not a hash.
    pub fn hstrlen(
        self: *Storage,
        key: []const u8,
        field: []const u8,
    ) error{WrongType}!usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return 0;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return 0;
        }

        switch (entry.value_ptr.*) {
            .hash => |*hash_val| {
                if (hash_val.data.get(field)) |field_val| {
                    // Check field expiration
                    if (field_val.expires_at) |expires_at| {
                        if (expires_at <= getCurrentTimestamp()) {
                            return 0; // Expired field
                        }
                    }
                    return field_val.data.len;
                } else {
                    return 0;
                }
            },
            else => return error.WrongType,
        }
    }

    /// Return random field(s) from hash
    /// If count is null, returns a single field
    /// If count is positive, returns up to count distinct fields (no repeats)
    /// If count is negative, may return repeated fields (allows duplicates)
    /// If with_values is true, returns field-value pairs (field1, value1, field2, value2, ...)
    /// Otherwise returns just fields
    /// Returns null if key doesn't exist
    /// Caller must free the returned slice
    pub fn hrandfield(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        count: ?i64,
        with_values: bool,
    ) error{ WrongType, OutOfMemory }!?[]const []const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .hash => |*hash_val| {
                const hash_size = hash_val.data.count();
                if (hash_size == 0) {
                    return null;
                }

                if (count == null) {
                    // Return single random field
                    const idx = @as(usize, @intCast(@mod(std.time.nanoTimestamp(), @as(i128, @intCast(hash_size)))));
                    var it = hash_val.data.iterator();
                    var current: usize = 0;
                    while (it.next()) |entry_item| {
                        if (current == idx) {
                            var result = try allocator.alloc([]const u8, 1);
                            result[0] = entry_item.key_ptr.*;
                            return result;
                        }
                        current += 1;
                    }
                    return null;
                } else {
                    const count_val = count.?;
                    const abs_count = if (count_val < 0) -count_val else count_val;
                    const allow_duplicates = count_val < 0;

                    // Collect all fields into an array
                    var all_fields = try std.ArrayList([]const u8).initCapacity(allocator, hash_size);
                    defer all_fields.deinit(allocator);

                    var it = hash_val.data.iterator();
                    while (it.next()) |entry_item| {
                        try all_fields.append(allocator, entry_item.key_ptr.*);
                    }

                    // Determine how many to return
                    const result_count = if (allow_duplicates) @as(usize, @intCast(abs_count)) else @min(@as(usize, @intCast(abs_count)), hash_size);

                    // Build result
                    const result_size = if (with_values) result_count * 2 else result_count;
                    var result = try allocator.alloc([]const u8, result_size);

                    if (allow_duplicates) {
                        // Can have repeats - just pick randomly
                        for (0..result_count) |i| {
                            const idx = @as(usize, @intCast(@mod(std.time.nanoTimestamp() +% @as(i128, @intCast(i)), @as(i128, @intCast(hash_size)))));
                            const field = all_fields.items[idx];
                            if (with_values) {
                                result[i * 2] = field;
                                result[i * 2 + 1] = hash_val.data.get(field).?.data;
                            } else {
                                result[i] = field;
                            }
                        }
                    } else {
                        // No repeats - use simple random selection with visited tracking
                        var visited = try allocator.alloc(bool, hash_size);
                        defer allocator.free(visited);
                        @memset(visited, false);

                        for (0..result_count) |i| {
                            const rnd = @as(usize, @intCast(@mod(std.time.nanoTimestamp() +% @as(i128, @intCast(i)), @as(i128, @intCast(hash_size)))));
                            // Find next unvisited starting at rnd
                            var j: usize = 0;
                            while (j < hash_size) : (j += 1) {
                                const candidate = (rnd + j) % hash_size;
                                if (!visited[candidate]) {
                                    visited[candidate] = true;
                                    const field = all_fields.items[candidate];
                                    if (with_values) {
                                        result[i * 2] = field;
                                        result[i * 2 + 1] = hash_val.data.get(field).?.data;
                                    } else {
                                        result[i] = field;
                                    }
                                    break;
                                }
                            }
                        }
                    }

                    return result;
                }
            },
            else => return error.WrongType,
        }
    }

    /// Set expiration time for hash fields
    /// options: bit flags (NX=1, XX=2, GT=4, LT=8)
    /// Returns number of fields successfully updated
    /// Returns error.WrongType if key is not a hash
    pub fn hexpire(
        self: *Storage,
        key: []const u8,
        fields: []const []const u8,
        expires_at_ms: i64,
        options: u8,
    ) error{WrongType}!usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return 0;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return 0;
        }

        switch (entry.value_ptr.*) {
            .hash => |*hash_val| {
                const nx_flag = (options & 1) != 0; // NX: set expiry only if field has no expiry
                const xx_flag = (options & 2) != 0; // XX: set expiry only if field has expiry
                const gt_flag = (options & 4) != 0; // GT: set expiry only if new expiry > current
                const lt_flag = (options & 8) != 0; // LT: set expiry only if new expiry < current

                var count: usize = 0;
                for (fields) |field| {
                    if (hash_val.data.getEntry(field)) |field_entry| {
                        const field_val_ptr = field_entry.value_ptr;
                        const current_expiry = field_val_ptr.expires_at;

                        // Apply option flags
                        if (nx_flag and current_expiry != null) continue;
                        if (xx_flag and current_expiry == null) continue;
                        if (gt_flag and current_expiry != null and expires_at_ms <= current_expiry.?) continue;
                        if (lt_flag and current_expiry != null and expires_at_ms >= current_expiry.?) continue;

                        field_val_ptr.expires_at = expires_at_ms;
                        count += 1;
                    }
                }
                return count;
            },
            else => return error.WrongType,
        }
    }

    /// Remove expiration from hash fields
    /// Returns number of fields with expiration removed
    /// Returns error.WrongType if key is not a hash
    pub fn hpersist(
        self: *Storage,
        key: []const u8,
        fields: []const []const u8,
    ) error{WrongType}!usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return 0;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return 0;
        }

        switch (entry.value_ptr.*) {
            .hash => |*hash_val| {
                var count: usize = 0;
                for (fields) |field| {
                    if (hash_val.data.getEntry(field)) |field_entry| {
                        if (field_entry.value_ptr.expires_at != null) {
                            field_entry.value_ptr.expires_at = null;
                            count += 1;
                        }
                    }
                }
                return count;
            },
            else => return error.WrongType,
        }
    }

    /// Get TTL in milliseconds for hash fields
    /// Returns array of TTL values (-2 if field doesn't exist, -1 if no expiry, positive for TTL)
    /// Caller must free the returned slice
    /// Returns error.WrongType if key is not a hash
    pub fn hpttl(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        fields: []const []const u8,
    ) error{ WrongType, OutOfMemory }![]i64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var result = try allocator.alloc(i64, fields.len);

        const entry = self.data.getEntry(key) orelse {
            // Key doesn't exist - all fields return -2
            for (result) |*r| r.* = -2;
            return result;
        };

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            // Key expired - all fields return -2
            for (result) |*r| r.* = -2;
            return result;
        }

        switch (entry.value_ptr.*) {
            .hash => |*hash_val| {
                const now = getCurrentTimestamp();
                for (fields, 0..) |field, i| {
                    if (hash_val.data.get(field)) |field_val| {
                        if (field_val.expires_at) |expires_at| {
                            const ttl = expires_at - now;
                            result[i] = if (ttl > 0) ttl else -2; // Expired
                        } else {
                            result[i] = -1; // No expiry
                        }
                    } else {
                        result[i] = -2; // Field doesn't exist
                    }
                }
                return result;
            },
            else => {
                allocator.free(result);
                return error.WrongType;
            },
        }
    }

    /// Get TTL in seconds for hash fields
    /// Returns array of TTL values (-2 if field doesn't exist, -1 if no expiry, positive for TTL)
    /// Caller must free the returned slice
    /// Returns error.WrongType if key is not a hash
    pub fn httl(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        fields: []const []const u8,
    ) error{ WrongType, OutOfMemory }![]i64 {
        const ttls_ms = try self.hpttl(allocator, key, fields);
        // Convert milliseconds to seconds
        for (ttls_ms) |*ttl| {
            if (ttl.* > 0) {
                ttl.* = @divFloor(ttl.*, 1000);
            }
        }
        return ttls_ms;
    }

    /// Get expiration time in milliseconds (Unix timestamp) for hash fields
    /// Returns array of expiration times (-2 if field doesn't exist, -1 if no expiry, positive for timestamp)
    /// Caller must free the returned slice
    /// Returns error.WrongType if key is not a hash
    pub fn hpexpiretime(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        fields: []const []const u8,
    ) error{ WrongType, OutOfMemory }![]i64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var result = try allocator.alloc(i64, fields.len);

        const entry = self.data.getEntry(key) orelse {
            for (result) |*r| r.* = -2;
            return result;
        };

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            for (result) |*r| r.* = -2;
            return result;
        }

        switch (entry.value_ptr.*) {
            .hash => |*hash_val| {
                for (fields, 0..) |field, i| {
                    if (hash_val.data.get(field)) |field_val| {
                        if (field_val.expires_at) |expires_at| {
                            result[i] = expires_at;
                        } else {
                            result[i] = -1; // No expiry
                        }
                    } else {
                        result[i] = -2; // Field doesn't exist
                    }
                }
                return result;
            },
            else => {
                allocator.free(result);
                return error.WrongType;
            },
        }
    }

    /// Get expiration time in seconds (Unix timestamp) for hash fields
    /// Returns array of expiration times (-2 if field doesn't exist, -1 if no expiry, positive for timestamp)
    /// Caller must free the returned slice
    /// Returns error.WrongType if key is not a hash
    pub fn hexpiretime(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        fields: []const []const u8,
    ) error{ WrongType, OutOfMemory }![]i64 {
        const times_ms = try self.hpexpiretime(allocator, key, fields);
        // Convert milliseconds to seconds
        for (times_ms) |*t| {
            if (t.* > 0) {
                t.* = @divFloor(t.*, 1000);
            }
        }
        return times_ms;
    }

    /// HGETDEL — Atomically get hash field values and delete the fields
    /// Returns array of values (null for non-existent fields)
    /// Automatically deletes the hash key when all fields are removed
    /// Caller must free the returned slice and all string values within it
    /// Returns error.WrongType if key is not a hash
    pub fn hgetdel(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        fields: []const []const u8,
    ) error{ WrongType, OutOfMemory }![]?[]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Allocate result array
        const values = try allocator.alloc(?[]const u8, fields.len);
        errdefer allocator.free(values);

        const entry = self.data.getEntry(key) orelse {
            // Key doesn't exist - return all nulls
            for (values) |*v| v.* = null;
            return values;
        };

        // Check key expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            for (values) |*v| v.* = null;
            return values;
        }

        switch (entry.value_ptr.*) {
            .hash => |*hash_val| {
                // First pass: clone values for fields that exist
                for (fields, 0..) |field, i| {
                    if (hash_val.data.get(field)) |field_val| {
                        // Check field expiration
                        if (field_val.expires_at) |exp_time| {
                            if (getCurrentTimestamp() >= exp_time) {
                                // Field expired
                                values[i] = null;
                                continue;
                            }
                        }
                        // Clone the value before we delete it
                        values[i] = try allocator.dupe(u8, field_val.data);
                    } else {
                        values[i] = null;
                    }
                }

                // Second pass: delete the fields
                for (fields) |field| {
                    if (hash_val.data.fetchRemove(field)) |removed| {
                        self.allocator.free(removed.key);
                        var field_val = removed.value;
                        field_val.deinit(self.allocator);
                    }
                }

                // Auto-cleanup: if hash is now empty, remove the key
                if (hash_val.data.count() == 0) {
                    const owned_key = entry.key_ptr.*;
                    var value = entry.value_ptr.*;
                    _ = self.removeKeyCleanup(key);
                    self.allocator.free(owned_key);
                    value.deinit(self.allocator);
                }

                return values;
            },
            else => return error.WrongType,
        }
    }

    /// HGETEX — Atomically get hash field values and set/update field expiration
    /// Returns array of values (null for non-existent fields)
    /// Caller must free the returned slice and all string values within it
    /// Returns error.WrongType if key is not a hash
    pub fn hgetex(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        fields: []const []const u8,
        expires_at_ms: ?i64, // null means PERSIST (remove expiration)
    ) error{ WrongType, OutOfMemory }![]?[]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Allocate result array
        const values = try allocator.alloc(?[]const u8, fields.len);
        errdefer allocator.free(values);

        const entry = self.data.getEntry(key) orelse {
            // Key doesn't exist - return all nulls
            for (values) |*v| v.* = null;
            return values;
        };

        // Check key expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            for (values) |*v| v.* = null;
            return values;
        }

        switch (entry.value_ptr.*) {
            .hash => |*hash_val| {
                // Get values and update expiration
                for (fields, 0..) |field, i| {
                    if (hash_val.data.getEntry(field)) |field_entry| {
                        // Check field expiration
                        if (field_entry.value_ptr.expires_at) |exp_time| {
                            if (getCurrentTimestamp() >= exp_time) {
                                // Field expired
                                values[i] = null;
                                continue;
                            }
                        }
                        // Clone the value
                        values[i] = try allocator.dupe(u8, field_entry.value_ptr.data);
                        // Update expiration
                        field_entry.value_ptr.expires_at = expires_at_ms;
                    } else {
                        values[i] = null;
                    }
                }

                return values;
            },
            else => return error.WrongType,
        }
    }

    /// HSETEX — Atomically set hash fields with values and expiration
    /// Returns 1 if all fields were set, 0 if conditional (fnx/fxx) failed
    /// fnx: all fields must NOT exist (Field-Not-eXists)
    /// fxx: all fields must exist (Field-eXists-eXists)
    /// keep_ttl: preserve existing field TTL for updated fields, null for new fields
    /// expires_at_ms: expiration time (null for no expiration, unless keep_ttl is true)
    /// Returns error.WrongType if key exists but is not a hash
    pub fn hsetex(
        self: *Storage,
        key: []const u8,
        fields: []const []const u8,
        values: []const []const u8,
        fnx: bool, // Field-Not-eXists (all must be new)
        fxx: bool, // Field-eXists-eXists (all must exist)
        keep_ttl: bool, // Preserve existing field TTL
        expires_at_ms: ?i64, // null for no expiration
        expires_at: ?i64, // Key-level expiration (for new hashes)
    ) error{ WrongType, OutOfMemory }!u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key);

        if (entry) |kv_entry| {
            // Check key expiration
            if (kv_entry.value_ptr.isExpired(getCurrentTimestamp())) {
                const owned_key = kv_entry.key_ptr.*;
                var value = kv_entry.value_ptr.*;
                _ = self.removeKeyCleanup(key);
                self.allocator.free(owned_key);
                value.deinit(self.allocator);
                // Fall through to create new hash
            } else {
                switch (kv_entry.value_ptr.*) {
                    .hash => |*hash_val| {
                        // Validate FNX/FXX conditions
                        if (fnx) {
                            // FNX: all fields must NOT exist
                            for (fields) |field| {
                                if (hash_val.data.contains(field)) {
                                    // At least one field exists, fail
                                    return 0;
                                }
                            }
                        }

                        if (fxx) {
                            // FXX: all fields must exist
                            for (fields) |field| {
                                if (!hash_val.data.contains(field)) {
                                    // At least one field doesn't exist, fail
                                    return 0;
                                }
                            }
                        }

                        // All conditions met - set the fields
                        for (fields, 0..) |field, i| {
                            const is_new = !hash_val.data.contains(field);

                            // Determine field expiration
                            const field_expiration = if (keep_ttl and !is_new)
                                // KEEPTTL: preserve existing expiration for updated fields
                                hash_val.data.get(field).?.expires_at
                            else
                                // Use provided expiration (or null for new fields when keep_ttl is true)
                                expires_at_ms;

                            if (is_new) {
                                // New field - duplicate both
                                const owned_field = try self.allocator.dupe(u8, field);
                                errdefer self.allocator.free(owned_field);

                                const owned_value = try self.allocator.dupe(u8, values[i]);
                                errdefer self.allocator.free(owned_value);

                                try hash_val.data.put(owned_field, Value.FieldValue{
                                    .data = owned_value,
                                    .expires_at = if (keep_ttl) null else field_expiration,
                                });
                            } else {
                                // Existing field - free old value, set new one
                                const old_field_val = hash_val.data.get(field).?;
                                self.allocator.free(old_field_val.data);

                                const owned_value = try self.allocator.dupe(u8, values[i]);
                                errdefer self.allocator.free(owned_value);

                                try hash_val.data.put(field, Value.FieldValue{
                                    .data = owned_value,
                                    .expires_at = field_expiration,
                                });
                            }
                        }

                        return 1;
                    },
                    else => return error.WrongType,
                }
            }
        }

        // Create new hash (either key doesn't exist or was expired)
        if (fxx) {
            // FXX requires all fields to exist, but hash doesn't exist
            return 0;
        }

        var hash_map = std.StringHashMap(Value.FieldValue).init(self.allocator);
        errdefer {
            var it = hash_map.iterator();
            while (it.next()) |hash_entry| {
                self.allocator.free(hash_entry.key_ptr.*);
                var field_val = hash_entry.value_ptr.*;
                field_val.deinit(self.allocator);
            }
            hash_map.deinit();
        }

        // Add all field-value pairs
        for (fields, 0..) |field, i| {
            const owned_field = try self.allocator.dupe(u8, field);
            errdefer self.allocator.free(owned_field);

            const owned_value = try self.allocator.dupe(u8, values[i]);
            errdefer self.allocator.free(owned_value);

            const field_expiration = if (keep_ttl) null else expires_at_ms;

            try hash_map.put(owned_field, Value.FieldValue{
                .data = owned_value,
                .expires_at = field_expiration,
            });
        }

        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);

        try self.data.put(owned_key, Value{
            .hash = .{
                .data = hash_map,
                .expires_at = expires_at,
            },
        });

        return 1;
    }

    /// Get rank (0-based index) of member in sorted set
    /// If reverse is true, return rank from end (ZREVRANK)
    /// Returns null if key or member does not exist
    pub fn zrank(
        self: *Storage,
        key: []const u8,
        member: []const u8,
        reverse: bool,
    ) ?usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| {
                for (zset.sorted_list.items, 0..) |item, idx| {
                    if (std.mem.eql(u8, item.member, member)) {
                        if (reverse) {
                            return zset.sorted_list.items.len - 1 - idx;
                        }
                        return idx;
                    }
                }
                return null;
            },
            else => return null,
        }
    }

    /// Get score of member in sorted set for ZRANK WITHSCORE variant
    /// Returns null if key or member does not exist
    pub fn zrankScore(
        self: *Storage,
        key: []const u8,
        member: []const u8,
    ) ?f64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| return zset.members.get(member),
            else => return null,
        }
    }

    /// Increment score of member in sorted set by increment
    /// Creates member with score=increment if it does not exist
    /// Returns new score after increment
    /// Returns error.WrongType if key is not a sorted set
    pub fn zincrby(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        increment: f64,
        member: []const u8,
    ) !f64 {
        _ = allocator;
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.data.getEntry(key)) |entry| {
            switch (entry.value_ptr.*) {
                .sorted_set => |*zset| {
                    if (zset.members.get(member)) |old_score| {
                        const new_score = old_score + increment;
                        // Remove from sorted list
                        for (zset.sorted_list.items, 0..) |item, idx| {
                            if (std.mem.eql(u8, item.member, member)) {
                                _ = zset.sorted_list.orderedRemove(idx);
                                break;
                            }
                        }
                        // Update score in map
                        try zset.members.put(member, new_score);
                        // Re-insert at correct position
                        const stored_key = zset.members.getKey(member).?;
                        try insertSortedMember(&zset.sorted_list, new_score, stored_key, self.allocator);
                        return new_score;
                    } else {
                        // New member
                        const owned_member = try self.allocator.dupe(u8, member);
                        errdefer self.allocator.free(owned_member);
                        try zset.members.put(owned_member, increment);
                        const stored_key = zset.members.getKey(member).?;
                        try insertSortedMember(&zset.sorted_list, increment, stored_key, self.allocator);
                        return increment;
                    }
                },
                else => return error.WrongType,
            }
        } else {
            // Create new sorted set
            var member_map = std.StringHashMap(f64).init(self.allocator);
            errdefer {
                var it = member_map.keyIterator();
                while (it.next()) |k| self.allocator.free(k.*);
                member_map.deinit();
            }

            var sorted: std.ArrayList(Value.ScoredMember) = .{
                .items = &.{},
                .capacity = 0,
            };
            errdefer sorted.deinit(self.allocator);

            const owned_member = try self.allocator.dupe(u8, member);
            errdefer self.allocator.free(owned_member);
            try member_map.put(owned_member, increment);
            const stored_key = member_map.getKey(member).?;
            try insertSortedMember(&sorted, increment, stored_key, self.allocator);

            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);
            try self.data.put(owned_key, Value{
                .sorted_set = .{
                    .members = member_map,
                    .sorted_list = sorted,
                    .expires_at = null,
                },
            });
            return increment;
        }
    }

    /// Count members in sorted set with scores in [min, max]
    /// Supports exclusive bounds via min_excl/max_excl flags
    /// Returns 0 if key does not exist
    pub fn zcount(
        self: *Storage,
        key: []const u8,
        min: f64,
        max: f64,
        min_excl: bool,
        max_excl: bool,
    ) usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return 0;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return 0;
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| {
                var count: usize = 0;
                for (zset.sorted_list.items) |item| {
                    const score = item.score;
                    const above_min = if (min_excl) score > min else score >= min;
                    const below_max = if (max_excl) score < max else score <= max;
                    if (above_min and below_max) count += 1;
                }
                return count;
            },
            else => return 0,
        }
    }

    /// Compute union of sets at keys
    /// Returns owned slice of unique member strings (caller frees outer slice; NOT member strings)
    /// Non-existent keys are treated as empty sets
    /// Returns error.WrongType if any key is not a set
    pub fn sunion(
        self: *Storage,
        allocator: std.mem.Allocator,
        keys: []const []const u8,
    ) ![][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Accumulate unique members using a temporary string set
        var accum = std.StringHashMap(void).init(allocator);
        defer accum.deinit();

        for (keys) |key| {
            const entry = self.data.getEntry(key) orelse continue;
            if (entry.value_ptr.isExpired(getCurrentTimestamp())) continue;
            switch (entry.value_ptr.*) {
                .set => |*set_val| {
                    switch (set_val.encoding) {
                        .intset => {
                            var i: usize = 0;
                            while (i < set_val.data.intset.length) : (i += 1) {
                                const int_val = try set_val.data.intset.getAt(i);
                                const str = try std.fmt.allocPrint(allocator, "{d}", .{int_val});
                                defer allocator.free(str);
                                if (!accum.contains(str)) {
                                    const owned_str = try allocator.dupe(u8, str);
                                    try accum.put(owned_str, {});
                                }
                            }
                        },
                        .hashmap => {
                            var it = set_val.data.hashmap.keyIterator();
                            while (it.next()) |member_ptr| {
                                try accum.put(member_ptr.*, {});
                            }
                        },
                    }
                },
                else => return error.WrongType,
            }
        }

        var result = try allocator.alloc([]const u8, accum.count());
        var it = accum.keyIterator();
        var i: usize = 0;
        while (it.next()) |k| : (i += 1) {
            result[i] = k.*;
        }
        return result;
    }

    /// Compute intersection of sets at keys
    /// Returns owned slice of member strings present in all sets
    /// Non-existent keys are treated as empty sets (result is empty)
    /// Returns error.WrongType if any key is not a set
    pub fn sinter(
        self: *Storage,
        allocator: std.mem.Allocator,
        keys: []const []const u8,
    ) (error{ WrongType, OutOfMemory, IndexOutOfBounds } || std.fmt.ParseIntError)![][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (keys.len == 0) return try allocator.alloc([]const u8, 0);

        // Find the first set to seed candidates
        var candidates: ?*Value.SetValue = null;
        for (keys) |key| {
            const entry = self.data.getEntry(key) orelse {
                // Key missing = empty set, intersection is empty
                return try allocator.alloc([]const u8, 0);
            };
            if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
                return try allocator.alloc([]const u8, 0);
            }
            switch (entry.value_ptr.*) {
                .set => |*sv| {
                    candidates = sv;
                    break;
                },
                else => return error.WrongType,
            }
        }

        const first_set = candidates orelse return try allocator.alloc([]const u8, 0);

        // For each candidate in first set, check membership in all other sets
        var result_list = std.ArrayList([]const u8){
            .items = &.{},
            .capacity = 0,
        };
        defer result_list.deinit(allocator);

        switch (first_set.encoding) {
            .intset => {
                var idx: usize = 0;
                outer: while (idx < first_set.data.intset.length) : (idx += 1) {
                    const int_val = try first_set.data.intset.getAt(idx);
                    const member = try std.fmt.allocPrint(allocator, "{d}", .{int_val});
                    defer allocator.free(member);

                    for (keys[1..]) |key| {
                        const entry = self.data.getEntry(key) orelse continue :outer;
                        if (entry.value_ptr.isExpired(getCurrentTimestamp())) continue :outer;
                        switch (entry.value_ptr.*) {
                            .set => |*sv| {
                                if (!(try sv.contains(member))) continue :outer;
                            },
                            else => return error.WrongType,
                        }
                    }
                    try result_list.append(allocator, try allocator.dupe(u8, member));
                }
            },
            .hashmap => {
                var it = first_set.data.hashmap.keyIterator();
                outer: while (it.next()) |member_ptr| {
                    const member = member_ptr.*;
                    for (keys[1..]) |key| {
                        const entry = self.data.getEntry(key) orelse continue :outer;
                        if (entry.value_ptr.isExpired(getCurrentTimestamp())) continue :outer;
                        switch (entry.value_ptr.*) {
                            .set => |*sv| {
                                if (!(try sv.contains(member))) continue :outer;
                            },
                            else => return error.WrongType,
                        }
                    }
                    try result_list.append(allocator, member);
                }
            },
        }

        const result = try allocator.dupe([]const u8, result_list.items);
        return result;
    }

    /// Compute difference: members in keys[0] not in any of keys[1..]
    /// Returns owned slice of member strings
    /// Non-existent keys are treated as empty sets
    /// Returns error.WrongType if any key is not a set
    pub fn sdiff(
        self: *Storage,
        allocator: std.mem.Allocator,
        keys: []const []const u8,
    ) (error{ WrongType, OutOfMemory, IndexOutOfBounds } || std.fmt.ParseIntError)![][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (keys.len == 0) return try allocator.alloc([]const u8, 0);

        // Get the first set
        const first_entry = self.data.getEntry(keys[0]) orelse {
            return try allocator.alloc([]const u8, 0);
        };
        if (first_entry.value_ptr.isExpired(getCurrentTimestamp())) {
            return try allocator.alloc([]const u8, 0);
        }

        const first_set = switch (first_entry.value_ptr.*) {
            .set => |*sv| sv,
            else => return error.WrongType,
        };

        // Validate remaining keys are sets
        for (keys[1..]) |key| {
            const entry = self.data.getEntry(key) orelse continue;
            if (entry.value_ptr.isExpired(getCurrentTimestamp())) continue;
            switch (entry.value_ptr.*) {
                .set => {},
                else => return error.WrongType,
            }
        }

        var result_list = std.ArrayList([]const u8){
            .items = &.{},
            .capacity = 0,
        };
        defer result_list.deinit(allocator);

        switch (first_set.encoding) {
            .intset => {
                var idx: usize = 0;
                outer: while (idx < first_set.data.intset.length) : (idx += 1) {
                    const int_val = try first_set.data.intset.getAt(idx);
                    const member = try std.fmt.allocPrint(allocator, "{d}", .{int_val});
                    defer allocator.free(member);

                    for (keys[1..]) |key| {
                        const entry = self.data.getEntry(key) orelse continue;
                        if (entry.value_ptr.isExpired(getCurrentTimestamp())) continue;
                        switch (entry.value_ptr.*) {
                            .set => |*sv| {
                                if (try sv.contains(member)) continue :outer;
                            },
                            else => {},
                        }
                    }
                    try result_list.append(allocator, try allocator.dupe(u8, member));
                }
            },
            .hashmap => {
                var it = first_set.data.hashmap.keyIterator();
                outer: while (it.next()) |member_ptr| {
                    const member = member_ptr.*;
                    for (keys[1..]) |key| {
                        const entry = self.data.getEntry(key) orelse continue;
                        if (entry.value_ptr.isExpired(getCurrentTimestamp())) continue;
                        switch (entry.value_ptr.*) {
                            .set => |*sv| {
                                if (try sv.contains(member)) continue :outer;
                            },
                            else => {},
                        }
                    }
                    try result_list.append(allocator, member);
                }
            },
        }

        const result = try allocator.dupe([]const u8, result_list.items);
        return result;
    }

    /// Store union of sets at keys into destination
    /// Overwrites destination. Returns count of members in result.
    pub fn sunionstore(
        self: *Storage,
        allocator: std.mem.Allocator,
        dest: []const u8,
        keys: []const []const u8,
    ) !usize {
        const members = try self.sunion(allocator, keys);
        defer allocator.free(members);
        return self.storeSetMembers(allocator, dest, members);
    }

    /// Store intersection of sets at keys into destination
    /// Overwrites destination. Returns count of members in result.
    pub fn sinterstore(
        self: *Storage,
        allocator: std.mem.Allocator,
        dest: []const u8,
        keys: []const []const u8,
    ) !usize {
        const members = try self.sinter(allocator, keys);
        defer allocator.free(members);
        return self.storeSetMembers(allocator, dest, members);
    }

    /// Store difference of sets (keys[0] minus keys[1..]) into destination
    /// Overwrites destination. Returns count of members in result.
    pub fn sdiffstore(
        self: *Storage,
        allocator: std.mem.Allocator,
        dest: []const u8,
        keys: []const []const u8,
    ) !usize {
        const members = try self.sdiff(allocator, keys);
        defer allocator.free(members);
        return self.storeSetMembers(allocator, dest, members);
    }

    /// Internal helper: create/overwrite a set at dest with the given member strings
    /// Member strings are NOT owned by this function (they point into other sets)
    fn storeSetMembers(
        self: *Storage,
        allocator: std.mem.Allocator,
        dest: []const u8,
        members: []const []const u8,
    ) !usize {
        _ = allocator;
        self.mutex.lock();
        defer self.mutex.unlock();

        // Remove existing key at dest (if any)
        if (self.data.fetchRemove(dest)) |kv| {
            self.allocator.free(kv.key);
            var old_value = kv.value;
            old_value.deinit(self.allocator);
        }

        var set_map = std.StringHashMap(void).init(self.allocator);
        errdefer {
            var it = set_map.keyIterator();
            while (it.next()) |k| self.allocator.free(k.*);
            set_map.deinit();
        }

        for (members) |member| {
            if (!set_map.contains(member)) {
                const owned = try self.allocator.dupe(u8, member);
                errdefer self.allocator.free(owned);
                try set_map.put(owned, {});
            }
        }

        const count = set_map.count();
        const owned_key = try self.allocator.dupe(u8, dest);
        errdefer self.allocator.free(owned_key);
        try self.data.put(owned_key, Value{
            .set = .{ .encoding = .hashmap, .data = .{ .hashmap = set_map }, .expires_at = null },
        });
        return count;
    }

    /// Format a float as a string without trailing zeros
    fn formatFloat(buf: []u8, value: f64) []const u8 {
        // Use standard formatting then trim trailing zeros after decimal
        const s = std.fmt.bufPrint(buf, "{d}", .{value}) catch return "0";
        // If no decimal point, return as-is
        if (std.mem.indexOf(u8, s, ".") == null) return s;
        // Trim trailing zeros
        var end = s.len;
        while (end > 0 and s[end - 1] == '0') end -= 1;
        if (end > 0 and s[end - 1] == '.') end -= 1;
        return s[0..end];
    }

    /// Get current Unix timestamp in milliseconds
    /// Return the number of keys in storage (excluding expired)
    pub fn dbSize(self: *Storage) usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = getCurrentTimestamp();
        var count: usize = 0;
        var it = self.data.iterator();
        while (it.next()) |entry| {
            if (!entry.value_ptr.isExpired(now)) count += 1;
        }
        return count;
    }

    // ── Keyspace / TTL operations ─────────────────────────────────────────────

    /// Set expiry on an existing key.
    /// Returns false if key doesn't exist or is already expired.
    /// Pass null expires_at to remove expiry (PERSIST behavior).
    /// options bitmask: 1=NX (only if no expiry), 2=XX (only if has expiry),
    ///                  4=GT (only if new > current), 8=LT (only if new < current)
    pub fn setExpiry(self: *Storage, key: []const u8, expires_at: ?i64, options: u8) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return false;

        // Lazily delete expired key
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return false;
        }

        const current_exp = entry.value_ptr.getExpiration();

        // NX: only set if no current expiry
        if (options & 1 != 0 and current_exp != null) return false;
        // XX: only set if has current expiry
        if (options & 2 != 0 and current_exp == null) return false;
        // GT: only set if new expiry > current
        if (options & 4 != 0) {
            if (expires_at == null) return false;
            if (current_exp) |cur| {
                if (expires_at.? <= cur) return false;
            }
        }
        // LT: only set if new expiry < current
        if (options & 8 != 0) {
            if (expires_at == null) return false;
            if (current_exp) |cur| {
                if (expires_at.? >= cur) return false;
            }
        }

        // Apply expiry to the value
        switch (entry.value_ptr.*) {
            .string => |*v| v.expires_at = expires_at,
            .list => |*v| v.expires_at = expires_at,
            .set => |*v| v.expires_at = expires_at,
            .hash => |*v| v.expires_at = expires_at,
            .sorted_set => |*v| v.expires_at = expires_at,
            .stream => |*v| v.expires_at = expires_at,
            .hyperloglog => |*v| v.expires_at = expires_at,
            .json => |*v| v.expires_at = expires_at,
            .timeseries => |*v| v.expires_at = expires_at,
            .bloom => |*v| v.expires_at = expires_at,
            .cuckoo => |*v| v.expires_at = expires_at,
            .count_min_sketch => {}, // CMS doesn't support expiration - no-op
            .top_k => |*v| v.expires_at = expires_at,
            .t_digest => {}, // T-Digest doesn't support expiration yet - no-op
            .vector_set => {}, // Vector sets don't support expiration yet - no-op
        }
        return true;
    }

    /// Get TTL of key in milliseconds.
    /// Returns -2 if key does not exist (or is expired).
    /// Returns -1 if key has no expiry.
    /// Otherwise returns remaining milliseconds.
    pub fn getTtlMs(self: *Storage, key: []const u8) i64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return -2;
        const now = getCurrentTimestamp();

        if (entry.value_ptr.isExpired(now)) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return -2;
        }

        const exp = entry.value_ptr.getExpiration() orelse return -1;
        return exp - now;
    }

    // ── String counter operations ─────────────────────────────────────────────

    /// Increment a string value by delta. Creates key at "0" if missing.
    /// Returns the new value, or error.WrongType if not a string,
    /// or error.NotInteger if the value is not a parseable integer,
    /// or error.Overflow if the operation would overflow.
    pub fn incrby(self: *Storage, key: []const u8, delta: i64) !i64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var current: i64 = 0;
        var current_exp: ?i64 = null;

        if (self.data.getEntry(key)) |entry| {
            if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
                const owned_key = entry.key_ptr.*;
                var value = entry.value_ptr.*;
                _ = self.removeKeyCleanup(key);
                self.allocator.free(owned_key);
                value.deinit(self.allocator);
            } else {
                switch (entry.value_ptr.*) {
                    .string => |*sv| {
                        current = std.fmt.parseInt(i64, sv.data, 10) catch return error.NotInteger;
                        current_exp = sv.expires_at;
                    },
                    else => return error.WrongType,
                }
            }
        }

        const new_val = std.math.add(i64, current, delta) catch return error.Overflow;

        // Format new value as string
        var buf: [32]u8 = undefined;
        const new_str = std.fmt.bufPrint(&buf, "{d}", .{new_val}) catch unreachable;
        const owned_str = try self.allocator.dupe(u8, new_str);
        errdefer self.allocator.free(owned_str);

        if (self.data.getEntry(key)) |entry| {
            var old_value = entry.value_ptr.*;
            old_value.deinit(self.allocator);
            entry.value_ptr.* = Value{ .string = .{ .data = owned_str, .expires_at = current_exp } };
        } else {
            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);
            try self.data.put(owned_key, Value{ .string = .{ .data = owned_str, .expires_at = current_exp } });
        }

        return new_val;
    }

    /// Increment a string value by a float delta. Creates key at "0" if missing.
    /// Returns the new value, or error.WrongType / error.NotFloat.
    pub fn incrbyfloat(self: *Storage, key: []const u8, delta: f64) !f64 {
        // Reject NaN and infinity as delta values immediately
        if (std.math.isNan(delta) or std.math.isInf(delta)) {
            return error.NanOrInfinity;
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        var current: f64 = 0.0;
        var current_exp: ?i64 = null;

        if (self.data.getEntry(key)) |entry| {
            if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
                const owned_key = entry.key_ptr.*;
                var value = entry.value_ptr.*;
                _ = self.removeKeyCleanup(key);
                self.allocator.free(owned_key);
                value.deinit(self.allocator);
            } else {
                switch (entry.value_ptr.*) {
                    .string => |*sv| {
                        const parsed = std.fmt.parseFloat(f64, sv.data) catch return error.NotFloat;
                        if (std.math.isNan(parsed) or std.math.isInf(parsed)) {
                            return error.NanOrInfinity;
                        }
                        current = parsed;
                        current_exp = sv.expires_at;
                    },
                    else => return error.WrongType,
                }
            }
        }

        const new_val = current + delta;

        // Reject NaN and infinity as result
        if (std.math.isNan(new_val) or std.math.isInf(new_val)) {
            return error.NanOrInfinity;
        }

        // Format new value matching Redis: use decimal notation, strip trailing zeros
        var buf: [64]u8 = undefined;
        const new_str = formatFloat(&buf, new_val);
        const owned_str = try self.allocator.dupe(u8, new_str);
        errdefer self.allocator.free(owned_str);

        if (self.data.getEntry(key)) |entry| {
            var old_value = entry.value_ptr.*;
            old_value.deinit(self.allocator);
            entry.value_ptr.* = Value{ .string = .{ .data = owned_str, .expires_at = current_exp } };
        } else {
            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);
            try self.data.put(owned_key, Value{ .string = .{ .data = owned_str, .expires_at = current_exp } });
        }

        return new_val;
    }

    // ── String mutation operations ────────────────────────────────────────────

    /// Append suffix to a string value. Creates key as "" if missing.
    /// Returns the new length, or error.WrongType if not a string.
    pub fn appendString(self: *Storage, key: []const u8, suffix: []const u8) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.data.getEntry(key)) |entry| {
            if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
                const owned_key = entry.key_ptr.*;
                var value = entry.value_ptr.*;
                _ = self.removeKeyCleanup(key);
                self.allocator.free(owned_key);
                value.deinit(self.allocator);
            } else {
                switch (entry.value_ptr.*) {
                    .string => |*sv| {
                        // Build new string = existing + suffix
                        const new_len = sv.data.len + suffix.len;
                        const new_buf = try self.allocator.alloc(u8, new_len);
                        errdefer self.allocator.free(new_buf);
                        @memcpy(new_buf[0..sv.data.len], sv.data);
                        @memcpy(new_buf[sv.data.len..], suffix);
                        self.allocator.free(sv.data);
                        sv.data = new_buf;
                        return new_len;
                    },
                    else => return error.WrongType,
                }
            }
        }

        // Key doesn't exist (or was expired): create with suffix as value
        const owned_val = try self.allocator.dupe(u8, suffix);
        errdefer self.allocator.free(owned_val);
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        try self.data.put(owned_key, Value{ .string = .{ .data = owned_val, .expires_at = null } });
        return suffix.len;
    }

    /// Get and delete a key.
    /// Returns null if key doesn't exist. Returns error.WrongType if not a string.
    /// Caller owns the returned slice and must free it.
    pub fn getdel(self: *Storage, key: []const u8) !?[]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .string => |sv| {
                // Copy the value before deleting the key
                const result = try self.allocator.dupe(u8, sv.data);
                const owned_key = entry.key_ptr.*;
                var value = entry.value_ptr.*;
                _ = self.removeKeyCleanup(key);
                self.allocator.free(owned_key);
                value.deinit(self.allocator);
                return result;
            },
            else => return error.WrongType,
        }
    }

    /// Get string value and optionally update its expiry.
    /// Returns null if key doesn't exist. Returns error.WrongType if not a string.
    /// If persist=true, removes any expiry. If expires_at is non-null, sets new expiry.
    /// Caller owns the returned slice and must free it.
    pub fn getex(self: *Storage, key: []const u8, expires_at: ?i64, persist: bool) !?[]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .string => |*sv| {
                const result = try self.allocator.dupe(u8, sv.data);
                errdefer self.allocator.free(result);
                if (persist) {
                    sv.expires_at = null;
                } else if (expires_at) |exp| {
                    sv.expires_at = exp;
                }
                return result;
            },
            else => return error.WrongType,
        }
    }

    // ── Key rename operations ─────────────────────────────────────────────────

    /// Rename key to newkey. Returns error.NoSuchKey if source doesn't exist.
    /// If newkey already exists, it is overwritten.
    pub fn rename(self: *Storage, key: []const u8, newkey: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Check source exists
        const src_entry = self.data.getEntry(key) orelse return error.NoSuchKey;

        if (src_entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = src_entry.key_ptr.*;
            var value = src_entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return error.NoSuchKey;
        }

        // Clone the value to move it under the new key
        const src_value = src_entry.value_ptr.*;

        // Remove old destination if it exists
        if (self.data.fetchRemove(newkey)) |old| {
            self.allocator.free(old.key);
            var old_v = old.value;
            old_v.deinit(self.allocator);
        }

        // Insert under newkey
        const owned_newkey = try self.allocator.dupe(u8, newkey);
        errdefer self.allocator.free(owned_newkey);

        // Remove from old location (we already have the value copied above)
        const src_owned_key = src_entry.key_ptr.*;
        _ = self.removeKeyCleanup(key);
        self.allocator.free(src_owned_key);

        try self.data.put(owned_newkey, src_value);
    }

    /// Rename key to newkey only if newkey does not already exist.
    /// Returns true if renamed, false if newkey already exists.
    /// Returns error.NoSuchKey if source doesn't exist.
    pub fn renamenx(self: *Storage, key: []const u8, newkey: []const u8) !bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Check source exists
        const src_entry = self.data.getEntry(key) orelse return error.NoSuchKey;

        if (src_entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = src_entry.key_ptr.*;
            var value = src_entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return error.NoSuchKey;
        }

        // If destination exists and is not expired, return false
        if (self.data.getEntry(newkey)) |dst| {
            if (!dst.value_ptr.isExpired(getCurrentTimestamp())) return false;
            // Destination is expired — remove it
            const owned_key = dst.key_ptr.*;
            var value = dst.value_ptr.*;
            _ = self.data.remove(newkey);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
        }

        // Move value
        const src_value = src_entry.value_ptr.*;
        const src_owned_key = src_entry.key_ptr.*;
        _ = self.removeKeyCleanup(key);
        self.allocator.free(src_owned_key);

        const owned_newkey = try self.allocator.dupe(u8, newkey);
        errdefer self.allocator.free(owned_newkey);
        try self.data.put(owned_newkey, src_value);
        return true;
    }

    // ── Advanced key commands ────────────────────────────────────────────────

    /// Serialize a single value to RDB format (for DUMP command).
    /// Returns owned byte slice that caller must free.
    /// Returns null if key doesn't exist or is expired.
    pub fn dumpValue(self: *Storage, allocator: std.mem.Allocator, key: []const u8) !?[]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.get(key) orelse return null;
        const now = getCurrentTimestamp();
        if (entry.isExpired(now)) return null;

        // Use the persistence module's RDB serialization format
        var buf = std.ArrayList(u8){};
        errdefer buf.deinit(allocator);

        const w = buf.writer(allocator);

        // Write type byte
        const type_byte: u8 = switch (entry) {
            .string => 0x00, // RDB_TYPE_STRING
            .list => 0x01, // RDB_TYPE_LIST
            .set => 0x02, // RDB_TYPE_SET
            .hash => 0x04, // RDB_TYPE_HASH
            .sorted_set => 0x03, // RDB_TYPE_SORTED_SET
            .stream => 0xFF, // Stream type
            .hyperloglog => 0xFE, // HyperLogLog type
            .json => 0x0F, // JSON type
            .timeseries => 0xFD, // Time Series type
            .bloom => 0xFC, // Bloom Filter type
            .cuckoo => 0xFB, // Cuckoo Filter type
            .count_min_sketch => 0xFA, // Count-Min Sketch type
            .top_k => 0xF9, // Top-K type
            .t_digest => 0xF8, // T-Digest type
            .vector_set => 0xF7, // Vector Set type
        };
        try w.writeByte(type_byte);

        // Write expiration if present
        const expires_at = entry.getExpiration();
        if (expires_at) |exp| {
            try w.writeByte(1);
            try w.writeInt(i64, exp, .little);
        } else {
            try w.writeByte(0);
        }

        // Write value data
        switch (entry) {
            .string => |s| {
                try writeBlob(w, s.data);
            },
            .list => |l| {
                try w.writeInt(u32, @intCast(l.data.items.len), .little);
                for (l.data.items) |elem| try writeBlob(w, elem);
            },
            .set => |s| {
                const count = switch (s.encoding) {
                    .intset => s.data.intset.length,
                    .hashmap => s.data.hashmap.count(),
                };
                try w.writeInt(u32, @intCast(count), .little);

                switch (s.encoding) {
                    .intset => {
                        var i: usize = 0;
                        while (i < s.data.intset.length) : (i += 1) {
                            const int_val = try s.data.intset.getAt(i);
                            const str = try std.fmt.allocPrint(allocator, "{d}", .{int_val});
                            defer allocator.free(str);
                            try writeBlob(w, str);
                        }
                    },
                    .hashmap => {
                        var kit = s.data.hashmap.keyIterator();
                        while (kit.next()) |k| try writeBlob(w, k.*);
                    },
                }
            },
            .hash => |h| {
                try w.writeInt(u32, @intCast(h.data.count()), .little);
                var hit = h.data.iterator();
                while (hit.next()) |e| {
                    try writeBlob(w, e.key_ptr.*);
                    try writeBlob(w, e.value_ptr.*.data);
                }
            },
            .sorted_set => |z| {
                try w.writeInt(u32, @intCast(z.sorted_list.items.len), .little);
                for (z.sorted_list.items) |scored| {
                    const score_bits = @as(u64, @bitCast(scored.score));
                    try w.writeInt(u64, score_bits, .little);
                    try writeBlob(w, scored.member);
                }
            },
            .stream => |st| {
                try w.writeInt(u32, @intCast(st.entries.items.len), .little);
                for (st.entries.items) |e| {
                    try w.writeInt(i64, e.id.ms, .little);
                    try w.writeInt(u64, e.id.seq, .little);
                    // Fields are stored as flat array [field1, value1, field2, value2, ...]
                    try w.writeInt(u32, @intCast(e.fields.items.len), .little);
                    for (e.fields.items) |field_or_value| {
                        try writeBlob(w, field_or_value);
                    }
                }
            },
            .hyperloglog => |hll| {
                // Serialize HyperLogLog as raw bytes (16384 bytes for 16384 6-bit registers)
                try writeBlob(w, &hll.registers);
            },
            .json => |j| {
                // Serialize JSON to string
                const json_str = try j.root.stringify(j.allocator);
                defer j.allocator.free(json_str);
                try writeBlob(w, json_str);
            },
            .timeseries => {
                // Time series not yet implemented in dump
                try w.writeInt(u32, 0, .little);
            },
            .bloom => {
                // Bloom filter not yet implemented in dump
                try w.writeInt(u32, 0, .little);
            },
            .cuckoo => {
                // Cuckoo filter not yet implemented in dump
                try w.writeInt(u32, 0, .little);
            },
            .count_min_sketch => {
                // Count-Min Sketch not yet implemented in dump
                try w.writeInt(u32, 0, .little);
            },
            .top_k => {
                // Top-K not yet implemented in dump
                try w.writeInt(u32, 0, .little);
            },
            .t_digest => {
                // T-Digest not yet implemented in dump
                try w.writeInt(u32, 0, .little);
            },
            .vector_set => {
                // Vector set not yet implemented in dump
                try w.writeInt(u32, 0, .little);
            },
        }

        // Add CRC32 checksum
        const crc = std.hash.Crc32.hash(buf.items);
        var crc_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &crc_bytes, crc, .little);
        try buf.appendSlice(allocator, &crc_bytes);

        return try buf.toOwnedSlice(allocator);
    }

    /// Deserialize and restore a value from RDB format (for RESTORE command).
    /// ttl_ms: 0 = no expiration, >0 = expiration in milliseconds from now
    /// replace: if true, overwrite existing key; if false, fail if key exists
    /// absttl: when true, ttl_ms is an absolute Unix timestamp in milliseconds (RESTORE ABSTTL option).
    /// When false (default), ttl_ms is a relative TTL in milliseconds.
    pub fn restoreValue(self: *Storage, key: []const u8, serialized: []const u8, ttl_ms: i64, replace: bool, absttl: bool) !void {
        if (serialized.len < 6) return error.InvalidDumpPayload; // type + exp_flag + crc (min)

        // Verify checksum
        const payload = serialized[0 .. serialized.len - 4];
        const stored_crc = std.mem.readInt(u32, serialized[serialized.len - 4 ..][0..4], .little);
        const computed_crc = std.hash.Crc32.hash(payload);
        if (stored_crc != computed_crc) return error.DumpChecksumMismatch;

        var pos: usize = 0;

        // Read type
        const type_byte = payload[pos];
        pos += 1;

        // Read expiration flag
        const has_expiration = payload[pos];
        pos += 1;

        var expires_at: ?i64 = null;
        if (has_expiration == 1) {
            if (payload.len < pos + 8) return error.InvalidDumpPayload;
            expires_at = std.mem.readInt(i64, payload[pos..][0..8], .little);
            pos += 8;
        }

        // Override with ttl_ms if provided
        if (ttl_ms > 0) {
            if (absttl) {
                // ABSTTL: ttl_ms is absolute Unix timestamp in milliseconds
                expires_at = ttl_ms;
            } else {
                // Default: ttl_ms is relative TTL in milliseconds
                expires_at = getCurrentTimestamp() + ttl_ms;
            }
        } else if (ttl_ms == 0) {
            expires_at = null;
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        // Check if key exists
        if (!replace) {
            if (self.data.get(key)) |existing| {
                const now = getCurrentTimestamp();
                if (!existing.isExpired(now)) return error.KeyAlreadyExists;
            }
        }

        // Parse and create value based on type
        var value: Value = undefined;

        switch (type_byte) {
            0x00 => { // String
                const data = try readBlob(payload, &pos, self.allocator);
                value = Value{ .string = .{ .data = data, .expires_at = expires_at } };
            },
            0x01 => { // List
                if (payload.len < pos + 4) return error.InvalidDumpPayload;
                const count = std.mem.readInt(u32, payload[pos..][0..4], .little);
                pos += 4;

                var list = std.ArrayList([]const u8){};
                errdefer {
                    for (list.items) |elem| self.allocator.free(elem);
                    list.deinit(self.allocator);
                }

                var i: u32 = 0;
                while (i < count) : (i += 1) {
                    const elem = try readBlob(payload, &pos, self.allocator);
                    try list.append(self.allocator, elem);
                }

                value = Value{ .list = .{ .data = list, .expires_at = expires_at } };
            },
            0x02 => { // Set
                if (payload.len < pos + 4) return error.InvalidDumpPayload;
                const count = std.mem.readInt(u32, payload[pos..][0..4], .little);
                pos += 4;

                var set_data = std.StringHashMap(void).init(self.allocator);
                errdefer {
                    var it = set_data.keyIterator();
                    while (it.next()) |k| self.allocator.free(k.*);
                    set_data.deinit();
                }

                var i: u32 = 0;
                while (i < count) : (i += 1) {
                    const member = try readBlob(payload, &pos, self.allocator);
                    try set_data.put(member, {});
                }

                value = Value{ .set = .{ .encoding = .hashmap, .data = .{ .hashmap = set_data }, .expires_at = expires_at } };
            },
            0x04 => { // Hash
                if (payload.len < pos + 4) return error.InvalidDumpPayload;
                const count = std.mem.readInt(u32, payload[pos..][0..4], .little);
                pos += 4;

                var hash = std.StringHashMap(Value.FieldValue).init(self.allocator);
                errdefer {
                    var it = hash.iterator();
                    while (it.next()) |e| {
                        self.allocator.free(e.key_ptr.*);
                        var field_val = e.value_ptr.*;
                        field_val.deinit(self.allocator);
                    }
                    hash.deinit();
                }

                var i: u32 = 0;
                while (i < count) : (i += 1) {
                    const field = try readBlob(payload, &pos, self.allocator);
                    const val = try readBlob(payload, &pos, self.allocator);
                    try hash.put(field, Value.FieldValue{
                        .data = val,
                        .expires_at = null, // No per-field expiration in RDB format yet
                    });
                }

                value = Value{ .hash = .{ .data = hash, .expires_at = expires_at } };
            },
            0x03 => { // Sorted Set
                if (payload.len < pos + 4) return error.InvalidDumpPayload;
                const count = std.mem.readInt(u32, payload[pos..][0..4], .little);
                pos += 4;

                var members = std.StringHashMap(f64).init(self.allocator);
                var sorted_list = std.ArrayList(Value.ScoredMember){};

                errdefer {
                    var it = members.keyIterator();
                    while (it.next()) |k| self.allocator.free(k.*);
                    members.deinit();
                    sorted_list.deinit(self.allocator);
                }

                var i: u32 = 0;
                while (i < count) : (i += 1) {
                    if (payload.len < pos + 8) return error.InvalidDumpPayload;
                    const score_bits = std.mem.readInt(u64, payload[pos..][0..8], .little);
                    const score = @as(f64, @bitCast(score_bits));
                    pos += 8;

                    const member = try readBlob(payload, &pos, self.allocator);
                    try members.put(member, score);
                    try sorted_list.append(self.allocator, .{ .score = score, .member = member });
                }

                value = Value{ .sorted_set = .{ .members = members, .sorted_list = sorted_list, .expires_at = expires_at } };
            },
            0xFE => { // Stream
                if (payload.len < pos + 4) return error.InvalidDumpPayload;
                const count = std.mem.readInt(u32, payload[pos..][0..4], .little);
                pos += 4;

                var entries = std.ArrayList(Value.StreamEntry){};
                errdefer {
                    for (entries.items) |*e| e.deinit(self.allocator);
                    entries.deinit(self.allocator);
                }

                var i: u32 = 0;
                while (i < count) : (i += 1) {
                    if (payload.len < pos + 16) return error.InvalidDumpPayload;
                    const ms = std.mem.readInt(i64, payload[pos..][0..8], .little);
                    pos += 8;
                    const seq = std.mem.readInt(u64, payload[pos..][0..8], .little);
                    pos += 8;

                    if (payload.len < pos + 4) return error.InvalidDumpPayload;
                    const items_count = std.mem.readInt(u32, payload[pos..][0..4], .little);
                    pos += 4;

                    var fields = std.ArrayList([]const u8){};
                    errdefer {
                        for (fields.items) |item| self.allocator.free(item);
                        fields.deinit(self.allocator);
                    }

                    var j: u32 = 0;
                    while (j < items_count) : (j += 1) {
                        const item = try readBlob(payload, &pos, self.allocator);
                        try fields.append(self.allocator, item);
                    }

                    try entries.append(self.allocator, .{
                        .id = .{ .ms = ms, .seq = seq },
                        .fields = fields,
                    });
                }

                var last_id: ?Value.StreamId = null;
                if (entries.items.len > 0) {
                    last_id = entries.items[entries.items.len - 1].id;
                }

                value = Value{ .stream = .{
                    .entries = entries,
                    .last_id = last_id,
                    .expires_at = expires_at,
                    .consumer_groups = std.StringHashMap(Value.ConsumerGroup).init(self.allocator),
                    .entries_added = @intCast(entries.items.len),
                    .max_deleted_entry_id = .{ .ms = 0, .seq = 0 },
                } };
            },
            0xFD => { // HyperLogLog
                const registers_data = try readBlob(payload, &pos, self.allocator);
                defer self.allocator.free(registers_data);

                if (registers_data.len != 16384) return error.InvalidDumpPayload;

                var hll = Value.HyperLogLogValue.init();
                hll.expires_at = expires_at;
                @memcpy(&hll.registers, registers_data);

                value = Value{ .hyperloglog = hll };
            },
            else => return error.UnknownDumpType,
        }

        // Remove old value if exists
        if (self.data.fetchRemove(key)) |kv| {
            self.allocator.free(kv.key);
            var old_value = kv.value;
            old_value.deinit(self.allocator);
        }

        // Insert new value
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        try self.data.put(owned_key, value);
    }

    /// Copy a key to a new key (for COPY command).
    /// replace: if true, overwrite destination; if false, fail if destination exists.
    /// Returns true if copy succeeded, false if destination exists and replace=false.
    pub fn copyKey(self: *Storage, source: []const u8, destination: []const u8, replace: bool) !bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Check source exists and is not expired
        const src_entry = self.data.get(source) orelse return error.NoSuchKey;
        const now = getCurrentTimestamp();
        if (src_entry.isExpired(now)) return error.NoSuchKey;

        // Check destination
        if (!replace) {
            if (self.data.get(destination)) |dst| {
                if (!dst.isExpired(now)) return false;
            }
        }

        // Deep copy the value
        var copied_value = try self.deepCopyValue(src_entry);
        errdefer copied_value.deinit(self.allocator);

        // Remove old destination if exists
        if (self.data.fetchRemove(destination)) |kv| {
            self.allocator.free(kv.key);
            var old_value = kv.value;
            old_value.deinit(self.allocator);
        }

        // Insert copied value
        const owned_dest = try self.allocator.dupe(u8, destination);
        errdefer self.allocator.free(owned_dest);
        try self.data.put(owned_dest, copied_value);

        return true;
    }

    /// Copy a key from this storage to another storage (cross-database).
    /// source_key: key to copy from (in this storage)
    /// dest: destination storage
    /// dest_key: key to copy to (in destination storage)
    /// replace: if true, overwrite destination; if false, fail if destination exists.
    /// Returns true if copy succeeded, false if destination exists and replace=false.
    pub fn copyKeyToStorage(self: *Storage, source_key: []const u8, dest: *Storage, dest_key: []const u8, replace: bool) !bool {
        // Step 1: Lock source, read value, deep copy to dest allocator, unlock source
        var copied_value: Value = undefined;
        {
            self.mutex.lock();
            defer self.mutex.unlock();

            // Check source exists and is not expired
            const src_entry = self.data.get(source_key) orelse return error.NoSuchKey;
            const now = getCurrentTimestamp();
            if (src_entry.isExpired(now)) return error.NoSuchKey;

            // Deep copy using destination storage's allocator
            copied_value = try deepCopyValueWithAllocator(dest.allocator, src_entry);
            errdefer copied_value.deinit(dest.allocator);
        }

        // Step 2: Lock dest, check/insert value, unlock dest
        dest.mutex.lock();
        defer dest.mutex.unlock();

        // Check destination
        if (!replace) {
            if (dest.data.get(dest_key)) |dst| {
                const now = getCurrentTimestamp();
                if (!dst.isExpired(now)) {
                    // Destination exists and is not expired, return false and clean up copied value
                    copied_value.deinit(dest.allocator);
                    return false;
                }
            }
        }

        // Remove old destination if exists
        if (dest.data.fetchRemove(dest_key)) |kv| {
            dest.allocator.free(kv.key);
            var old_value = kv.value;
            old_value.deinit(dest.allocator);
        }

        // Insert copied value
        const owned_dest_key = try dest.allocator.dupe(u8, dest_key);
        errdefer dest.allocator.free(owned_dest_key);
        try dest.data.put(owned_dest_key, copied_value);

        return true;
    }

    /// Deep copy a Value using an explicit allocator.
    /// Used by COPY command with cross-database support.
    /// The returned Value's memory is owned by the provided allocator.
    fn deepCopyValueWithAllocator(alloc: std.mem.Allocator, value: Value) !Value {
        return switch (value) {
            .string => |s| blk: {
                const data_copy = try alloc.dupe(u8, s.data);
                break :blk Value{ .string = .{ .data = data_copy, .expires_at = s.expires_at } };
            },
            .list => |l| blk: {
                var list_copy = std.ArrayList([]const u8){};
                errdefer {
                    for (list_copy.items) |elem| alloc.free(elem);
                    list_copy.deinit(alloc);
                }
                for (l.data.items) |elem| {
                    const elem_copy = try alloc.dupe(u8, elem);
                    try list_copy.append(alloc, elem_copy);
                }
                break :blk Value{ .list = .{ .data = list_copy, .expires_at = l.expires_at } };
            },
            .set => |s| blk: {
                switch (s.encoding) {
                    .intset => {
                        var intset = intset_mod.IntSet.init(alloc);
                        errdefer intset.deinit();

                        var i: usize = 0;
                        while (i < s.data.intset.length) : (i += 1) {
                            const int_val = try s.data.intset.getAt(i);
                            _ = try intset.add(int_val);
                        }

                        break :blk Value{ .set = .{ .encoding = .intset, .data = .{ .intset = intset }, .expires_at = s.expires_at } };
                    },
                    .hashmap => {
                        var set_copy = std.StringHashMap(void).init(alloc);
                        errdefer {
                            var it = set_copy.keyIterator();
                            while (it.next()) |k| alloc.free(k.*);
                            set_copy.deinit();
                        }
                        var it = s.data.hashmap.keyIterator();
                        while (it.next()) |k| {
                            const key_copy = try alloc.dupe(u8, k.*);
                            try set_copy.put(key_copy, {});
                        }
                        break :blk Value{ .set = .{ .encoding = .hashmap, .data = .{ .hashmap = set_copy }, .expires_at = s.expires_at } };
                    },
                }
            },
            .hash => |h| blk: {
                var hash_copy = std.StringHashMap(Value.FieldValue).init(alloc);
                errdefer {
                    var it = hash_copy.iterator();
                    while (it.next()) |e| {
                        alloc.free(e.key_ptr.*);
                        var field_val = e.value_ptr.*;
                        field_val.deinit(alloc);
                    }
                    hash_copy.deinit();
                }
                var it = h.data.iterator();
                while (it.next()) |e| {
                    const field_copy = try alloc.dupe(u8, e.key_ptr.*);
                    const val_copy = try alloc.dupe(u8, e.value_ptr.*.data);
                    try hash_copy.put(field_copy, Value.FieldValue{
                        .data = val_copy,
                        .expires_at = e.value_ptr.*.expires_at, // Preserve field expiration
                    });
                }
                break :blk Value{ .hash = .{ .data = hash_copy, .expires_at = h.expires_at } };
            },
            .sorted_set => |z| blk: {
                var members_copy = std.StringHashMap(f64).init(alloc);
                var sorted_list_copy = std.ArrayList(Value.ScoredMember){};

                errdefer {
                    var it = members_copy.keyIterator();
                    while (it.next()) |k| alloc.free(k.*);
                    members_copy.deinit();
                    sorted_list_copy.deinit(alloc);
                }

                for (z.sorted_list.items) |scored| {
                    const member_copy = try alloc.dupe(u8, scored.member);
                    try members_copy.put(member_copy, scored.score);
                    try sorted_list_copy.append(alloc, .{ .score = scored.score, .member = member_copy });
                }

                break :blk Value{ .sorted_set = .{ .members = members_copy, .sorted_list = sorted_list_copy, .expires_at = z.expires_at } };
            },
            .stream => |st| blk: {
                var entries_copy = std.ArrayList(Value.StreamEntry){};
                errdefer {
                    for (entries_copy.items) |*e| e.deinit(alloc);
                    entries_copy.deinit(alloc);
                }

                for (st.entries.items) |e| {
                    var fields_copy = std.ArrayList([]const u8){};
                    errdefer {
                        for (fields_copy.items) |item| alloc.free(item);
                        fields_copy.deinit(alloc);
                    }

                    for (e.fields.items) |item| {
                        const item_copy = try alloc.dupe(u8, item);
                        try fields_copy.append(alloc, item_copy);
                    }

                    try entries_copy.append(alloc, .{
                        .id = e.id,
                        .fields = fields_copy,
                    });
                }

                break :blk Value{ .stream = .{
                    .entries = entries_copy,
                    .last_id = st.last_id,
                    .expires_at = st.expires_at,
                    .consumer_groups = std.StringHashMap(Value.ConsumerGroup).init(alloc),
                    .entries_added = st.entries_added,
                    .max_deleted_entry_id = st.max_deleted_entry_id,
                } };
            },
            .hyperloglog => |hll| blk: {
                // HyperLogLog is a fixed-size array, simple copy
                var hll_copy = Value.HyperLogLogValue.init();
                hll_copy.registers = hll.registers;
                hll_copy.expires_at = hll.expires_at;
                break :blk Value{ .hyperloglog = hll_copy };
            },
            .json => |j| blk: {
                // Deep clone JSON tree
                const cloned_root = try j.root.clone(alloc);
                break :blk Value{ .json = .{
                    .root = cloned_root,
                    .expires_at = j.expires_at,
                    .allocator = alloc,
                } };
            },
            .timeseries => |ts| blk: {
                // Time series deep copy not yet implemented
                break :blk Value{ .timeseries = ts };
            },
            .bloom => |b| blk: {
                // Bloom filter deep copy not yet implemented
                break :blk Value{ .bloom = b };
            },
            .cuckoo => |c| blk: {
                // Cuckoo filter deep copy not yet implemented
                break :blk Value{ .cuckoo = c };
            },
            .count_min_sketch => |cms| blk: {
                // Count-Min Sketch deep copy not yet implemented
                break :blk Value{ .count_min_sketch = cms };
            },
            .top_k => |tk| blk: {
                // Top-K deep copy not yet implemented
                break :blk Value{ .top_k = tk };
            },
            .t_digest => |td| blk: {
                // T-Digest deep copy not yet implemented
                break :blk Value{ .t_digest = td };
            },
            .vector_set => |vs| blk: {
                // Vector set deep copy not yet implemented
                break :blk Value{ .vector_set = vs };
            },
        };
    }

    /// Deep copy a Value (helper for COPY command)
    fn deepCopyValue(self: *Storage, value: Value) !Value {
        return try deepCopyValueWithAllocator(self.allocator, value);
    }

    /// Touch one or more keys (update last access time - currently a stub).
    /// Returns count of existing non-expired keys touched.
    pub fn touch(self: *Storage, keys: []const []const u8) usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = getCurrentTimestamp();
        var count: usize = 0;

        for (keys) |key| {
            if (self.data.get(key)) |value| {
                if (!value.isExpired(now)) {
                    count += 1;
                    // In a full implementation, we'd update an access timestamp here
                }
            }
        }

        return count;
    }

    // ── Key listing ──────────────────────────────────────────────────────────

    /// Return all non-expired keys matching the glob pattern.
    /// Pass "*" to get all keys.
    /// Caller owns the returned slice (and must free each element and the slice).
    /// The returned strings are copies.
    pub fn listKeys(self: *Storage, allocator: std.mem.Allocator, pattern: []const u8) ![][]const u8 {
        _ = pattern; // will be used by the caller via glob; we return all live keys here
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = getCurrentTimestamp();
        var result: std.ArrayList([]const u8) = .{ .items = &.{}, .capacity = 0 };
        errdefer {
            for (result.items) |item| allocator.free(item);
            result.deinit(allocator);
        }

        var it = self.data.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.isExpired(now)) continue;
            const copy = try allocator.dupe(u8, entry.key_ptr.*);
            errdefer allocator.free(copy);
            try result.append(allocator, copy);
        }

        return result.toOwnedSlice(allocator);
    }

    /// Remove all keys from storage
    pub fn flushAll(self: *Storage) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var it = self.data.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var value = entry.value_ptr.*;
            value.deinit(self.allocator);
        }
        self.data.clearRetainingCapacity();
        self.lfu_last_access.clearRetainingCapacity();
    }

    /// Flush all keys asynchronously (lazy freeing)
    pub fn flushAllAsync(self: *Storage) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Collect all keys first (can't mutate while iterating)
        var keys_to_remove = std.ArrayList([]const u8){};
        defer keys_to_remove.deinit(self.allocator);

        var it = self.data.keyIterator();
        while (it.next()) |key_ptr| {
            try keys_to_remove.append(self.allocator, key_ptr.*);
        }

        // Now remove each key and transfer ownership to background thread
        for (keys_to_remove.items) |key| {
            if (self.data.fetchRemove(key)) |kv| {
                self.cleanupLfuTracking(kv.key); // Clean up LFU tracking

                // Allocate Value on heap for background thread
                const value_ptr = try self.allocator.create(Value);
                errdefer self.allocator.destroy(value_ptr);
                value_ptr.* = kv.value; // Transfer ownership

                const owned_key = try self.allocator.dupe(u8, kv.key);
                errdefer self.allocator.free(owned_key);

                // Create work item
                const work = LazyFreeWork{
                    .work_type = .free_key,
                    .key = owned_key,
                    .db_num = null,
                    .value_ptr = value_ptr,
                    .allocator = self.allocator,
                };

                // Submit to lazy free task
                self.lazyfree_task.submitWork(work) catch |err| {
                    // On submit failure, clean up the work item
                    self.allocator.free(owned_key);
                    value_ptr.deinit(self.allocator);
                    self.allocator.destroy(value_ptr);
                    return err;
                };

                // Free the HashMap's key (background has its own copy)
                self.allocator.free(kv.key);
            }
        }
    }

    /// Unlink keys asynchronously (lazy freeing)
    /// Returns the count of keys that existed and were scheduled for deletion
    pub fn unlinkAsync(self: *Storage, keys: []const []const u8) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        var deleted_count: usize = 0;

        for (keys) |key| {
            if (self.data.fetchRemove(key)) |kv| {
                deleted_count += 1;

                self.cleanupLfuTracking(kv.key); // Clean up LFU tracking

                // Allocate Value on heap for background thread
                const value_ptr = try self.allocator.create(Value);
                errdefer self.allocator.destroy(value_ptr);
                value_ptr.* = kv.value;

                // Create owned copies for background thread
                const owned_key = try self.allocator.dupe(u8, kv.key);
                errdefer self.allocator.free(owned_key);

                // Create work item (value will be freed in background)
                const work = LazyFreeWork{
                    .work_type = .free_key,
                    .key = owned_key,
                    .db_num = null,
                    .value_ptr = value_ptr,
                    .allocator = self.allocator,
                };

                // Submit to lazy free task
                self.lazyfree_task.submitWork(work) catch |err| {
                    // On submit failure, clean up the work item
                    self.allocator.free(owned_key);
                    value_ptr.deinit(self.allocator);
                    self.allocator.destroy(value_ptr);
                    return err;
                };

                // Free the HashMap key (value is transferred to work item)
                self.allocator.free(kv.key);
            }
        }

        return deleted_count;
    }

    pub fn getCurrentTimestamp() i64 {
        return std.time.milliTimestamp();
    }

    // ── Set: new operations ───────────────────────────────────────────────────

    /// Pop random members from a set. count=0 means pop 1.
    /// Returns owned slice of member strings (caller must free slice and each string).
    /// Returns null if key does not exist.
    /// Returns error.WrongType if key is not a set.
    pub fn spop(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        count: usize,
    ) (error{ WrongType, OutOfMemory, IndexOutOfBounds } || std.fmt.ParseIntError)!?[][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .set => |*set_val| {
                // For intset, promote to hashmap for SPOP (random selection is simpler with hashmap)
                if (set_val.encoding == .intset) {
                    try set_val.promoteToHashmap(self.allocator);
                }

                const pop_count = if (count == 0) @as(usize, 1) else @min(count, set_val.count());
                if (pop_count == 0) return try allocator.alloc([]const u8, 0);

                var result = try allocator.alloc([]const u8, pop_count);
                errdefer {
                    for (result) |s| allocator.free(s);
                    allocator.free(result);
                }

                var i: usize = 0;
                while (i < pop_count) : (i += 1) {
                    // Pick a pseudo-random member by iterating to a random offset
                    const set_len = set_val.count();
                    if (set_len == 0) break;
                    const rnd_idx = @as(usize, @intCast(@mod(std.time.nanoTimestamp() +% @as(i128, @intCast(i)), @as(i128, @intCast(set_len)))));
                    var it = set_val.data.hashmap.keyIterator();
                    var idx: usize = 0;
                    while (it.next()) |member_ptr| {
                        if (idx == rnd_idx) {
                            const member = member_ptr.*;
                            result[i] = try allocator.dupe(u8, member);
                            // Remove from set
                            if (set_val.data.hashmap.fetchRemove(member)) |kv| {
                                self.allocator.free(kv.key);
                            }
                            break;
                        }
                        idx += 1;
                    }
                }

                // Auto-delete empty set
                if (set_val.count() == 0) {
                    const owned_key = entry.key_ptr.*;
                    var value = entry.value_ptr.*;
                    _ = self.removeKeyCleanup(key);
                    self.allocator.free(owned_key);
                    value.deinit(self.allocator);
                }

                return result;
            },
            else => return error.WrongType,
        }
    }

    /// Return random members from a set without removing them.
    /// If count >= 0: return min(count, setLen) distinct members.
    /// If count < 0: return abs(count) members (may repeat).
    /// Returns owned slice of member strings (caller must free slice and each string).
    /// Returns null if key does not exist.
    /// Returns error.WrongType if key is not a set.
    pub fn srandmember(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        count: i64,
    ) (error{ WrongType, OutOfMemory, IndexOutOfBounds } || std.fmt.ParseIntError)!?[][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .set => |*set_val| {
                const set_len = set_val.count();

                // Collect all members into a temporary slice for indexing
                var all_members = try allocator.alloc([]const u8, set_len);
                defer allocator.free(all_members);

                switch (set_val.encoding) {
                    .intset => {
                        var i: usize = 0;
                        while (i < set_len) : (i += 1) {
                            const int_val = try set_val.data.intset.getAt(i);
                            all_members[i] = try std.fmt.allocPrint(allocator, "{d}", .{int_val});
                        }
                    },
                    .hashmap => {
                        var it = set_val.data.hashmap.keyIterator();
                        var idx: usize = 0;
                        while (it.next()) |member_ptr| : (idx += 1) {
                            all_members[idx] = member_ptr.*;
                        }
                    },
                }

                if (count >= 0) {
                    // Distinct members up to min(count, setLen)
                    const n = @min(@as(usize, @intCast(count)), set_len);
                    var result = try allocator.alloc([]const u8, n);
                    errdefer {
                        for (result) |s| allocator.free(s);
                        allocator.free(result);
                    }
                    // Simple selection without replacement using a boolean visited array
                    var visited = try allocator.alloc(bool, set_len);
                    defer allocator.free(visited);
                    @memset(visited, false);
                    var ri: usize = 0;
                    while (ri < n) : (ri += 1) {
                        const rnd = @as(usize, @intCast(@mod(std.time.nanoTimestamp() +% @as(i128, @intCast(ri)), @as(i128, @intCast(set_len)))));
                        // Find next unvisited starting at rnd
                        var j: usize = 0;
                        while (j < set_len) : (j += 1) {
                            const candidate = (rnd + j) % set_len;
                            if (!visited[candidate]) {
                                visited[candidate] = true;
                                result[ri] = try allocator.dupe(u8, all_members[candidate]);
                                break;
                            }
                        }
                    }
                    return result;
                } else {
                    // May repeat; abs(count) elements
                    const n = @as(usize, @intCast(-count));
                    var result = try allocator.alloc([]const u8, n);
                    errdefer {
                        for (result) |s| allocator.free(s);
                        allocator.free(result);
                    }
                    for (0..n) |ri| {
                        const rnd = @as(usize, @intCast(@mod(std.time.nanoTimestamp() +% @as(i128, @intCast(ri)), @as(i128, @intCast(set_len)))));
                        result[ri] = try allocator.dupe(u8, all_members[rnd % set_len]);
                    }
                    return result;
                }
            },
            else => return error.WrongType,
        }
    }

    /// Atomically move member from source set to destination set.
    /// Returns true if moved, false if member not found in source.
    /// Returns error.WrongType if source or destination is not a set.
    pub fn smove(
        self: *Storage,
        source: []const u8,
        destination: []const u8,
        member: []const u8,
    ) (error{ WrongType, OutOfMemory, IndexOutOfBounds } || std.fmt.ParseIntError)!bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Verify source
        const src_entry = self.data.getEntry(source) orelse return false;

        if (src_entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = src_entry.key_ptr.*;
            var value = src_entry.value_ptr.*;
            _ = self.data.remove(source);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return false;
        }

        switch (src_entry.value_ptr.*) {
            .set => {},
            else => return error.WrongType,
        }

        const src_set = &src_entry.value_ptr.set;
        if (!(try src_set.contains(member))) return false;

        // Check destination type (if it exists)
        if (self.data.getEntry(destination)) |dst_entry| {
            if (!dst_entry.value_ptr.isExpired(getCurrentTimestamp())) {
                switch (dst_entry.value_ptr.*) {
                    .set => {},
                    else => return error.WrongType,
                }
            }
        }

        // Remove from source
        const owned_member: []const u8 = blk: {
            switch (src_set.encoding) {
                .intset => {
                    const int_val = std.fmt.parseInt(i64, member, 10) catch return false;
                    _ = try src_set.data.intset.remove(int_val);
                    break :blk try self.allocator.dupe(u8, member);
                },
                .hashmap => {
                    if (src_set.data.hashmap.fetchRemove(member)) |kv| {
                        break :blk kv.key;
                    }
                    return false;
                },
            }
        };

        // Auto-delete source if empty
        if (src_set.count() == 0) {
            const owned_key = src_entry.key_ptr.*;
            var value = src_entry.value_ptr.*;
            _ = self.data.remove(source);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
        }

        // Insert into destination
        if (self.data.getEntry(destination)) |dst_entry| {
            if (dst_entry.value_ptr.isExpired(getCurrentTimestamp())) {
                const owned_key = dst_entry.key_ptr.*;
                var value = dst_entry.value_ptr.*;
                _ = self.data.remove(destination);
                self.allocator.free(owned_key);
                value.deinit(self.allocator);
                // fall through to create new
            } else {
                switch (dst_entry.value_ptr.*) {
                    .set => |*dst_set| {
                        // member already owned; if destination already has it, free the dup
                        if (try dst_set.contains(owned_member)) {
                            self.allocator.free(owned_member);
                        } else {
                            // Need to promote to hashmap for adding string
                            if (dst_set.encoding == .intset) {
                                try dst_set.promoteToHashmap(self.allocator);
                            }
                            try dst_set.data.hashmap.put(owned_member, {});
                        }
                        return true;
                    },
                    else => {
                        self.allocator.free(owned_member);
                        return error.WrongType;
                    },
                }
            }
        }

        // Create new destination set
        var new_set = std.StringHashMap(void).init(self.allocator);
        errdefer {
            var it = new_set.keyIterator();
            while (it.next()) |k| self.allocator.free(k.*);
            new_set.deinit();
        }
        try new_set.put(owned_member, {});
        const owned_dst_key = try self.allocator.dupe(u8, destination);
        errdefer self.allocator.free(owned_dst_key);
        try self.data.put(owned_dst_key, Value{
            .set = .{
                .encoding = .hashmap,
                .data = .{ .hashmap = new_set },
                .expires_at = null,
            },
        });
        return true;
    }

    /// Check membership for multiple members. Returns owned slice of bool (true=member).
    /// Returns error.WrongType if key is not a set.
    pub fn smismember(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        members: []const []const u8,
    ) (error{ WrongType, OutOfMemory, IndexOutOfBounds } || std.fmt.ParseIntError)![]bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        var result = try allocator.alloc(bool, members.len);
        errdefer allocator.free(result);

        const entry = self.data.getEntry(key) orelse {
            @memset(result, false);
            return result;
        };

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            @memset(result, false);
            return result;
        }

        switch (entry.value_ptr.*) {
            .set => |*set_val| {
                for (members, 0..) |member, i| {
                    result[i] = try set_val.contains(member);
                }
                return result;
            },
            else => {
                allocator.free(result);
                return error.WrongType;
            },
        }
    }

    /// Return cardinality of set intersection, optionally capped at limit (0 = no cap).
    /// Returns error.WrongType if any key is not a set.
    pub fn sintercard(
        self: *Storage,
        allocator: std.mem.Allocator,
        keys: []const []const u8,
        limit: usize,
    ) (error{ WrongType, OutOfMemory, IndexOutOfBounds } || std.fmt.ParseIntError)!usize {
        const members = try self.sinter(allocator, keys);
        defer allocator.free(members);
        const count = members.len;
        if (limit > 0 and count > limit) return limit;
        return count;
    }

    // ── Sorted Set: new operations ────────────────────────────────────────────

    /// Pop count lowest-score members from sorted set.
    /// Returns owned slice of ScoredMember (caller must free).
    /// Returns null if key does not exist.
    /// Returns error.WrongType if key is not a sorted set.
    pub fn zpopmin(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        count: usize,
    ) error{ WrongType, OutOfMemory }!?[]Value.ScoredMember {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| {
                const pop_count = @min(count, zset.sorted_list.items.len);
                var result = try allocator.alloc(Value.ScoredMember, pop_count);
                errdefer allocator.free(result);

                for (0..pop_count) |i| {
                    const item = zset.sorted_list.items[0];
                    result[i] = Value.ScoredMember{
                        .score = item.score,
                        .member = try allocator.dupe(u8, item.member),
                    };
                    _ = zset.sorted_list.orderedRemove(0);
                    if (zset.members.fetchRemove(item.member)) |kv| {
                        self.allocator.free(kv.key);
                    }
                }

                if (zset.members.count() == 0) {
                    const owned_key = entry.key_ptr.*;
                    var value = entry.value_ptr.*;
                    _ = self.removeKeyCleanup(key);
                    self.allocator.free(owned_key);
                    value.deinit(self.allocator);
                }

                return result;
            },
            else => return error.WrongType,
        }
    }

    /// Pop count highest-score members from sorted set.
    /// Returns owned slice of ScoredMember (caller must free).
    /// Returns null if key does not exist.
    /// Returns error.WrongType if key is not a sorted set.
    pub fn zpopmax(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        count: usize,
    ) error{ WrongType, OutOfMemory }!?[]Value.ScoredMember {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| {
                const pop_count = @min(count, zset.sorted_list.items.len);
                var result = try allocator.alloc(Value.ScoredMember, pop_count);
                errdefer allocator.free(result);

                for (0..pop_count) |i| {
                    const last_idx = zset.sorted_list.items.len - 1;
                    const item = zset.sorted_list.items[last_idx];
                    result[i] = Value.ScoredMember{
                        .score = item.score,
                        .member = try allocator.dupe(u8, item.member),
                    };
                    _ = zset.sorted_list.pop();
                    if (zset.members.fetchRemove(item.member)) |kv| {
                        self.allocator.free(kv.key);
                    }
                }

                if (zset.members.count() == 0) {
                    const owned_key = entry.key_ptr.*;
                    var value = entry.value_ptr.*;
                    _ = self.removeKeyCleanup(key);
                    self.allocator.free(owned_key);
                    value.deinit(self.allocator);
                }

                return result;
            },
            else => return error.WrongType,
        }
    }

    /// Get scores for multiple members. Returns owned slice of ?f64 (null for missing).
    /// Returns error.WrongType if key is not a sorted set.
    pub fn zmscore(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        members: []const []const u8,
    ) error{ WrongType, OutOfMemory }![]?f64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var result = try allocator.alloc(?f64, members.len);
        errdefer allocator.free(result);

        const entry = self.data.getEntry(key) orelse {
            @memset(result, null);
            return result;
        };

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            @memset(result, null);
            return result;
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| {
                for (members, 0..) |member, i| {
                    result[i] = zset.members.get(member);
                }
                return result;
            },
            else => {
                allocator.free(result);
                return error.WrongType;
            },
        }
    }

    /// Return random members from sorted set.
    /// count >= 0: up to count distinct members; count < 0: abs(count) members (may repeat).
    /// Returns owned slice of ScoredMember (caller must free each .member and the slice).
    /// Returns null if key does not exist.
    /// Returns error.WrongType if key is not a sorted set.
    pub fn zrandmember(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        count: i64,
    ) error{ WrongType, OutOfMemory }!?[]Value.ScoredMember {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| {
                const items = zset.sorted_list.items;
                const set_len = items.len;
                if (set_len == 0) return try allocator.alloc(Value.ScoredMember, 0);

                if (count >= 0) {
                    const n = @min(@as(usize, @intCast(count)), set_len);
                    var result = try allocator.alloc(Value.ScoredMember, n);
                    errdefer {
                        for (result) |sm| allocator.free(sm.member);
                        allocator.free(result);
                    }
                    var visited = try allocator.alloc(bool, set_len);
                    defer allocator.free(visited);
                    @memset(visited, false);
                    for (0..n) |ri| {
                        const rnd = @as(usize, @intCast(@mod(std.time.nanoTimestamp() +% @as(i128, @intCast(ri)), @as(i128, @intCast(set_len)))));
                        var j: usize = 0;
                        while (j < set_len) : (j += 1) {
                            const candidate = (rnd + j) % set_len;
                            if (!visited[candidate]) {
                                visited[candidate] = true;
                                result[ri] = Value.ScoredMember{
                                    .score = items[candidate].score,
                                    .member = try allocator.dupe(u8, items[candidate].member),
                                };
                                break;
                            }
                        }
                    }
                    return result;
                } else {
                    const n = @as(usize, @intCast(-count));
                    var result = try allocator.alloc(Value.ScoredMember, n);
                    errdefer {
                        for (result) |sm| allocator.free(sm.member);
                        allocator.free(result);
                    }
                    for (0..n) |ri| {
                        const rnd = @as(usize, @intCast(@mod(std.time.nanoTimestamp() +% @as(i128, @intCast(ri)), @as(i128, @intCast(set_len)))));
                        result[ri] = Value.ScoredMember{
                            .score = items[rnd].score,
                            .member = try allocator.dupe(u8, items[rnd].member),
                        };
                    }
                    return result;
                }
            },
            else => return error.WrongType,
        }
    }

    /// Get range of members by rank in reverse order (highest rank first).
    /// Returns owned slice of ScoredMember (caller must free each .member and the slice).
    /// Returns null if key does not exist.
    pub fn zrevrange(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        start: i64,
        stop: i64,
    ) error{ WrongType, OutOfMemory }!?[]Value.ScoredMember {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| {
                const len = zset.sorted_list.items.len;
                if (len == 0) return try allocator.alloc(Value.ScoredMember, 0);

                // Normalize indices as ZRANGE but the view is reversed
                const norm_start: usize = blk: {
                    if (start < 0) {
                        const s = @as(i64, @intCast(len)) + start;
                        break :blk if (s < 0) 0 else @as(usize, @intCast(s));
                    } else {
                        if (@as(usize, @intCast(start)) >= len) return try allocator.alloc(Value.ScoredMember, 0);
                        break :blk @as(usize, @intCast(start));
                    }
                };

                const norm_stop: usize = blk: {
                    if (stop < 0) {
                        const s = @as(i64, @intCast(len)) + stop;
                        if (s < 0) return try allocator.alloc(Value.ScoredMember, 0);
                        break :blk @as(usize, @intCast(s));
                    } else {
                        break :blk @min(@as(usize, @intCast(stop)), len - 1);
                    }
                };

                if (norm_start > norm_stop) return try allocator.alloc(Value.ScoredMember, 0);

                const count = norm_stop - norm_start + 1;
                var result = try allocator.alloc(Value.ScoredMember, count);
                errdefer {
                    for (result) |sm| allocator.free(sm.member);
                    allocator.free(result);
                }

                // Reversed: index 0 in result = highest rank = last in sorted_list
                for (0..count) |i| {
                    const src_idx = len - 1 - norm_start - i;
                    result[i] = Value.ScoredMember{
                        .score = zset.sorted_list.items[src_idx].score,
                        .member = try allocator.dupe(u8, zset.sorted_list.items[src_idx].member),
                    };
                }
                return result;
            },
            else => return error.WrongType,
        }
    }

    /// Get members by score in descending order (max to min).
    /// Returns owned slice of ScoredMember (caller must free each .member and the slice).
    /// Returns null if key does not exist.
    pub fn zrevrangebyscore(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        max: f64,
        min: f64,
        offset: usize,
        limit: i64,
    ) error{ WrongType, OutOfMemory }!?[]Value.ScoredMember {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| {
                var result_list: std.ArrayList(Value.ScoredMember) = .{
                    .items = &.{},
                    .capacity = 0,
                };
                defer result_list.deinit(allocator);

                // Iterate sorted_list in reverse (highest score first)
                const items = zset.sorted_list.items;
                var i: usize = items.len;
                while (i > 0) {
                    i -= 1;
                    const item = items[i];
                    if (item.score >= min and item.score <= max) {
                        try result_list.append(allocator, item);
                    } else if (item.score < min) {
                        break;
                    }
                }

                // Apply offset/limit
                const start_idx = @min(offset, result_list.items.len);
                const end_idx = if (limit < 0)
                    result_list.items.len
                else
                    @min(start_idx + @as(usize, @intCast(limit)), result_list.items.len);

                const slice = result_list.items[start_idx..end_idx];
                var result = try allocator.alloc(Value.ScoredMember, slice.len);
                errdefer {
                    for (result) |sm| allocator.free(sm.member);
                    allocator.free(result);
                }
                for (slice, 0..) |sm, ri| {
                    result[ri] = Value.ScoredMember{
                        .score = sm.score,
                        .member = try allocator.dupe(u8, sm.member),
                    };
                }
                return result;
            },
            else => return error.WrongType,
        }
    }

    // ── Sorted Set Set-Like Operations ────────────────────────────────────────

    /// Compute union of sorted sets at keys with optional aggregation and weights.
    /// Returns owned slice of ScoredMember. For duplicate members across sets, scores are aggregated.
    /// Non-existent keys are treated as empty sorted sets.
    /// Returns error.WrongType if any key is not a sorted set.
    pub fn zunion(
        self: *Storage,
        allocator: std.mem.Allocator,
        keys: []const []const u8,
    ) error{ WrongType, OutOfMemory }![]Value.ScoredMember {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Accumulate members and their summed scores
        var score_map = std.StringHashMap(f64).init(allocator);
        defer score_map.deinit();

        for (keys) |key| {
            const entry = self.data.getEntry(key) orelse continue;
            if (entry.value_ptr.isExpired(getCurrentTimestamp())) continue;
            switch (entry.value_ptr.*) {
                .sorted_set => |*zset| {
                    var it = zset.members.iterator();
                    while (it.next()) |kv| {
                        const member = kv.key_ptr.*;
                        const score = kv.value_ptr.*;
                        if (score_map.get(member)) |existing| {
                            try score_map.put(member, existing + score);
                        } else {
                            try score_map.put(member, score);
                        }
                    }
                },
                else => return error.WrongType,
            }
        }

        // Build result sorted by score
        var result = try allocator.alloc(Value.ScoredMember, score_map.count());
        errdefer allocator.free(result);
        var i: usize = 0;
        var it = score_map.iterator();
        while (it.next()) |kv| : (i += 1) {
            result[i] = Value.ScoredMember{
                .score = kv.value_ptr.*,
                .member = try allocator.dupe(u8, kv.key_ptr.*),
            };
        }

        // Sort by score (ascending), then lexicographically
        std.mem.sort(Value.ScoredMember, result, {}, struct {
            fn lessThan(_: void, a: Value.ScoredMember, b: Value.ScoredMember) bool {
                if (a.score < b.score) return true;
                if (a.score > b.score) return false;
                return std.mem.lessThan(u8, a.member, b.member);
            }
        }.lessThan);

        return result;
    }

    /// Compute intersection of sorted sets at keys with optional aggregation and weights.
    /// Returns owned slice of ScoredMember present in all sets. For duplicate members, scores are summed.
    /// Non-existent keys are treated as empty sorted sets (result is empty).
    /// Returns error.WrongType if any key is not a sorted set.
    pub fn zinter(
        self: *Storage,
        allocator: std.mem.Allocator,
        keys: []const []const u8,
    ) error{ WrongType, OutOfMemory }![]Value.ScoredMember {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (keys.len == 0) return try allocator.alloc(Value.ScoredMember, 0);

        // Find the first sorted set to seed candidates
        var first_zset: ?*Value.SortedSetValue = null;
        for (keys) |key| {
            const entry = self.data.getEntry(key) orelse {
                // Key missing = empty set, intersection is empty
                return try allocator.alloc(Value.ScoredMember, 0);
            };
            if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
                return try allocator.alloc(Value.ScoredMember, 0);
            }
            switch (entry.value_ptr.*) {
                .sorted_set => |*zs| {
                    first_zset = zs;
                    break;
                },
                else => return error.WrongType,
            }
        }

        const first = first_zset orelse return try allocator.alloc(Value.ScoredMember, 0);

        // For each candidate in first set, check membership in all other sets
        // Accumulate scores for members present in all sets
        var score_map = std.StringHashMap(f64).init(allocator);
        defer score_map.deinit();

        var it = first.members.iterator();
        outer: while (it.next()) |kv| {
            const member = kv.key_ptr.*;
            var total_score = kv.value_ptr.*;

            // Check in all remaining keys
            for (keys[1..]) |key| {
                const entry = self.data.getEntry(key) orelse continue :outer;
                if (entry.value_ptr.isExpired(getCurrentTimestamp())) continue :outer;
                switch (entry.value_ptr.*) {
                    .sorted_set => |*zs| {
                        if (zs.members.get(member)) |score| {
                            total_score += score;
                        } else {
                            continue :outer;
                        }
                    },
                    else => return error.WrongType,
                }
            }

            // Member exists in all sets, add to result
            try score_map.put(member, total_score);
        }

        // Build result sorted by score
        var result = try allocator.alloc(Value.ScoredMember, score_map.count());
        errdefer allocator.free(result);
        var idx: usize = 0;
        var result_it = score_map.iterator();
        while (result_it.next()) |kv| : (idx += 1) {
            result[idx] = Value.ScoredMember{
                .score = kv.value_ptr.*,
                .member = try allocator.dupe(u8, kv.key_ptr.*),
            };
        }

        // Sort by score (ascending), then lexicographically
        std.mem.sort(Value.ScoredMember, result, {}, struct {
            fn lessThan(_: void, a: Value.ScoredMember, b: Value.ScoredMember) bool {
                if (a.score < b.score) return true;
                if (a.score > b.score) return false;
                return std.mem.lessThan(u8, a.member, b.member);
            }
        }.lessThan);

        return result;
    }

    /// Compute difference of sorted sets: first set minus all other sets.
    /// Returns owned slice of ScoredMember from first set not present in any other set.
    /// Non-existent keys are treated as empty sorted sets.
    /// Returns error.WrongType if any key is not a sorted set.
    pub fn zdiff(
        self: *Storage,
        allocator: std.mem.Allocator,
        keys: []const []const u8,
    ) error{ WrongType, OutOfMemory }![]Value.ScoredMember {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (keys.len == 0) return try allocator.alloc(Value.ScoredMember, 0);

        // Get the first sorted set
        const first_entry = self.data.getEntry(keys[0]) orelse {
            return try allocator.alloc(Value.ScoredMember, 0);
        };
        if (first_entry.value_ptr.isExpired(getCurrentTimestamp())) {
            return try allocator.alloc(Value.ScoredMember, 0);
        }

        const first_zset = switch (first_entry.value_ptr.*) {
            .sorted_set => |*zs| zs,
            else => return error.WrongType,
        };

        // Validate remaining keys are sorted sets
        for (keys[1..]) |key| {
            const entry = self.data.getEntry(key) orelse continue;
            if (entry.value_ptr.isExpired(getCurrentTimestamp())) continue;
            switch (entry.value_ptr.*) {
                .sorted_set => {},
                else => return error.WrongType,
            }
        }

        // Build result: members from first set not in any other set
        var result_list = std.ArrayList(Value.ScoredMember){
            .items = &.{},
            .capacity = 0,
        };
        defer result_list.deinit(allocator);

        var it = first_zset.members.iterator();
        outer: while (it.next()) |kv| {
            const member = kv.key_ptr.*;
            const score = kv.value_ptr.*;

            // Check if member exists in any of the remaining sets
            for (keys[1..]) |key| {
                const entry = self.data.getEntry(key) orelse continue;
                if (entry.value_ptr.isExpired(getCurrentTimestamp())) continue;
                switch (entry.value_ptr.*) {
                    .sorted_set => |*zs| {
                        if (zs.members.contains(member)) continue :outer;
                    },
                    else => {},
                }
            }

            // Member not in any other set, add to result
            try result_list.append(allocator, Value.ScoredMember{
                .score = score,
                .member = try allocator.dupe(u8, member),
            });
        }

        return try result_list.toOwnedSlice(allocator);
    }

    /// Store union of sorted sets at keys into destination.
    /// Returns count of members in result set.
    /// Returns error.WrongType if any key is not a sorted set.
    pub fn zunionstore(
        self: *Storage,
        allocator: std.mem.Allocator,
        dest: []const u8,
        keys: []const []const u8,
    ) !usize {
        const members = try self.zunion(allocator, keys);
        defer {
            for (members) |m| allocator.free(m.member);
            allocator.free(members);
        }

        // Delete destination if it exists
        _ = self.del(&[_][]const u8{dest});

        if (members.len == 0) return 0;

        // Create new sorted set
        var scores = try std.ArrayList(f64).initCapacity(allocator, members.len);
        defer scores.deinit(allocator);
        var member_strings = try std.ArrayList([]const u8).initCapacity(allocator, members.len);
        defer member_strings.deinit(allocator);

        for (members) |sm| {
            try scores.append(allocator, sm.score);
            try member_strings.append(allocator, sm.member);
        }

        _ = try self.zadd(dest, scores.items, member_strings.items, 0, null);
        return members.len;
    }

    /// Store intersection of sorted sets at keys into destination.
    /// Returns count of members in result set.
    /// Returns error.WrongType if any key is not a sorted set.
    pub fn zinterstore(
        self: *Storage,
        allocator: std.mem.Allocator,
        dest: []const u8,
        keys: []const []const u8,
    ) !usize {
        const members = try self.zinter(allocator, keys);
        defer {
            for (members) |m| allocator.free(m.member);
            allocator.free(members);
        }

        // Delete destination if it exists
        _ = self.del(&[_][]const u8{dest});

        if (members.len == 0) return 0;

        // Create new sorted set
        var scores = try std.ArrayList(f64).initCapacity(allocator, members.len);
        defer scores.deinit(allocator);
        var member_strings = try std.ArrayList([]const u8).initCapacity(allocator, members.len);
        defer member_strings.deinit(allocator);

        for (members) |sm| {
            try scores.append(allocator, sm.score);
            try member_strings.append(allocator, sm.member);
        }

        _ = try self.zadd(dest, scores.items, member_strings.items, 0, null);
        return members.len;
    }

    /// Store difference of sorted sets (first minus others) into destination.
    /// Returns count of members in result set.
    /// Returns error.WrongType if any key is not a sorted set.
    pub fn zdiffstore(
        self: *Storage,
        allocator: std.mem.Allocator,
        dest: []const u8,
        keys: []const []const u8,
    ) !usize {
        const members = try self.zdiff(allocator, keys);
        defer {
            for (members) |m| allocator.free(m.member);
            allocator.free(members);
        }

        // Delete destination if it exists
        _ = self.del(&[_][]const u8{dest});

        if (members.len == 0) return 0;

        // Create new sorted set
        var scores = try std.ArrayList(f64).initCapacity(allocator, members.len);
        defer scores.deinit(allocator);
        var member_strings = try std.ArrayList([]const u8).initCapacity(allocator, members.len);
        defer member_strings.deinit(allocator);

        for (members) |sm| {
            try scores.append(allocator, sm.score);
            try member_strings.append(allocator, sm.member);
        }

        _ = try self.zadd(dest, scores.items, member_strings.items, 0, null);
        return members.len;
    }

    /// Remove all members in a sorted set within the given rank range [start, stop]
    /// Supports negative indices (-1 = last element)
    /// Returns the number of elements removed
    pub fn zremrangebyrank(
        self: *Storage,
        key: []const u8,
        start: i64,
        stop: i64,
    ) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return 0;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return 0;
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| {
                const len = @as(i64, @intCast(zset.sorted_list.items.len));
                if (len == 0) return 0;

                // Normalize negative indices
                const norm_start: i64 = if (start < 0) @max(len + start, 0) else @min(start, len - 1);
                const norm_stop: i64 = if (stop < 0) @max(len + stop, -1) else @min(stop, len - 1);

                if (norm_start > norm_stop or norm_start >= len) return 0;

                const start_idx = @as(usize, @intCast(norm_start));
                const stop_idx = @as(usize, @intCast(norm_stop));
                const count = stop_idx - start_idx + 1;

                // Collect members to remove
                var to_remove = try std.ArrayList([]const u8).initCapacity(self.allocator, count);
                defer to_remove.deinit(self.allocator);

                var i: usize = start_idx;
                while (i <= stop_idx and i < zset.sorted_list.items.len) : (i += 1) {
                    try to_remove.append(self.allocator, zset.sorted_list.items[i].member);
                }

                // Remove from both structures
                var removed: usize = 0;
                for (to_remove.items) |member| {
                    // Remove from hash map (frees the key)
                    if (zset.members.fetchRemove(member)) |kv| {
                        self.allocator.free(kv.key);
                        removed += 1;
                    }
                }

                // Remove from sorted list (in reverse to maintain indices)
                var j: usize = stop_idx + 1;
                while (j > start_idx) {
                    j -= 1;
                    _ = zset.sorted_list.orderedRemove(j);
                }

                // Auto-delete if sorted set becomes empty
                if (zset.members.count() == 0) {
                    const owned_key = entry.key_ptr.*;
                    var value = entry.value_ptr.*;
                    _ = self.removeKeyCleanup(key);
                    self.allocator.free(owned_key);
                    value.deinit(self.allocator);
                }

                return removed;
            },
            else => return error.WrongType,
        }
    }

    /// Remove all members in a sorted set within the given score range [min, max]
    /// Supports exclusive ranges with "(" prefix
    /// Returns the number of elements removed
    pub fn zremrangebyscore(
        self: *Storage,
        key: []const u8,
        min: f64,
        max: f64,
        min_exclusive: bool,
        max_exclusive: bool,
    ) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return 0;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return 0;
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| {
                // Collect members to remove
                var to_remove = std.ArrayList([]const u8){};
                defer to_remove.deinit(self.allocator);

                for (zset.sorted_list.items) |item| {
                    const in_range = if (min_exclusive and item.score <= min) false else if (max_exclusive and item.score >= max) false else item.score >= min and item.score <= max;

                    if (in_range) {
                        try to_remove.append(self.allocator, item.member);
                    }
                }

                // Remove from both structures
                var removed: usize = 0;
                for (to_remove.items) |member| {
                    // Remove from sorted list
                    for (zset.sorted_list.items, 0..) |item, idx| {
                        if (std.mem.eql(u8, item.member, member)) {
                            _ = zset.sorted_list.orderedRemove(idx);
                            break;
                        }
                    }
                    // Remove from hash map (frees the key)
                    if (zset.members.fetchRemove(member)) |kv| {
                        self.allocator.free(kv.key);
                        removed += 1;
                    }
                }

                // Auto-delete if sorted set becomes empty
                if (zset.members.count() == 0) {
                    const owned_key = entry.key_ptr.*;
                    var value = entry.value_ptr.*;
                    _ = self.removeKeyCleanup(key);
                    self.allocator.free(owned_key);
                    value.deinit(self.allocator);
                }

                return removed;
            },
            else => return error.WrongType,
        }
    }

    /// Remove all members in a sorted set within the given lexicographical range
    /// min and max can be "-" (neg infinity), "+" (pos infinity), "[" (inclusive), or "(" (exclusive) prefix
    /// Returns the number of elements removed
    pub fn zremrangebylex(
        self: *Storage,
        key: []const u8,
        min: []const u8,
        max: []const u8,
    ) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return 0;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return 0;
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| {
                // Parse min/max
                const min_info = try parseLexRange(min);
                const max_info = try parseLexRange(max);

                // Collect members to remove
                var to_remove = std.ArrayList([]const u8){};
                defer to_remove.deinit(self.allocator);

                for (zset.sorted_list.items) |item| {
                    if (!inLexRange(item.member, min_info, max_info)) continue;
                    try to_remove.append(self.allocator, item.member);
                }

                // Remove from both structures
                var removed: usize = 0;
                for (to_remove.items) |member| {
                    // Remove from sorted list
                    for (zset.sorted_list.items, 0..) |item, idx| {
                        if (std.mem.eql(u8, item.member, member)) {
                            _ = zset.sorted_list.orderedRemove(idx);
                            break;
                        }
                    }
                    // Remove from hash map (frees the key)
                    if (zset.members.fetchRemove(member)) |kv| {
                        self.allocator.free(kv.key);
                        removed += 1;
                    }
                }

                // Auto-delete if sorted set becomes empty
                if (zset.members.count() == 0) {
                    const owned_key = entry.key_ptr.*;
                    var value = entry.value_ptr.*;
                    _ = self.removeKeyCleanup(key);
                    self.allocator.free(owned_key);
                    value.deinit(self.allocator);
                }

                return removed;
            },
            else => return error.WrongType,
        }
    }

    /// Get members in sorted set within the given lexicographical range
    /// min and max can be "-" (neg infinity), "+" (pos infinity), "[" (inclusive), or "(" (exclusive) prefix
    /// Returns owned slice that caller must free (both the slice and each member string)
    pub fn zrangebylex(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        min: []const u8,
        max: []const u8,
        offset: ?usize,
        count: ?usize,
    ) ![][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse {
            return try allocator.alloc([]const u8, 0);
        };

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return try allocator.alloc([]const u8, 0);
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| {
                // Parse min/max
                const min_info = try parseLexRange(min);
                const max_info = try parseLexRange(max);

                // Collect matching members
                var result = std.ArrayList([]const u8){};
                defer result.deinit(allocator);

                var skip = offset orelse 0;
                var remaining = count orelse std.math.maxInt(usize);

                for (zset.sorted_list.items) |item| {
                    if (!inLexRange(item.member, min_info, max_info)) continue;

                    if (skip > 0) {
                        skip -= 1;
                        continue;
                    }

                    if (remaining == 0) break;

                    try result.append(allocator, try allocator.dupe(u8, item.member));
                    remaining -= 1;
                }

                return try result.toOwnedSlice(allocator);
            },
            else => return error.WrongType,
        }
    }

    /// Get members in reverse order within the given lexicographical range
    /// Returns owned slice that caller must free
    pub fn zrevrangebylex(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        max: []const u8,
        min: []const u8,
        offset: ?usize,
        count: ?usize,
    ) ![][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse {
            return try allocator.alloc([]const u8, 0);
        };

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return try allocator.alloc([]const u8, 0);
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| {
                // Parse min/max
                const min_info = try parseLexRange(min);
                const max_info = try parseLexRange(max);

                // Collect matching members in reverse
                var result = std.ArrayList([]const u8){};
                defer result.deinit(allocator);

                var skip = offset orelse 0;
                var remaining = count orelse std.math.maxInt(usize);

                var i: usize = zset.sorted_list.items.len;
                while (i > 0) {
                    i -= 1;
                    const item = zset.sorted_list.items[i];

                    if (!inLexRange(item.member, min_info, max_info)) continue;

                    if (skip > 0) {
                        skip -= 1;
                        continue;
                    }

                    if (remaining == 0) break;

                    try result.append(allocator, try allocator.dupe(u8, item.member));
                    remaining -= 1;
                }

                return try result.toOwnedSlice(allocator);
            },
            else => return error.WrongType,
        }
    }

    /// Count members within the given lexicographical range
    pub fn zlexcount(
        self: *Storage,
        key: []const u8,
        min: []const u8,
        max: []const u8,
    ) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return 0;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return 0;
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| {
                const min_info = try parseLexRange(min);
                const max_info = try parseLexRange(max);

                var count: usize = 0;
                for (zset.sorted_list.items) |item| {
                    if (inLexRange(item.member, min_info, max_info)) {
                        count += 1;
                    }
                }
                return count;
            },
            else => return error.WrongType,
        }
    }

    /// Store ZRANGE result in destination sorted set
    /// Returns number of members stored
    /// Returns error.WrongType if source key is not a sorted set
    pub fn zrangestore(
        self: *Storage,
        allocator: std.mem.Allocator,
        dest: []const u8,
        source: []const u8,
        start: i64,
        stop: i64,
        with_scores: bool,
    ) !usize {
        _ = allocator;
        _ = with_scores;
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(source) orelse return 0;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.data.remove(source);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return 0;
        }

        switch (entry.value_ptr.*) {
            .sorted_set => |*zset| {
                const len = zset.sorted_list.items.len;
                if (len == 0) {
                    // Empty source - delete destination if it exists
                    if (self.data.getEntry(dest)) |dest_entry| {
                        const dest_key = dest_entry.key_ptr.*;
                        var dest_value = dest_entry.value_ptr.*;
                        _ = self.data.remove(dest);
                        self.allocator.free(dest_key);
                        dest_value.deinit(self.allocator);
                    }
                    return 0;
                }

                // Normalize indices
                const norm_start: i64 = if (start < 0)
                    @max(0, @as(i64, @intCast(len)) + start)
                else
                    @min(start, @as(i64, @intCast(len)));

                const norm_stop: i64 = if (stop < 0)
                    @max(-1, @as(i64, @intCast(len)) + stop)
                else
                    @min(stop, @as(i64, @intCast(len)) - 1);

                if (norm_start > norm_stop or norm_start >= @as(i64, @intCast(len))) {
                    // Empty range - delete destination
                    if (self.data.getEntry(dest)) |dest_entry| {
                        const dest_key = dest_entry.key_ptr.*;
                        var dest_value = dest_entry.value_ptr.*;
                        _ = self.data.remove(dest);
                        self.allocator.free(dest_key);
                        dest_value.deinit(self.allocator);
                    }
                    return 0;
                }

                const u_start: usize = @intCast(norm_start);
                const u_stop: usize = @intCast(norm_stop);

                // Build destination sorted set
                var dest_members = std.StringHashMap(f64).init(self.allocator);
                errdefer {
                    var it = dest_members.keyIterator();
                    while (it.next()) |k| self.allocator.free(k.*);
                    dest_members.deinit();
                }

                var dest_sorted: std.ArrayList(Value.ScoredMember) = .{
                    .items = &.{},
                    .capacity = 0,
                };
                errdefer dest_sorted.deinit(self.allocator);

                for (zset.sorted_list.items[u_start .. u_stop + 1]) |item| {
                    const owned_member = try self.allocator.dupe(u8, item.member);
                    errdefer self.allocator.free(owned_member);
                    try dest_members.put(owned_member, item.score);
                    const stored_key = dest_members.getKey(item.member).?;
                    try insertSortedMember(&dest_sorted, item.score, stored_key, self.allocator);
                }

                const member_count = dest_members.count();

                // Delete existing destination if it exists
                if (self.data.getEntry(dest)) |dest_entry| {
                    const dest_key = dest_entry.key_ptr.*;
                    var dest_value = dest_entry.value_ptr.*;
                    _ = self.data.remove(dest);
                    self.allocator.free(dest_key);
                    dest_value.deinit(self.allocator);
                }

                // Store new destination
                const owned_dest = try self.allocator.dupe(u8, dest);
                errdefer self.allocator.free(owned_dest);
                try self.data.put(owned_dest, Value{
                    .sorted_set = .{
                        .members = dest_members,
                        .sorted_list = dest_sorted,
                        .expires_at = null,
                    },
                });

                return member_count;
            },
            else => return error.WrongType,
        }
    }

    /// Count intersection of multiple sorted sets with optional limit
    /// Returns cardinality of intersection up to limit (0 = no limit)
    /// Returns error.WrongType if any key is not a sorted set
    pub fn zintercard(
        self: *Storage,
        allocator: std.mem.Allocator,
        keys: []const []const u8,
        limit: usize,
    ) !usize {
        _ = allocator;
        self.mutex.lock();
        defer self.mutex.unlock();

        if (keys.len == 0) return 0;

        // Get first set as baseline
        const first_entry = self.data.getEntry(keys[0]) orelse return 0;
        if (first_entry.value_ptr.isExpired(getCurrentTimestamp())) {
            return 0;
        }

        const first_zset = switch (first_entry.value_ptr.*) {
            .sorted_set => |*z| z,
            else => return error.WrongType,
        };

        var count: usize = 0;
        const effective_limit = if (limit == 0) std.math.maxInt(usize) else limit;

        // For each member in first set, check if it exists in all other sets
        for (first_zset.sorted_list.items) |item| {
            if (count >= effective_limit) break;

            var in_all = true;
            for (keys[1..]) |key| {
                const entry = self.data.getEntry(key) orelse {
                    in_all = false;
                    break;
                };

                if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
                    in_all = false;
                    break;
                }

                switch (entry.value_ptr.*) {
                    .sorted_set => |*zset| {
                        if (!zset.members.contains(item.member)) {
                            in_all = false;
                            break;
                        }
                    },
                    else => return error.WrongType,
                }
            }

            if (in_all) {
                count += 1;
            }
        }

        return count;
    }

    // ── String range operations ───────────────────────────────────────────────

    /// Get range of string value bytes. Negative indices supported.
    /// Returns empty string if key doesn't exist or range is out of bounds.
    /// Caller owns the returned slice and must free it.
    pub fn getrange(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        start: i64,
        end: i64,
    ) error{ WrongType, OutOfMemory }![]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse {
            return try allocator.dupe(u8, "");
        };

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return try allocator.dupe(u8, "");
        }

        switch (entry.value_ptr.*) {
            .string => |*sv| {
                const slen = @as(i64, @intCast(sv.data.len));

                // Normalize negative indices
                const norm_start: i64 = if (start < 0) @max(slen + start, 0) else @min(start, slen);
                const norm_end: i64 = if (end < 0) @max(slen + end, -1) else @min(end, slen - 1);

                if (norm_start > norm_end or norm_start >= slen) {
                    return try allocator.dupe(u8, "");
                }

                const s = @as(usize, @intCast(norm_start));
                const e = @as(usize, @intCast(norm_end)) + 1;
                return try allocator.dupe(u8, sv.data[s..e]);
            },
            else => return error.WrongType,
        }
    }

    /// Overwrite bytes of string at offset. Zero-pads if offset > len.
    /// Returns new total length.
    /// Returns error.WrongType if key is not a string.
    pub fn setrange(
        self: *Storage,
        key: []const u8,
        offset: usize,
        value: []const u8,
    ) error{ WrongType, OutOfMemory }!usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const required_len = offset + value.len;

        if (self.data.getEntry(key)) |entry| {
            if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
                const owned_key = entry.key_ptr.*;
                var old_value = entry.value_ptr.*;
                _ = self.removeKeyCleanup(key);
                self.allocator.free(owned_key);
                old_value.deinit(self.allocator);
                // Fall through to create new
            } else {
                switch (entry.value_ptr.*) {
                    .string => |*sv| {
                        const new_len = @max(sv.data.len, required_len);
                        // Allocate a mutable buffer to perform the overwrite
                        const new_buf = try self.allocator.alloc(u8, new_len);
                        @memcpy(new_buf[0..sv.data.len], sv.data);
                        if (new_len > sv.data.len) {
                            @memset(new_buf[sv.data.len..], 0);
                        }
                        if (value.len > 0) {
                            @memcpy(new_buf[offset .. offset + value.len], value);
                        }
                        self.allocator.free(sv.data);
                        sv.data = new_buf;
                        return sv.data.len;
                    },
                    else => return error.WrongType,
                }
            }
        }

        // Key doesn't exist: create zero-padded string
        const new_buf = try self.allocator.alloc(u8, required_len);
        errdefer self.allocator.free(new_buf);
        @memset(new_buf, 0);
        if (value.len > 0) {
            @memcpy(new_buf[offset..], value);
        }
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        try self.data.put(owned_key, Value{
            .string = .{ .data = new_buf, .expires_at = null },
        });
        return required_len;
    }

    // ── Bit operations ────────────────────────────────────────────────────────

    /// Set bit at offset to value (0 or 1)
    /// Returns the original bit value at that position
    pub fn setbit(
        self: *Storage,
        key: []const u8,
        offset: usize,
        value: u1,
    ) error{ WrongType, OutOfMemory }!u1 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const byte_offset = offset / 8;
        const bit_offset: u3 = @intCast(offset % 8);
        const required_len = byte_offset + 1;

        if (self.data.getEntry(key)) |entry| {
            if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
                const owned_key = entry.key_ptr.*;
                var old_value = entry.value_ptr.*;
                _ = self.removeKeyCleanup(key);
                self.allocator.free(owned_key);
                old_value.deinit(self.allocator);
                // Fall through to create new
            } else {
                switch (entry.value_ptr.*) {
                    .string => |*sv| {
                        // Expand if necessary
                        if (sv.data.len < required_len) {
                            const new_buf = try self.allocator.alloc(u8, required_len);
                            @memcpy(new_buf[0..sv.data.len], sv.data);
                            @memset(new_buf[sv.data.len..], 0);
                            self.allocator.free(sv.data);
                            sv.data = new_buf;
                        }

                        // Get original bit value
                        const byte = sv.data[byte_offset];
                        const original_bit: u1 = @intCast((byte >> (7 - bit_offset)) & 1);

                        // Set new bit value
                        const new_byte = if (value == 1)
                            byte | (@as(u8, 1) << (7 - bit_offset))
                        else
                            byte & ~(@as(u8, 1) << (7 - bit_offset));

                        // Modify in place (cast away const)
                        const mutable_data: []u8 = @constCast(sv.data);
                        mutable_data[byte_offset] = new_byte;

                        return original_bit;
                    },
                    else => return error.WrongType,
                }
            }
        }

        // Key doesn't exist: create zero-padded string
        const new_buf = try self.allocator.alloc(u8, required_len);
        errdefer self.allocator.free(new_buf);
        @memset(new_buf, 0);

        // Set the bit
        if (value == 1) {
            new_buf[byte_offset] = @as(u8, 1) << (7 - bit_offset);
        }

        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        try self.data.put(owned_key, Value{
            .string = .{ .data = new_buf, .expires_at = null },
        });
        return 0; // Original bit was 0
    }

    /// Get bit at offset (returns 0 or 1)
    pub fn getbit(
        self: *Storage,
        key: []const u8,
        offset: usize,
    ) error{WrongType}!u1 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const byte_offset = offset / 8;
        const bit_offset: u3 = @intCast(offset % 8);

        if (self.data.get(key)) |value| {
            if (value.isExpired(getCurrentTimestamp())) {
                return 0;
            }

            switch (value) {
                .string => |sv| {
                    if (byte_offset >= sv.data.len) {
                        return 0;
                    }
                    const byte = sv.data[byte_offset];
                    return @intCast((byte >> (7 - bit_offset)) & 1);
                },
                else => return error.WrongType,
            }
        }

        return 0; // Key doesn't exist
    }

    /// Count set bits (population count) in string
    /// unit: RangeUnit.byte (default) or RangeUnit.bit (Redis 7.0+)
    pub fn bitcount(
        self: *Storage,
        key: []const u8,
        start: ?i64,
        end: ?i64,
        unit: RangeUnit,
    ) error{WrongType}!i64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.data.get(key)) |value| {
            if (value.isExpired(getCurrentTimestamp())) {
                return 0;
            }

            switch (value) {
                .string => |sv| {
                    if (sv.data.len == 0) return 0;

                    const bit_len: i64 = @intCast(sv.data.len * 8);
                    const byte_len: i64 = @intCast(sv.data.len);

                    // Compute start/end as bit indices
                    const start_bit: i64 = if (unit == .bit) blk: {
                        if (start) |s| {
                            const s_norm = if (s < 0) bit_len + s else s;
                            break :blk @max(0, @min(s_norm, bit_len - 1));
                        }
                        break :blk 0;
                    } else blk: {
                        // BYTE mode: start is a byte index
                        if (start) |s| {
                            const s_norm = if (s < 0) byte_len + s else s;
                            break :blk @max(0, @min(s_norm, byte_len - 1)) * 8;
                        }
                        break :blk 0;
                    };

                    const end_bit: i64 = if (unit == .bit) blk: {
                        if (end) |e| {
                            const e_norm = if (e < 0) bit_len + e else e;
                            break :blk @max(0, @min(e_norm, bit_len - 1));
                        }
                        break :blk bit_len - 1;
                    } else blk: {
                        // BYTE mode: end is a byte index (inclusive)
                        if (end) |e| {
                            const e_norm = if (e < 0) byte_len + e else e;
                            break :blk (@max(0, @min(e_norm, byte_len - 1)) + 1) * 8 - 1;
                        }
                        break :blk bit_len - 1;
                    };

                    if (start_bit > end_bit) return 0;

                    var count: i64 = 0;

                    if (unit == .byte) {
                        // BYTE mode: start/end are byte-aligned — use fast @popCount path
                        const start_byte: usize = @intCast(@divTrunc(start_bit, 8));
                        const end_byte: usize = @intCast(@divTrunc(end_bit, 8));
                        for (sv.data[start_byte .. end_byte + 1]) |b| {
                            count += @popCount(b);
                        }
                    } else {
                        // BIT mode: count set bits one at a time (MSB-first ordering)
                        const start_u: usize = @intCast(start_bit);
                        const end_u: usize = @intCast(end_bit);
                        var bit_pos: usize = start_u;
                        while (bit_pos <= end_u) : (bit_pos += 1) {
                            const byte_idx = bit_pos / 8;
                            const bit_offset: u3 = @intCast(bit_pos % 8);
                            const current_bit: u1 = @intCast((sv.data[byte_idx] >> (7 - bit_offset)) & 1);
                            count += current_bit;
                        }
                    }
                    return count;
                },
                else => return error.WrongType,
            }
        }

        return 0; // Key doesn't exist
    }

    pub const BitOp = enum {
        AND,
        OR,
        XOR,
        NOT,
        DIFF, // Redis 8.2: Set difference (first & ~(OR of rest))
        DIFF1, // Redis 8.2: Optimized single-source difference
        ANDOR, // Redis 8.2: Pairwise (k1&k2)|(k3&k4)|...
        ONE, // Redis 8.2: Population count per byte position
    };

    /// Perform bitwise operation between strings
    /// Returns the length of the result string
    pub fn bitop(
        self: *Storage,
        operation: BitOp,
        destkey: []const u8,
        srckeys: []const []const u8,
    ) error{ WrongType, OutOfMemory }!usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = getCurrentTimestamp();

        // Collect source string data
        var max_len: usize = 0;
        var src_data = try self.allocator.alloc(?[]const u8, srckeys.len);
        defer self.allocator.free(src_data);

        for (srckeys, 0..) |srckey, i| {
            if (self.data.get(srckey)) |value| {
                if (value.isExpired(now)) {
                    src_data[i] = null;
                } else {
                    switch (value) {
                        .string => |sv| {
                            src_data[i] = sv.data;
                            max_len = @max(max_len, sv.data.len);
                        },
                        else => return error.WrongType,
                    }
                }
            } else {
                src_data[i] = null;
            }
        }

        // Allocate result buffer
        const result_len = if (operation == .NOT) blk: {
            if (srckeys.len != 1) return error.WrongType;
            break :blk if (src_data[0]) |data| data.len else 0;
        } else max_len;

        const result = try self.allocator.alloc(u8, result_len);
        errdefer self.allocator.free(result);
        @memset(result, 0);

        // Perform operation
        switch (operation) {
            .AND => {
                if (srckeys.len == 0) {
                    // No sources: result is empty
                } else {
                    @memset(result, 0xFF); // Start with all 1s
                    for (src_data) |src_opt| {
                        if (src_opt) |src| {
                            for (result, 0..) |*dest_byte, i| {
                                if (i < src.len) {
                                    dest_byte.* &= src[i];
                                } else {
                                    dest_byte.* = 0;
                                }
                            }
                        } else {
                            @memset(result, 0); // Missing key = all zeros
                            break;
                        }
                    }
                }
            },
            .OR => {
                for (src_data) |src_opt| {
                    if (src_opt) |src| {
                        for (result, 0..) |*dest_byte, i| {
                            if (i < src.len) {
                                dest_byte.* |= src[i];
                            }
                        }
                    }
                }
            },
            .XOR => {
                for (src_data) |src_opt| {
                    if (src_opt) |src| {
                        for (result, 0..) |*dest_byte, i| {
                            if (i < src.len) {
                                dest_byte.* ^= src[i];
                            }
                        }
                    }
                }
            },
            .NOT => {
                if (src_data[0]) |src| {
                    for (result, 0..) |*dest_byte, i| {
                        dest_byte.* = ~src[i];
                    }
                }
            },
            .DIFF => {
                // Set difference: first & ~(OR of rest)
                if (srckeys.len < 2) {
                    // DIFF requires at least 2 keys
                    return error.WrongType;
                }
                // Start with first key
                if (src_data[0]) |src| {
                    for (result, 0..) |*dest_byte, i| {
                        if (i < src.len) {
                            dest_byte.* = src[i];
                        }
                    }
                }
                // OR all remaining keys and NOT the result
                const or_result = try self.allocator.alloc(u8, result_len);
                defer self.allocator.free(or_result);
                @memset(or_result, 0);
                for (src_data[1..]) |src_opt| {
                    if (src_opt) |src| {
                        for (or_result, 0..) |*or_byte, i| {
                            if (i < src.len) {
                                or_byte.* |= src[i];
                            }
                        }
                    }
                }
                // AND with NOT of OR result
                for (result, 0..) |*dest_byte, i| {
                    dest_byte.* &= ~or_result[i];
                }
            },
            .DIFF1 => {
                // Optimized for single source difference: key1 & ~key2
                if (srckeys.len != 2) {
                    return error.WrongType;
                }
                if (src_data[0]) |src1| {
                    if (src_data[1]) |src2| {
                        for (result, 0..) |*dest_byte, i| {
                            const byte1 = if (i < src1.len) src1[i] else 0;
                            const byte2 = if (i < src2.len) src2[i] else 0;
                            dest_byte.* = byte1 & ~byte2;
                        }
                    } else {
                        // src2 doesn't exist, result is src1
                        for (result, 0..) |*dest_byte, i| {
                            if (i < src1.len) {
                                dest_byte.* = src1[i];
                            }
                        }
                    }
                }
            },
            .ANDOR => {
                // Pairwise AND-OR: (k1&k2)|(k3&k4)|...
                if (srckeys.len % 2 != 0) {
                    return error.WrongType; // Must have even number of keys
                }
                var i: usize = 0;
                while (i < srckeys.len) : (i += 2) {
                    const src1_opt = src_data[i];
                    const src2_opt = src_data[i + 1];
                    if (src1_opt != null and src2_opt != null) {
                        const src1 = src1_opt.?;
                        const src2 = src2_opt.?;
                        for (result, 0..) |*dest_byte, j| {
                            const byte1 = if (j < src1.len) src1[j] else 0;
                            const byte2 = if (j < src2.len) src2[j] else 0;
                            dest_byte.* |= (byte1 & byte2);
                        }
                    }
                }
            },
            .ONE => {
                // Population count: count of 1-bits at each byte position across all sources
                for (result, 0..) |*dest_byte, i| {
                    var count: u8 = 0;
                    for (src_data) |src_opt| {
                        if (src_opt) |src| {
                            if (i < src.len) {
                                // Count 1-bits in this byte
                                count +%= @popCount(src[i]);
                            }
                        }
                    }
                    dest_byte.* = count;
                }
            },
        }

        // Store result
        if (self.data.getEntry(destkey)) |entry| {
            if (entry.value_ptr.isExpired(now)) {
                const owned_key = entry.key_ptr.*;
                var old_value = entry.value_ptr.*;
                _ = self.data.remove(destkey);
                self.allocator.free(owned_key);
                old_value.deinit(self.allocator);
            } else {
                var old_value = entry.value_ptr.*;
                old_value.deinit(self.allocator);
                entry.value_ptr.* = Value{
                    .string = .{ .data = result, .expires_at = null },
                };
                return result_len;
            }
        }

        // Create new entry
        const owned_key = try self.allocator.dupe(u8, destkey);
        errdefer self.allocator.free(owned_key);
        try self.data.put(owned_key, Value{
            .string = .{ .data = result, .expires_at = null },
        });

        return result_len;
    }

    /// Find first bit set to 0 or 1 in a string
    /// bit: 0 or 1 to search for
    /// Find first bit set to 0 or 1 in a string
    /// key: The key holding the string value
    /// bit: 0 or 1 - the bit value to search for
    /// start: Start position (byte or bit index depending on unit)
    /// end: End position (byte or bit index depending on unit)
    /// unit: RangeUnit.byte (default) or RangeUnit.bit (Redis 7.0+)
    /// Returns bit position (0-based, absolute) or -1 if not found
    pub fn bitpos(
        self: *Storage,
        key: []const u8,
        bit: u1,
        start: ?i64,
        end: ?i64,
        unit: RangeUnit,
    ) error{WrongType}!i64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.data.get(key)) |value| {
            if (value.isExpired(getCurrentTimestamp())) {
                // Expired key - treat as non-existent
                // If searching for 0, first bit (position 0) is 0
                // If searching for 1, not found
                return if (bit == 0) 0 else -1;
            }

            switch (value) {
                .string => |sv| {
                    if (sv.data.len == 0) {
                        return if (bit == 0) 0 else -1;
                    }

                    // Convert to byte-level indices regardless of unit
                    const bit_len: i64 = @intCast(sv.data.len * 8);
                    const byte_len: i64 = @intCast(sv.data.len);

                    var start_bit: i64 = 0;
                    var end_bit: i64 = bit_len - 1;
                    var has_explicit_end: bool = false;

                    if (unit == .bit) {
                        // BIT mode: start/end are bit indices
                        if (start) |s| {
                            const s_norm = if (s < 0) bit_len + s else s;
                            start_bit = @max(0, @min(s_norm, bit_len - 1));
                        }
                        if (end) |e| {
                            const e_norm = if (e < 0) bit_len + e else e;
                            end_bit = @max(0, @min(e_norm, bit_len - 1));
                            has_explicit_end = true;
                        } else if (start != null) {
                            // Only start specified, end is end of string
                            end_bit = bit_len - 1;
                        }
                    } else {
                        // BYTE mode: start/end are byte indices
                        if (start) |s| {
                            const s_norm = if (s < 0) byte_len + s else s;
                            start_bit = @max(0, @min(s_norm, byte_len - 1)) * 8;
                        }
                        if (end) |e| {
                            const e_norm = if (e < 0) byte_len + e else e;
                            end_bit = (@max(0, @min(e_norm, byte_len - 1)) + 1) * 8 - 1;
                            has_explicit_end = true;
                        } else if (start != null) {
                            // Only start specified, end is end of string
                            end_bit = bit_len - 1;
                        }
                    }

                    // After normalization, convert to unsigned for the search loop
                    const start_bit_u: usize = @intCast(@max(0, start_bit));
                    const end_bit_u: usize = @intCast(@max(0, @min(end_bit, bit_len - 1)));

                    if (start_bit_u > end_bit_u) {
                        return -1;
                    }

                    // Search bit by bit within range (unsigned arithmetic - no overflow risk)
                    var bit_pos: usize = start_bit_u;
                    while (bit_pos <= end_bit_u) : (bit_pos += 1) {
                        const byte_idx = bit_pos / 8; // No cast needed
                        const bit_offset: u3 = @intCast(bit_pos % 8); // Safe: 0-7
                        // Redis uses MSB-first bit ordering: bit 0 is MSB of byte 0
                        const current_bit: u1 = @intCast((sv.data[byte_idx] >> (7 - bit_offset)) & 1);

                        if (current_bit == bit) {
                            return @intCast(bit_pos); // Convert back to i64 for return
                        }
                    }

                    // Not found in range
                    // Special case: if searching for 0 and end is not specified,
                    // return position after last byte (padding behavior)
                    if (bit == 0 and !has_explicit_end) {
                        return bit_len;
                    }

                    return -1;
                },
                else => return error.WrongType,
            }
        }

        // Key doesn't exist
        // If searching for 0, first bit (position 0) is 0
        // If searching for 1, not found
        return if (bit == 0) 0 else -1;
    }

    // ── Stream operations ─────────────────────────────────────────────────────

    /// Add entry to stream with auto-generated or explicit ID.
    /// Returns the assigned StreamId or null if NOMKSTREAM and key doesn't exist.
    /// Other errors indicate invalid IDs or type mismatches.
    pub fn xadd(
        self: *Storage,
        key: []const u8,
        id_str: []const u8,
        fields: []const []const u8,
        expires_at: ?i64,
        opts: XAddOptions,
    ) error{ WrongType, OutOfMemory, InvalidStreamId, StreamIdTooSmall, Overflow, InvalidCharacter }!?Value.StreamId {
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = getCurrentTimestamp();

        // Get or create stream
        const entry = try self.data.getOrPut(key);
        if (!entry.found_existing) {
            // Stream doesn't exist
            if (opts.nomkstream) {
                // NOMKSTREAM: return null without creating
                return null;
            }

            const owned_key = try self.allocator.dupe(u8, key);
            entry.key_ptr.* = owned_key;
            entry.value_ptr.* = Value{
                .stream = .{
                    .entries = std.ArrayList(Value.StreamEntry){},
                    .last_id = null,
                    .expires_at = expires_at,
                    .consumer_groups = std.StringHashMap(Value.ConsumerGroup).init(self.allocator),
                    .entries_added = 0,
                    .max_deleted_entry_id = .{ .ms = 0, .seq = 0 },
                },
            };
        } else {
            // Check expiration
            if (entry.value_ptr.isExpired(now)) {
                var value = entry.value_ptr.*;
                value.deinit(self.allocator);
                entry.value_ptr.* = Value{
                    .stream = .{
                        .entries = std.ArrayList(Value.StreamEntry){},
                        .last_id = null,
                        .expires_at = expires_at,
                        .consumer_groups = std.StringHashMap(Value.ConsumerGroup).init(self.allocator),
                        .entries_added = 0,
                        .max_deleted_entry_id = .{ .ms = 0, .seq = 0 },
                    },
                };
            }
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                // Parse and validate ID
                const id = try Value.StreamId.parse(id_str, stream_val.last_id);

                // Validate ID is greater than last_id
                if (stream_val.last_id) |last| {
                    if (!id.lessThan(last) and !id.equals(last)) {
                        // ok
                    } else {
                        return error.StreamIdTooSmall;
                    }
                }

                // Create owned copies of fields
                var owned_fields = try std.ArrayList([]const u8).initCapacity(self.allocator, fields.len);
                errdefer {
                    for (owned_fields.items) |f| self.allocator.free(f);
                    owned_fields.deinit(self.allocator);
                }

                for (fields) |field| {
                    const owned = try self.allocator.dupe(u8, field);
                    try owned_fields.append(self.allocator, owned);
                }

                // Add entry
                try stream_val.entries.append(self.allocator, .{
                    .id = id,
                    .fields = owned_fields,
                });
                stream_val.last_id = id;
                stream_val.entries_added += 1;

                // Apply trimming if requested
                if (opts.maxlen) |maxlen| {
                    _ = try self.xtrimByMaxlen(key, maxlen, opts.limit);
                }
                if (opts.minid_str) |minid_str| {
                    _ = try self.xtrimByMinId(key, minid_str, opts.limit);
                }

                return id;
            },
            else => return error.WrongType,
        }
    }

    /// Get stream length (number of entries).
    /// Returns null if key doesn't exist, error.WrongType if not a stream.
    pub fn xlen(self: *Storage, key: []const u8) error{WrongType}!?usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| return stream_val.entries.items.len,
            else => return error.WrongType,
        }
    }

    /// Query range of stream entries by ID.
    /// Returns slice of (id, fields) tuples. Caller must free.
    /// start/end can be "-" (min) or "+" (max) or specific IDs.
    pub fn xrange(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        start_str: []const u8,
        end_str: []const u8,
        count: ?usize,
    ) error{ WrongType, OutOfMemory, InvalidStreamId, Overflow, InvalidCharacter }!?[]const Value.StreamEntry {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                // Parse start/end
                const start_id = if (std.mem.eql(u8, start_str, "-"))
                    Value.StreamId{ .ms = std.math.minInt(i64), .seq = 0 }
                else
                    try Value.StreamId.parse(start_str, null);

                const end_id = if (std.mem.eql(u8, end_str, "+"))
                    Value.StreamId{ .ms = std.math.maxInt(i64), .seq = std.math.maxInt(u64) }
                else
                    try Value.StreamId.parse(end_str, null);

                // Filter entries in range - just reference existing entries
                var result = std.ArrayList(Value.StreamEntry){};
                defer result.deinit(allocator);

                for (stream_val.entries.items) |entry_item| {
                    if ((start_id.lessThan(entry_item.id) or start_id.equals(entry_item.id)) and
                        (entry_item.id.lessThan(end_id) or entry_item.id.equals(end_id)))
                    {
                        try result.append(allocator, entry_item);
                        if (count) |c| {
                            if (result.items.len >= c) break;
                        }
                    }
                }

                const owned_slice = try result.toOwnedSlice(allocator);
                return owned_slice;
            },
            else => return error.WrongType,
        }
    }

    /// Query range of stream entries in reverse order (newest to oldest).
    /// Returns slice of entries. Caller must free.
    /// start/end can be "+" (max) or "-" (min) or specific IDs.
    pub fn xrevrange(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        start_str: []const u8,
        end_str: []const u8,
        count: ?usize,
    ) error{ WrongType, OutOfMemory, InvalidStreamId, Overflow, InvalidCharacter }!?[]const Value.StreamEntry {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                // Parse start/end (note: start is max, end is min in XREVRANGE)
                const start_id = if (std.mem.eql(u8, start_str, "+"))
                    Value.StreamId{ .ms = std.math.maxInt(i64), .seq = std.math.maxInt(u64) }
                else
                    try Value.StreamId.parse(start_str, null);

                const end_id = if (std.mem.eql(u8, end_str, "-"))
                    Value.StreamId{ .ms = std.math.minInt(i64), .seq = 0 }
                else
                    try Value.StreamId.parse(end_str, null);

                // Filter entries in range (reverse order)
                var result = std.ArrayList(Value.StreamEntry){};
                defer result.deinit(allocator);

                var i = stream_val.entries.items.len;
                while (i > 0) {
                    i -= 1;
                    const entry_item = stream_val.entries.items[i];
                    if ((end_id.lessThan(entry_item.id) or end_id.equals(entry_item.id)) and
                        (entry_item.id.lessThan(start_id) or entry_item.id.equals(start_id)))
                    {
                        try result.append(allocator, entry_item);
                        if (count) |c| {
                            if (result.items.len >= c) break;
                        }
                    }
                }

                const owned_slice = try result.toOwnedSlice(allocator);
                return owned_slice;
            },
            else => return error.WrongType,
        }
    }

    /// Delete specific entries from stream by ID.
    /// Returns number of entries deleted.
    pub fn xdel(
        self: *Storage,
        key: []const u8,
        ids: []const []const u8,
    ) error{ WrongType, OutOfMemory, InvalidStreamId, Overflow, InvalidCharacter }!usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return 0;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return 0;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                // Parse IDs to delete
                var target_ids = try std.ArrayList(Value.StreamId).initCapacity(self.allocator, ids.len);
                defer target_ids.deinit(self.allocator);

                for (ids) |id_str| {
                    const id = try Value.StreamId.parse(id_str, null);
                    try target_ids.append(self.allocator, id);
                }

                // Remove matching entries
                var deleted: usize = 0;
                var i: usize = 0;
                while (i < stream_val.entries.items.len) {
                    const entry_id = stream_val.entries.items[i].id;
                    var should_delete = false;
                    for (target_ids.items) |target| {
                        if (entry_id.equals(target)) {
                            should_delete = true;
                            break;
                        }
                    }

                    if (should_delete) {
                        var removed = stream_val.entries.orderedRemove(i);
                        removed.deinit(self.allocator);
                        deleted += 1;
                    } else {
                        i += 1;
                    }
                }

                return deleted;
            },
            else => return error.WrongType,
        }
    }

    /// Trim stream to at most maxlen entries (delete oldest entries).
    /// Returns number of entries deleted. Does not respect limit when deleting.
    fn xtrimByMaxlen(
        self: *Storage,
        key: []const u8,
        maxlen: usize,
        limit: ?usize,
    ) !usize {
        const entry = self.data.getEntry(key) orelse return 0;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return 0;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                const current_len = stream_val.entries.items.len;
                if (current_len <= maxlen) return 0;

                var to_delete = current_len - maxlen;
                if (limit) |lim| {
                    to_delete = @min(to_delete, lim);
                }

                // Delete oldest entries
                for (0..to_delete) |_| {
                    var removed = stream_val.entries.orderedRemove(0);
                    removed.deinit(self.allocator);
                }

                return to_delete;
            },
            else => return error.WrongType,
        }
    }

    /// Trim stream to remove entries with ID strictly less than minid.
    /// Returns number of entries deleted.
    /// This is private and should only be called from xadd() which holds the lock.
    fn xtrimByMinId(
        self: *Storage,
        key: []const u8,
        minid_str: []const u8,
        limit: ?usize,
    ) !usize {
        const minid = Value.StreamId.parse(minid_str, null) catch return error.InvalidStreamId;

        const entry = self.data.getEntry(key) orelse return 0;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.removeKeyCleanup(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return 0;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                var deleted: usize = 0;
                var i: usize = 0;

                // Find the first entry with ID >= minid
                while (i < stream_val.entries.items.len and stream_val.entries.items[i].id.lessThan(minid)) {
                    i += 1;
                    deleted += 1;

                    // Respect LIMIT if provided
                    if (limit) |lim| {
                        if (deleted >= lim) break;
                    }
                }

                // Delete entries from beginning up to index i
                for (0..i) |_| {
                    var removed = stream_val.entries.orderedRemove(0);
                    removed.deinit(self.allocator);
                }

                return deleted;
            },
            else => return error.WrongType,
        }
    }

    /// Trim stream to approximately maxlen entries (using MAXLEN strategy).
    /// Returns number of entries deleted.
    /// Public wrapper for backward compatibility and command dispatch.
    pub fn xtrim(
        self: *Storage,
        key: []const u8,
        maxlen: usize,
    ) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        return try self.xtrimByMaxlen(key, maxlen, null);
    }

    /// Public wrapper for xtrimByMaxlen with mutex.
    /// Returns number of entries deleted.
    pub fn xtrimMaxlen(
        self: *Storage,
        key: []const u8,
        maxlen: usize,
        limit: ?usize,
    ) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        return try self.xtrimByMaxlen(key, maxlen, limit);
    }

    /// Public wrapper for xtrimByMinId with mutex.
    /// Returns number of entries deleted.
    pub fn xtrimMinid(
        self: *Storage,
        key: []const u8,
        minid_str: []const u8,
        limit: ?usize,
    ) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        return try self.xtrimByMinId(key, minid_str, limit);
    }

    /// Create a consumer group for a stream
    pub fn xgroupCreate(
        self: *Storage,
        key: []const u8,
        group_name: []const u8,
        id_str: []const u8,
    ) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return error.NoKey;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            return error.NoKey;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                // Check if group already exists
                if (stream_val.consumer_groups.contains(group_name)) {
                    return error.GroupExists;
                }

                // Parse starting ID
                const starting_id = if (std.mem.eql(u8, id_str, "$"))
                    stream_val.last_id orelse Value.StreamId{ .ms = 0, .seq = 0 }
                else if (std.mem.eql(u8, id_str, "0"))
                    Value.StreamId{ .ms = 0, .seq = 0 }
                else
                    try Value.StreamId.parse(id_str, null);

                // Determine if this is an arbitrary start (affects lag calculation)
                // Arbitrary = not "0-0", not stream's first entry, not stream's last entry ("$")
                const is_arbitrary = blk: {
                    if (std.mem.eql(u8, id_str, "0")) break :blk false;
                    if (std.mem.eql(u8, id_str, "$")) break :blk false;
                    break :blk true;
                };

                // Calculate initial entries_read based on starting position
                const initial_entries_read: u64 = if (std.mem.eql(u8, id_str, "$"))
                    stream_val.entries_added
                else
                    0;

                // Create consumer group
                const owned_name = try self.allocator.dupe(u8, group_name);
                errdefer self.allocator.free(owned_name);

                const now_ms = std.time.milliTimestamp();

                try stream_val.consumer_groups.put(owned_name, Value.ConsumerGroup{
                    .name = owned_name,
                    .last_delivered_id = starting_id,
                    .consumers = std.StringHashMap(Value.Consumer).init(self.allocator),
                    .pending = std.ArrayList(Value.PendingEntry){},
                    .entries_read = initial_entries_read,
                    .creation_time = now_ms,
                    .arbitrary_start = is_arbitrary,
                });
            },
            else => return error.WrongType,
        }
    }

    /// Destroy a consumer group
    pub fn xgroupDestroy(
        self: *Storage,
        key: []const u8,
        group_name: []const u8,
    ) !bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return false;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            return false;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                if (stream_val.consumer_groups.fetchRemove(group_name)) |kv| {
                    var group = kv.value;
                    group.deinit(self.allocator);
                    return true;
                }
                return false;
            },
            else => return error.WrongType,
        }
    }

    /// Set the last delivered ID for a consumer group
    pub fn xgroupSetId(
        self: *Storage,
        key: []const u8,
        group_name: []const u8,
        id_str: []const u8,
        entries_read: ?u64,
    ) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return error.NoKey;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            return error.NoKey;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                const group_ptr = stream_val.consumer_groups.getPtr(group_name) orelse return error.NoGroup;

                const new_id = if (std.mem.eql(u8, id_str, "$"))
                    stream_val.last_id orelse Value.StreamId{ .ms = 0, .seq = 0 }
                else
                    try Value.StreamId.parse(id_str, null);

                group_ptr.last_delivered_id = new_id;

                if (entries_read) |er| {
                    // ENTRIESREAD provided: set counter and enable accurate lag calculation
                    group_ptr.entries_read = er;
                    group_ptr.arbitrary_start = false;
                } else {
                    // No ENTRIESREAD: mark as arbitrary if not "0" or "$"
                    if (!std.mem.eql(u8, id_str, "0") and !std.mem.eql(u8, id_str, "$")) {
                        group_ptr.arbitrary_start = true;
                    }
                }
            },
            else => return error.WrongType,
        }
    }

    /// Create a consumer in a consumer group
    /// Returns true if consumer was created, false if it already exists
    pub fn xgroupCreateConsumer(
        self: *Storage,
        key: []const u8,
        group_name: []const u8,
        consumer_name: []const u8,
    ) !bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return error.NoKey;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            return error.NoKey;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                const group_ptr = stream_val.consumer_groups.getPtr(group_name) orelse return error.NoGroup;

                // Check if consumer already exists
                if (group_ptr.consumers.contains(consumer_name)) {
                    return false;
                }

                // Create consumer
                const owned_consumer_name = try self.allocator.dupe(u8, consumer_name);
                errdefer self.allocator.free(owned_consumer_name);

                const now_ms = std.time.milliTimestamp();

                try group_ptr.consumers.put(owned_consumer_name, Value.Consumer{
                    .name = owned_consumer_name,
                    .pending = std.ArrayList(Value.StreamId){},
                    .last_attempted_time = now_ms,
                    .last_successful_time = now_ms,
                    .creation_time = now_ms,
                });

                return true;
            },
            else => return error.WrongType,
        }
    }

    /// Delete a consumer from a consumer group
    /// Returns the number of pending messages the consumer had (always 0 or more)
    pub fn xgroupDelConsumer(
        self: *Storage,
        key: []const u8,
        group_name: []const u8,
        consumer_name: []const u8,
    ) !i64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return error.NoKey;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            return error.NoKey;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                const group_ptr = stream_val.consumer_groups.getPtr(group_name) orelse return error.NoGroup;

                // Remove consumer if exists
                if (group_ptr.consumers.fetchRemove(consumer_name)) |kv| {
                    var consumer = kv.value;
                    const pending_count: i64 = @intCast(consumer.pending.items.len);

                    // Remove all pending entries for this consumer from the group's pending list
                    var i: usize = 0;
                    while (i < group_ptr.pending.items.len) {
                        if (std.mem.eql(u8, group_ptr.pending.items[i].consumer, consumer_name)) {
                            var removed_entry = group_ptr.pending.orderedRemove(i);
                            removed_entry.deinit(self.allocator);
                        } else {
                            i += 1;
                        }
                    }

                    consumer.deinit(self.allocator);
                    return pending_count;
                }

                return error.NoConsumer;
            },
            else => return error.WrongType,
        }
    }

    /// Read from a stream for a consumer group
    pub fn xreadgroup(
        self: *Storage,
        allocator: std.mem.Allocator,
        group_name: []const u8,
        consumer_name: []const u8,
        key: []const u8,
        id_str: []const u8,
        count: ?usize,
        noack: bool,
    ) !?std.ArrayList(Value.StreamEntry) {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            return null;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                const group_ptr = stream_val.consumer_groups.getPtr(group_name) orelse return error.NoGroup;

                const now_ms = std.time.milliTimestamp();

                // Get or create consumer
                const consumer_entry = try group_ptr.consumers.getOrPut(consumer_name);
                if (!consumer_entry.found_existing) {
                    const owned_consumer_name = try self.allocator.dupe(u8, consumer_name);
                    consumer_entry.key_ptr.* = owned_consumer_name;
                    consumer_entry.value_ptr.* = Value.Consumer{
                        .name = owned_consumer_name,
                        .pending = std.ArrayList(Value.StreamId){},
                        .last_attempted_time = now_ms,
                        .last_successful_time = now_ms,
                        .creation_time = now_ms,
                    };
                }

                // Update last_attempted_time for every XREADGROUP call
                consumer_entry.value_ptr.last_attempted_time = now_ms;

                // Determine starting ID
                const start_id = if (std.mem.eql(u8, id_str, ">")) blk: {
                    // Read new messages (greater than last delivered)
                    break :blk group_ptr.last_delivered_id;
                } else if (std.mem.eql(u8, id_str, "0") or std.mem.eql(u8, id_str, "0-0")) {
                    // Read pending messages for this consumer
                    // For now, we'll return an empty list as pending message delivery is complex
                    return std.ArrayList(Value.StreamEntry){};
                } else blk: {
                    break :blk try Value.StreamId.parse(id_str, null);
                };

                // Collect entries
                var result = std.ArrayList(Value.StreamEntry){};
                errdefer result.deinit(allocator);

                var collected: usize = 0;
                for (stream_val.entries.items) |*entry_item| {
                    if (count) |max| {
                        if (collected >= max) break;
                    }

                    if (entry_item.id.lessThan(start_id) or entry_item.id.equals(start_id)) {
                        continue;
                    }

                    // Clone entry
                    var cloned_fields = std.ArrayList([]const u8){};
                    for (entry_item.fields.items) |field| {
                        const owned_field = try allocator.dupe(u8, field);
                        try cloned_fields.append(allocator, owned_field);
                    }

                    try result.append(allocator, Value.StreamEntry{
                        .id = entry_item.id,
                        .fields = cloned_fields,
                    });

                    collected += 1;

                    // Update last delivered ID
                    group_ptr.last_delivered_id = entry_item.id;

                    // Add to pending unless NOACK
                    if (!noack) {
                        try consumer_entry.value_ptr.pending.append(self.allocator, entry_item.id);
                        try group_ptr.pending.append(self.allocator, Value.PendingEntry{
                            .id = entry_item.id,
                            .consumer = try self.allocator.dupe(u8, consumer_name),
                            .delivery_time = std.time.milliTimestamp(),
                            .delivery_count = 1,
                        });
                    }
                }

                if (result.items.len == 0) {
                    result.deinit(allocator);
                    return null;
                }

                // Update last_successful_time and entries_read when messages were delivered
                consumer_entry.value_ptr.last_successful_time = now_ms;
                group_ptr.entries_read += result.items.len;

                return result;
            },
            else => return error.WrongType,
        }
    }

    /// Acknowledge messages in a consumer group
    pub fn xack(
        self: *Storage,
        key: []const u8,
        group_name: []const u8,
        ids: []const []const u8,
    ) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return 0;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            return 0;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                const group_ptr = stream_val.consumer_groups.getPtr(group_name) orelse return 0;

                var acked: usize = 0;

                for (ids) |id_str| {
                    const id = Value.StreamId.parse(id_str, null) catch continue;

                    // Find and remove from pending list
                    var i: usize = 0;
                    while (i < group_ptr.pending.items.len) : (i += 1) {
                        if (group_ptr.pending.items[i].id.equals(id)) {
                            var removed = group_ptr.pending.orderedRemove(i);
                            removed.deinit(self.allocator);

                            // Also remove from consumer's pending list
                            if (group_ptr.consumers.getPtr(removed.consumer)) |consumer_ptr| {
                                var j: usize = 0;
                                while (j < consumer_ptr.pending.items.len) : (j += 1) {
                                    if (consumer_ptr.pending.items[j].equals(id)) {
                                        _ = consumer_ptr.pending.orderedRemove(j);
                                        break;
                                    }
                                }
                            }

                            acked += 1;
                            break;
                        }
                    }
                }

                return acked;
            },
            else => return error.WrongType,
        }
    }

    /// Check if an entry is acknowledged by ALL consumer groups
    /// Returns true if entry is not in any group's pending list
    fn checkAllGroupsAcknowledged(
        stream_val: *Value.StreamValue,
        id: Value.StreamId,
    ) bool {
        // If no consumer groups exist, return false (cannot verify)
        if (stream_val.consumer_groups.count() == 0) {
            return false;
        }

        // Check if ID exists in ANY group's pending list
        var it = stream_val.consumer_groups.valueIterator();
        while (it.next()) |group| {
            for (group.pending.items) |pending| {
                if (pending.id.equals(id)) {
                    return false; // Found in pending, not fully acknowledged
                }
            }
        }

        return true; // Not found in any pending list
    }

    /// Remove entry ID from all consumer groups' pending lists
    /// Used by DELREF mode for aggressive cleanup
    fn removeFromAllPendingLists(
        self: *Storage,
        stream_val: *Value.StreamValue,
        id: Value.StreamId,
    ) void {
        var group_it = stream_val.consumer_groups.valueIterator();
        while (group_it.next()) |group| {
            // Remove from group pending
            var i: usize = 0;
            while (i < group.pending.items.len) {
                if (group.pending.items[i].id.equals(id)) {
                    var removed = group.pending.orderedRemove(i);
                    removed.deinit(self.allocator);
                    // Don't increment i, next item shifted down
                } else {
                    i += 1;
                }
            }

            // Remove from all consumer pendings
            var consumer_it = group.consumers.valueIterator();
            while (consumer_it.next()) |consumer| {
                var j: usize = 0;
                while (j < consumer.pending.items.len) {
                    if (consumer.pending.items[j].equals(id)) {
                        _ = consumer.pending.orderedRemove(j);
                    } else {
                        j += 1;
                    }
                }
            }
        }
    }

    /// Acknowledge entries in a consumer group and conditionally delete from stream
    /// Returns array of status codes (1=deleted, -1=not found, 2=not deleted)
    pub fn xackdel(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        group_name: []const u8,
        ids: []const []const u8,
        mode: XRefMode,
    ) ![]i8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var result = try std.ArrayList(i8).initCapacity(allocator, ids.len);
        errdefer result.deinit(allocator);

        const entry = self.data.getEntry(key) orelse {
            // Stream doesn't exist - return -1 for all IDs
            for (ids) |_| {
                try result.append(allocator, -1);
            }
            return try result.toOwnedSlice(allocator);
        };

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            for (ids) |_| {
                try result.append(allocator, -1);
            }
            return try result.toOwnedSlice(allocator);
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                const group_ptr = stream_val.consumer_groups.getPtr(group_name) orelse {
                    return error.NoGroup;
                };

                for (ids) |id_str| {
                    const id = Value.StreamId.parse(id_str, null) catch {
                        try result.append(allocator, -1);
                        continue;
                    };

                    // First, acknowledge the entry (remove from this group's PEL)
                    var acked_in_group = false;
                    var i: usize = 0;
                    while (i < group_ptr.pending.items.len) {
                        if (group_ptr.pending.items[i].id.equals(id)) {
                            var removed = group_ptr.pending.orderedRemove(i);
                            const consumer_name = removed.consumer;

                            // Also remove from consumer's pending list
                            if (group_ptr.consumers.getPtr(consumer_name)) |consumer_ptr| {
                                var j: usize = 0;
                                while (j < consumer_ptr.pending.items.len) {
                                    if (consumer_ptr.pending.items[j].equals(id)) {
                                        _ = consumer_ptr.pending.orderedRemove(j);
                                        break;
                                    } else {
                                        j += 1;
                                    }
                                }
                            }

                            removed.deinit(self.allocator);
                            acked_in_group = true;
                            break;
                        } else {
                            i += 1;
                        }
                    }

                    // Now handle deletion based on mode
                    switch (mode) {
                        .keepref => {
                            // Delete from stream, preserve PEL refs in other groups
                            var found_in_stream = false;
                            var k: usize = 0;
                            while (k < stream_val.entries.items.len) {
                                if (stream_val.entries.items[k].id.equals(id)) {
                                    var removed_entry = stream_val.entries.orderedRemove(k);
                                    removed_entry.deinit(self.allocator);
                                    found_in_stream = true;
                                    break;
                                } else {
                                    k += 1;
                                }
                            }
                            try result.append(allocator, if (found_in_stream) 1 else -1);
                        },
                        .delref => {
                            // Delete from stream AND remove all PEL refs
                            var found_in_stream = false;
                            var k: usize = 0;
                            while (k < stream_val.entries.items.len) {
                                if (stream_val.entries.items[k].id.equals(id)) {
                                    var removed_entry = stream_val.entries.orderedRemove(k);
                                    removed_entry.deinit(self.allocator);
                                    found_in_stream = true;
                                    break;
                                } else {
                                    k += 1;
                                }
                            }

                            // Remove from ALL group PELs (even if not in stream)
                            removeFromAllPendingLists(self, stream_val, id);

                            try result.append(allocator, if (found_in_stream) 1 else -1);
                        },
                        .acked => {
                            // Only delete if ALL groups acknowledged
                            if (checkAllGroupsAcknowledged(stream_val, id)) {
                                // All groups acknowledged - safe to delete
                                var found_in_stream = false;
                                var k: usize = 0;
                                while (k < stream_val.entries.items.len) {
                                    if (stream_val.entries.items[k].id.equals(id)) {
                                        var removed_entry = stream_val.entries.orderedRemove(k);
                                        removed_entry.deinit(self.allocator);
                                        found_in_stream = true;
                                        break;
                                    } else {
                                        k += 1;
                                    }
                                }
                                try result.append(allocator, if (found_in_stream) 1 else -1);
                            } else {
                                // Not all groups acknowledged - don't delete, return 2
                                try result.append(allocator, 2);
                            }
                        },
                    }
                }

                return try result.toOwnedSlice(allocator);
            },
            else => return error.WrongType,
        }
    }

    /// Delete stream entries with consumer group reference control
    /// Returns array of status codes (1=deleted, -1=not found, 2=not deleted)
    pub fn xdelex(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        ids: []const []const u8,
        mode: XRefMode,
    ) ![]i8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var result = try std.ArrayList(i8).initCapacity(allocator, ids.len);
        errdefer result.deinit(allocator);

        const entry = self.data.getEntry(key) orelse {
            // Stream doesn't exist - return -1 for all IDs
            for (ids) |_| {
                try result.append(allocator, -1);
            }
            return try result.toOwnedSlice(allocator);
        };

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            for (ids) |_| {
                try result.append(allocator, -1);
            }
            return try result.toOwnedSlice(allocator);
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                for (ids) |id_str| {
                    const id = Value.StreamId.parse(id_str, null) catch {
                        try result.append(allocator, -1);
                        continue;
                    };

                    switch (mode) {
                        .keepref => {
                            // Traditional XDEL behavior - delete from stream, preserve PEL refs
                            var found_in_stream = false;
                            var i: usize = 0;
                            while (i < stream_val.entries.items.len) {
                                if (stream_val.entries.items[i].id.equals(id)) {
                                    var removed_entry = stream_val.entries.orderedRemove(i);
                                    removed_entry.deinit(self.allocator);
                                    found_in_stream = true;
                                    break;
                                } else {
                                    i += 1;
                                }
                            }
                            try result.append(allocator, if (found_in_stream) 1 else -1);
                        },
                        .delref => {
                            // Delete from stream AND remove all PEL refs
                            var found_in_stream = false;
                            var i: usize = 0;
                            while (i < stream_val.entries.items.len) {
                                if (stream_val.entries.items[i].id.equals(id)) {
                                    var removed_entry = stream_val.entries.orderedRemove(i);
                                    removed_entry.deinit(self.allocator);
                                    found_in_stream = true;
                                    break;
                                } else {
                                    i += 1;
                                }
                            }

                            // Remove from ALL group PELs (even if not in stream - cleans dangling refs)
                            removeFromAllPendingLists(self, stream_val, id);

                            try result.append(allocator, if (found_in_stream) 1 else -1);
                        },
                        .acked => {
                            // Only delete if ALL groups acknowledged
                            if (checkAllGroupsAcknowledged(stream_val, id)) {
                                // All groups acknowledged (or no groups exist) - safe to delete
                                var found_in_stream = false;
                                var i: usize = 0;
                                while (i < stream_val.entries.items.len) {
                                    if (stream_val.entries.items[i].id.equals(id)) {
                                        var removed_entry = stream_val.entries.orderedRemove(i);
                                        removed_entry.deinit(self.allocator);
                                        found_in_stream = true;
                                        break;
                                    } else {
                                        i += 1;
                                    }
                                }
                                try result.append(allocator, if (found_in_stream) 1 else -1);
                            } else {
                                // Not all groups acknowledged - don't delete, return 2
                                try result.append(allocator, 2);
                            }
                        },
                    }
                }

                return try result.toOwnedSlice(allocator);
            },
            else => return error.WrongType,
        }
    }

    /// Claim ownership of pending messages in a consumer group
    /// Returns claimed entries
    pub fn xclaim(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        group_name: []const u8,
        consumer_name: []const u8,
        min_idle_time: i64,
        ids: []const []const u8,
        idle: ?i64,
        time: ?i64,
        retrycount: ?u64,
        force: bool,
        justid: bool,
    ) !?std.ArrayList(Value.StreamEntry) {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return error.NoKey;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            return error.NoKey;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                const group_ptr = stream_val.consumer_groups.getPtr(group_name) orelse return error.NoGroup;

                const current_time = time orelse std.time.milliTimestamp();

                // Ensure target consumer exists
                const consumer_entry = try group_ptr.consumers.getOrPut(consumer_name);
                if (!consumer_entry.found_existing) {
                    const owned_consumer_name = try self.allocator.dupe(u8, consumer_name);
                    consumer_entry.key_ptr.* = owned_consumer_name;
                    consumer_entry.value_ptr.* = Value.Consumer{
                        .name = owned_consumer_name,
                        .pending = std.ArrayList(Value.StreamId){},
                        .last_attempted_time = current_time,
                        .last_successful_time = current_time,
                        .creation_time = current_time,
                    };
                }

                // Update last_attempted_time for every XCLAIM call
                consumer_entry.value_ptr.last_attempted_time = current_time;
                var result = std.ArrayList(Value.StreamEntry){};

                for (ids) |id_str| {
                    const id = Value.StreamId.parse(id_str, null) catch continue;

                    // Find the pending entry
                    var pending_idx: ?usize = null;
                    var old_consumer: ?[]const u8 = null;
                    var old_delivery_count: u64 = 0;

                    for (group_ptr.pending.items, 0..) |*pending, idx| {
                        if (pending.id.equals(id)) {
                            // Check if idle time is sufficient
                            const idle_time = current_time - pending.delivery_time;
                            if (!force and idle_time < min_idle_time) {
                                continue;
                            }

                            pending_idx = idx;
                            old_consumer = pending.consumer;
                            old_delivery_count = pending.delivery_count;
                            break;
                        }
                    }

                    // If not in pending, check if FORCE is set
                    if (pending_idx == null and !force) {
                        continue;
                    }

                    // Find the actual stream entry
                    var stream_entry: ?*Value.StreamEntry = null;
                    for (stream_val.entries.items) |*item| {
                        if (item.id.equals(id)) {
                            stream_entry = item;
                            break;
                        }
                    }

                    if (stream_entry == null and !force) {
                        continue;
                    }

                    // Remove from old consumer's pending list if it exists
                    if (pending_idx) |idx| {
                        if (old_consumer) |old_cons| {
                            if (group_ptr.consumers.getPtr(old_cons)) |old_consumer_ptr| {
                                var i: usize = 0;
                                while (i < old_consumer_ptr.pending.items.len) : (i += 1) {
                                    if (old_consumer_ptr.pending.items[i].equals(id)) {
                                        _ = old_consumer_ptr.pending.orderedRemove(i);
                                        break;
                                    }
                                }
                            }
                        }

                        // Update the pending entry
                        var pending = &group_ptr.pending.items[idx];
                        self.allocator.free(pending.consumer);
                        pending.consumer = try self.allocator.dupe(u8, consumer_name);
                        pending.delivery_time = if (idle) |i| current_time - i else current_time;
                        if (retrycount) |rc| {
                            pending.delivery_count = rc;
                        } else {
                            pending.delivery_count = old_delivery_count + 1;
                        }
                    } else if (force) {
                        // Add new pending entry if FORCE is set and entry doesn't exist in PEL
                        try group_ptr.pending.append(self.allocator, Value.PendingEntry{
                            .id = id,
                            .consumer = try self.allocator.dupe(u8, consumer_name),
                            .delivery_time = if (idle) |i| current_time - i else current_time,
                            .delivery_count = retrycount orelse 1,
                        });
                    }

                    // Add to new consumer's pending list
                    try consumer_entry.value_ptr.pending.append(self.allocator, id);

                    // Build result entry
                    if (stream_entry) |se| {
                        if (!justid) {
                            var cloned_fields = std.ArrayList([]const u8){};
                            for (se.fields.items) |field| {
                                const owned_field = try allocator.dupe(u8, field);
                                try cloned_fields.append(allocator, owned_field);
                            }

                            try result.append(allocator, Value.StreamEntry{
                                .id = se.id,
                                .fields = cloned_fields,
                            });
                        } else {
                            try result.append(allocator, Value.StreamEntry{
                                .id = se.id,
                                .fields = std.ArrayList([]const u8){}, // Empty fields for JUSTID
                            });
                        }
                    }
                }

                if (result.items.len == 0) {
                    result.deinit(allocator);
                    return null;
                }

                // Update last_successful_time when messages were claimed
                consumer_entry.value_ptr.last_successful_time = current_time;

                return result;
            },
            else => return error.WrongType,
        }
    }

    /// Auto-claim old pending messages in a consumer group
    /// Returns (claimed entries, next cursor, deleted IDs)
    /// deleted_ids: IDs that were in PEL but no longer exist in the stream (cleaned up automatically)
    pub fn xautoclaim(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        group_name: []const u8,
        consumer_name: []const u8,
        min_idle_time: i64,
        start: []const u8,
        count: usize,
        justid: bool,
    ) !struct { entries: ?std.ArrayList(Value.StreamEntry), next_cursor: []const u8, deleted_ids: std.ArrayList(Value.StreamId) } {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return error.NoKey;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            return error.NoKey;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                const group_ptr = stream_val.consumer_groups.getPtr(group_name) orelse return error.NoGroup;

                const current_time = std.time.milliTimestamp();

                // Ensure target consumer exists
                const consumer_entry = try group_ptr.consumers.getOrPut(consumer_name);
                if (!consumer_entry.found_existing) {
                    const owned_consumer_name = try self.allocator.dupe(u8, consumer_name);
                    consumer_entry.key_ptr.* = owned_consumer_name;
                    consumer_entry.value_ptr.* = Value.Consumer{
                        .name = owned_consumer_name,
                        .pending = std.ArrayList(Value.StreamId){},
                        .last_attempted_time = current_time,
                        .last_successful_time = current_time,
                        .creation_time = current_time,
                    };
                }

                // Update last_attempted_time for every XAUTOCLAIM call
                consumer_entry.value_ptr.last_attempted_time = current_time;
                var result = std.ArrayList(Value.StreamEntry){};
                var deleted_ids = std.ArrayList(Value.StreamId){};
                errdefer deleted_ids.deinit(allocator);
                var deleted_indices = std.ArrayList(usize){};
                errdefer deleted_indices.deinit(allocator);
                var claimed_count: usize = 0;
                var next_id_str: []const u8 = "0-0";

                // Parse start cursor
                const start_id = if (std.mem.eql(u8, start, "0") or std.mem.eql(u8, start, "0-0"))
                    Value.StreamId{ .ms = 0, .seq = 0 }
                else
                    Value.StreamId.parse(start, null) catch Value.StreamId{ .ms = 0, .seq = 0 };

                // Scan through pending entries looking for old ones
                for (group_ptr.pending.items, 0..) |*pending, pending_idx| {
                    // Skip entries before start cursor
                    if (pending.id.lessThan(start_id) or pending.id.equals(start_id)) {
                        continue;
                    }

                    if (claimed_count >= count) {
                        // Set next cursor
                        next_id_str = try std.fmt.allocPrint(allocator, "{d}-{d}", .{ pending.id.ms, pending.id.seq });
                        break;
                    }

                    // Check if entry is old enough
                    const idle_time = current_time - pending.delivery_time;
                    if (idle_time < min_idle_time) {
                        continue;
                    }

                    // Find the stream entry
                    var stream_entry: ?*Value.StreamEntry = null;
                    for (stream_val.entries.items) |*item| {
                        if (item.id.equals(pending.id)) {
                            stream_entry = item;
                            break;
                        }
                    }

                    if (stream_entry == null) {
                        // Entry exists in PEL but was deleted from stream via XDEL.
                        // Collect for removal and report to caller (Redis 7.0+ behavior).
                        try deleted_ids.append(allocator, pending.id);
                        try deleted_indices.append(allocator, pending_idx);
                        continue;
                    }

                    // Remove from old consumer
                    if (group_ptr.consumers.getPtr(pending.consumer)) |old_consumer_ptr| {
                        var i: usize = 0;
                        while (i < old_consumer_ptr.pending.items.len) : (i += 1) {
                            if (old_consumer_ptr.pending.items[i].equals(pending.id)) {
                                _ = old_consumer_ptr.pending.orderedRemove(i);
                                break;
                            }
                        }
                    }

                    // Update pending entry
                    self.allocator.free(pending.consumer);
                    pending.consumer = try self.allocator.dupe(u8, consumer_name);
                    pending.delivery_time = current_time;
                    pending.delivery_count += 1;

                    // Add to new consumer
                    try consumer_entry.value_ptr.pending.append(self.allocator, pending.id);

                    // Build result
                    if (stream_entry) |se| {
                        if (!justid) {
                            var cloned_fields = std.ArrayList([]const u8){};
                            for (se.fields.items) |field| {
                                const owned_field = try allocator.dupe(u8, field);
                                try cloned_fields.append(allocator, owned_field);
                            }

                            try result.append(allocator, Value.StreamEntry{
                                .id = se.id,
                                .fields = cloned_fields,
                            });
                        } else {
                            try result.append(allocator, Value.StreamEntry{
                                .id = se.id,
                                .fields = std.ArrayList([]const u8){},
                            });
                        }
                    }

                    claimed_count += 1;
                }

                // Remove deleted entries from PEL in reverse order (preserves earlier indices).
                // Also remove from the original consumer's pending list.
                var di = deleted_indices.items.len;
                while (di > 0) {
                    di -= 1;
                    const del_idx = deleted_indices.items[di];
                    const del_pending = group_ptr.pending.items[del_idx];
                    if (group_ptr.consumers.getPtr(del_pending.consumer)) |old_consumer_ptr| {
                        var ci: usize = 0;
                        while (ci < old_consumer_ptr.pending.items.len) : (ci += 1) {
                            if (old_consumer_ptr.pending.items[ci].equals(del_pending.id)) {
                                _ = old_consumer_ptr.pending.orderedRemove(ci);
                                break;
                            }
                        }
                    }
                    self.allocator.free(del_pending.consumer);
                    _ = group_ptr.pending.orderedRemove(del_idx);
                }
                deleted_indices.deinit(allocator);

                // Update last_successful_time if any messages were claimed
                if (claimed_count > 0) {
                    consumer_entry.value_ptr.last_successful_time = current_time;
                }

                return .{
                    .entries = if (result.items.len > 0) result else null,
                    .next_cursor = next_id_str,
                    .deleted_ids = deleted_ids,
                };
            },
            else => return error.WrongType,
        }
    }

    /// Get summary of pending messages for a consumer group
    /// Returns RESP-formatted string: [count, min_id, max_id, [[consumer, count], ...]]
    pub fn xpendingSummary(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        group_name: []const u8,
    ) ![]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return error.NoSuchKey;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            return error.NoSuchKey;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                const group_ptr = stream_val.consumer_groups.getPtr(group_name) orelse return error.NoSuchGroup;

                if (group_ptr.pending.items.len == 0) {
                    return try std.fmt.allocPrint(allocator, "*4\r\n:0\r\n$-1\r\n$-1\r\n*0\r\n", .{});
                }

                // Find min and max IDs
                var min_id = group_ptr.pending.items[0].id;
                var max_id = group_ptr.pending.items[0].id;
                for (group_ptr.pending.items) |pending| {
                    if (pending.id.lessThan(min_id)) min_id = pending.id;
                    if (max_id.lessThan(pending.id)) max_id = pending.id;
                }

                // Count per consumer
                var consumer_counts = std.StringHashMap(usize).init(allocator);
                defer consumer_counts.deinit();

                for (group_ptr.pending.items) |pending| {
                    const entry_ptr = try consumer_counts.getOrPut(pending.consumer);
                    if (entry_ptr.found_existing) {
                        entry_ptr.value_ptr.* += 1;
                    } else {
                        entry_ptr.value_ptr.* = 1;
                    }
                }

                // Format response
                var buf = std.ArrayList(u8){};
                defer buf.deinit(allocator);

                const writer = buf.writer(allocator);
                try writer.print("*4\r\n:{d}\r\n${d}\r\n{d}-{d}\r\n${d}\r\n{d}-{d}\r\n*{d}\r\n", .{
                    group_ptr.pending.items.len,
                    std.fmt.count("{d}-{d}", .{ min_id.ms, min_id.seq }),
                    min_id.ms,
                    min_id.seq,
                    std.fmt.count("{d}-{d}", .{ max_id.ms, max_id.seq }),
                    max_id.ms,
                    max_id.seq,
                    consumer_counts.count(),
                });

                var it = consumer_counts.iterator();
                while (it.next()) |kv| {
                    try writer.print("*2\r\n${d}\r\n{s}\r\n:{d}\r\n", .{
                        kv.key_ptr.len,
                        kv.key_ptr.*,
                        kv.value_ptr.*,
                    });
                }

                return try allocator.dupe(u8, buf.items);
            },
            else => return error.WrongType,
        }
    }

    /// Get detailed list of pending messages in a range
    pub fn xpendingRange(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        group_name: []const u8,
        start: []const u8,
        end: []const u8,
        count: usize,
        consumer_name: ?[]const u8,
        idle_time: ?i64,
    ) ![]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return error.NoSuchKey;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            return error.NoSuchKey;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                const group_ptr = stream_val.consumer_groups.getPtr(group_name) orelse return error.NoSuchGroup;

                // Parse start and end IDs
                const start_id = if (std.mem.eql(u8, start, "-"))
                    Value.StreamId{ .ms = 0, .seq = 0 }
                else
                    try Value.StreamId.parse(start, null);

                const end_id = if (std.mem.eql(u8, end, "+"))
                    Value.StreamId{ .ms = std.math.maxInt(i64), .seq = std.math.maxInt(u64) }
                else
                    try Value.StreamId.parse(end, null);

                // Collect matching pending entries
                var result_list = std.ArrayList(Value.PendingEntry){};
                defer result_list.deinit(allocator);

                const now = std.time.milliTimestamp();

                for (group_ptr.pending.items) |pending| {
                    if (pending.id.lessThan(start_id) or end_id.lessThan(pending.id)) {
                        continue;
                    }

                    // Filter by consumer if specified
                    if (consumer_name) |cname| {
                        if (!std.mem.eql(u8, pending.consumer, cname)) {
                            continue;
                        }
                    }

                    // Filter by idle time if specified
                    if (idle_time) |idle_ms| {
                        const elapsed = now - pending.delivery_time;
                        if (elapsed < idle_ms) {
                            continue;
                        }
                    }

                    try result_list.append(allocator, pending);

                    if (result_list.items.len >= count) {
                        break;
                    }
                }

                // Format response: array of [id, consumer, elapsed_ms, delivery_count]
                var buf = std.ArrayList(u8){};
                defer buf.deinit(allocator);

                const writer = buf.writer(allocator);
                try writer.print("*{d}\r\n", .{result_list.items.len});

                for (result_list.items) |pending| {
                    const elapsed = now - pending.delivery_time;
                    const id_str = try std.fmt.allocPrint(allocator, "{d}-{d}", .{ pending.id.ms, pending.id.seq });
                    defer allocator.free(id_str);

                    try writer.print("*4\r\n${d}\r\n{s}\r\n${d}\r\n{s}\r\n:{d}\r\n:{d}\r\n", .{
                        id_str.len,
                        id_str,
                        pending.consumer.len,
                        pending.consumer,
                        elapsed,
                        pending.delivery_count,
                    });
                }

                return try allocator.dupe(u8, buf.items);
            },
            else => return error.WrongType,
        }
    }

    /// Get information about a stream
    pub fn xinfoStream(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        full_mode: bool,
        count_limit: ?usize,
    ) ![]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return error.NoSuchKey;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            return error.NoSuchKey;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                var buf = std.ArrayList(u8){};
                defer buf.deinit(allocator);

                const writer = buf.writer(allocator);

                if (!full_mode) {
                    // Simple mode: basic stream metadata
                    const last_id = stream_val.last_id orelse Value.StreamId{ .ms = 0, .seq = 0 };

                    try writer.print("*18\r\n", .{});
                    try writer.print("$6\r\nlength\r\n:{d}\r\n", .{stream_val.entries.items.len});
                    try writer.print("$15\r\nradix-tree-keys\r\n:1\r\n", .{});
                    try writer.print("$17\r\nradix-tree-nodes\r\n:2\r\n", .{});
                    try writer.print("$6\r\ngroups\r\n:{d}\r\n", .{stream_val.consumer_groups.count()});
                    try writer.print("$13\r\nlast-entry-id\r\n${d}\r\n{d}-{d}\r\n", .{
                        std.fmt.count("{d}-{d}", .{ last_id.ms, last_id.seq }),
                        last_id.ms,
                        last_id.seq,
                    });
                    try writer.print("$13\r\nidmp-duration\r\n:{d}\r\n", .{stream_val.idmp_duration_sec});
                    try writer.print("$12\r\nidmp-maxsize\r\n:{d}\r\n", .{stream_val.idmp_maxsize});
                    try writer.print("$13\r\nfirst-entry\r\n", .{});
                    if (stream_val.entries.items.len > 0) {
                        const first_entry = stream_val.entries.items[0];
                        try writer.print("*2\r\n${d}\r\n{d}-{d}\r\n*{d}\r\n", .{
                            std.fmt.count("{d}-{d}", .{ first_entry.id.ms, first_entry.id.seq }),
                            first_entry.id.ms,
                            first_entry.id.seq,
                            first_entry.fields.items.len,
                        });
                        for (first_entry.fields.items) |field| {
                            try writer.print("${d}\r\n{s}\r\n", .{ field.len, field });
                        }
                    } else {
                        try writer.print("$-1\r\n", .{});
                    }
                    try writer.print("$10\r\nlast-entry\r\n", .{});
                    if (stream_val.entries.items.len > 0) {
                        const last_entry = stream_val.entries.items[stream_val.entries.items.len - 1];
                        try writer.print("*2\r\n${d}\r\n{d}-{d}\r\n*{d}\r\n", .{
                            std.fmt.count("{d}-{d}", .{ last_entry.id.ms, last_entry.id.seq }),
                            last_entry.id.ms,
                            last_entry.id.seq,
                            last_entry.fields.items.len,
                        });
                        for (last_entry.fields.items) |field| {
                            try writer.print("${d}\r\n{s}\r\n", .{ field.len, field });
                        }
                    } else {
                        try writer.print("$-1\r\n", .{});
                    }
                } else {
                    // Full mode: include all entries
                    const limit = count_limit orelse stream_val.entries.items.len;
                    const num_entries = @min(limit, stream_val.entries.items.len);

                    try writer.print("*14\r\n", .{});
                    try writer.print("$6\r\nlength\r\n:{d}\r\n", .{stream_val.entries.items.len});
                    try writer.print("$15\r\nradix-tree-keys\r\n:1\r\n", .{});
                    try writer.print("$17\r\nradix-tree-nodes\r\n:2\r\n", .{});

                    const last_id = stream_val.last_id orelse Value.StreamId{ .ms = 0, .seq = 0 };
                    try writer.print("$13\r\nlast-entry-id\r\n${d}\r\n{d}-{d}\r\n", .{
                        std.fmt.count("{d}-{d}", .{ last_id.ms, last_id.seq }),
                        last_id.ms,
                        last_id.seq,
                    });
                    try writer.print("$13\r\nidmp-duration\r\n:{d}\r\n", .{stream_val.idmp_duration_sec});
                    try writer.print("$12\r\nidmp-maxsize\r\n:{d}\r\n", .{stream_val.idmp_maxsize});

                    try writer.print("$7\r\nentries\r\n*{d}\r\n", .{num_entries});
                    for (stream_val.entries.items[0..num_entries]) |stream_entry| {
                        try writer.print("*2\r\n${d}\r\n{d}-{d}\r\n*{d}\r\n", .{
                            std.fmt.count("{d}-{d}", .{ stream_entry.id.ms, stream_entry.id.seq }),
                            stream_entry.id.ms,
                            stream_entry.id.seq,
                            stream_entry.fields.items.len,
                        });
                        for (stream_entry.fields.items) |field| {
                            try writer.print("${d}\r\n{s}\r\n", .{ field.len, field });
                        }
                    }

                    try writer.print("$6\r\ngroups\r\n*{d}\r\n", .{stream_val.consumer_groups.count()});
                    var group_it = stream_val.consumer_groups.iterator();
                    while (group_it.next()) |group_kv| {
                        const group = group_kv.value_ptr;
                        try writer.print("*8\r\n", .{});
                        try writer.print("$4\r\nname\r\n${d}\r\n{s}\r\n", .{ group_kv.key_ptr.len, group_kv.key_ptr.* });
                        try writer.print("$9\r\nconsumers\r\n:{d}\r\n", .{group.consumers.count()});
                        try writer.print("$7\r\npending\r\n:{d}\r\n", .{group.pending.items.len});
                        try writer.print("$16\r\nlast-delivered-id\r\n${d}\r\n{d}-{d}\r\n", .{
                            std.fmt.count("{d}-{d}", .{ group.last_delivered_id.ms, group.last_delivered_id.seq }),
                            group.last_delivered_id.ms,
                            group.last_delivered_id.seq,
                        });
                    }
                }

                return try allocator.dupe(u8, buf.items);
            },
            else => return error.WrongType,
        }
    }

    /// Get the Unix timestamp (seconds) of the last successful RDB save
    pub fn getLastSaveTime(self: *Storage) i64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.last_save_time;
    }

    /// Update the last save time to current Unix timestamp and reset dirty counter
    pub fn updateLastSaveTime(self: *Storage) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.last_save_time = @divFloor(std.time.milliTimestamp(), 1000);
        _ = self.dirty_count.store(0, .monotonic);
    }

    /// Return the number of write operations since the last successful RDB save
    pub fn getDirtyCount(self: *const Storage) u64 {
        return self.dirty_count.load(.monotonic);
    }

    /// Increment the dirty counter by n (called on each key-modifying operation)
    fn incrementDirty(self: *Storage, n: u64) void {
        _ = self.dirty_count.fetchAdd(n, .monotonic);
    }

    /// Update peak memory usage if current > stored peak (called from INFO memory section)
    pub fn updatePeakMemory(self: *Storage, current_bytes: usize) void {
        var current_peak = self.peak_memory.load(.monotonic);
        while (current_bytes > current_peak) {
            if (self.peak_memory.cmpxchgWeak(current_peak, current_bytes, .monotonic, .monotonic)) |updated| {
                current_peak = updated;
            } else {
                break;
            }
        }
    }

    /// Get peak memory usage in bytes
    pub fn getPeakMemory(self: *const Storage) usize {
        return self.peak_memory.load(.monotonic);
    }

    /// Get consumer information for a group (XINFO CONSUMERS)
    /// Returns array of consumers with name, pending, idle, inactive fields
    pub fn xinfoConsumers(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
        group_name: []const u8,
    ) ![]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return error.NoGroup;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            return error.NoGroup;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                const group_ptr = stream_val.consumer_groups.getPtr(group_name) orelse return error.NoGroup;

                const now_ms = std.time.milliTimestamp();
                var buf = std.ArrayList(u8){};
                const writer = buf.writer(allocator);

                // RESP array header for consumers
                try writer.print("*{d}\r\n", .{group_ptr.consumers.count()});

                // Iterate over consumers
                var it = group_ptr.consumers.iterator();
                while (it.next()) |cons_entry| {
                    const consumer = cons_entry.value_ptr.*;

                    // Each consumer is a flat array of 8 elements (4 fields × 2)
                    try writer.writeAll("*8\r\n");

                    // name
                    try writer.writeAll("$4\r\nname\r\n");
                    try writer.print("${d}\r\n{s}\r\n", .{ consumer.name.len, consumer.name });

                    // pending
                    try writer.writeAll("$7\r\npending\r\n");
                    try writer.print(":{d}\r\n", .{consumer.pending.items.len});

                    // idle (ms since last attempt)
                    try writer.writeAll("$4\r\nidle\r\n");
                    const idle_ms = now_ms - consumer.last_attempted_time;
                    try writer.print(":{d}\r\n", .{idle_ms});

                    // inactive (ms since last success)
                    try writer.writeAll("$8\r\ninactive\r\n");
                    const inactive_ms = now_ms - consumer.last_successful_time;
                    try writer.print(":{d}\r\n", .{inactive_ms});
                }

                return try buf.toOwnedSlice(allocator);
            },
            else => return error.WrongType,
        }
    }

    /// Get consumer group information (XINFO GROUPS)
    /// Returns array of groups with name, consumers, pending, last-delivered-id, entries-read, lag fields
    pub fn xinfoGroups(
        self: *Storage,
        allocator: std.mem.Allocator,
        key: []const u8,
    ) ![]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse {
            // Key not found - return empty array (different from CONSUMERS which returns NOGROUP)
            return try allocator.dupe(u8, "*0\r\n");
        };

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            return try allocator.dupe(u8, "*0\r\n");
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                var buf = std.ArrayList(u8){};
                const writer = buf.writer(allocator);

                // RESP array header for groups
                try writer.print("*{d}\r\n", .{stream_val.consumer_groups.count()});

                // Iterate over groups
                var it = stream_val.consumer_groups.iterator();
                while (it.next()) |group_entry| {
                    const group = group_entry.value_ptr.*;

                    // Each group is a flat array of 12 elements (6 fields × 2)
                    try writer.writeAll("*12\r\n");

                    // name
                    try writer.writeAll("$4\r\nname\r\n");
                    try writer.print("${d}\r\n{s}\r\n", .{ group.name.len, group.name });

                    // consumers
                    try writer.writeAll("$9\r\nconsumers\r\n");
                    try writer.print(":{d}\r\n", .{group.consumers.count()});

                    // pending
                    try writer.writeAll("$7\r\npending\r\n");
                    try writer.print(":{d}\r\n", .{group.pending.items.len});

                    // last-delivered-id
                    try writer.writeAll("$17\r\nlast-delivered-id\r\n");
                    const id_str = try std.fmt.allocPrint(allocator, "{d}-{d}", .{ group.last_delivered_id.ms, group.last_delivered_id.seq });
                    defer allocator.free(id_str);
                    try writer.print("${d}\r\n{s}\r\n", .{ id_str.len, id_str });

                    // entries-read
                    try writer.writeAll("$12\r\nentries-read\r\n");
                    try writer.print(":{d}\r\n", .{group.entries_read});

                    // lag (null if arbitrary start, otherwise stream.entries_added - group.entries_read)
                    try writer.writeAll("$3\r\nlag\r\n");
                    if (group.arbitrary_start) {
                        // Lag is unavailable (null bulk string)
                        try writer.writeAll("$-1\r\n");
                    } else {
                        const lag: i64 = @as(i64, @intCast(stream_val.entries_added)) - @as(i64, @intCast(group.entries_read));
                        try writer.print(":{d}\r\n", .{lag});
                    }
                }

                return try buf.toOwnedSlice(allocator);
            },
            else => return error.WrongType,
        }
    }

    /// XSETID key <ID | $> [ENTRIESADDED entries-added] [MAXDELETEDID max-deleted-id]
    /// Set stream metadata (last_id, entries_added, max_deleted_entry_id)
    /// Creates stream if it doesn't exist
    /// Returns error if key exists and is not a stream
    /// Set stream metadata (last_id, entries_added, max_deleted_entry_id).
    ///
    /// Creates stream if it doesn't exist. For new streams, $ placeholder uses 0-0.
    /// For existing streams, $ placeholder preserves current last_id.
    ///
    /// Arguments:
    ///   - key: Stream key name
    ///   - new_last_id: Stream ID string or "$" placeholder
    ///   - entries_added: Optional total entries counter
    ///   - max_deleted_id: Optional highest deleted entry ID
    ///
    /// Returns error.WrongType if key exists but is not a stream.
    /// Returns error.InvalidStreamId if ID format is invalid.
    pub fn xsetid(
        self: *Storage,
        key: []const u8,
        new_last_id: []const u8,
        entries_added: ?u64,
        max_deleted_id: ?[]const u8,
    ) error{ WrongType, InvalidStreamId, OutOfMemory, Overflow, InvalidCharacter }!void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = getCurrentTimestamp();

        // Get or create stream
        const entry = try self.data.getOrPut(key);
        if (!entry.found_existing) {
            // Create new stream
            const owned_key = try self.allocator.dupe(u8, key);
            errdefer {
                self.allocator.free(owned_key);
                _ = self.removeKeyCleanup(key);
            }
            entry.key_ptr.* = owned_key;

            // Parse new_last_id ($ means 0-0 for empty streams)
            const parsed_id = if (std.mem.eql(u8, new_last_id, "$"))
                Value.StreamId{ .ms = 0, .seq = 0 }
            else
                try Value.StreamId.parse(new_last_id, null);

            // Parse max_deleted_id if provided
            const parsed_max_deleted = if (max_deleted_id) |mdid|
                try Value.StreamId.parse(mdid, null)
            else
                Value.StreamId{ .ms = 0, .seq = 0 };

            entry.value_ptr.* = Value{
                .stream = .{
                    .entries = std.ArrayList(Value.StreamEntry){},
                    .last_id = parsed_id,
                    .expires_at = null,
                    .consumer_groups = std.StringHashMap(Value.ConsumerGroup).init(self.allocator),
                    .entries_added = entries_added orelse 0,
                    .max_deleted_entry_id = parsed_max_deleted,
                },
            };
        } else {
            // Check expiration
            if (entry.value_ptr.isExpired(now)) {
                var value = entry.value_ptr.*;
                value.deinit(self.allocator);

                // Parse new_last_id ($ means 0-0 for empty streams)
                const parsed_id = if (std.mem.eql(u8, new_last_id, "$"))
                    Value.StreamId{ .ms = 0, .seq = 0 }
                else
                    try Value.StreamId.parse(new_last_id, null);

                // Parse max_deleted_id if provided
                const parsed_max_deleted = if (max_deleted_id) |mdid|
                    try Value.StreamId.parse(mdid, null)
                else
                    Value.StreamId{ .ms = 0, .seq = 0 };

                entry.value_ptr.* = Value{
                    .stream = .{
                        .entries = std.ArrayList(Value.StreamEntry){},
                        .last_id = parsed_id,
                        .expires_at = null,
                        .consumer_groups = std.StringHashMap(Value.ConsumerGroup).init(self.allocator),
                        .entries_added = entries_added orelse 0,
                        .max_deleted_entry_id = parsed_max_deleted,
                    },
                };
            } else {
                // Update existing stream
                switch (entry.value_ptr.*) {
                    .stream => |*stream_val| {
                        // Parse new_last_id ($ means use current last_id, or 0-0 for empty streams)
                        const parsed_id = if (std.mem.eql(u8, new_last_id, "$"))
                            stream_val.last_id orelse Value.StreamId{ .ms = 0, .seq = 0 }
                        else
                            try Value.StreamId.parse(new_last_id, null);

                        stream_val.last_id = parsed_id;

                        // Update entries_added if provided
                        if (entries_added) |ea| {
                            stream_val.entries_added = ea;
                        }

                        // Update max_deleted_entry_id if provided
                        if (max_deleted_id) |mdid| {
                            const parsed_max_deleted = try Value.StreamId.parse(mdid, null);
                            stream_val.max_deleted_entry_id = parsed_max_deleted;
                        }
                    },
                    else => return error.WrongType,
                }
            }
        }
    }

    /// Configure stream IDMP (Idempotent Message Processing) settings (Redis 8.6+)
    /// XCFGSET key [IDMP-DURATION duration] [IDMP-MAXSIZE maxsize]
    ///
    /// Parameters:
    /// - duration: 1-86400 seconds (time to retain idempotent IDs)
    /// - maxsize: 1-10000 entries (max iids per producer)
    ///
    /// Important: Calling XCFGSET clears the IDMP map (not yet implemented)
    ///
    /// Returns error if:
    /// - Stream does not exist
    /// - Key is not a stream
    /// - Duration/maxsize out of valid range
    pub fn xcfgset(
        self: *Storage,
        key: []const u8,
        duration: ?u32,
        maxsize: ?u32,
    ) error{ NoSuchKey, WrongType, InvalidDuration, InvalidMaxsize }!void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // At least one parameter must be provided (validated by caller)
        // Stream must exist (no implicit creation)
        const entry = self.data.getEntry(key) orelse return error.NoSuchKey;

        const now = getCurrentTimestamp();
        if (entry.value_ptr.isExpired(now)) {
            return error.NoSuchKey;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                // Validate duration (1-86400 seconds)
                if (duration) |d| {
                    if (d < 1 or d > 86400) {
                        return error.InvalidDuration;
                    }
                    stream_val.idmp_duration_sec = d;
                }

                // Validate maxsize (1-10000 entries)
                if (maxsize) |ms| {
                    if (ms < 1 or ms > 10000) {
                        return error.InvalidMaxsize;
                    }
                    stream_val.idmp_maxsize = ms;
                }

                // TODO: Clear IDMP map when implemented
                // When IDMP tracking is added (producer ID -> idempotent ID map),
                // this function should clear the map after updating config
            },
            else => return error.WrongType,
        }
    }
};

// Embedded unit tests

test "storage - init and deinit" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
}

test "storage - set and get string" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("key1", "value1", null);
    const result = storage.get("key1");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("value1", result.?);
}

test "storage - get non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = storage.get("nosuchkey");
    try std.testing.expect(result == null);
}

test "storage - set overwrites existing key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("key1", "value1", null);
    try storage.set("key1", "value2", null);
    const result = storage.get("key1");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("value2", result.?);
}

test "storage - del single key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("key1", "value1", null);
    const keys = [_][]const u8{"key1"};
    const count = storage.del(&keys);
    try std.testing.expectEqual(@as(usize, 1), count);

    const result = storage.get("key1");
    try std.testing.expect(result == null);
}

test "storage - del multiple keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("key1", "value1", null);
    try storage.set("key2", "value2", null);
    try storage.set("key3", "value3", null);

    const keys = [_][]const u8{ "key1", "key2", "nosuchkey" };
    const count = storage.del(&keys);
    try std.testing.expectEqual(@as(usize, 2), count);

    try std.testing.expect(storage.get("key1") == null);
    try std.testing.expect(storage.get("key2") == null);
    try std.testing.expect(storage.get("key3") != null);
}

test "storage - del non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const keys = [_][]const u8{"nosuchkey"};
    const count = storage.del(&keys);
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "storage - exists returns true for existing key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("key1", "value1", null);
    try std.testing.expect(storage.exists("key1"));
}

test "storage - exists returns false for non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try std.testing.expect(!storage.exists("nosuchkey"));
}

test "storage - set with expiration in future" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const now = Storage.getCurrentTimestamp();
    const expires_at = now + 10000; // 10 seconds in future

    try storage.set("key1", "value1", expires_at);
    const result = storage.get("key1");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("value1", result.?);
}

test "storage - get expired key returns null and deletes" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const now = Storage.getCurrentTimestamp();
    const expires_at = now - 1000; // 1 second in past (already expired)

    try storage.set("key1", "value1", expires_at);
    const result = storage.get("key1");
    try std.testing.expect(result == null);

    // Verify key was deleted
    try std.testing.expect(!storage.exists("key1"));
}

test "storage - exists on expired key returns false and deletes" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const now = Storage.getCurrentTimestamp();
    const expires_at = now - 1000; // Already expired

    try storage.set("key1", "value1", expires_at);
    try std.testing.expect(!storage.exists("key1"));
}

test "storage - set updates expiration" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const now = Storage.getCurrentTimestamp();

    // Set with expiration
    try storage.set("key1", "value1", now + 10000);

    // Update without expiration
    try storage.set("key1", "value2", null);

    const result = storage.get("key1");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("value2", result.?);
}

test "storage - evictExpired removes expired keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const now = Storage.getCurrentTimestamp();

    // Set some keys with different expirations
    try storage.set("key1", "value1", now - 1000); // Expired
    try storage.set("key2", "value2", now + 10000); // Not expired
    try storage.set("key3", "value3", now - 500); // Expired
    try storage.set("key4", "value4", null); // No expiration

    const count = storage.evictExpired();
    try std.testing.expectEqual(@as(usize, 2), count);

    // Verify only non-expired keys remain
    try std.testing.expect(storage.get("key1") == null);
    try std.testing.expect(storage.get("key2") != null);
    try std.testing.expect(storage.get("key3") == null);
    try std.testing.expect(storage.get("key4") != null);
}

test "storage - multiple operations" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set multiple keys
    try storage.set("name", "Alice", null);
    try storage.set("age", "30", null);
    try storage.set("city", "NYC", null);

    // Get values
    try std.testing.expectEqualStrings("Alice", storage.get("name").?);
    try std.testing.expectEqualStrings("30", storage.get("age").?);

    // Update a value
    try storage.set("age", "31", null);
    try std.testing.expectEqualStrings("31", storage.get("age").?);

    // Delete one key
    const keys = [_][]const u8{"age"};
    _ = storage.del(&keys);
    try std.testing.expect(storage.get("age") == null);

    // Check existence
    try std.testing.expect(storage.exists("name"));
    try std.testing.expect(!storage.exists("age"));
    try std.testing.expect(storage.exists("city"));
}

test "storage - getType returns correct type" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("mystring", "value", null);
    try std.testing.expectEqual(ValueType.string, storage.getType("mystring").?);

    const elements = [_][]const u8{"a"};
    _ = try storage.lpush("mylist", &elements, null);
    try std.testing.expectEqual(ValueType.list, storage.getType("mylist").?);

    try std.testing.expect(storage.getType("nosuchkey") == null);
}

test "storage - lpush creates new list" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{"world"};
    const len = try storage.lpush("mylist", &elements, null);
    try std.testing.expectEqual(@as(usize, 1), len);
}

test "storage - lpush multiple elements order" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    const len = try storage.lpush("mylist", &elements, null);
    try std.testing.expectEqual(@as(usize, 3), len);

    // Verify order: LPUSH a b c -> [c, b, a]
    const range = (try storage.lrange(allocator, "mylist", 0, -1)).?;
    defer {
        allocator.free(range);
    }
    try std.testing.expectEqual(@as(usize, 3), range.len);
    try std.testing.expectEqualStrings("c", range[0]);
    try std.testing.expectEqualStrings("b", range[1]);
    try std.testing.expectEqualStrings("a", range[2]);
}

test "storage - lpush on existing string returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("mykey", "string", null);
    const elements = [_][]const u8{"value"};
    try std.testing.expectError(error.WrongType, storage.lpush("mykey", &elements, null));
}

test "storage - rpush creates new list" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{"hello"};
    const len = try storage.rpush("mylist", &elements, null);
    try std.testing.expectEqual(@as(usize, 1), len);
}

test "storage - rpush multiple elements order" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    const len = try storage.rpush("mylist", &elements, null);
    try std.testing.expectEqual(@as(usize, 3), len);

    // Verify order: RPUSH a b c -> [a, b, c]
    const range = (try storage.lrange(allocator, "mylist", 0, -1)).?;
    defer {
        allocator.free(range);
    }
    try std.testing.expectEqual(@as(usize, 3), range.len);
    try std.testing.expectEqualStrings("a", range[0]);
    try std.testing.expectEqualStrings("b", range[1]);
    try std.testing.expectEqualStrings("c", range[2]);
}

test "storage - rpush on existing string returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("mykey", "string", null);
    const elements = [_][]const u8{"value"};
    try std.testing.expectError(error.WrongType, storage.rpush("mykey", &elements, null));
}

test "storage - lpop single element" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    const result = (try storage.lpop(allocator, "mylist", 1)).?;
    defer {
        for (result) |elem| allocator.free(elem);
        allocator.free(result);
    }

    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqualStrings("a", result[0]);
}

test "storage - lpop multiple elements" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c", "d" };
    _ = try storage.rpush("mylist", &elements, null);

    const result = (try storage.lpop(allocator, "mylist", 2)).?;
    defer {
        for (result) |elem| allocator.free(elem);
        allocator.free(result);
    }

    try std.testing.expectEqual(@as(usize, 2), result.len);
    try std.testing.expectEqualStrings("a", result[0]);
    try std.testing.expectEqualStrings("b", result[1]);
}

test "storage - lpop auto-deletes empty list" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{"a"};
    _ = try storage.rpush("mylist", &elements, null);

    const result = (try storage.lpop(allocator, "mylist", 1)).?;
    defer {
        for (result) |elem| allocator.free(elem);
        allocator.free(result);
    }

    try std.testing.expect(!storage.exists("mylist"));
}

test "storage - lpop on non-existent key returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try storage.lpop(allocator, "nosuchkey", 1);
    try std.testing.expect(result == null);
}

test "storage - lpop count greater than length" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b" };
    _ = try storage.rpush("mylist", &elements, null);

    const result = (try storage.lpop(allocator, "mylist", 10)).?;
    defer {
        for (result) |elem| allocator.free(elem);
        allocator.free(result);
    }

    try std.testing.expectEqual(@as(usize, 2), result.len);
}

test "storage - rpop single element" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    const result = (try storage.rpop(allocator, "mylist", 1)).?;
    defer {
        for (result) |elem| allocator.free(elem);
        allocator.free(result);
    }

    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqualStrings("c", result[0]);
}

test "storage - rpop multiple elements" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c", "d" };
    _ = try storage.rpush("mylist", &elements, null);

    const result = (try storage.rpop(allocator, "mylist", 2)).?;
    defer {
        for (result) |elem| allocator.free(elem);
        allocator.free(result);
    }

    try std.testing.expectEqual(@as(usize, 2), result.len);
    try std.testing.expectEqualStrings("d", result[0]);
    try std.testing.expectEqualStrings("c", result[1]);
}

test "storage - rpop auto-deletes empty list" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{"a"};
    _ = try storage.rpush("mylist", &elements, null);

    const result = (try storage.rpop(allocator, "mylist", 1)).?;
    defer {
        for (result) |elem| allocator.free(elem);
        allocator.free(result);
    }

    try std.testing.expect(!storage.exists("mylist"));
}

test "storage - lrange all elements" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    const result = (try storage.lrange(allocator, "mylist", 0, -1)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqualStrings("a", result[0]);
    try std.testing.expectEqualStrings("b", result[1]);
    try std.testing.expectEqualStrings("c", result[2]);
}

test "storage - lrange with positive indices" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c", "d", "e" };
    _ = try storage.rpush("mylist", &elements, null);

    const result = (try storage.lrange(allocator, "mylist", 1, 3)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqualStrings("b", result[0]);
    try std.testing.expectEqualStrings("c", result[1]);
    try std.testing.expectEqualStrings("d", result[2]);
}

test "storage - lrange with negative indices" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c", "d", "e" };
    _ = try storage.rpush("mylist", &elements, null);

    const result = (try storage.lrange(allocator, "mylist", -3, -1)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqualStrings("c", result[0]);
    try std.testing.expectEqualStrings("d", result[1]);
    try std.testing.expectEqualStrings("e", result[2]);
}

test "storage - lrange out of bounds returns empty" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    const result = (try storage.lrange(allocator, "mylist", 5, 10)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 0), result.len);
}

test "storage - lrange on non-existent key returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try storage.lrange(allocator, "nosuchkey", 0, -1);
    try std.testing.expect(result == null);
}

test "storage - llen returns length" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    const len = storage.llen("mylist");
    try std.testing.expectEqual(@as(usize, 3), len.?);
}

test "storage - llen on non-existent key returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const len = storage.llen("nosuchkey");
    try std.testing.expect(len == null);
}

test "storage - llen on string key returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("mykey", "value", null);
    const len = storage.llen("mykey");
    try std.testing.expect(len == null);
}

test "storage - lindex returns element at positive index" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    const result = try storage.lindex("mylist", 1);
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("b", result.?);
}

test "storage - lindex returns element at negative index" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    const result = try storage.lindex("mylist", -1);
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("c", result.?);
}

test "storage - lindex out of range returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b" };
    _ = try storage.rpush("mylist", &elements, null);

    try std.testing.expect((try storage.lindex("mylist", 5)) == null);
    try std.testing.expect((try storage.lindex("mylist", -5)) == null);
}

test "storage - lindex on non-existent key returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try std.testing.expect((try storage.lindex("nosuchkey", 0)) == null);
}

test "storage - lindex wrong type returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("mykey", "value", null);
    try std.testing.expectError(error.WrongType, storage.lindex("mykey", 0));
}

test "storage - lset updates element" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    try storage.lset("mylist", 1, "z");
    const result = try storage.lindex("mylist", 1);
    try std.testing.expectEqualStrings("z", result.?);
}

test "storage - lset negative index" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    try storage.lset("mylist", -1, "z");
    const result = try storage.lindex("mylist", 2);
    try std.testing.expectEqualStrings("z", result.?);
}

test "storage - lset out of range returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{"a"};
    _ = try storage.rpush("mylist", &elements, null);

    try std.testing.expectError(error.IndexOutOfRange, storage.lset("mylist", 5, "z"));
}

test "storage - ltrim basic range" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c", "d", "e" };
    _ = try storage.rpush("mylist", &elements, null);

    try storage.ltrim("mylist", 1, 3);
    const result = (try storage.lrange(allocator, "mylist", 0, -1)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqualStrings("b", result[0]);
    try std.testing.expectEqualStrings("d", result[2]);
}

test "storage - ltrim start greater than stop deletes key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    try storage.ltrim("mylist", 5, 1);
    try std.testing.expect(!storage.exists("mylist"));
}

test "storage - lrem removes from head" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "a", "c", "a" };
    _ = try storage.rpush("mylist", &elements, null);

    const removed = try storage.lrem("mylist", 2, "a");
    try std.testing.expectEqual(@as(usize, 2), removed);

    const result = (try storage.lrange(allocator, "mylist", 0, -1)).?;
    defer allocator.free(result);
    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqualStrings("b", result[0]);
    try std.testing.expectEqualStrings("c", result[1]);
    try std.testing.expectEqualStrings("a", result[2]);
}

test "storage - lrem count zero removes all" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "a", "a" };
    _ = try storage.rpush("mylist", &elements, null);

    const removed = try storage.lrem("mylist", 0, "a");
    try std.testing.expectEqual(@as(usize, 3), removed);

    const result = (try storage.lrange(allocator, "mylist", 0, -1)).?;
    defer allocator.free(result);
    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqualStrings("b", result[0]);
}

test "storage - lpushx on existing list" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const init_elems = [_][]const u8{"a"};
    _ = try storage.rpush("mylist", &init_elems, null);

    const new_elems = [_][]const u8{"b"};
    const len = try storage.lpushx("mylist", &new_elems);
    try std.testing.expect(len != null);
    try std.testing.expectEqual(@as(usize, 2), len.?);
}

test "storage - lpushx on missing key returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elems = [_][]const u8{"a"};
    const len = try storage.lpushx("nosuchkey", &elems);
    try std.testing.expect(len == null);
    try std.testing.expect(!storage.exists("nosuchkey"));
}

test "storage - rpushx on existing list" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const init_elems = [_][]const u8{"a"};
    _ = try storage.rpush("mylist", &init_elems, null);

    const new_elems = [_][]const u8{"b"};
    const len = try storage.rpushx("mylist", &new_elems);
    try std.testing.expect(len != null);
    try std.testing.expectEqual(@as(usize, 2), len.?);
}

test "storage - rpushx on missing key returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elems = [_][]const u8{"a"};
    const len = try storage.rpushx("nosuchkey", &elems);
    try std.testing.expect(len == null);
}

test "storage - linsert before pivot" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    const new_len = try storage.linsert("mylist", true, "b", "x");
    try std.testing.expectEqual(@as(i64, 4), new_len);

    const result = (try storage.lrange(allocator, "mylist", 0, -1)).?;
    defer allocator.free(result);
    try std.testing.expectEqualStrings("x", result[1]);
    try std.testing.expectEqualStrings("b", result[2]);
}

test "storage - linsert after pivot" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    const new_len = try storage.linsert("mylist", false, "b", "x");
    try std.testing.expectEqual(@as(i64, 4), new_len);

    const result = (try storage.lrange(allocator, "mylist", 0, -1)).?;
    defer allocator.free(result);
    try std.testing.expectEqualStrings("b", result[1]);
    try std.testing.expectEqualStrings("x", result[2]);
}

test "storage - linsert pivot not found returns -1" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b" };
    _ = try storage.rpush("mylist", &elements, null);

    const result = try storage.linsert("mylist", true, "z", "x");
    try std.testing.expectEqual(@as(i64, -1), result);
}

test "storage - linsert on missing key returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try storage.linsert("nosuchkey", true, "pivot", "elem");
    try std.testing.expectEqual(@as(i64, 0), result);
}

test "storage - lpos finds first occurrence" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "a", "c", "a" };
    _ = try storage.rpush("mylist", &elements, null);

    const result = try storage.lpos(allocator, "mylist", "a", 1, 1, 0);
    defer allocator.free(result);
    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqual(@as(usize, 0), result[0]);
}

test "storage - lpos count 0 finds all" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "a", "c", "a" };
    _ = try storage.rpush("mylist", &elements, null);

    const result = try storage.lpos(allocator, "mylist", "a", 1, 0, 0);
    defer allocator.free(result);
    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqual(@as(usize, 0), result[0]);
    try std.testing.expectEqual(@as(usize, 2), result[1]);
    try std.testing.expectEqual(@as(usize, 4), result[2]);
}

test "storage - lmove left to right" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const src_elems = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("src", &src_elems, null);

    const dst_elems = [_][]const u8{"x"};
    _ = try storage.rpush("dst", &dst_elems, null);

    // LMOVE src dst LEFT RIGHT: pop from src head, push to dst tail
    const moved = try storage.lmove(allocator, "src", "dst", true, false);
    defer if (moved) |m| allocator.free(m);

    try std.testing.expect(moved != null);
    try std.testing.expectEqualStrings("a", moved.?);

    const dst_range = (try storage.lrange(allocator, "dst", 0, -1)).?;
    defer allocator.free(dst_range);
    try std.testing.expectEqual(@as(usize, 2), dst_range.len);
    try std.testing.expectEqualStrings("x", dst_range[0]);
    try std.testing.expectEqualStrings("a", dst_range[1]);
}

test "storage - lmove on empty source returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try storage.lmove(allocator, "nosuchkey", "dst", true, true);
    try std.testing.expect(result == null);
}

test "storage - sadd creates new set" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const members = [_][]const u8{"hello"};
    const added = try storage.sadd("myset", &members, null);
    try std.testing.expectEqual(@as(usize, 1), added);
}

test "storage - sadd adds multiple members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const members = [_][]const u8{ "one", "two", "three" };
    const added = try storage.sadd("myset", &members, null);
    try std.testing.expectEqual(@as(usize, 3), added);
}

test "storage - sadd ignores duplicates" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const members1 = [_][]const u8{ "one", "two" };
    _ = try storage.sadd("myset", &members1, null);

    const members2 = [_][]const u8{"two"};
    const added = try storage.sadd("myset", &members2, null);
    try std.testing.expectEqual(@as(usize, 0), added);
}

test "storage - sadd on existing string returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("mykey", "string", null);
    const members = [_][]const u8{"value"};
    try std.testing.expectError(error.WrongType, storage.sadd("mykey", &members, null));
}

test "storage - sadd with mixed new and existing members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const members1 = [_][]const u8{ "one", "two" };
    _ = try storage.sadd("myset", &members1, null);

    const members2 = [_][]const u8{ "two", "three", "four" };
    const added = try storage.sadd("myset", &members2, null);
    try std.testing.expectEqual(@as(usize, 2), added);
}

test "storage - srem removes members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const members = [_][]const u8{ "one", "two", "three" };
    _ = try storage.sadd("myset", &members, null);

    const to_remove = [_][]const u8{"one"};
    const removed = try storage.srem(allocator, "myset", &to_remove);
    try std.testing.expectEqual(@as(usize, 1), removed);
}

test "storage - srem returns count of removed" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const members = [_][]const u8{ "one", "two", "three" };
    _ = try storage.sadd("myset", &members, null);

    const to_remove = [_][]const u8{ "one", "three" };
    const removed = try storage.srem(allocator, "myset", &to_remove);
    try std.testing.expectEqual(@as(usize, 2), removed);
}

test "storage - srem auto-deletes empty set" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const members = [_][]const u8{"one"};
    _ = try storage.sadd("myset", &members, null);

    const to_remove = [_][]const u8{"one"};
    _ = try storage.srem(allocator, "myset", &to_remove);

    try std.testing.expect(!storage.exists("myset"));
}

test "storage - srem on non-existent key returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const to_remove = [_][]const u8{"one"};
    const removed = try storage.srem(allocator, "nosuchkey", &to_remove);
    try std.testing.expectEqual(@as(usize, 0), removed);
}

test "storage - srem on non-existent member returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const members = [_][]const u8{"one"};
    _ = try storage.sadd("myset", &members, null);

    const to_remove = [_][]const u8{"two"};
    const removed = try storage.srem(allocator, "myset", &to_remove);
    try std.testing.expectEqual(@as(usize, 0), removed);
}

test "storage - sismember returns true for member" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const members = [_][]const u8{ "one", "two" };
    _ = try storage.sadd("myset", &members, null);

    const is_member = try storage.sismember("myset", "one");
    try std.testing.expect(is_member);
}

test "storage - sismember returns false for non-member" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const members = [_][]const u8{ "one", "two" };
    _ = try storage.sadd("myset", &members, null);

    const is_member = try storage.sismember("myset", "three");
    try std.testing.expect(!is_member);
}

test "storage - sismember returns false for non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const is_member = try storage.sismember("nosuchkey", "one");
    try std.testing.expect(!is_member);
}

test "storage - sismember on string key returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("mykey", "string", null);
    try std.testing.expectError(error.WrongType, storage.sismember("mykey", "one"));
}

test "storage - smembers returns all members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const members = [_][]const u8{ "one", "two", "three" };
    _ = try storage.sadd("myset", &members, null);

    const result = (try storage.smembers(allocator, "myset")).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 3), result.len);
}

test "storage - smembers returns null for non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try storage.smembers(allocator, "nosuchkey");
    try std.testing.expect(result == null);
}

test "storage - smembers returns null for string key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("mykey", "string", null);
    const result = try storage.smembers(allocator, "mykey");
    try std.testing.expect(result == null);
}

test "storage - scard returns cardinality" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const members = [_][]const u8{ "one", "two", "three" };
    _ = try storage.sadd("myset", &members, null);

    const cardinality = storage.scard("myset");
    try std.testing.expectEqual(@as(usize, 3), cardinality.?);
}

test "storage - scard returns null for non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const cardinality = storage.scard("nosuchkey");
    try std.testing.expect(cardinality == null);
}

test "storage - scard returns null for string key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("mykey", "string", null);
    const cardinality = storage.scard("mykey");
    try std.testing.expect(cardinality == null);
}

test "storage - set respects expiration" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const now = Storage.getCurrentTimestamp();
    const expires_at = now - 1000; // Already expired

    const members = [_][]const u8{"one"};
    _ = try storage.sadd("myset", &members, expires_at);

    // Should return false/null for expired set
    const is_member = try storage.sismember("myset", "one");
    try std.testing.expect(!is_member);
}

test "storage - getType returns set type" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const members = [_][]const u8{"one"};
    _ = try storage.sadd("myset", &members, null);

    try std.testing.expectEqual(ValueType.set, storage.getType("myset").?);
}

// Sorted set unit tests

test "storage - zadd creates new sorted set" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores = [_]f64{1.0};
    const members = [_][]const u8{"alpha"};
    const result = try storage.zadd("myzset", &scores, &members, 0, null);
    try std.testing.expectEqual(@as(usize, 1), result.added);
    try std.testing.expectEqual(@as(usize, 1), result.changed);
}

test "storage - zadd multiple members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores = [_]f64{ 1.0, 2.0, 3.0 };
    const members = [_][]const u8{ "one", "two", "three" };
    const result = try storage.zadd("myzset", &scores, &members, 0, null);
    try std.testing.expectEqual(@as(usize, 3), result.added);
}

test "storage - zadd updates existing member score" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores1 = [_]f64{1.0};
    const members1 = [_][]const u8{"one"};
    _ = try storage.zadd("myzset", &scores1, &members1, 0, null);

    const scores2 = [_]f64{5.0};
    const members2 = [_][]const u8{"one"};
    const result = try storage.zadd("myzset", &scores2, &members2, 0, null);
    try std.testing.expectEqual(@as(usize, 0), result.added);
    try std.testing.expectEqual(@as(usize, 1), result.changed);

    const score = storage.zscore("myzset", "one");
    try std.testing.expect(score != null);
    try std.testing.expectEqual(@as(f64, 5.0), score.?);
}

test "storage - zadd with NX flag skips existing members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores1 = [_]f64{1.0};
    const members1 = [_][]const u8{"one"};
    _ = try storage.zadd("myzset", &scores1, &members1, 0, null);

    // NX = 1: only add new
    const scores2 = [_]f64{5.0};
    const members2 = [_][]const u8{"one"};
    const result = try storage.zadd("myzset", &scores2, &members2, 1, null);
    try std.testing.expectEqual(@as(usize, 0), result.added);

    // Score should remain 1.0
    const score = storage.zscore("myzset", "one");
    try std.testing.expectEqual(@as(f64, 1.0), score.?);
}

test "storage - zadd with XX flag skips new members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // XX = 2: only update existing, don't create new
    const scores = [_]f64{1.0};
    const members = [_][]const u8{"one"};
    const result = try storage.zadd("myzset", &scores, &members, 2, null);
    try std.testing.expectEqual(@as(usize, 0), result.added);

    // Key should not exist
    try std.testing.expect(storage.zcard("myzset") == null);
}

test "storage - zadd on non-sorted-set returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("mykey", "string", null);
    const scores = [_]f64{1.0};
    const members = [_][]const u8{"one"};
    try std.testing.expectError(error.WrongType, storage.zadd("mykey", &scores, &members, 0, null));
}

test "storage - zrem removes members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores = [_]f64{ 1.0, 2.0, 3.0 };
    const members = [_][]const u8{ "one", "two", "three" };
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    const to_remove = [_][]const u8{"one"};
    const removed = try storage.zrem("myzset", &to_remove);
    try std.testing.expectEqual(@as(usize, 1), removed);
    try std.testing.expectEqual(@as(usize, 2), storage.zcard("myzset").?);
}

test "storage - zrem non-existent member returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores = [_]f64{1.0};
    const members = [_][]const u8{"one"};
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    const to_remove = [_][]const u8{"nosuchmember"};
    const removed = try storage.zrem("myzset", &to_remove);
    try std.testing.expectEqual(@as(usize, 0), removed);
}

test "storage - zrem auto-deletes empty sorted set" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores = [_]f64{1.0};
    const members = [_][]const u8{"one"};
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    const to_remove = [_][]const u8{"one"};
    _ = try storage.zrem("myzset", &to_remove);

    try std.testing.expect(storage.zcard("myzset") == null);
}

test "storage - zrem non-existent key returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const to_remove = [_][]const u8{"one"};
    const removed = try storage.zrem("nosuchkey", &to_remove);
    try std.testing.expectEqual(@as(usize, 0), removed);
}

test "storage - zscore returns score for existing member" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores = [_]f64{3.14};
    const members = [_][]const u8{"pi"};
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    const score = storage.zscore("myzset", "pi");
    try std.testing.expect(score != null);
    try std.testing.expectEqual(@as(f64, 3.14), score.?);
}

test "storage - zscore returns null for non-existent member" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores = [_]f64{1.0};
    const members = [_][]const u8{"one"};
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    const score = storage.zscore("myzset", "nosuchmember");
    try std.testing.expect(score == null);
}

test "storage - zscore returns null for non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const score = storage.zscore("nosuchkey", "member");
    try std.testing.expect(score == null);
}

test "storage - zcard returns count" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores = [_]f64{ 1.0, 2.0, 3.0 };
    const members = [_][]const u8{ "one", "two", "three" };
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    try std.testing.expectEqual(@as(usize, 3), storage.zcard("myzset").?);
}

test "storage - zcard returns null for non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try std.testing.expect(storage.zcard("nosuchkey") == null);
}

test "storage - zrange returns members in order" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores = [_]f64{ 3.0, 1.0, 2.0 };
    const members = [_][]const u8{ "three", "one", "two" };
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    const result = (try storage.zrange(allocator, "myzset", 0, -1, false)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqualStrings("one", result[0]);
    try std.testing.expectEqualStrings("two", result[1]);
    try std.testing.expectEqualStrings("three", result[2]);
}

test "storage - zrange with WITHSCORES" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores = [_]f64{ 1.0, 2.0 };
    const members = [_][]const u8{ "one", "two" };
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    const result = (try storage.zrange(allocator, "myzset", 0, -1, true)).?;
    defer {
        // Only free score strings (even indices are member refs, odd are owned scores)
        var i: usize = 1;
        while (i < result.len) : (i += 2) {
            allocator.free(result[i]);
        }
        allocator.free(result);
    }

    try std.testing.expectEqual(@as(usize, 4), result.len);
    try std.testing.expectEqualStrings("one", result[0]);
    try std.testing.expectEqualStrings("two", result[2]);
}

test "storage - zrange with negative indices" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const members = [_][]const u8{ "one", "two", "three", "four", "five" };
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    const result = (try storage.zrange(allocator, "myzset", -3, -1, false)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqualStrings("three", result[0]);
    try std.testing.expectEqualStrings("four", result[1]);
    try std.testing.expectEqualStrings("five", result[2]);
}

test "storage - zrange out of bounds returns empty" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores = [_]f64{ 1.0, 2.0 };
    const members = [_][]const u8{ "one", "two" };
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    const result = (try storage.zrange(allocator, "myzset", 5, 10, false)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 0), result.len);
}

test "storage - zrangebyscore returns members in score range" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const members = [_][]const u8{ "one", "two", "three", "four", "five" };
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    const result = (try storage.zrangebyscore(allocator, "myzset", 2.0, 4.0, false, false, false, null, null)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqualStrings("two", result[0]);
    try std.testing.expectEqualStrings("three", result[1]);
    try std.testing.expectEqualStrings("four", result[2]);
}

test "storage - zrangebyscore with exclusive intervals" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores = [_]f64{ 1.0, 2.0, 3.0 };
    const members = [_][]const u8{ "one", "two", "three" };
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    // Exclusive min and max: (1 to (3 = score > 1 and score < 3
    const result = (try storage.zrangebyscore(allocator, "myzset", 1.0, 3.0, true, true, false, null, null)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqualStrings("two", result[0]);
}

test "storage - zrangebyscore with LIMIT" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const members = [_][]const u8{ "one", "two", "three", "four", "five" };
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    // LIMIT 1 2: skip 1, take 2
    const result = (try storage.zrangebyscore(allocator, "myzset", 1.0, 5.0, false, false, false, 1, 2)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 2), result.len);
    try std.testing.expectEqualStrings("two", result[0]);
    try std.testing.expectEqualStrings("three", result[1]);
}

test "storage - sorted set lexicographic ordering for equal scores" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores = [_]f64{ 1.0, 1.0, 1.0 };
    const members = [_][]const u8{ "charlie", "alpha", "beta" };
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    const result = (try storage.zrange(allocator, "myzset", 0, -1, false)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqualStrings("alpha", result[0]);
    try std.testing.expectEqualStrings("beta", result[1]);
    try std.testing.expectEqualStrings("charlie", result[2]);
}

test "storage - getType returns sorted_set type" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const scores = [_]f64{1.0};
    const members = [_][]const u8{"one"};
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    try std.testing.expectEqual(ValueType.sorted_set, storage.getType("myzset").?);
}

// ── Stream tests ──────────────────────────────────────────────────────────────

test "storage - xadd creates stream with auto-generated ID" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "temp", "25", "humidity", "60" };
    const id = try storage.xadd("weather", "*", &fields, null, .{});

    try std.testing.expect(id.ms > 0);
    try std.testing.expectEqual(@as(u64, 0), id.seq);
}

test "storage - xadd with explicit ID" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "field1", "value1" };
    const id = try storage.xadd("mystream", "1234567890-0", &fields, null, .{});

    try std.testing.expectEqual(@as(i64, 1234567890), id.ms);
    try std.testing.expectEqual(@as(u64, 0), id.seq);
}

test "storage - xadd enforces ID ordering" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "a", "1" };
    _ = try storage.xadd("s", "1000-0", &fields, null, .{});

    // Try to add earlier ID - should fail
    const result = storage.xadd("s", "999-0", &fields, null, .{});
    try std.testing.expectError(error.StreamIdTooSmall, result);
}

test "storage - xadd with partial auto-seq ID (ms-*)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "field", "value" };
    // Use a fixed ms with auto seq
    const id = try storage.xadd("mystream", "1700000000-*", &fields, null, .{});

    try std.testing.expectEqual(@as(i64, 1700000000), id.ms);
    try std.testing.expectEqual(@as(u64, 0), id.seq);
}

test "storage - xadd partial auto-seq increments seq when same ms" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "f", "v" };
    // Insert explicit entry at ms=5000 seq=2
    _ = try storage.xadd("s", "5000-2", &fields, null, .{});
    // Now partial ID with same ms — should get seq=3
    const id = try storage.xadd("s", "5000-*", &fields, null, .{});
    try std.testing.expectEqual(@as(i64, 5000), id.ms);
    try std.testing.expectEqual(@as(u64, 3), id.seq);
}

test "storage - xadd partial auto-seq resets seq for new ms" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "f", "v" };
    _ = try storage.xadd("s", "5000-7", &fields, null, .{});
    // Different ms — seq resets to 0
    const id = try storage.xadd("s", "6000-*", &fields, null, .{});
    try std.testing.expectEqual(@as(i64, 6000), id.ms);
    try std.testing.expectEqual(@as(u64, 0), id.seq);
}

test "storage - xadd partial auto-seq on empty stream starts at 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "f", "v" };
    const id = try storage.xadd("newstream", "9999-*", &fields, null, .{});
    try std.testing.expectEqual(@as(i64, 9999), id.ms);
    try std.testing.expectEqual(@as(u64, 0), id.seq);
}

test "storage - xadd partial auto-seq enforces ms ordering" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "f", "v" };
    _ = try storage.xadd("s", "9000-5", &fields, null, .{});
    // Older ms-* should fail
    const result = storage.xadd("s", "8000-*", &fields, null, .{});
    try std.testing.expectError(error.StreamIdTooSmall, result);
}

test "storage - xlen returns entry count" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "a", "1" };
    _ = try storage.xadd("s", "1000-0", &fields, null, .{});
    _ = try storage.xadd("s", "1001-0", &fields, null, .{});
    _ = try storage.xadd("s", "1002-0", &fields, null, .{});

    const len = (try storage.xlen("s")).?;
    try std.testing.expectEqual(@as(usize, 3), len);
}

test "storage - xlen on non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const len = try storage.xlen("nosuchkey");
    try std.testing.expect(len == null);
}

test "storage - xrange returns entries in range" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "data", "x" };
    _ = try storage.xadd("s", "1000-0", &fields, null, .{});
    _ = try storage.xadd("s", "2000-0", &fields, null, .{});
    _ = try storage.xadd("s", "3000-0", &fields, null, .{});

    const result = (try storage.xrange(allocator, "s", "1500-0", "2500-0", null)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqual(@as(i64, 2000), result[0].id.ms);
}

test "storage - xrange with - and + bounds" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "x", "y" };
    _ = try storage.xadd("s", "1000-0", &fields, null, .{});
    _ = try storage.xadd("s", "2000-0", &fields, null, .{});

    const result = (try storage.xrange(allocator, "s", "-", "+", null)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 2), result.len);
}

test "storage - xrange with COUNT limit" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "a", "b" };
    _ = try storage.xadd("s", "1000-0", &fields, null, .{});
    _ = try storage.xadd("s", "2000-0", &fields, null, .{});
    _ = try storage.xadd("s", "3000-0", &fields, null, .{});

    const result = (try storage.xrange(allocator, "s", "-", "+", 2)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 2), result.len);
}

test "storage - getType returns stream type" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "a", "1" };
    _ = try storage.xadd("mystream", "*", &fields, null, .{});

    try std.testing.expectEqual(ValueType.stream, storage.getType("mystream").?);
}

// ── Helper functions for DUMP/RESTORE serialization ─────────────────────────

/// Write a length-prefixed blob
fn writeBlob(w: anytype, data: []const u8) !void {
    try w.writeInt(u32, @intCast(data.len), .little);
    try w.writeAll(data);
}

/// Read a length-prefixed blob; caller owns returned memory
fn readBlob(data: []const u8, pos: *usize, allocator: std.mem.Allocator) ![]u8 {
    if (pos.* + 4 > data.len) return error.InvalidDumpPayload;
    const len = std.mem.readInt(u32, data[pos.*..][0..4], .little);
    pos.* += 4;
    if (pos.* + len > data.len) return error.InvalidDumpPayload;
    const blob = try allocator.dupe(u8, data[pos.* .. pos.* + len]);
    pos.* += len;
    return blob;
}

// ── Unit tests for DUMP/RESTORE/COPY/TOUCH ──────────────────────────────────

test "storage - dump and restore string" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("mykey", "hello", null);

    const dump = (try storage.dumpValue(allocator, "mykey")).?;
    defer allocator.free(dump);

    try storage.restoreValue("newkey", dump, 0, false, false);

    const value = storage.get("newkey").?;
    try std.testing.expectEqualStrings("hello", value);
}

test "storage - xinfoConsumers returns consumer list with timing fields" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create stream and group
    const fields = [_][]const u8{ "x", "1" };
    _ = try storage.xadd("s", "1000-0", &fields, null, .{});
    try storage.xgroupCreate("s", "g", "0");

    // Create consumer via XREADGROUP
    _ = try storage.xreadgroup(allocator, "g", "c1", "s", ">", null, false);

    // Get consumer info
    const info = try storage.xinfoConsumers(allocator, "s", "g");
    defer allocator.free(info);

    // Should contain consumer "c1" with pending, idle, inactive fields
    try std.testing.expect(std.mem.indexOf(u8, info, "c1") != null);
    try std.testing.expect(std.mem.indexOf(u8, info, "pending") != null);
    try std.testing.expect(std.mem.indexOf(u8, info, "idle") != null);
    try std.testing.expect(std.mem.indexOf(u8, info, "inactive") != null);
}

test "storage - xinfoConsumers returns NOGROUP for missing group" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "x", "1" };
    _ = try storage.xadd("s", "1000-0", &fields, null, .{});

    const result = storage.xinfoConsumers(allocator, "s", "nogroup");
    try std.testing.expectError(error.NoGroup, result);
}

test "storage - xinfoGroups returns group list with lag" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create stream with 3 entries
    const fields = [_][]const u8{ "x", "1" };
    _ = try storage.xadd("s", "1000-0", &fields, null, .{});
    _ = try storage.xadd("s", "1001-0", &fields, null, .{});
    _ = try storage.xadd("s", "1002-0", &fields, null, .{});

    // Create group starting at 0
    try storage.xgroupCreate("s", "g", "0");

    // Read 1 entry
    if (try storage.xreadgroup(allocator, "g", "c1", "s", ">", 1, false)) |entries_const| {
        var entries = entries_const;
        for (entries.items) |*entry| {
            entry.deinit(allocator);
        }
        entries.deinit(allocator);
    }

    // Get group info
    const info = try storage.xinfoGroups(allocator, "s");
    defer allocator.free(info);

    // Should contain group "g" with lag field
    try std.testing.expect(std.mem.indexOf(u8, info, "name") != null);
    try std.testing.expect(std.mem.indexOf(u8, info, "consumers") != null);
    try std.testing.expect(std.mem.indexOf(u8, info, "pending") != null);
    try std.testing.expect(std.mem.indexOf(u8, info, "entries-read") != null);
    try std.testing.expect(std.mem.indexOf(u8, info, "lag") != null);
}

test "storage - xinfoGroups lag is null for arbitrary start" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "x", "1" };
    _ = try storage.xadd("s", "1000-0", &fields, null, .{});
    _ = try storage.xadd("s", "2000-0", &fields, null, .{});

    // Create group at arbitrary position (not 0 or $)
    try storage.xgroupCreate("s", "g", "1500-0");

    const info = try storage.xinfoGroups(allocator, "s");
    defer allocator.free(info);

    // Lag should be null ($-1 in RESP)
    try std.testing.expect(std.mem.indexOf(u8, info, "$-1") != null);
}

test "storage - xinfoGroups returns empty array for missing key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const info = try storage.xinfoGroups(allocator, "nosuchkey");
    defer allocator.free(info);

    try std.testing.expectEqualStrings("*0\r\n", info);
}

test "storage - xgroupSetId with ENTRIESREAD enables accurate lag" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "x", "1" };
    _ = try storage.xadd("s", "1000-0", &fields, null, .{});
    _ = try storage.xadd("s", "2000-0", &fields, null, .{});
    _ = try storage.xadd("s", "3000-0", &fields, null, .{});

    // Create group at arbitrary position — lag would be null
    try storage.xgroupCreate("s", "g", "1500-0");

    // XGROUP SETID with ENTRIESREAD 1 (we've read 1 entry logically)
    try storage.xgroupSetId("s", "g", "1500-0", 1);

    const info = try storage.xinfoGroups(allocator, "s");
    defer allocator.free(info);

    // Lag should now be available (not null): entries_added=3, entries_read=1, lag=2
    try std.testing.expect(std.mem.indexOf(u8, info, "$-1") == null); // no null lag
    try std.testing.expect(std.mem.indexOf(u8, info, ":2") != null); // lag is 2
}

test "storage - xgroupSetId without ENTRIESREAD preserves arbitrary_start" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "x", "1" };
    _ = try storage.xadd("s", "1000-0", &fields, null, .{});
    _ = try storage.xadd("s", "2000-0", &fields, null, .{});

    // Create group at arbitrary position
    try storage.xgroupCreate("s", "g", "1500-0");

    // XGROUP SETID without ENTRIESREAD should still be arbitrary
    try storage.xgroupSetId("s", "g", "1800-0", null);

    const info = try storage.xinfoGroups(allocator, "s");
    defer allocator.free(info);

    // Lag should be null because arbitrary_start is still true
    try std.testing.expect(std.mem.indexOf(u8, info, "$-1") != null);
}

test "storage - dump and restore list" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    const dump = (try storage.dumpValue(allocator, "mylist")).?;
    defer allocator.free(dump);

    try storage.restoreValue("newlist", dump, 0, false, false);

    const len = storage.llen("newlist").?;
    try std.testing.expectEqual(@as(usize, 3), len);
}

test "storage - dump and restore with TTL" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("mykey", "value", null);

    const dump = (try storage.dumpValue(allocator, "mykey")).?;
    defer allocator.free(dump);

    // Restore with 1 hour TTL
    try storage.restoreValue("newkey", dump, 3600 * 1000, false, false);

    const value = storage.get("newkey").?;
    try std.testing.expectEqualStrings("value", value);

    // Check expiration is set
    const ttl = storage.getTtlMs("newkey");
    try std.testing.expect(ttl > 0);
}

test "storage - restore fails if key exists" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("key1", "value1", null);
    try storage.set("key2", "value2", null);

    const dump = (try storage.dumpValue(allocator, "key1")).?;
    defer allocator.free(dump);

    // Should fail without replace flag
    const result = storage.restoreValue("key2", dump, 0, false, false);
    try std.testing.expectError(error.KeyAlreadyExists, result);
}

test "storage - restore with replace overwrites" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("key1", "value1", null);
    try storage.set("key2", "old", null);

    const dump = (try storage.dumpValue(allocator, "key1")).?;
    defer allocator.free(dump);

    // Should succeed with replace flag
    try storage.restoreValue("key2", dump, 0, true, false);

    const value = storage.get("key2").?;
    try std.testing.expectEqualStrings("value1", value);
}

test "storage - restore with ABSTTL uses absolute timestamp" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("src", "absttl_val", null);
    const dump = (try storage.dumpValue(allocator, "src")).?;
    defer allocator.free(dump);

    // Use a far-future absolute timestamp (year 2100)
    const far_future_ms: i64 = 4102444800000; // 2100-01-01 00:00:00 UTC in ms
    try storage.restoreValue("dst_absttl", dump, far_future_ms, false, true);

    const value = storage.get("dst_absttl").?;
    try std.testing.expectEqualStrings("absttl_val", value);

    // TTL should be large (close to far_future_ms - now)
    const ttl = storage.getTtlMs("dst_absttl");
    try std.testing.expect(ttl > 0);
    try std.testing.expect(ttl > 1000 * 60 * 60 * 24 * 365); // more than 1 year remaining
}

test "storage - restore ABSTTL with past timestamp expires key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("src2", "expired_val", null);
    const dump = (try storage.dumpValue(allocator, "src2")).?;
    defer allocator.free(dump);

    // Past timestamp (year 2000)
    const past_ms: i64 = 946684800000; // 2000-01-01 00:00:00 UTC in ms
    try storage.restoreValue("dst_expired", dump, past_ms, false, true);

    // Key should be expired (not accessible)
    const value = storage.get("dst_expired");
    try std.testing.expect(value == null);
}

test "storage - copy key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("source", "hello", null);

    const success = try storage.copyKey("source", "dest", false);
    try std.testing.expect(success);

    const value = storage.get("dest").?;
    try std.testing.expectEqualStrings("hello", value);

    // Verify source still exists
    const source_value = storage.get("source").?;
    try std.testing.expectEqualStrings("hello", source_value);
}

test "storage - copy fails if destination exists" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("source", "value1", null);
    try storage.set("dest", "value2", null);

    const success = try storage.copyKey("source", "dest", false);
    try std.testing.expect(!success);

    // Destination should be unchanged
    const value = storage.get("dest").?;
    try std.testing.expectEqualStrings("value2", value);
}

test "storage - copy with replace overwrites" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("source", "new_value", null);
    try storage.set("dest", "old_value", null);

    const success = try storage.copyKey("source", "dest", true);
    try std.testing.expect(success);

    const value = storage.get("dest").?;
    try std.testing.expectEqualStrings("new_value", value);
}

test "storage - copy key to different storage (cross-database)" {
    const allocator = std.testing.allocator;
    var storage1 = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage1.deinit();
    var storage2 = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage2.deinit();

    try storage1.set("mykey", "hello", null);

    const success = try storage1.copyKeyToStorage("mykey", &storage2, "mykey", false);
    try std.testing.expect(success);

    // Verify in destination storage
    const value = storage2.get("mykey").?;
    try std.testing.expectEqualStrings("hello", value);

    // Verify source still exists
    const source_value = storage1.get("mykey").?;
    try std.testing.expectEqualStrings("hello", source_value);
}

test "storage - cross-database copy fails if destination exists" {
    const allocator = std.testing.allocator;
    var storage1 = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage1.deinit();
    var storage2 = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage2.deinit();

    try storage1.set("source", "value1", null);
    try storage2.set("dest", "value2", null);

    const success = try storage1.copyKeyToStorage("source", &storage2, "dest", false);
    try std.testing.expect(!success);

    // Destination should be unchanged
    const value = storage2.get("dest").?;
    try std.testing.expectEqualStrings("value2", value);
}

test "storage - cross-database copy with replace overwrites" {
    const allocator = std.testing.allocator;
    var storage1 = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage1.deinit();
    var storage2 = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage2.deinit();

    try storage1.set("source", "new_value", null);
    try storage2.set("dest", "old_value", null);

    const success = try storage1.copyKeyToStorage("source", &storage2, "dest", true);
    try std.testing.expect(success);

    const value = storage2.get("dest").?;
    try std.testing.expectEqualStrings("new_value", value);
}

test "storage - cross-database copy list" {
    const allocator = std.testing.allocator;
    var storage1 = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage1.deinit();
    var storage2 = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage2.deinit();

    const items = [_][]const u8{ "a", "b", "c" };
    _ = try storage1.lpush("mylist", &items);

    const success = try storage1.copyKeyToStorage("mylist", &storage2, "mylist", false);
    try std.testing.expect(success);

    // Verify list in destination
    const list_len = storage2.llen("mylist") catch 0;
    try std.testing.expectEqual(@as(usize, 3), list_len);
}

test "storage - cross-database copy hash" {
    const allocator = std.testing.allocator;
    var storage1 = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage1.deinit();
    var storage2 = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage2.deinit();

    const fields = [_][]const u8{ "f1", "f2" };
    const values = [_][]const u8{ "v1", "v2" };
    _ = try storage1.hset("myhash", &fields, &values, null);

    const success = try storage1.copyKeyToStorage("myhash", &storage2, "myhash", false);
    try std.testing.expect(success);

    // Verify hash in destination
    if (storage2.hget("myhash", "f1")) |val| {
        try std.testing.expectEqualStrings("v1", val);
    } else {
        try std.testing.expect(false); // Should not reach here
    }
}

test "storage - cross-database copy nonexistent key returns error" {
    const allocator = std.testing.allocator;
    var storage1 = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage1.deinit();
    var storage2 = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage2.deinit();

    const result = storage1.copyKeyToStorage("nonexistent", &storage2, "dest", false);
    try std.testing.expectError(error.NoSuchKey, result);
}

test "storage - touch counts existing keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("key1", "a", null);
    try storage.set("key2", "b", null);

    const keys = [_][]const u8{ "key1", "key2", "key3" };
    const count = storage.touch(&keys);

    try std.testing.expectEqual(@as(usize, 2), count);
}

test "BITOP DIFF operation" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("k1", &[_]u8{0xF0}, null);
    try storage.set("k2", &[_]u8{0xAA}, null);

    const len = try storage.bitop(.DIFF, "result", &[_][]const u8{ "k1", "k2" });
    try std.testing.expectEqual(@as(usize, 1), len);
    const val = storage.get("result").?;
    try std.testing.expectEqual(@as(u8, 0x50), val.string.data[0]);
}

test "BITOP DIFF1 operation" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("k1", &[_]u8{0xF0}, null);
    try storage.set("k2", &[_]u8{0xAA}, null);

    const len = try storage.bitop(.DIFF1, "result", &[_][]const u8{ "k1", "k2" });
    try std.testing.expectEqual(@as(usize, 1), len);
    const val = storage.get("result").?;
    try std.testing.expectEqual(@as(u8, 0x50), val.string.data[0]);
}

test "BITOP ANDOR operation" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("k1", &[_]u8{0xF0}, null);
    try storage.set("k2", &[_]u8{0x0F}, null);
    try storage.set("k3", &[_]u8{0xFF}, null);
    try storage.set("k4", &[_]u8{0xAA}, null);

    const len = try storage.bitop(.ANDOR, "result", &[_][]const u8{ "k1", "k2", "k3", "k4" });
    try std.testing.expectEqual(@as(usize, 1), len);
    const val = storage.get("result").?;
    try std.testing.expectEqual(@as(u8, 0xAA), val.string.data[0]);
}

test "BITOP ONE operation" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("k1", &[_]u8{0xF0}, null);
    try storage.set("k2", &[_]u8{0x0F}, null);
    try storage.set("k3", &[_]u8{0xAA}, null);

    const len = try storage.bitop(.ONE, "result", &[_][]const u8{ "k1", "k2", "k3" });
    try std.testing.expectEqual(@as(usize, 1), len);
    const val = storage.get("result").?;
    try std.testing.expectEqual(@as(u8, 12), val.string.data[0]);
}

// Keyspace notification tests for expired and evicted events

test "keyspace notifications - lazy expiration (get)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Enable expired notifications
    const flags = notifications_mod.parseNotificationFlags("Kx");
    storage.notification_flags.store(flags, .monotonic);

    const now = Storage.getCurrentTimestamp();
    const expires_at = now - 1000; // Already expired

    // Set an expired key
    try storage.set("mykey", "myvalue", expires_at);

    // Access the expired key (lazy expiration should fire event)
    const result = storage.get("mykey");
    try std.testing.expect(result == null);

    // Key should be deleted
    try std.testing.expect(!storage.exists("mykey"));
}

test "keyspace notifications - lazy expiration (exists)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Enable expired notifications
    const flags = notifications_mod.parseNotificationFlags("Ex");
    storage.notification_flags.store(flags, .monotonic);

    const now = Storage.getCurrentTimestamp();
    const expires_at = now - 1000;

    try storage.set("key1", "value1", expires_at);

    // Check existence should trigger lazy expiration
    const exists = storage.exists("key1");
    try std.testing.expect(!exists);

    // Key should be deleted
    try std.testing.expect(storage.get("key1") == null);
}

test "keyspace notifications - active expiration" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Enable expired notifications
    const flags = notifications_mod.parseNotificationFlags("Ex");
    storage.notification_flags.store(flags, .monotonic);

    const now = Storage.getCurrentTimestamp();

    // Set multiple keys with different expirations
    try storage.set("key1", "value1", now - 1000); // Expired
    try storage.set("key2", "value2", now + 10000); // Not expired
    try storage.set("key3", "value3", now - 500); // Expired

    // Run active expiration
    const count = storage.evictExpired();
    try std.testing.expectEqual(@as(usize, 2), count);

    // Verify only non-expired key remains
    try std.testing.expect(storage.get("key1") == null);
    try std.testing.expect(storage.get("key2") != null);
    try std.testing.expect(storage.get("key3") == null);
}

test "keyspace notifications - disabled when flags are zero" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // notification_flags should be 0 by default - no overhead
    storage.notification_flags.store(0, .monotonic);

    const now = Storage.getCurrentTimestamp();
    const expires_at = now - 1000;

    try storage.set("key1", "value1", expires_at);

    // Get should handle expired key without notification overhead
    const result = storage.get("key1");
    try std.testing.expect(result == null);

    try std.testing.expect(!storage.exists("key1"));
}

test "keyspace notifications - expired flag required" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Enable keyspace/keyevent but NOT expired flag
    const flags = notifications_mod.parseNotificationFlags("KE");
    storage.notification_flags.store(flags, .monotonic);

    const now = Storage.getCurrentTimestamp();
    const expires_at = now - 1000;

    try storage.set("key1", "value1", expires_at);

    // Expired event should not fire (flag not enabled)
    const result = storage.get("key1");
    try std.testing.expect(result == null);
}

test "keyspace notifications - evicted event infrastructure" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Enable evicted notifications
    const flags = notifications_mod.parseNotificationFlags("Ee");
    storage.notification_flags.store(flags, .monotonic);

    // Verify infrastructure is properly configured
    const stored_flags = storage.notification_flags.load(.monotonic);
    try std.testing.expect(stored_flags != 0);
    try std.testing.expect(notifications_mod.shouldNotify(flags, .evicted));

    // Set a key to verify eviction paths work
    try storage.set("test_key", "test_value", null);
    try std.testing.expect(storage.get("test_key") != null);
}

test "LFU decay - counter decreases over time" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set lfu-decay-time to 1 minute
    try storage.config.set(allocator, "lfu-decay-time", "1");

    // Set a key and increment its LFU counter
    try storage.set("key1", "value1", null);
    try storage.lfu_counter.increment("key1");
    try storage.lfu_counter.increment("key1");
    try storage.lfu_counter.increment("key1");

    const initial_counter = storage.lfu_counter.getCounter("key1");
    try std.testing.expect(initial_counter > 0);

    // Manually set last access time to 61 seconds ago (decay_time = 1 minute = 60 seconds)
    const now_ms = Storage.getCurrentTimestamp();
    const old_time_ms = now_ms - (61 * 1000); // 61 seconds ago
    try storage.setKeyAccessTime("key1", old_time_ms);

    // Apply decay
    try storage.applyLfuDecay("key1");

    const decayed_counter = storage.lfu_counter.getCounter("key1");
    // Counter should have decreased by at least 1
    try std.testing.expect(decayed_counter < initial_counter);
}

test "LFU decay - respects lfu_decay_time config" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create two keys
    try storage.set("key_fast_decay", "value1", null);
    try storage.set("key_slow_decay", "value2", null);

    // Set counter to 10 for both
    try storage.lfu_counter.counters.put("key_fast_decay", 10);
    try storage.lfu_counter.counters.put("key_slow_decay", 10);

    const now_ms = Storage.getCurrentTimestamp();
    const old_time_ms = now_ms - (5 * 60 * 1000); // 5 minutes ago

    // Fast decay: lfu_decay_time = 1 minute
    try storage.config.set(allocator, "lfu-decay-time", "1");
    try storage.setKeyAccessTime("key_fast_decay", old_time_ms);
    try storage.applyLfuDecay("key_fast_decay");
    const fast_decay_counter = storage.lfu_counter.getCounter("key_fast_decay");

    // Slow decay: lfu_decay_time = 10 minutes
    try storage.config.set(allocator, "lfu-decay-time", "10");
    try storage.setKeyAccessTime("key_slow_decay", old_time_ms);
    try storage.applyLfuDecay("key_slow_decay");
    const slow_decay_counter = storage.lfu_counter.getCounter("key_slow_decay");

    // Fast decay should have decayed more than slow decay
    try std.testing.expect(fast_decay_counter < slow_decay_counter);
}

test "LFU decay - counter never goes negative" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set(allocator, "lfu-decay-time", "1");

    try storage.set("key1", "value1", null);
    // Set counter to 1
    try storage.lfu_counter.counters.put("key1", 1);

    const now_ms = Storage.getCurrentTimestamp();
    // Simulate 10 decay periods (10 minutes with 1 minute decay_time)
    const very_old_time_ms = now_ms - (10 * 60 * 1000);
    try storage.setKeyAccessTime("key1", very_old_time_ms);

    try storage.applyLfuDecay("key1");

    const final_counter = storage.lfu_counter.getCounter("key1");
    // Counter should never be negative
    try std.testing.expect(final_counter >= 0);
    // Counter should be 0 after sufficient decay
    try std.testing.expectEqual(@as(u8, 0), final_counter);
}

test "LFU decay - recent access prevents decay" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set(allocator, "lfu-decay-time", "1");

    try storage.set("key1", "value1", null);
    try storage.lfu_counter.counters.put("key1", 5);

    // Set access time to NOW
    const now_ms = Storage.getCurrentTimestamp();
    try storage.setKeyAccessTime("key1", now_ms);

    const initial_counter = storage.lfu_counter.getCounter("key1");
    try storage.applyLfuDecay("key1");
    const after_decay = storage.lfu_counter.getCounter("key1");

    // Counter should be unchanged when accessed recently
    try std.testing.expectEqual(initial_counter, after_decay);
}

test "LFU decay - multiple periods accumulate" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.config.set(allocator, "lfu-decay-time", "1");

    try storage.set("key1", "value1", null);
    try storage.lfu_counter.counters.put("key1", 10);

    const now_ms = Storage.getCurrentTimestamp();
    // Simulate 5 minutes elapsed with 1 minute decay_time
    const old_time_ms = now_ms - (5 * 60 * 1000);
    try storage.setKeyAccessTime("key1", old_time_ms);

    try storage.applyLfuDecay("key1");

    const decayed_counter = storage.lfu_counter.getCounter("key1");
    // With decay_time=1 min and elapsed=5 min, counter should decrease by ~5
    // (formula: counter -= elapsed_ms / (decay_time_ms))
    // This test allows some margin for rounding
    try std.testing.expect(decayed_counter <= 5);
}

// ===== HOTKEY TRACKER TESTS =====

test "HotkeyTracker init creates tracker with correct config" {
    const allocator = std.testing.allocator;

    const config = HotkeyTracker.TrackerConfig{
        .metrics_count = 2,
        .track_cpu = true,
        .track_net = true,
        .top_k = 10,
        .sample_ratio = 100,
        .duration_ms = null,
    };

    const tracker = try HotkeyTracker.init(allocator, config);
    defer tracker.deinit();

    try std.testing.expect(!tracker.is_active);
    try std.testing.expectEqual(@as(u32, 2), tracker.metrics_count);
    try std.testing.expect(tracker.track_cpu);
    try std.testing.expect(tracker.track_net);
    try std.testing.expectEqual(@as(u32, 10), tracker.top_k);
    try std.testing.expectEqual(@as(u32, 100), tracker.sample_ratio);
    try std.testing.expect(tracker.duration_ms == null);
    try std.testing.expectEqual(@as(u64, 0), tracker.keys_sampled);
    try std.testing.expectEqual(@as(u64, 0), tracker.total_cpu_us);
    try std.testing.expectEqual(@as(u64, 0), tracker.total_net_bytes);
}

test "HotkeyTracker start marks active and records time" {
    const allocator = std.testing.allocator;

    const config = HotkeyTracker.TrackerConfig{
        .metrics_count = 1,
        .track_cpu = true,
        .track_net = false,
        .top_k = 5,
        .sample_ratio = 50,
        .duration_ms = 60000,
    };

    const tracker = try HotkeyTracker.init(allocator, config);
    defer tracker.deinit();

    try std.testing.expect(!tracker.is_active);

    tracker.start();

    try std.testing.expect(tracker.is_active);
    try std.testing.expect(tracker.start_time_ms > 0);
}

test "HotkeyTracker stop marks inactive" {
    const allocator = std.testing.allocator;

    const config = HotkeyTracker.TrackerConfig{
        .metrics_count = 1,
        .track_cpu = true,
        .track_net = false,
        .top_k = 5,
        .sample_ratio = 50,
        .duration_ms = null,
    };

    const tracker = try HotkeyTracker.init(allocator, config);
    defer tracker.deinit();

    tracker.start();
    try std.testing.expect(tracker.is_active);

    tracker.stop();
    try std.testing.expect(!tracker.is_active);
}

test "HotkeyTracker reset clears counters" {
    const allocator = std.testing.allocator;

    const config = HotkeyTracker.TrackerConfig{
        .metrics_count = 2,
        .track_cpu = true,
        .track_net = true,
        .top_k = 10,
        .sample_ratio = 100,
        .duration_ms = null,
    };

    const tracker = try HotkeyTracker.init(allocator, config);
    defer tracker.deinit();

    tracker.start();
    tracker.recordAccess("key1", 100, 200);
    tracker.recordAccess("key2", 50, 150);

    try std.testing.expectEqual(@as(u64, 2), tracker.keys_sampled);
    try std.testing.expectEqual(@as(u64, 150), tracker.total_cpu_us);
    try std.testing.expectEqual(@as(u64, 350), tracker.total_net_bytes);

    tracker.reset();

    try std.testing.expectEqual(@as(u64, 0), tracker.keys_sampled);
    try std.testing.expectEqual(@as(u64, 0), tracker.total_cpu_us);
    try std.testing.expectEqual(@as(u64, 0), tracker.total_net_bytes);
    try std.testing.expectEqual(@as(i64, 0), tracker.start_time_ms);
}

test "HotkeyTracker recordAccess with CPU metric" {
    const allocator = std.testing.allocator;

    const config = HotkeyTracker.TrackerConfig{
        .metrics_count = 1,
        .track_cpu = true,
        .track_net = false,
        .top_k = 10,
        .sample_ratio = 100,
        .duration_ms = null,
    };

    const tracker = try HotkeyTracker.init(allocator, config);
    defer tracker.deinit();

    tracker.recordAccess("key1", 123, 456);

    try std.testing.expectEqual(@as(u64, 1), tracker.keys_sampled);
    try std.testing.expectEqual(@as(u64, 123), tracker.total_cpu_us);
    try std.testing.expectEqual(@as(u64, 0), tracker.total_net_bytes); // NET not tracked
}

test "HotkeyTracker recordAccess with NET metric" {
    const allocator = std.testing.allocator;

    const config = HotkeyTracker.TrackerConfig{
        .metrics_count = 1,
        .track_cpu = false,
        .track_net = true,
        .top_k = 10,
        .sample_ratio = 100,
        .duration_ms = null,
    };

    const tracker = try HotkeyTracker.init(allocator, config);
    defer tracker.deinit();

    tracker.recordAccess("key1", 123, 456);

    try std.testing.expectEqual(@as(u64, 1), tracker.keys_sampled);
    try std.testing.expectEqual(@as(u64, 0), tracker.total_cpu_us); // CPU not tracked
    try std.testing.expectEqual(@as(u64, 456), tracker.total_net_bytes);
}

test "HotkeyTracker recordAccess with both CPU and NET metrics" {
    const allocator = std.testing.allocator;

    const config = HotkeyTracker.TrackerConfig{
        .metrics_count = 2,
        .track_cpu = true,
        .track_net = true,
        .top_k = 10,
        .sample_ratio = 100,
        .duration_ms = null,
    };

    const tracker = try HotkeyTracker.init(allocator, config);
    defer tracker.deinit();

    tracker.recordAccess("key1", 100, 200);
    tracker.recordAccess("key2", 50, 150);
    tracker.recordAccess("key3", 75, 100);

    try std.testing.expectEqual(@as(u64, 3), tracker.keys_sampled);
    try std.testing.expectEqual(@as(u64, 225), tracker.total_cpu_us); // 100 + 50 + 75
    try std.testing.expectEqual(@as(u64, 450), tracker.total_net_bytes); // 200 + 150 + 100
}

test "HotkeyTracker isExpired returns false when not active" {
    const allocator = std.testing.allocator;

    const config = HotkeyTracker.TrackerConfig{
        .metrics_count = 1,
        .track_cpu = true,
        .track_net = false,
        .top_k = 10,
        .sample_ratio = 100,
        .duration_ms = 1000,
    };

    const tracker = try HotkeyTracker.init(allocator, config);
    defer tracker.deinit();

    // Not active, should return false regardless of duration
    try std.testing.expect(!tracker.isExpired());
}

test "HotkeyTracker isExpired returns false when duration is null" {
    const allocator = std.testing.allocator;

    const config = HotkeyTracker.TrackerConfig{
        .metrics_count = 1,
        .track_cpu = true,
        .track_net = false,
        .top_k = 10,
        .sample_ratio = 100,
        .duration_ms = null,
    };

    const tracker = try HotkeyTracker.init(allocator, config);
    defer tracker.deinit();

    tracker.start();

    // Active but no duration set
    try std.testing.expect(!tracker.isExpired());
}

test "HotkeyTracker state transitions: init -> start -> stop -> reset" {
    const allocator = std.testing.allocator;

    const config = HotkeyTracker.TrackerConfig{
        .metrics_count = 2,
        .track_cpu = true,
        .track_net = true,
        .top_k = 10,
        .sample_ratio = 100,
        .duration_ms = null,
    };

    const tracker = try HotkeyTracker.init(allocator, config);
    defer tracker.deinit();

    // Initial state: not active, zero counters
    try std.testing.expect(!tracker.is_active);
    try std.testing.expectEqual(@as(u64, 0), tracker.keys_sampled);

    // After start: active
    tracker.start();
    try std.testing.expect(tracker.is_active);

    // Record some data
    tracker.recordAccess("key1", 100, 200);
    try std.testing.expectEqual(@as(u64, 1), tracker.keys_sampled);
    try std.testing.expectEqual(@as(u64, 100), tracker.total_cpu_us);
    try std.testing.expectEqual(@as(u64, 200), tracker.total_net_bytes);

    // After stop: inactive but data preserved
    tracker.stop();
    try std.testing.expect(!tracker.is_active);
    try std.testing.expectEqual(@as(u64, 1), tracker.keys_sampled);
    try std.testing.expectEqual(@as(u64, 100), tracker.total_cpu_us);
    try std.testing.expectEqual(@as(u64, 200), tracker.total_net_bytes);

    // After reset: counters cleared
    tracker.reset();
    try std.testing.expectEqual(@as(u64, 0), tracker.keys_sampled);
    try std.testing.expectEqual(@as(u64, 0), tracker.total_cpu_us);
    try std.testing.expectEqual(@as(u64, 0), tracker.total_net_bytes);
}

test "HotkeyTracker multiple recordAccess accumulates correctly" {
    const allocator = std.testing.allocator;

    const config = HotkeyTracker.TrackerConfig{
        .metrics_count = 2,
        .track_cpu = true,
        .track_net = true,
        .top_k = 10,
        .sample_ratio = 100,
        .duration_ms = null,
    };

    const tracker = try HotkeyTracker.init(allocator, config);
    defer tracker.deinit();

    // Record multiple accesses
    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        tracker.recordAccess("key", 10, 20);
    }

    try std.testing.expectEqual(@as(u64, 10), tracker.keys_sampled);
    try std.testing.expectEqual(@as(u64, 100), tracker.total_cpu_us);
    try std.testing.expectEqual(@as(u64, 200), tracker.total_net_bytes);
}

test "storage - xadd with NOMKSTREAM returns null when stream doesn't exist" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const fields = [_][]const u8{ "a", "1" };
    const opts = XAddOptions{ .nomkstream = true };
    const result = try storage.xadd("nonexistent", "*", &fields, null, opts);

    try std.testing.expectEqual(@as(?Value.StreamId, null), result);
}

test "storage - xadd with NOMKSTREAM adds when stream exists" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Create stream first
    const fields1 = [_][]const u8{ "a", "1" };
    _ = try storage.xadd("mystream", "1000-0", &fields1, null, .{});

    // Add with NOMKSTREAM to existing
    const fields2 = [_][]const u8{ "b", "2" };
    const opts = XAddOptions{ .nomkstream = true };
    const result = try storage.xadd("mystream", "2000-0", &fields2, null, opts);

    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(i64, 2000), result.?.ms);
}

test "storage - xtrimByMinId removes entries less than threshold" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Add entries at 1000-0, 2000-0, 3000-0
    const ids = [_][]const u8{ "1000-0", "2000-0", "3000-0" };
    for (ids) |id| {
        const fields = [_][]const u8{ "f", "v" };
        _ = try storage.xadd("s", id, &fields, null, .{});
    }

    // Trim entries < 2500-0 (should remove 1000-0 and 2000-0)
    const deleted = try storage.xtrimMinid("s", "2500-0", null);

    try std.testing.expectEqual(@as(usize, 2), deleted);

    // Verify remaining count
    const len = try storage.xlen("s");
    try std.testing.expectEqual(@as(?usize, 1), len);
}

test "storage - xtrimByMinId with LIMIT respects deletion limit" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Add 5 entries: 1000-0 through 5000-0
    var i: u32 = 1000;
    while (i <= 5000) : (i += 1000) {
        const fields = [_][]const u8{ "f", "v" };
        const id = try std.fmt.allocPrint(allocator, "{d}-0", .{i});
        defer allocator.free(id);
        _ = try storage.xadd("s", id, &fields, null, .{});
    }

    // Trim with LIMIT 2 (should delete at most 2)
    const deleted = try storage.xtrimMinid("s", "3500-0", 2);

    try std.testing.expectEqual(@as(usize, 2), deleted);

    // Verify remaining count: 3 (3000-0, 4000-0, 5000-0)
    const len = try storage.xlen("s");
    try std.testing.expectEqual(@as(?usize, 3), len);
}

test "storage - xtrimByMaxlen with LIMIT respects deletion limit" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Add 5 entries
    var i: u32 = 1000;
    while (i <= 5000) : (i += 1000) {
        const fields = [_][]const u8{ "f", "v" };
        const id = try std.fmt.allocPrint(allocator, "{d}-0", .{i});
        defer allocator.free(id);
        _ = try storage.xadd("s", id, &fields, null, .{});
    }

    // Trim to 1 entry with LIMIT 2 (should delete at most 2)
    const deleted = try storage.xtrimMaxlen("s", 1, 2);

    try std.testing.expectEqual(@as(usize, 2), deleted);

    // Verify remaining count: 3
    const len = try storage.xlen("s");
    try std.testing.expectEqual(@as(?usize, 3), len);
}

// ─────────────────────────────────────────────────────────────────────────────
// bitcount BIT mode tests (Iteration 281 — Redis 7.0)
// ─────────────────────────────────────────────────────────────────────────────

test "storage - bitcount BYTE mode full string" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("k", "\xFF\x00", null);
    // All bits in 0xFF = 8, all bits in 0x00 = 0 → total 8
    const total = try storage.bitcount("k", null, null, .byte);
    try std.testing.expectEqual(@as(i64, 8), total);
}

test "storage - bitcount BYTE mode range" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("k", "\xFF\x00", null);
    // Only byte 0 → 8 bits
    const byte0 = try storage.bitcount("k", 0, 0, .byte);
    try std.testing.expectEqual(@as(i64, 8), byte0);

    // Only byte 1 → 0 bits
    const byte1 = try storage.bitcount("k", 1, 1, .byte);
    try std.testing.expectEqual(@as(i64, 0), byte1);
}

test "storage - bitcount BIT mode full byte" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("k", "\xFF", null);
    // BIT mode bits 0-7 = all 8 bits of 0xFF
    const all = try storage.bitcount("k", 0, 7, .bit);
    try std.testing.expectEqual(@as(i64, 8), all);
}

test "storage - bitcount BIT mode single bit" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // 0x80 = 10000000: MSB set, rest clear
    try storage.set("k", "\x80", null);

    // Bit 0 (MSB) → 1
    const msb = try storage.bitcount("k", 0, 0, .bit);
    try std.testing.expectEqual(@as(i64, 1), msb);

    // Bits 1-7 → 0
    const rest = try storage.bitcount("k", 1, 7, .bit);
    try std.testing.expectEqual(@as(i64, 0), rest);
}

test "storage - bitcount BIT mode cross-byte" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // "\xFF\xFF": all 16 bits set
    try storage.set("k", "\xFF\xFF", null);

    // Bits 4-11 (8 bits spanning both bytes, all set) → 8
    const cross = try storage.bitcount("k", 4, 11, .bit);
    try std.testing.expectEqual(@as(i64, 8), cross);
}

test "storage - bitcount BIT mode negative indices" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // 0xF0 = 11110000: bits 0-3 set, bits 4-7 clear
    try storage.set("k", "\xF0", null);

    // Bits -4 to -1 (bits 4-7, the clear bits) → 0
    const clear_bits = try storage.bitcount("k", -4, -1, .bit);
    try std.testing.expectEqual(@as(i64, 0), clear_bits);

    // Bits -8 to -5 (bits 0-3, the set bits) → 4
    const set_bits = try storage.bitcount("k", -8, -5, .bit);
    try std.testing.expectEqual(@as(i64, 4), set_bits);
}

test "storage - bitcount BIT mode empty range returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("k", "\xFF", null);
    // start > end after normalization → 0
    const empty = try storage.bitcount("k", 5, 3, .bit);
    try std.testing.expectEqual(@as(i64, 0), empty);
}

test "storage - bitcount non-existent key returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try storage.bitcount("nosuchkey", null, null, .byte);
    try std.testing.expectEqual(@as(i64, 0), result);
}

test "storage - bitcount WRONGTYPE error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.lpush("mylist", &[_][]const u8{"value"});
    const result = storage.bitcount("mylist", null, null, .byte);
    try std.testing.expectError(error.WrongType, result);
}

// ── Keyspace hits/misses tracking tests ──────────────────────────────────────

test "keyspace hits - GET on existing string key increments hit counter" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("mykey", "hello", null);
    const initial_hits = storage.getKeyspaceHits();
    const initial_misses = storage.getKeyspaceMisses();

    _ = storage.get("mykey");

    try std.testing.expectEqual(initial_hits + 1, storage.getKeyspaceHits());
    try std.testing.expectEqual(initial_misses, storage.getKeyspaceMisses());
}

test "keyspace misses - GET on non-existent key increments miss counter" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const initial_hits = storage.getKeyspaceHits();
    const initial_misses = storage.getKeyspaceMisses();

    _ = storage.get("nosuchkey");

    try std.testing.expectEqual(initial_hits, storage.getKeyspaceHits());
    try std.testing.expectEqual(initial_misses + 1, storage.getKeyspaceMisses());
}

test "keyspace hits/misses - multiple GETs accumulate correctly" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("k1", "v1", null);
    try storage.set("k2", "v2", null);

    _ = storage.get("k1");   // hit
    _ = storage.get("k2");   // hit
    _ = storage.get("k3");   // miss
    _ = storage.get("k4");   // miss
    _ = storage.get("k5");   // miss

    try std.testing.expectEqual(@as(u64, 2), storage.getKeyspaceHits());
    try std.testing.expectEqual(@as(u64, 3), storage.getKeyspaceMisses());
}

test "keyspace stats - resetKeyspaceStats clears both counters" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("mykey", "hello", null);
    _ = storage.get("mykey");
    _ = storage.get("nosuchkey");

    // Verify counters are non-zero
    try std.testing.expect(storage.getKeyspaceHits() > 0);
    try std.testing.expect(storage.getKeyspaceMisses() > 0);

    storage.resetKeyspaceStats();

    try std.testing.expectEqual(@as(u64, 0), storage.getKeyspaceHits());
    try std.testing.expectEqual(@as(u64, 0), storage.getKeyspaceMisses());
}

test "keyspace hits - GET on expired key counts as miss" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set with expiry far in the past (1 ms after epoch = already expired)
    const past_time_ms: i64 = 1;
    try storage.set("expiredkey", "value", past_time_ms);

    const initial_misses = storage.getKeyspaceMisses();

    // Attempt to GET the expired key — should count as miss
    _ = storage.get("expiredkey");

    try std.testing.expectEqual(initial_misses + 1, storage.getKeyspaceMisses());
}

test "commandstats - recordCommandStat increments calls and usec" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.recordCommandStat("GET", 10);
    storage.recordCommandStat("GET", 20);
    storage.recordCommandStat("SET", 5);

    const snap = try storage.snapshotCommandStats(allocator);
    defer allocator.free(snap);

    var found_get = false;
    var found_set = false;
    for (snap) |item| {
        if (std.mem.eql(u8, item.name, "GET")) {
            try std.testing.expectEqual(@as(u64, 2), item.entry.calls);
            try std.testing.expectEqual(@as(u64, 30), item.entry.usec);
            found_get = true;
        }
        if (std.mem.eql(u8, item.name, "SET")) {
            try std.testing.expectEqual(@as(u64, 1), item.entry.calls);
            try std.testing.expectEqual(@as(u64, 5), item.entry.usec);
            found_set = true;
        }
    }
    try std.testing.expect(found_get);
    try std.testing.expect(found_set);
}

test "commandstats - resetCommandStats zeroes all entries" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.recordCommandStat("GET", 100);
    storage.recordCommandStat("SET", 50);
    storage.resetCommandStats();

    const snap = try storage.snapshotCommandStats(allocator);
    defer allocator.free(snap);

    for (snap) |item| {
        try std.testing.expectEqual(@as(u64, 0), item.entry.calls);
        try std.testing.expectEqual(@as(u64, 0), item.entry.usec);
    }
}

test "errorstats - recordErrorStat increments count per type" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.recordErrorStat("ERR");
    storage.recordErrorStat("ERR");
    storage.recordErrorStat("WRONGTYPE");

    const snap = try storage.snapshotErrorStats(allocator);
    defer allocator.free(snap);

    var err_count: u64 = 0;
    var wt_count: u64 = 0;
    for (snap) |item| {
        if (std.mem.eql(u8, item.error_type, "ERR")) err_count = item.count;
        if (std.mem.eql(u8, item.error_type, "WRONGTYPE")) wt_count = item.count;
    }
    try std.testing.expectEqual(@as(u64, 2), err_count);
    try std.testing.expectEqual(@as(u64, 1), wt_count);
}

test "errorstats - resetCommandStats zeroes error counts" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.recordErrorStat("ERR");
    storage.resetCommandStats();

    const snap = try storage.snapshotErrorStats(allocator);
    defer allocator.free(snap);

    for (snap) |item| {
        try std.testing.expectEqual(@as(u64, 0), item.count);
    }
}

test "getStringWithExpiry - returns null for missing key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const result = try storage.getStringWithExpiry("nokey");
    try std.testing.expect(result == null);
}

test "getStringWithExpiry - returns value and null expiry for string without TTL" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("mykey", "hello", null);
    const result = try storage.getStringWithExpiry("mykey");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("hello", result.?.value);
    try std.testing.expect(result.?.expires_at == null);
}

test "getStringWithExpiry - returns value and expiry for string with TTL" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const future = Storage.getCurrentTimestamp() + 5000;
    try storage.set("mykey", "world", future);
    const result = try storage.getStringWithExpiry("mykey");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("world", result.?.value);
    try std.testing.expect(result.?.expires_at != null);
    try std.testing.expectEqual(future, result.?.expires_at.?);
}

test "getStringWithExpiry - returns WrongType for non-string key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    _ = try storage.lpush("listkey", &[_][]const u8{"a"}, true);
    const result = storage.getStringWithExpiry("listkey");
    try std.testing.expectError(error.WrongType, result);
}

test "getStringWithExpiry - returns null for expired key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const past = Storage.getCurrentTimestamp() - 1000;
    try storage.set("mykey", "expired", past);
    const result = try storage.getStringWithExpiry("mykey");
    try std.testing.expect(result == null);
}

test "storage - expired_keys counter increments on lazy expiration" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try std.testing.expectEqual(@as(u64, 0), storage.getExpiredKeysCount());

    const past = Storage.getCurrentTimestamp() - 1000;
    try storage.set("key1", "val1", past);
    try storage.set("key2", "val2", past);

    // Accessing expired keys triggers lazy deletion and increments the counter
    _ = storage.get("key1");
    try std.testing.expectEqual(@as(u64, 1), storage.getExpiredKeysCount());
    _ = storage.get("key2");
    try std.testing.expectEqual(@as(u64, 2), storage.getExpiredKeysCount());
}

test "storage - expired_keys counter increments on active expiration" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const past = Storage.getCurrentTimestamp() - 1000;
    try storage.set("key1", "val1", past);
    try storage.set("key2", "val2", past);

    // evictExpired sweeps all keys and counts expired ones
    const count = storage.evictExpired();
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectEqual(@as(u64, 2), storage.getExpiredKeysCount());
}

test "storage - run_id is 40 hex chars" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try std.testing.expectEqual(@as(usize, 40), storage.run_id.len);
    for (storage.run_id) |c| {
        const is_hex = (c >= '0' and c <= '9') or (c >= 'a' and c <= 'f');
        try std.testing.expect(is_hex);
    }
}

test "storage - dirty_count increments on set()" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try std.testing.expectEqual(@as(u64, 0), storage.getDirtyCount());

    try storage.set("k1", "v1", null);
    try std.testing.expectEqual(@as(u64, 1), storage.getDirtyCount());

    try storage.set("k1", "v2", null); // overwrite
    try std.testing.expectEqual(@as(u64, 2), storage.getDirtyCount());

    try storage.set("k2", "v2", null);
    try std.testing.expectEqual(@as(u64, 3), storage.getDirtyCount());
}

test "storage - dirty_count increments on del()" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("a", "1", null);
    try storage.set("b", "2", null);
    _ = storage.getDirtyCount(); // reset mental state after 2 sets

    const before = storage.getDirtyCount();
    _ = storage.del(&[_][]const u8{ "a", "b" });
    try std.testing.expectEqual(before + 2, storage.getDirtyCount());

    // del of non-existent key should NOT increment
    const after_del_existing = storage.getDirtyCount();
    _ = storage.del(&[_][]const u8{"nonexistent"});
    try std.testing.expectEqual(after_del_existing, storage.getDirtyCount());
}

test "storage - dirty_count resets after updateLastSaveTime()" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    try storage.set("k1", "v1", null);
    try storage.set("k2", "v2", null);
    try std.testing.expect(storage.getDirtyCount() > 0);

    storage.updateLastSaveTime();
    try std.testing.expectEqual(@as(u64, 0), storage.getDirtyCount());

    // Writes after save increment again
    try storage.set("k3", "v3", null);
    try std.testing.expectEqual(@as(u64, 1), storage.getDirtyCount());
}

test "storage - dirty_count increments on lpush()" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const before = storage.getDirtyCount();
    _ = try storage.lpush("mylist", &[_][]const u8{ "a", "b" }, null);
    try std.testing.expectEqual(before + 1, storage.getDirtyCount());

    _ = try storage.lpush("mylist", &[_][]const u8{"c"}, null);
    try std.testing.expectEqual(before + 2, storage.getDirtyCount());
}

test "storage - dirty_count increments on hset()" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const before = storage.getDirtyCount();
    _ = try storage.hset("myhash", &[_][]const u8{"f1"}, &[_][]const u8{"v1"}, null);
    try std.testing.expectEqual(before + 1, storage.getDirtyCount());
}
