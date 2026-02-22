const std = @import("std");
const config_mod = @import("config.zig");

pub const Config = config_mod.Config;

/// Type of value stored in the key-value store
pub const ValueType = enum {
    string,
    list,
    set,
    hash,
    sorted_set,
    stream,
    hyperloglog,
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

    /// Set value with optional expiration
    /// Uses hash map for O(1) membership testing
    pub const SetValue = struct {
        data: std.StringHashMap(void),
        expires_at: ?i64,

        pub fn deinit(self: *SetValue, allocator: std.mem.Allocator) void {
            // Free all member strings (keys in the hash map)
            var it = self.data.keyIterator();
            while (it.next()) |key| {
                allocator.free(key.*);
            }
            self.data.deinit();
        }
    };

    /// Hash value with optional expiration
    /// Maps field names to values
    pub const HashValue = struct {
        data: std.StringHashMap([]const u8),
        expires_at: ?i64,

        pub fn deinit(self: *HashValue, allocator: std.mem.Allocator) void {
            // Free all field names (keys) and field values
            var it = self.data.iterator();
            while (it.next()) |entry| {
                allocator.free(entry.key_ptr.*);
                allocator.free(entry.value_ptr.*);
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

        /// Parse ID from string like "1234567890-0" or "*" for auto-generation
        pub fn parse(s: []const u8, last_id: ?StreamId) !StreamId {
            if (std.mem.eql(u8, s, "*")) {
                // Auto-generate ID based on current time
                const now = std.time.milliTimestamp();
                const ms = if (last_id) |lid| @max(now, lid.ms) else now;
                const seq = if (last_id) |lid| (if (ms == lid.ms) lid.seq + 1 else 0) else 0;
                return StreamId{ .ms = ms, .seq = seq };
            }

            // Parse "ms-seq" format
            const dash = std.mem.indexOf(u8, s, "-") orelse return error.InvalidStreamId;
            const ms = try std.fmt.parseInt(i64, s[0..dash], 10);
            const seq = try std.fmt.parseInt(u64, s[dash + 1 ..], 10);
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
    /// Uses 16384 6-bit registers (14-bit precision, standard Redis configuration)
    pub const HyperLogLogValue = struct {
        registers: [16384]u8, // 16384 registers, each 6-bit max value
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

        /// Estimate cardinality using HyperLogLog algorithm
        pub fn count(self: *const HyperLogLogValue) u64 {
            // Standard HyperLogLog cardinality estimation
            const m: f64 = 16384.0; // number of registers
            const alpha: f64 = 0.7213 / (1.0 + 1.079 / m); // bias correction

            var raw_sum: f64 = 0.0;
            var zero_count: u32 = 0;

            for (self.registers) |register_val| {
                raw_sum += std.math.pow(f64, 2.0, -@as(f64, @floatFromInt(register_val)));
                if (register_val == 0) zero_count += 1;
            }

            const raw_estimate = alpha * m * m / raw_sum;

            // Small range correction
            if (raw_estimate <= 5.0 * m) {
                if (zero_count > 0) {
                    const v: f64 = @floatFromInt(zero_count);
                    return @intFromFloat(m * @log(m / v));
                }
            }

            // Large range correction
            if (raw_estimate > (1.0 / 30.0) * 4294967296.0) {
                return @intFromFloat(-4294967296.0 * @log(1.0 - raw_estimate / 4294967296.0));
            }

            return @intFromFloat(raw_estimate);
        }

        /// Merge another HyperLogLog into this one
        /// Takes the maximum value for each register
        pub fn merge(self: *HyperLogLogValue, other: *const HyperLogLogValue) void {
            for (&self.registers, other.registers) |*reg, other_reg| {
                reg.* = @max(reg.*, other_reg);
            }
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
        };
    }

    /// Check if value is expired
    pub fn isExpired(self: Value, now: i64) bool {
        const exp = self.getExpiration() orelse return false;
        return now >= exp;
    }
};

/// Thread-safe in-memory storage engine with TTL support
pub const Storage = struct {
    allocator: std.mem.Allocator,
    data: std.StringHashMap(Value),
    config: *Config,
    mutex: std.Thread.Mutex,

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

        storage.* = Storage{
            .allocator = allocator,
            .data = std.StringHashMap(Value).init(allocator),
            .config = cfg,
            .mutex = std.Thread.Mutex{},
        };

        return storage;
    }

    /// Deinitialize storage and free all keys and values
    pub fn deinit(self: *Storage) void {
        self.mutex.lock();

        // Free all keys and values
        var it = self.data.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var value = entry.value_ptr.*;
            value.deinit(self.allocator);
        }
        self.data.deinit();

        self.config.deinit();

        const allocator = self.allocator;
        self.mutex.unlock();
        allocator.destroy(self);
    }

    /// Get type of value stored at key, or null if key doesn't exist
    pub fn getType(self: *Storage, key: []const u8) ?ValueType {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.data.remove(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        return std.meta.activeTag(entry.value_ptr.*);
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
    }

    /// Get string value for key
    /// Returns null if key doesn't exist, is expired, or is not a string
    /// Expired keys are lazily deleted
    pub fn get(self: *Storage, key: []const u8) ?[]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            // Expired - delete and return null
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.data.remove(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        // Check type
        return switch (entry.value_ptr.*) {
            .string => |s| s.data,
            else => null,
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
                self.allocator.free(kv.key);
                var value = kv.value;
                value.deinit(self.allocator);
                count += 1;
            }
        }
        return count;
    }

    /// Check if key exists (respects expiration)
    /// Expired keys count as non-existent and are lazily deleted
    pub fn exists(self: *Storage, key: []const u8) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return false;

        // Check expiration
        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            // Expired - delete and return false
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.data.remove(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return false;
        }

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

        // Remove expired keys
        for (expired_keys.items) |key| {
            if (self.data.fetchRemove(key)) |kv| {
                self.allocator.free(kv.key);
                var value = kv.value;
                value.deinit(self.allocator);
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
                    _ = self.data.remove(key);
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
                    _ = self.data.remove(key);
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
                    _ = self.data.remove(key);
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
                    _ = self.data.remove(key);
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
                    _ = self.data.remove(key);
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

        if (self.data.getEntry(key)) |entry| {
            // Key exists - verify it's a set
            switch (entry.value_ptr.*) {
                .set => |*set_val| {
                    // Add members to existing set
                    for (members) |member| {
                        // Check if member already exists
                        if (!set_val.data.contains(member)) {
                            // Duplicate the member string
                            const owned_member = try self.allocator.dupe(u8, member);
                            errdefer self.allocator.free(owned_member);

                            try set_val.data.put(owned_member, {});
                            added_count += 1;
                        }
                    }
                    return added_count;
                },
                else => return error.WrongType,
            }
        } else {
            // Create new set
            var set_map = std.StringHashMap(void).init(self.allocator);
            errdefer {
                var it = set_map.keyIterator();
                while (it.next()) |k| {
                    self.allocator.free(k.*);
                }
                set_map.deinit();
            }

            // Add all members (skip duplicates within same command)
            for (members) |member| {
                if (!set_map.contains(member)) {
                    const owned_member = try self.allocator.dupe(u8, member);
                    errdefer self.allocator.free(owned_member);

                    try set_map.put(owned_member, {});
                    added_count += 1;
                }
            }

            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);

            try self.data.put(owned_key, Value{
                .set = .{
                    .data = set_map,
                    .expires_at = expires_at,
                },
            });

            return added_count;
        }
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

                for (members) |member| {
                    if (set_val.data.fetchRemove(member)) |kv| {
                        self.allocator.free(kv.key);
                        removed_count += 1;
                    }
                }

                // Auto-delete if set becomes empty
                if (set_val.data.count() == 0) {
                    const owned_key = entry.key_ptr.*;
                    var value = entry.value_ptr.*;
                    _ = self.data.remove(key);
                    self.allocator.free(owned_key);
                    value.deinit(self.allocator);
                }

                return removed_count;
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
            _ = self.data.remove(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return false;
        }

        switch (entry.value_ptr.*) {
            .set => |*set_val| {
                return set_val.data.contains(member);
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
            _ = self.data.remove(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .set => |*set_val| {
                const member_count = set_val.data.count();
                var result = try allocator.alloc([]const u8, member_count);
                errdefer allocator.free(result);

                var it = set_val.data.keyIterator();
                var i: usize = 0;
                while (it.next()) |member_ptr| : (i += 1) {
                    result[i] = member_ptr.*;
                }

                return result;
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
            _ = self.data.remove(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .set => |set_val| return set_val.data.count(),
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

                            try hash_val.data.put(owned_field, owned_value);
                            added_count += 1;
                        } else {
                            // Existing field - free old value, set new one
                            const old_value = hash_val.data.get(field).?;
                            self.allocator.free(old_value);

                            const owned_value = try self.allocator.dupe(u8, value);
                            errdefer self.allocator.free(owned_value);

                            try hash_val.data.put(field, owned_value);
                        }
                    }
                    return added_count;
                },
                else => return error.WrongType,
            }
        } else {
            // Create new hash
            var hash_map = std.StringHashMap([]const u8).init(self.allocator);
            errdefer {
                var it = hash_map.iterator();
                while (it.next()) |entry| {
                    self.allocator.free(entry.key_ptr.*);
                    self.allocator.free(entry.value_ptr.*);
                }
                hash_map.deinit();
            }

            // Add all field-value pairs
            for (fields, values) |field, value| {
                const owned_field = try self.allocator.dupe(u8, field);
                errdefer self.allocator.free(owned_field);

                const owned_value = try self.allocator.dupe(u8, value);
                errdefer self.allocator.free(owned_value);

                try hash_map.put(owned_field, owned_value);
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
            _ = self.data.remove(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .hash => |*hash_val| {
                return hash_val.data.get(field);
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
                        self.allocator.free(kv.value);
                        deleted_count += 1;
                    }
                }

                // Auto-delete if hash becomes empty
                if (hash_val.data.count() == 0) {
                    const owned_key = entry.key_ptr.*;
                    var value = entry.value_ptr.*;
                    _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
                    result[i] = pair.key_ptr.*;
                    result[i + 1] = pair.value_ptr.*;
                    i += 2;
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
            _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
                while (it.next()) |value_ptr| : (i += 1) {
                    result[i] = value_ptr.*;
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
            _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
                    _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
                    const current_str = hash_val.data.get(field) orelse "0";
                    const current = std.fmt.parseInt(i64, current_str, 10) catch return error.InvalidValue;
                    const new_value = current + increment;

                    // Format new value as string
                    var buf: [32]u8 = undefined;
                    const new_str = std.fmt.bufPrint(&buf, "{d}", .{new_value}) catch return error.InvalidValue;
                    const owned_new = try allocator.dupe(u8, new_str);
                    errdefer allocator.free(owned_new);

                    if (hash_val.data.contains(field)) {
                        // Free old value string
                        const old_val = hash_val.data.get(field).?;
                        self.allocator.free(old_val);
                        try hash_val.data.put(field, owned_new);
                    } else {
                        // New field: duplicate key
                        const owned_field = try allocator.dupe(u8, field);
                        errdefer allocator.free(owned_field);
                        try hash_val.data.put(owned_field, owned_new);
                    }
                    return new_value;
                },
                else => return error.WrongType,
            }
        } else {
            // Create new hash with single field
            var hash_map = std.StringHashMap([]const u8).init(self.allocator);
            errdefer {
                var it = hash_map.iterator();
                while (it.next()) |e| {
                    self.allocator.free(e.key_ptr.*);
                    self.allocator.free(e.value_ptr.*);
                }
                hash_map.deinit();
            }

            var buf: [32]u8 = undefined;
            const new_str = std.fmt.bufPrint(&buf, "{d}", .{increment}) catch return error.InvalidValue;
            const owned_field = try self.allocator.dupe(u8, field);
            errdefer self.allocator.free(owned_field);
            const owned_value = try self.allocator.dupe(u8, new_str);
            errdefer self.allocator.free(owned_value);
            try hash_map.put(owned_field, owned_value);

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
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.data.getEntry(key)) |entry| {
            switch (entry.value_ptr.*) {
                .hash => |*hash_val| {
                    const current_str = hash_val.data.get(field) orelse "0";
                    const current = std.fmt.parseFloat(f64, current_str) catch return error.InvalidValue;
                    const new_value = current + increment;

                    // Format with enough precision, trim trailing zeros
                    var buf: [64]u8 = undefined;
                    const new_str = formatFloat(&buf, new_value);
                    const owned_new = try allocator.dupe(u8, new_str);
                    errdefer allocator.free(owned_new);

                    if (hash_val.data.contains(field)) {
                        const old_val = hash_val.data.get(field).?;
                        self.allocator.free(old_val);
                        try hash_val.data.put(field, owned_new);
                    } else {
                        const owned_field = try allocator.dupe(u8, field);
                        errdefer allocator.free(owned_field);
                        try hash_val.data.put(owned_field, owned_new);
                    }
                    return new_value;
                },
                else => return error.WrongType,
            }
        } else {
            var hash_map = std.StringHashMap([]const u8).init(self.allocator);
            errdefer {
                var it = hash_map.iterator();
                while (it.next()) |e| {
                    self.allocator.free(e.key_ptr.*);
                    self.allocator.free(e.value_ptr.*);
                }
                hash_map.deinit();
            }

            var buf: [64]u8 = undefined;
            const new_str = formatFloat(&buf, increment);
            const owned_field = try self.allocator.dupe(u8, field);
            errdefer self.allocator.free(owned_field);
            const owned_value = try self.allocator.dupe(u8, new_str);
            errdefer self.allocator.free(owned_value);
            try hash_map.put(owned_field, owned_value);

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
                    try hash_val.data.put(owned_field, owned_value);
                    return true;
                },
                else => return error.WrongType,
            }
        } else {
            var hash_map = std.StringHashMap([]const u8).init(self.allocator);
            errdefer {
                var it = hash_map.iterator();
                while (it.next()) |e| {
                    self.allocator.free(e.key_ptr.*);
                    self.allocator.free(e.value_ptr.*);
                }
                hash_map.deinit();
            }
            const owned_field = try self.allocator.dupe(u8, field);
            errdefer self.allocator.free(owned_field);
            const owned_value = try self.allocator.dupe(u8, value);
            errdefer self.allocator.free(owned_value);
            try hash_map.put(owned_field, owned_value);

            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);
            try self.data.put(owned_key, Value{
                .hash = .{ .data = hash_map, .expires_at = null },
            });
            return true;
        }
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
            _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
                    var it = set_val.data.keyIterator();
                    while (it.next()) |member_ptr| {
                        try accum.put(member_ptr.*, {});
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
    ) ![][]const u8 {
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

        var it = first_set.data.keyIterator();
        outer: while (it.next()) |member_ptr| {
            const member = member_ptr.*;
            // Check in all keys (skip index 0 = already seeded)
            for (keys[1..]) |key| {
                const entry = self.data.getEntry(key) orelse continue :outer;
                if (entry.value_ptr.isExpired(getCurrentTimestamp())) continue :outer;
                switch (entry.value_ptr.*) {
                    .set => |*sv| {
                        if (!sv.data.contains(member)) continue :outer;
                    },
                    else => return error.WrongType,
                }
            }
            try result_list.append(allocator, member);
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
    ) ![][]const u8 {
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

        var it = first_set.data.keyIterator();
        outer: while (it.next()) |member_ptr| {
            const member = member_ptr.*;
            for (keys[1..]) |key| {
                const entry = self.data.getEntry(key) orelse continue;
                if (entry.value_ptr.isExpired(getCurrentTimestamp())) continue;
                switch (entry.value_ptr.*) {
                    .set => |*sv| {
                        if (sv.data.contains(member)) continue :outer;
                    },
                    else => {},
                }
            }
            try result_list.append(allocator, member);
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
            .set = .{ .data = set_map, .expires_at = null },
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
            _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
                _ = self.data.remove(key);
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
        self.mutex.lock();
        defer self.mutex.unlock();

        var current: f64 = 0.0;
        var current_exp: ?i64 = null;

        if (self.data.getEntry(key)) |entry| {
            if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
                const owned_key = entry.key_ptr.*;
                var value = entry.value_ptr.*;
                _ = self.data.remove(key);
                self.allocator.free(owned_key);
                value.deinit(self.allocator);
            } else {
                switch (entry.value_ptr.*) {
                    .string => |*sv| {
                        current = std.fmt.parseFloat(f64, sv.data) catch return error.NotFloat;
                        current_exp = sv.expires_at;
                    },
                    else => return error.WrongType,
                }
            }
        }

        const new_val = current + delta;

        // Format new value (trim trailing zeros like Redis)
        var buf: [64]u8 = undefined;
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
                _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
                _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
        _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
        _ = self.data.remove(key);
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
            .stream => 0xFE, // Stream type
            .hyperloglog => 0xFD, // HyperLogLog type
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
                try w.writeInt(u32, @intCast(s.data.count()), .little);
                var kit = s.data.keyIterator();
                while (kit.next()) |k| try writeBlob(w, k.*);
            },
            .hash => |h| {
                try w.writeInt(u32, @intCast(h.data.count()), .little);
                var hit = h.data.iterator();
                while (hit.next()) |e| {
                    try writeBlob(w, e.key_ptr.*);
                    try writeBlob(w, e.value_ptr.*);
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
    pub fn restoreValue(self: *Storage, key: []const u8, serialized: []const u8, ttl_ms: i64, replace: bool) !void {
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
            expires_at = getCurrentTimestamp() + ttl_ms;
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

                value = Value{ .set = .{ .data = set_data, .expires_at = expires_at } };
            },
            0x04 => { // Hash
                if (payload.len < pos + 4) return error.InvalidDumpPayload;
                const count = std.mem.readInt(u32, payload[pos..][0..4], .little);
                pos += 4;

                var hash = std.StringHashMap([]const u8).init(self.allocator);
                errdefer {
                    var it = hash.iterator();
                    while (it.next()) |e| {
                        self.allocator.free(e.key_ptr.*);
                        self.allocator.free(e.value_ptr.*);
                    }
                    hash.deinit();
                }

                var i: u32 = 0;
                while (i < count) : (i += 1) {
                    const field = try readBlob(payload, &pos, self.allocator);
                    const val = try readBlob(payload, &pos, self.allocator);
                    try hash.put(field, val);
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

    /// Deep copy a Value (helper for COPY command)
    fn deepCopyValue(self: *Storage, value: Value) !Value {
        return switch (value) {
            .string => |s| blk: {
                const data_copy = try self.allocator.dupe(u8, s.data);
                break :blk Value{ .string = .{ .data = data_copy, .expires_at = s.expires_at } };
            },
            .list => |l| blk: {
                var list_copy = std.ArrayList([]const u8){};
                errdefer {
                    for (list_copy.items) |elem| self.allocator.free(elem);
                    list_copy.deinit(self.allocator);
                }
                for (l.data.items) |elem| {
                    const elem_copy = try self.allocator.dupe(u8, elem);
                    try list_copy.append(self.allocator, elem_copy);
                }
                break :blk Value{ .list = .{ .data = list_copy, .expires_at = l.expires_at } };
            },
            .set => |s| blk: {
                var set_copy = std.StringHashMap(void).init(self.allocator);
                errdefer {
                    var it = set_copy.keyIterator();
                    while (it.next()) |k| self.allocator.free(k.*);
                    set_copy.deinit();
                }
                var it = s.data.keyIterator();
                while (it.next()) |k| {
                    const key_copy = try self.allocator.dupe(u8, k.*);
                    try set_copy.put(key_copy, {});
                }
                break :blk Value{ .set = .{ .data = set_copy, .expires_at = s.expires_at } };
            },
            .hash => |h| blk: {
                var hash_copy = std.StringHashMap([]const u8).init(self.allocator);
                errdefer {
                    var it = hash_copy.iterator();
                    while (it.next()) |e| {
                        self.allocator.free(e.key_ptr.*);
                        self.allocator.free(e.value_ptr.*);
                    }
                    hash_copy.deinit();
                }
                var it = h.data.iterator();
                while (it.next()) |e| {
                    const field_copy = try self.allocator.dupe(u8, e.key_ptr.*);
                    const val_copy = try self.allocator.dupe(u8, e.value_ptr.*);
                    try hash_copy.put(field_copy, val_copy);
                }
                break :blk Value{ .hash = .{ .data = hash_copy, .expires_at = h.expires_at } };
            },
            .sorted_set => |z| blk: {
                var members_copy = std.StringHashMap(f64).init(self.allocator);
                var sorted_list_copy = std.ArrayList(Value.ScoredMember){};

                errdefer {
                    var it = members_copy.keyIterator();
                    while (it.next()) |k| self.allocator.free(k.*);
                    members_copy.deinit();
                    sorted_list_copy.deinit(self.allocator);
                }

                for (z.sorted_list.items) |scored| {
                    const member_copy = try self.allocator.dupe(u8, scored.member);
                    try members_copy.put(member_copy, scored.score);
                    try sorted_list_copy.append(self.allocator, .{ .score = scored.score, .member = member_copy });
                }

                break :blk Value{ .sorted_set = .{ .members = members_copy, .sorted_list = sorted_list_copy, .expires_at = z.expires_at } };
            },
            .stream => |st| blk: {
                var entries_copy = std.ArrayList(Value.StreamEntry){};
                errdefer {
                    for (entries_copy.items) |*e| e.deinit(self.allocator);
                    entries_copy.deinit(self.allocator);
                }

                for (st.entries.items) |e| {
                    var fields_copy = std.ArrayList([]const u8){};
                    errdefer {
                        for (fields_copy.items) |item| self.allocator.free(item);
                        fields_copy.deinit(self.allocator);
                    }

                    for (e.fields.items) |item| {
                        const item_copy = try self.allocator.dupe(u8, item);
                        try fields_copy.append(self.allocator, item_copy);
                    }

                    try entries_copy.append(self.allocator, .{
                        .id = e.id,
                        .fields = fields_copy,
                    });
                }

                break :blk Value{ .stream = .{
                    .entries = entries_copy,
                    .last_id = st.last_id,
                    .expires_at = st.expires_at,
                    .consumer_groups = std.StringHashMap(Value.ConsumerGroup).init(self.allocator),
                } };
            },
            .hyperloglog => |hll| blk: {
                // HyperLogLog is a fixed-size array, simple copy
                var hll_copy = Value.HyperLogLogValue.init();
                hll_copy.registers = hll.registers;
                hll_copy.expires_at = hll.expires_at;
                break :blk Value{ .hyperloglog = hll_copy };
            },
        };
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
    ) error{ WrongType, OutOfMemory }!?[][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.data.remove(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .set => |*set_val| {
                const pop_count = if (count == 0) @as(usize, 1) else @min(count, set_val.data.count());
                if (pop_count == 0) return try allocator.alloc([]const u8, 0);

                var result = try allocator.alloc([]const u8, pop_count);
                errdefer {
                    for (result) |s| allocator.free(s);
                    allocator.free(result);
                }

                var i: usize = 0;
                while (i < pop_count) : (i += 1) {
                    // Pick a pseudo-random member by iterating to a random offset
                    const set_len = set_val.data.count();
                    if (set_len == 0) break;
                    const rnd_idx = @as(usize, @intCast(@mod(std.time.nanoTimestamp() +% @as(i128, @intCast(i)), @as(i128, @intCast(set_len)))));
                    var it = set_val.data.keyIterator();
                    var idx: usize = 0;
                    while (it.next()) |member_ptr| {
                        if (idx == rnd_idx) {
                            const member = member_ptr.*;
                            result[i] = try allocator.dupe(u8, member);
                            // Remove from set
                            if (set_val.data.fetchRemove(member)) |kv| {
                                self.allocator.free(kv.key);
                            }
                            break;
                        }
                        idx += 1;
                    }
                }

                // Auto-delete empty set
                if (set_val.data.count() == 0) {
                    const owned_key = entry.key_ptr.*;
                    var value = entry.value_ptr.*;
                    _ = self.data.remove(key);
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
    ) error{ WrongType, OutOfMemory }!?[][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return null;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.data.remove(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return null;
        }

        switch (entry.value_ptr.*) {
            .set => |*set_val| {
                const set_len = set_val.data.count();

                // Collect all members into a temporary slice for indexing
                var all_members = try allocator.alloc([]const u8, set_len);
                defer allocator.free(all_members);
                var it = set_val.data.keyIterator();
                var idx: usize = 0;
                while (it.next()) |member_ptr| : (idx += 1) {
                    all_members[idx] = member_ptr.*;
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
    ) error{ WrongType, OutOfMemory }!bool {
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
        if (!src_set.data.contains(member)) return false;

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
            if (src_set.data.fetchRemove(member)) |kv| {
                break :blk kv.key;
            }
            return false;
        };

        // Auto-delete source if empty
        if (src_set.data.count() == 0) {
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
                        if (dst_set.data.contains(owned_member)) {
                            self.allocator.free(owned_member);
                        } else {
                            try dst_set.data.put(owned_member, {});
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
            .set = .{ .data = new_set, .expires_at = null },
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
    ) error{ WrongType, OutOfMemory }![]bool {
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
            _ = self.data.remove(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            @memset(result, false);
            return result;
        }

        switch (entry.value_ptr.*) {
            .set => |*set_val| {
                for (members, 0..) |member, i| {
                    result[i] = set_val.data.contains(member);
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
    ) error{ WrongType, OutOfMemory }!usize {
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
            _ = self.data.remove(key);
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
                    _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
                    _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
                _ = self.data.remove(key);
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
                _ = self.data.remove(key);
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
    pub fn bitcount(
        self: *Storage,
        key: []const u8,
        start: ?i64,
        end: ?i64,
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

                    const len: i64 = @intCast(sv.data.len);
                    const start_idx = if (start) |s| blk: {
                        const idx = if (s < 0) len + s else s;
                        break :blk @max(0, @min(idx, len - 1));
                    } else 0;
                    const end_idx = if (end) |e| blk: {
                        const idx = if (e < 0) len + e else e;
                        break :blk @max(0, @min(idx, len - 1));
                    } else len - 1;

                    if (start_idx > end_idx) return 0;

                    var count: i64 = 0;
                    const start_u: usize = @intCast(start_idx);
                    const end_u: usize = @intCast(end_idx);
                    for (sv.data[start_u .. end_u + 1]) |byte| {
                        count += @popCount(byte);
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

    // ── Stream operations ─────────────────────────────────────────────────────

    /// Add entry to stream with auto-generated or explicit ID.
    /// Returns the assigned StreamId or error if ID is invalid.
    pub fn xadd(
        self: *Storage,
        key: []const u8,
        id_str: []const u8,
        fields: []const []const u8,
        expires_at: ?i64,
    ) error{ WrongType, OutOfMemory, InvalidStreamId, StreamIdTooSmall, Overflow, InvalidCharacter }!Value.StreamId {
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = getCurrentTimestamp();

        // Get or create stream
        const entry = try self.data.getOrPut(key);
        if (!entry.found_existing) {
            const owned_key = try self.allocator.dupe(u8, key);
            entry.key_ptr.* = owned_key;
            entry.value_ptr.* = Value{
                .stream = .{
                    .entries = std.ArrayList(Value.StreamEntry){},
                    .last_id = null,
                    .expires_at = expires_at,
                    .consumer_groups = std.StringHashMap(Value.ConsumerGroup).init(self.allocator),
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
            _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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
            _ = self.data.remove(key);
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

    /// Trim stream to approximately maxlen entries (using MAXLEN strategy).
    /// Returns number of entries deleted.
    pub fn xtrim(
        self: *Storage,
        key: []const u8,
        maxlen: usize,
    ) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.data.getEntry(key) orelse return 0;

        if (entry.value_ptr.isExpired(getCurrentTimestamp())) {
            const owned_key = entry.key_ptr.*;
            var value = entry.value_ptr.*;
            _ = self.data.remove(key);
            self.allocator.free(owned_key);
            value.deinit(self.allocator);
            return 0;
        }

        switch (entry.value_ptr.*) {
            .stream => |*stream_val| {
                const current_len = stream_val.entries.items.len;
                if (current_len <= maxlen) return 0;

                const to_delete = current_len - maxlen;

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

                // Create consumer group
                const owned_name = try self.allocator.dupe(u8, group_name);
                errdefer self.allocator.free(owned_name);

                try stream_val.consumer_groups.put(owned_name, Value.ConsumerGroup{
                    .name = owned_name,
                    .last_delivered_id = starting_id,
                    .consumers = std.StringHashMap(Value.Consumer).init(self.allocator),
                    .pending = std.ArrayList(Value.PendingEntry){},
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

                // Get or create consumer
                const consumer_entry = try group_ptr.consumers.getOrPut(consumer_name);
                if (!consumer_entry.found_existing) {
                    const owned_consumer_name = try self.allocator.dupe(u8, consumer_name);
                    consumer_entry.key_ptr.* = owned_consumer_name;
                    consumer_entry.value_ptr.* = Value.Consumer{
                        .name = owned_consumer_name,
                        .pending = std.ArrayList(Value.StreamId){},
                    };
                }

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

                    try writer.print("*14\r\n", .{});
                    try writer.print("$6\r\nlength\r\n:{d}\r\n", .{stream_val.entries.items.len});
                    try writer.print("$15\r\nradix-tree-keys\r\n:1\r\n", .{});
                    try writer.print("$17\r\nradix-tree-nodes\r\n:2\r\n", .{});
                    try writer.print("$6\r\ngroups\r\n:{d}\r\n", .{stream_val.consumer_groups.count()});
                    try writer.print("$13\r\nlast-entry-id\r\n${d}\r\n{d}-{d}\r\n", .{
                        std.fmt.count("{d}-{d}", .{ last_id.ms, last_id.seq }),
                        last_id.ms,
                        last_id.seq,
                    });
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

                    try writer.print("*12\r\n", .{});
                    try writer.print("$6\r\nlength\r\n:{d}\r\n", .{stream_val.entries.items.len});
                    try writer.print("$15\r\nradix-tree-keys\r\n:1\r\n", .{});
                    try writer.print("$17\r\nradix-tree-nodes\r\n:2\r\n", .{});

                    const last_id = stream_val.last_id orelse Value.StreamId{ .ms = 0, .seq = 0 };
                    try writer.print("$13\r\nlast-entry-id\r\n${d}\r\n{d}-{d}\r\n", .{
                        std.fmt.count("{d}-{d}", .{ last_id.ms, last_id.seq }),
                        last_id.ms,
                        last_id.seq,
                    });

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
};

// Embedded unit tests

test "storage - init and deinit" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
}

test "storage - set and get string" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);
    const result = storage.get("key1");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("value1", result.?);
}

test "storage - get non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const result = storage.get("nosuchkey");
    try std.testing.expect(result == null);
}

test "storage - set overwrites existing key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);
    try storage.set("key1", "value2", null);
    const result = storage.get("key1");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("value2", result.?);
}

test "storage - del single key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const keys = [_][]const u8{"nosuchkey"};
    const count = storage.del(&keys);
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "storage - exists returns true for existing key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);
    try std.testing.expect(storage.exists("key1"));
}

test "storage - exists returns false for non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try std.testing.expect(!storage.exists("nosuchkey"));
}

test "storage - set with expiration in future" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const now = Storage.getCurrentTimestamp();
    const expires_at = now - 1000; // Already expired

    try storage.set("key1", "value1", expires_at);
    try std.testing.expect(!storage.exists("key1"));
}

test "storage - set updates expiration" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const elements = [_][]const u8{"world"};
    const len = try storage.lpush("mylist", &elements, null);
    try std.testing.expectEqual(@as(usize, 1), len);
}

test "storage - lpush multiple elements order" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "string", null);
    const elements = [_][]const u8{"value"};
    try std.testing.expectError(error.WrongType, storage.lpush("mykey", &elements, null));
}

test "storage - rpush creates new list" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const elements = [_][]const u8{"hello"};
    const len = try storage.rpush("mylist", &elements, null);
    try std.testing.expectEqual(@as(usize, 1), len);
}

test "storage - rpush multiple elements order" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "string", null);
    const elements = [_][]const u8{"value"};
    try std.testing.expectError(error.WrongType, storage.rpush("mykey", &elements, null));
}

test "storage - lpop single element" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const result = try storage.lpop(allocator, "nosuchkey", 1);
    try std.testing.expect(result == null);
}

test "storage - lpop count greater than length" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    const result = (try storage.lrange(allocator, "mylist", 5, 10)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 0), result.len);
}

test "storage - lrange on non-existent key returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const result = try storage.lrange(allocator, "nosuchkey", 0, -1);
    try std.testing.expect(result == null);
}

test "storage - llen returns length" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    const len = storage.llen("mylist");
    try std.testing.expectEqual(@as(usize, 3), len.?);
}

test "storage - llen on non-existent key returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const len = storage.llen("nosuchkey");
    try std.testing.expect(len == null);
}

test "storage - llen on string key returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "value", null);
    const len = storage.llen("mykey");
    try std.testing.expect(len == null);
}

test "storage - lindex returns element at positive index" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    const result = try storage.lindex("mylist", 1);
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("b", result.?);
}

test "storage - lindex returns element at negative index" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    const result = try storage.lindex("mylist", -1);
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("c", result.?);
}

test "storage - lindex out of range returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b" };
    _ = try storage.rpush("mylist", &elements, null);

    try std.testing.expect((try storage.lindex("mylist", 5)) == null);
    try std.testing.expect((try storage.lindex("mylist", -5)) == null);
}

test "storage - lindex on non-existent key returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try std.testing.expect((try storage.lindex("nosuchkey", 0)) == null);
}

test "storage - lindex wrong type returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "value", null);
    try std.testing.expectError(error.WrongType, storage.lindex("mykey", 0));
}

test "storage - lset updates element" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    try storage.lset("mylist", 1, "z");
    const result = try storage.lindex("mylist", 1);
    try std.testing.expectEqualStrings("z", result.?);
}

test "storage - lset negative index" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    try storage.lset("mylist", -1, "z");
    const result = try storage.lindex("mylist", 2);
    try std.testing.expectEqualStrings("z", result.?);
}

test "storage - lset out of range returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const elements = [_][]const u8{"a"};
    _ = try storage.rpush("mylist", &elements, null);

    try std.testing.expectError(error.IndexOutOfRange, storage.lset("mylist", 5, "z"));
}

test "storage - ltrim basic range" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    try storage.ltrim("mylist", 5, 1);
    try std.testing.expect(!storage.exists("mylist"));
}

test "storage - lrem removes from head" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const elems = [_][]const u8{"a"};
    const len = try storage.lpushx("nosuchkey", &elems);
    try std.testing.expect(len == null);
    try std.testing.expect(!storage.exists("nosuchkey"));
}

test "storage - rpushx on existing list" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const elems = [_][]const u8{"a"};
    const len = try storage.rpushx("nosuchkey", &elems);
    try std.testing.expect(len == null);
}

test "storage - linsert before pivot" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b" };
    _ = try storage.rpush("mylist", &elements, null);

    const result = try storage.linsert("mylist", true, "z", "x");
    try std.testing.expectEqual(@as(i64, -1), result);
}

test "storage - linsert on missing key returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const result = try storage.linsert("nosuchkey", true, "pivot", "elem");
    try std.testing.expectEqual(@as(i64, 0), result);
}

test "storage - lpos finds first occurrence" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const result = try storage.lmove(allocator, "nosuchkey", "dst", true, true);
    try std.testing.expect(result == null);
}

test "storage - sadd creates new set" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const members = [_][]const u8{"hello"};
    const added = try storage.sadd("myset", &members, null);
    try std.testing.expectEqual(@as(usize, 1), added);
}

test "storage - sadd adds multiple members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const members = [_][]const u8{ "one", "two", "three" };
    const added = try storage.sadd("myset", &members, null);
    try std.testing.expectEqual(@as(usize, 3), added);
}

test "storage - sadd ignores duplicates" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const members1 = [_][]const u8{ "one", "two" };
    _ = try storage.sadd("myset", &members1, null);

    const members2 = [_][]const u8{"two"};
    const added = try storage.sadd("myset", &members2, null);
    try std.testing.expectEqual(@as(usize, 0), added);
}

test "storage - sadd on existing string returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "string", null);
    const members = [_][]const u8{"value"};
    try std.testing.expectError(error.WrongType, storage.sadd("mykey", &members, null));
}

test "storage - sadd with mixed new and existing members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const members1 = [_][]const u8{ "one", "two" };
    _ = try storage.sadd("myset", &members1, null);

    const members2 = [_][]const u8{ "two", "three", "four" };
    const added = try storage.sadd("myset", &members2, null);
    try std.testing.expectEqual(@as(usize, 2), added);
}

test "storage - srem removes members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const members = [_][]const u8{ "one", "two", "three" };
    _ = try storage.sadd("myset", &members, null);

    const to_remove = [_][]const u8{"one"};
    const removed = try storage.srem(allocator, "myset", &to_remove);
    try std.testing.expectEqual(@as(usize, 1), removed);
}

test "storage - srem returns count of removed" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const members = [_][]const u8{ "one", "two", "three" };
    _ = try storage.sadd("myset", &members, null);

    const to_remove = [_][]const u8{ "one", "three" };
    const removed = try storage.srem(allocator, "myset", &to_remove);
    try std.testing.expectEqual(@as(usize, 2), removed);
}

test "storage - srem auto-deletes empty set" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const members = [_][]const u8{"one"};
    _ = try storage.sadd("myset", &members, null);

    const to_remove = [_][]const u8{"one"};
    _ = try storage.srem(allocator, "myset", &to_remove);

    try std.testing.expect(!storage.exists("myset"));
}

test "storage - srem on non-existent key returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const to_remove = [_][]const u8{"one"};
    const removed = try storage.srem(allocator, "nosuchkey", &to_remove);
    try std.testing.expectEqual(@as(usize, 0), removed);
}

test "storage - srem on non-existent member returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const members = [_][]const u8{"one"};
    _ = try storage.sadd("myset", &members, null);

    const to_remove = [_][]const u8{"two"};
    const removed = try storage.srem(allocator, "myset", &to_remove);
    try std.testing.expectEqual(@as(usize, 0), removed);
}

test "storage - sismember returns true for member" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const members = [_][]const u8{ "one", "two" };
    _ = try storage.sadd("myset", &members, null);

    const is_member = try storage.sismember("myset", "one");
    try std.testing.expect(is_member);
}

test "storage - sismember returns false for non-member" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const members = [_][]const u8{ "one", "two" };
    _ = try storage.sadd("myset", &members, null);

    const is_member = try storage.sismember("myset", "three");
    try std.testing.expect(!is_member);
}

test "storage - sismember returns false for non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const is_member = try storage.sismember("nosuchkey", "one");
    try std.testing.expect(!is_member);
}

test "storage - sismember on string key returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "string", null);
    try std.testing.expectError(error.WrongType, storage.sismember("mykey", "one"));
}

test "storage - smembers returns all members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const members = [_][]const u8{ "one", "two", "three" };
    _ = try storage.sadd("myset", &members, null);

    const result = (try storage.smembers(allocator, "myset")).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 3), result.len);
}

test "storage - smembers returns null for non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const result = try storage.smembers(allocator, "nosuchkey");
    try std.testing.expect(result == null);
}

test "storage - smembers returns null for string key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "string", null);
    const result = try storage.smembers(allocator, "mykey");
    try std.testing.expect(result == null);
}

test "storage - scard returns cardinality" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const members = [_][]const u8{ "one", "two", "three" };
    _ = try storage.sadd("myset", &members, null);

    const cardinality = storage.scard("myset");
    try std.testing.expectEqual(@as(usize, 3), cardinality.?);
}

test "storage - scard returns null for non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const cardinality = storage.scard("nosuchkey");
    try std.testing.expect(cardinality == null);
}

test "storage - scard returns null for string key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "string", null);
    const cardinality = storage.scard("mykey");
    try std.testing.expect(cardinality == null);
}

test "storage - set respects expiration" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const members = [_][]const u8{"one"};
    _ = try storage.sadd("myset", &members, null);

    try std.testing.expectEqual(ValueType.set, storage.getType("myset").?);
}

// Sorted set unit tests

test "storage - zadd creates new sorted set" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const scores = [_]f64{1.0};
    const members = [_][]const u8{"alpha"};
    const result = try storage.zadd("myzset", &scores, &members, 0, null);
    try std.testing.expectEqual(@as(usize, 1), result.added);
    try std.testing.expectEqual(@as(usize, 1), result.changed);
}

test "storage - zadd multiple members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const scores = [_]f64{ 1.0, 2.0, 3.0 };
    const members = [_][]const u8{ "one", "two", "three" };
    const result = try storage.zadd("myzset", &scores, &members, 0, null);
    try std.testing.expectEqual(@as(usize, 3), result.added);
}

test "storage - zadd updates existing member score" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "string", null);
    const scores = [_]f64{1.0};
    const members = [_][]const u8{"one"};
    try std.testing.expectError(error.WrongType, storage.zadd("mykey", &scores, &members, 0, null));
}

test "storage - zrem removes members" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const to_remove = [_][]const u8{"one"};
    const removed = try storage.zrem("nosuchkey", &to_remove);
    try std.testing.expectEqual(@as(usize, 0), removed);
}

test "storage - zscore returns score for existing member" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const scores = [_]f64{1.0};
    const members = [_][]const u8{"one"};
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    const score = storage.zscore("myzset", "nosuchmember");
    try std.testing.expect(score == null);
}

test "storage - zscore returns null for non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const score = storage.zscore("nosuchkey", "member");
    try std.testing.expect(score == null);
}

test "storage - zcard returns count" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const scores = [_]f64{ 1.0, 2.0, 3.0 };
    const members = [_][]const u8{ "one", "two", "three" };
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    try std.testing.expectEqual(@as(usize, 3), storage.zcard("myzset").?);
}

test "storage - zcard returns null for non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try std.testing.expect(storage.zcard("nosuchkey") == null);
}

test "storage - zrange returns members in order" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
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
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const scores = [_]f64{1.0};
    const members = [_][]const u8{"one"};
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    try std.testing.expectEqual(ValueType.sorted_set, storage.getType("myzset").?);
}

// ── Stream tests ──────────────────────────────────────────────────────────────

test "storage - xadd creates stream with auto-generated ID" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const fields = [_][]const u8{ "temp", "25", "humidity", "60" };
    const id = try storage.xadd("weather", "*", &fields, null);

    try std.testing.expect(id.ms > 0);
    try std.testing.expectEqual(@as(u64, 0), id.seq);
}

test "storage - xadd with explicit ID" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const fields = [_][]const u8{ "field1", "value1" };
    const id = try storage.xadd("mystream", "1234567890-0", &fields, null);

    try std.testing.expectEqual(@as(i64, 1234567890), id.ms);
    try std.testing.expectEqual(@as(u64, 0), id.seq);
}

test "storage - xadd enforces ID ordering" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const fields = [_][]const u8{ "a", "1" };
    _ = try storage.xadd("s", "1000-0", &fields, null);

    // Try to add earlier ID - should fail
    const result = storage.xadd("s", "999-0", &fields, null);
    try std.testing.expectError(error.StreamIdTooSmall, result);
}

test "storage - xlen returns entry count" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const fields = [_][]const u8{ "a", "1" };
    _ = try storage.xadd("s", "1000-0", &fields, null);
    _ = try storage.xadd("s", "1001-0", &fields, null);
    _ = try storage.xadd("s", "1002-0", &fields, null);

    const len = (try storage.xlen("s")).?;
    try std.testing.expectEqual(@as(usize, 3), len);
}

test "storage - xlen on non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const len = try storage.xlen("nosuchkey");
    try std.testing.expect(len == null);
}

test "storage - xrange returns entries in range" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const fields = [_][]const u8{ "data", "x" };
    _ = try storage.xadd("s", "1000-0", &fields, null);
    _ = try storage.xadd("s", "2000-0", &fields, null);
    _ = try storage.xadd("s", "3000-0", &fields, null);

    const result = (try storage.xrange(allocator, "s", "1500-0", "2500-0", null)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqual(@as(i64, 2000), result[0].id.ms);
}

test "storage - xrange with - and + bounds" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const fields = [_][]const u8{ "x", "y" };
    _ = try storage.xadd("s", "1000-0", &fields, null);
    _ = try storage.xadd("s", "2000-0", &fields, null);

    const result = (try storage.xrange(allocator, "s", "-", "+", null)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 2), result.len);
}

test "storage - xrange with COUNT limit" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const fields = [_][]const u8{ "a", "b" };
    _ = try storage.xadd("s", "1000-0", &fields, null);
    _ = try storage.xadd("s", "2000-0", &fields, null);
    _ = try storage.xadd("s", "3000-0", &fields, null);

    const result = (try storage.xrange(allocator, "s", "-", "+", 2)).?;
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 2), result.len);
}

test "storage - getType returns stream type" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const fields = [_][]const u8{ "a", "1" };
    _ = try storage.xadd("mystream", "*", &fields, null);

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

    try storage.restoreValue("newkey", dump, 0, false);

    const value = storage.get("newkey").?;
    try std.testing.expectEqualStrings("hello", value);
}

test "storage - dump and restore list" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const elements = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elements, null);

    const dump = (try storage.dumpValue(allocator, "mylist")).?;
    defer allocator.free(dump);

    try storage.restoreValue("newlist", dump, 0, false);

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
    try storage.restoreValue("newkey", dump, 3600 * 1000, false);

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
    const result = storage.restoreValue("key2", dump, 0, false);
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
    try storage.restoreValue("key2", dump, 0, true);

    const value = storage.get("key2").?;
    try std.testing.expectEqualStrings("value1", value);
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
