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
};

/// Value stored in the key-value store with optional expiration
/// Tagged union supporting multiple Redis data types
pub const Value = union(ValueType) {
    string: StringValue,
    list: ListValue,
    set: SetValue,
    hash: HashValue,
    sorted_set: SortedSetValue,

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

    /// Deinitialize value and free all associated memory
    pub fn deinit(self: *Value, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .string => |*s| s.deinit(allocator),
            .list => |*l| l.deinit(allocator),
            .set => |*s| s.deinit(allocator),
            .hash => |*h| h.deinit(allocator),
            .sorted_set => |*z| z.deinit(allocator),
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
