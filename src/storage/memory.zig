const std = @import("std");

/// Type of value stored in the key-value store
pub const ValueType = enum {
    string,
    list,
    set,
    hash,
};

/// Value stored in the key-value store with optional expiration
/// Tagged union supporting multiple Redis data types
pub const Value = union(ValueType) {
    string: StringValue,
    list: ListValue,
    set: SetValue,
    hash: HashValue,

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

    /// Deinitialize value and free all associated memory
    pub fn deinit(self: *Value, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .string => |*s| s.deinit(allocator),
            .list => |*l| l.deinit(allocator),
            .set => |*s| s.deinit(allocator),
            .hash => |*h| h.deinit(allocator),
        }
    }

    /// Get expiration timestamp for any value type
    pub fn getExpiration(self: Value) ?i64 {
        return switch (self) {
            .string => |s| s.expires_at,
            .list => |l| l.expires_at,
            .set => |s| s.expires_at,
            .hash => |h| h.expires_at,
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
    mutex: std.Thread.Mutex,

    /// Initialize a new storage instance
    pub fn init(allocator: std.mem.Allocator) !*Storage {
        const storage = try allocator.create(Storage);
        errdefer allocator.destroy(storage);

        storage.* = Storage{
            .allocator = allocator,
            .data = std.StringHashMap(Value).init(allocator),
            .mutex = std.Thread.Mutex{},
        };

        return storage;
    }

    /// Deinitialize storage and free all keys and values
    pub fn deinit(self: *Storage) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Free all keys and values
        var it = self.data.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var value = entry.value_ptr.*;
            value.deinit(self.allocator);
        }
        self.data.deinit();

        const allocator = self.allocator;
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
                    // Insert elements at head in reverse order to maintain order
                    var i: usize = elements.len;
                    while (i > 0) {
                        i -= 1;
                        const owned_elem = try self.allocator.dupe(u8, elements[i]);
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

    /// Get current Unix timestamp in milliseconds
    pub fn getCurrentTimestamp() i64 {
        return std.time.milliTimestamp();
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
