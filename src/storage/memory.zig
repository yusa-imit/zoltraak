const std = @import("std");

/// Type of value stored in the key-value store
pub const ValueType = enum {
    string,
    list,
};

/// Value stored in the key-value store with optional expiration
/// Tagged union supporting multiple Redis data types
pub const Value = union(ValueType) {
    string: StringValue,
    list: ListValue,

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

    /// Deinitialize value and free all associated memory
    pub fn deinit(self: *Value, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .string => |*s| s.deinit(allocator),
            .list => |*l| l.deinit(allocator),
        }
    }

    /// Get expiration timestamp for any value type
    pub fn getExpiration(self: Value) ?i64 {
        return switch (self) {
            .string => |s| s.expires_at,
            .list => |l| l.expires_at,
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
