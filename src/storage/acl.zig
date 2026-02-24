const std = @import("std");
const Allocator = std.mem.Allocator;

/// ACL user representation
pub const User = struct {
    username: []const u8,
    password: ?[]const u8, // null = nopass
    enabled: bool,
    // Simplified: all commands allowed for now (stub implementation)

    pub fn deinit(self: *User, allocator: Allocator) void {
        allocator.free(self.username);
        if (self.password) |pwd| {
            allocator.free(pwd);
        }
    }

    pub fn clone(self: *const User, allocator: Allocator) !User {
        const username_copy = try allocator.dupe(u8, self.username);
        errdefer allocator.free(username_copy);

        const password_copy = if (self.password) |pwd|
            try allocator.dupe(u8, pwd)
        else
            null;

        return User{
            .username = username_copy,
            .password = password_copy,
            .enabled = self.enabled,
        };
    }
};

/// ACL storage and management
pub const ACLStore = struct {
    allocator: Allocator,
    users: std.StringHashMap(User),
    mutex: std.Thread.Mutex,

    pub fn init(allocator: Allocator) !ACLStore {
        var store = ACLStore{
            .allocator = allocator,
            .users = std.StringHashMap(User).init(allocator),
            .mutex = .{},
        };

        // Create default user with full permissions
        try store.createDefaultUser();

        return store;
    }

    pub fn deinit(self: *ACLStore) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var iter = self.users.iterator();
        while (iter.next()) |entry| {
            var user = entry.value_ptr;
            user.deinit(self.allocator);
        }
        self.users.deinit();
    }

    fn createDefaultUser(self: *ACLStore) !void {
        const default_username = try self.allocator.dupe(u8, "default");
        const user = User{
            .username = default_username,
            .password = null, // nopass
            .enabled = true,
        };
        try self.users.put(default_username, user);
    }

    /// Get user by username
    pub fn getUser(self: *ACLStore, username: []const u8) ?*const User {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.users.getPtr(username);
    }

    /// Set or update user
    pub fn setUser(self: *ACLStore, username: []const u8, enabled: bool, password: ?[]const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Remove existing user if present
        if (self.users.fetchRemove(username)) |kv| {
            var old_user = kv.value;
            old_user.deinit(self.allocator);
        }

        const username_copy = try self.allocator.dupe(u8, username);
        errdefer self.allocator.free(username_copy);

        const password_copy = if (password) |pwd|
            try self.allocator.dupe(u8, pwd)
        else
            null;

        const user = User{
            .username = username_copy,
            .password = password_copy,
            .enabled = enabled,
        };

        try self.users.put(username_copy, user);
    }

    /// Delete user
    pub fn deleteUser(self: *ACLStore, username: []const u8) !bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Cannot delete default user
        if (std.mem.eql(u8, username, "default")) {
            return error.CannotDeleteDefaultUser;
        }

        if (self.users.fetchRemove(username)) |kv| {
            var user = kv.value;
            user.deinit(self.allocator);
            return true;
        }

        return false;
    }

    /// List all usernames
    pub fn listUsernames(self: *ACLStore, allocator: Allocator) ![][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var list = std.ArrayList([]const u8).init(allocator);
        errdefer list.deinit();

        var iter = self.users.keyIterator();
        while (iter.next()) |username| {
            const copy = try allocator.dupe(u8, username.*);
            try list.append(copy);
        }

        return list.toOwnedSlice();
    }

    /// Get ACL rules list (simplified stub implementation)
    pub fn getACLList(self: *ACLStore, allocator: Allocator) ![][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var list = std.ArrayList([]const u8).init(allocator);
        errdefer {
            for (list.items) |item| {
                allocator.free(item);
            }
            list.deinit();
        }

        var iter = self.users.iterator();
        while (iter.next()) |entry| {
            const user = entry.value_ptr;

            // Format: "user <username> <enabled> <nopass|>password> ~* +@all"
            const rule = if (user.password == null)
                try std.fmt.allocPrint(allocator, "user {s} {s} nopass ~* +@all", .{
                    user.username,
                    if (user.enabled) "on" else "off",
                })
            else
                try std.fmt.allocPrint(allocator, "user {s} {s} >****** ~* +@all", .{
                    user.username,
                    if (user.enabled) "on" else "off",
                });

            try list.append(rule);
        }

        return list.toOwnedSlice();
    }
};

// Unit tests
test "ACLStore: create and get default user" {
    const allocator = std.testing.allocator;

    var store = try ACLStore.init(allocator);
    defer store.deinit();

    const user = store.getUser("default");
    try std.testing.expect(user != null);
    try std.testing.expectEqualStrings("default", user.?.username);
    try std.testing.expect(user.?.password == null);
    try std.testing.expect(user.?.enabled);
}

test "ACLStore: set and get custom user" {
    const allocator = std.testing.allocator;

    var store = try ACLStore.init(allocator);
    defer store.deinit();

    try store.setUser("alice", true, "secret123");

    const user = store.getUser("alice");
    try std.testing.expect(user != null);
    try std.testing.expectEqualStrings("alice", user.?.username);
    try std.testing.expect(user.?.password != null);
    try std.testing.expectEqualStrings("secret123", user.?.password.?);
    try std.testing.expect(user.?.enabled);
}

test "ACLStore: delete user" {
    const allocator = std.testing.allocator;

    var store = try ACLStore.init(allocator);
    defer store.deinit();

    try store.setUser("bob", true, null);
    try std.testing.expect(store.getUser("bob") != null);

    const deleted = try store.deleteUser("bob");
    try std.testing.expect(deleted);
    try std.testing.expect(store.getUser("bob") == null);
}

test "ACLStore: cannot delete default user" {
    const allocator = std.testing.allocator;

    var store = try ACLStore.init(allocator);
    defer store.deinit();

    const result = store.deleteUser("default");
    try std.testing.expectError(error.CannotDeleteDefaultUser, result);
}

test "ACLStore: list usernames" {
    const allocator = std.testing.allocator;

    var store = try ACLStore.init(allocator);
    defer store.deinit();

    try store.setUser("alice", true, "pass1");
    try store.setUser("bob", false, null);

    const usernames = try store.listUsernames(allocator);
    defer {
        for (usernames) |username| {
            allocator.free(username);
        }
        allocator.free(usernames);
    }

    try std.testing.expect(usernames.len == 3); // default, alice, bob
}

test "ACLStore: get ACL list" {
    const allocator = std.testing.allocator;

    var store = try ACLStore.init(allocator);
    defer store.deinit();

    try store.setUser("alice", true, "secret");

    const rules = try store.getACLList(allocator);
    defer {
        for (rules) |rule| {
            allocator.free(rule);
        }
        allocator.free(rules);
    }

    try std.testing.expect(rules.len == 2); // default + alice
}
