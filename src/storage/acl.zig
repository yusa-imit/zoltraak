const std = @import("std");
const Allocator = std.mem.Allocator;
const CommandRegistry = @import("../commands/command_registry.zig");

/// ACL command category enum (21 core categories)
pub const CommandCategory = CommandRegistry.CommandCategory;

/// ACL user representation with command and key permission support
pub const User = struct {
    username: []const u8,
    password: ?[]const u8, // null = nopass
    enabled: bool,

    // Command permission management
    all_commands_allowed: bool = false, // false = starts with -@all (deny all)
    allowed_commands: std.StringHashMap(void), // Set of explicitly allowed commands
    denied_commands: std.StringHashMap(void), // Set of explicitly denied commands
    allowed_categories: std.AutoHashMap(CommandCategory, void), // Set of allowed categories
    denied_categories: std.AutoHashMap(CommandCategory, void), // Set of denied categories

    // Key pattern permission management
    all_keys_allowed: bool = true, // true = default user can access all keys
    allowed_key_patterns: std.ArrayList([]const u8), // ~pattern — allowed glob patterns
    read_only_key_patterns: std.ArrayList([]const u8), // %R~pattern — read-only glob patterns
    write_only_key_patterns: std.ArrayList([]const u8), // %W~pattern — write-only glob patterns

    pub fn deinit(self: *User, allocator: Allocator) void {
        allocator.free(self.username);
        if (self.password) |pwd| {
            allocator.free(pwd);
        }
        // Deinit permission maps
        var iter = self.allowed_commands.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
        self.allowed_commands.deinit();

        iter = self.denied_commands.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
        self.denied_commands.deinit();

        self.allowed_categories.deinit();
        self.denied_categories.deinit();

        // Deinit key pattern lists
        for (self.allowed_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        self.allowed_key_patterns.deinit(allocator);

        for (self.read_only_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        self.read_only_key_patterns.deinit(allocator);

        for (self.write_only_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        self.write_only_key_patterns.deinit(allocator);
    }

    pub fn clone(self: *const User, allocator: Allocator) !User {
        const username_copy = try allocator.dupe(u8, self.username);
        errdefer allocator.free(username_copy);

        const password_copy = if (self.password) |pwd|
            try allocator.dupe(u8, pwd)
        else
            null;
        errdefer if (password_copy) |pwd| allocator.free(pwd);

        // Clone allowed_commands
        var allowed_cmds = std.StringHashMap(void).init(allocator);
        errdefer {
            var it = allowed_cmds.keyIterator();
            while (it.next()) |key| {
                allocator.free(key.*);
            }
            allowed_cmds.deinit();
        }

        var iter = self.allowed_commands.keyIterator();
        while (iter.next()) |key| {
            const key_copy = try allocator.dupe(u8, key.*);
            errdefer allocator.free(key_copy);
            try allowed_cmds.put(key_copy, {});
        }

        // Clone denied_commands
        var denied_cmds = std.StringHashMap(void).init(allocator);
        errdefer {
            var it = denied_cmds.keyIterator();
            while (it.next()) |key| {
                allocator.free(key.*);
            }
            denied_cmds.deinit();
        }

        iter = self.denied_commands.keyIterator();
        while (iter.next()) |key| {
            const key_copy = try allocator.dupe(u8, key.*);
            errdefer allocator.free(key_copy);
            try denied_cmds.put(key_copy, {});
        }

        // Clone allowed_categories
        var allowed_cats = std.AutoHashMap(CommandCategory, void).init(allocator);
        errdefer allowed_cats.deinit();

        var cat_iter = self.allowed_categories.keyIterator();
        while (cat_iter.next()) |cat| {
            try allowed_cats.put(cat.*, {});
        }

        // Clone denied_categories
        var denied_cats = std.AutoHashMap(CommandCategory, void).init(allocator);
        errdefer denied_cats.deinit();

        cat_iter = self.denied_categories.keyIterator();
        while (cat_iter.next()) |cat| {
            try denied_cats.put(cat.*, {});
        }

        // Clone key pattern lists
        var allowed_key_pats = std.ArrayList([]const u8){};
        errdefer {
            for (allowed_key_pats.items) |pat| {
                allocator.free(pat);
            }
            allowed_key_pats.deinit(allocator);
        }

        for (self.allowed_key_patterns.items) |pat| {
            const pat_copy = try allocator.dupe(u8, pat);
            errdefer allocator.free(pat_copy);
            try allowed_key_pats.append(allocator, pat_copy);
        }

        var read_only_pats = std.ArrayList([]const u8){};
        errdefer {
            for (read_only_pats.items) |pat| {
                allocator.free(pat);
            }
            read_only_pats.deinit(allocator);
        }

        for (self.read_only_key_patterns.items) |pat| {
            const pat_copy = try allocator.dupe(u8, pat);
            errdefer allocator.free(pat_copy);
            try read_only_pats.append(allocator, pat_copy);
        }

        var write_only_pats = std.ArrayList([]const u8){};
        errdefer {
            for (write_only_pats.items) |pat| {
                allocator.free(pat);
            }
            write_only_pats.deinit(allocator);
        }

        for (self.write_only_key_patterns.items) |pat| {
            const pat_copy = try allocator.dupe(u8, pat);
            errdefer allocator.free(pat_copy);
            try write_only_pats.append(allocator, pat_copy);
        }

        return User{
            .username = username_copy,
            .password = password_copy,
            .enabled = self.enabled,
            .all_commands_allowed = self.all_commands_allowed,
            .allowed_commands = allowed_cmds,
            .denied_commands = denied_cmds,
            .allowed_categories = allowed_cats,
            .denied_categories = denied_cats,
            .all_keys_allowed = self.all_keys_allowed,
            .allowed_key_patterns = allowed_key_pats,
            .read_only_key_patterns = read_only_pats,
            .write_only_key_patterns = write_only_pats,
        };
    }

    /// Check if a user has permission to execute a command
    /// Implements left-to-right precedence: last matching rule wins
    pub fn hasCommandPermission(self: *const User, command_name: []const u8) bool {
        // Always-allowed commands bypass ACL
        const always_allowed = [_][]const u8{
            "AUTH", "HELLO", "PING",
        };
        for (always_allowed) |cmd| {
            if (std.mem.eql(u8, command_name, cmd)) {
                return true;
            }
        }

        // Uppercase for comparison
        var buf: [64]u8 = undefined;
        const cmd_upper = std.ascii.upperString(&buf, command_name);

        // Check explicit denied commands first (highest priority)
        if (self.denied_commands.contains(cmd_upper)) {
            return false;
        }

        // Check explicit allowed commands
        if (self.allowed_commands.contains(cmd_upper)) {
            return true;
        }

        // Check category-based permissions
        const categories = CommandRegistry.getCategoriesForCommand(cmd_upper) catch return false;

        // Check if any category is explicitly denied
        for (categories) |cat| {
            if (self.denied_categories.contains(cat)) {
                return false;
            }
        }

        // Check if any category is explicitly allowed
        for (categories) |cat| {
            if (self.allowed_categories.contains(cat)) {
                return true;
            }
        }

        // Fall back to all_commands_allowed setting
        return self.all_commands_allowed;
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
            .all_commands_allowed = true, // Default user has all commands
            .allowed_commands = std.StringHashMap(void).init(self.allocator),
            .denied_commands = std.StringHashMap(void).init(self.allocator),
            .allowed_categories = std.AutoHashMap(CommandCategory, void).init(self.allocator),
            .denied_categories = std.AutoHashMap(CommandCategory, void).init(self.allocator),
            .all_keys_allowed = true, // Default user has all keys
            .allowed_key_patterns = std.ArrayList([]const u8){},
            .read_only_key_patterns = std.ArrayList([]const u8){},
            .write_only_key_patterns = std.ArrayList([]const u8){},
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
        self.setUserWithPermissions(username, enabled, password, false, null, null, null, null) catch |err| {
            return err;
        };
    }

    /// Set or update user with explicit permission parameters
    /// If permissions are null, uses defaults: all_commands_allowed=false, empty sets
    pub fn setUserWithPermissions(
        self: *ACLStore,
        username: []const u8,
        enabled: bool,
        password: ?[]const u8,
        all_commands_allowed: ?bool,
        allowed_commands: ?std.StringHashMap(void),
        denied_commands: ?std.StringHashMap(void),
        allowed_categories: ?std.AutoHashMap(CommandCategory, void),
        denied_categories: ?std.AutoHashMap(CommandCategory, void),
    ) !void {
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

        // Initialize permission sets with defaults or provided values
        var allowed_cmds = if (allowed_commands) |cmds| cmds else std.StringHashMap(void).init(self.allocator);
        errdefer {
            var iter = allowed_cmds.keyIterator();
            while (iter.next()) |key| {
                self.allocator.free(key.*);
            }
            allowed_cmds.deinit();
        }

        var denied_cmds = if (denied_commands) |cmds| cmds else std.StringHashMap(void).init(self.allocator);
        errdefer {
            var iter = denied_cmds.keyIterator();
            while (iter.next()) |key| {
                self.allocator.free(key.*);
            }
            denied_cmds.deinit();
        }

        var allowed_cats = if (allowed_categories) |cats| cats else std.AutoHashMap(CommandCategory, void).init(self.allocator);
        errdefer allowed_cats.deinit();

        var denied_cats = if (denied_categories) |cats| cats else std.AutoHashMap(CommandCategory, void).init(self.allocator);
        errdefer denied_cats.deinit();

        const user = User{
            .username = username_copy,
            .password = password_copy,
            .enabled = enabled,
            .all_commands_allowed = all_commands_allowed orelse false,
            .allowed_commands = allowed_cmds,
            .denied_commands = denied_cmds,
            .allowed_categories = allowed_cats,
            .denied_categories = denied_cats,
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

        var list = std.ArrayList([]const u8){};
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

        var list = std.ArrayList([]const u8){};
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

            try list.append(allocator, rule);
        }

        return list.toOwnedSlice(allocator);
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

test "User: hasCommandPermission allows AUTH/HELLO/PING always" {
    const allocator = std.testing.allocator;

    var user = User{
        .username = try allocator.dupe(u8, "test"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = false,
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
    };
    defer user.deinit(allocator);

    // Even with no permissions, AUTH/HELLO/PING should work
    try std.testing.expect(user.hasCommandPermission("AUTH"));
    try std.testing.expect(user.hasCommandPermission("HELLO"));
    try std.testing.expect(user.hasCommandPermission("PING"));
}

test "User: hasCommandPermission respects all_commands_allowed flag" {
    const allocator = std.testing.allocator;

    var user = User{
        .username = try allocator.dupe(u8, "test"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = true,
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
    };
    defer user.deinit(allocator);

    try std.testing.expect(user.hasCommandPermission("GET"));
    try std.testing.expect(user.hasCommandPermission("SET"));
    try std.testing.expect(user.hasCommandPermission("ANYCOMMAND"));
}

test "User: hasCommandPermission denies by default (all_commands_allowed=false)" {
    const allocator = std.testing.allocator;

    var user = User{
        .username = try allocator.dupe(u8, "test"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = false,
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
    };
    defer user.deinit(allocator);

    // GET should be denied (not in allowed set, not in allowed categories)
    try std.testing.expect(!user.hasCommandPermission("GET"));
    try std.testing.expect(!user.hasCommandPermission("SET"));
}

test "User: hasCommandPermission respects explicit allowed commands" {
    const allocator = std.testing.allocator;

    var allowed = std.StringHashMap(void).init(allocator);
    defer {
        var iter = allowed.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
        allowed.deinit();
    }

    try allowed.put(try allocator.dupe(u8, "GET"), {});

    var user = User{
        .username = try allocator.dupe(u8, "test"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = false,
        .allowed_commands = allowed,
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
    };
    defer user.deinit(allocator);

    try std.testing.expect(user.hasCommandPermission("GET"));
    try std.testing.expect(!user.hasCommandPermission("SET")); // Not in allowed set
}

test "User: hasCommandPermission respects explicit denied commands (takes priority)" {
    const allocator = std.testing.allocator;

    var denied = std.StringHashMap(void).init(allocator);
    defer {
        var iter = denied.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
        denied.deinit();
    }

    try denied.put(try allocator.dupe(u8, "DEL"), {});

    var user = User{
        .username = try allocator.dupe(u8, "test"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = true, // All allowed
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = denied,
        .allowed_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
    };
    defer user.deinit(allocator);

    // DEL should be denied even though all_commands_allowed=true
    try std.testing.expect(!user.hasCommandPermission("DEL"));
    try std.testing.expect(user.hasCommandPermission("SET")); // Other commands allowed
}

test "User: hasCommandPermission respects allowed categories" {
    const allocator = std.testing.allocator;

    var allowed_cats = std.AutoHashMap(CommandCategory, void).init(allocator);
    try allowed_cats.put(CommandCategory.fast, {});

    var user = User{
        .username = try allocator.dupe(u8, "test"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = false,
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = allowed_cats,
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
    };
    defer user.deinit(allocator);

    // GET is in fast category, should be allowed
    try std.testing.expect(user.hasCommandPermission("GET"));
    try std.testing.expect(user.hasCommandPermission("PING"));
    // LPUSH is not in fast category, should be denied
    try std.testing.expect(!user.hasCommandPermission("LPUSH"));
}

test "User: hasCommandPermission respects denied categories (takes priority)" {
    const allocator = std.testing.allocator;

    var allowed_cats = std.AutoHashMap(CommandCategory, void).init(allocator);
    try allowed_cats.put(CommandCategory.string, {});

    var denied_cats = std.AutoHashMap(CommandCategory, void).init(allocator);
    try denied_cats.put(CommandCategory.write, {});

    var user = User{
        .username = try allocator.dupe(u8, "test"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = false,
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = allowed_cats,
        .denied_categories = denied_cats,
    };
    defer user.deinit(allocator);

    // GET is in string+read, allowed
    try std.testing.expect(user.hasCommandPermission("GET"));
    // APPEND is in string+write, write is denied so should be denied
    try std.testing.expect(!user.hasCommandPermission("APPEND"));
}
