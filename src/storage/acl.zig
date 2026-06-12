const std = @import("std");
const Allocator = std.mem.Allocator;
const CommandRegistry = @import("../commands/command_registry.zig");
const glob = @import("../utils/glob.zig");

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

    /// Check if a user has permission to access a key with the given access mode.
    ///
    /// Arguments:
    ///   - key: The key to check permission for
    ///   - access_mode: Must be "read" or "write" (asserts in debug mode)
    ///
    /// Returns:
    ///   - true if user is authorized for the specified access mode
    ///   - false if user is denied or access_mode is invalid
    ///
    /// Permission Check Order:
    ///   1. If all_keys_allowed=true, grant access immediately
    ///   2. Check allowed_key_patterns (full read+write access)
    ///   3. Check read_only_key_patterns (only if access_mode="read")
    ///   4. Check write_only_key_patterns (only if access_mode="write")
    ///   5. Deny if no patterns match
    pub fn hasKeyPermission(self: *const User, key: []const u8, access_mode: []const u8) bool {
        // Validate access_mode (debug builds only)
        std.debug.assert(std.mem.eql(u8, access_mode, "read") or std.mem.eql(u8, access_mode, "write"));

        // If all keys are allowed, grant access immediately
        if (self.all_keys_allowed) {
            return true;
        }

        // Check allowed_key_patterns (full access: read + write)
        for (self.allowed_key_patterns.items) |pattern| {
            if (glob.matchGlob(pattern, key)) {
                return true;
            }
        }

        // Check read_only_key_patterns (read-only access)
        if (std.mem.eql(u8, access_mode, "read")) {
            for (self.read_only_key_patterns.items) |pattern| {
                if (glob.matchGlob(pattern, key)) {
                    return true;
                }
            }
        }

        // Check write_only_key_patterns (write-only access)
        if (std.mem.eql(u8, access_mode, "write")) {
            for (self.write_only_key_patterns.items) |pattern| {
                if (glob.matchGlob(pattern, key)) {
                    return true;
                }
            }
        }

        // No matching patterns found
        return false;
    }
};

/// Reason why an ACL denial occurred
pub const DenialReason = enum {
    auth, // NOAUTH: authentication required
    command, // NOPERM: user lacks command permission
    key, // NOPERM: user lacks key permission
    channel, // NOPERM: user lacks channel permission
};

/// A single ACL violation log entry
pub const LogEntry = struct {
    count: u64, // how many times this pattern was seen
    reason: DenialReason,
    object: []const u8, // command name or key that was denied
    username: []const u8, // authenticated user (or "(noauth)")
    client_info: []const u8, // client address/info string
    timestamp_sec: i64, // Unix timestamp of first occurrence
    timestamp_last_sec: i64, // Unix timestamp of last occurrence
    entry_id: u64, // monotonically increasing entry ID

    pub fn deinit(self: *LogEntry, allocator: Allocator) void {
        allocator.free(self.object);
        allocator.free(self.username);
        allocator.free(self.client_info);
    }
};

/// Default max ACL log entries (matches Redis default acllog-max-len=128)
pub const ACL_LOG_MAX_DEFAULT: usize = 128;

/// ACL storage and management
pub const ACLStore = struct {
    allocator: Allocator,
    users: std.StringHashMap(User),
    mutex: std.Thread.Mutex,
    log: std.ArrayList(LogEntry),
    log_max_len: usize,
    next_entry_id: u64,

    pub fn init(allocator: Allocator) !ACLStore {
        var store = ACLStore{
            .allocator = allocator,
            .users = std.StringHashMap(User).init(allocator),
            .mutex = .{},
            .log = std.ArrayList(LogEntry){},
            .log_max_len = ACL_LOG_MAX_DEFAULT,
            .next_entry_id = 0,
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

        for (self.log.items) |*entry| {
            entry.deinit(self.allocator);
        }
        self.log.deinit(self.allocator);
    }

    /// Record an ACL violation. Deduplicates by (reason, object, username):
    /// if the most recent entry matches, just increments count and updates timestamp.
    pub fn addLogEntry(
        self: *ACLStore,
        reason: DenialReason,
        object: []const u8,
        username: []const u8,
        client_info: []const u8,
    ) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = std.time.timestamp();

        // Check if last entry is the same violation (dedup)
        if (self.log.items.len > 0) {
            const last = &self.log.items[self.log.items.len - 1];
            if (last.reason == reason and
                std.mem.eql(u8, last.object, object) and
                std.mem.eql(u8, last.username, username))
            {
                last.count += 1;
                last.timestamp_last_sec = now;
                return;
            }
        }

        // Enforce max length: remove oldest entry if at capacity
        if (self.log.items.len >= self.log_max_len) {
            var oldest = self.log.orderedRemove(0);
            oldest.deinit(self.allocator);
        }

        const obj_copy = self.allocator.dupe(u8, object) catch return;
        const user_copy = self.allocator.dupe(u8, username) catch {
            self.allocator.free(obj_copy);
            return;
        };
        const info_copy = self.allocator.dupe(u8, client_info) catch {
            self.allocator.free(obj_copy);
            self.allocator.free(user_copy);
            return;
        };

        const entry = LogEntry{
            .count = 1,
            .reason = reason,
            .object = obj_copy,
            .username = user_copy,
            .client_info = info_copy,
            .timestamp_sec = now,
            .timestamp_last_sec = now,
            .entry_id = self.next_entry_id,
        };
        self.next_entry_id += 1;

        self.log.append(self.allocator, entry) catch {
            self.allocator.free(obj_copy);
            self.allocator.free(user_copy);
            self.allocator.free(info_copy);
        };
    }

    /// Get recent log entries. count=0 means return all entries.
    /// Returns entries in reverse chronological order (newest first).
    /// Caller owns returned slice but NOT the LogEntry fields (they point into the store).
    pub fn getLogEntries(self: *ACLStore, count: usize) []const LogEntry {
        self.mutex.lock();
        defer self.mutex.unlock();

        const total = self.log.items.len;
        if (total == 0) return &[_]LogEntry{};

        const limit = if (count == 0 or count >= total) total else count;
        const start = total - limit;
        return self.log.items[start..];
    }

    /// Clear the ACL log
    pub fn resetLog(self: *ACLStore) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.log.items) |*entry| {
            entry.deinit(self.allocator);
        }
        self.log.clearRetainingCapacity();
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
            .all_keys_allowed = false, // Default to restrictive
            .allowed_key_patterns = std.ArrayList([]const u8){},
            .read_only_key_patterns = std.ArrayList([]const u8){},
            .write_only_key_patterns = std.ArrayList([]const u8){},
        };

        try self.users.put(username_copy, user);
    }

    /// Create or update user with full permissions including key patterns
    /// This is the complete version used by ACL SETUSER command
    pub fn createOrUpdateUser(
        self: *ACLStore,
        username: []const u8,
        enabled: bool,
        password: ?[]const u8,
        all_commands_allowed: bool,
        allowed_commands: std.StringHashMap(void),
        denied_commands: std.StringHashMap(void),
        allowed_categories: std.AutoHashMap(CommandCategory, void),
        denied_categories: std.AutoHashMap(CommandCategory, void),
        all_keys_allowed: bool,
        allowed_key_patterns: std.ArrayList([]const u8),
        read_only_key_patterns: std.ArrayList([]const u8),
        write_only_key_patterns: std.ArrayList([]const u8),
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

        const user = User{
            .username = username_copy,
            .password = password_copy,
            .enabled = enabled,
            .all_commands_allowed = all_commands_allowed,
            .allowed_commands = allowed_commands,
            .denied_commands = denied_commands,
            .allowed_categories = allowed_categories,
            .denied_categories = denied_categories,
            .all_keys_allowed = all_keys_allowed,
            .allowed_key_patterns = allowed_key_patterns,
            .read_only_key_patterns = read_only_key_patterns,
            .write_only_key_patterns = write_only_key_patterns,
        };

        try self.users.put(username_copy, user);
    }

    /// Update the default user's password (used by requirepass config)
    /// Pass null or empty string to restore nopass behavior.
    pub fn updateDefaultUserPassword(self: *ACLStore, password: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const user = self.users.getPtr("default") orelse return error.UserNotFound;
        // Free old password
        if (user.password) |old_pwd| {
            self.allocator.free(old_pwd);
            user.password = null;
        }
        // Set new password (empty string = nopass)
        if (password.len > 0) {
            user.password = try self.allocator.dupe(u8, password);
        }
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
        errdefer {
            for (list.items) |name| allocator.free(name);
            list.deinit(allocator);
        }

        var iter = self.users.keyIterator();
        while (iter.next()) |username| {
            const copy = try allocator.dupe(u8, username.*);
            try list.append(allocator, copy);
        }

        return list.toOwnedSlice(allocator);
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
            list.deinit(allocator);
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

// ── Key Permission Tests ──────────────────────────────────────────────────────

test "User: hasKeyPermission with all_keys_allowed=true allows any key" {
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
        .all_keys_allowed = true,
        .allowed_key_patterns = std.ArrayList([]const u8){},
        .read_only_key_patterns = std.ArrayList([]const u8){},
        .write_only_key_patterns = std.ArrayList([]const u8){},
    };
    defer user.deinit(allocator);

    // all_keys_allowed=true → should allow any key for any access mode
    try std.testing.expect(user.hasKeyPermission("user:123", "read"));
    try std.testing.expect(user.hasKeyPermission("user:123", "write"));
    try std.testing.expect(user.hasKeyPermission("session:abc", "read"));
    try std.testing.expect(user.hasKeyPermission("session:abc", "write"));
    try std.testing.expect(user.hasKeyPermission("anykey", "read"));
    try std.testing.expect(user.hasKeyPermission("anykey", "write"));
}

test "User: hasKeyPermission with all_keys_allowed=false and empty patterns denies all keys" {
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
        .all_keys_allowed = false,
        .allowed_key_patterns = std.ArrayList([]const u8){},
        .read_only_key_patterns = std.ArrayList([]const u8){},
        .write_only_key_patterns = std.ArrayList([]const u8){},
    };
    defer user.deinit(allocator);

    // all_keys_allowed=false + no patterns → should deny all keys
    try std.testing.expect(!user.hasKeyPermission("user:123", "read"));
    try std.testing.expect(!user.hasKeyPermission("user:123", "write"));
    try std.testing.expect(!user.hasKeyPermission("anykey", "read"));
    try std.testing.expect(!user.hasKeyPermission("anykey", "write"));
}

test "User: hasKeyPermission with ~pattern allows read and write" {
    const allocator = std.testing.allocator;

    var allowed_patterns = std.ArrayList([]const u8){};
    try allowed_patterns.append(allocator, try allocator.dupe(u8, "user:*"));
    try allowed_patterns.append(allocator, try allocator.dupe(u8, "session:*"));

    var user = User{
        .username = try allocator.dupe(u8, "test"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = false,
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .all_keys_allowed = false,
        .allowed_key_patterns = allowed_patterns,
        .read_only_key_patterns = std.ArrayList([]const u8){},
        .write_only_key_patterns = std.ArrayList([]const u8){},
    };
    defer user.deinit(allocator);

    // Keys matching ~user:* should be allowed for both read and write
    try std.testing.expect(user.hasKeyPermission("user:123", "read"));
    try std.testing.expect(user.hasKeyPermission("user:123", "write"));
    try std.testing.expect(user.hasKeyPermission("user:999", "read"));
    try std.testing.expect(user.hasKeyPermission("user:999", "write"));

    // Keys matching ~session:* should be allowed for both read and write
    try std.testing.expect(user.hasKeyPermission("session:abc", "read"));
    try std.testing.expect(user.hasKeyPermission("session:abc", "write"));

    // Keys not matching any pattern should be denied
    try std.testing.expect(!user.hasKeyPermission("cache:123", "read"));
    try std.testing.expect(!user.hasKeyPermission("cache:123", "write"));
    try std.testing.expect(!user.hasKeyPermission("other:key", "read"));
    try std.testing.expect(!user.hasKeyPermission("other:key", "write"));
}

test "User: hasKeyPermission with %R~pattern allows read only" {
    const allocator = std.testing.allocator;

    var read_only_patterns = std.ArrayList([]const u8){};
    try read_only_patterns.append(allocator, try allocator.dupe(u8, "config:*"));

    var user = User{
        .username = try allocator.dupe(u8, "test"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = false,
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .all_keys_allowed = false,
        .allowed_key_patterns = std.ArrayList([]const u8){},
        .read_only_key_patterns = read_only_patterns,
        .write_only_key_patterns = std.ArrayList([]const u8){},
    };
    defer user.deinit(allocator);

    // Keys matching %R~config:* should be allowed for read but denied for write
    try std.testing.expect(user.hasKeyPermission("config:db", "read"));
    try std.testing.expect(!user.hasKeyPermission("config:db", "write"));
    try std.testing.expect(user.hasKeyPermission("config:cache", "read"));
    try std.testing.expect(!user.hasKeyPermission("config:cache", "write"));

    // Keys not matching should be denied for both
    try std.testing.expect(!user.hasKeyPermission("user:123", "read"));
    try std.testing.expect(!user.hasKeyPermission("user:123", "write"));
}

test "User: hasKeyPermission with %W~pattern allows write only" {
    const allocator = std.testing.allocator;

    var write_only_patterns = std.ArrayList([]const u8){};
    try write_only_patterns.append(allocator, try allocator.dupe(u8, "logs:*"));

    var user = User{
        .username = try allocator.dupe(u8, "test"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = false,
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .all_keys_allowed = false,
        .allowed_key_patterns = std.ArrayList([]const u8){},
        .read_only_key_patterns = std.ArrayList([]const u8){},
        .write_only_key_patterns = write_only_patterns,
    };
    defer user.deinit(allocator);

    // Keys matching %W~logs:* should be allowed for write but denied for read
    try std.testing.expect(!user.hasKeyPermission("logs:app", "read"));
    try std.testing.expect(user.hasKeyPermission("logs:app", "write"));
    try std.testing.expect(!user.hasKeyPermission("logs:system", "read"));
    try std.testing.expect(user.hasKeyPermission("logs:system", "write"));

    // Keys not matching should be denied for both
    try std.testing.expect(!user.hasKeyPermission("user:123", "read"));
    try std.testing.expect(!user.hasKeyPermission("user:123", "write"));
}

test "User: hasKeyPermission with multiple patterns - first match wins" {
    const allocator = std.testing.allocator;

    var allowed_patterns = std.ArrayList([]const u8){};
    try allowed_patterns.append(allocator, try allocator.dupe(u8, "user:*"));

    var read_only_patterns = std.ArrayList([]const u8){};
    try read_only_patterns.append(allocator, try allocator.dupe(u8, "user:admin:*"));

    var user = User{
        .username = try allocator.dupe(u8, "test"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = false,
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .all_keys_allowed = false,
        .allowed_key_patterns = allowed_patterns,
        .read_only_key_patterns = read_only_patterns,
        .write_only_key_patterns = std.ArrayList([]const u8){},
    };
    defer user.deinit(allocator);

    // user:admin:123 matches ~user:* first (allowed for both read and write)
    // Order: allowed_key_patterns → read_only_key_patterns → write_only_key_patterns
    try std.testing.expect(user.hasKeyPermission("user:admin:123", "read"));
    try std.testing.expect(user.hasKeyPermission("user:admin:123", "write"));

    // user:regular:456 matches ~user:* (allowed for both)
    try std.testing.expect(user.hasKeyPermission("user:regular:456", "read"));
    try std.testing.expect(user.hasKeyPermission("user:regular:456", "write"));
}

test "User: hasKeyPermission with glob wildcards - asterisk" {
    const allocator = std.testing.allocator;

    var allowed_patterns = std.ArrayList([]const u8){};
    try allowed_patterns.append(allocator, try allocator.dupe(u8, "cache:*:data"));

    var user = User{
        .username = try allocator.dupe(u8, "test"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = false,
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .all_keys_allowed = false,
        .allowed_key_patterns = allowed_patterns,
        .read_only_key_patterns = std.ArrayList([]const u8){},
        .write_only_key_patterns = std.ArrayList([]const u8){},
    };
    defer user.deinit(allocator);

    // Keys matching cache:*:data pattern
    try std.testing.expect(user.hasKeyPermission("cache:123:data", "read"));
    try std.testing.expect(user.hasKeyPermission("cache:abc:data", "write"));
    try std.testing.expect(user.hasKeyPermission("cache:user:session:data", "read"));

    // Keys not matching pattern
    try std.testing.expect(!user.hasKeyPermission("cache:123:meta", "read"));
    try std.testing.expect(!user.hasKeyPermission("cache:123", "write"));
    try std.testing.expect(!user.hasKeyPermission("data:cache:123", "read"));
}

test "User: hasKeyPermission with glob wildcards - question mark" {
    const allocator = std.testing.allocator;

    var allowed_patterns = std.ArrayList([]const u8){};
    try allowed_patterns.append(allocator, try allocator.dupe(u8, "tmp:?:key"));

    var user = User{
        .username = try allocator.dupe(u8, "test"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = false,
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .all_keys_allowed = false,
        .allowed_key_patterns = allowed_patterns,
        .read_only_key_patterns = std.ArrayList([]const u8){},
        .write_only_key_patterns = std.ArrayList([]const u8){},
    };
    defer user.deinit(allocator);

    // Keys matching tmp:?:key pattern (exactly one character)
    try std.testing.expect(user.hasKeyPermission("tmp:1:key", "read"));
    try std.testing.expect(user.hasKeyPermission("tmp:a:key", "write"));
    try std.testing.expect(user.hasKeyPermission("tmp:z:key", "read"));

    // Keys not matching pattern
    try std.testing.expect(!user.hasKeyPermission("tmp:12:key", "read")); // Two characters
    try std.testing.expect(!user.hasKeyPermission("tmp::key", "write")); // Zero characters
    try std.testing.expect(!user.hasKeyPermission("tmp:abc:key", "read")); // Three characters
}

test "User: hasKeyPermission with glob wildcards - character class" {
    const allocator = std.testing.allocator;

    var allowed_patterns = std.ArrayList([]const u8){};
    try allowed_patterns.append(allocator, try allocator.dupe(u8, "env:[abc]:*"));

    var user = User{
        .username = try allocator.dupe(u8, "test"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = false,
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .all_keys_allowed = false,
        .allowed_key_patterns = allowed_patterns,
        .read_only_key_patterns = std.ArrayList([]const u8){},
        .write_only_key_patterns = std.ArrayList([]const u8){},
    };
    defer user.deinit(allocator);

    // Keys matching env:[abc]:* pattern
    try std.testing.expect(user.hasKeyPermission("env:a:config", "read"));
    try std.testing.expect(user.hasKeyPermission("env:b:settings", "write"));
    try std.testing.expect(user.hasKeyPermission("env:c:db", "read"));

    // Keys not matching pattern
    try std.testing.expect(!user.hasKeyPermission("env:d:config", "read"));
    try std.testing.expect(!user.hasKeyPermission("env:x:settings", "write"));
    try std.testing.expect(!user.hasKeyPermission("env:1:db", "read"));
}

test "User: hasKeyPermission with glob wildcards - character range" {
    const allocator = std.testing.allocator;

    var allowed_patterns = std.ArrayList([]const u8){};
    try allowed_patterns.append(allocator, try allocator.dupe(u8, "shard:[0-9]:*"));

    var user = User{
        .username = try allocator.dupe(u8, "test"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = false,
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .all_keys_allowed = false,
        .allowed_key_patterns = allowed_patterns,
        .read_only_key_patterns = std.ArrayList([]const u8){},
        .write_only_key_patterns = std.ArrayList([]const u8){},
    };
    defer user.deinit(allocator);

    // Keys matching shard:[0-9]:* pattern
    try std.testing.expect(user.hasKeyPermission("shard:0:data", "read"));
    try std.testing.expect(user.hasKeyPermission("shard:5:data", "write"));
    try std.testing.expect(user.hasKeyPermission("shard:9:data", "read"));

    // Keys not matching pattern
    try std.testing.expect(!user.hasKeyPermission("shard:a:data", "read"));
    try std.testing.expect(!user.hasKeyPermission("shard:10:data", "write")); // Two digits
    try std.testing.expect(!user.hasKeyPermission("shard:x:data", "read"));
}

test "User: hasKeyPermission with glob wildcards - negated character class" {
    const allocator = std.testing.allocator;

    var allowed_patterns = std.ArrayList([]const u8){};
    try allowed_patterns.append(allocator, try allocator.dupe(u8, "data:[^t]*"));

    var user = User{
        .username = try allocator.dupe(u8, "test"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = false,
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .all_keys_allowed = false,
        .allowed_key_patterns = allowed_patterns,
        .read_only_key_patterns = std.ArrayList([]const u8){},
        .write_only_key_patterns = std.ArrayList([]const u8){},
    };
    defer user.deinit(allocator);

    // Keys matching data:[^t]* pattern (second char is not 't')
    try std.testing.expect(user.hasKeyPermission("data:a123", "read"));
    try std.testing.expect(user.hasKeyPermission("data:b456", "write"));
    try std.testing.expect(user.hasKeyPermission("data:x", "read"));

    // Keys not matching pattern (second char is 't')
    try std.testing.expect(!user.hasKeyPermission("data:t123", "read"));
    try std.testing.expect(!user.hasKeyPermission("data:temp", "write"));
}

test "User: hasKeyPermission with common Redis key patterns" {
    const allocator = std.testing.allocator;

    var allowed_patterns = std.ArrayList([]const u8){};
    try allowed_patterns.append(allocator, try allocator.dupe(u8, "user:*"));

    var read_only_patterns = std.ArrayList([]const u8){};
    try read_only_patterns.append(allocator, try allocator.dupe(u8, "session:*"));

    var write_only_patterns = std.ArrayList([]const u8){};
    try write_only_patterns.append(allocator, try allocator.dupe(u8, "cache:*:data"));

    var user = User{
        .username = try allocator.dupe(u8, "test"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = false,
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .all_keys_allowed = false,
        .allowed_key_patterns = allowed_patterns,
        .read_only_key_patterns = read_only_patterns,
        .write_only_key_patterns = write_only_patterns,
    };
    defer user.deinit(allocator);

    // user:* pattern - read/write allowed
    try std.testing.expect(user.hasKeyPermission("user:1", "read"));
    try std.testing.expect(user.hasKeyPermission("user:1", "write"));
    try std.testing.expect(user.hasKeyPermission("user:1000", "read"));
    try std.testing.expect(user.hasKeyPermission("user:1000", "write"));

    // session:* pattern - read-only
    try std.testing.expect(user.hasKeyPermission("session:abc123", "read"));
    try std.testing.expect(!user.hasKeyPermission("session:abc123", "write"));
    try std.testing.expect(user.hasKeyPermission("session:xyz", "read"));
    try std.testing.expect(!user.hasKeyPermission("session:xyz", "write"));

    // cache:*:data pattern - write-only
    try std.testing.expect(!user.hasKeyPermission("cache:temp:data", "read"));
    try std.testing.expect(user.hasKeyPermission("cache:temp:data", "write"));
    try std.testing.expect(!user.hasKeyPermission("cache:123:data", "read"));
    try std.testing.expect(user.hasKeyPermission("cache:123:data", "write"));

    // No matching pattern - denied
    try std.testing.expect(!user.hasKeyPermission("admin:config", "read"));
    try std.testing.expect(!user.hasKeyPermission("admin:config", "write"));
}

test "ACLStore: createOrUpdateUser creates new user with all permissions" {
    const allocator = std.testing.allocator;

    var store = try ACLStore.init(allocator);
    defer store.deinit();

    // Create permission structures
    var allowed_cmds = std.StringHashMap(void).init(allocator);
    try allowed_cmds.put(try allocator.dupe(u8, "GET"), {});
    try allowed_cmds.put(try allocator.dupe(u8, "SET"), {});

    const denied_cmds = std.StringHashMap(void).init(allocator);

    const allowed_cats = std.AutoHashMap(CommandCategory, void).init(allocator);
    const denied_cats = std.AutoHashMap(CommandCategory, void).init(allocator);

    var allowed_keys = std.ArrayList([]const u8){};
    try allowed_keys.append(allocator, try allocator.dupe(u8, "user:*"));

    const read_only_keys = std.ArrayList([]const u8){};
    const write_only_keys = std.ArrayList([]const u8){};

    // Create user
    try store.createOrUpdateUser(
        "testuser",
        true,
        "password123",
        false,
        allowed_cmds,
        denied_cmds,
        allowed_cats,
        denied_cats,
        false,
        allowed_keys,
        read_only_keys,
        write_only_keys,
    );

    // Verify user was created
    const user = store.getUser("testuser");
    try std.testing.expect(user != null);
    try std.testing.expectEqualStrings("testuser", user.?.username);
    try std.testing.expect(user.?.enabled);
    try std.testing.expectEqualStrings("password123", user.?.password.?);
    try std.testing.expect(!user.?.all_commands_allowed);
    try std.testing.expectEqual(@as(usize, 2), user.?.allowed_commands.count());
    try std.testing.expect(user.?.allowed_commands.contains("GET"));
    try std.testing.expect(user.?.allowed_commands.contains("SET"));
    try std.testing.expect(!user.?.all_keys_allowed);
    try std.testing.expectEqual(@as(usize, 1), user.?.allowed_key_patterns.items.len);
}

test "ACLStore: createOrUpdateUser updates existing user" {
    const allocator = std.testing.allocator;

    var store = try ACLStore.init(allocator);
    defer store.deinit();

    // Create initial user
    var allowed_cmds1 = std.StringHashMap(void).init(allocator);
    try allowed_cmds1.put(try allocator.dupe(u8, "GET"), {});
    const denied_cmds1 = std.StringHashMap(void).init(allocator);
    const allowed_cats1 = std.AutoHashMap(CommandCategory, void).init(allocator);
    const denied_cats1 = std.AutoHashMap(CommandCategory, void).init(allocator);
    const allowed_keys1 = std.ArrayList([]const u8){};
    const read_only_keys1 = std.ArrayList([]const u8){};
    const write_only_keys1 = std.ArrayList([]const u8){};

    try store.createOrUpdateUser(
        "testuser",
        true,
        "oldpass",
        false,
        allowed_cmds1,
        denied_cmds1,
        allowed_cats1,
        denied_cats1,
        false,
        allowed_keys1,
        read_only_keys1,
        write_only_keys1,
    );

    // Update user with new permissions
    var allowed_cmds2 = std.StringHashMap(void).init(allocator);
    try allowed_cmds2.put(try allocator.dupe(u8, "SET"), {});
    try allowed_cmds2.put(try allocator.dupe(u8, "DEL"), {});
    const denied_cmds2 = std.StringHashMap(void).init(allocator);
    const allowed_cats2 = std.AutoHashMap(CommandCategory, void).init(allocator);
    const denied_cats2 = std.AutoHashMap(CommandCategory, void).init(allocator);
    var allowed_keys2 = std.ArrayList([]const u8){};
    try allowed_keys2.append(allocator, try allocator.dupe(u8, "cache:*"));
    const read_only_keys2 = std.ArrayList([]const u8){};
    const write_only_keys2 = std.ArrayList([]const u8){};

    try store.createOrUpdateUser(
        "testuser",
        false,
        "newpass",
        false,
        allowed_cmds2,
        denied_cmds2,
        allowed_cats2,
        denied_cats2,
        false,
        allowed_keys2,
        read_only_keys2,
        write_only_keys2,
    );

    // Verify user was updated (not duplicated)
    const user = store.getUser("testuser");
    try std.testing.expect(user != null);
    try std.testing.expect(!user.?.enabled); // Changed from true to false
    try std.testing.expectEqualStrings("newpass", user.?.password.?); // Changed password
    try std.testing.expectEqual(@as(usize, 2), user.?.allowed_commands.count());
    try std.testing.expect(user.?.allowed_commands.contains("SET"));
    try std.testing.expect(user.?.allowed_commands.contains("DEL"));
    try std.testing.expect(!user.?.allowed_commands.contains("GET")); // Old permission removed
    try std.testing.expectEqual(@as(usize, 1), user.?.allowed_key_patterns.items.len);
}
