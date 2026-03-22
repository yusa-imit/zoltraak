const std = @import("std");
const Allocator = std.mem.Allocator;
const Writer = @import("../protocol/writer.zig").Writer;
const RespValue = @import("../protocol/parser.zig").RespValue;
const CommandRegistry = @import("command_registry.zig");
const ACLStorage = @import("../storage/acl.zig");
const CommandCategory = CommandRegistry.CommandCategory;

/// ACL WHOAMI - Returns current connection username
pub fn cmdACLWhoami(
    allocator: Allocator,
    _: []const RespValue,
) ![]const u8 {
    // Stub: always return "default" for now
    var w = Writer.init(allocator);
    defer w.deinit();
    return w.writeBulkString("default");
}

/// ACL LIST - List all ACL rules
pub fn cmdACLList(
    allocator: Allocator,
    _: []const RespValue,
) ![]const u8 {
    // Stub: return basic default user rule
    const rules = [_][]const u8{
        "user default on nopass ~* +@all",
    };

    // Build RESP array manually
    var buffer = std.ArrayList(u8){};
    errdefer buffer.deinit(allocator);

    try buffer.append(allocator, '*');
    try std.fmt.format(buffer.writer(allocator), "{d}", .{rules.len});
    try buffer.appendSlice(allocator, "\r\n");

    for (rules) |rule| {
        try buffer.append(allocator, '$');
        try std.fmt.format(buffer.writer(allocator), "{d}", .{rule.len});
        try buffer.appendSlice(allocator, "\r\n");
        try buffer.appendSlice(allocator, rule);
        try buffer.appendSlice(allocator, "\r\n");
    }

    return buffer.toOwnedSlice(allocator);
}

/// ACL USERS - List all usernames
pub fn cmdACLUsers(
    allocator: Allocator,
    _: []const RespValue,
) ![]const u8 {
    // Stub: only default user exists
    const users = [_][]const u8{"default"};

    var buffer = std.ArrayList(u8){};
    errdefer buffer.deinit(allocator);

    try buffer.append(allocator, '*');
    try std.fmt.format(buffer.writer(allocator), "{d}", .{users.len});
    try buffer.appendSlice(allocator, "\r\n");

    for (users) |user| {
        try buffer.append(allocator, '$');
        try std.fmt.format(buffer.writer(allocator), "{d}", .{user.len});
        try buffer.appendSlice(allocator, "\r\n");
        try buffer.appendSlice(allocator, user);
        try buffer.appendSlice(allocator, "\r\n");
    }

    return buffer.toOwnedSlice(allocator);
}

/// ACL GETUSER - Get user details
pub fn cmdACLGetuser(
    allocator: Allocator,
    array: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (array.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'acl|getuser' command");
    }

    const username = switch (array[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid username"),
    };

    // Stub: only support "default" user
    if (!std.mem.eql(u8, username, "default")) {
        return w.writeNull();
    }

    // Return user details in simplified flat array format
    const details = [_][]const u8{
        "flags",
        "on",
        "allkeys",
        "allcommands",
        "nopass",
        "passwords",
        "commands",
        "+@all",
        "keys",
        "~*",
    };

    var buffer = std.ArrayList(u8){};
    errdefer buffer.deinit(allocator);

    try buffer.append(allocator, '*');
    try std.fmt.format(buffer.writer(allocator), "{d}", .{details.len});
    try buffer.appendSlice(allocator, "\r\n");

    for (details) |detail| {
        try buffer.append(allocator, '$');
        try std.fmt.format(buffer.writer(allocator), "{d}", .{detail.len});
        try buffer.appendSlice(allocator, "\r\n");
        try buffer.appendSlice(allocator, detail);
        try buffer.appendSlice(allocator, "\r\n");
    }

    return buffer.toOwnedSlice(allocator);
}

/// Parse ACL key pattern rules (~pattern, %R~pattern, %W~pattern, allkeys, resetkeys)
/// Returns pattern lists for key access control
pub fn parseKeyPatternRules(
    allocator: Allocator,
    rules: []const []const u8,
) error{OutOfMemory}!struct {
    all_keys_allowed: bool,
    allowed_key_patterns: std.ArrayList([]const u8),
    read_only_key_patterns: std.ArrayList([]const u8),
    write_only_key_patterns: std.ArrayList([]const u8),
} {
    var all_keys_allowed = false;
    var allowed_key_patterns = std.ArrayList([]const u8){};
    errdefer {
        for (allowed_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        allowed_key_patterns.deinit(allocator);
    }

    var read_only_key_patterns = std.ArrayList([]const u8){};
    errdefer {
        for (read_only_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        read_only_key_patterns.deinit(allocator);
    }

    var write_only_key_patterns = std.ArrayList([]const u8){};
    errdefer {
        for (write_only_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        write_only_key_patterns.deinit(allocator);
    }

    for (rules) |rule| {
        if (std.mem.eql(u8, rule, "allkeys")) {
            // Set all_keys_allowed flag and clear all patterns
            all_keys_allowed = true;
            for (allowed_key_patterns.items) |pattern| {
                allocator.free(pattern);
            }
            allowed_key_patterns.clearRetainingCapacity();
            for (read_only_key_patterns.items) |pattern| {
                allocator.free(pattern);
            }
            read_only_key_patterns.clearRetainingCapacity();
            for (write_only_key_patterns.items) |pattern| {
                allocator.free(pattern);
            }
            write_only_key_patterns.clearRetainingCapacity();
        } else if (std.mem.eql(u8, rule, "resetkeys")) {
            // Reset all_keys_allowed and clear all patterns
            all_keys_allowed = false;
            for (allowed_key_patterns.items) |pattern| {
                allocator.free(pattern);
            }
            allowed_key_patterns.clearRetainingCapacity();
            for (read_only_key_patterns.items) |pattern| {
                allocator.free(pattern);
            }
            read_only_key_patterns.clearRetainingCapacity();
            for (write_only_key_patterns.items) |pattern| {
                allocator.free(pattern);
            }
            write_only_key_patterns.clearRetainingCapacity();
        } else if (std.mem.startsWith(u8, rule, "%R~")) {
            // Read-only pattern: %R~pattern
            const pattern = rule[3..];
            const pattern_copy = try allocator.dupe(u8, pattern);
            errdefer allocator.free(pattern_copy);
            try read_only_key_patterns.append(allocator, pattern_copy);
        } else if (std.mem.startsWith(u8, rule, "%W~")) {
            // Write-only pattern: %W~pattern
            const pattern = rule[3..];
            const pattern_copy = try allocator.dupe(u8, pattern);
            errdefer allocator.free(pattern_copy);
            try write_only_key_patterns.append(allocator, pattern_copy);
        } else if (std.mem.startsWith(u8, rule, "~")) {
            // Full access pattern: ~pattern
            const pattern = rule[1..];
            const pattern_copy = try allocator.dupe(u8, pattern);
            errdefer allocator.free(pattern_copy);
            try allowed_key_patterns.append(allocator, pattern_copy);
        }
        // Other rules (not key patterns) are ignored
    }

    return .{
        .all_keys_allowed = all_keys_allowed,
        .allowed_key_patterns = allowed_key_patterns,
        .read_only_key_patterns = read_only_key_patterns,
        .write_only_key_patterns = write_only_key_patterns,
    };
}

/// Parse ACL rules for a user (+cmd, -cmd, +@cat, -@cat, etc.)
/// Implements left-to-right precedence: last matching rule wins per command
pub const PermissionChangeError = error{
    InvalidRule,
    InvalidCategory,
    InvalidCommand,
    OutOfMemory,
};

/// Parse ACL rules and return permission sets
/// Rules are processed left-to-right, last rule wins per command
pub fn parsePermissionRules(
    allocator: Allocator,
    rules: []const []const u8,
) PermissionChangeError!struct {
    all_commands_allowed: bool,
    allowed_commands: std.StringHashMap(void),
    denied_commands: std.StringHashMap(void),
    allowed_categories: std.AutoHashMap(CommandCategory, void),
    denied_categories: std.AutoHashMap(CommandCategory, void),
} {
    var allowed_commands = std.StringHashMap(void).init(allocator);
    errdefer {
        var iter = allowed_commands.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
        allowed_commands.deinit();
    }

    var denied_commands = std.StringHashMap(void).init(allocator);
    errdefer {
        var iter = denied_commands.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
        denied_commands.deinit();
    }

    var allowed_categories = std.AutoHashMap(CommandCategory, void).init(allocator);
    errdefer allowed_categories.deinit();

    var denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator);
    errdefer denied_categories.deinit();

    var all_commands_allowed = false;

    for (rules) |rule| {
        if (std.mem.eql(u8, rule, "allcommands") or std.mem.eql(u8, rule, "+@all")) {
            // Allow all commands
            all_commands_allowed = true;
            // Clear denied to match Redis semantics
            denied_commands.clearRetainingCapacity();
            denied_categories.clearRetainingCapacity();
        } else if (std.mem.eql(u8, rule, "nocommands") or std.mem.eql(u8, rule, "-@all")) {
            // Deny all commands (reset to default deny)
            all_commands_allowed = false;
            allowed_commands.clearRetainingCapacity();
            allowed_categories.clearRetainingCapacity();
        } else if (std.mem.startsWith(u8, rule, "+@")) {
            // Allow category
            const cat_name = rule[2..];
            if (stringToCategory(cat_name)) |cat| {
                try allowed_categories.put(cat, {});
                // Remove from denied categories
                _ = denied_categories.remove(cat);
            } else {
                return PermissionChangeError.InvalidCategory;
            }
        } else if (std.mem.startsWith(u8, rule, "-@")) {
            // Deny category
            const cat_name = rule[2..];
            if (stringToCategory(cat_name)) |cat| {
                try denied_categories.put(cat, {});
                // Remove from allowed categories
                _ = allowed_categories.remove(cat);
            } else {
                return PermissionChangeError.InvalidCategory;
            }
        } else if (std.mem.startsWith(u8, rule, "+")) {
            // Allow command
            const cmd_name = rule[1..];
            var buf: [64]u8 = undefined;
            const cmd_upper = std.ascii.upperString(&buf, cmd_name);
            const cmd_copy = try allocator.dupe(u8, cmd_upper);
            errdefer allocator.free(cmd_copy);

            try allowed_commands.put(cmd_copy, {});
            // Remove from denied commands
            if (denied_commands.fetchRemove(cmd_upper)) |kv| {
                allocator.free(kv.key);
            }
        } else if (std.mem.startsWith(u8, rule, "-")) {
            // Deny command
            const cmd_name = rule[1..];
            var buf: [64]u8 = undefined;
            const cmd_upper = std.ascii.upperString(&buf, cmd_name);
            const cmd_copy = try allocator.dupe(u8, cmd_upper);
            errdefer allocator.free(cmd_copy);

            try denied_commands.put(cmd_copy, {});
            // Remove from allowed commands
            if (allowed_commands.fetchRemove(cmd_upper)) |kv| {
                allocator.free(kv.key);
            }
        } else if (std.mem.eql(u8, rule, "on")) {
            // Enable user - handled separately
        } else if (std.mem.eql(u8, rule, "off")) {
            // Disable user - handled separately
        } else if (std.mem.startsWith(u8, rule, ">") or std.mem.startsWith(u8, rule, "<")) {
            // Password - handled separately
        } else if (std.mem.eql(u8, rule, "nopass")) {
            // No password - handled separately
        } else if (std.mem.startsWith(u8, rule, "~")) {
            // Key pattern - handled separately, not yet implemented
        } else {
            return PermissionChangeError.InvalidRule;
        }
    }

    return .{
        .all_commands_allowed = all_commands_allowed,
        .allowed_commands = allowed_commands,
        .denied_commands = denied_commands,
        .allowed_categories = allowed_categories,
        .denied_categories = denied_categories,
    };
}

/// Convert string to CommandCategory
fn stringToCategory(name: []const u8) ?CommandCategory {
    inline for (@typeInfo(CommandCategory).@"enum".fields) |field| {
        if (std.mem.eql(u8, name, field.name)) {
            return @enumFromInt(field.value);
        }
    }
    return null;
}

/// ACL SETUSER - Create or modify user
pub fn cmdACLSetuser(
    allocator: Allocator,
    array: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (array.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'acl|setuser' command");
    }

    const _username = switch (array[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid username"),
    };
    _ = _username; // TODO: Wire into ACLStore

    // Parse all rules (starting from array[2])
    var enabled = true;
    var password: ?[]const u8 = null;
    var permission_rules = std.ArrayList([]const u8){};
    defer permission_rules.deinit(allocator);
    var key_pattern_rules = std.ArrayList([]const u8){};
    defer key_pattern_rules.deinit(allocator);

    for (array[2..]) |arg| {
        const rule = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid rule format"),
        };

        if (std.mem.eql(u8, rule, "on")) {
            enabled = true;
        } else if (std.mem.eql(u8, rule, "off")) {
            enabled = false;
        } else if (std.mem.startsWith(u8, rule, ">")) {
            // Set password (SHA256 hash format, but we'll store plaintext for now)
            password = rule[1..];
        } else if (std.mem.startsWith(u8, rule, "<")) {
            // Remove password? Not standard, skip
        } else if (std.mem.eql(u8, rule, "nopass")) {
            password = null;
        } else if (std.mem.startsWith(u8, rule, "~") or std.mem.startsWith(u8, rule, "%R~") or std.mem.startsWith(u8, rule, "%W~") or std.mem.eql(u8, rule, "allkeys") or std.mem.eql(u8, rule, "resetkeys")) {
            // Key pattern rules
            try key_pattern_rules.append(allocator, rule);
        } else {
            // Permission rule
            try permission_rules.append(allocator, rule);
        }
    }

    // Parse permission rules
    var perm_result = parsePermissionRules(allocator, permission_rules.items) catch |err| {
        return w.writeError(switch (err) {
            PermissionChangeError.InvalidCategory => "ERR invalid category",
            PermissionChangeError.InvalidRule => "ERR invalid rule",
            else => "ERR invalid permissions",
        });
    };
    defer {
        var iter = perm_result.allowed_commands.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
        perm_result.allowed_commands.deinit();

        iter = perm_result.denied_commands.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
        perm_result.denied_commands.deinit();

        perm_result.allowed_categories.deinit();
        perm_result.denied_categories.deinit();
    }

    // Parse key pattern rules
    var key_result = try parseKeyPatternRules(allocator, key_pattern_rules.items);
    defer {
        for (key_result.allowed_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        key_result.allowed_key_patterns.deinit(allocator);

        for (key_result.read_only_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        key_result.read_only_key_patterns.deinit(allocator);

        for (key_result.write_only_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        key_result.write_only_key_patterns.deinit(allocator);
    }

    // Stub: we can't actually store without access to ACLStore
    // For now, just return OK after validating rules parse correctly
    // TODO: Wire this into actual ACLStore
    // enabled and password variables are parsed but not yet used

    return w.writeSimpleString("OK");
}

/// ACL DELUSER - Delete user
pub fn cmdACLDeluser(
    allocator: Allocator,
    array: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (array.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'acl|deluser' command");
    }

    const username = switch (array[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid username"),
    };

    // Cannot delete default user
    if (std.mem.eql(u8, username, "default")) {
        return w.writeError("ERR The 'default' user cannot be removed");
    }

    // Stub: pretend user doesn't exist
    return w.writeInteger(0);
}

/// ACL CAT - List command categories
pub fn cmdACLCat(
    allocator: Allocator,
    array: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (array.len == 1) {
        // List all categories
        const categories = [_][]const u8{
            "keyspace",
            "read",
            "write",
            "set",
            "sortedset",
            "list",
            "hash",
            "string",
            "bitmap",
            "hyperloglog",
            "geo",
            "stream",
            "pubsub",
            "admin",
            "fast",
            "slow",
            "blocking",
            "dangerous",
            "connection",
            "transaction",
            "scripting",
        };

        var buffer = std.ArrayList(u8){};
        errdefer buffer.deinit(allocator);

        try buffer.append(allocator, '*');
        try std.fmt.format(buffer.writer(allocator), "{d}", .{categories.len});
        try buffer.appendSlice(allocator, "\r\n");

        for (categories) |cat| {
            try buffer.append(allocator, '$');
            try std.fmt.format(buffer.writer(allocator), "{d}", .{cat.len});
            try buffer.appendSlice(allocator, "\r\n");
            try buffer.appendSlice(allocator, cat);
            try buffer.appendSlice(allocator, "\r\n");
        }

        return buffer.toOwnedSlice(allocator);
    } else {
        // List commands in category (stub: return empty array)
        var buffer = std.ArrayList(u8){};
        errdefer buffer.deinit(allocator);

        try buffer.appendSlice(allocator, "*0\r\n");
        return buffer.toOwnedSlice(allocator);
    }
}

/// ACL HELP - Show ACL command help
pub fn cmdACLHelp(
    allocator: Allocator,
    _: []const RespValue,
) ![]const u8 {
    const help_lines = [_][]const u8{
        "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "CAT [<category>]",
        "    List all commands that belong to <category>, or all command categories",
        "    when no category is specified.",
        "DELUSER <username> [<username> ...]",
        "    Delete a list of users.",
        "GETUSER <username>",
        "    Get the user's details.",
        "HELP",
        "    Prints this help.",
        "LIST",
        "    Show users details in config file format.",
        "SETUSER <username> <attribute> [<attribute> ...]",
        "    Create or modify a user with the specified attributes.",
        "USERS",
        "    List all the registered usernames.",
        "WHOAMI",
        "    Return the current connection username.",
    };

    var buffer = std.ArrayList(u8){};
    errdefer buffer.deinit(allocator);

    try buffer.append(allocator, '*');
    try std.fmt.format(buffer.writer(allocator), "{d}", .{help_lines.len});
    try buffer.appendSlice(allocator, "\r\n");

    for (help_lines) |line| {
        try buffer.append(allocator, '$');
        try std.fmt.format(buffer.writer(allocator), "{d}", .{line.len});
        try buffer.appendSlice(allocator, "\r\n");
        try buffer.appendSlice(allocator, line);
        try buffer.appendSlice(allocator, "\r\n");
    }

    return buffer.toOwnedSlice(allocator);
}

// Unit tests
test "ACL WHOAMI returns default" {
    const allocator = std.testing.allocator;

    const args = [_]RespValue{
        RespValue{ .bulk_string = "ACL" },
        RespValue{ .bulk_string = "WHOAMI" },
    };

    const result = try cmdACLWhoami(allocator, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "$7\r\ndefault\r\n") != null);
}

test "ACL LIST returns users" {
    const allocator = std.testing.allocator;

    const args = [_]RespValue{
        RespValue{ .bulk_string = "ACL" },
        RespValue{ .bulk_string = "LIST" },
    };

    const result = try cmdACLList(allocator, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "*1\r\n") != null); // Array of 1
}

test "ACL USERS returns usernames" {
    const allocator = std.testing.allocator;

    const args = [_]RespValue{
        RespValue{ .bulk_string = "ACL" },
        RespValue{ .bulk_string = "USERS" },
    };

    const result = try cmdACLUsers(allocator, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "default") != null);
}

test "ACL SETUSER creates user" {
    const allocator = std.testing.allocator;

    const args = [_]RespValue{
        RespValue{ .bulk_string = "ACL" },
        RespValue{ .bulk_string = "SETUSER" },
        RespValue{ .bulk_string = "testuser" },
    };

    const result = try cmdACLSetuser(allocator, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "+OK\r\n") != null);
}

test "ACL DELUSER cannot delete default" {
    const allocator = std.testing.allocator;

    const args = [_]RespValue{
        RespValue{ .bulk_string = "ACL" },
        RespValue{ .bulk_string = "DELUSER" },
        RespValue{ .bulk_string = "default" },
    };

    const result = try cmdACLDeluser(allocator, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR") != null);
}

test "ACL CAT lists categories" {
    const allocator = std.testing.allocator;

    const args = [_]RespValue{
        RespValue{ .bulk_string = "ACL" },
        RespValue{ .bulk_string = "CAT" },
    };

    const result = try cmdACLCat(allocator, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "keyspace") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "string") != null);
}

test "parsePermissionRules handles +@fast" {
    const allocator = std.testing.allocator;

    const rules = [_][]const u8{"+@fast"};
    const result = try parsePermissionRules(allocator, &rules);
    defer {
        var iter = result.allowed_commands.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
        result.allowed_commands.deinit();

        iter = result.denied_commands.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
        result.denied_commands.deinit();

        result.allowed_categories.deinit();
        result.denied_categories.deinit();
    }

    try std.testing.expect(!result.all_commands_allowed);
    try std.testing.expect(result.allowed_categories.contains(CommandCategory.fast));
}

test "parsePermissionRules handles +GET -SET" {
    const allocator = std.testing.allocator;

    const rules = [_][]const u8{ "+GET", "-SET" };
    const result = try parsePermissionRules(allocator, &rules);
    defer {
        var iter = result.allowed_commands.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
        result.allowed_commands.deinit();

        iter = result.denied_commands.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
        result.denied_commands.deinit();

        result.allowed_categories.deinit();
        result.denied_categories.deinit();
    }

    try std.testing.expect(result.allowed_commands.contains("GET"));
    try std.testing.expect(result.denied_commands.contains("SET"));
}

test "parsePermissionRules handles allcommands" {
    const allocator = std.testing.allocator;

    const rules = [_][]const u8{"allcommands"};
    const result = try parsePermissionRules(allocator, &rules);
    defer {
        var iter = result.allowed_commands.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
        result.allowed_commands.deinit();

        iter = result.denied_commands.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
        result.denied_commands.deinit();

        result.allowed_categories.deinit();
        result.denied_categories.deinit();
    }

    try std.testing.expect(result.all_commands_allowed);
}

test "parsePermissionRules handles nocommands" {
    const allocator = std.testing.allocator;

    const rules = [_][]const u8{ "allcommands", "nocommands" };
    const result = try parsePermissionRules(allocator, &rules);
    defer {
        var iter = result.allowed_commands.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
        result.allowed_commands.deinit();

        iter = result.denied_commands.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
        result.denied_commands.deinit();

        result.allowed_categories.deinit();
        result.denied_categories.deinit();
    }

    try std.testing.expect(!result.all_commands_allowed);
}

test "parseKeyPatternRules parses ~pattern for full access" {
    const allocator = std.testing.allocator;

    const rules = [_][]const u8{ "~user:*", "~session:*" };
    const result = try parseKeyPatternRules(allocator, &rules);
    defer {
        for (result.allowed_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        result.allowed_key_patterns.deinit(allocator);

        for (result.read_only_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        result.read_only_key_patterns.deinit(allocator);

        for (result.write_only_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        result.write_only_key_patterns.deinit(allocator);
    }

    try std.testing.expect(!result.all_keys_allowed);
    try std.testing.expectEqual(@as(usize, 2), result.allowed_key_patterns.items.len);
    try std.testing.expectEqualStrings("user:*", result.allowed_key_patterns.items[0]);
    try std.testing.expectEqualStrings("session:*", result.allowed_key_patterns.items[1]);
}

test "parseKeyPatternRules parses %R~pattern for read-only" {
    const allocator = std.testing.allocator;

    const rules = [_][]const u8{ "%R~cache:*", "%R~tmp:*" };
    const result = try parseKeyPatternRules(allocator, &rules);
    defer {
        for (result.allowed_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        result.allowed_key_patterns.deinit(allocator);

        for (result.read_only_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        result.read_only_key_patterns.deinit(allocator);

        for (result.write_only_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        result.write_only_key_patterns.deinit(allocator);
    }

    try std.testing.expect(!result.all_keys_allowed);
    try std.testing.expectEqual(@as(usize, 2), result.read_only_key_patterns.items.len);
    try std.testing.expectEqualStrings("cache:*", result.read_only_key_patterns.items[0]);
    try std.testing.expectEqualStrings("tmp:*", result.read_only_key_patterns.items[1]);
}

test "parseKeyPatternRules parses %W~pattern for write-only" {
    const allocator = std.testing.allocator;

    const rules = [_][]const u8{ "%W~log:*", "%W~metric:*" };
    const result = try parseKeyPatternRules(allocator, &rules);
    defer {
        for (result.allowed_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        result.allowed_key_patterns.deinit(allocator);

        for (result.read_only_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        result.read_only_key_patterns.deinit(allocator);

        for (result.write_only_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        result.write_only_key_patterns.deinit(allocator);
    }

    try std.testing.expect(!result.all_keys_allowed);
    try std.testing.expectEqual(@as(usize, 2), result.write_only_key_patterns.items.len);
    try std.testing.expectEqualStrings("log:*", result.write_only_key_patterns.items[0]);
    try std.testing.expectEqualStrings("metric:*", result.write_only_key_patterns.items[1]);
}

test "parseKeyPatternRules handles allkeys flag" {
    const allocator = std.testing.allocator;

    const rules = [_][]const u8{"allkeys"};
    const result = try parseKeyPatternRules(allocator, &rules);
    defer {
        for (result.allowed_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        result.allowed_key_patterns.deinit(allocator);

        for (result.read_only_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        result.read_only_key_patterns.deinit(allocator);

        for (result.write_only_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        result.write_only_key_patterns.deinit(allocator);
    }

    try std.testing.expect(result.all_keys_allowed);
    try std.testing.expectEqual(@as(usize, 0), result.allowed_key_patterns.items.len);
}

test "parseKeyPatternRules handles resetkeys flag" {
    const allocator = std.testing.allocator;

    const rules = [_][]const u8{ "allkeys", "resetkeys" };
    const result = try parseKeyPatternRules(allocator, &rules);
    defer {
        for (result.allowed_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        result.allowed_key_patterns.deinit(allocator);

        for (result.read_only_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        result.read_only_key_patterns.deinit(allocator);

        for (result.write_only_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        result.write_only_key_patterns.deinit(allocator);
    }

    try std.testing.expect(!result.all_keys_allowed);
    try std.testing.expectEqual(@as(usize, 0), result.allowed_key_patterns.items.len);
}

test "parseKeyPatternRules handles mixed pattern types" {
    const allocator = std.testing.allocator;

    const rules = [_][]const u8{ "~user:*", "%R~cache:*", "%W~log:*" };
    const result = try parseKeyPatternRules(allocator, &rules);
    defer {
        for (result.allowed_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        result.allowed_key_patterns.deinit(allocator);

        for (result.read_only_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        result.read_only_key_patterns.deinit(allocator);

        for (result.write_only_key_patterns.items) |pattern| {
            allocator.free(pattern);
        }
        result.write_only_key_patterns.deinit(allocator);
    }

    try std.testing.expect(!result.all_keys_allowed);
    try std.testing.expectEqual(@as(usize, 1), result.allowed_key_patterns.items.len);
    try std.testing.expectEqual(@as(usize, 1), result.read_only_key_patterns.items.len);
    try std.testing.expectEqual(@as(usize, 1), result.write_only_key_patterns.items.len);
}

test "ACL SETUSER parses key pattern rules" {
    const allocator = std.testing.allocator;

    const args = [_]RespValue{
        RespValue{ .bulk_string = "ACL" },
        RespValue{ .bulk_string = "SETUSER" },
        RespValue{ .bulk_string = "testuser" },
        RespValue{ .bulk_string = "~user:*" },
        RespValue{ .bulk_string = "%R~cache:*" },
        RespValue{ .bulk_string = "%W~log:*" },
        RespValue{ .bulk_string = "allkeys" },
        RespValue{ .bulk_string = "resetkeys" },
    };

    const result = try cmdACLSetuser(allocator, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "+OK\r\n") != null);
}

test "parsePermissionRules implements left-to-right precedence" {
    const allocator = std.testing.allocator;

    // +GET then -GET: last rule wins, so GET should be denied
    const rules = [_][]const u8{ "+GET", "-GET" };
    const result = try parsePermissionRules(allocator, &rules);
    defer {
        var iter = result.allowed_commands.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
        result.allowed_commands.deinit();

        iter = result.denied_commands.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
        result.denied_commands.deinit();

        result.allowed_categories.deinit();
        result.denied_categories.deinit();
    }

    try std.testing.expect(!result.allowed_commands.contains("GET"));
    try std.testing.expect(result.denied_commands.contains("GET"));
}

// ────────────────────────────────────────────────────────────────────────────
// Integration Tests for Key Permission Enforcement (Iteration 124)
// ────────────────────────────────────────────────────────────────────────────

test "Key permission enforcement: pattern allows matching keys" {
    const allocator = std.testing.allocator;
    const ACLStore = @import("../storage/acl.zig").ACLStore;

    var acl_store = ACLStore.init(allocator);
    defer acl_store.deinit();

    // Create user with ~user:* pattern (full access to user:* keys)
    var user = try ACLStore.User.init(allocator, "testuser");
    defer user.deinit();

    user.enabled = true;
    user.all_keys_allowed = false;
    try user.allowed_key_patterns.append(allocator, try allocator.dupe(u8, "user:*"));

    // Test that user CAN access user:123
    try std.testing.expect(user.hasKeyPermission("user:123", .read));
    try std.testing.expect(user.hasKeyPermission("user:123", .write));
    try std.testing.expect(user.hasKeyPermission("user:foo", .read));

    // Test that user CANNOT access other keys
    try std.testing.expect(!user.hasKeyPermission("cache:123", .read));
    try std.testing.expect(!user.hasKeyPermission("session:abc", .write));
}

test "Key permission enforcement: read-only pattern restricts writes" {
    const allocator = std.testing.allocator;
    const ACLStore = @import("../storage/acl.zig").ACLStore;

    var user = try ACLStore.User.init(allocator, "readonly_user");
    defer user.deinit();

    user.enabled = true;
    user.all_keys_allowed = false;
    try user.read_only_key_patterns.append(allocator, try allocator.dupe(u8, "cache:*"));

    // Can read cache:* keys
    try std.testing.expect(user.hasKeyPermission("cache:foo", .read));
    try std.testing.expect(user.hasKeyPermission("cache:bar", .read));

    // Cannot write to cache:* keys
    try std.testing.expect(!user.hasKeyPermission("cache:foo", .write));
    try std.testing.expect(!user.hasKeyPermission("cache:bar", .write));
}

test "Key permission enforcement: write-only pattern restricts reads" {
    const allocator = std.testing.allocator;
    const ACLStore = @import("../storage/acl.zig").ACLStore;

    var user = try ACLStore.User.init(allocator, "writeonly_user");
    defer user.deinit();

    user.enabled = true;
    user.all_keys_allowed = false;
    try user.write_only_key_patterns.append(allocator, try allocator.dupe(u8, "log:*"));

    // Can write to log:* keys
    try std.testing.expect(user.hasKeyPermission("log:event", .write));
    try std.testing.expect(user.hasKeyPermission("log:error", .write));

    // Cannot read from log:* keys
    try std.testing.expect(!user.hasKeyPermission("log:event", .read));
    try std.testing.expect(!user.hasKeyPermission("log:error", .read));
}

test "Key permission enforcement: all_keys_allowed grants access to all keys" {
    const allocator = std.testing.allocator;
    const ACLStore = @import("../storage/acl.zig").ACLStore;

    var user = try ACLStore.User.init(allocator, "admin");
    defer user.deinit();

    user.enabled = true;
    user.all_keys_allowed = true;

    // Can access any key with any permission
    try std.testing.expect(user.hasKeyPermission("any:key", .read));
    try std.testing.expect(user.hasKeyPermission("any:key", .write));
    try std.testing.expect(user.hasKeyPermission("another:key", .read));
    try std.testing.expect(user.hasKeyPermission("xyz", .write));
}

test "Key permission enforcement: empty patterns deny all keys" {
    const allocator = std.testing.allocator;
    const ACLStore = @import("../storage/acl.zig").ACLStore;

    var user = try ACLStore.User.init(allocator, "restricted");
    defer user.deinit();

    user.enabled = true;
    user.all_keys_allowed = false;
    // No patterns added — should deny all

    // Cannot access any key
    try std.testing.expect(!user.hasKeyPermission("user:123", .read));
    try std.testing.expect(!user.hasKeyPermission("cache:foo", .write));
    try std.testing.expect(!user.hasKeyPermission("any", .read));
}

test "Key permission enforcement: mixed patterns with precedence" {
    const allocator = std.testing.allocator;
    const ACLStore = @import("../storage/acl.zig").ACLStore;

    var user = try ACLStore.User.init(allocator, "mixed");
    defer user.deinit();

    user.enabled = true;
    user.all_keys_allowed = false;

    // Full access to user:*
    try user.allowed_key_patterns.append(allocator, try allocator.dupe(u8, "user:*"));
    // Read-only for cache:*
    try user.read_only_key_patterns.append(allocator, try allocator.dupe(u8, "cache:*"));
    // Write-only for log:*
    try user.write_only_key_patterns.append(allocator, try allocator.dupe(u8, "log:*"));

    // user:* — full access
    try std.testing.expect(user.hasKeyPermission("user:123", .read));
    try std.testing.expect(user.hasKeyPermission("user:123", .write));

    // cache:* — read only
    try std.testing.expect(user.hasKeyPermission("cache:foo", .read));
    try std.testing.expect(!user.hasKeyPermission("cache:foo", .write));

    // log:* — write only
    try std.testing.expect(!user.hasKeyPermission("log:error", .read));
    try std.testing.expect(user.hasKeyPermission("log:error", .write));

    // other:* — no access
    try std.testing.expect(!user.hasKeyPermission("other:key", .read));
    try std.testing.expect(!user.hasKeyPermission("other:key", .write));
}

test "Key permission enforcement: glob wildcard patterns" {
    const allocator = std.testing.allocator;
    const ACLStore = @import("../storage/acl.zig").ACLStore;

    var user = try ACLStore.User.init(allocator, "glob_user");
    defer user.deinit();

    user.enabled = true;
    user.all_keys_allowed = false;
    try user.allowed_key_patterns.append(allocator, try allocator.dupe(u8, "shard:[0-9]:*"));
    try user.allowed_key_patterns.append(allocator, try allocator.dupe(u8, "env:[abc]:*"));
    try user.allowed_key_patterns.append(allocator, try allocator.dupe(u8, "tmp:?:key"));

    // shard:[0-9]:* — matches single digit
    try std.testing.expect(user.hasKeyPermission("shard:1:data", .read));
    try std.testing.expect(user.hasKeyPermission("shard:9:config", .write));
    try std.testing.expect(!user.hasKeyPermission("shard:10:data", .read)); // two digits

    // env:[abc]:* — matches a, b, or c
    try std.testing.expect(user.hasKeyPermission("env:a:config", .read));
    try std.testing.expect(user.hasKeyPermission("env:b:setting", .write));
    try std.testing.expect(!user.hasKeyPermission("env:d:config", .read)); // d not in [abc]

    // tmp:?:key — matches single character
    try std.testing.expect(user.hasKeyPermission("tmp:x:key", .read));
    try std.testing.expect(user.hasKeyPermission("tmp:1:key", .write));
    try std.testing.expect(!user.hasKeyPermission("tmp:ab:key", .read)); // two chars
}
