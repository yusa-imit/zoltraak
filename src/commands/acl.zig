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
        } else if (std.mem.startsWith(u8, rule, "~") or std.mem.startsWith(u8, rule, "+~") or std.mem.startsWith(u8, rule, "-~")) {
            // Key pattern rules - not yet implemented, skip silently
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

    // Stub: we can't actually store without access to ACLStore
    // For now, just return OK
    // TODO: Wire this into actual ACLStore

    // Clean up allocated permission sets
    {
        var iter = perm_result.allowed_commands.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
    }
    {
        var iter = perm_result.denied_commands.keyIterator();
        while (iter.next()) |key| {
            allocator.free(key.*);
        }
    }
    perm_result.allowed_categories.deinit();
    perm_result.denied_categories.deinit();

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
