const std = @import("std");
const Allocator = std.mem.Allocator;
const Writer = @import("../protocol/writer.zig").Writer;
const RespValue = @import("../protocol/parser.zig").RespValue;

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

/// ACL SETUSER - Create or modify user
pub fn cmdACLSetuser(
    allocator: Allocator,
    _: []const RespValue,
) ![]const u8 {
    // Stub: pretend to accept but don't actually store
    var w = Writer.init(allocator);
    defer w.deinit();
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
