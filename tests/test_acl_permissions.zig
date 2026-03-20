/// Integration tests for ACL command permission enforcement
const std = @import("std");
const acl = @import("../src/storage/acl.zig");
const registry = @import("../src/commands/command_registry.zig");
const acl_cmd = @import("../src/commands/acl.zig");

const CommandCategory = registry.CommandCategory;
const User = acl.User;

test "ACL: User with all_commands_allowed=true can execute any command" {
    const allocator = std.testing.allocator;

    var user = User{
        .username = try allocator.dupe(u8, "admin"),
        .password = try allocator.dupe(u8, "secret"),
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
    try std.testing.expect(user.hasCommandPermission("FLUSHDB"));
    try std.testing.expect(user.hasCommandPermission("ANY_COMMAND"));
}

test "ACL: User with all_commands_allowed=false denies all except auth commands" {
    const allocator = std.testing.allocator;

    var user = User{
        .username = try allocator.dupe(u8, "restricted"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = false,
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
    };
    defer user.deinit(allocator);

    try std.testing.expect(!user.hasCommandPermission("GET"));
    try std.testing.expect(!user.hasCommandPermission("SET"));
    try std.testing.expect(user.hasCommandPermission("AUTH")); // Always allowed
    try std.testing.expect(user.hasCommandPermission("PING")); // Always allowed
    try std.testing.expect(user.hasCommandPermission("HELLO")); // Always allowed
}

test "ACL: User can execute specific allowed commands" {
    const allocator = std.testing.allocator;

    var allowed = std.StringHashMap(void).init(allocator);
    try allowed.put(try allocator.dupe(u8, "GET"), {});
    try allowed.put(try allocator.dupe(u8, "MGET"), {});

    var user = User{
        .username = try allocator.dupe(u8, "reader"),
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
    try std.testing.expect(user.hasCommandPermission("MGET"));
    try std.testing.expect(!user.hasCommandPermission("SET")); // Not allowed
    try std.testing.expect(!user.hasCommandPermission("DEL")); // Not allowed
}

test "ACL: Explicit deny takes precedence over all_commands_allowed" {
    const allocator = std.testing.allocator;

    var denied = std.StringHashMap(void).init(allocator);
    try denied.put(try allocator.dupe(u8, "FLUSHDB"), {});

    var user = User{
        .username = try allocator.dupe(u8, "admin_limited"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = true, // Has all commands
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = denied,
        .allowed_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
    };
    defer user.deinit(allocator);

    try std.testing.expect(user.hasCommandPermission("SET"));
    try std.testing.expect(user.hasCommandPermission("GET"));
    try std.testing.expect(!user.hasCommandPermission("FLUSHDB")); // Explicitly denied
}

test "ACL: Category-based permissions work correctly" {
    const allocator = std.testing.allocator;

    var allowed_cats = std.AutoHashMap(CommandCategory, void).init(allocator);
    try allowed_cats.put(CommandCategory.fast, {});

    var user = User{
        .username = try allocator.dupe(u8, "fast_user"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = false,
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = allowed_cats,
        .denied_categories = std.AutoHashMap(CommandCategory, void).init(allocator),
    };
    defer user.deinit(allocator);

    // Fast commands should be allowed
    try std.testing.expect(user.hasCommandPermission("GET"));
    try std.testing.expect(user.hasCommandPermission("INCR"));
    try std.testing.expect(user.hasCommandPermission("PING"));

    // Non-fast commands should be denied
    try std.testing.expect(!user.hasCommandPermission("SORT")); // Not fast
    try std.testing.expect(!user.hasCommandPermission("BLPOP")); // Not fast
}

test "ACL: Category deny takes precedence over category allow" {
    const allocator = std.testing.allocator;

    var allowed_cats = std.AutoHashMap(CommandCategory, void).init(allocator);
    try allowed_cats.put(CommandCategory.string, {});

    var denied_cats = std.AutoHashMap(CommandCategory, void).init(allocator);
    try denied_cats.put(CommandCategory.write, {});

    var user = User{
        .username = try allocator.dupe(u8, "read_only"),
        .password = null,
        .enabled = true,
        .all_commands_allowed = false,
        .allowed_commands = std.StringHashMap(void).init(allocator),
        .denied_commands = std.StringHashMap(void).init(allocator),
        .allowed_categories = allowed_cats,
        .denied_categories = denied_cats,
    };
    defer user.deinit(allocator);

    // GET is string + read, allowed
    try std.testing.expect(user.hasCommandPermission("GET"));

    // SET is string + write, write is denied so it's denied overall
    try std.testing.expect(!user.hasCommandPermission("SET"));

    // APPEND is string + write, write is denied
    try std.testing.expect(!user.hasCommandPermission("APPEND"));
}

test "ACL: Default user has all commands allowed" {
    const allocator = std.testing.allocator;

    var store = try acl.ACLStore.init(allocator);
    defer store.deinit();

    const default_user = store.getUser("default");
    try std.testing.expect(default_user != null);

    // Default user should have all_commands_allowed=true
    try std.testing.expect(default_user.?.all_commands_allowed);
    try std.testing.expect(default_user.?.hasCommandPermission("GET"));
    try std.testing.expect(default_user.?.hasCommandPermission("SET"));
    try std.testing.expect(default_user.?.hasCommandPermission("FLUSHALL"));
}

test "ACL: Rule parsing handles +@fast -SET" {
    const allocator = std.testing.allocator;

    const rules = [_][]const u8{ "+@fast", "-SET" };
    const result = try acl_cmd.parsePermissionRules(allocator, &rules);
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

    try std.testing.expect(result.allowed_categories.contains(CommandCategory.fast));
    try std.testing.expect(result.denied_commands.contains("SET"));
}

test "ACL: Rule parsing handles left-to-right precedence for categories" {
    const allocator = std.testing.allocator;

    // +@fast then -@fast: last wins, so @fast is denied
    const rules = [_][]const u8{ "+@fast", "-@fast" };
    const result = try acl_cmd.parsePermissionRules(allocator, &rules);
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

    try std.testing.expect(!result.allowed_categories.contains(CommandCategory.fast));
    try std.testing.expect(result.denied_categories.contains(CommandCategory.fast));
}

test "ACL: Command category registry has comprehensive command mapping" {
    const allocator = std.testing.allocator;

    // Test that several commands are properly categorized
    const get_cats = try registry.getCategoriesForCommand("GET");
    try std.testing.expect(get_cats.len > 0);

    var found_string = false;
    var found_read = false;
    for (get_cats) |cat| {
        if (cat == .string) found_string = true;
        if (cat == .read) found_read = true;
    }
    try std.testing.expect(found_string and found_read);

    // SET should be in string and write categories
    const set_cats = try registry.getCategoriesForCommand("SET");
    found_string = false;
    var found_write = false;
    for (set_cats) |cat| {
        if (cat == .string) found_string = true;
        if (cat == .write) found_write = true;
    }
    try std.testing.expect(found_string and found_write);

    // INCR should be in fast category
    const incr_cats = try registry.getCategoriesForCommand("INCR");
    var found_fast = false;
    for (incr_cats) |cat| {
        if (cat == .fast) {
            found_fast = true;
            break;
        }
    }
    try std.testing.expect(found_fast);
}
