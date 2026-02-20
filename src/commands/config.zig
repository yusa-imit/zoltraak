const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");
const config_mod = @import("../storage/config.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;
const Config = config_mod.Config;

/// Execute CONFIG subcommand
/// Caller owns returned memory and must free it
pub fn executeConfigCommand(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const RespValue,
) ![]const u8 {
    if (args.len < 2) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'config' command");
    }

    const subcommand = switch (args[1]) {
        .bulk_string => |s| s,
        else => {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR invalid subcommand format");
        },
    };

    const subcmd_upper = try std.ascii.allocUpperString(allocator, subcommand);
    defer allocator.free(subcmd_upper);

    if (std.mem.eql(u8, subcmd_upper, "GET")) {
        return cmdConfigGet(allocator, storage, args);
    } else if (std.mem.eql(u8, subcmd_upper, "SET")) {
        return cmdConfigSet(allocator, storage, args);
    } else if (std.mem.eql(u8, subcmd_upper, "REWRITE")) {
        return cmdConfigRewrite(allocator, storage, args);
    } else if (std.mem.eql(u8, subcmd_upper, "RESETSTAT")) {
        return cmdConfigResetStat(allocator, storage, args);
    } else if (std.mem.eql(u8, subcmd_upper, "HELP")) {
        return cmdConfigHelp(allocator, args);
    } else {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR unknown CONFIG subcommand");
    }
}

/// CONFIG GET pattern [pattern ...]
/// Returns array of [parameter, value, parameter, value, ...]
fn cmdConfigGet(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const RespValue,
) ![]const u8 {
    if (args.len < 3) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'config get' command");
    }

    var results = std.ArrayList([]const u8){};
    defer {
        for (results.items) |item| {
            allocator.free(item);
        }
        results.deinit(allocator);
    }

    // Process each pattern
    for (args[2..]) |arg| {
        const pattern = switch (arg) {
            .bulk_string => |s| s,
            else => {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR invalid pattern format");
            },
        };

        // Get matching parameter names
        const matches = try storage.config.getMatching(pattern);
        defer {
            for (matches) |match| {
                allocator.free(match);
            }
            allocator.free(matches);
        }

        // For each match, get name and value
        for (matches) |param_name| {
            const value = try storage.config.get(param_name);
            defer if (value) |v| allocator.free(v);

            if (value) |v| {
                // Add parameter name
                const name_dup = try allocator.dupe(u8, param_name);
                errdefer allocator.free(name_dup);
                try results.append(allocator, name_dup);

                // Add parameter value
                const value_dup = try allocator.dupe(u8, v);
                errdefer allocator.free(value_dup);
                try results.append(allocator, value_dup);
            }
        }
    }

    // Build RESP array manually
    var buffer = std.ArrayList(u8){};
    errdefer buffer.deinit(allocator);

    // Write array header: *N\r\n
    try buffer.append(allocator, '*');
    try std.fmt.format(buffer.writer(allocator), "{d}", .{results.items.len});
    try buffer.appendSlice(allocator, "\r\n");

    // Write each bulk string element
    for (results.items) |item| {
        try buffer.append(allocator, '$');
        try std.fmt.format(buffer.writer(allocator), "{d}", .{item.len});
        try buffer.appendSlice(allocator, "\r\n");
        try buffer.appendSlice(allocator, item);
        try buffer.appendSlice(allocator, "\r\n");
    }

    return buffer.toOwnedSlice(allocator);
}

/// CONFIG SET parameter value [parameter value ...]
/// Returns +OK on success, error on failure
fn cmdConfigSet(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const RespValue,
) ![]const u8 {
    if (args.len < 4 or (args.len - 2) % 2 != 0) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'config set' command");
    }

    // Process each parameter/value pair
    var idx: usize = 2;
    while (idx < args.len) : (idx += 2) {
        const param_name = switch (args[idx]) {
            .bulk_string => |s| s,
            else => {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR invalid parameter name format");
            },
        };

        const value = switch (args[idx + 1]) {
            .bulk_string => |s| s,
            else => {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR invalid parameter value format");
            },
        };

        // Attempt to set parameter
        storage.config.set(param_name, value) catch |err| {
            var w = Writer.init(allocator);
            defer w.deinit();

            const err_msg = switch (err) {
                error.UnknownParameter => "ERR Unsupported CONFIG parameter",
                error.ReadOnlyParameter => "ERR Unsupported CONFIG parameter (read-only)",
                error.InvalidValue => "ERR Invalid argument",
                else => "ERR Failed to set parameter",
            };
            return w.writeError(err_msg);
        };
    }

    var w = Writer.init(allocator);
    defer w.deinit();
    return w.writeSimpleString("OK");
}

/// CONFIG REWRITE
/// Writes current configuration to zoltraak.conf file
fn cmdConfigRewrite(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const RespValue,
) ![]const u8 {
    if (args.len != 2) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'config rewrite' command");
    }

    // Get all parameters
    const all_params = try storage.config.getMatching("*");
    defer {
        for (all_params) |param| {
            allocator.free(param);
        }
        allocator.free(all_params);
    }

    // Build config file content
    var content = std.ArrayList(u8){};
    defer content.deinit(allocator);

    const writer = content.writer(allocator);
    try writer.print("# Zoltraak configuration file\n", .{});
    try writer.print("# Auto-generated by CONFIG REWRITE\n\n", .{});

    for (all_params) |param_name| {
        const value = try storage.config.get(param_name);
        defer if (value) |v| allocator.free(v);

        if (value) |v| {
            try writer.print("{s} {s}\n", .{ param_name, v });
        }
    }

    // Write to file atomically
    const config_path = "zoltraak.conf";
    const data = try content.toOwnedSlice(allocator);
    defer allocator.free(data);

    std.fs.cwd().writeFile(.{
        .sub_path = config_path,
        .data = data,
    }) catch |err| {
        const err_msg = try std.fmt.allocPrint(
            allocator,
            "ERR Failed to write config file: {s}",
            .{@errorName(err)},
        );
        defer allocator.free(err_msg);
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError(err_msg);
    };

    var w = Writer.init(allocator);
    defer w.deinit();
    return w.writeSimpleString("OK");
}

/// CONFIG RESETSTAT
/// Resets server statistics (placeholder for future implementation)
fn cmdConfigResetStat(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const RespValue,
) ![]const u8 {
    if (args.len != 2) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'config resetstat' command");
    }

    storage.config.resetStats();

    var w = Writer.init(allocator);
    defer w.deinit();
    return w.writeSimpleString("OK");
}

/// CONFIG HELP
/// Returns help text for CONFIG command
fn cmdConfigHelp(allocator: std.mem.Allocator, args: []const RespValue) ![]const u8 {
    if (args.len != 2) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'config help' command");
    }

    const help_lines = [_][]const u8{
        "CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "GET <pattern>",
        "    Return parameters matching the glob-style <pattern>.",
        "SET <directive> <value>",
        "    Set configuration directives at runtime.",
        "REWRITE",
        "    Rewrite the configuration file with the current configuration.",
        "RESETSTAT",
        "    Reset statistics reported by the INFO command.",
        "HELP",
        "    Print this help.",
    };

    // Build RESP array manually
    var buffer = std.ArrayList(u8){};
    errdefer buffer.deinit(allocator);

    // Write array header: *N\r\n
    try buffer.append(allocator, '*');
    try std.fmt.format(buffer.writer(allocator), "{d}", .{help_lines.len});
    try buffer.appendSlice(allocator, "\r\n");

    // Write each bulk string element
    for (help_lines) |line| {
        try buffer.append(allocator, '$');
        try std.fmt.format(buffer.writer(allocator), "{d}", .{line.len});
        try buffer.appendSlice(allocator, "\r\n");
        try buffer.appendSlice(allocator, line);
        try buffer.appendSlice(allocator, "\r\n");
    }

    return buffer.toOwnedSlice(allocator);
}

// ── Tests ────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "CONFIG GET single parameter" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Build command: CONFIG GET maxmemory
    var args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "maxmemory" },
    };

    const response = try executeConfigCommand(allocator, storage, &args);
    defer allocator.free(response);

    // Response should be array with 2 elements: ["maxmemory", "0"]
    try testing.expect(std.mem.indexOf(u8, response, "maxmemory") != null);
    try testing.expect(std.mem.indexOf(u8, response, "0") != null);
}

test "CONFIG GET with wildcard pattern" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Build command: CONFIG GET maxmemory*
    var args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "maxmemory*" },
    };

    const response = try executeConfigCommand(allocator, storage, &args);
    defer allocator.free(response);

    // Should match maxmemory and maxmemory-policy
    try testing.expect(std.mem.indexOf(u8, response, "maxmemory") != null);
    try testing.expect(std.mem.indexOf(u8, response, "maxmemory-policy") != null);
}

test "CONFIG GET all parameters" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Build command: CONFIG GET *
    var args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "*" },
    };

    const response = try executeConfigCommand(allocator, storage, &args);
    defer allocator.free(response);

    // Should contain multiple parameters
    try testing.expect(std.mem.indexOf(u8, response, "maxmemory") != null);
    try testing.expect(std.mem.indexOf(u8, response, "port") != null);
    try testing.expect(std.mem.indexOf(u8, response, "appendonly") != null);
}

test "CONFIG SET valid parameter" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Build command: CONFIG SET maxmemory 1073741824
    var set_args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "maxmemory" },
        RespValue{ .bulk_string = "1073741824" },
    };

    const set_response = try executeConfigCommand(allocator, storage, &set_args);
    defer allocator.free(set_response);

    try testing.expect(std.mem.indexOf(u8, set_response, "OK") != null);

    // Verify with GET
    var get_args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "maxmemory" },
    };

    const get_response = try executeConfigCommand(allocator, storage, &get_args);
    defer allocator.free(get_response);

    try testing.expect(std.mem.indexOf(u8, get_response, "1073741824") != null);
}

test "CONFIG SET read-only parameter fails" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Build command: CONFIG SET port 8080
    var args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "port" },
        RespValue{ .bulk_string = "8080" },
    };

    const response = try executeConfigCommand(allocator, storage, &args);
    defer allocator.free(response);

    // Should return error
    try testing.expect(std.mem.indexOf(u8, response, "ERR") != null);
    try testing.expect(std.mem.indexOf(u8, response, "read-only") != null);
}

test "CONFIG SET invalid value fails" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Build command: CONFIG SET maxmemory not-a-number
    var args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "maxmemory" },
        RespValue{ .bulk_string = "not-a-number" },
    };

    const response = try executeConfigCommand(allocator, storage, &args);
    defer allocator.free(response);

    // Should return error
    try testing.expect(std.mem.indexOf(u8, response, "ERR") != null);
}

test "CONFIG SET multiple parameters" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Build command: CONFIG SET maxmemory 2048 timeout 60
    var args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "maxmemory" },
        RespValue{ .bulk_string = "2048" },
        RespValue{ .bulk_string = "timeout" },
        RespValue{ .bulk_string = "60" },
    };

    const response = try executeConfigCommand(allocator, storage, &args);
    defer allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "OK") != null);

    // Verify both parameters
    var get_args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "maxmemory" },
        RespValue{ .bulk_string = "timeout" },
    };

    const get_response = try executeConfigCommand(allocator, storage, &get_args);
    defer allocator.free(get_response);

    try testing.expect(std.mem.indexOf(u8, get_response, "2048") != null);
    try testing.expect(std.mem.indexOf(u8, get_response, "60") != null);
}

test "CONFIG RESETSTAT returns OK" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Build command: CONFIG RESETSTAT
    var args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "RESETSTAT" },
    };

    const response = try executeConfigCommand(allocator, storage, &args);
    defer allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "OK") != null);
}

test "CONFIG HELP returns help text" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Build command: CONFIG HELP
    var args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "HELP" },
    };

    const response = try executeConfigCommand(allocator, storage, &args);
    defer allocator.free(response);

    // Should contain help text
    try testing.expect(std.mem.indexOf(u8, response, "GET") != null);
    try testing.expect(std.mem.indexOf(u8, response, "SET") != null);
    try testing.expect(std.mem.indexOf(u8, response, "REWRITE") != null);
    try testing.expect(std.mem.indexOf(u8, response, "RESETSTAT") != null);
}

test "CONFIG with invalid subcommand" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Build command: CONFIG INVALID
    var args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "INVALID" },
    };

    const response = try executeConfigCommand(allocator, storage, &args);
    defer allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "ERR") != null);
    try testing.expect(std.mem.indexOf(u8, response, "unknown") != null);
}

test "CONFIG GET case insensitive" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Build command: CONFIG GET MaxMemory
    var args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "get" }, // lowercase subcommand
        RespValue{ .bulk_string = "MaxMemory" }, // mixed case param
    };

    const response = try executeConfigCommand(allocator, storage, &args);
    defer allocator.free(response);

    try testing.expect(std.mem.indexOf(u8, response, "maxmemory") != null);
}

test "CONFIG SET boolean parameter" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Test different boolean formats
    const test_cases = [_]struct {
        value: []const u8,
        expected: []const u8,
    }{
        .{ .value = "yes", .expected = "yes" },
        .{ .value = "YES", .expected = "yes" },
        .{ .value = "true", .expected = "yes" },
        .{ .value = "1", .expected = "yes" },
        .{ .value = "no", .expected = "no" },
        .{ .value = "NO", .expected = "no" },
        .{ .value = "false", .expected = "no" },
        .{ .value = "0", .expected = "no" },
    };

    for (test_cases) |tc| {
        var set_args = [_]RespValue{
            RespValue{ .bulk_string = "CONFIG" },
            RespValue{ .bulk_string = "SET" },
            RespValue{ .bulk_string = "appendonly" },
            RespValue{ .bulk_string = tc.value },
        };

        const set_response = try executeConfigCommand(allocator, storage, &set_args);
        defer allocator.free(set_response);

        var get_args = [_]RespValue{
            RespValue{ .bulk_string = "CONFIG" },
            RespValue{ .bulk_string = "GET" },
            RespValue{ .bulk_string = "appendonly" },
        };

        const get_response = try executeConfigCommand(allocator, storage, &get_args);
        defer allocator.free(get_response);

        try testing.expect(std.mem.indexOf(u8, get_response, tc.expected) != null);
    }
}

test "CONFIG REWRITE creates config file" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Set some parameters first
    var set_args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "maxmemory" },
        RespValue{ .bulk_string = "4096" },
    };

    const set_response = try executeConfigCommand(allocator, storage, &set_args);
    defer allocator.free(set_response);

    // Build command: CONFIG REWRITE
    var rewrite_args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "REWRITE" },
    };

    const rewrite_response = try executeConfigCommand(allocator, storage, &rewrite_args);
    defer allocator.free(rewrite_response);

    try testing.expect(std.mem.indexOf(u8, rewrite_response, "OK") != null);

    // Verify file was created
    const file = std.fs.cwd().openFile("zoltraak.conf", .{}) catch |err| {
        std.debug.print("Failed to open config file: {}\n", .{err});
        return err;
    };
    defer {
        file.close();
        std.fs.cwd().deleteFile("zoltraak.conf") catch {};
    }

    // Read and verify content
    const content = try file.readToEndAlloc(allocator, 1024 * 1024);
    defer allocator.free(content);

    try testing.expect(std.mem.indexOf(u8, content, "maxmemory") != null);
    try testing.expect(std.mem.indexOf(u8, content, "4096") != null);
}
