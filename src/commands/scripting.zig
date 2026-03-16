const std = @import("std");
const Allocator = std.mem.Allocator;
const Storage = @import("../storage/memory.zig").Storage;
const ScriptStore = @import("../storage/scripting.zig").ScriptStore;
const writer_mod = @import("../protocol/writer.zig");
const Writer = writer_mod.Writer;
const LuaEngine = @import("../scripting/lua_engine.zig").LuaEngine;

/// EVAL script numkeys key [key ...] arg [arg ...]
/// Execute a Lua script server side
pub fn cmdEval(
    allocator: Allocator,
    storage: *Storage,
    script_store: *ScriptStore,
    args: []const []const u8,
    resp_version: u8,
) ![]const u8 {
    _ = storage; // TODO: Wire redis.call/pcall to storage operations
    _ = resp_version;

    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'eval' command");
    }

    const script = args[0];
    const numkeys_str = args[1];

    // Parse numkeys
    const numkeys = std.fmt.parseInt(usize, numkeys_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    // Validate argument count
    if (args.len < 2 + numkeys) {
        return w.writeError("ERR Number of keys can't be greater than number of args");
    }

    // Extract KEYS and ARGV
    const keys_start = 2;
    const keys_end = 2 + numkeys;
    const argv_start = keys_end;

    const keys = args[keys_start..keys_end];
    const argv = args[argv_start..];

    // Cache the script
    const sha1 = try script_store.loadScript(script);
    defer allocator.free(sha1);

    // Create Lua engine and execute script
    var lua_engine = try LuaEngine.init(allocator, null);
    defer lua_engine.deinit();

    const result_str = try lua_engine.eval(script, numkeys, keys, argv);
    defer allocator.free(result_str);

    // Check if result is an error
    if (std.mem.startsWith(u8, result_str, "ERR ")) {
        return w.writeError(result_str);
    }

    // Return the result as a bulk string
    return w.writeBulkString(result_str);
}

/// EVALSHA sha1 numkeys key [key ...] arg [arg ...]
/// Execute a Lua script server side by SHA1 digest
pub fn cmdEvalSHA(
    allocator: Allocator,
    storage: *Storage,
    script_store: *ScriptStore,
    args: []const []const u8,
    resp_version: u8,
) ![]const u8 {
    _ = storage; // TODO: Wire redis.call/pcall to storage operations
    _ = resp_version;

    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'evalsha' command");
    }

    const sha1 = args[0];
    const numkeys_str = args[1];

    // Validate SHA1 format (40 hex characters)
    if (sha1.len != 40) {
        return w.writeError("ERR Invalid SHA1 digest");
    }
    for (sha1) |c| {
        if (!std.ascii.isHex(c)) {
            return w.writeError("ERR Invalid SHA1 digest");
        }
    }

    // Check if script exists
    const script = script_store.getScript(sha1) orelse {
        return w.writeError("NOSCRIPT No matching script. Please use EVAL.");
    };

    // Parse numkeys
    const numkeys = std.fmt.parseInt(usize, numkeys_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    // Validate argument count
    if (args.len < 2 + numkeys) {
        return w.writeError("ERR Number of keys can't be greater than number of args");
    }

    // Extract KEYS and ARGV
    const keys_start = 2;
    const keys_end = 2 + numkeys;
    const argv_start = keys_end;

    const keys = args[keys_start..keys_end];
    const argv = args[argv_start..];

    // Create Lua engine and execute script
    var lua_engine = try LuaEngine.init(allocator, null);
    defer lua_engine.deinit();

    const result_str = try lua_engine.eval(script, numkeys, keys, argv);
    defer allocator.free(result_str);

    // Check if result is an error
    if (std.mem.startsWith(u8, result_str, "ERR ")) {
        return w.writeError(result_str);
    }

    // Return the result as a bulk string
    return w.writeBulkString(result_str);
}

/// SCRIPT LOAD script
/// Load a script into the script cache
pub fn cmdScriptLoad(
    allocator: Allocator,
    script_store: *ScriptStore,
    args: []const []const u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 1) {
        return w.writeError("ERR wrong number of arguments for 'script|load' command");
    }

    const script = args[0];
    const sha1 = try script_store.loadScript(script);
    defer allocator.free(sha1);

    return w.writeBulkString(sha1);
}

/// SCRIPT EXISTS sha1 [sha1 ...]
/// Check if scripts exist in the script cache
pub fn cmdScriptExists(
    allocator: Allocator,
    script_store: *ScriptStore,
    args: []const []const u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len == 0) {
        return w.writeError("ERR wrong number of arguments for 'script|exists' command");
    }

    var result = std.ArrayList(u8){};
    const array_writer = result.writer(allocator);

    // Write array header
    try array_writer.print("*{d}\r\n", .{args.len});

    // Check each SHA1
    for (args) |sha1| {
        const exists = script_store.exists(sha1);
        const value: i64 = if (exists) 1 else 0;
        try array_writer.print(":{d}\r\n", .{value});
    }

    return result.toOwnedSlice(allocator);
}

/// SCRIPT FLUSH [ASYNC|SYNC]
/// Remove all scripts from the script cache
pub fn cmdScriptFlush(
    allocator: Allocator,
    script_store: *ScriptStore,
    args: []const []const u8,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Validate optional ASYNC/SYNC argument
    if (args.len > 1) {
        return w.writeError("ERR wrong number of arguments for 'script|flush' command");
    }

    if (args.len == 1) {
        const mode = args[0];
        if (!std.mem.eql(u8, mode, "ASYNC") and !std.mem.eql(u8, mode, "SYNC") and
            !std.mem.eql(u8, mode, "async") and !std.mem.eql(u8, mode, "sync"))
        {
            return w.writeError("ERR syntax error");
        }
    }

    script_store.flush();
    return w.writeSimpleString("OK");
}

/// SCRIPT HELP
/// Show help for SCRIPT command
pub fn cmdScriptHelp(allocator: Allocator) ![]const u8 {
    const help_lines = [_][]const u8{
        "SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "LOAD <script>",
        "    Load a script into the scripts cache without executing it.",
        "EXISTS <sha1> [<sha1> ...]",
        "    Check existence of scripts in the script cache.",
        "FLUSH [ASYNC|SYNC]",
        "    Remove all scripts from the script cache.",
        "HELP",
        "    Print this help.",
    };

    var result = std.ArrayList(u8){};
    const array_writer = result.writer(allocator);

    try array_writer.print("*{d}\r\n", .{help_lines.len});
    for (help_lines) |line| {
        try array_writer.print("${d}\r\n{s}\r\n", .{ line.len, line });
    }

    return result.toOwnedSlice(allocator);
}

test "cmdScriptLoad" {
    const allocator = std.testing.allocator;
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    const script = "return redis.call('get', KEYS[1])";
    const args = [_][]const u8{script};

    const response = try cmdScriptLoad(allocator, &script_store, &args);
    defer allocator.free(response);

    // Should return a bulk string with SHA1 (40 hex chars)
    try std.testing.expect(std.mem.startsWith(u8, response, "$40\r\n"));
}

test "cmdScriptExists" {
    const allocator = std.testing.allocator;
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    const script = "return 42";
    const sha1 = try script_store.loadScript(script);
    defer allocator.free(sha1);

    const args = [_][]const u8{ sha1, "nonexistent" };
    const response = try cmdScriptExists(allocator, &script_store, &args);
    defer allocator.free(response);

    // Should return array with [1, 0]
    try std.testing.expect(std.mem.startsWith(u8, response, "*2\r\n:1\r\n:0\r\n"));
}

test "cmdScriptFlush" {
    const allocator = std.testing.allocator;
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    const script = "return ARGV[1]";
    const sha1 = try script_store.loadScript(script);
    defer allocator.free(sha1);

    try std.testing.expect(script_store.exists(sha1));

    const args = [_][]const u8{};
    const response = try cmdScriptFlush(allocator, &script_store, &args);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("+OK\r\n", response);
    try std.testing.expect(!script_store.exists(sha1));
}

test "cmdEval basic" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    const script = "return 42";
    const args = [_][]const u8{ script, "0" };

    const response = try cmdEval(allocator, storage, &script_store, &args, 2);
    defer allocator.free(response);

    // Real Lua engine returns "42"
    try std.testing.expectEqualStrings("$2\r\n42\r\n", response);
}

test "cmdEval with KEYS" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    const script = "return KEYS[1]";
    const args = [_][]const u8{ script, "1", "mykey" };

    const response = try cmdEval(allocator, storage, &script_store, &args, 2);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("$5\r\nmykey\r\n", response);
}

test "cmdEval with ARGV" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    const script = "return ARGV[1] .. ' ' .. ARGV[2]";
    const args = [_][]const u8{ script, "0", "hello", "world" };

    const response = try cmdEval(allocator, storage, &script_store, &args, 2);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("$11\r\nhello world\r\n", response);
}

test "cmdEvalSHA nonexistent script" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    const fake_sha1 = "0123456789abcdef0123456789abcdef01234567";
    const args = [_][]const u8{ fake_sha1, "0" };

    const response = try cmdEvalSHA(allocator, storage, &script_store, &args, 2);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-NOSCRIPT"));
}
