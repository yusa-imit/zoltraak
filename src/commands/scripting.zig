const std = @import("std");
const Allocator = std.mem.Allocator;
const Storage = @import("../storage/memory.zig").Storage;
const ScriptStore = @import("../storage/scripting.zig").ScriptStore;
const writer_mod = @import("../protocol/writer.zig");
const Writer = writer_mod.Writer;
const LuaEngine = @import("../scripting/lua_engine.zig").LuaEngine;
const RedisContext = @import("../scripting/redis_api.zig").RedisContext;
const Aof = @import("../storage/aof.zig").Aof;
const PubSub = @import("../storage/pubsub.zig").PubSub;
const TxState = @import("../commands/transactions.zig").TxState;
const ReplicationState = @import("../storage/replication.zig").ReplicationState;
const ClientRegistry = @import("../commands/client.zig").ClientRegistry;

/// EVAL script numkeys key [key ...] arg [arg ...]
/// Execute a Lua script server side
pub fn cmdEval(
    allocator: Allocator,
    storage: *Storage,
    script_store: *ScriptStore,
    args: []const []const u8,
    resp_version: u8,
    aof: ?*Aof,
    ps: *PubSub,
    subscriber_id: u64,
    tx: *TxState,
    repl: ?*ReplicationState,
    my_port: u16,
    replica_stream: ?std.net.Stream,
    replica_idx: ?usize,
    client_registry: *ClientRegistry,
    client_id: u64,
    shutdown_state: ?*@import("../server.zig").ShutdownState,
    databases: []Storage,
    num_databases: u16,
) ![]const u8 {
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

    // Create RedisContext with all required parameters
    var redis_ctx = RedisContext{
        .allocator = allocator,
        .storage = storage,
        .aof = aof,
        .ps = ps,
        .subscriber_id = subscriber_id,
        .tx = tx,
        .repl = repl,
        .my_port = my_port,
        .replica_stream = replica_stream,
        .replica_idx = replica_idx,
        .client_registry = client_registry,
        .client_id = client_id,
        .script_store = script_store,
        .shutdown_state = shutdown_state,
        .databases = databases,
        .num_databases = num_databases,
    };

    // Get lua-time-limit from config
    const timeout_ms = blk: {
        storage.config.mutex.lock();
        defer storage.config.mutex.unlock();
        const timeout_val = storage.config.params.get("lua-time-limit") orelse break :blk 5000; // default 5000ms
        break :blk timeout_val.int;
    };

    // Create Lua engine with redis.call/pcall enabled
    var lua_engine = try LuaEngine.init(allocator, &redis_ctx, timeout_ms);
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
    aof: ?*Aof,
    ps: *PubSub,
    subscriber_id: u64,
    tx: *TxState,
    repl: ?*ReplicationState,
    my_port: u16,
    replica_stream: ?std.net.Stream,
    replica_idx: ?usize,
    client_registry: *ClientRegistry,
    client_id: u64,
    shutdown_state: ?*@import("../server.zig").ShutdownState,
    databases: []Storage,
    num_databases: u16,
) ![]const u8 {
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

    // Create RedisContext with all required parameters
    var redis_ctx = RedisContext{
        .allocator = allocator,
        .storage = storage,
        .aof = aof,
        .ps = ps,
        .subscriber_id = subscriber_id,
        .tx = tx,
        .repl = repl,
        .my_port = my_port,
        .replica_stream = replica_stream,
        .replica_idx = replica_idx,
        .client_registry = client_registry,
        .client_id = client_id,
        .script_store = script_store,
        .shutdown_state = shutdown_state,
        .databases = databases,
        .num_databases = num_databases,
    };

    // Get lua-time-limit from config
    const timeout_ms = blk: {
        storage.config.mutex.lock();
        defer storage.config.mutex.unlock();
        const timeout_val = storage.config.params.get("lua-time-limit") orelse break :blk 5000; // default 5000ms
        break :blk timeout_val.int;
    };

    // Create Lua engine with redis.call/pcall enabled
    var lua_engine = try LuaEngine.init(allocator, &redis_ctx, timeout_ms);
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

/// EVAL_RO script numkeys key [key ...] arg [arg ...]
/// Execute a read-only Lua script server side (Redis 7.0+).
/// Identical to EVAL but blocks write commands within the script.
pub fn cmdEvalRo(
    allocator: Allocator,
    storage: *Storage,
    script_store: *ScriptStore,
    args: []const []const u8,
    resp_version: u8,
    aof: ?*Aof,
    ps: *PubSub,
    subscriber_id: u64,
    tx: *TxState,
    repl: ?*ReplicationState,
    my_port: u16,
    replica_stream: ?std.net.Stream,
    replica_idx: ?usize,
    client_registry: *ClientRegistry,
    client_id: u64,
    shutdown_state: ?*@import("../server.zig").ShutdownState,
    databases: []Storage,
    num_databases: u16,
) ![]const u8 {
    _ = resp_version;

    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'eval_ro' command");
    }

    const script = args[0];
    const numkeys_str = args[1];

    const numkeys = std.fmt.parseInt(usize, numkeys_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    if (args.len < 2 + numkeys) {
        return w.writeError("ERR Number of keys can't be greater than number of args");
    }

    const keys = args[2 .. 2 + numkeys];
    const argv = args[2 + numkeys ..];

    const sha1 = try script_store.loadScript(script);
    defer allocator.free(sha1);

    var redis_ctx = RedisContext{
        .allocator = allocator,
        .storage = storage,
        .aof = aof,
        .ps = ps,
        .subscriber_id = subscriber_id,
        .tx = tx,
        .repl = repl,
        .my_port = my_port,
        .replica_stream = replica_stream,
        .replica_idx = replica_idx,
        .client_registry = client_registry,
        .client_id = client_id,
        .script_store = script_store,
        .shutdown_state = shutdown_state,
        .databases = databases,
        .num_databases = num_databases,
        .read_only = true,
    };

    const timeout_ms = blk: {
        storage.config.mutex.lock();
        defer storage.config.mutex.unlock();
        const timeout_val = storage.config.params.get("lua-time-limit") orelse break :blk 5000;
        break :blk timeout_val.int;
    };

    var lua_engine = try LuaEngine.init(allocator, &redis_ctx, timeout_ms);
    defer lua_engine.deinit();

    const result_str = try lua_engine.eval(script, numkeys, keys, argv);
    defer allocator.free(result_str);

    if (std.mem.startsWith(u8, result_str, "ERR ")) {
        return w.writeError(result_str);
    }

    return w.writeBulkString(result_str);
}

/// EVALSHA_RO sha1 numkeys key [key ...] arg [arg ...]
/// Execute a cached read-only Lua script by SHA1 (Redis 7.0+).
/// Identical to EVALSHA but blocks write commands within the script.
pub fn cmdEvalShaRo(
    allocator: Allocator,
    storage: *Storage,
    script_store: *ScriptStore,
    args: []const []const u8,
    resp_version: u8,
    aof: ?*Aof,
    ps: *PubSub,
    subscriber_id: u64,
    tx: *TxState,
    repl: ?*ReplicationState,
    my_port: u16,
    replica_stream: ?std.net.Stream,
    replica_idx: ?usize,
    client_registry: *ClientRegistry,
    client_id: u64,
    shutdown_state: ?*@import("../server.zig").ShutdownState,
    databases: []Storage,
    num_databases: u16,
) ![]const u8 {
    _ = resp_version;

    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'evalsha_ro' command");
    }

    const sha1 = args[0];
    const numkeys_str = args[1];

    if (sha1.len != 40) {
        return w.writeError("ERR Invalid SHA1 digest");
    }
    for (sha1) |c| {
        if (!std.ascii.isHex(c)) {
            return w.writeError("ERR Invalid SHA1 digest");
        }
    }

    const script = script_store.getScript(sha1) orelse {
        return w.writeError("NOSCRIPT No matching script. Please use EVAL.");
    };

    const numkeys = std.fmt.parseInt(usize, numkeys_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    if (args.len < 2 + numkeys) {
        return w.writeError("ERR Number of keys can't be greater than number of args");
    }

    const keys = args[2 .. 2 + numkeys];
    const argv = args[2 + numkeys ..];

    var redis_ctx = RedisContext{
        .allocator = allocator,
        .storage = storage,
        .aof = aof,
        .ps = ps,
        .subscriber_id = subscriber_id,
        .tx = tx,
        .repl = repl,
        .my_port = my_port,
        .replica_stream = replica_stream,
        .replica_idx = replica_idx,
        .client_registry = client_registry,
        .client_id = client_id,
        .script_store = script_store,
        .shutdown_state = shutdown_state,
        .databases = databases,
        .num_databases = num_databases,
        .read_only = true,
    };

    const timeout_ms = blk: {
        storage.config.mutex.lock();
        defer storage.config.mutex.unlock();
        const timeout_val = storage.config.params.get("lua-time-limit") orelse break :blk 5000;
        break :blk timeout_val.int;
    };

    var lua_engine = try LuaEngine.init(allocator, &redis_ctx, timeout_ms);
    defer lua_engine.deinit();

    const result_str = try lua_engine.eval(script, numkeys, keys, argv);
    defer allocator.free(result_str);

    if (std.mem.startsWith(u8, result_str, "ERR ")) {
        return w.writeError(result_str);
    }

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

/// SCRIPT KILL
/// Terminate a currently executing script
/// Returns error if no script is running
pub fn cmdScriptKill(
    allocator: Allocator,
    script_store: *ScriptStore,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Request script termination
    // The actual termination happens in the Lua debug hook
    script_store.requestKill();

    // NOTE: In a real implementation, we would check if a script is actually running
    // and return an error if not. For now, we always return OK since we don't
    // track script execution state globally. The kill flag will be checked on
    // the next hook invocation.
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
        "KILL",
        "    Kill the currently executing script.",
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

    // Create stub parameters for testing
    var ps = try PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init();
    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const script = "return 42";
    const args = [_][]const u8{ script, "0" };

    const response = try cmdEval(allocator, &storage, &script_store, &args, 2, null, &ps, 0, &tx, null, 6379, null, null, &client_registry, 0, null);
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

    // Create stub parameters
    var ps = try PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init();
    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const script = "return KEYS[1]";
    const args = [_][]const u8{ script, "1", "mykey" };

    const response = try cmdEval(allocator, &storage, &script_store, &args, 2, null, &ps, 0, &tx, null, 6379, null, null, &client_registry, 0, null);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("$5\r\nmykey\r\n", response);
}

test "cmdEval with ARGV" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    // Create stub parameters
    var ps = try PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init();
    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const script = "return ARGV[1] .. ' ' .. ARGV[2]";
    const args = [_][]const u8{ script, "0", "hello", "world" };

    const response = try cmdEval(allocator, &storage, &script_store, &args, 2, null, &ps, 0, &tx, null, 6379, null, null, &client_registry, 0, null);
    defer allocator.free(response);

    try std.testing.expectEqualStrings("$11\r\nhello world\r\n", response);
}

test "cmdEvalSHA nonexistent script" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    // Create stub parameters
    var ps = try PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init();
    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const fake_sha1 = "0123456789abcdef0123456789abcdef01234567";
    const args = [_][]const u8{ fake_sha1, "0" };

    const response = try cmdEvalSHA(allocator, &storage, &script_store, &args, 2, null, &ps, 0, &tx, null, 6379, null, null, &client_registry, 0, null);
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-NOSCRIPT"));
}

test "redis.call executes real Redis commands" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    // Create stub parameters
    var ps = try PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init();
    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Script that uses redis.call to SET and GET a key
    const script =
        \\redis.call('SET', 'mykey', 'myvalue')
        \\return redis.call('GET', 'mykey')
    ;
    const args = [_][]const u8{ script, "0" };

    const response = try cmdEval(allocator, &storage, &script_store, &args, 2, null, &ps, 0, &tx, null, 6379, null, null, &client_registry, 0, null);
    defer allocator.free(response);

    // Should return "myvalue"
    try std.testing.expectEqualStrings("$7\r\nmyvalue\r\n", response);
}

test "redis.call with multiple arguments" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    // Create stub parameters
    var ps = try PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init();
    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Script that uses redis.call to RPUSH multiple values
    const script =
        \\redis.call('RPUSH', 'mylist', 'a', 'b', 'c')
        \\return redis.call('LRANGE', 'mylist', '0', '-1')
    ;
    const args = [_][]const u8{ script, "0" };

    const response = try cmdEval(allocator, &storage, &script_store, &args, 2, null, &ps, 0, &tx, null, 6379, null, null, &client_registry, 0, null);
    defer allocator.free(response);

    // Should return array ['a', 'b', 'c']
    try std.testing.expect(std.mem.indexOf(u8, response, "a") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "b") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "c") != null);
}

test "redis.pcall catches errors" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    // Create stub parameters
    var ps = try PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init();
    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Script that uses redis.pcall with invalid command
    const script =
        \\local result = redis.pcall('INVALIDCMD', 'key')
        \\if result.err then
        \\  return 'error caught'
        \\else
        \\  return 'no error'
        \\end
    ;
    const args = [_][]const u8{ script, "0" };

    const response = try cmdEval(allocator, &storage, &script_store, &args, 2, null, &ps, 0, &tx, null, 6379, null, null, &client_registry, 0, null);
    defer allocator.free(response);

    // Should return "error caught"
    try std.testing.expect(std.mem.indexOf(u8, response, "error caught") != null);
}

test "redis.call with numeric return values" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    // Create stub parameters
    var ps = try PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init();
    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Script that uses redis.call to INCR and returns the integer
    const script =
        \\redis.call('SET', 'counter', '0')
        \\local val = redis.call('INCR', 'counter')
        \\return val
    ;
    const args = [_][]const u8{ script, "0" };

    const response = try cmdEval(allocator, &storage, &script_store, &args, 2, null, &ps, 0, &tx, null, 6379, null, null, &client_registry, 0, null);
    defer allocator.free(response);

    // Should return integer 1
    try std.testing.expect(std.mem.indexOf(u8, response, "1") != null);
}

test "cmdScriptKill sets kill flag" {
    const allocator = std.testing.allocator;
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    // Initially kill flag should be false
    try std.testing.expect(!script_store.isKillRequested());

    // Call SCRIPT KILL
    const response = try cmdScriptKill(allocator, &script_store);
    defer allocator.free(response);

    // Should return OK
    try std.testing.expectEqualStrings("+OK\r\n", response);

    // Kill flag should now be true
    try std.testing.expect(script_store.isKillRequested());

    // Clear the flag
    script_store.clearKill();
    try std.testing.expect(!script_store.isKillRequested());
}

test "cmdEvalRo executes read-only script" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();
    var ps = try PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init();
    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const databases = [_]Storage{};
    const script = "return 'hello'";
    const args = [_][]const u8{ script, "0" };

    const response = try cmdEvalRo(allocator, &storage, &script_store, &args, 2, null, &ps, 0, &tx, null, 6379, null, null, &client_registry, 0, null, &databases, 0);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "hello") != null);
}

test "cmdEvalRo blocks write commands" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();
    var ps = try PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init();
    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const databases = [_]Storage{};
    // SET is a write command — should be blocked in read-only mode
    const script = "redis.call('SET', 'key', 'val') return 1";
    const args = [_][]const u8{ script, "0" };

    const response = try cmdEvalRo(allocator, &storage, &script_store, &args, 2, null, &ps, 0, &tx, null, 6379, null, null, &client_registry, 0, null, &databases, 0);
    defer allocator.free(response);

    // Write command blocked — response should contain an error
    try std.testing.expect(std.mem.indexOf(u8, response, "ERR") != null or
        std.mem.indexOf(u8, response, "Write") != null);
}

test "cmdEvalShaRo executes cached script in read-only mode" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();
    var ps = try PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init();
    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const databases = [_]Storage{};

    // First load the script
    const script = "return 42";
    const sha1_owned = try script_store.loadScript(script);
    defer allocator.free(sha1_owned);
    const sha1 = sha1_owned;

    const args = [_][]const u8{ sha1, "0" };
    const response = try cmdEvalShaRo(allocator, &storage, &script_store, &args, 2, null, &ps, 0, &tx, null, 6379, null, null, &client_registry, 0, null, &databases, 0);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "42") != null);
}

test "cmdEvalShaRo returns NOSCRIPT for unknown sha" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();
    var ps = try PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init();
    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    const databases = [_]Storage{};
    const fake_sha = "0000000000000000000000000000000000000000";
    const args = [_][]const u8{ fake_sha, "0" };

    const response = try cmdEvalShaRo(allocator, &storage, &script_store, &args, 2, null, &ps, 0, &tx, null, 6379, null, null, &client_registry, 0, null, &databases, 0);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "NOSCRIPT") != null);
}

test "cmdEvalRo allows read-only commands" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();
    var ps = try PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init();
    var client_registry = try ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Pre-populate a key for reading
    _ = try storage.set("rokey", "rovalue", null);

    const databases = [_]Storage{};
    const script = "return redis.call('GET', 'rokey')";
    const args = [_][]const u8{ script, "1", "rokey" };

    const response = try cmdEvalRo(allocator, &storage, &script_store, &args, 2, null, &ps, 0, &tx, null, 6379, null, null, &client_registry, 0, null, &databases, 0);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "rovalue") != null);
}
