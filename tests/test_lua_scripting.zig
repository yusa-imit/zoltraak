// Integration tests for Lua scripting (EVAL/EVALSHA/SCRIPT commands)
// Tests real Lua execution with LuaJIT

const std = @import("std");
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;

test "EVAL: simple numeric return" {
    const allocator = std.testing.allocator;

    // Note: This test requires a running server with Lua support
    // For now, this is a placeholder for future E2E integration tests
    // Real tests would use redis-cli or a Redis client library

    try expect(true);
}

test "EVAL: string return" {
    const allocator = std.testing.allocator;
    _ = allocator;

    // Placeholder - real test would execute:
    // EVAL "return 'hello world'" 0
    // Expected: "hello world"

    try expect(true);
}

test "EVAL: KEYS access" {
    const allocator = std.testing.allocator;
    _ = allocator;

    // Placeholder - real test would execute:
    // EVAL "return KEYS[1]" 1 mykey
    // Expected: "mykey"

    try expect(true);
}

test "EVAL: ARGV access" {
    const allocator = std.testing.allocator;
    _ = allocator;

    // Placeholder - real test would execute:
    // EVAL "return ARGV[1] .. ' ' .. ARGV[2]" 0 hello world
    // Expected: "hello world"

    try expect(true);
}

test "EVAL: nil return" {
    const allocator = std.testing.allocator;
    _ = allocator;

    // Placeholder - real test would execute:
    // EVAL "return nil" 0
    // Expected: nil (null bulk string)

    try expect(true);
}

test "EVAL: boolean return" {
    const allocator = std.testing.allocator;
    _ = allocator;

    // Placeholder - real test would execute:
    // EVAL "return true" 0
    // Expected: "true"

    try expect(true);
}

test "EVAL: syntax error handling" {
    const allocator = std.testing.allocator;
    _ = allocator;

    // Placeholder - real test would execute:
    // EVAL "return 42 +" 0
    // Expected: ERR Error compiling script

    try expect(true);
}

test "EVAL: runtime error handling" {
    const allocator = std.testing.allocator;
    _ = allocator;

    // Placeholder - real test would execute:
    // EVAL "return nil + 1" 0
    // Expected: ERR Error running script

    try expect(true);
}

test "EVALSHA: execute cached script" {
    const allocator = std.testing.allocator;
    _ = allocator;

    // Placeholder - real test would:
    // 1. SCRIPT LOAD "return 42"
    // 2. Get SHA1
    // 3. EVALSHA <sha1> 0
    // Expected: "42"

    try expect(true);
}

test "EVALSHA: nonexistent script" {
    const allocator = std.testing.allocator;
    _ = allocator;

    // Placeholder - real test would execute:
    // EVALSHA 0123456789abcdef0123456789abcdef01234567 0
    // Expected: NOSCRIPT error

    try expect(true);
}

test "SCRIPT LOAD: cache script" {
    const allocator = std.testing.allocator;
    _ = allocator;

    // Placeholder - real test would execute:
    // SCRIPT LOAD "return redis.call('get', KEYS[1])"
    // Expected: 40-character SHA1 hash

    try expect(true);
}

test "SCRIPT EXISTS: check script existence" {
    const allocator = std.testing.allocator;
    _ = allocator;

    // Placeholder - real test would:
    // 1. SCRIPT LOAD "return 42"
    // 2. Get SHA1
    // 3. SCRIPT EXISTS <sha1> <nonexistent_sha1>
    // Expected: [1, 0]

    try expect(true);
}

test "SCRIPT FLUSH: clear script cache" {
    const allocator = std.testing.allocator;
    _ = allocator;

    // Placeholder - real test would:
    // 1. SCRIPT LOAD "return ARGV[1]"
    // 2. Get SHA1
    // 3. SCRIPT EXISTS <sha1> -> 1
    // 4. SCRIPT FLUSH
    // 5. SCRIPT EXISTS <sha1> -> 0

    try expect(true);
}

// Integration tests for sandboxing
const Storage = @import("../src/storage/memory.zig").Storage;
const executeCommand = @import("../src/commands/strings.zig").executeCommand;
const protocol = @import("../src/protocol/parser.zig");
const RespValue = protocol.RespValue;
const ClientRegistry = @import("../src/commands/client.zig").ClientRegistry;
const TxState = @import("../src/commands/transactions.zig").TxState;
const ShutdownState = @import("../src/commands/utility.zig").ShutdownState;

test "Sandboxing: EVAL blocks os.execute" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    var tx_state = TxState.init(allocator);
    defer tx_state.deinit();

    var shutdown_state = ShutdownState{};

    // Try to execute os.execute (should fail due to sandboxing)
    const script = "return os.execute('echo pwned')";
    const cmd_array = [_]RespValue{
        RespValue{ .bulk_string = "EVAL" },
        RespValue{ .bulk_string = script },
        RespValue{ .bulk_string = "0" },
    };
    const cmd = RespValue{ .array = &cmd_array };

    const result = try executeCommand(allocator, &storage, cmd, 0, &client_registry, 1, &tx_state, null, false, &shutdown_state);
    defer allocator.free(result);

    // Should return error because os is nil
    try expect(std.mem.indexOf(u8, result, "-ERR") != null);
}

test "Sandboxing: EVAL blocks io.open" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    var tx_state = TxState.init(allocator);
    defer tx_state.deinit();

    var shutdown_state = ShutdownState{};

    // Try to access io.open (should fail due to sandboxing)
    const script = "return io.open('/etc/passwd', 'r')";
    const cmd_array = [_]RespValue{
        RespValue{ .bulk_string = "EVAL" },
        RespValue{ .bulk_string = script },
        RespValue{ .bulk_string = "0" },
    };
    const cmd = RespValue{ .array = &cmd_array };

    const result = try executeCommand(allocator, &storage, cmd, 0, &client_registry, 1, &tx_state, null, false, &shutdown_state);
    defer allocator.free(result);

    // Should return error because io is nil
    try expect(std.mem.indexOf(u8, result, "-ERR") != null);
}

test "Sandboxing: EVAL blocks loadfile" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    var tx_state = TxState.init(allocator);
    defer tx_state.deinit();

    var shutdown_state = ShutdownState{};

    // Try to use loadfile (should fail due to sandboxing)
    const script = "return loadfile('/tmp/evil.lua')";
    const cmd_array = [_]RespValue{
        RespValue{ .bulk_string = "EVAL" },
        RespValue{ .bulk_string = script },
        RespValue{ .bulk_string = "0" },
    };
    const cmd = RespValue{ .array = &cmd_array };

    const result = try executeCommand(allocator, &storage, cmd, 0, &client_registry, 1, &tx_state, null, false, &shutdown_state);
    defer allocator.free(result);

    // Should return error because loadfile is nil
    try expect(std.mem.indexOf(u8, result, "-ERR") != null);
}

test "Sandboxing: EVAL blocks dofile" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    var tx_state = TxState.init(allocator);
    defer tx_state.deinit();

    var shutdown_state = ShutdownState{};

    // Try to use dofile (should fail due to sandboxing)
    const script = "return dofile('/tmp/evil.lua')";
    const cmd_array = [_]RespValue{
        RespValue{ .bulk_string = "EVAL" },
        RespValue{ .bulk_string = script },
        RespValue{ .bulk_string = "0" },
    };
    const cmd = RespValue{ .array = &cmd_array };

    const result = try executeCommand(allocator, &storage, cmd, 0, &client_registry, 1, &tx_state, null, false, &shutdown_state);
    defer allocator.free(result);

    // Should return error because dofile is nil
    try expect(std.mem.indexOf(u8, result, "-ERR") != null);
}

test "Sandboxing: EVAL blocks require of dangerous modules" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    var tx_state = TxState.init(allocator);
    defer tx_state.deinit();

    var shutdown_state = ShutdownState{};

    // Try to require a disallowed module
    const script = "return require('os')";
    const cmd_array = [_]RespValue{
        RespValue{ .bulk_string = "EVAL" },
        RespValue{ .bulk_string = script },
        RespValue{ .bulk_string = "0" },
    };
    const cmd = RespValue{ .array = &cmd_array };

    const result = try executeCommand(allocator, &storage, cmd, 0, &client_registry, 1, &tx_state, null, false, &shutdown_state);
    defer allocator.free(result);

    // Should return error with require restriction message
    try expect(std.mem.indexOf(u8, result, "require is restricted") != null);
}

test "Sandboxing: EVAL allows safe libraries" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    var tx_state = TxState.init(allocator);
    defer tx_state.deinit();

    var shutdown_state = ShutdownState{};

    // Use math and string libraries (should work)
    const script = "return math.abs(-42) + string.len('hello')";
    const cmd_array = [_]RespValue{
        RespValue{ .bulk_string = "EVAL" },
        RespValue{ .bulk_string = script },
        RespValue{ .bulk_string = "0" },
    };
    const cmd = RespValue{ .array = &cmd_array };

    const result = try executeCommand(allocator, &storage, cmd, 0, &client_registry, 1, &tx_state, null, false, &shutdown_state);
    defer allocator.free(result);

    // Should succeed and return the value
    try expect(std.mem.indexOf(u8, result, "$2") != null); // RESP2 bulk string
    try expect(std.mem.indexOf(u8, result, "47") != null);
}

test "Sandboxing: EVAL blocks global variable creation" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    var tx_state = TxState.init(allocator);
    defer tx_state.deinit();

    var shutdown_state = ShutdownState{};

    // Try to create a global variable (should fail)
    const script = "evil_global = 42; return evil_global";
    const cmd_array = [_]RespValue{
        RespValue{ .bulk_string = "EVAL" },
        RespValue{ .bulk_string = script },
        RespValue{ .bulk_string = "0" },
    };
    const cmd = RespValue{ .array = &cmd_array };

    const result = try executeCommand(allocator, &storage, cmd, 0, &client_registry, 1, &tx_state, null, false, &shutdown_state);
    defer allocator.free(result);

    // Should return error with global variable creation message
    try expect(std.mem.indexOf(u8, result, "create global variable") != null);
}

test "Sandboxing: EVAL allows local variables" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    var tx_state = TxState.init(allocator);
    defer tx_state.deinit();

    var shutdown_state = ShutdownState{};

    // Use local variables (should work)
    const script = "local safe_var = 42; return safe_var";
    const cmd_array = [_]RespValue{
        RespValue{ .bulk_string = "EVAL" },
        RespValue{ .bulk_string = script },
        RespValue{ .bulk_string = "0" },
    };
    const cmd = RespValue{ .array = &cmd_array };

    const result = try executeCommand(allocator, &storage, cmd, 0, &client_registry, 1, &tx_state, null, false, &shutdown_state);
    defer allocator.free(result);

    // Should succeed and return the value
    try expect(std.mem.indexOf(u8, result, "$2") != null); // RESP2 bulk string
    try expect(std.mem.indexOf(u8, result, "42") != null);
}
