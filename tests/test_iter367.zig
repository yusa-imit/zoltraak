// Iteration 367: Real Lua scripting unit tests
//
// Replaces 13 placeholder tests in test_lua_scripting.zig with real assertions.
// Tests exercise EVAL, EVALSHA, and SCRIPT subcommands via the full command
// dispatcher (commands.executeCommand), verifying actual Lua execution results.
//
// All tests use the same pattern as iterations 338-366: @import("zoltraak")
// module with an execCmd helper that drives executeCommand end-to-end.

const std = @import("std");
const testing = std.testing;
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const ClientRegistry = zoltraak.ClientRegistry;
const commands = zoltraak.commands;
const PubSub = zoltraak.pubsub.PubSub;
const scripting = zoltraak.scripting_storage;
const ScriptStore = scripting.ScriptStore;
const transactions_mod = zoltraak.transactions_commands;

/// Execute a command using a caller-supplied script_store (needed for
/// SCRIPT LOAD → EVALSHA flows where the script must persist between calls).
fn execCmdWithStore(
    allocator: std.mem.Allocator,
    storage: *Storage,
    client_registry: *ClientRegistry,
    client_id: u64,
    ps: *PubSub,
    script_store: *ScriptStore,
    args: []const []const u8,
) ![]const u8 {
    var resp_args = try allocator.alloc(RespValue, args.len);
    defer allocator.free(resp_args);
    for (args, 0..) |a, i| resp_args[i] = .{ .bulk_string = a };
    const cmd = RespValue{ .array = resp_args };
    var tx = transactions_mod.TxState.init(allocator);
    defer tx.deinit();
    var databases = [_]Storage{storage.*};
    return commands.executeCommand(
        allocator,
        storage,
        cmd,
        null,
        ps,
        0,
        &tx,
        null,
        6379,
        null,
        null,
        client_registry,
        client_id,
        script_store,
        null,
        &databases,
        1,
    );
}

/// Shorthand for tests that do not need script persistence across calls.
fn execCmd(
    allocator: std.mem.Allocator,
    storage: *Storage,
    client_registry: *ClientRegistry,
    client_id: u64,
    ps: *PubSub,
    args: []const []const u8,
) ![]const u8 {
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();
    return execCmdWithStore(
        allocator,
        storage,
        client_registry,
        client_id,
        ps,
        &script_store,
        args,
    );
}

fn setup(allocator: std.mem.Allocator) !struct {
    storage: *Storage,
    registry: ClientRegistry,
    ps: PubSub,
    client_id: u64,
} {
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    var registry = ClientRegistry.init(allocator);
    const client_id = try registry.registerClient("127.0.0.1:9200", 10, "127.0.0.1:6379");
    const ps = PubSub.init(allocator);
    return .{ .storage = storage, .registry = registry, .ps = ps, .client_id = client_id };
}

// ── 1. EVAL: simple numeric return ───────────────────────────────────────────

test "EVAL: simple numeric return" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL", "return 42", "0",
    });
    defer allocator.free(result);

    // Redis returns integer 42 → RESP2 ":42\r\n"
    try testing.expectEqualStrings(":42\r\n", result);
}

// ── 2. EVAL: string return ────────────────────────────────────────────────────

test "EVAL: string return" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL", "return 'hello world'", "0",
    });
    defer allocator.free(result);

    // RESP2 bulk string "$11\r\nhello world\r\n"
    try testing.expectEqualStrings("$11\r\nhello world\r\n", result);
}

// ── 3. EVAL: KEYS access ──────────────────────────────────────────────────────

test "EVAL: KEYS access" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL", "return KEYS[1]", "1", "mykey",
    });
    defer allocator.free(result);

    try testing.expectEqualStrings("$5\r\nmykey\r\n", result);
}

// ── 4. EVAL: ARGV access ──────────────────────────────────────────────────────

test "EVAL: ARGV access" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL", "return ARGV[1] .. ' ' .. ARGV[2]", "0", "hello", "world",
    });
    defer allocator.free(result);

    try testing.expectEqualStrings("$11\r\nhello world\r\n", result);
}

// ── 5. EVAL: nil return ───────────────────────────────────────────────────────

test "EVAL: nil return" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL", "return nil", "0",
    });
    defer allocator.free(result);

    // Nil → RESP2 null bulk string "$-1\r\n"
    try testing.expectEqualStrings("$-1\r\n", result);
}

// ── 6. EVAL: boolean true return ─────────────────────────────────────────────

test "EVAL: boolean true return" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL", "return true", "0",
    });
    defer allocator.free(result);

    // Redis converts Lua true → integer 1
    try testing.expectEqualStrings(":1\r\n", result);
}

// ── 7. EVAL: boolean false return ────────────────────────────────────────────

test "EVAL: boolean false return" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL", "return false", "0",
    });
    defer allocator.free(result);

    // Redis converts Lua false → null bulk string (nil)
    try testing.expectEqualStrings("$-1\r\n", result);
}

// ── 8. EVAL: table (array) return ────────────────────────────────────────────

test "EVAL: table array return" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL", "return {1, 2, 3}", "0",
    });
    defer allocator.free(result);

    // RESP2 array of integers
    try testing.expectEqualStrings("*3\r\n:1\r\n:2\r\n:3\r\n", result);
}

// ── 9. EVAL: syntax error handling ───────────────────────────────────────────

test "EVAL: syntax error handling" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVAL", "return 42 +", "0",
    });
    defer allocator.free(result);

    // Should return a RESP2 error response
    try testing.expect(result[0] == '-');
    try testing.expect(std.mem.indexOf(u8, result, "ERR") != null);
}

// ── 10. SCRIPT LOAD and EVALSHA roundtrip ────────────────────────────────────

test "SCRIPT LOAD and EVALSHA roundtrip" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    // Load a script → get SHA1
    const load_result = try execCmdWithStore(
        allocator,
        s.storage,
        &s.registry,
        s.client_id,
        &s.ps,
        &script_store,
        &.{ "SCRIPT", "LOAD", "return 99" },
    );
    defer allocator.free(load_result);

    // Should return a 40-character SHA1 bulk string: "$40\r\n<sha>\r\n"
    try testing.expect(std.mem.startsWith(u8, load_result, "$40\r\n"));
    try testing.expectEqual(@as(usize, 47), load_result.len); // "$40\r\n"(5) + 40 SHA chars + "\r\n"(2) = 47

    // Extract the SHA from the response
    const sha = load_result[5 .. 5 + 40];

    // Execute via EVALSHA
    const eval_result = try execCmdWithStore(
        allocator,
        s.storage,
        &s.registry,
        s.client_id,
        &s.ps,
        &script_store,
        &.{ "EVALSHA", sha, "0" },
    );
    defer allocator.free(eval_result);

    try testing.expectEqualStrings(":99\r\n", eval_result);
}

// ── 11. EVALSHA: nonexistent script returns NOSCRIPT ─────────────────────────

test "EVALSHA: nonexistent script returns NOSCRIPT error" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{
        "EVALSHA", "0000000000000000000000000000000000000000", "0",
    });
    defer allocator.free(result);

    try testing.expect(result[0] == '-');
    try testing.expect(std.mem.indexOf(u8, result, "NOSCRIPT") != null);
}

// ── 12. SCRIPT EXISTS: 1 for known, 0 for unknown ────────────────────────────

test "SCRIPT EXISTS: returns 1 for loaded script, 0 for unknown" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    // Load a script to get its SHA1
    const load_result = try execCmdWithStore(
        allocator,
        s.storage,
        &s.registry,
        s.client_id,
        &s.ps,
        &script_store,
        &.{ "SCRIPT", "LOAD", "return 'exists'" },
    );
    defer allocator.free(load_result);
    const sha = load_result[5 .. 5 + 40];

    const unknown_sha = "ffffffffffffffffffffffffffffffffffffffff";

    // SCRIPT EXISTS <known_sha> <unknown_sha> → [1, 0]
    const exists_result = try execCmdWithStore(
        allocator,
        s.storage,
        &s.registry,
        s.client_id,
        &s.ps,
        &script_store,
        &.{ "SCRIPT", "EXISTS", sha, unknown_sha },
    );
    defer allocator.free(exists_result);

    try testing.expectEqualStrings("*2\r\n:1\r\n:0\r\n", exists_result);
}

// ── 13. SCRIPT FLUSH: clears all cached scripts ──────────────────────────────

test "SCRIPT FLUSH: clears cached scripts" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    // Load a script
    const load_result = try execCmdWithStore(
        allocator,
        s.storage,
        &s.registry,
        s.client_id,
        &s.ps,
        &script_store,
        &.{ "SCRIPT", "LOAD", "return 'flush test'" },
    );
    defer allocator.free(load_result);
    const sha = load_result[5 .. 5 + 40];

    // Verify it exists (returns 1)
    const before_result = try execCmdWithStore(
        allocator,
        s.storage,
        &s.registry,
        s.client_id,
        &s.ps,
        &script_store,
        &.{ "SCRIPT", "EXISTS", sha },
    );
    defer allocator.free(before_result);
    try testing.expectEqualStrings("*1\r\n:1\r\n", before_result);

    // Flush
    const flush_result = try execCmdWithStore(
        allocator,
        s.storage,
        &s.registry,
        s.client_id,
        &s.ps,
        &script_store,
        &.{ "SCRIPT", "FLUSH" },
    );
    defer allocator.free(flush_result);
    try testing.expectEqualStrings("+OK\r\n", flush_result);

    // Verify it's gone (returns 0)
    const after_result = try execCmdWithStore(
        allocator,
        s.storage,
        &s.registry,
        s.client_id,
        &s.ps,
        &script_store,
        &.{ "SCRIPT", "EXISTS", sha },
    );
    defer allocator.free(after_result);
    try testing.expectEqualStrings("*1\r\n:0\r\n", after_result);
}
