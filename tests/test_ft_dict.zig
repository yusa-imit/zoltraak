const std = @import("std");
const Storage = @import("../src/storage/memory.zig").Storage;
const parseCommand = @import("../src/protocol/parser.zig").parseCommand;
const processCommand = @import("../src/commands/strings.zig").processCommand;
const TxState = @import("../src/commands/strings.zig").TxState;
const ReplicationState = @import("../src/commands/strings.zig").ReplicationState;
const ScriptStore = @import("../src/commands/strings.zig").ScriptStore;
const PubSubState = @import("../src/storage/pubsub.zig").PubSubState;
const ClientRegistry = @import("../src/commands/client.zig").ClientRegistry;

// ============================================================================
// FT.DICTADD Tests
// ============================================================================

test "FT.DICTADD: creates dictionary and adds single term" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Add single term to new dictionary
    const cmd = "FT.DICTADD stopwords_en the\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should return :1\r\n (1 term added)
    try std.testing.expect(std.mem.indexOf(u8, result, ":1\r\n") != null);
}

test "FT.DICTADD: adds multiple terms at once" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Add 3 terms
    const cmd = "FT.DICTADD stopwords_en a the is\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should return :3\r\n (3 terms added)
    try std.testing.expect(std.mem.indexOf(u8, result, ":3\r\n") != null);
}

test "FT.DICTADD: ignores duplicate terms (same call)" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Add "the" twice in same command
    const cmd = "FT.DICTADD stopwords_en the the a\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should return :2\r\n (only "the" and "a" counted, duplicate "the" ignored)
    try std.testing.expect(std.mem.indexOf(u8, result, ":2\r\n") != null);
}

test "FT.DICTADD: ignores duplicate terms (across calls)" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // First call: add "the" and "a"
    {
        const cmd1 = "FT.DICTADD stopwords_en the a\r\n";
        const parsed1 = try parseCommand(allocator, cmd1);
        defer parsed1.deinit(allocator);

        var arena1 = std.heap.ArenaAllocator.init(allocator);
        defer arena1.deinit();

        const result1 = try processCommand(arena1.allocator(), parsed1.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result1);

        try std.testing.expect(std.mem.indexOf(u8, result1, ":2\r\n") != null);
    }

    // Second call: add "the" again (duplicate) and "is" (new)
    {
        const cmd2 = "FT.DICTADD stopwords_en the is\r\n";
        const parsed2 = try parseCommand(allocator, cmd2);
        defer parsed2.deinit(allocator);

        var arena2 = std.heap.ArenaAllocator.init(allocator);
        defer arena2.deinit();

        const result2 = try processCommand(arena2.allocator(), parsed2.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result2);

        // Should return :1\r\n (only "is" is new, "the" already exists)
        try std.testing.expect(std.mem.indexOf(u8, result2, ":1\r\n") != null);
    }
}

test "FT.DICTADD: terms are case-sensitive" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Add "The" and "the" (different case)
    const cmd = "FT.DICTADD stopwords_en The the\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should return :2\r\n (both counted as different terms)
    try std.testing.expect(std.mem.indexOf(u8, result, ":2\r\n") != null);
}

test "FT.DICTADD: arity validation (requires at least one term)" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // No terms provided
    const cmd = "FT.DICTADD stopwords_en\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR") != null);
}

// ============================================================================
// FT.DICTDEL Tests
// ============================================================================

test "FT.DICTDEL: removes existing terms" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Add terms
    {
        const cmd1 = "FT.DICTADD stopwords_en the a is are\r\n";
        const parsed1 = try parseCommand(allocator, cmd1);
        defer parsed1.deinit(allocator);

        var arena1 = std.heap.ArenaAllocator.init(allocator);
        defer arena1.deinit();

        const result1 = try processCommand(arena1.allocator(), parsed1.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result1);
    }

    // Delete 2 terms
    {
        const cmd2 = "FT.DICTDEL stopwords_en the a\r\n";
        const parsed2 = try parseCommand(allocator, cmd2);
        defer parsed2.deinit(allocator);

        var arena2 = std.heap.ArenaAllocator.init(allocator);
        defer arena2.deinit();

        const result2 = try processCommand(arena2.allocator(), parsed2.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result2);

        // Should return :2\r\n (2 terms removed)
        try std.testing.expect(std.mem.indexOf(u8, result2, ":2\r\n") != null);
    }
}

test "FT.DICTDEL: returns 0 for non-existing terms" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Add terms
    {
        const cmd1 = "FT.DICTADD stopwords_en the a\r\n";
        const parsed1 = try parseCommand(allocator, cmd1);
        defer parsed1.deinit(allocator);

        var arena1 = std.heap.ArenaAllocator.init(allocator);
        defer arena1.deinit();

        const result1 = try processCommand(arena1.allocator(), parsed1.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result1);
    }

    // Delete non-existing term
    {
        const cmd2 = "FT.DICTDEL stopwords_en xyz\r\n";
        const parsed2 = try parseCommand(allocator, cmd2);
        defer parsed2.deinit(allocator);

        var arena2 = std.heap.ArenaAllocator.init(allocator);
        defer arena2.deinit();

        const result2 = try processCommand(arena2.allocator(), parsed2.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result2);

        // Should return :0\r\n (0 terms removed)
        try std.testing.expect(std.mem.indexOf(u8, result2, ":0\r\n") != null);
    }
}

test "FT.DICTDEL: returns 0 for non-existing dictionary" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Delete from non-existing dictionary
    const cmd = "FT.DICTDEL nonexistent_dict the\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should return :0\r\n (0 terms removed)
    try std.testing.expect(std.mem.indexOf(u8, result, ":0\r\n") != null);
}

test "FT.DICTDEL: partial deletion counts only removed terms" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Add terms
    {
        const cmd1 = "FT.DICTADD stopwords_en the a\r\n";
        const parsed1 = try parseCommand(allocator, cmd1);
        defer parsed1.deinit(allocator);

        var arena1 = std.heap.ArenaAllocator.init(allocator);
        defer arena1.deinit();

        const result1 = try processCommand(arena1.allocator(), parsed1.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result1);
    }

    // Delete mix of existing and non-existing terms
    {
        const cmd2 = "FT.DICTDEL stopwords_en the xyz a abc\r\n";
        const parsed2 = try parseCommand(allocator, cmd2);
        defer parsed2.deinit(allocator);

        var arena2 = std.heap.ArenaAllocator.init(allocator);
        defer arena2.deinit();

        const result2 = try processCommand(arena2.allocator(), parsed2.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result2);

        // Should return :2\r\n (only "the" and "a" removed, "xyz" and "abc" don't exist)
        try std.testing.expect(std.mem.indexOf(u8, result2, ":2\r\n") != null);
    }
}

test "FT.DICTDEL: arity validation (requires at least one term)" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // No terms provided
    const cmd = "FT.DICTDEL stopwords_en\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should return error
    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR") != null);
}

// ============================================================================
// FT.DICTDUMP Tests
// ============================================================================

test "FT.DICTDUMP: returns all terms in dictionary" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Add terms
    {
        const cmd1 = "FT.DICTADD stopwords_en the a is\r\n";
        const parsed1 = try parseCommand(allocator, cmd1);
        defer parsed1.deinit(allocator);

        var arena1 = std.heap.ArenaAllocator.init(allocator);
        defer arena1.deinit();

        const result1 = try processCommand(arena1.allocator(), parsed1.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result1);
    }

    // Dump dictionary
    {
        const cmd2 = "FT.DICTDUMP stopwords_en\r\n";
        const parsed2 = try parseCommand(allocator, cmd2);
        defer parsed2.deinit(allocator);

        var arena2 = std.heap.ArenaAllocator.init(allocator);
        defer arena2.deinit();

        const result2 = try processCommand(arena2.allocator(), parsed2.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result2);

        // Should return array with 3 elements: *3\r\n
        try std.testing.expect(std.mem.indexOf(u8, result2, "*3\r\n") != null);
        // Should contain all terms
        try std.testing.expect(std.mem.indexOf(u8, result2, "the") != null);
        try std.testing.expect(std.mem.indexOf(u8, result2, "a") != null);
        try std.testing.expect(std.mem.indexOf(u8, result2, "is") != null);
    }
}

test "FT.DICTDUMP: returns empty array for non-existing dictionary" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Dump non-existing dictionary
    const cmd = "FT.DICTDUMP nonexistent_dict\r\n";
    const parsed = try parseCommand(allocator, cmd);
    defer parsed.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
    defer allocator.free(result);

    // Should return empty array: *0\r\n
    try std.testing.expect(std.mem.indexOf(u8, result, "*0\r\n") != null);
}

test "FT.DICTDUMP: returns empty array for empty dictionary" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Create dictionary with terms then delete all
    {
        const cmd1 = "FT.DICTADD stopwords_en the a\r\n";
        const parsed1 = try parseCommand(allocator, cmd1);
        defer parsed1.deinit(allocator);

        var arena1 = std.heap.ArenaAllocator.init(allocator);
        defer arena1.deinit();

        const result1 = try processCommand(arena1.allocator(), parsed1.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result1);
    }

    {
        const cmd2 = "FT.DICTDEL stopwords_en the a\r\n";
        const parsed2 = try parseCommand(allocator, cmd2);
        defer parsed2.deinit(allocator);

        var arena2 = std.heap.ArenaAllocator.init(allocator);
        defer arena2.deinit();

        const result2 = try processCommand(arena2.allocator(), parsed2.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result2);
    }

    // Dump empty dictionary
    {
        const cmd3 = "FT.DICTDUMP stopwords_en\r\n";
        const parsed3 = try parseCommand(allocator, cmd3);
        defer parsed3.deinit(allocator);

        var arena3 = std.heap.ArenaAllocator.init(allocator);
        defer arena3.deinit();

        const result3 = try processCommand(arena3.allocator(), parsed3.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result3);

        // Should return empty array: *0\r\n
        try std.testing.expect(std.mem.indexOf(u8, result3, "*0\r\n") != null);
    }
}

test "FT.DICTDUMP: terms are returned in insertion order" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Add terms in specific order
    {
        const cmd1 = "FT.DICTADD stopwords_en zebra apple middle\r\n";
        const parsed1 = try parseCommand(allocator, cmd1);
        defer parsed1.deinit(allocator);

        var arena1 = std.heap.ArenaAllocator.init(allocator);
        defer arena1.deinit();

        const result1 = try processCommand(arena1.allocator(), parsed1.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result1);
    }

    // Dump dictionary
    {
        const cmd2 = "FT.DICTDUMP stopwords_en\r\n";
        const parsed2 = try parseCommand(allocator, cmd2);
        defer parsed2.deinit(allocator);

        var arena2 = std.heap.ArenaAllocator.init(allocator);
        defer arena2.deinit();

        const result2 = try processCommand(arena2.allocator(), parsed2.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result2);

        // Verify order: zebra should appear before apple, apple before middle
        const zebra_pos = std.mem.indexOf(u8, result2, "zebra").?;
        const apple_pos = std.mem.indexOf(u8, result2, "apple").?;
        const middle_pos = std.mem.indexOf(u8, result2, "middle").?;

        try std.testing.expect(zebra_pos < apple_pos);
        try std.testing.expect(apple_pos < middle_pos);
    }
}

test "FT.DICTDUMP: arity validation (requires exactly 2 args)" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Too few args
    {
        const cmd1 = "FT.DICTDUMP\r\n";
        const parsed1 = try parseCommand(allocator, cmd1);
        defer parsed1.deinit(allocator);

        var arena1 = std.heap.ArenaAllocator.init(allocator);
        defer arena1.deinit();

        const result1 = try processCommand(arena1.allocator(), parsed1.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result1);

        // Should return error
        try std.testing.expect(std.mem.indexOf(u8, result1, "-ERR") != null);
    }

    // Too many args
    {
        const cmd2 = "FT.DICTDUMP stopwords_en extra\r\n";
        const parsed2 = try parseCommand(allocator, cmd2);
        defer parsed2.deinit(allocator);

        var arena2 = std.heap.ArenaAllocator.init(allocator);
        defer arena2.deinit();

        const result2 = try processCommand(arena2.allocator(), parsed2.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result2);

        // Should return error
        try std.testing.expect(std.mem.indexOf(u8, result2, "-ERR") != null);
    }
}

// ============================================================================
// Cross-Command Integration Tests
// ============================================================================

test "FT.DICT: full lifecycle (add, dump, delete, dump)" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // 1. Add terms
    {
        const cmd = "FT.DICTADD stopwords_en the a is\r\n";
        const parsed = try parseCommand(allocator, cmd);
        defer parsed.deinit(allocator);

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result);

        try std.testing.expect(std.mem.indexOf(u8, result, ":3\r\n") != null);
    }

    // 2. Dump (should have 3 terms)
    {
        const cmd = "FT.DICTDUMP stopwords_en\r\n";
        const parsed = try parseCommand(allocator, cmd);
        defer parsed.deinit(allocator);

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result);

        try std.testing.expect(std.mem.indexOf(u8, result, "*3\r\n") != null);
    }

    // 3. Delete one term
    {
        const cmd = "FT.DICTDEL stopwords_en a\r\n";
        const parsed = try parseCommand(allocator, cmd);
        defer parsed.deinit(allocator);

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result);

        try std.testing.expect(std.mem.indexOf(u8, result, ":1\r\n") != null);
    }

    // 4. Dump again (should have 2 terms now)
    {
        const cmd = "FT.DICTDUMP stopwords_en\r\n";
        const parsed = try parseCommand(allocator, cmd);
        defer parsed.deinit(allocator);

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result);

        try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
        try std.testing.expect(std.mem.indexOf(u8, result, "the") != null);
        try std.testing.expect(std.mem.indexOf(u8, result, "is") != null);
        // "a" should not be present
        const has_a_alone = std.mem.indexOf(u8, result, "$1\r\na\r\n") != null;
        try std.testing.expect(!has_a_alone);
    }
}

test "FT.DICT: multiple dictionaries are independent" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Add to dict1
    {
        const cmd = "FT.DICTADD dict1 apple banana\r\n";
        const parsed = try parseCommand(allocator, cmd);
        defer parsed.deinit(allocator);

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result);
    }

    // Add to dict2
    {
        const cmd = "FT.DICTADD dict2 cherry date\r\n";
        const parsed = try parseCommand(allocator, cmd);
        defer parsed.deinit(allocator);

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result);
    }

    // Dump dict1 (should only have apple, banana)
    {
        const cmd = "FT.DICTDUMP dict1\r\n";
        const parsed = try parseCommand(allocator, cmd);
        defer parsed.deinit(allocator);

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result);

        try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
        try std.testing.expect(std.mem.indexOf(u8, result, "apple") != null);
        try std.testing.expect(std.mem.indexOf(u8, result, "banana") != null);
        try std.testing.expect(std.mem.indexOf(u8, result, "cherry") == null);
    }

    // Dump dict2 (should only have cherry, date)
    {
        const cmd = "FT.DICTDUMP dict2\r\n";
        const parsed = try parseCommand(allocator, cmd);
        defer parsed.deinit(allocator);

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result);

        try std.testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
        try std.testing.expect(std.mem.indexOf(u8, result, "cherry") != null);
        try std.testing.expect(std.mem.indexOf(u8, result, "date") != null);
        try std.testing.expect(std.mem.indexOf(u8, result, "apple") == null);
    }
}

test "FT.DICT: stress test with many terms" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var tx = TxState.init(allocator);
    defer tx.deinit();

    var repl = ReplicationState.init(allocator);
    defer repl.deinit();

    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    var ps = PubSubState.init(allocator);
    defer ps.deinit();

    var client_registry = ClientRegistry.init(allocator);
    defer client_registry.deinit();

    // Add 100 unique terms
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        var buf: [256]u8 = undefined;
        const cmd_str = try std.fmt.bufPrint(&buf, "FT.DICTADD stress_dict term{d} term{d} term{d} term{d} term{d} term{d} term{d} term{d} term{d} term{d}\r\n", .{ i * 10, i * 10 + 1, i * 10 + 2, i * 10 + 3, i * 10 + 4, i * 10 + 5, i * 10 + 6, i * 10 + 7, i * 10 + 8, i * 10 + 9 });

        const parsed = try parseCommand(allocator, cmd_str);
        defer parsed.deinit(allocator);

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result);

        // Each batch should add exactly 10 terms
        try std.testing.expect(std.mem.indexOf(u8, result, ":10\r\n") != null);
    }

    // Dump should return all 100 terms
    {
        const cmd = "FT.DICTDUMP stress_dict\r\n";
        const parsed = try parseCommand(allocator, cmd);
        defer parsed.deinit(allocator);

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        const result = try processCommand(arena.allocator(), parsed.value.array, storage, &ps, &tx, &script_store, &repl, &client_registry, 1);
        defer allocator.free(result);

        try std.testing.expect(std.mem.indexOf(u8, result, "*100\r\n") != null);
    }
}
