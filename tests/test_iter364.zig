// Iteration 364: Add 117 module commands to ALL_COMMANDS and COMMAND_DOCS
//
// BF.*, CF.*, CMS.*, TOPK.*, TDIGEST.*, JSON.*, FT.*, TS.* commands are now
// registered in ALL_COMMANDS so COMMAND INFO returns valid metadata instead of nil.
// COMMAND COUNT is now 387 (was 270, +117 module commands).

const std = @import("std");
const testing = std.testing;
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const ClientRegistry = zoltraak.ClientRegistry;
const commands = zoltraak.commands;
const PubSub = zoltraak.pubsub.PubSub;
const scripting = zoltraak.scripting_storage;
const transactions_mod = zoltraak.transactions_commands;

fn execCmd(
    allocator: std.mem.Allocator,
    storage: *Storage,
    client_registry: *ClientRegistry,
    client_id: u64,
    ps: *PubSub,
    args: []const []const u8,
) ![]const u8 {
    var resp_args = try allocator.alloc(RespValue, args.len);
    defer allocator.free(resp_args);
    for (args, 0..) |a, i| {
        resp_args[i] = .{ .bulk_string = a };
    }
    const cmd = RespValue{ .array = resp_args };
    var tx = transactions_mod.TxState.init(allocator);
    defer tx.deinit();
    var script_store = scripting.ScriptStore.init(allocator);
    defer script_store.deinit();
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
        &script_store,
        null,
        &databases,
        1,
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

test "iter364 - COMMAND COUNT increased to 387" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "COUNT" });
    defer allocator.free(result);
    try testing.expectEqualStrings(":387\r\n", result);
}

test "iter364 - COMMAND INFO for BF commands returns valid info" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const bf_commands = [_][]const u8{ "BF.RESERVE", "BF.ADD", "BF.MADD", "BF.EXISTS", "BF.MEXISTS", "BF.INSERT", "BF.INFO", "BF.CARD", "BF.SCANDUMP", "BF.LOADCHUNK" };
    for (bf_commands) |cmd| {
        const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", cmd });
        defer allocator.free(result);
        // Should NOT be a nil response ($-1)
        try testing.expect(!std.mem.eql(u8, result, "*1\r\n$-1\r\n"));
        // Should be an array response
        try testing.expect(result[0] == '*');
    }
}

test "iter364 - COMMAND INFO for CF commands returns valid info" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const cf_commands = [_][]const u8{ "CF.RESERVE", "CF.ADD", "CF.ADDNX", "CF.EXISTS", "CF.MEXISTS", "CF.DEL", "CF.COUNT", "CF.INSERT", "CF.INSERTNX", "CF.INFO", "CF.SCANDUMP", "CF.LOADCHUNK" };
    for (cf_commands) |cmd| {
        const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", cmd });
        defer allocator.free(result);
        try testing.expect(!std.mem.eql(u8, result, "*1\r\n$-1\r\n"));
        try testing.expect(result[0] == '*');
    }
}

test "iter364 - COMMAND INFO for CMS commands returns valid info" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const cms_commands = [_][]const u8{ "CMS.INITBYDIM", "CMS.INITBYPROB", "CMS.INCRBY", "CMS.QUERY", "CMS.MERGE", "CMS.INFO" };
    for (cms_commands) |cmd| {
        const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", cmd });
        defer allocator.free(result);
        try testing.expect(!std.mem.eql(u8, result, "*1\r\n$-1\r\n"));
        try testing.expect(result[0] == '*');
    }
}

test "iter364 - COMMAND INFO for TOPK commands returns valid info" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const topk_commands = [_][]const u8{ "TOPK.RESERVE", "TOPK.ADD", "TOPK.INCRBY", "TOPK.QUERY", "TOPK.COUNT", "TOPK.LIST", "TOPK.INFO" };
    for (topk_commands) |cmd| {
        const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", cmd });
        defer allocator.free(result);
        try testing.expect(!std.mem.eql(u8, result, "*1\r\n$-1\r\n"));
        try testing.expect(result[0] == '*');
    }
}

test "iter364 - COMMAND INFO for TDIGEST commands returns valid info" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const td_commands = [_][]const u8{ "TDIGEST.CREATE", "TDIGEST.ADD", "TDIGEST.RESET", "TDIGEST.MERGE", "TDIGEST.QUANTILE", "TDIGEST.CDF", "TDIGEST.MIN", "TDIGEST.MAX", "TDIGEST.RANK", "TDIGEST.REVRANK", "TDIGEST.BYRANK", "TDIGEST.BYREVRANK", "TDIGEST.INFO", "TDIGEST.TRIMMED_MEAN" };
    for (td_commands) |cmd| {
        const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", cmd });
        defer allocator.free(result);
        try testing.expect(!std.mem.eql(u8, result, "*1\r\n$-1\r\n"));
        try testing.expect(result[0] == '*');
    }
}

test "iter364 - COMMAND INFO for JSON commands returns valid info" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const json_commands = [_][]const u8{ "JSON.SET", "JSON.GET", "JSON.DEL", "JSON.FORGET", "JSON.TYPE", "JSON.MGET", "JSON.MSET", "JSON.NUMINCRBY", "JSON.NUMMULTBY", "JSON.STRAPPEND", "JSON.STRLEN", "JSON.TOGGLE", "JSON.CLEAR", "JSON.ARRAPPEND", "JSON.ARRINDEX", "JSON.ARRINSERT", "JSON.ARRLEN", "JSON.ARRPOP", "JSON.ARRTRIM", "JSON.OBJKEYS", "JSON.OBJLEN", "JSON.RESP", "JSON.MERGE", "JSON.DEBUG" };
    for (json_commands) |cmd| {
        const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", cmd });
        defer allocator.free(result);
        try testing.expect(!std.mem.eql(u8, result, "*1\r\n$-1\r\n"));
        try testing.expect(result[0] == '*');
    }
}

test "iter364 - COMMAND INFO for FT commands returns valid info" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const ft_commands = [_][]const u8{ "FT.CREATE", "FT._LIST", "FT.DROPINDEX", "FT.INFO", "FT.ALTER", "FT.SEARCH", "FT.AGGREGATE", "FT.EXPLAIN", "FT.EXPLAINCLI", "FT.PROFILE", "FT.SPELLCHECK", "FT.CURSOR", "FT.ALIASADD", "FT.ALIASDEL", "FT.ALIASUPDATE", "FT.DICTADD", "FT.DICTDEL", "FT.DICTDUMP", "FT.SYNDUMP", "FT.SYNUPDATE", "FT.SUGADD", "FT.SUGGET", "FT.SUGLEN", "FT.SUGDEL", "FT.TAGVALS", "FT.CONFIG", "FT.HYBRID" };
    for (ft_commands) |cmd| {
        const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", cmd });
        defer allocator.free(result);
        try testing.expect(!std.mem.eql(u8, result, "*1\r\n$-1\r\n"));
        try testing.expect(result[0] == '*');
    }
}

test "iter364 - COMMAND INFO for TS commands returns valid info" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const ts_commands = [_][]const u8{ "TS.CREATE", "TS.ALTER", "TS.ADD", "TS.MADD", "TS.INCRBY", "TS.DECRBY", "TS.DEL", "TS.GET", "TS.INFO", "TS.MGET", "TS.RANGE", "TS.REVRANGE", "TS.MRANGE", "TS.MREVRANGE", "TS.QUERYINDEX", "TS.CREATERULE", "TS.DELETERULE" };
    for (ts_commands) |cmd| {
        const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", cmd });
        defer allocator.free(result);
        try testing.expect(!std.mem.eql(u8, result, "*1\r\n$-1\r\n"));
        try testing.expect(result[0] == '*');
    }
}

test "iter364 - BF.ADD arity is 3 (exact)" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", "BF.ADD" });
    defer allocator.free(result);
    // Arity 3 appears in the response
    try testing.expect(std.mem.indexOf(u8, result, ":3\r\n") != null);
}

test "iter364 - JSON.SET arity is -4 (minimum 4)" {
    const allocator = testing.allocator;
    var s = try setup(allocator);
    defer s.storage.deinit();
    defer s.registry.deinit();
    defer s.ps.deinit();

    const result = try execCmd(allocator, s.storage, &s.registry, s.client_id, &s.ps, &.{ "COMMAND", "INFO", "JSON.SET" });
    defer allocator.free(result);
    // Arity -4 appears in the response
    try testing.expect(std.mem.indexOf(u8, result, ":-4\r\n") != null);
}
