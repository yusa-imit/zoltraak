// Iteration 395: RESP3 double type for GEOPOS coordinates + sailor v2.73.0
//
// Redis protocol behavior:
// - GEOPOS RESP2: coordinates as bulk strings ($N\r\nval\r\n); missing member as null array (*-1\r\n)
// - GEOPOS RESP3: coordinates as native double type (,val\r\n); missing member as null (_\r\n)
//
// sailor v2.73.0: BubbleChart widget added (library update, no API changes)

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
    for (args, 0..) |a, i| resp_args[i] = .{ .bulk_string = a };
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

fn setup(allocator: std.mem.Allocator, port_str: []const u8) !struct {
    storage: *Storage,
    registry: ClientRegistry,
    ps: PubSub,
    client_id: u64,
} {
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    var registry = ClientRegistry.init(allocator);
    const client_id = try registry.registerClient(port_str, 10, "127.0.0.1:6379");
    const ps = PubSub.init(allocator);
    return .{ .storage = storage, .registry = registry, .ps = ps, .client_id = client_id };
}

// ─── GEOPOS RESP2 ──────────────────────────────────────────────────────────────

test "iter395 - GEOPOS RESP2 returns bulk strings for coordinates" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9820");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Add a location (Palermo, Sicily)
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOADD", "mygeo", "13.361389", "38.115556", "Palermo",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOPOS", "mygeo", "Palermo",
    });
    defer allocator.free(resp);

    // RESP2: outer *1\r\n, inner *2\r\n, then two bulk strings
    try testing.expect(resp[0] == '*');
    // Contains bulk string marker ($)
    try testing.expect(std.mem.indexOf(u8, resp, "$") != null);
    // Does NOT contain RESP3 double marker (,)
    try testing.expect(std.mem.indexOf(u8, resp, ",13.") == null);
    try testing.expect(std.mem.indexOf(u8, resp, ",38.") == null);
    // Contains coordinate values
    try testing.expect(std.mem.indexOf(u8, resp, "13.") != null);
    try testing.expect(std.mem.indexOf(u8, resp, "38.") != null);
}

test "iter395 - GEOPOS RESP2 missing member returns null array (*-1)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9821");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOPOS", "mygeo", "NoSuchMember",
    });
    defer allocator.free(resp);

    // RESP2: outer *1\r\n, inner *-1\r\n (null array)
    try testing.expect(std.mem.indexOf(u8, resp, "*-1\r\n") != null);
    // Must NOT contain RESP3 null (_\r\n)
    try testing.expect(std.mem.indexOf(u8, resp, "_\r\n") == null);
}

// ─── GEOPOS RESP3 ──────────────────────────────────────────────────────────────

test "iter395 - GEOPOS RESP3 returns doubles for coordinates" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9822");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOADD", "mygeo", "13.361389", "38.115556", "Palermo",
    }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOPOS", "mygeo", "Palermo",
    });
    defer allocator.free(resp);

    // RESP3: outer *1\r\n (array), inner *2\r\n, then two doubles (,val\r\n)
    try testing.expect(resp[0] == '*');
    // Contains RESP3 double markers: ,13. and ,38.
    try testing.expect(std.mem.indexOf(u8, resp, ",13.") != null);
    try testing.expect(std.mem.indexOf(u8, resp, ",38.") != null);
    // Must NOT contain bulk string ($) for coordinates
    try testing.expect(std.mem.indexOf(u8, resp, "$13") == null);
    try testing.expect(std.mem.indexOf(u8, resp, "$38") == null);
}

test "iter395 - GEOPOS RESP3 missing member returns null type (_)" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9823");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOPOS", "mygeo", "NoSuchMember",
    });
    defer allocator.free(resp);

    // RESP3: _\r\n (null type) instead of *-1\r\n (null array)
    try testing.expect(std.mem.indexOf(u8, resp, "_\r\n") != null);
    // Must NOT contain RESP2 null array
    try testing.expect(std.mem.indexOf(u8, resp, "*-1\r\n") == null);
}

test "iter395 - GEOPOS RESP3 multiple members mixed present and missing" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9824");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Add one location, query two (one found, one not)
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOADD", "mygeo", "2.349014", "48.864716", "Paris",
    }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOPOS", "mygeo", "Paris", "London",
    });
    defer allocator.free(resp);

    // Outer *2\r\n array
    try testing.expect(std.mem.startsWith(u8, resp, "*2\r\n"));
    // Paris: doubles present
    try testing.expect(std.mem.indexOf(u8, resp, ",2.") != null);
    try testing.expect(std.mem.indexOf(u8, resp, ",48.") != null);
    // London: null type
    try testing.expect(std.mem.indexOf(u8, resp, "_\r\n") != null);
}

test "iter395 - GEOPOS RESP3 two members both present returns two doubles each" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9825");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOADD", "mygeo",
        "13.361389", "38.115556", "Palermo",
        "15.087269", "37.502669", "Catania",
    }));

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOPOS", "mygeo", "Palermo", "Catania",
    });
    defer allocator.free(resp);

    // Outer *2\r\n
    try testing.expect(std.mem.startsWith(u8, resp, "*2\r\n"));
    // Both have doubles - check for ,1 pattern (13, 15) and ,3 or ,4 for latitudes
    try testing.expect(std.mem.indexOf(u8, resp, ",13.") != null);
    try testing.expect(std.mem.indexOf(u8, resp, ",38.") != null);
    try testing.expect(std.mem.indexOf(u8, resp, ",15.") != null);
    try testing.expect(std.mem.indexOf(u8, resp, ",37.") != null);
    // No null type (all found)
    try testing.expect(std.mem.indexOf(u8, resp, "_\r\n") == null);
}

test "iter395 - GEOPOS RESP2 multiple members stays as bulk strings" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9826");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOADD", "mygeo",
        "13.361389", "38.115556", "Palermo",
        "15.087269", "37.502669", "Catania",
    }));

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOPOS", "mygeo", "Palermo", "Catania",
    });
    defer allocator.free(resp);

    // Contains bulk string markers
    try testing.expect(std.mem.indexOf(u8, resp, "$") != null);
    // No RESP3 doubles
    try testing.expect(std.mem.indexOf(u8, resp, ",13.") == null);
    try testing.expect(std.mem.indexOf(u8, resp, ",15.") == null);
    // No null type
    try testing.expect(std.mem.indexOf(u8, resp, "_\r\n") == null);
}

test "iter395 - GEOPOS RESP3 empty key returns all nulls" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9827");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOPOS", "nosuchkey", "member1", "member2",
    });
    defer allocator.free(resp);

    // Outer *2\r\n, both members: null type
    try testing.expect(std.mem.startsWith(u8, resp, "*2\r\n"));
    // Count null types
    var count: usize = 0;
    var it = std.mem.splitSequence(u8, resp, "_\r\n");
    while (it.next()) |_| count += 1;
    // Should have at least 2 _\r\n occurrences
    try testing.expect(count >= 3); // 2 nulls → 3 parts when split
}

test "iter395 - GEOPOS error on wrong number of args" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9828");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOPOS",
    });
    defer allocator.free(resp);

    try testing.expect(std.mem.startsWith(u8, resp, "-ERR"));
}

test "iter395 - GEOPOS RESP3 single member no-members query returns empty outer array" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "9829");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    ctx.registry.setProtocol(ctx.client_id, .RESP3);

    // GEOPOS with key but no members — outer array *0\r\n
    // Note: actually Redis requires at least one member. Let's test with no members arg:
    // "GEOPOS", "mygeo" — this is valid with 0 member requests, returns *0\r\n
    const resp = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{
        "GEOPOS", "mygeo",
    });
    defer allocator.free(resp);

    // 0 members requested → *0\r\n outer array
    try testing.expectEqualStrings("*0\r\n", resp);
}

test "iter395 - sailor v2.73.0 build verification" {
    // Sailor v2.73.0 adds BubbleChart widget (library-level change, no API changes needed).
    const sailor = @import("sailor");
    try testing.expect(@hasDecl(sailor, "tui"));
    try testing.expect(@typeInfo(sailor.tui.widgets.BubbleChart) == .@"struct");
}
