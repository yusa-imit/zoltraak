// Iteration 387: RESP3 set type for KEYS + RESP3 double type for GEORADIUS/GEOSEARCH WITHDIST and WITHCOORD
//
// KEYS in RESP3: returns ~N set type (keys are always unique → set semantics)
// RESP2: *N array type (unchanged)
//
// GEORADIUS/GEORADIUSBYMEMBER/GEOSEARCH WITHDIST in RESP3: distance uses ,val\r\n (double type)
// RESP2: distance uses $N\r\nval\r\n (bulk string, unchanged)
//
// GEORADIUS/GEORADIUSBYMEMBER/GEOSEARCH WITHCOORD in RESP3: lon/lat use ,val\r\n (double type)
// RESP2: lon/lat use $N\r\nval\r\n (bulk string, unchanged)

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

// ─── KEYS RESP2 array ────────────────────────────────────────────────────────

test "iter387 - KEYS RESP2 returns plain array *N" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8700");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "k1", "v1" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "k2", "v2" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "KEYS", "*" });
    defer allocator.free(result);

    // RESP2: plain array prefix
    try testing.expect(std.mem.startsWith(u8, result, "*2\r\n"));
    // Must NOT be set type
    try testing.expect(!std.mem.startsWith(u8, result, "~"));
}

// ─── KEYS RESP3 set type ─────────────────────────────────────────────────────

test "iter387 - KEYS RESP3 returns set type ~N" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8701");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Negotiate RESP3
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "alpha", "1" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "beta", "2" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "gamma", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "KEYS", "*" });
    defer allocator.free(result);

    // RESP3: set type prefix ~3
    try testing.expect(std.mem.startsWith(u8, result, "~3\r\n"));
    // Must NOT be array type
    try testing.expect(!std.mem.startsWith(u8, result, "*"));
}

// ─── KEYS RESP3 empty keyspace ───────────────────────────────────────────────

test "iter387 - KEYS RESP3 empty keyspace returns ~0" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8702");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "KEYS", "*" });
    defer allocator.free(result);

    try testing.expectEqualStrings("~0\r\n", result);
}

// ─── KEYS RESP3 pattern filtering ────────────────────────────────────────────

test "iter387 - KEYS RESP3 pattern filtered set" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8703");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "user:1", "a" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "user:2", "b" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "SET", "other", "c" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "KEYS", "user:*" });
    defer allocator.free(result);

    // Only the 2 user:* keys matched → ~2
    try testing.expect(std.mem.startsWith(u8, result, "~2\r\n"));
    try testing.expect(std.mem.indexOf(u8, result, "user:") != null);
    try testing.expect(std.mem.indexOf(u8, result, "other") == null);
}

// ─── GEORADIUS WITHDIST RESP2 bulk string ────────────────────────────────────

test "iter387 - GEORADIUS WITHDIST RESP2 returns bulk string distance" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8704");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    // Add Palermo (lon 13.361389, lat 38.115556)
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "GEOADD", "mygeo", "13.361389", "38.115556", "Palermo" }));

    // RESP2 by default: distance is a bulk string $N
    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "GEORADIUS", "mygeo", "15", "37", "200", "km", "WITHDIST" });
    defer allocator.free(result);

    // Should contain Palermo as bulk string member
    try testing.expect(std.mem.indexOf(u8, result, "Palermo") != null);
    // Distance should be bulk string format $N\r\n
    try testing.expect(std.mem.indexOf(u8, result, "$") != null);
    // Must NOT use double type prefix ,
    // (there should be no comma-prefixed double since this is RESP2)
    try testing.expect(std.mem.indexOf(u8, result, ",1") == null);
}

// ─── GEORADIUS WITHDIST RESP3 double type ────────────────────────────────────

test "iter387 - GEORADIUS WITHDIST RESP3 returns double type distance" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8705");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "GEOADD", "mygeo", "13.361389", "38.115556", "Palermo" }));

    // RESP3: distance should use ,val\r\n double type
    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "GEORADIUS", "mygeo", "15", "37", "200", "km", "WITHDIST" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "Palermo") != null);
    // Distance should start with comma prefix (double type)
    try testing.expect(std.mem.indexOf(u8, result, ",") != null);
}

// ─── GEORADIUS WITHCOORD RESP3 double type ───────────────────────────────────

test "iter387 - GEORADIUS WITHCOORD RESP3 returns double type coordinates" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8706");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "GEOADD", "mygeo", "13.361389", "38.115556", "Palermo" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "GEORADIUS", "mygeo", "15", "37", "200", "km", "WITHCOORD" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "Palermo") != null);
    // Coordinates array *2\r\n should be present
    try testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
    // Coordinate values use double type ','
    try testing.expect(std.mem.indexOf(u8, result, ",") != null);
}

// ─── GEOSEARCH WITHDIST RESP3 double type ────────────────────────────────────

test "iter387 - GEOSEARCH WITHDIST RESP3 returns double type distance" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8707");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "GEOADD", "geo2", "13.361389", "38.115556", "Palermo" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "GEOADD", "geo2", "15.087269", "37.502669", "Catania" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "GEOSEARCH", "geo2", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC", "WITHDIST" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "Catania") != null);
    // Distance uses double type in RESP3
    try testing.expect(std.mem.indexOf(u8, result, ",") != null);
}

// ─── GEOSEARCH WITHCOORD RESP3 double type ───────────────────────────────────

test "iter387 - GEOSEARCH WITHCOORD RESP3 returns double type coordinates" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8708");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "GEOADD", "geo3", "13.361389", "38.115556", "Palermo" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "GEOSEARCH", "geo3", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC", "WITHCOORD" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "Palermo") != null);
    // Coord array present
    try testing.expect(std.mem.indexOf(u8, result, "*2\r\n") != null);
    // Coordinate values use double type
    try testing.expect(std.mem.indexOf(u8, result, ",") != null);
}

// ─── GEORADIUSBYMEMBER WITHDIST RESP3 double type ────────────────────────────

test "iter387 - GEORADIUSBYMEMBER WITHDIST RESP3 returns double type distance" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8709");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "HELLO", "3" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "GEOADD", "geo4", "13.361389", "38.115556", "Palermo" }));
    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "GEOADD", "geo4", "15.087269", "37.502669", "Catania" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "GEORADIUSBYMEMBER", "geo4", "Palermo", "300", "km", "WITHDIST", "ASC" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "Palermo") != null);
    try testing.expect(std.mem.indexOf(u8, result, "Catania") != null);
    // RESP3 double type for distances
    try testing.expect(std.mem.indexOf(u8, result, ",") != null);
}

// ─── GEORADIUS RESP2 WITHCOORD bulk string (regression) ─────────────────────

test "iter387 - GEORADIUS WITHCOORD RESP2 still uses bulk string coordinates" {
    const allocator = testing.allocator;
    var ctx = try setup(allocator, "8710");
    defer ctx.storage.deinit();
    defer ctx.registry.deinit();
    defer ctx.ps.deinit();

    allocator.free(try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "GEOADD", "geo5", "13.361389", "38.115556", "Palermo" }));

    const result = try execCmd(allocator, ctx.storage, &ctx.registry, ctx.client_id, &ctx.ps, &.{ "GEORADIUS", "geo5", "15", "37", "200", "km", "WITHCOORD" });
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "Palermo") != null);
    // RESP2: coordinates are bulk strings — should see $ prefix
    try testing.expect(std.mem.indexOf(u8, result, "$") != null);
}
