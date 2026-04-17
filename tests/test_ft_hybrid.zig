const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const server_mod = @import("../src/server.zig");
const storage_mod = @import("../src/storage/memory.zig");

const Server = server_mod.Server;
const RespValue = protocol.RespValue;

test "FT.HYBRID: basic syntax validation - missing arguments" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var server = try Server.init(allocator, "127.0.0.1", 6379);
    defer server.deinit();

    // Too few arguments
    const cmd1 = try protocol.parseCommand(allocator, "*2\r\n$9\r\nFT.HYBRID\r\n$8\r\nmyindex\r\n");
    const resp1 = try server.handleCommand(allocator, cmd1);
    try std.testing.expect(resp1.error_string != null);
    try std.testing.expect(std.mem.indexOf(u8, resp1.error_string.?, "wrong number of arguments") != null);
}

test "FT.HYBRID: validates SEARCH keyword" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var server = try Server.init(allocator, "127.0.0.1", 6379);
    defer server.deinit();

    // Create index first
    const create_cmd = try protocol.parseCommand(allocator, "*6\r\n$9\r\nFT.CREATE\r\n$8\r\nmyindex\r\n$2\r\nON\r\n$4\r\nHASH\r\n$6\r\nSCHEMA\r\n$5\r\ntitle\r\n$4\r\nTEXT\r\n");
    _ = try server.handleCommand(allocator, create_cmd);

    // Missing SEARCH keyword
    const cmd = try protocol.parseCommand(allocator, "*6\r\n$9\r\nFT.HYBRID\r\n$8\r\nmyindex\r\n$7\r\nINVALID\r\n$5\r\nquery\r\n$4\r\nVSIM\r\n$6\r\n@field\r\n");
    const resp = try server.handleCommand(allocator, cmd);
    try std.testing.expect(resp.error_string != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.error_string.?, "expected SEARCH") != null);
}

test "FT.HYBRID: requires VSIM clause" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var server = try Server.init(allocator, "127.0.0.1", 6379);
    defer server.deinit();

    // Create index
    const create_cmd = try protocol.parseCommand(allocator, "*6\r\n$9\r\nFT.CREATE\r\n$8\r\nmyindex\r\n$2\r\nON\r\n$4\r\nHASH\r\n$6\r\nSCHEMA\r\n$5\r\ntitle\r\n$4\r\nTEXT\r\n");
    _ = try server.handleCommand(allocator, create_cmd);

    // Missing VSIM clause
    const cmd = try protocol.parseCommand(allocator, "*5\r\n$9\r\nFT.HYBRID\r\n$8\r\nmyindex\r\n$6\r\nSEARCH\r\n$6\r\nlaptop\r\n$6\r\nPARAMS\r\n");
    const resp = try server.handleCommand(allocator, cmd);
    try std.testing.expect(resp.error_string != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.error_string.?, "VSIM clause is required") != null);
}

test "FT.HYBRID: validates vector field starts with @" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var server = try Server.init(allocator, "127.0.0.1", 6379);
    defer server.deinit();

    // Create index
    const create_cmd = try protocol.parseCommand(allocator, "*6\r\n$9\r\nFT.CREATE\r\n$8\r\nmyindex\r\n$2\r\nON\r\n$4\r\nHASH\r\n$6\r\nSCHEMA\r\n$5\r\ntitle\r\n$4\r\nTEXT\r\n");
    _ = try server.handleCommand(allocator, create_cmd);

    // Vector field missing @
    const cmd = try protocol.parseCommand(allocator, "*8\r\n$9\r\nFT.HYBRID\r\n$8\r\nmyindex\r\n$6\r\nSEARCH\r\n$6\r\nlaptop\r\n$4\r\nVSIM\r\n$5\r\nfield\r\n$4\r\n$vec\r\n$6\r\nPARAMS\r\n");
    const resp = try server.handleCommand(allocator, cmd);
    try std.testing.expect(resp.error_string != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.error_string.?, "vector field must start with @") != null);
}

test "FT.HYBRID: validates vector parameter starts with $" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var server = try Server.init(allocator, "127.0.0.1", 6379);
    defer server.deinit();

    // Create index
    const create_cmd = try protocol.parseCommand(allocator, "*6\r\n$9\r\nFT.CREATE\r\n$8\r\nmyindex\r\n$2\r\nON\r\n$4\r\nHASH\r\n$6\r\nSCHEMA\r\n$5\r\ntitle\r\n$4\r\nTEXT\r\n");
    _ = try server.handleCommand(allocator, create_cmd);

    // Vector parameter missing $
    const cmd = try protocol.parseCommand(allocator, "*8\r\n$9\r\nFT.HYBRID\r\n$8\r\nmyindex\r\n$6\r\nSEARCH\r\n$6\r\nlaptop\r\n$4\r\nVSIM\r\n$6\r\n@field\r\n$3\r\nvec\r\n$6\r\nPARAMS\r\n");
    const resp = try server.handleCommand(allocator, cmd);
    try std.testing.expect(resp.error_string != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.error_string.?, "vector parameter must start with $") != null);
}

test "FT.HYBRID: requires PARAMS clause" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var server = try Server.init(allocator, "127.0.0.1", 6379);
    defer server.deinit();

    // Create index
    const create_cmd = try protocol.parseCommand(allocator, "*6\r\n$9\r\nFT.CREATE\r\n$8\r\nmyindex\r\n$2\r\nON\r\n$4\r\nHASH\r\n$6\r\nSCHEMA\r\n$5\r\ntitle\r\n$4\r\nTEXT\r\n");
    _ = try server.handleCommand(allocator, create_cmd);

    // Missing PARAMS clause
    const cmd = try protocol.parseCommand(allocator, "*7\r\n$9\r\nFT.HYBRID\r\n$8\r\nmyindex\r\n$6\r\nSEARCH\r\n$6\r\nlaptop\r\n$4\r\nVSIM\r\n$6\r\n@field\r\n$4\r\n$vec\r\n");
    const resp = try server.handleCommand(allocator, cmd);
    try std.testing.expect(resp.error_string != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.error_string.?, "PARAMS clause is required") != null);
}

test "FT.HYBRID: validates PARAMS nargs is integer" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var server = try Server.init(allocator, "127.0.0.1", 6379);
    defer server.deinit();

    // Create index
    const create_cmd = try protocol.parseCommand(allocator, "*6\r\n$9\r\nFT.CREATE\r\n$8\r\nmyindex\r\n$2\r\nON\r\n$4\r\nHASH\r\n$6\r\nSCHEMA\r\n$5\r\ntitle\r\n$4\r\nTEXT\r\n");
    _ = try server.handleCommand(allocator, create_cmd);

    // Invalid nargs
    const cmd = try protocol.parseCommand(allocator, "*9\r\n$9\r\nFT.HYBRID\r\n$8\r\nmyindex\r\n$6\r\nSEARCH\r\n$6\r\nlaptop\r\n$4\r\nVSIM\r\n$6\r\n@field\r\n$4\r\n$vec\r\n$6\r\nPARAMS\r\n$7\r\ninvalid\r\n");
    const resp = try server.handleCommand(allocator, cmd);
    try std.testing.expect(resp.error_string != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.error_string.?, "nargs must be a valid integer") != null);
}

test "FT.HYBRID: validates PARAMS nargs is even" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var server = try Server.init(allocator, "127.0.0.1", 6379);
    defer server.deinit();

    // Create index
    const create_cmd = try protocol.parseCommand(allocator, "*6\r\n$9\r\nFT.CREATE\r\n$8\r\nmyindex\r\n$2\r\nON\r\n$4\r\nHASH\r\n$6\r\nSCHEMA\r\n$5\r\ntitle\r\n$4\r\nTEXT\r\n");
    _ = try server.handleCommand(allocator, create_cmd);

    // Odd nargs
    const cmd = try protocol.parseCommand(allocator, "*11\r\n$9\r\nFT.HYBRID\r\n$8\r\nmyindex\r\n$6\r\nSEARCH\r\n$6\r\nlaptop\r\n$4\r\nVSIM\r\n$6\r\n@field\r\n$4\r\n$vec\r\n$6\r\nPARAMS\r\n$1\r\n3\r\n$3\r\nvec\r\n$4\r\nblob\r\n");
    const resp = try server.handleCommand(allocator, cmd);
    try std.testing.expect(resp.error_string != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.error_string.?, "nargs must be even") != null);
}

test "FT.HYBRID: validates KNN syntax" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var server = try Server.init(allocator, "127.0.0.1", 6379);
    defer server.deinit();

    // Create index
    const create_cmd = try protocol.parseCommand(allocator, "*6\r\n$9\r\nFT.CREATE\r\n$8\r\nmyindex\r\n$2\r\nON\r\n$4\r\nHASH\r\n$6\r\nSCHEMA\r\n$5\r\ntitle\r\n$4\r\nTEXT\r\n");
    _ = try server.handleCommand(allocator, create_cmd);

    // Invalid KNN - missing K keyword
    const cmd = try protocol.parseCommand(allocator, "*14\r\n$9\r\nFT.HYBRID\r\n$8\r\nmyindex\r\n$6\r\nSEARCH\r\n$6\r\nlaptop\r\n$4\r\nVSIM\r\n$6\r\n@field\r\n$4\r\n$vec\r\n$3\r\nKNN\r\n$1\r\n2\r\n$2\r\n10\r\n$6\r\nPARAMS\r\n$1\r\n2\r\n$3\r\nvec\r\n$4\r\nblob\r\n");
    const resp = try server.handleCommand(allocator, cmd);
    try std.testing.expect(resp.error_string != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.error_string.?, "KNN requires K keyword") != null);
}

test "FT.HYBRID: validates RANGE syntax" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var server = try Server.init(allocator, "127.0.0.1", 6379);
    defer server.deinit();

    // Create index
    const create_cmd = try protocol.parseCommand(allocator, "*6\r\n$9\r\nFT.CREATE\r\n$8\r\nmyindex\r\n$2\r\nON\r\n$4\r\nHASH\r\n$6\r\nSCHEMA\r\n$5\r\ntitle\r\n$4\r\nTEXT\r\n");
    _ = try server.handleCommand(allocator, create_cmd);

    // Invalid RANGE - missing RADIUS keyword
    const cmd = try protocol.parseCommand(allocator, "*14\r\n$9\r\nFT.HYBRID\r\n$8\r\nmyindex\r\n$6\r\nSEARCH\r\n$6\r\nlaptop\r\n$4\r\nVSIM\r\n$6\r\n@field\r\n$4\r\n$vec\r\n$5\r\nRANGE\r\n$1\r\n2\r\n$3\r\n0.5\r\n$6\r\nPARAMS\r\n$1\r\n2\r\n$3\r\nvec\r\n$4\r\nblob\r\n");
    const resp = try server.handleCommand(allocator, cmd);
    try std.testing.expect(resp.error_string != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.error_string.?, "RANGE requires RADIUS keyword") != null);
}

test "FT.HYBRID: successful stub execution returns empty results" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var server = try Server.init(allocator, "127.0.0.1", 6379);
    defer server.deinit();

    // Create index
    const create_cmd = try protocol.parseCommand(allocator, "*6\r\n$9\r\nFT.CREATE\r\n$8\r\nmyindex\r\n$2\r\nON\r\n$4\r\nHASH\r\n$6\r\nSCHEMA\r\n$5\r\ntitle\r\n$4\r\nTEXT\r\n");
    _ = try server.handleCommand(allocator, create_cmd);

    // Valid hybrid search (stub)
    const cmd = try protocol.parseCommand(allocator, "*12\r\n$9\r\nFT.HYBRID\r\n$8\r\nmyindex\r\n$6\r\nSEARCH\r\n$6\r\nlaptop\r\n$4\r\nVSIM\r\n$6\r\n@field\r\n$4\r\n$vec\r\n$3\r\nKNN\r\n$1\r\n2\r\n$1\r\nK\r\n$2\r\n10\r\n$6\r\nPARAMS\r\n$1\r\n2\r\n$3\r\nvec\r\n$4\r\nblob\r\n");
    const resp = try server.handleCommand(allocator, cmd);

    // Should return array [0]
    try std.testing.expect(resp.array != null);
    try std.testing.expectEqual(@as(usize, 1), resp.array.?.len);
    try std.testing.expectEqual(@as(i64, 0), resp.array.?[0].integer);
}

test "FT.HYBRID: nonexistent index error" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var server = try Server.init(allocator, "127.0.0.1", 6379);
    defer server.deinit();

    // No index created
    const cmd = try protocol.parseCommand(allocator, "*12\r\n$9\r\nFT.HYBRID\r\n$12\r\nnonexistent\r\n$6\r\nSEARCH\r\n$6\r\nlaptop\r\n$4\r\nVSIM\r\n$6\r\n@field\r\n$4\r\n$vec\r\n$3\r\nKNN\r\n$1\r\n2\r\n$1\r\nK\r\n$2\r\n10\r\n$6\r\nPARAMS\r\n$1\r\n2\r\n$3\r\nvec\r\n$4\r\nblob\r\n");
    const resp = try server.handleCommand(allocator, cmd);
    try std.testing.expect(resp.error_string != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.error_string.?, "no such index") != null);
}

test "FT.HYBRID: works with index alias" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var server = try Server.init(allocator, "127.0.0.1", 6379);
    defer server.deinit();

    // Create index and alias
    const create_cmd = try protocol.parseCommand(allocator, "*6\r\n$9\r\nFT.CREATE\r\n$8\r\nmyindex\r\n$2\r\nON\r\n$4\r\nHASH\r\n$6\r\nSCHEMA\r\n$5\r\ntitle\r\n$4\r\nTEXT\r\n");
    _ = try server.handleCommand(allocator, create_cmd);

    const alias_cmd = try protocol.parseCommand(allocator, "*3\r\n$10\r\nFT.ALIASADD\r\n$7\r\nmyalias\r\n$8\r\nmyindex\r\n");
    _ = try server.handleCommand(allocator, alias_cmd);

    // Use alias in hybrid search
    const cmd = try protocol.parseCommand(allocator, "*12\r\n$9\r\nFT.HYBRID\r\n$7\r\nmyalias\r\n$6\r\nSEARCH\r\n$6\r\nlaptop\r\n$4\r\nVSIM\r\n$6\r\n@field\r\n$4\r\n$vec\r\n$3\r\nKNN\r\n$1\r\n2\r\n$1\r\nK\r\n$2\r\n10\r\n$6\r\nPARAMS\r\n$1\r\n2\r\n$3\r\nvec\r\n$4\r\nblob\r\n");
    const resp = try server.handleCommand(allocator, cmd);

    // Should return array [0] (stub)
    try std.testing.expect(resp.array != null);
    try std.testing.expectEqual(@as(usize, 1), resp.array.?.len);
    try std.testing.expectEqual(@as(i64, 0), resp.array.?[0].integer);
}

test "FT.HYBRID: complex query with multiple options" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var server = try Server.init(allocator, "127.0.0.1", 6379);
    defer server.deinit();

    // Create index
    const create_cmd = try protocol.parseCommand(allocator, "*6\r\n$9\r\nFT.CREATE\r\n$8\r\nmyindex\r\n$2\r\nON\r\n$4\r\nHASH\r\n$6\r\nSCHEMA\r\n$5\r\ntitle\r\n$4\r\nTEXT\r\n");
    _ = try server.handleCommand(allocator, create_cmd);

    // Complex query with SCORER, YIELD_SCORE_AS, KNN, COMBINE, LIMIT, SORTBY, LOAD, TIMEOUT
    const cmd = try protocol.parseCommand(allocator, "*26\r\n$9\r\nFT.HYBRID\r\n$8\r\nmyindex\r\n$6\r\nSEARCH\r\n$6\r\nlaptop\r\n$6\r\nSCORER\r\n$4\r\nBM25\r\n$13\r\nYIELD_SCORE_AS\r\n$10\r\ntext_score\r\n$4\r\nVSIM\r\n$6\r\n@field\r\n$4\r\n$vec\r\n$3\r\nKNN\r\n$1\r\n2\r\n$1\r\nK\r\n$2\r\n20\r\n$7\r\nCOMBINE\r\n$3\r\nRRF\r\n$5\r\nLIMIT\r\n$1\r\n0\r\n$2\r\n10\r\n$6\r\nSORTBY\r\n$5\r\nscore\r\n$4\r\nLOAD\r\n$5\r\ntitle\r\n$7\r\nTIMEOUT\r\n$3\r\n500\r\n$6\r\nPARAMS\r\n$1\r\n2\r\n$3\r\nvec\r\n$4\r\nblob\r\n");
    const resp = try server.handleCommand(allocator, cmd);

    // Should return array [0] (stub accepts all valid options)
    try std.testing.expect(resp.array != null);
    try std.testing.expectEqual(@as(usize, 1), resp.array.?.len);
    try std.testing.expectEqual(@as(i64, 0), resp.array.?[0].integer);
}
