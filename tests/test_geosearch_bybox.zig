const std = @import("std");
const testing = std.testing;

const protocol = @import("../src/protocol/parser.zig");
const writer_mod = @import("../src/protocol/writer.zig");
const storage_mod = @import("../src/storage/memory.zig");
const geo_cmds = @import("../src/commands/geo.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;

fn parseResp(allocator: std.mem.Allocator, data: []const u8) ![]RespValue {
    var parser = protocol.RespParser.init(allocator);
    return try parser.parse(data);
}

test "GEOSEARCH BYBOX" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Add some locations to a sorted set
    const geoadd_cmd = "*8\r\n$6\r\nGEOADD\r\n$6\r\nSicily\r\n$2\r\n15\r\n$2\r\n37\r\n$7\r\nPalermo\r\n$2\r\n13\r\n$2\r\n38\r\n$7\r\nCatania\r\n";
    const geoadd_args = try parseResp(allocator, geoadd_cmd);
    defer {
        for (geoadd_args) |arg| {
            arg.deinit(allocator);
        }
        allocator.free(geoadd_args);
    }
    const geoadd_result = try geo_cmds.cmdGeoadd(allocator, &storage, geoadd_args);
    defer allocator.free(geoadd_result);

    // Test GEOSEARCH with BYBOX
    const geosearch_cmd = "*9\r\n$9\r\nGEOSEARCH\r\n$6\r\nSicily\r\n$11\r\nFROMLONLAT\r\n$2\r\n15\r\n$2\r\n37\r\n$5\r\nBYBOX\r\n$3\r\n400\r\n$3\r\n400\r\n$2\r\nkm\r\n";
    const geosearch_args = try parseResp(allocator, geosearch_cmd);
    defer {
        for (geosearch_args) |arg| {
            arg.deinit(allocator);
        }
        allocator.free(geosearch_args);
    }
    const result = try geo_cmds.cmdGeosearch(allocator, &storage, geosearch_args);
    defer allocator.free(result);

    // Should find both Palermo and Catania
    try testing.expect(std.mem.indexOf(u8, result, "Palermo") != null);
    try testing.expect(std.mem.indexOf(u8, result, "Catania") != null);
}

test "GEOSEARCHSTORE with BYBOX" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Add some locations
    const geoadd_cmd = "*8\r\n$6\r\nGEOADD\r\n$6\r\nSicily\r\n$2\r\n15\r\n$2\r\n37\r\n$7\r\nPalermo\r\n$2\r\n13\r\n$2\r\n38\r\n$7\r\nCatania\r\n";
    const geoadd_args = try parseResp(allocator, geoadd_cmd);
    defer {
        for (geoadd_args) |arg| {
            arg.deinit(allocator);
        }
        allocator.free(geoadd_args);
    }
    const geoadd_result = try geo_cmds.cmdGeoadd(allocator, &storage, geoadd_args);
    defer allocator.free(geoadd_result);

    // Test GEOSEARCHSTORE
    const geosearchstore_cmd = "*10\r\n$14\r\nGEOSEARCHSTORE\r\n$6\r\nresult\r\n$6\r\nSicily\r\n$11\r\nFROMLONLAT\r\n$2\r\n15\r\n$2\r\n37\r\n$5\r\nBYBOX\r\n$3\r\n400\r\n$3\r\n400\r\n$2\r\nkm\r\n";
    const geosearchstore_args = try parseResp(allocator, geosearchstore_cmd);
    defer {
        for (geosearchstore_args) |arg| {
            arg.deinit(allocator);
        }
        allocator.free(geosearchstore_args);
    }
    const result = try geo_cmds.cmdGeosearchstore(allocator, &storage, geosearchstore_args);
    defer allocator.free(result);

    // Should return count of 2
    try testing.expectEqualStrings(":2\r\n", result);

    // Verify the result set exists
    const count = storage.zcard("result");
    try testing.expectEqual(@as(i64, 2), count);
}

test "GEOSEARCHSTORE with STOREDIST" {
    const allocator = testing.allocator;
    var storage = Storage.init(allocator);
    defer storage.deinit();

    // Add some locations
    const geoadd_cmd = "*8\r\n$6\r\nGEOADD\r\n$6\r\nSicily\r\n$2\r\n15\r\n$2\r\n37\r\n$7\r\nPalermo\r\n$2\r\n13\r\n$2\r\n38\r\n$7\r\nCatania\r\n";
    const geoadd_args = try parseResp(allocator, geoadd_cmd);
    defer {
        for (geoadd_args) |arg| {
            arg.deinit(allocator);
        }
        allocator.free(geoadd_args);
    }
    const geoadd_result = try geo_cmds.cmdGeoadd(allocator, &storage, geoadd_args);
    defer allocator.free(geoadd_result);

    // Test GEOSEARCHSTORE with STOREDIST
    const geosearchstore_cmd = "*11\r\n$14\r\nGEOSEARCHSTORE\r\n$6\r\nresult\r\n$6\r\nSicily\r\n$11\r\nFROMLONLAT\r\n$2\r\n15\r\n$2\r\n37\r\n$5\r\nBYBOX\r\n$3\r\n400\r\n$3\r\n400\r\n$2\r\nkm\r\n$9\r\nSTOREDIST\r\n";
    const geosearchstore_args = try parseResp(allocator, geosearchstore_cmd);
    defer {
        for (geosearchstore_args) |arg| {
            arg.deinit(allocator);
        }
        allocator.free(geosearchstore_args);
    }
    const result = try geo_cmds.cmdGeosearchstore(allocator, &storage, geosearchstore_args);
    defer allocator.free(result);

    // Should return count of 2
    try testing.expectEqualStrings(":2\r\n", result);

    // Verify scores are distances (not geohashes)
    const palermo_score = storage.zscore("result", "Palermo");
    try testing.expect(palermo_score != null);
    // Distance should be reasonable (not a huge geohash value)
    try testing.expect(palermo_score.? < 300.0); // Less than 300 km
}
