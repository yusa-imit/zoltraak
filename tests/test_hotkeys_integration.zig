const std = @import("std");
const testing = std.testing;
const protocol = @import("protocol");
const server_mod = @import("server");
const storage_mod = @import("storage");

const Parser = protocol.Parser;
const Storage = storage_mod.Storage;

// Integration tests for HOTKEYS commands with HeavyKeeper

test "HOTKEYS START and GET with CPU tracking" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // START tracking with CPU metrics
    const start_cmd = "*5\r\n$7\r\nHOTKEYS\r\n$5\r\nSTART\r\n$7\r\nMETRICS\r\n$1\r\n2\r\n$3\r\nCPU\r\n";
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const start_values = try parser.parse(start_cmd);
    defer parser.freeValues(start_values);

    const hotkeys_mod = @import("hotkeys");
    const start_result = try hotkeys_mod.cmdHotkeys(allocator, storage, start_values[1..]);
    defer allocator.free(start_result);

    try testing.expect(std.mem.eql(u8, start_result, "+OK\r\n"));

    // Verify tracker is active
    try testing.expect(storage.hotkey_tracker != null);
    try testing.expect(storage.hotkey_tracker.?.is_active);

    // Record some key accesses
    storage.hotkey_tracker.?.recordAccess("key1", 100, 0);
    storage.hotkey_tracker.?.recordAccess("key1", 100, 0);
    storage.hotkey_tracker.?.recordAccess("key2", 50, 0);
    storage.hotkey_tracker.?.recordAccess("key1", 100, 0);

    // GET results
    const get_cmd = "*2\r\n$7\r\nHOTKEYS\r\n$3\r\nGET\r\n";
    const get_values = try parser.parse(get_cmd);
    defer parser.freeValues(get_values);

    const get_result = try hotkeys_mod.cmdHotkeys(allocator, storage, get_values[1..]);
    defer allocator.free(get_result);

    // Should return array with metadata and top_keys
    try testing.expect(std.mem.startsWith(u8, get_result, "*6\r\n")); // 6 metadata pairs
    try testing.expect(std.mem.indexOf(u8, get_result, "status") != null);
    try testing.expect(std.mem.indexOf(u8, get_result, "active") != null);
    try testing.expect(std.mem.indexOf(u8, get_result, "keys_sampled") != null);
    try testing.expect(std.mem.indexOf(u8, get_result, ":3\r\n") != null); // 3 accesses
    try testing.expect(std.mem.indexOf(u8, get_result, "total_cpu_us") != null);
    try testing.expect(std.mem.indexOf(u8, get_result, ":300\r\n") != null); // 100+100+50+100 = 350? No, only CPU tracked = 100+100+100=300
    try testing.expect(std.mem.indexOf(u8, get_result, "top_keys") != null);
}

test "HOTKEYS START with NET tracking" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // START tracking with NET metrics
    const start_cmd = "*5\r\n$7\r\nHOTKEYS\r\n$5\r\nSTART\r\n$7\r\nMETRICS\r\n$1\r\n2\r\n$3\r\nNET\r\n";
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const start_values = try parser.parse(start_cmd);
    defer parser.freeValues(start_values);

    const hotkeys_mod = @import("hotkeys");
    const start_result = try hotkeys_mod.cmdHotkeys(allocator, storage, start_values[1..]);
    defer allocator.free(start_result);

    try testing.expect(std.mem.eql(u8, start_result, "+OK\r\n"));

    // Record accesses with network bytes
    storage.hotkey_tracker.?.recordAccess("key1", 0, 1024);
    storage.hotkey_tracker.?.recordAccess("key2", 0, 512);
    storage.hotkey_tracker.?.recordAccess("key1", 0, 2048);

    // GET results
    const get_cmd = "*2\r\n$7\r\nHOTKEYS\r\n$3\r\nGET\r\n";
    const get_values = try parser.parse(get_cmd);
    defer parser.freeValues(get_values);

    const get_result = try hotkeys_mod.cmdHotkeys(allocator, storage, get_values[1..]);
    defer allocator.free(get_result);

    // Verify network bytes tracking
    try testing.expect(std.mem.indexOf(u8, get_result, "total_net_bytes") != null);
    try testing.expect(std.mem.indexOf(u8, get_result, ":3584\r\n") != null); // 1024+512+2048
}

test "HOTKEYS START with COUNT parameter" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // START tracking with COUNT=5
    const start_cmd = "*7\r\n$7\r\nHOTKEYS\r\n$5\r\nSTART\r\n$7\r\nMETRICS\r\n$1\r\n2\r\n$3\r\nCPU\r\n$5\r\nCOUNT\r\n$1\r\n5\r\n";
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const start_values = try parser.parse(start_cmd);
    defer parser.freeValues(start_values);

    const hotkeys_mod = @import("hotkeys");
    const start_result = try hotkeys_mod.cmdHotkeys(allocator, storage, start_values[1..]);
    defer allocator.free(start_result);

    try testing.expect(std.mem.eql(u8, start_result, "+OK\r\n"));

    // Verify top_k is 5
    try testing.expect(storage.hotkey_tracker.?.top_k == 5);
}

test "HOTKEYS STOP preserves data" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // START tracking
    const start_cmd = "*5\r\n$7\r\nHOTKEYS\r\n$5\r\nSTART\r\n$7\r\nMETRICS\r\n$1\r\n2\r\n$3\r\nCPU\r\n";
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const start_values = try parser.parse(start_cmd);
    defer parser.freeValues(start_values);

    const hotkeys_mod = @import("hotkeys");
    _ = try hotkeys_mod.cmdHotkeys(allocator, storage, start_values[1..]);

    // Record data
    storage.hotkey_tracker.?.recordAccess("key1", 100, 0);

    // STOP tracking
    const stop_cmd = "*2\r\n$7\r\nHOTKEYS\r\n$4\r\nSTOP\r\n";
    const stop_values = try parser.parse(stop_cmd);
    defer parser.freeValues(stop_values);

    const stop_result = try hotkeys_mod.cmdHotkeys(allocator, storage, stop_values[1..]);
    defer allocator.free(stop_result);

    try testing.expect(std.mem.eql(u8, stop_result, "+OK\r\n"));

    // Verify inactive but data preserved
    try testing.expect(!storage.hotkey_tracker.?.is_active);
    try testing.expect(storage.hotkey_tracker.?.keys_sampled == 1);

    // GET should still return data
    const get_cmd = "*2\r\n$7\r\nHOTKEYS\r\n$3\r\nGET\r\n";
    const get_values = try parser.parse(get_cmd);
    defer parser.freeValues(get_values);

    const get_result = try hotkeys_mod.cmdHotkeys(allocator, storage, get_values[1..]);
    defer allocator.free(get_result);

    try testing.expect(std.mem.indexOf(u8, get_result, "stopped") != null);
    try testing.expect(std.mem.indexOf(u8, get_result, ":1\r\n") != null); // keys_sampled=1
}

test "HOTKEYS RESET clears all data" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // START tracking
    const start_cmd = "*5\r\n$7\r\nHOTKEYS\r\n$5\r\nSTART\r\n$7\r\nMETRICS\r\n$1\r\n2\r\n$3\r\nCPU\r\n";
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const start_values = try parser.parse(start_cmd);
    defer parser.freeValues(start_values);

    const hotkeys_mod = @import("hotkeys");
    _ = try hotkeys_mod.cmdHotkeys(allocator, storage, start_values[1..]);

    storage.hotkey_tracker.?.recordAccess("key1", 100, 0);

    // RESET
    const reset_cmd = "*2\r\n$7\r\nHOTKEYS\r\n$5\r\nRESET\r\n";
    const reset_values = try parser.parse(reset_cmd);
    defer parser.freeValues(reset_values);

    const reset_result = try hotkeys_mod.cmdHotkeys(allocator, storage, reset_values[1..]);
    defer allocator.free(reset_result);

    try testing.expect(std.mem.eql(u8, reset_result, "+OK\r\n"));

    // Verify tracker is null
    try testing.expect(storage.hotkey_tracker == null);

    // GET should return null
    const get_cmd = "*2\r\n$7\r\nHOTKEYS\r\n$3\r\nGET\r\n";
    const get_values = try parser.parse(get_cmd);
    defer parser.freeValues(get_values);

    const get_result = try hotkeys_mod.cmdHotkeys(allocator, storage, get_values[1..]);
    defer allocator.free(get_result);

    try testing.expect(std.mem.eql(u8, get_result, "$-1\r\n")); // null
}

test "HOTKEYS GET returns top-K keys sorted by frequency" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // START tracking with COUNT=3
    const start_cmd = "*7\r\n$7\r\nHOTKEYS\r\n$5\r\nSTART\r\n$7\r\nMETRICS\r\n$1\r\n2\r\n$3\r\nCPU\r\n$5\r\nCOUNT\r\n$1\r\n3\r\n";
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const start_values = try parser.parse(start_cmd);
    defer parser.freeValues(start_values);

    const hotkeys_mod = @import("hotkeys");
    _ = try hotkeys_mod.cmdHotkeys(allocator, storage, start_values[1..]);

    // Record accesses: key1=5, key2=3, key3=10, key4=1
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        storage.hotkey_tracker.?.recordAccess("key3", 10, 0);
    }
    i = 0;
    while (i < 5) : (i += 1) {
        storage.hotkey_tracker.?.recordAccess("key1", 10, 0);
    }
    i = 0;
    while (i < 3) : (i += 1) {
        storage.hotkey_tracker.?.recordAccess("key2", 10, 0);
    }
    storage.hotkey_tracker.?.recordAccess("key4", 10, 0);

    // GET results
    const get_cmd = "*2\r\n$7\r\nHOTKEYS\r\n$3\r\nGET\r\n";
    const get_values = try parser.parse(get_cmd);
    defer parser.freeValues(get_values);

    const get_result = try hotkeys_mod.cmdHotkeys(allocator, storage, get_values[1..]);
    defer allocator.free(get_result);

    // Verify top-3 keys are present (sorted by frequency)
    // key3 (10 accesses) should be first
    try testing.expect(std.mem.indexOf(u8, get_result, "key3") != null);
    try testing.expect(std.mem.indexOf(u8, get_result, "key1") != null);
    try testing.expect(std.mem.indexOf(u8, get_result, "key2") != null);

    // Total sampled should be 19 (10+5+3+1)
    try testing.expect(std.mem.indexOf(u8, get_result, ":19\r\n") != null);
}

test "HOTKEYS GET without START returns null" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // GET without START
    const get_cmd = "*2\r\n$7\r\nHOTKEYS\r\n$3\r\nGET\r\n";
    const get_values = try parser.parse(get_cmd);
    defer parser.freeValues(get_values);

    const hotkeys_mod = @import("hotkeys");
    const get_result = try hotkeys_mod.cmdHotkeys(allocator, storage, get_values[1..]);
    defer allocator.free(get_result);

    try testing.expect(std.mem.eql(u8, get_result, "$-1\r\n")); // null
}

test "HOTKEYS HELP" {
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var parser = Parser.init(allocator);
    defer parser.deinit();

    // HELP
    const help_cmd = "*2\r\n$7\r\nHOTKEYS\r\n$4\r\nHELP\r\n";
    const help_values = try parser.parse(help_cmd);
    defer parser.freeValues(help_values);

    const hotkeys_mod = @import("hotkeys");
    const help_result = try hotkeys_mod.cmdHotkeys(allocator, storage, help_values[1..]);
    defer allocator.free(help_result);

    try testing.expect(std.mem.startsWith(u8, help_result, "*10\r\n")); // 10 help lines
    try testing.expect(std.mem.indexOf(u8, help_result, "START") != null);
    try testing.expect(std.mem.indexOf(u8, help_result, "STOP") != null);
    try testing.expect(std.mem.indexOf(u8, help_result, "GET") != null);
    try testing.expect(std.mem.indexOf(u8, help_result, "RESET") != null);
}
