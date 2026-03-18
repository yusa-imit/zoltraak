// Integration tests for SCRIPT KILL command
const std = @import("std");
const Storage = @import("../src/storage/memory.zig").Storage;
const ScriptStore = @import("../src/storage/scripting.zig").ScriptStore;
const PubSub = @import("../src/storage/pubsub.zig").PubSub;
const TxState = @import("../src/commands/transactions.zig").TxState;
const ClientRegistry = @import("../src/commands/client.zig").ClientRegistry;
const scripting_cmds = @import("../src/commands/scripting.zig");

test "SCRIPT KILL sets and clears kill flag" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();
    var script_store = ScriptStore.init(allocator);
    defer script_store.deinit();

    // Initially kill flag should be false
    try std.testing.expect(!script_store.isKillRequested());

    // Call SCRIPT KILL
    const response = try scripting_cmds.cmdScriptKill(allocator, &script_store);
    defer allocator.free(response);

    // Should return OK
    try std.testing.expectEqualStrings("+OK\r\n", response);

    // Kill flag should now be true
    try std.testing.expect(script_store.isKillRequested());

    // Clear the flag
    script_store.clearKill();
    try std.testing.expect(!script_store.isKillRequested());
}

test "SCRIPT HELP includes KILL subcommand" {
    const allocator = std.testing.allocator;

    const response = try scripting_cmds.cmdScriptHelp(allocator);
    defer allocator.free(response);

    // Should mention KILL
    try std.testing.expect(std.mem.indexOf(u8, response, "KILL") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "Kill the currently executing script") != null);
}
