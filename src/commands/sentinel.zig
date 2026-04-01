const std = @import("std");
const storage_mod = @import("../storage/memory.zig");
const writer_mod = @import("../protocol/writer.zig");

const Storage = storage_mod.Storage;
const Writer = writer_mod.Writer;

/// Handle SENTINEL PING command
/// Returns a simple string +PONG\r\n
pub fn cmdSentinelPing(
    allocator: std.mem.Allocator,
    args: []const []const u8,
    storage: *Storage,
    _: ?*anyopaque,
    _: u64,
) ![]const u8 {
    _ = storage;

    // SENTINEL PING takes no additional arguments (only SENTINEL and PING in args[0] and args[1])
    if (args.len > 2) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return try w.writeError("ERR wrong number of arguments for 'sentinel|ping' command");
    }

    var w = Writer.init(allocator);
    defer w.deinit();
    return try w.writeSimpleString("PONG");
}
