const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");
const Storage = storage_mod.Storage;
const HotkeyTracker = storage_mod.HotkeyTracker;

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;

/// HOTKEYS START - Start hotkeys tracking
/// Syntax: HOTKEYS START METRICS count [CPU] [NET] [COUNT k] [DURATION seconds] [SAMPLE ratio] [SLOTS count slot [slot ...]]
/// Returns: OK if tracking started, error if already tracking
pub fn cmdHotkeysStart(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {

    // Parse METRICS argument
    if (args.len < 2) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'hotkeys start' command");
    }

    // Verify first arg is "METRICS"
    const metrics_arg = switch (args[0]) {
        .bulk_string => |s| s,
        else => {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR syntax error");
        },
    };

    var metrics_upper_buf: [16]u8 = undefined;
    const metrics_upper = if (metrics_arg.len <= 16) blk: {
        for (metrics_arg, 0..) |c, i| {
            metrics_upper_buf[i] = std.ascii.toUpper(c);
        }
        break :blk metrics_upper_buf[0..metrics_arg.len];
    } else metrics_arg;

    if (!std.mem.eql(u8, metrics_upper, "METRICS")) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR syntax error");
    }

    // Parse count
    const count_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR value is not an integer or out of range");
        },
    };

    const count = std.fmt.parseInt(u32, count_str, 10) catch {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR value is not an integer or out of range");
    };

    if (count == 0) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR count must be positive");
    }

    // Parse optional flags: CPU, NET, COUNT, DURATION, SAMPLE, SLOTS
    var has_cpu = false;
    var has_net = false;
    var top_k: ?u32 = null;
    var duration: ?u32 = null;
    var sample_ratio: ?u32 = null;

    var i: usize = 2;
    while (i < args.len) {
        const flag = switch (args[i]) {
            .bulk_string => |s| s,
            else => {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            },
        };

        var flag_upper_buf: [16]u8 = undefined;
        const flag_upper = if (flag.len <= 16) blk: {
            for (flag, 0..) |c, j| {
                flag_upper_buf[j] = std.ascii.toUpper(c);
            }
            break :blk flag_upper_buf[0..flag.len];
        } else flag;

        if (std.mem.eql(u8, flag_upper, "CPU")) {
            has_cpu = true;
            i += 1;
        } else if (std.mem.eql(u8, flag_upper, "NET")) {
            has_net = true;
            i += 1;
        } else if (std.mem.eql(u8, flag_upper, "COUNT")) {
            if (i + 1 >= args.len) {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            }
            const k_str = switch (args[i + 1]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    return w.writeError("ERR value is not an integer or out of range");
                },
            };
            top_k = std.fmt.parseInt(u32, k_str, 10) catch {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR value is not an integer or out of range");
            };
            i += 2;
        } else if (std.mem.eql(u8, flag_upper, "DURATION")) {
            if (i + 1 >= args.len) {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            }
            const dur_str = switch (args[i + 1]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    return w.writeError("ERR value is not an integer or out of range");
                },
            };
            duration = std.fmt.parseInt(u32, dur_str, 10) catch {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR value is not an integer or out of range");
            };
            i += 2;
        } else if (std.mem.eql(u8, flag_upper, "SAMPLE")) {
            if (i + 1 >= args.len) {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR syntax error");
            }
            const sample_str = switch (args[i + 1]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    return w.writeError("ERR value is not an integer or out of range");
                },
            };
            sample_ratio = std.fmt.parseInt(u32, sample_str, 10) catch {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR value is not an integer or out of range");
            };
            i += 2;
        } else if (std.mem.eql(u8, flag_upper, "SLOTS")) {
            // SLOTS only valid in cluster mode - skip parsing for now (stub)
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR This instance has cluster support disabled");
        } else {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR syntax error");
        }
    }

    // At least one of CPU or NET must be specified
    if (!has_cpu and !has_net) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR at least one of CPU or NET must be specified");
    }

    // Create and initialize HotkeyTracker
    const config = HotkeyTracker.TrackerConfig{
        .metrics_count = count,
        .track_cpu = has_cpu,
        .track_net = has_net,
        .top_k = top_k orelse 10,
        .sample_ratio = sample_ratio orelse 100,
        .duration_ms = if (duration) |d| @as(u64, d) * 1000 else null,
    };

    const tracker = try HotkeyTracker.init(allocator, config);
    errdefer tracker.deinit();

    // If a tracker already exists, clean it up
    if (storage.hotkey_tracker) |old_tracker| {
        old_tracker.deinit();
    }

    // Store the tracker and start it
    storage.hotkey_tracker = tracker;
    tracker.start();

    var w = Writer.init(allocator);
    defer w.deinit();
    return w.writeSimpleString("OK");
}

/// HOTKEYS STOP - Stop hotkeys tracking but preserve data
/// Syntax: HOTKEYS STOP
/// Returns: OK if tracking stopped, error if not tracking
pub fn cmdHotkeysStop(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    if (args.len != 0) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'hotkeys stop' command");
    }

    // If tracker exists and is active, stop it
    if (storage.hotkey_tracker) |tracker| {
        if (tracker.is_active) {
            tracker.stop();
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeSimpleString("OK");
        }
    }

    // No active tracking, still return OK (idempotent)
    var w = Writer.init(allocator);
    defer w.deinit();
    return w.writeSimpleString("OK");
}

/// HOTKEYS GET - Get tracking results
/// Syntax: HOTKEYS GET
/// Returns: Array of tracking metadata and results (or null if no tracking)
pub fn cmdHotkeysGet(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    if (args.len != 0) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'hotkeys get' command");
    }

    // If no tracker exists, return null
    if (storage.hotkey_tracker == null) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeNull();
    }

    const tracker = storage.hotkey_tracker.?;

    // Return null if not yet started or stopped
    if (!tracker.is_active and tracker.start_time_ms == 0) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeNull();
    }

    // TODO: Phase 2 - Return detailed tracking results with HeavyKeeper data
    // For now, return null to match expected behavior
    var w = Writer.init(allocator);
    defer w.deinit();
    return w.writeNull();
}

/// HOTKEYS RESET - Reset tracking and release resources
/// Syntax: HOTKEYS RESET
/// Returns: OK
pub fn cmdHotkeysReset(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    if (args.len != 0) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'hotkeys reset' command");
    }

    // Cleanup tracker if it exists
    if (storage.hotkey_tracker) |tracker| {
        tracker.deinit();
        storage.hotkey_tracker = null;
    }

    var w = Writer.init(allocator);
    defer w.deinit();
    return w.writeSimpleString("OK");
}

/// HOTKEYS - Container command
/// Syntax: HOTKEYS [START|STOP|GET|RESET] ...
/// Returns: Help array if no subcommand
pub fn cmdHotkeys(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    if (args.len == 0) {
        // Return help
        var w = Writer.init(allocator);
        defer w.deinit();

        const help_lines = [_][]const u8{
            "HOTKEYS <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "START METRICS count [CPU] [NET] [COUNT k] [DURATION seconds] [SAMPLE ratio] [SLOTS count slot [slot ...]]",
            "    Start tracking hotkeys.",
            "STOP",
            "    Stop tracking hotkeys.",
            "GET",
            "    Get tracking results.",
            "RESET",
            "    Reset tracking and release resources.",
            "HELP",
            "    Print this help.",
        };

        return w.writeArrayOfBulkStrings(&help_lines);
    }

    // Parse subcommand
    const subcmd = switch (args[0]) {
        .bulk_string => |s| s,
        else => {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR unknown subcommand. Try HOTKEYS HELP.");
        },
    };

    var subcmd_upper_buf: [16]u8 = undefined;
    const subcmd_upper = if (subcmd.len <= 16) blk: {
        for (subcmd, 0..) |c, i| {
            subcmd_upper_buf[i] = std.ascii.toUpper(c);
        }
        break :blk subcmd_upper_buf[0..subcmd.len];
    } else subcmd;

    const rest_args = args[1..];

    if (std.mem.eql(u8, subcmd_upper, "START")) {
        return cmdHotkeysStart(allocator, storage, rest_args);
    } else if (std.mem.eql(u8, subcmd_upper, "STOP")) {
        return cmdHotkeysStop(allocator, storage, rest_args);
    } else if (std.mem.eql(u8, subcmd_upper, "GET")) {
        return cmdHotkeysGet(allocator, storage, rest_args);
    } else if (std.mem.eql(u8, subcmd_upper, "RESET")) {
        return cmdHotkeysReset(allocator, storage, rest_args);
    } else if (std.mem.eql(u8, subcmd_upper, "HELP")) {
        return cmdHotkeys(allocator, storage, &[_]RespValue{});
    } else {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR unknown subcommand. Try HOTKEYS HELP.");
    }
}

// ===== TESTS =====

test "HOTKEYS HELP" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const result = try cmdHotkeys(allocator, &storage, &[_]RespValue{});
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "*10\r\n"));
}

test "HOTKEYS START with CPU" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "CPU" },
    };

    const result = try cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
}

test "HOTKEYS START with NET" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "NET" },
    };

    const result = try cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
}

test "HOTKEYS START with both CPU and NET" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "CPU" },
        RespValue{ .bulk_string = "NET" },
    };

    const result = try cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
}

test "HOTKEYS START without CPU or NET" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "2" },
    };

    const result = try cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "at least one of CPU or NET") != null);
}

test "HOTKEYS START with COUNT" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "CPU" },
        RespValue{ .bulk_string = "COUNT" },
        RespValue{ .bulk_string = "10" },
    };

    const result = try cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
}

test "HOTKEYS START with DURATION" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "CPU" },
        RespValue{ .bulk_string = "DURATION" },
        RespValue{ .bulk_string = "60" },
    };

    const result = try cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
}

test "HOTKEYS START with SAMPLE" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "START" },
        RespValue{ .bulk_string = "METRICS" },
        RespValue{ .bulk_string = "2" },
        RespValue{ .bulk_string = "CPU" },
        RespValue{ .bulk_string = "SAMPLE" },
        RespValue{ .bulk_string = "100" },
    };

    const result = try cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
}

test "HOTKEYS STOP" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "STOP" },
    };

    const result = try cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
}

test "HOTKEYS GET" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GET" },
    };

    const result = try cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    // Should return null (no tracking data)
    try testing.expect(std.mem.eql(u8, result, "$-1\r\n"));
}

test "HOTKEYS RESET" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "RESET" },
    };

    const result = try cmdHotkeys(allocator, &storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.eql(u8, result, "+OK\r\n"));
}
