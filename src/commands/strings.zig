const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");

const RespValue = protocol.RespValue;
const RespType = protocol.RespType;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;

/// Execute a RESP command and return the serialized response
/// Caller owns returned memory and must free it
pub fn executeCommand(allocator: std.mem.Allocator, storage: *Storage, cmd: RespValue) ![]const u8 {
    const array = switch (cmd) {
        .array => |arr| arr,
        else => {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR expected array");
        },
    };

    if (array.len == 0) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR empty command");
    }

    const cmd_name = switch (array[0]) {
        .bulk_string => |s| s,
        else => {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR invalid command format");
        },
    };

    // Command dispatch (case-insensitive)
    const cmd_upper = try std.ascii.allocUpperString(allocator, cmd_name);
    defer allocator.free(cmd_upper);

    if (std.mem.eql(u8, cmd_upper, "PING")) {
        return cmdPing(allocator, array);
    } else if (std.mem.eql(u8, cmd_upper, "SET")) {
        return cmdSet(allocator, storage, array);
    } else if (std.mem.eql(u8, cmd_upper, "GET")) {
        return cmdGet(allocator, storage, array);
    } else if (std.mem.eql(u8, cmd_upper, "DEL")) {
        return cmdDel(allocator, storage, array);
    } else if (std.mem.eql(u8, cmd_upper, "EXISTS")) {
        return cmdExists(allocator, storage, array);
    } else {
        var w = Writer.init(allocator);
        defer w.deinit();
        var buf: [256]u8 = undefined;
        const err_msg = try std.fmt.bufPrint(&buf, "ERR unknown command '{s}'", .{cmd_name});
        return w.writeError(err_msg);
    }
}

/// PING [message]
/// Returns PONG or echoes the message
fn cmdPing(allocator: std.mem.Allocator, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len == 1) {
        // No argument: return +PONG\r\n
        return w.writeSimpleString("PONG");
    } else if (args.len == 2) {
        // With argument: return bulk string
        const message = switch (args[1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR wrong number of arguments for 'ping' command"),
        };
        return w.writeBulkString(message);
    } else {
        return w.writeError("ERR wrong number of arguments for 'ping' command");
    }
}

/// SET key value [EX seconds] [PX milliseconds] [NX|XX]
/// Returns +OK or $-1 if condition not met
fn cmdSet(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Minimum: SET key value
    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'set' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const value = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid value"),
    };

    var expires_at: ?i64 = null;
    var nx = false;
    var xx = false;

    // Parse options
    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        const opt_upper = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(opt_upper);

        if (std.mem.eql(u8, opt_upper, "EX")) {
            if (expires_at != null) {
                return w.writeError("ERR syntax error");
            }
            i += 1;
            if (i >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const seconds = parseInteger(args[i]) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            if (seconds <= 0) {
                return w.writeError("ERR invalid expire time in 'set' command");
            }
            expires_at = Storage.getCurrentTimestamp() + (seconds * 1000);
        } else if (std.mem.eql(u8, opt_upper, "PX")) {
            if (expires_at != null) {
                return w.writeError("ERR syntax error");
            }
            i += 1;
            if (i >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const milliseconds = parseInteger(args[i]) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            if (milliseconds <= 0) {
                return w.writeError("ERR invalid expire time in 'set' command");
            }
            expires_at = Storage.getCurrentTimestamp() + milliseconds;
        } else if (std.mem.eql(u8, opt_upper, "NX")) {
            if (xx) {
                return w.writeError("ERR syntax error");
            }
            nx = true;
        } else if (std.mem.eql(u8, opt_upper, "XX")) {
            if (nx) {
                return w.writeError("ERR syntax error");
            }
            xx = true;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Check NX condition
    if (nx and storage.exists(key)) {
        return w.writeNull(); // $-1\r\n
    }

    // Check XX condition
    if (xx and !storage.exists(key)) {
        return w.writeNull(); // $-1\r\n
    }

    // Execute SET
    try storage.set(key, value, expires_at);
    return w.writeOK();
}

/// GET key
/// Returns bulk string value or null
fn cmdGet(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'get' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const value = storage.get(key);
    return w.writeBulkString(value);
}

/// DEL key [key ...]
/// Returns integer count of deleted keys
fn cmdDel(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'del' command");
    }

    // Extract keys
    var keys = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 1);
    defer keys.deinit(allocator);

    for (args[1..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    const deleted_count = storage.del(keys.items);
    return w.writeInteger(@intCast(deleted_count));
}

/// EXISTS key [key ...]
/// Returns integer count of existing keys (duplicates counted multiple times)
fn cmdExists(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'exists' command");
    }

    var count: i64 = 0;
    for (args[1..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        if (storage.exists(key)) {
            count += 1;
        }
    }

    return w.writeInteger(count);
}

// Helper functions

fn parseInteger(value: RespValue) !i64 {
    const str = switch (value) {
        .bulk_string => |s| s,
        else => return error.InvalidInteger,
    };
    return std.fmt.parseInt(i64, str, 10);
}

// Embedded unit tests

test "commands - PING no argument" {
    const allocator = std.testing.allocator;

    const args = [_]RespValue{
        RespValue{ .bulk_string = "PING" },
    };

    const result = try cmdPing(allocator, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+PONG\r\n", result);
}

test "commands - PING with message" {
    const allocator = std.testing.allocator;

    const args = [_]RespValue{
        RespValue{ .bulk_string = "PING" },
        RespValue{ .bulk_string = "hello" },
    };

    const result = try cmdPing(allocator, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$5\r\nhello\r\n", result);
}

test "commands - PING too many arguments" {
    const allocator = std.testing.allocator;

    const args = [_]RespValue{
        RespValue{ .bulk_string = "PING" },
        RespValue{ .bulk_string = "arg1" },
        RespValue{ .bulk_string = "arg2" },
    };

    const result = try cmdPing(allocator, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("-ERR wrong number of arguments for 'ping' command\r\n", result);
}

test "commands - SET basic" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqualStrings("value1", storage.get("key1").?);
}

test "commands - SET with EX option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "EX" },
        RespValue{ .bulk_string = "60" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqualStrings("value1", storage.get("key1").?);
}

test "commands - SET with PX option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "PX" },
        RespValue{ .bulk_string = "5000" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqualStrings("value1", storage.get("key1").?);
}

test "commands - SET with NX when key doesn't exist" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "NX" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "commands - SET with NX when key exists" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "existing", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "NX" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
    try std.testing.expectEqualStrings("existing", storage.get("key1").?);
}

test "commands - SET with XX when key exists" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "existing", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "new_value" },
        RespValue{ .bulk_string = "XX" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqualStrings("new_value", storage.get("key1").?);
}

test "commands - SET with XX when key doesn't exist" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "XX" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
    try std.testing.expect(storage.get("key1") == null);
}

test "commands - SET with both NX and XX returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "NX" },
        RespValue{ .bulk_string = "XX" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("-ERR syntax error\r\n", result);
}

test "commands - SET with negative expiration" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "EX" },
        RespValue{ .bulk_string = "-1" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("-ERR invalid expire time in 'set' command\r\n", result);
}

test "commands - GET existing key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "hello", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "key1" },
    };

    const result = try cmdGet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$5\r\nhello\r\n", result);
}

test "commands - GET non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    const result = try cmdGet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "commands - GET wrong number of arguments" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GET" },
    };

    const result = try cmdGet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("-ERR wrong number of arguments for 'get' command\r\n", result);
}

test "commands - DEL single key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DEL" },
        RespValue{ .bulk_string = "key1" },
    };

    const result = try cmdDel(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
    try std.testing.expect(storage.get("key1") == null);
}

test "commands - DEL multiple keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);
    try storage.set("key2", "value2", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DEL" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key2" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    const result = try cmdDel(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":2\r\n", result);
}

test "commands - DEL non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DEL" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    const result = try cmdDel(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "commands - EXISTS single existing key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "EXISTS" },
        RespValue{ .bulk_string = "key1" },
    };

    const result = try cmdExists(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
}

test "commands - EXISTS non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "EXISTS" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    const result = try cmdExists(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "commands - EXISTS multiple keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);
    try storage.set("key2", "value2", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "EXISTS" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key2" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    const result = try cmdExists(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":2\r\n", result);
}

test "commands - EXISTS with duplicate keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "EXISTS" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key1" },
    };

    const result = try cmdExists(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":3\r\n", result);
}

test "commands - executeCommand dispatches PING" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "PING" },
    };
    const cmd = RespValue{ .array = &args };

    const result = try executeCommand(allocator, storage, cmd);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+PONG\r\n", result);
}

test "commands - executeCommand case insensitive" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "ping" },
    };
    const cmd = RespValue{ .array = &args };

    const result = try executeCommand(allocator, storage, cmd);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+PONG\r\n", result);
}

test "commands - executeCommand unknown command" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "UNKNOWN" },
    };
    const cmd = RespValue{ .array = &args };

    const result = try executeCommand(allocator, storage, cmd);
    defer allocator.free(result);

    const expected = "-ERR unknown command 'UNKNOWN'\r\n";
    try std.testing.expectEqualStrings(expected, result);
}
