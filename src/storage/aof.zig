const std = @import("std");
const memory = @import("memory.zig");
const Storage = memory.Storage;

/// AOF (Append-Only File) persistence.
///
/// Every write command is appended to the AOF file as a RESP array.
/// On startup, the AOF is replayed to restore state.
///
/// File format: standard RESP protocol arrays, e.g.:
///   *3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n
pub const Aof = struct {
    file: std.fs.File,
    mutex: std.Thread.Mutex,

    /// Open (or create) the AOF file for appending.
    /// Returned pointer is heap-allocated via page_allocator; call close() to free.
    pub fn open(path: []const u8) !*Aof {
        const aof = try std.heap.page_allocator.create(Aof);
        errdefer std.heap.page_allocator.destroy(aof);

        const file = std.fs.cwd().openFile(path, .{ .mode = .read_write }) catch |err| blk: {
            if (err == error.FileNotFound) {
                break :blk try std.fs.cwd().createFile(path, .{ .read = true });
            }
            return err;
        };

        // Seek to end for appending
        try file.seekFromEnd(0);

        aof.* = .{
            .file = file,
            .mutex = .{},
        };
        return aof;
    }

    pub fn close(self: *Aof) void {
        self.file.close();
        std.heap.page_allocator.destroy(self);
    }

    /// Append a RESP command to the AOF file.
    /// `args` is a slice of string arguments (command name first).
    pub fn appendCommand(self: *Aof, args: []const []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var buf = std.ArrayList(u8){};
        defer buf.deinit(std.heap.page_allocator);

        const w = buf.writer(std.heap.page_allocator);

        // Write RESP array header
        try w.print("*{d}\r\n", .{args.len});
        for (args) |arg| {
            try w.print("${d}\r\n{s}\r\n", .{ arg.len, arg });
        }

        try self.file.writeAll(buf.items);
    }

    /// Replay an AOF file into storage.
    /// Returns the number of commands replayed.
    pub fn replay(storage: *Storage, path: []const u8, allocator: std.mem.Allocator) !usize {
        const file = std.fs.cwd().openFile(path, .{}) catch |err| {
            if (err == error.FileNotFound) return 0;
            return err;
        };
        defer file.close();

        const data = try file.readToEndAlloc(allocator, 512 * 1024 * 1024);
        defer allocator.free(data);

        if (data.len == 0) return 0;

        var pos: usize = 0;
        var cmd_count: usize = 0;

        while (pos < data.len) {
            if (data[pos] != '*') break;
            pos += 1;
            const count = try parseRespInt(data, &pos);
            if (count <= 0) break;

            const ucount: usize = @intCast(count);
            var args = try allocator.alloc([]u8, ucount);
            var parsed: usize = 0;

            // Cleanup on error
            defer {
                for (args[0..parsed]) |a| allocator.free(a);
                allocator.free(args);
            }

            var ok = true;
            for (0..ucount) |i| {
                if (pos >= data.len or data[pos] != '$') {
                    ok = false;
                    break;
                }
                pos += 1;
                const len = parseRespInt(data, &pos) catch {
                    ok = false;
                    break;
                };
                if (len < 0) {
                    ok = false;
                    break;
                }
                const ulen: usize = @intCast(len);
                if (pos + ulen + 2 > data.len) {
                    ok = false;
                    break;
                }
                args[i] = try allocator.dupe(u8, data[pos .. pos + ulen]);
                parsed += 1;
                pos += ulen + 2; // skip data + \r\n
            }

            if (!ok) break;

            executeStorageCommand(storage, args[0..parsed], allocator) catch |err| {
                std.debug.print("AOF replay warning: command failed: {any}\n", .{err});
            };
            cmd_count += 1;
        }

        return cmd_count;
    }

    /// Rewrite the AOF file from current storage state.
    /// Produces a minimal, compact representation of the current dataset.
    pub fn rewrite(storage: *Storage, path: []const u8, allocator: std.mem.Allocator) !void {
        var buf = std.ArrayList(u8){};
        defer buf.deinit(allocator);

        const w = buf.writer(allocator);

        storage.mutex.lock();
        defer storage.mutex.unlock();

        var it = storage.data.iterator();
        const now = Storage.getCurrentTimestamp();

        while (it.next()) |entry| {
            if (entry.value_ptr.isExpired(now)) continue;

            const key = entry.key_ptr.*;
            const value = entry.value_ptr.*;
            const expires_at = value.getExpiration();

            switch (value) {
                .string => |s| {
                    if (expires_at) |exp| {
                        const remaining = exp - now;
                        if (remaining <= 0) continue;
                        const rem_str = try std.fmt.allocPrint(allocator, "{d}", .{remaining});
                        defer allocator.free(rem_str);
                        try writeRespArgs(w, &[_][]const u8{ "SET", key, s.data, "PX", rem_str });
                    } else {
                        try writeRespArgs(w, &[_][]const u8{ "SET", key, s.data });
                    }
                },
                .list => |l| {
                    if (l.data.items.len == 0) continue;
                    // Build: RPUSH key elem1 elem2 ...
                    var rargs = try allocator.alloc([]const u8, 2 + l.data.items.len);
                    defer allocator.free(rargs);
                    rargs[0] = "RPUSH";
                    rargs[1] = key;
                    for (l.data.items, 0..) |elem, i| rargs[2 + i] = elem;
                    try writeRespArgs(w, rargs);
                    if (expires_at) |exp| {
                        const remaining = exp - now;
                        if (remaining > 0) {
                            const rem_str = try std.fmt.allocPrint(allocator, "{d}", .{remaining});
                            defer allocator.free(rem_str);
                            try writeRespArgs(w, &[_][]const u8{ "PEXPIRE", key, rem_str });
                        }
                    }
                },
                .set => |s| {
                    var sargs = std.ArrayList([]const u8){};
                    defer sargs.deinit(allocator);
                    try sargs.append(allocator, "SADD");
                    try sargs.append(allocator, key);
                    var kit = s.data.keyIterator();
                    while (kit.next()) |k| try sargs.append(allocator, k.*);
                    if (sargs.items.len > 2) try writeRespArgs(w, sargs.items);
                    if (expires_at) |exp| {
                        const remaining = exp - now;
                        if (remaining > 0) {
                            const rem_str = try std.fmt.allocPrint(allocator, "{d}", .{remaining});
                            defer allocator.free(rem_str);
                            try writeRespArgs(w, &[_][]const u8{ "PEXPIRE", key, rem_str });
                        }
                    }
                },
                .hash => |h| {
                    var hargs = std.ArrayList([]const u8){};
                    defer hargs.deinit(allocator);
                    try hargs.append(allocator, "HSET");
                    try hargs.append(allocator, key);
                    var hit = h.data.iterator();
                    while (hit.next()) |e| {
                        try hargs.append(allocator, e.key_ptr.*);
                        try hargs.append(allocator, e.value_ptr.*);
                    }
                    if (hargs.items.len > 2) try writeRespArgs(w, hargs.items);
                    if (expires_at) |exp| {
                        const remaining = exp - now;
                        if (remaining > 0) {
                            const rem_str = try std.fmt.allocPrint(allocator, "{d}", .{remaining});
                            defer allocator.free(rem_str);
                            try writeRespArgs(w, &[_][]const u8{ "PEXPIRE", key, rem_str });
                        }
                    }
                },
                .sorted_set => |z| {
                    var zargs = std.ArrayList([]const u8){};
                    defer zargs.deinit(allocator);
                    try zargs.append(allocator, "ZADD");
                    try zargs.append(allocator, key);
                    for (z.sorted_list.items) |scored| {
                        const score_str = try std.fmt.allocPrint(allocator, "{d}", .{scored.score});
                        defer allocator.free(score_str);
                        try zargs.append(allocator, score_str);
                        try zargs.append(allocator, scored.member);
                    }
                    if (zargs.items.len > 2) try writeRespArgs(w, zargs.items);
                    if (expires_at) |exp| {
                        const remaining = exp - now;
                        if (remaining > 0) {
                            const rem_str = try std.fmt.allocPrint(allocator, "{d}", .{remaining});
                            defer allocator.free(rem_str);
                            try writeRespArgs(w, &[_][]const u8{ "PEXPIRE", key, rem_str });
                        }
                    }
                },
                .stream => {
                    // Streams not yet implemented in AOF - skip for now
                },
                .hyperloglog => {
                    // HyperLogLog not yet implemented in AOF - skip for now
                    // (Would require serializing 16384 bytes as args, not practical)
                },
            }
        }

        // Atomic write: tmp file → rename
        var tmp_buf: [512]u8 = undefined;
        const tmp_path = try std.fmt.bufPrint(&tmp_buf, "{s}.tmp", .{path});
        try std.fs.cwd().writeFile(.{ .sub_path = tmp_path, .data = buf.items });
        try std.fs.cwd().rename(tmp_path, path);
    }
};

// ============================================================
// Internal helpers
// ============================================================

/// Parse a decimal integer up to the next \r\n, advance pos past \r\n.
fn parseRespInt(data: []const u8, pos: *usize) !i64 {
    const start = pos.*;
    var end = start;
    while (end < data.len and data[end] != '\r') : (end += 1) {}
    if (end + 1 >= data.len or data[end + 1] != '\n') return error.InvalidAofFile;
    const n = try std.fmt.parseInt(i64, data[start..end], 10);
    pos.* = end + 2;
    return n;
}

/// Write a RESP array from a slice of strings
fn writeRespArgs(w: anytype, args: []const []const u8) !void {
    try w.print("*{d}\r\n", .{args.len});
    for (args) |arg| {
        try w.print("${d}\r\n{s}\r\n", .{ arg.len, arg });
    }
}

/// Execute a parsed command array against storage during AOF replay.
/// Only write commands that mutate state are handled; read-only commands are silently skipped.
fn executeStorageCommand(storage: *Storage, args: [][]u8, allocator: std.mem.Allocator) !void {
    if (args.len == 0) return;

    const cmd = try std.ascii.allocUpperString(allocator, args[0]);
    defer allocator.free(cmd);

    if (std.mem.eql(u8, cmd, "SET")) {
        if (args.len < 3) return error.InvalidAofCommand;
        const key = args[1];
        const value = args[2];
        var expires_at: ?i64 = null;

        var i: usize = 3;
        while (i < args.len) : (i += 1) {
            const opt = try std.ascii.allocUpperString(allocator, args[i]);
            defer allocator.free(opt);
            if (std.mem.eql(u8, opt, "EX") and i + 1 < args.len) {
                i += 1;
                const secs = try std.fmt.parseInt(i64, args[i], 10);
                expires_at = Storage.getCurrentTimestamp() + (secs * 1000);
            } else if (std.mem.eql(u8, opt, "PX") and i + 1 < args.len) {
                i += 1;
                const ms = try std.fmt.parseInt(i64, args[i], 10);
                expires_at = Storage.getCurrentTimestamp() + ms;
            }
        }
        try storage.set(key, value, expires_at);
    } else if (std.mem.eql(u8, cmd, "DEL")) {
        const keys: []const []const u8 = @ptrCast(args[1..]);
        _ = storage.del(keys);
    } else if (std.mem.eql(u8, cmd, "LPUSH")) {
        if (args.len < 3) return error.InvalidAofCommand;
        const elems: []const []const u8 = @ptrCast(args[2..]);
        _ = try storage.lpush(args[1], elems, null);
    } else if (std.mem.eql(u8, cmd, "RPUSH")) {
        if (args.len < 3) return error.InvalidAofCommand;
        const elems: []const []const u8 = @ptrCast(args[2..]);
        _ = try storage.rpush(args[1], elems, null);
    } else if (std.mem.eql(u8, cmd, "LPOP")) {
        if (args.len < 2) return error.InvalidAofCommand;
        const result = try storage.lpop(allocator, args[1], 1);
        if (result) |r| {
            for (r) |elem| allocator.free(elem);
            allocator.free(r);
        }
    } else if (std.mem.eql(u8, cmd, "RPOP")) {
        if (args.len < 2) return error.InvalidAofCommand;
        const result = try storage.rpop(allocator, args[1], 1);
        if (result) |r| {
            for (r) |elem| allocator.free(elem);
            allocator.free(r);
        }
    } else if (std.mem.eql(u8, cmd, "SADD")) {
        if (args.len < 3) return error.InvalidAofCommand;
        const members: []const []const u8 = @ptrCast(args[2..]);
        _ = try storage.sadd(args[1], members, null);
    } else if (std.mem.eql(u8, cmd, "SREM")) {
        if (args.len < 3) return error.InvalidAofCommand;
        const members: []const []const u8 = @ptrCast(args[2..]);
        _ = try storage.srem(allocator, args[1], members);
    } else if (std.mem.eql(u8, cmd, "HSET")) {
        if (args.len < 4) return error.InvalidAofCommand;
        // HSET key field1 value1 [field2 value2 ...]
        const pair_count = (args.len - 2) / 2;
        var fields = try allocator.alloc([]const u8, pair_count);
        defer allocator.free(fields);
        var values = try allocator.alloc([]const u8, pair_count);
        defer allocator.free(values);
        for (0..pair_count) |fi| {
            fields[fi] = args[2 + fi * 2];
            values[fi] = args[3 + fi * 2];
        }
        _ = try storage.hset(args[1], fields, values, null);
    } else if (std.mem.eql(u8, cmd, "HDEL")) {
        if (args.len < 3) return error.InvalidAofCommand;
        const fields: []const []const u8 = @ptrCast(args[2..]);
        _ = try storage.hdel(args[1], fields);
    } else if (std.mem.eql(u8, cmd, "ZADD")) {
        if (args.len < 4) return error.InvalidAofCommand;
        // ZADD key score1 member1 [score2 member2 ...]
        const pair_count = (args.len - 2) / 2;
        var scores = try allocator.alloc(f64, pair_count);
        defer allocator.free(scores);
        var members = try allocator.alloc([]const u8, pair_count);
        defer allocator.free(members);
        for (0..pair_count) |pi| {
            scores[pi] = try std.fmt.parseFloat(f64, args[2 + pi * 2]);
            members[pi] = args[3 + pi * 2];
        }
        _ = try storage.zadd(args[1], scores, members, 0, null);
    } else if (std.mem.eql(u8, cmd, "ZREM")) {
        if (args.len < 3) return error.InvalidAofCommand;
        const members: []const []const u8 = @ptrCast(args[2..]);
        _ = try storage.zrem(args[1], members);
    } else if (std.mem.eql(u8, cmd, "FLUSHALL") or std.mem.eql(u8, cmd, "FLUSHDB")) {
        storage.flushAll();
    }
    // PEXPIRE and read-only commands (GET, EXISTS, etc.) are safely ignored during replay
}

// ============================================================
// Unit Tests
// ============================================================

test "aof - appendCommand and replay strings" {
    const allocator = std.testing.allocator;

    const path = "/tmp/zoltraak_test_aof.aof";
    defer std.fs.cwd().deleteFile(path) catch {};

    {
        const aof = try Aof.open(path);
        defer aof.close();

        try aof.appendCommand(&[_][]const u8{ "SET", "hello", "world" });
        try aof.appendCommand(&[_][]const u8{ "SET", "foo", "bar" });
        try aof.appendCommand(&[_][]const u8{ "DEL", "foo" });
    }

    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const count = try Aof.replay(storage, path, allocator);
    try std.testing.expectEqual(@as(usize, 3), count);
    try std.testing.expectEqualStrings("world", storage.get("hello").?);
    try std.testing.expect(storage.get("foo") == null);
}

test "aof - replay empty file" {
    const allocator = std.testing.allocator;
    const path = "/tmp/zoltraak_test_aof_empty.aof";
    defer std.fs.cwd().deleteFile(path) catch {};

    const f = try std.fs.cwd().createFile(path, .{});
    f.close();

    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const count = try Aof.replay(storage, path, allocator);
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "aof - replay nonexistent file returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const count = try Aof.replay(storage, "/tmp/zoltraak_no_such.aof", allocator);
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "aof - rewrite produces correct RESP" {
    const allocator = std.testing.allocator;
    const path = "/tmp/zoltraak_test_aof_rewrite.aof";
    defer std.fs.cwd().deleteFile(path) catch {};

    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "val1", null);
    var members = [_][]const u8{ "a", "b" };
    _ = try storage.sadd("myset", &members, null);

    try Aof.rewrite(storage, path, allocator);

    const storage2 = try Storage.init(allocator);
    defer storage2.deinit();

    const count = try Aof.replay(storage2, path, allocator);
    try std.testing.expect(count >= 2);
    try std.testing.expectEqualStrings("val1", storage2.get("key1").?);
    try std.testing.expectEqual(@as(usize, 2), storage2.scard("myset").?);
}

test "aof - replay list commands" {
    const allocator = std.testing.allocator;
    const path = "/tmp/zoltraak_test_aof_list.aof";
    defer std.fs.cwd().deleteFile(path) catch {};

    {
        const aof = try Aof.open(path);
        defer aof.close();
        try aof.appendCommand(&[_][]const u8{ "RPUSH", "mylist", "a", "b", "c" });
        try aof.appendCommand(&[_][]const u8{ "LPOP", "mylist" });
    }

    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const count = try Aof.replay(storage, path, allocator);
    try std.testing.expectEqual(@as(usize, 2), count);
    // After RPUSH a,b,c then LPOP: list should be [b,c]
    try std.testing.expectEqual(@as(usize, 2), storage.llen("mylist").?);
}
