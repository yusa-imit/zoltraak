//! Bounded single-line reply reader for outbound connections (MIGRATE targets).
//!
//! The peer on the other end of an outbound connection is not under our control, so the length
//! of the line it sends back is data, not a precondition: a peer that streams bytes and never
//! sends `\n` must cost us a typed error, not unbounded memory.
//!
//! Allocation: one result buffer per call, owned by the caller; nothing is retained.
//! Cost: one `read` syscall per byte, at most `reply_line_max` of them per call; replies to
//! AUTH/SELECT/RESTORE are a handful of bytes, so batching would only add a lookahead buffer
//! that must not swallow the bytes of the next reply.

const std = @import("std");
const assert = std.debug.assert;

/// Upper bound, including the trailing `\r\n`, on one reply line.
pub const reply_line_max: u32 = 4096;

pub const ReadError = error{ EndOfStream, ReplyTooLong, OutOfMemory } ||
    std.net.Stream.ReadError;

/// Read one reply line (up to and including `\n`) from `stream`. Caller owns the result.
/// Fails with `error.ReplyTooLong` when no `\n` arrives within `reply_line_max` bytes and with
/// `error.EndOfStream` when the peer closes first.
pub fn read_reply_line(gpa: std.mem.Allocator, stream: std.net.Stream) ReadError![]u8 {
    var line = try std.ArrayList(u8).initCapacity(gpa, 64);
    errdefer line.deinit(gpa);

    var byte: [1]u8 = undefined;
    for (0..reply_line_max) |_| {
        const read_count = try stream.read(&byte);
        if (read_count == 0) return error.EndOfStream;

        try line.append(gpa, byte[0]);
        if (byte[0] == '\n') {
            assert(line.items.len <= reply_line_max);
            return line.toOwnedSlice(gpa);
        }
    }
    assert(line.items.len == reply_line_max);
    return error.ReplyTooLong;
}

/// Test double for a remote peer: accepts one connection, answers the first request with
/// `reply` verbatim, then either closes at once (`hold_open == false`, the client sees end of
/// stream) or holds the connection open until the client hangs up, so that the client never
/// observes a premature end of stream.
fn mock_peer_run(server: *std.net.Server, reply: []const u8, hold_open: bool) void {
    const conn = server.accept() catch return;
    defer conn.stream.close();

    var request: [64]u8 = undefined;
    const request_count = conn.stream.read(&request) catch return;
    if (request_count == 0) return;
    conn.stream.writeAll(reply) catch return;
    if (!hold_open) return;

    for (0..16) |_| {
        const drained = conn.stream.read(&request) catch return;
        if (drained == 0) return;
    }
}

/// Send one request to a mock peer that answers with `reply`, and read its reply line.
fn exchange(gpa: std.mem.Allocator, reply: []const u8, hold_open: bool) ReadError![]u8 {
    const bind_address = std.net.Address.parseIp("127.0.0.1", 0) catch unreachable; // literal
    var server = bind_address.listen(.{ .reuse_address = true }) catch return error.Unexpected;
    defer server.deinit();

    const thread = std.Thread.spawn(.{}, mock_peer_run, .{ &server, reply, hold_open }) catch {
        return error.Unexpected;
    };
    defer thread.join();

    const stream = std.net.tcpConnectToAddress(server.listen_address) catch {
        return error.Unexpected;
    };
    defer stream.close();

    stream.writeAll("PING\r\n") catch return error.Unexpected;
    return read_reply_line(gpa, stream);
}

fn make_reply_line(gpa: std.mem.Allocator, line_len: u32, terminated: bool) ![]u8 {
    assert(line_len >= 3);
    const reply = try gpa.alloc(u8, line_len);
    @memset(reply, 'x');
    if (terminated) {
        reply[0] = '+';
        reply[line_len - 2] = '\r';
        reply[line_len - 1] = '\n';
    }
    return reply;
}

test "read_reply_line - short simple-string reply is returned verbatim" {
    const gpa = std.testing.allocator;

    const line = try exchange(gpa, "+OK\r\n", true);
    defer gpa.free(line);

    try std.testing.expectEqualStrings("+OK\r\n", line);
}

test "read_reply_line - reply of exactly reply_line_max bytes is accepted" {
    const gpa = std.testing.allocator;
    const reply = try make_reply_line(gpa, reply_line_max, true);
    defer gpa.free(reply);

    const line = try exchange(gpa, reply, true);
    defer gpa.free(line);

    try std.testing.expectEqual(@as(usize, reply_line_max), line.len);
    try std.testing.expectEqualSlices(u8, reply, line);
}

test "read_reply_line - terminated reply one byte over reply_line_max is rejected" {
    const gpa = std.testing.allocator;
    const reply = try make_reply_line(gpa, reply_line_max + 1, true);
    defer gpa.free(reply);

    try std.testing.expectError(error.ReplyTooLong, exchange(gpa, reply, true));
}

test "read_reply_line - endless line without a newline is rejected" {
    const gpa = std.testing.allocator;
    const reply = try make_reply_line(gpa, reply_line_max * 2, false);
    defer gpa.free(reply);

    try std.testing.expectError(error.ReplyTooLong, exchange(gpa, reply, true));
}

test "read_reply_line - peer closing mid-line returns EndOfStream" {
    const gpa = std.testing.allocator;

    try std.testing.expectError(error.EndOfStream, exchange(gpa, "+OK", false));
}
