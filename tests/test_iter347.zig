// Iteration 347: ACL GENPASS + ACL DRYRUN
// ACL GENPASS generates cryptographically random hex passwords.
// ACL DRYRUN checks if a user can run a command without executing it.
//
// NOTE: Command functions receive args as dispatched from strings.zig via array[1..],
// so args[0] = subcommand name ("GENPASS"/"DRYRUN"), remaining args follow.
const std = @import("std");
const testing = std.testing;
const zoltraak = @import("zoltraak");

const Storage = zoltraak.storage.Storage;
const RespValue = zoltraak.protocol.RespValue;
const acl = zoltraak.acl_commands;

test "iter347 - ACL GENPASS default returns 128 hex chars (512 bits)" {
    const allocator = testing.allocator;

    // Dispatch passes array[1..] → args[0]="GENPASS", no bits arg
    const args = [_]RespValue{
        .{ .bulk_string = "GENPASS" },
    };
    const result = try acl.cmdACLGenpass(allocator, &args);
    defer allocator.free(result);

    // Format: $128\r\n<128 hex chars>\r\n
    try testing.expect(std.mem.startsWith(u8, result, "$128\r\n"));
    try testing.expect(std.mem.endsWith(u8, result, "\r\n"));

    // Extract the hex string
    const hex_start = "$128\r\n".len;
    const hex_end = result.len - 2; // trim trailing \r\n
    const hex = result[hex_start..hex_end];
    try testing.expectEqual(@as(usize, 128), hex.len);

    // All chars must be lowercase hex
    for (hex) |c| {
        try testing.expect((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'));
    }
}

test "iter347 - ACL GENPASS with bits=64 returns 16 hex chars" {
    const allocator = testing.allocator;

    // Dispatch: array[1..] → args[0]="GENPASS", args[1]="64"
    const args = [_]RespValue{
        .{ .bulk_string = "GENPASS" },
        .{ .bulk_string = "64" },
    };
    const result = try acl.cmdACLGenpass(allocator, &args);
    defer allocator.free(result);

    // Format: $16\r\n<16 hex chars>\r\n
    try testing.expect(std.mem.startsWith(u8, result, "$16\r\n"));
    const hex_start = "$16\r\n".len;
    const hex_end = result.len - 2;
    const hex = result[hex_start..hex_end];
    try testing.expectEqual(@as(usize, 16), hex.len);
    for (hex) |c| {
        try testing.expect((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'));
    }
}

test "iter347 - ACL GENPASS with bits=256 returns 64 hex chars" {
    const allocator = testing.allocator;

    const args = [_]RespValue{
        .{ .bulk_string = "GENPASS" },
        .{ .bulk_string = "256" },
    };
    const result = try acl.cmdACLGenpass(allocator, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "$64\r\n"));
    const hex_start = "$64\r\n".len;
    const hex_end = result.len - 2;
    const hex = result[hex_start..hex_end];
    try testing.expectEqual(@as(usize, 64), hex.len);
}

test "iter347 - ACL GENPASS bits rounds up to multiple of 4" {
    const allocator = testing.allocator;

    // 5 bits rounds up to 8 bits = 2 hex chars
    const args = [_]RespValue{
        .{ .bulk_string = "GENPASS" },
        .{ .bulk_string = "5" },
    };
    const result = try acl.cmdACLGenpass(allocator, &args);
    defer allocator.free(result);

    // 5 bits → rounds to 8 bits → 2 hex chars
    try testing.expect(std.mem.startsWith(u8, result, "$2\r\n"));
    const hex_start = "$2\r\n".len;
    const hex_end = result.len - 2;
    const hex = result[hex_start..hex_end];
    try testing.expectEqual(@as(usize, 2), hex.len);
}

test "iter347 - ACL GENPASS rejects 0 bits" {
    const allocator = testing.allocator;

    const args = [_]RespValue{
        .{ .bulk_string = "GENPASS" },
        .{ .bulk_string = "0" },
    };
    const result = try acl.cmdACLGenpass(allocator, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

test "iter347 - ACL GENPASS rejects bits > 4096" {
    const allocator = testing.allocator;

    const args = [_]RespValue{
        .{ .bulk_string = "GENPASS" },
        .{ .bulk_string = "4097" },
    };
    const result = try acl.cmdACLGenpass(allocator, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

test "iter347 - ACL GENPASS returns different values each call" {
    const allocator = testing.allocator;

    const args = [_]RespValue{
        .{ .bulk_string = "GENPASS" },
    };
    const result1 = try acl.cmdACLGenpass(allocator, &args);
    defer allocator.free(result1);
    const result2 = try acl.cmdACLGenpass(allocator, &args);
    defer allocator.free(result2);

    // Extremely unlikely to be the same (2^(-512) probability)
    try testing.expect(!std.mem.eql(u8, result1, result2));
}

test "iter347 - ACL DRYRUN default user can run GET" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Dispatch: array[1..] → args[0]="DRYRUN", args[1]="default", args[2]="GET"
    const args = [_]RespValue{
        .{ .bulk_string = "DRYRUN" },
        .{ .bulk_string = "default" },
        .{ .bulk_string = "GET" },
        .{ .bulk_string = "mykey" },
    };
    const result = try acl.cmdACLDryrun(allocator, storage, &args);
    defer allocator.free(result);

    try testing.expectEqualStrings("+OK\r\n", result);
}

test "iter347 - ACL DRYRUN non-existent user returns error" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        .{ .bulk_string = "DRYRUN" },
        .{ .bulk_string = "nonexistent_user_xyz" },
        .{ .bulk_string = "GET" },
    };
    const result = try acl.cmdACLDryrun(allocator, storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-ERR"));
    try testing.expect(std.mem.indexOf(u8, result, "nonexistent_user_xyz") != null);
    try testing.expect(std.mem.indexOf(u8, result, "not found") != null);
}

test "iter347 - ACL DRYRUN wrong number of args returns error" {
    const allocator = testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Only subcommand + username, no command
    const args = [_]RespValue{
        .{ .bulk_string = "DRYRUN" },
        .{ .bulk_string = "default" },
    };
    const result = try acl.cmdACLDryrun(allocator, storage, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.startsWith(u8, result, "-ERR"));
}

test "iter347 - ACL HELP includes GENPASS and DRYRUN" {
    const allocator = testing.allocator;

    const args = [_]RespValue{
        .{ .bulk_string = "HELP" },
    };
    const result = try acl.cmdACLHelp(allocator, &args);
    defer allocator.free(result);

    try testing.expect(std.mem.indexOf(u8, result, "GENPASS") != null);
    try testing.expect(std.mem.indexOf(u8, result, "DRYRUN") != null);
}
