const std = @import("std");
const protocol = @import("protocol");
const Storage = @import("storage").Storage;
const commands = @import("commands");
const client_mod = @import("commands").client;

const RespValue = protocol.RespValue;
const ClientRegistry = client_mod.ClientRegistry;

test "Integration: CLIENT NO-EVICT - set ON and OFF" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    // Test: CLIENT NO-EVICT ON
    {
        var args = std.ArrayList(RespValue){};
        try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "NO-EVICT" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
        const args_slice = try args.toOwnedSlice(arena_allocator);

        const response = try commands.executeCommand(
            allocator,
            &storage,
            args_slice,
            &registry,
            client_id,
        );
        defer allocator.free(response);

        try std.testing.expect(std.mem.eql(u8, response, "+OK\r\n"));
        try std.testing.expect(registry.getNoEvict(client_id) == true);
    }

    // Test: CLIENT NO-EVICT OFF
    {
        var args2 = std.ArrayList(RespValue){};
        try args2.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
        try args2.append(arena_allocator, RespValue{ .bulk_string = "NO-EVICT" });
        try args2.append(arena_allocator, RespValue{ .bulk_string = "OFF" });
        const args_slice2 = try args2.toOwnedSlice(arena_allocator);

        const response2 = try commands.executeCommand(
            allocator,
            &storage,
            args_slice2,
            &registry,
            client_id,
        );
        defer allocator.free(response2);

        try std.testing.expect(std.mem.eql(u8, response2, "+OK\r\n"));
        try std.testing.expect(registry.getNoEvict(client_id) == false);
    }
}

test "Integration: CLIENT NO-EVICT - get status without argument" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);
    registry.setNoEvict(client_id, true);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "NO-EVICT" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try commands.executeCommand(
        allocator,
        &storage,
        args_slice,
        &registry,
        client_id,
    );
    defer allocator.free(response);

    try std.testing.expect(std.mem.eql(u8, response, "+on\r\n"));
}

test "Integration: CLIENT REPLY - ON mode" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "REPLY" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try commands.executeCommand(
        allocator,
        &storage,
        args_slice,
        &registry,
        client_id,
    );
    defer allocator.free(response);

    try std.testing.expect(std.mem.eql(u8, response, "+OK\r\n"));
    try std.testing.expect(registry.getReplyMode(client_id) == .ON);
}

test "Integration: CLIENT REPLY - OFF mode" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "REPLY" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "OFF" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try commands.executeCommand(
        allocator,
        &storage,
        args_slice,
        &registry,
        client_id,
    );
    defer allocator.free(response);

    try std.testing.expect(std.mem.eql(u8, response, "+OK\r\n"));
    try std.testing.expect(registry.getReplyMode(client_id) == .OFF);
}

test "Integration: CLIENT REPLY - SKIP mode and revert" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "REPLY" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "SKIP" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try commands.executeCommand(
        allocator,
        &storage,
        args_slice,
        &registry,
        client_id,
    );
    defer allocator.free(response);

    try std.testing.expect(std.mem.eql(u8, response, "+OK\r\n"));
    try std.testing.expect(registry.getReplyMode(client_id) == .SKIP);

    // Simulate processing a command (SKIP should revert to ON)
    registry.processReplySkip(client_id);
    try std.testing.expect(registry.getReplyMode(client_id) == .ON);
}

test "Integration: CLIENT REPLY - invalid mode returns error" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "REPLY" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "INVALID" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try commands.executeCommand(
        allocator,
        &storage,
        args_slice,
        &registry,
        client_id,
    );
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
}

test "Integration: CLIENT NO-EVICT - invalid argument returns error" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, "127.0.0.1", 6379);
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    var args = std.ArrayList(RespValue){};
    try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "NO-EVICT" });
    try args.append(arena_allocator, RespValue{ .bulk_string = "MAYBE" });
    const args_slice = try args.toOwnedSlice(arena_allocator);

    const response = try commands.executeCommand(
        allocator,
        &storage,
        args_slice,
        &registry,
        client_id,
    );
    defer allocator.free(response);

    try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
    try std.testing.expect(std.mem.indexOf(u8, response, "'ON' or 'OFF'") != null);
}
