const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const Storage = @import("../src/storage/memory.zig").Storage;
const ClientRegistry = @import("../src/commands/client.zig").ClientRegistry;
const executeCommand = @import("../src/commands/strings.zig").executeCommand;
const BlockingQueue = @import("../src/storage/blocking.zig").BlockingQueue;

const RespValue = protocol.RespValue;

test "CLIENT TRACKING ON/OFF integration" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // Enable tracking
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var args = std.ArrayList(RespValue){};
        try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
        const args_slice = try args.toOwnedSlice(arena_allocator);

        const response = try executeCommand(allocator, &storage, &registry, client_id, args_slice, &blocking_queue);
        defer allocator.free(response);

        try std.testing.expect(std.mem.startsWith(u8, response, "+OK"));
    }

    // Verify tracking is enabled via TRACKINGINFO
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var args = std.ArrayList(RespValue){};
        try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKINGINFO" });
        const args_slice = try args.toOwnedSlice(arena_allocator);

        const response = try executeCommand(allocator, &storage, &registry, client_id, args_slice, &blocking_queue);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "flags") != null);
        try std.testing.expect(std.mem.indexOf(u8, response, "on") != null);
    }

    // Disable tracking
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var args = std.ArrayList(RespValue){};
        try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "OFF" });
        const args_slice = try args.toOwnedSlice(arena_allocator);

        const response = try executeCommand(allocator, &storage, &registry, client_id, args_slice, &blocking_queue);
        defer allocator.free(response);

        try std.testing.expect(std.mem.startsWith(u8, response, "+OK"));
    }

    // Verify tracking is disabled
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var args = std.ArrayList(RespValue){};
        try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKINGINFO" });
        const args_slice = try args.toOwnedSlice(arena_allocator);

        const response = try executeCommand(allocator, &storage, &registry, client_id, args_slice, &blocking_queue);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "off") != null);
    }
}

test "CLIENT TRACKING with OPTIN mode" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // Enable tracking with OPTIN
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var args = std.ArrayList(RespValue){};
        try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "OPTIN" });
        const args_slice = try args.toOwnedSlice(arena_allocator);

        const response = try executeCommand(allocator, &storage, &registry, client_id, args_slice, &blocking_queue);
        defer allocator.free(response);

        try std.testing.expect(std.mem.startsWith(u8, response, "+OK"));
    }

    // Verify OPTIN flag is set
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var args = std.ArrayList(RespValue){};
        try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKINGINFO" });
        const args_slice = try args.toOwnedSlice(arena_allocator);

        const response = try executeCommand(allocator, &storage, &registry, client_id, args_slice, &blocking_queue);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "optin") != null);
    }
}

test "CLIENT TRACKING with BCAST and PREFIX" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // Enable tracking with BCAST and PREFIX
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var args = std.ArrayList(RespValue){};
        try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "BCAST" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "PREFIX" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "user:" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "PREFIX" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "product:" });
        const args_slice = try args.toOwnedSlice(arena_allocator);

        const response = try executeCommand(allocator, &storage, &registry, client_id, args_slice, &blocking_queue);
        defer allocator.free(response);

        try std.testing.expect(std.mem.startsWith(u8, response, "+OK"));
    }

    // Verify BCAST flag and prefixes
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var args = std.ArrayList(RespValue){};
        try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKINGINFO" });
        const args_slice = try args.toOwnedSlice(arena_allocator);

        const response = try executeCommand(allocator, &storage, &registry, client_id, args_slice, &blocking_queue);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "bcast") != null);
        try std.testing.expect(std.mem.indexOf(u8, response, "user:") != null);
        try std.testing.expect(std.mem.indexOf(u8, response, "product:") != null);
    }
}

test "CLIENT CACHING YES/NO integration" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // CLIENT CACHING YES
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var args = std.ArrayList(RespValue){};
        try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "CACHING" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "YES" });
        const args_slice = try args.toOwnedSlice(arena_allocator);

        const response = try executeCommand(allocator, &storage, &registry, client_id, args_slice, &blocking_queue);
        defer allocator.free(response);

        try std.testing.expect(std.mem.startsWith(u8, response, "+OK"));
    }

    // CLIENT CACHING NO
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var args = std.ArrayList(RespValue){};
        try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "CACHING" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "NO" });
        const args_slice = try args.toOwnedSlice(arena_allocator);

        const response = try executeCommand(allocator, &storage, &registry, client_id, args_slice, &blocking_queue);
        defer allocator.free(response);

        try std.testing.expect(std.mem.startsWith(u8, response, "+OK"));
    }
}

test "CLIENT TRACKING - error cases" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    var blocking_queue = BlockingQueue.init(allocator);
    defer blocking_queue.deinit();

    const client_id = try registry.registerClient("127.0.0.1:12345", 42);

    // Test OPTIN and OPTOUT mutually exclusive
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var args = std.ArrayList(RespValue){};
        try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "OPTIN" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "OPTOUT" });
        const args_slice = try args.toOwnedSlice(arena_allocator);

        const response = try executeCommand(allocator, &storage, &registry, client_id, args_slice, &blocking_queue);
        defer allocator.free(response);

        try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
        try std.testing.expect(std.mem.indexOf(u8, response, "mutually exclusive") != null);
    }

    // Test invalid redirect client ID
    {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var args = std.ArrayList(RespValue){};
        try args.append(arena_allocator, RespValue{ .bulk_string = "CLIENT" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "TRACKING" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "ON" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "REDIRECT" });
        try args.append(arena_allocator, RespValue{ .bulk_string = "999" });
        const args_slice = try args.toOwnedSlice(arena_allocator);

        const response = try executeCommand(allocator, &storage, &registry, client_id, args_slice, &blocking_queue);
        defer allocator.free(response);

        try std.testing.expect(std.mem.startsWith(u8, response, "-ERR"));
        try std.testing.expect(std.mem.indexOf(u8, response, "invalid redirect") != null);
    }
}
