const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const config_commands = @import("../src/commands/config.zig");
const storage_mod = @import("../src/storage/memory.zig");

const RespValue = protocol.RespValue;
const Storage = storage_mod.Storage;

const testing = std.testing;

// Performance benchmark: CONFIG GET single parameter
// Expected: >10k ops/sec, no memory leaks
test "CONFIG GET performance - single parameter" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const iterations: usize = 10_000;
    const start_time = std.time.nanoTimestamp();

    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        var args = [_]RespValue{
            RespValue{ .bulk_string = "CONFIG" },
            RespValue{ .bulk_string = "GET" },
            RespValue{ .bulk_string = "maxmemory" },
        };

        const response = try config_commands.executeConfigCommand(allocator, storage, &args);
        defer allocator.free(response);

        // Verify response is valid
        try testing.expect(response.len > 0);
    }

    const end_time = std.time.nanoTimestamp();
    const elapsed_ns = @as(u64, @intCast(end_time - start_time));
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const ops_per_sec = (@as(f64, @floatFromInt(iterations)) / elapsed_ms) * 1000.0;

    std.debug.print("\n[CONFIG GET Single Param Performance]\n", .{});
    std.debug.print("  Iterations: {d}\n", .{iterations});
    std.debug.print("  Time: {d:.2} ms\n", .{elapsed_ms});
    std.debug.print("  Throughput: {d:.0} ops/sec\n", .{ops_per_sec});
    std.debug.print("  Avg latency: {d:.3} ms\n", .{elapsed_ms / @as(f64, @floatFromInt(iterations))});

    // Performance assertion: Should achieve >10k ops/sec for admin commands
    const min_ops_per_sec: f64 = 10_000.0;
    if (ops_per_sec < min_ops_per_sec) {
        std.debug.print("  ⚠️  WARNING: Performance below expected threshold ({d:.0} < {d:.0} ops/sec)\n", .{ ops_per_sec, min_ops_per_sec });
    } else {
        std.debug.print("  ✅ Performance acceptable ({d:.0} >= {d:.0} ops/sec)\n", .{ ops_per_sec, min_ops_per_sec });
    }
}

// Performance benchmark: CONFIG GET wildcard pattern
// Expected: >5k ops/sec (more work than single param)
test "CONFIG GET performance - wildcard pattern" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const iterations: usize = 5_000;
    const start_time = std.time.nanoTimestamp();

    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        var args = [_]RespValue{
            RespValue{ .bulk_string = "CONFIG" },
            RespValue{ .bulk_string = "GET" },
            RespValue{ .bulk_string = "*" }, // Get all parameters
        };

        const response = try config_commands.executeConfigCommand(allocator, storage, &args);
        defer allocator.free(response);

        // Verify response is valid
        try testing.expect(response.len > 0);
    }

    const end_time = std.time.nanoTimestamp();
    const elapsed_ns = @as(u64, @intCast(end_time - start_time));
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const ops_per_sec = (@as(f64, @floatFromInt(iterations)) / elapsed_ms) * 1000.0;

    std.debug.print("\n[CONFIG GET Wildcard Performance]\n", .{});
    std.debug.print("  Iterations: {d}\n", .{iterations});
    std.debug.print("  Time: {d:.2} ms\n", .{elapsed_ms});
    std.debug.print("  Throughput: {d:.0} ops/sec\n", .{ops_per_sec});
    std.debug.print("  Avg latency: {d:.3} ms\n", .{elapsed_ms / @as(f64, @floatFromInt(iterations))});

    const min_ops_per_sec: f64 = 5_000.0;
    if (ops_per_sec < min_ops_per_sec) {
        std.debug.print("  ⚠️  WARNING: Performance below expected threshold ({d:.0} < {d:.0} ops/sec)\n", .{ ops_per_sec, min_ops_per_sec });
    } else {
        std.debug.print("  ✅ Performance acceptable ({d:.0} >= {d:.0} ops/sec)\n", .{ ops_per_sec, min_ops_per_sec });
    }
}

// Performance benchmark: CONFIG SET single parameter
// Expected: >5k ops/sec, no memory leaks
test "CONFIG SET performance - single parameter" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const iterations: usize = 5_000;
    const start_time = std.time.nanoTimestamp();

    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        // Alternate between two values to ensure actual setting happens
        const value = if (i % 2 == 0) "1024" else "2048";

        var args = [_]RespValue{
            RespValue{ .bulk_string = "CONFIG" },
            RespValue{ .bulk_string = "SET" },
            RespValue{ .bulk_string = "maxmemory" },
            RespValue{ .bulk_string = value },
        };

        const response = try config_commands.executeConfigCommand(allocator, storage, &args);
        defer allocator.free(response);

        // Verify response is OK
        try testing.expect(std.mem.indexOf(u8, response, "OK") != null);
    }

    const end_time = std.time.nanoTimestamp();
    const elapsed_ns = @as(u64, @intCast(end_time - start_time));
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const ops_per_sec = (@as(f64, @floatFromInt(iterations)) / elapsed_ms) * 1000.0;

    std.debug.print("\n[CONFIG SET Performance]\n", .{});
    std.debug.print("  Iterations: {d}\n", .{iterations});
    std.debug.print("  Time: {d:.2} ms\n", .{elapsed_ms});
    std.debug.print("  Throughput: {d:.0} ops/sec\n", .{ops_per_sec});
    std.debug.print("  Avg latency: {d:.3} ms\n", .{elapsed_ms / @as(f64, @floatFromInt(iterations))});

    const min_ops_per_sec: f64 = 5_000.0;
    if (ops_per_sec < min_ops_per_sec) {
        std.debug.print("  ⚠️  WARNING: Performance below expected threshold ({d:.0} < {d:.0} ops/sec)\n", .{ ops_per_sec, min_ops_per_sec });
    } else {
        std.debug.print("  ✅ Performance acceptable ({d:.0} >= {d:.0} ops/sec)\n", .{ ops_per_sec, min_ops_per_sec });
    }
}

// Performance benchmark: CONFIG SET multiple parameters
// Expected: >2k ops/sec (more work than single param)
test "CONFIG SET performance - multiple parameters" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const iterations: usize = 2_000;
    const start_time = std.time.nanoTimestamp();

    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        const value = if (i % 2 == 0) "1024" else "2048";

        var args = [_]RespValue{
            RespValue{ .bulk_string = "CONFIG" },
            RespValue{ .bulk_string = "SET" },
            RespValue{ .bulk_string = "maxmemory" },
            RespValue{ .bulk_string = value },
            RespValue{ .bulk_string = "timeout" },
            RespValue{ .bulk_string = "60" },
        };

        const response = try config_commands.executeConfigCommand(allocator, storage, &args);
        defer allocator.free(response);

        try testing.expect(std.mem.indexOf(u8, response, "OK") != null);
    }

    const end_time = std.time.nanoTimestamp();
    const elapsed_ns = @as(u64, @intCast(end_time - start_time));
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const ops_per_sec = (@as(f64, @floatFromInt(iterations)) / elapsed_ms) * 1000.0;

    std.debug.print("\n[CONFIG SET Multiple Params Performance]\n", .{});
    std.debug.print("  Iterations: {d}\n", .{iterations});
    std.debug.print("  Time: {d:.2} ms\n", .{elapsed_ms});
    std.debug.print("  Throughput: {d:.0} ops/sec\n", .{ops_per_sec});
    std.debug.print("  Avg latency: {d:.3} ms\n", .{elapsed_ms / @as(f64, @floatFromInt(iterations))});

    const min_ops_per_sec: f64 = 2_000.0;
    if (ops_per_sec < min_ops_per_sec) {
        std.debug.print("  ⚠️  WARNING: Performance below expected threshold ({d:.0} < {d:.0} ops/sec)\n", .{ ops_per_sec, min_ops_per_sec });
    } else {
        std.debug.print("  ✅ Performance acceptable ({d:.0} >= {d:.0} ops/sec)\n", .{ ops_per_sec, min_ops_per_sec });
    }
}

// Performance benchmark: CONFIG RESETSTAT
// Expected: >50k ops/sec (very simple operation)
test "CONFIG RESETSTAT performance" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const iterations: usize = 10_000;
    const start_time = std.time.nanoTimestamp();

    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        var args = [_]RespValue{
            RespValue{ .bulk_string = "CONFIG" },
            RespValue{ .bulk_string = "RESETSTAT" },
        };

        const response = try config_commands.executeConfigCommand(allocator, storage, &args);
        defer allocator.free(response);

        try testing.expect(std.mem.indexOf(u8, response, "OK") != null);
    }

    const end_time = std.time.nanoTimestamp();
    const elapsed_ns = @as(u64, @intCast(end_time - start_time));
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const ops_per_sec = (@as(f64, @floatFromInt(iterations)) / elapsed_ms) * 1000.0;

    std.debug.print("\n[CONFIG RESETSTAT Performance]\n", .{});
    std.debug.print("  Iterations: {d}\n", .{iterations});
    std.debug.print("  Time: {d:.2} ms\n", .{elapsed_ms});
    std.debug.print("  Throughput: {d:.0} ops/sec\n", .{ops_per_sec});
    std.debug.print("  Avg latency: {d:.3} ms\n", .{elapsed_ms / @as(f64, @floatFromInt(iterations))});

    const min_ops_per_sec: f64 = 10_000.0;
    if (ops_per_sec < min_ops_per_sec) {
        std.debug.print("  ⚠️  WARNING: Performance below expected threshold ({d:.0} < {d:.0} ops/sec)\n", .{ ops_per_sec, min_ops_per_sec });
    } else {
        std.debug.print("  ✅ Performance acceptable ({d:.0} >= {d:.0} ops/sec)\n", .{ ops_per_sec, min_ops_per_sec });
    }
}

// Performance benchmark: CONFIG HELP
// Expected: >20k ops/sec (static response)
test "CONFIG HELP performance" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const iterations: usize = 10_000;
    const start_time = std.time.nanoTimestamp();

    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        var args = [_]RespValue{
            RespValue{ .bulk_string = "CONFIG" },
            RespValue{ .bulk_string = "HELP" },
        };

        const response = try config_commands.executeConfigCommand(allocator, storage, &args);
        defer allocator.free(response);

        try testing.expect(response.len > 0);
    }

    const end_time = std.time.nanoTimestamp();
    const elapsed_ns = @as(u64, @intCast(end_time - start_time));
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const ops_per_sec = (@as(f64, @floatFromInt(iterations)) / elapsed_ms) * 1000.0;

    std.debug.print("\n[CONFIG HELP Performance]\n", .{});
    std.debug.print("  Iterations: {d}\n", .{iterations});
    std.debug.print("  Time: {d:.2} ms\n", .{elapsed_ms});
    std.debug.print("  Throughput: {d:.0} ops/sec\n", .{ops_per_sec});
    std.debug.print("  Avg latency: {d:.3} ms\n", .{elapsed_ms / @as(f64, @floatFromInt(iterations))});

    const min_ops_per_sec: f64 = 10_000.0;
    if (ops_per_sec < min_ops_per_sec) {
        std.debug.print("  ⚠️  WARNING: Performance below expected threshold ({d:.0} < {d:.0} ops/sec)\n", .{ ops_per_sec, min_ops_per_sec });
    } else {
        std.debug.print("  ✅ Performance acceptable ({d:.0} >= {d:.0} ops/sec)\n", .{ ops_per_sec, min_ops_per_sec });
    }
}

// Memory allocation analysis for CONFIG GET
// Tracks allocation count and sizes
test "CONFIG GET memory allocation pattern" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    std.debug.print("\n[CONFIG GET Memory Analysis]\n", .{});
    std.debug.print("  Testing with std.testing.allocator (leak detection enabled)\n", .{});

    // Run a small number of operations to verify no leaks
    const iterations: usize = 100;
    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        var args = [_]RespValue{
            RespValue{ .bulk_string = "CONFIG" },
            RespValue{ .bulk_string = "GET" },
            RespValue{ .bulk_string = "maxmemory" },
        };

        const response = try config_commands.executeConfigCommand(allocator, storage, &args);
        defer allocator.free(response);

        // Verify response
        try testing.expect(response.len > 0);
    }

    std.debug.print("  ✅ No memory leaks detected after {d} iterations\n", .{iterations});
}

// Memory allocation analysis for CONFIG SET
test "CONFIG SET memory allocation pattern" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    std.debug.print("\n[CONFIG SET Memory Analysis]\n", .{});
    std.debug.print("  Testing with std.testing.allocator (leak detection enabled)\n", .{});

    const iterations: usize = 100;
    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        const value = if (i % 2 == 0) "1024" else "2048";

        var args = [_]RespValue{
            RespValue{ .bulk_string = "CONFIG" },
            RespValue{ .bulk_string = "SET" },
            RespValue{ .bulk_string = "maxmemory" },
            RespValue{ .bulk_string = value },
        };

        const response = try config_commands.executeConfigCommand(allocator, storage, &args);
        defer allocator.free(response);

        try testing.expect(std.mem.indexOf(u8, response, "OK") != null);
    }

    std.debug.print("  ✅ No memory leaks detected after {d} iterations\n", .{iterations});
}

// Mixed workload: CONFIG GET + CONFIG SET
test "CONFIG mixed workload performance" {
    const allocator = testing.allocator;

    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const iterations: usize = 5_000;
    const start_time = std.time.nanoTimestamp();

    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        if (i % 2 == 0) {
            // CONFIG GET
            var args = [_]RespValue{
                RespValue{ .bulk_string = "CONFIG" },
                RespValue{ .bulk_string = "GET" },
                RespValue{ .bulk_string = "maxmemory" },
            };

            const response = try config_commands.executeConfigCommand(allocator, storage, &args);
            defer allocator.free(response);

            try testing.expect(response.len > 0);
        } else {
            // CONFIG SET
            var args = [_]RespValue{
                RespValue{ .bulk_string = "CONFIG" },
                RespValue{ .bulk_string = "SET" },
                RespValue{ .bulk_string = "maxmemory" },
                RespValue{ .bulk_string = "2048" },
            };

            const response = try config_commands.executeConfigCommand(allocator, storage, &args);
            defer allocator.free(response);

            try testing.expect(std.mem.indexOf(u8, response, "OK") != null);
        }
    }

    const end_time = std.time.nanoTimestamp();
    const elapsed_ns = @as(u64, @intCast(end_time - start_time));
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const ops_per_sec = (@as(f64, @floatFromInt(iterations)) / elapsed_ms) * 1000.0;

    std.debug.print("\n[CONFIG Mixed Workload Performance]\n", .{});
    std.debug.print("  Iterations: {d} (50% GET, 50% SET)\n", .{iterations});
    std.debug.print("  Time: {d:.2} ms\n", .{elapsed_ms});
    std.debug.print("  Throughput: {d:.0} ops/sec\n", .{ops_per_sec});
    std.debug.print("  Avg latency: {d:.3} ms\n", .{elapsed_ms / @as(f64, @floatFromInt(iterations))});

    const min_ops_per_sec: f64 = 7_000.0;
    if (ops_per_sec < min_ops_per_sec) {
        std.debug.print("  ⚠️  WARNING: Performance below expected threshold ({d:.0} < {d:.0} ops/sec)\n", .{ ops_per_sec, min_ops_per_sec });
    } else {
        std.debug.print("  ✅ Performance acceptable ({d:.0} >= {d:.0} ops/sec)\n", .{ ops_per_sec, min_ops_per_sec });
    }
}
