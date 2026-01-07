const std = @import("std");
const server_mod = @import("server.zig");

// Re-export modules for testing
pub const protocol = @import("protocol/parser.zig");
pub const writer = @import("protocol/writer.zig");
pub const storage = @import("storage/memory.zig");
pub const commands = @import("commands/strings.zig");

const Server = server_mod.Server;
const Config = server_mod.Config;

/// Print usage information
fn printUsage(program_name: []const u8) void {
    std.debug.print("Usage: {s} [OPTIONS]\n\n", .{program_name});
    std.debug.print("Options:\n", .{});
    std.debug.print("  --host HOST    Bind address (default: 127.0.0.1)\n", .{});
    std.debug.print("  --port PORT    Listen port (default: 6379)\n", .{});
    std.debug.print("  --help         Show this help message\n", .{});
    std.debug.print("\n", .{});
    std.debug.print("Example:\n", .{});
    std.debug.print("  {s} --host 0.0.0.0 --port 6380\n", .{program_name});
}

/// Parse command-line arguments
fn parseArgs(allocator: std.mem.Allocator) !Config {
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    var config = Config{};
    var host_owned: ?[]u8 = null;

    // Skip program name
    _ = args.next();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage("zoltraak");
            std.process.exit(0);
        } else if (std.mem.eql(u8, arg, "--host")) {
            const host = args.next() orelse {
                std.debug.print("Error: --host requires an argument\n", .{});
                return error.InvalidArgument;
            };
            host_owned = try allocator.dupe(u8, host);
            config.host = host_owned.?;
        } else if (std.mem.eql(u8, arg, "--port")) {
            const port_str = args.next() orelse {
                std.debug.print("Error: --port requires an argument\n", .{});
                return error.InvalidArgument;
            };
            config.port = std.fmt.parseInt(u16, port_str, 10) catch {
                std.debug.print("Error: invalid port number '{s}'\n", .{port_str});
                return error.InvalidArgument;
            };
        } else {
            std.debug.print("Error: unknown option '{s}'\n", .{arg});
            std.debug.print("Use --help for usage information\n", .{});
            return error.InvalidArgument;
        }
    }

    return config;
}

pub fn main() !void {
    // Set up allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Parse command-line arguments
    const config = parseArgs(allocator) catch |err| {
        if (err == error.InvalidArgument) {
            std.process.exit(1);
        }
        return err;
    };
    const default_config = Config{};
    defer if (config.host.ptr != default_config.host.ptr) allocator.free(config.host);

    // Initialize server
    const server = try Server.init(allocator, config);
    defer server.deinit();

    // Set up signal handler for graceful shutdown
    const sigint_handler = struct {
        var srv: ?*Server = null;

        fn handle(sig: i32) callconv(.c) void {
            _ = sig;
            if (srv) |s| {
                std.debug.print("\nReceived interrupt signal, shutting down...\n", .{});
                s.stop();
            }
        }
    };
    sigint_handler.srv = server;

    // Register signal handler (POSIX systems)
    const act = std.posix.Sigaction{
        .handler = .{ .handler = sigint_handler.handle },
        .mask = 0,
        .flags = 0,
    };
    _ = std.posix.sigaction(std.posix.SIG.INT, &act, null);
    _ = std.posix.sigaction(std.posix.SIG.TERM, &act, null);

    // Start server (blocks until shutdown)
    try server.start();
}

// Minimal test to ensure modules compile
test "main - modules import correctly" {
    const allocator = std.testing.allocator;
    _ = allocator;
    // If we got here, all imports are valid
    try std.testing.expect(true);
}
