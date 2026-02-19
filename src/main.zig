const std = @import("std");
const server_mod = @import("server.zig");

// Re-export modules for testing
pub const protocol = @import("protocol/parser.zig");
pub const writer = @import("protocol/writer.zig");
pub const storage = @import("storage/memory.zig");
pub const commands = @import("commands/strings.zig");
pub const sorted_sets = @import("commands/sorted_sets.zig");
pub const persistence = @import("storage/persistence.zig");
pub const aof = @import("storage/aof.zig");
pub const pubsub = @import("storage/pubsub.zig");
pub const pubsub_commands = @import("commands/pubsub.zig");
pub const replication = @import("storage/replication.zig");
pub const replication_commands = @import("commands/replication.zig");

const Server = server_mod.Server;
const Config = server_mod.Config;

/// Print usage information
fn printUsage(program_name: []const u8) void {
    std.debug.print("Usage: {s} [OPTIONS]\n\n", .{program_name});
    std.debug.print("Options:\n", .{});
    std.debug.print("  --host HOST              Bind address (default: 127.0.0.1)\n", .{});
    std.debug.print("  --port PORT              Listen port (default: 6379)\n", .{});
    std.debug.print("  --replicaof HOST PORT    Replicate from primary at HOST:PORT\n", .{});
    std.debug.print("  --help                   Show this help message\n", .{});
    std.debug.print("\n", .{});
    std.debug.print("Examples:\n", .{});
    std.debug.print("  {s} --host 0.0.0.0 --port 6380\n", .{program_name});
    std.debug.print("  {s} --port 6380 --replicaof 127.0.0.1 6379\n", .{program_name});
}

/// Parsed command-line arguments, including owned strings that must be freed.
const ParsedArgs = struct {
    config: Config,
    /// Owned copy of --host value (or null if default was used)
    host_owned: ?[]u8,
    /// Owned copy of --replicaof host (or null if not provided)
    replicaof_host_owned: ?[]u8,

    fn deinit(self: *ParsedArgs, allocator: std.mem.Allocator) void {
        if (self.host_owned) |h| allocator.free(h);
        if (self.replicaof_host_owned) |h| allocator.free(h);
    }
};

/// Parse command-line arguments into a Config.
fn parseArgs(allocator: std.mem.Allocator) !ParsedArgs {
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    var parsed = ParsedArgs{
        .config = Config{},
        .host_owned = null,
        .replicaof_host_owned = null,
    };

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
            parsed.host_owned = try allocator.dupe(u8, host);
            parsed.config.host = parsed.host_owned.?;
        } else if (std.mem.eql(u8, arg, "--port")) {
            const port_str = args.next() orelse {
                std.debug.print("Error: --port requires an argument\n", .{});
                return error.InvalidArgument;
            };
            parsed.config.port = std.fmt.parseInt(u16, port_str, 10) catch {
                std.debug.print("Error: invalid port number '{s}'\n", .{port_str});
                return error.InvalidArgument;
            };
        } else if (std.mem.eql(u8, arg, "--replicaof")) {
            const host = args.next() orelse {
                std.debug.print("Error: --replicaof requires HOST PORT arguments\n", .{});
                return error.InvalidArgument;
            };
            const port_str = args.next() orelse {
                std.debug.print("Error: --replicaof requires HOST PORT arguments\n", .{});
                return error.InvalidArgument;
            };
            const port = std.fmt.parseInt(u16, port_str, 10) catch {
                std.debug.print("Error: invalid port number '{s}'\n", .{port_str});
                return error.InvalidArgument;
            };
            parsed.replicaof_host_owned = try allocator.dupe(u8, host);
            parsed.config.replicaof_host = parsed.replicaof_host_owned.?;
            parsed.config.replicaof_port = port;
        } else {
            std.debug.print("Error: unknown option '{s}'\n", .{arg});
            std.debug.print("Use --help for usage information\n", .{});
            return error.InvalidArgument;
        }
    }

    return parsed;
}

pub fn main() !void {
    // Set up allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Parse command-line arguments
    var parsed = parseArgs(allocator) catch |err| {
        if (err == error.InvalidArgument) {
            std.process.exit(1);
        }
        return err;
    };
    defer parsed.deinit(allocator);

    const config = parsed.config;

    // Initialize server
    const server = try Server.init(allocator, config);
    defer server.deinit();

    // Load RDB snapshot if it exists (skip when starting as a replica — RDB comes from primary)
    if (config.replicaof_host == null) {
        const Persistence = persistence.Persistence;
        const loaded = Persistence.load(server.storage, "dump.rdb", allocator) catch |err| blk: {
            std.debug.print("Warning: could not load dump.rdb: {any}\n", .{err});
            break :blk 0;
        };
        if (loaded > 0) {
            std.debug.print("Loaded {d} keys from dump.rdb\n", .{loaded});
        }

        // Replay AOF if it exists (applied on top of RDB)
        const Aof = aof.Aof;
        const replayed = Aof.replay(server.storage, "appendonly.aof", allocator) catch |err| blk: {
            std.debug.print("Warning: could not replay appendonly.aof: {any}\n", .{err});
            break :blk 0;
        };
        if (replayed > 0) {
            std.debug.print("Replayed {d} commands from appendonly.aof\n", .{replayed});
        }

        // Open AOF for appending (creates file if not present)
        const Aof2 = aof.Aof;
        server.aof = Aof2.open("appendonly.aof") catch |err| blk: {
            std.debug.print("Warning: could not open appendonly.aof for writing: {any}\n", .{err});
            break :blk null;
        };
    } else {
        std.debug.print("Replica mode: skipping local RDB/AOF load (will receive from primary)\n", .{});
    }

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
        .mask = std.posix.sigemptyset(),
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
