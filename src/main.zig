const std = @import("std");
const sailor = @import("sailor");
const server_mod = @import("server.zig");

// Re-export modules for testing
pub const protocol = @import("protocol/parser.zig");
pub const writer = @import("protocol/writer.zig");
pub const server = server_mod;
pub const storage = @import("storage/memory.zig");
pub const commands = @import("commands/strings.zig");
pub const sorted_sets = @import("commands/sorted_sets.zig");
pub const persistence = @import("storage/persistence.zig");
pub const aof = @import("storage/aof.zig");
pub const pubsub = @import("storage/pubsub.zig");
pub const pubsub_commands = @import("commands/pubsub.zig");
pub const replication = @import("storage/replication.zig");
pub const replication_commands = @import("commands/replication.zig");
pub const sentinel = @import("storage/sentinel.zig");
pub const client = @import("commands/client.zig");
pub const modules = @import("storage/modules.zig");
pub const hyperloglog_commands = @import("commands/hyperloglog.zig");
pub const streams_commands = @import("commands/streams.zig");
pub const streams_advanced_commands = @import("commands/streams_advanced.zig");
pub const keys_commands = @import("commands/keys.zig");
pub const lists_commands = @import("commands/lists.zig");
pub const sets_commands = @import("commands/sets.zig");
pub const blocking = @import("storage/blocking.zig");
pub const acl_commands = @import("commands/acl.zig");
pub const acl_storage = @import("storage/acl.zig");
pub const auth_commands = @import("commands/auth.zig");
pub const scripting_storage = @import("storage/scripting.zig");
pub const transactions_commands = @import("commands/transactions.zig");
pub const utility_commands = @import("commands/utility.zig");
pub const json_value = @import("storage/json_value.zig");
pub const json_commands = @import("commands/json.zig");
pub const tui_advanced = @import("tui_advanced.zig");
pub const bloom_commands = @import("commands/bloom.zig");
pub const timeseries_storage = @import("storage/timeseries.zig");
pub const cluster_commands = @import("commands/cluster.zig");
pub const cuckoo_storage = @import("storage/cuckoo.zig");

// Re-export common types
pub const ClientRegistry = client.ClientRegistry;
pub const ClientInfo = client.ClientInfo;
pub const BlockingQueue = blocking.BlockingQueue;

const Server = server_mod.Server;
const Config = server_mod.Config;

/// Usage text buffer size; the rendered text is well under 1 KiB for any sane program name.
const usage_size_max = 1024;

/// Render usage information into `buf`. Returns `error.NoSpaceLeft` when `buf` is too small.
fn usageText(buf: []u8, program_name: []const u8) std.fmt.BufPrintError![]const u8 {
    return std.fmt.bufPrint(buf,
        \\Usage: {[name]s} [OPTIONS]
        \\
        \\Options:
        \\  --host HOST                Bind address (default: 127.0.0.1)
        \\  -p, --port PORT            Listen port (default: 6379)
        \\  --replicaof HOST:PORT      Replicate from primary at HOST:PORT
        \\  -h, --help                 Show this help message
        \\
        \\Examples:
        \\  {[name]s} --host 0.0.0.0 --port 6380
        \\  {[name]s} --port 6380 --replicaof 127.0.0.1:6379
        \\
    , .{ .name = program_name });
}

/// Print usage information to stderr. Help output is the program's own product, not a
/// diagnostic, so it bypasses `std.log` (which would prefix every line with a level).
fn printUsage(program_name: []const u8) void {
    var buf: [usage_size_max]u8 = undefined;
    const text = usageText(&buf, program_name) catch |err| switch (err) {
        error.NoSpaceLeft => {
            std.log.err("usage text exceeds {d} bytes", .{usage_size_max});
            return;
        },
    };
    std.fs.File.stderr().writeAll(text) catch |err| {
        std.log.err("could not write usage text: {s}", .{@errorName(err)});
    };
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
    // Define flags using sailor
    const flags = [_]sailor.arg.FlagDef{
        .{ .name = "help", .short = 'h', .type = .bool, .help = "Show this help message" },
        .{ .name = "host", .type = .string, .default = "127.0.0.1", .help = "Bind address (default: 127.0.0.1)" },
        .{ .name = "port", .short = 'p', .type = .int, .default = "6379", .help = "Listen port (default: 6379)" },
        .{ .name = "replicaof", .type = .string, .help = "Replicate from primary at HOST:PORT (format: HOST:PORT)" },
    };

    // Initialize parser
    var parser = sailor.arg.Parser(&flags).init(allocator);
    defer parser.deinit();

    // Get args (skip program name)
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.next(); // Skip program name

    // Collect args into slice
    var arg_list = std.ArrayList([]const u8).empty;
    defer arg_list.deinit(allocator);
    while (args.next()) |arg| {
        try arg_list.append(allocator, arg);
    }

    // Parse with sailor
    parser.parse(arg_list.items) catch |err| {
        if (err == error.UnknownFlag) {
            std.log.err("unknown option", .{});
            std.log.info("use --help for usage information", .{});
            return error.InvalidArgument;
        }
        return err;
    };

    // Check for --help
    if (parser.getBool("help", false)) {
        printUsage("zoltraak");
        std.process.exit(0);
    }

    var parsed = ParsedArgs{
        .config = Config{},
        .host_owned = null,
        .replicaof_host_owned = null,
    };

    // Get host
    const host = parser.getString("host", "127.0.0.1");
    parsed.host_owned = try allocator.dupe(u8, host);
    parsed.config.host = parsed.host_owned.?;

    // Get port
    parsed.config.port = @intCast(parser.getInt("port", 6379));

    // Handle --replicaof if provided
    if (parser.get("replicaof")) |replica_val| {
        const replica_str = try replica_val.asString();

        // Parse HOST:PORT format
        if (std.mem.indexOf(u8, replica_str, ":")) |colon_pos| {
            const host_part = replica_str[0..colon_pos];
            const port_part = replica_str[colon_pos + 1 ..];

            const port = std.fmt.parseInt(u16, port_part, 10) catch {
                std.log.err("invalid port in --replicaof '{s}'", .{replica_str});
                return error.InvalidArgument;
            };

            parsed.replicaof_host_owned = try allocator.dupe(u8, host_part);
            parsed.config.replicaof_host = parsed.replicaof_host_owned.?;
            parsed.config.replicaof_port = port;
        } else {
            std.log.err("--replicaof requires HOST:PORT format (e.g., 127.0.0.1:6379)", .{});
            return error.InvalidArgument;
        }
    }

    return parsed;
}

pub fn main() !void {
    // Set up allocator
    var gpa = std.heap.DebugAllocator(.{}){};
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
    var server_instance = try Server.init(allocator, config);
    defer server_instance.deinit();

    // Load RDB snapshot if it exists (skip when starting as a replica — RDB comes from primary)
    if (config.replicaof_host == null) {
        const Persistence = persistence.Persistence;
        const loaded = Persistence.load(server_instance.databases, "dump.rdb", allocator) catch |err| blk: {
            std.log.warn("could not load dump.rdb: {any}", .{err});
            break :blk 0;
        };
        if (loaded > 0) {
            std.log.info("loaded {d} keys from dump.rdb", .{loaded});
        }

        // Replay AOF if it exists (applied on top of RDB)
        const Aof = aof.Aof;
        const replayed = Aof.replay(&server_instance.databases[0], "appendonly.aof", allocator) catch |err| blk: {
            std.log.warn("could not replay appendonly.aof: {any}", .{err});
            break :blk 0;
        };
        if (replayed > 0) {
            std.log.info("replayed {d} commands from appendonly.aof", .{replayed});
        }

        // Open AOF for appending (creates file if not present)
        const Aof2 = aof.Aof;
        server_instance.aof = Aof2.open("appendonly.aof") catch |err| blk: {
            std.log.warn("could not open appendonly.aof for writing: {any}", .{err});
            break :blk null;
        };
    } else {
        std.log.info("replica mode: skipping local RDB/AOF load (will receive from primary)", .{});
    }

    // Set up signal handler for graceful shutdown
    const sigint_handler = struct {
        var srv: ?*Server = null;

        fn handle(sig: i32) callconv(.c) void {
            _ = sig;
            if (srv) |s| {
                std.log.info("received interrupt signal, shutting down", .{});
                s.stop();
            }
        }
    };
    sigint_handler.srv = server_instance;

    // Register signal handler (POSIX systems)
    const act = std.posix.Sigaction{
        .handler = .{ .handler = sigint_handler.handle },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    _ = std.posix.sigaction(std.posix.SIG.INT, &act, null);
    _ = std.posix.sigaction(std.posix.SIG.TERM, &act, null);

    // Start server (blocks until shutdown)
    try server_instance.start();
}

test "usageText - names every option and substitutes the program name" {
    var buf: [1024]u8 = undefined;
    const text = try usageText(&buf, "zoltraak-test");
    try std.testing.expect(std.mem.startsWith(u8, text, "Usage: zoltraak-test [OPTIONS]\n"));
    for ([_][]const u8{ "--host HOST", "-p, --port PORT", "--replicaof HOST:PORT", "-h, --help" }) |opt| {
        try std.testing.expect(std.mem.indexOf(u8, text, opt) != null);
    }
    try std.testing.expect(std.mem.indexOf(u8, text, "{s}") == null);
    try std.testing.expect(std.mem.endsWith(u8, text, "--replicaof 127.0.0.1:6379\n"));
}

test "usageText - buffer too small returns NoSpaceLeft" {
    var buf: [16]u8 = undefined;
    try std.testing.expectError(error.NoSpaceLeft, usageText(&buf, "zoltraak"));
}

// Zig only runs `test` blocks of files that something analyzes; these two were imported but
// their tests never ran, so name them here. Wire further files in one at a time: most other
// files carry stale tests that do not compile yet.
test "main - storage modules are reachable from the test root" {
    _ = @import("storage/topk.zig");
    _ = @import("storage/heavykeeper.zig");
    _ = @import("storage/encodings.zig");
    _ = @import("storage/listpack.zig");
    _ = @import("storage/memory_tracker.zig");
    _ = @import("storage/slowlog.zig");
    _ = @import("storage/intset.zig");
    _ = @import("storage/latency.zig");
}

// Minimal test to ensure modules compile
test "main - modules import correctly" {
    const allocator = std.testing.allocator;
    _ = allocator;
    // If we got here, all imports are valid
    try std.testing.expect(true);
}
