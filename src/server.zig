const std = @import("std");
const protocol = @import("protocol/parser.zig");
const writer_mod = @import("protocol/writer.zig");
const commands = @import("commands/strings.zig");
const storage_mod = @import("storage/memory.zig");

const Parser = protocol.Parser;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;

/// Server configuration
pub const Config = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 6379,
    max_connections: u32 = 10000,
    buffer_size: usize = 4096,
};

/// TCP server for handling Redis-compatible connections
pub const Server = struct {
    allocator: std.mem.Allocator,
    config: Config,
    storage: *Storage,
    running: std.atomic.Value(bool),

    /// Initialize a new server instance
    pub fn init(allocator: std.mem.Allocator, config: Config) !*Server {
        const server = try allocator.create(Server);
        errdefer allocator.destroy(server);

        const storage = try Storage.init(allocator);
        errdefer storage.deinit();

        server.* = Server{
            .allocator = allocator,
            .config = config,
            .storage = storage,
            .running = std.atomic.Value(bool).init(false),
        };

        return server;
    }

    /// Deinitialize the server and free resources
    pub fn deinit(self: *Server) void {
        self.storage.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    /// Start the server and listen for connections
    pub fn start(self: *Server) !void {
        // Parse address
        const address = try std.net.Address.parseIp(self.config.host, self.config.port);

        // Create listener
        var listener = try address.listen(.{
            .reuse_address = true,
        });
        defer listener.deinit();

        self.running.store(true, .monotonic);

        std.debug.print("Zoltraak server starting...\n", .{});
        std.debug.print("Listening on {s}:{d}\n", .{ self.config.host, self.config.port });
        std.debug.print("Ready to accept connections.\n", .{});

        // Accept connections (single-threaded for Iteration 1)
        while (self.running.load(.monotonic)) {
            // Accept connection with timeout to allow checking running flag
            const connection = listener.accept() catch |err| {
                if (err == error.WouldBlock) continue;
                std.debug.print("Error accepting connection: {any}\n", .{err});
                continue;
            };

            // Handle connection
            self.handleConnection(connection) catch |err| {
                std.debug.print("Error handling connection: {any}\n", .{err});
            };
        }
    }

    /// Stop the server gracefully
    pub fn stop(self: *Server) void {
        self.running.store(false, .monotonic);
    }

    /// Handle a single client connection
    fn handleConnection(self: *Server, connection: std.net.Server.Connection) !void {
        defer connection.stream.close();

        std.debug.print("Client connected from {any}\n", .{connection.address});

        // Create arena allocator for this connection
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const arena_allocator = arena.allocator();

        // Read buffer
        var read_buffer: [4096]u8 = undefined;

        // Connection loop
        while (true) {
            // Read data from socket
            const bytes_read = connection.stream.read(&read_buffer) catch |err| {
                if (err == error.EndOfStream) break;
                std.debug.print("Read error: {any}\n", .{err});
                break;
            };

            if (bytes_read == 0) break;

            const data = read_buffer[0..bytes_read];

            // Parse command
            var parser = Parser.init(arena_allocator);
            const cmd = parser.parse(data) catch |err| {
                std.debug.print("Parse error: {any}\n", .{err});
                const error_response = "-ERR Protocol error\r\n";
                _ = connection.stream.write(error_response) catch break;
                continue;
            };

            // Execute command
            const response = commands.executeCommand(arena_allocator, self.storage, cmd) catch |err| {
                std.debug.print("Command execution error: {any}\n", .{err});
                const error_response = "-ERR Internal server error\r\n";
                _ = connection.stream.write(error_response) catch break;
                continue;
            };

            // Write response
            _ = connection.stream.write(response) catch |err| {
                std.debug.print("Write error: {any}\n", .{err});
                break;
            };

            // Reset arena for next command
            _ = arena.reset(.retain_capacity);
        }

        std.debug.print("Client disconnected\n", .{});
    }
};

// Note: Server tests would require integration testing with actual TCP connections
// Unit tests are provided for the individual components (parser, writer, storage, commands)
