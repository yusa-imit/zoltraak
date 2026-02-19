const std = @import("std");
const protocol = @import("protocol/parser.zig");
const writer_mod = @import("protocol/writer.zig");
const commands = @import("commands/strings.zig");
const storage_mod = @import("storage/memory.zig");
const aof_mod = @import("storage/aof.zig");
const pubsub_mod = @import("storage/pubsub.zig");
const repl_mod = @import("storage/replication.zig");

const Parser = protocol.Parser;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;
const Aof = aof_mod.Aof;
const PubSub = pubsub_mod.PubSub;
const ReplicationState = repl_mod.ReplicationState;

/// Server configuration
pub const Config = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 6379,
    max_connections: u32 = 10000,
    buffer_size: usize = 4096,
    /// If non-null, this server starts as a replica of the given primary.
    replicaof_host: ?[]const u8 = null,
    replicaof_port: u16 = 0,
};

/// TCP server for handling Redis-compatible connections
pub const Server = struct {
    allocator: std.mem.Allocator,
    config: Config,
    storage: *Storage,
    aof: ?*Aof,
    pubsub: PubSub,
    /// Replication state (always initialised; role depends on config)
    repl: ReplicationState,
    /// Monotonically increasing connection ID used as subscriber_id.
    next_subscriber_id: u64,
    running: std.atomic.Value(bool),

    /// Initialize a new server instance.
    /// If `config.replicaof_host` is set, the server starts as a replica.
    pub fn init(allocator: std.mem.Allocator, config: Config) !*Server {
        const server = try allocator.create(Server);
        errdefer allocator.destroy(server);

        const storage = try Storage.init(allocator);
        errdefer storage.deinit();

        // Initialise replication state based on config
        const repl = if (config.replicaof_host != null)
            try ReplicationState.initReplica(allocator, config.replicaof_host.?, config.replicaof_port)
        else
            try ReplicationState.initPrimary(allocator);

        server.* = Server{
            .allocator = allocator,
            .config = config,
            .storage = storage,
            .aof = null,
            .pubsub = PubSub.init(allocator),
            .repl = repl,
            .next_subscriber_id = 1,
            .running = std.atomic.Value(bool).init(false),
        };

        return server;
    }

    /// Deinitialize the server and free resources
    pub fn deinit(self: *Server) void {
        if (self.aof) |a| a.close();
        self.storage.deinit();
        self.pubsub.deinit();
        self.repl.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    /// Start the server and listen for connections.
    /// If this is a replica, the handshake is performed before accepting clients.
    pub fn start(self: *Server) !void {
        // If we are a replica, connect to the primary first
        if (self.repl.role == .replica) {
            std.debug.print("Replication: initiating handshake with primary {s}:{d}\n", .{
                self.repl.primary_host orelse "?",
                self.repl.primary_port,
            });
            self.repl.connectToPrimary(self.storage, self.config.port) catch |err| {
                std.debug.print("Replication: handshake failed: {any} — continuing as standalone\n", .{err});
                // Demote to primary on failure so clients can still connect
                self.repl.role = .primary;
            };
        }

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
        const role_str: []const u8 = switch (self.repl.role) {
            .primary => "primary",
            .replica => "replica",
        };
        std.debug.print("Role: {s}\n", .{role_str});
        std.debug.print("Ready to accept connections.\n", .{});

        // Accept connections (single-threaded)
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

    /// Handle a single client connection.
    ///
    /// Detects if the connecting client is a replica performing PSYNC by watching
    /// for the PSYNC command. After PSYNC, the connection transitions to a command
    /// stream receiver and the function returns.
    fn handleConnection(self: *Server, connection: std.net.Server.Connection) !void {
        defer connection.stream.close();

        std.debug.print("Client connected from {any}\n", .{connection.address});

        // Assign a unique subscriber ID for this connection
        const subscriber_id = self.next_subscriber_id;
        self.next_subscriber_id += 1;

        // Ensure subscriber state is cleaned up when client disconnects
        defer self.pubsub.unsubscribeAll(subscriber_id) catch {};

        // Per-connection transaction state (MULTI/EXEC/DISCARD/WATCH)
        var tx = commands.TxState.init(self.allocator);
        defer tx.deinit();

        // Track replica index if this connection performs PSYNC
        var this_replica_idx: ?usize = null;

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

            // Detect if this is a PSYNC command (replica connecting to us)
            const is_psync = detectPsync(cmd);

            if (is_psync and this_replica_idx == null) {
                // Register this connection as a new replica
                self.repl.addReplica(connection.stream, 0) catch |err| {
                    std.debug.print("Replication: could not register replica: {any}\n", .{err});
                };
                this_replica_idx = if (self.repl.replicas.items.len > 0)
                    self.repl.replicas.items.len - 1
                else
                    null;
            }

            // Execute command
            const response = commands.executeCommand(
                arena_allocator,
                self.storage,
                cmd,
                self.aof,
                &self.pubsub,
                subscriber_id,
                &tx,
                &self.repl,
                self.config.port,
                connection.stream,
                this_replica_idx,
            ) catch |err| {
                std.debug.print("Command execution error: {any}\n", .{err});
                const error_response = "-ERR Internal server error\r\n";
                _ = connection.stream.write(error_response) catch break;
                continue;
            };

            // Write command response (skip empty responses, e.g. after PSYNC which
            // already wrote directly to the stream)
            if (response.len > 0) {
                _ = connection.stream.write(response) catch |err| {
                    std.debug.print("Write error: {any}\n", .{err});
                    break;
                };
            }

            // After PSYNC, a replica connection enters streaming mode.
            // We no longer read commands from it; it is driven by propagation.
            if (is_psync) {
                std.debug.print("Replication: replica synced, connection will remain for propagation\n", .{});
                // Keep the stream alive for propagation; the loop below drains any
                // messages the replica sends (e.g., REPLCONF ACK).
                // For Iteration 10, we simply exit the handler; the replica stream
                // is tracked in repl.replicas and written to by propagate().
                break;
            }

            // Deliver any pending pub/sub messages that were queued during
            // this command (e.g., a PUBLISH from another connection's cycle
            // that this subscriber is waiting for). In our single-threaded
            // model, pending messages accumulate and are drained here.
            const pending = self.pubsub.pendingMessages(subscriber_id);
            if (pending.len > 0) {
                // Write all pending message frames back-to-back
                for (pending) |msg_frame| {
                    _ = connection.stream.write(msg_frame) catch |err| {
                        std.debug.print("Pub/Sub write error: {any}\n", .{err});
                        break;
                    };
                }
                self.pubsub.drainMessages(subscriber_id);
            }

            // Reset arena for next command
            _ = arena.reset(.retain_capacity);
        }

        std.debug.print("Client disconnected\n", .{});
    }

    /// Return true if `cmd` is a PSYNC command.
    fn detectPsync(cmd: protocol.RespValue) bool {
        const array = switch (cmd) {
            .array => |arr| arr,
            else => return false,
        };
        if (array.len == 0) return false;
        const name = switch (array[0]) {
            .bulk_string => |s| s,
            else => return false,
        };
        return std.ascii.eqlIgnoreCase(name, "PSYNC");
    }
};

// Note: Server tests would require integration testing with actual TCP connections.
// Unit tests are provided for the individual components (parser, writer, storage, commands).
