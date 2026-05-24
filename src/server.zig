const std = @import("std");
const sailor = @import("sailor");
const protocol = @import("protocol/parser.zig");
const writer_mod = @import("protocol/writer.zig");
const commands = @import("commands/strings.zig");
const storage_mod = @import("storage/memory.zig");
const aof_mod = @import("storage/aof.zig");
const pubsub_mod = @import("storage/pubsub.zig");
const repl_mod = @import("storage/replication.zig");
const client_mod = @import("commands/client.zig");
const scripting_mod = @import("storage/scripting.zig");
const cluster_mod = @import("storage/cluster.zig");

const Parser = protocol.Parser;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;
const Aof = aof_mod.Aof;
const PubSub = pubsub_mod.PubSub;
const ReplicationState = repl_mod.ReplicationState;
const ClientRegistry = client_mod.ClientRegistry;
const ScriptStore = scripting_mod.ScriptStore;
const ClusterState = cluster_mod.ClusterState;

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

/// Server statistics tracking
pub const ServerStats = struct {
    /// Total number of commands processed since server start
    total_commands_processed: std.atomic.Value(u64),
    /// Total number of connections received since server start
    total_connections_received: std.atomic.Value(u64),
    /// Server start time (Unix timestamp in seconds)
    start_time_seconds: i64,

    pub fn init() ServerStats {
        return ServerStats{
            .total_commands_processed = std.atomic.Value(u64).init(0),
            .total_connections_received = std.atomic.Value(u64).init(0),
            .start_time_seconds = std.time.timestamp(),
        };
    }

    pub fn recordCommand(self: *ServerStats) void {
        _ = self.total_commands_processed.fetchAdd(1, .release);
    }

    pub fn recordConnection(self: *ServerStats) void {
        _ = self.total_connections_received.fetchAdd(1, .release);
    }

    pub fn getCommandsProcessed(self: *const ServerStats) u64 {
        return self.total_commands_processed.load(.acquire);
    }

    pub fn getConnectionsReceived(self: *const ServerStats) u64 {
        return self.total_connections_received.load(.acquire);
    }

    pub fn getUptimeSeconds(self: *const ServerStats) i64 {
        return std.time.timestamp() - self.start_time_seconds;
    }
};

/// Shutdown request state
pub const ShutdownRequest = struct {
    save: bool, // true = save RDB before shutdown, false = don't save
    now: bool, // true = immediate shutdown, false = wait for clients
    force: bool, // true = skip save errors, false = abort on save errors
};

/// Shutdown state for the server
pub const ShutdownState = struct {
    requested: std.atomic.Value(bool),
    request: ?ShutdownRequest,
    mutex: std.Thread.Mutex,

    pub fn init() ShutdownState {
        return ShutdownState{
            .requested = std.atomic.Value(bool).init(false),
            .request = null,
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn requestShutdown(self: *ShutdownState, req: ShutdownRequest) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.request = req;
        self.requested.store(true, .release);
    }

    pub fn isRequested(self: *ShutdownState) bool {
        return self.requested.load(.acquire);
    }

    pub fn getRequest(self: *ShutdownState) ?ShutdownRequest {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.request;
    }
};

/// Background task for cluster gossip protocol
/// Periodically sends PING messages to random nodes and updates health status
pub const GossipTask = struct {
    allocator: std.mem.Allocator,
    /// Pointer to the primary database's cluster state
    cluster: *ClusterState,
    /// Thread handle for the background task
    thread: ?std.Thread,
    /// Flag to signal shutdown
    running: std.atomic.Value(bool),
    /// Interval between gossip cycles (milliseconds)
    interval_ms: u64,

    /// Initialize a new gossip task
    pub fn init(allocator: std.mem.Allocator, cluster: *ClusterState, interval_ms: u64) GossipTask {
        return GossipTask{
            .allocator = allocator,
            .cluster = cluster,
            .thread = null,
            .running = std.atomic.Value(bool).init(false),
            .interval_ms = interval_ms,
        };
    }

    /// Start the background gossip task
    /// Uses acquire/release ordering to ensure proper synchronization:
    /// - .acquire on load: ensures we see any prior stores before proceeding
    /// - .release on store: ensures running=true is visible to gossipLoop before thread starts
    pub fn start(self: *GossipTask) !void {
        if (self.running.load(.acquire)) {
            return error.AlreadyRunning;
        }

        // Release: ensure running=true is visible to spawned thread
        self.running.store(true, .release);
        self.thread = try std.Thread.spawn(.{}, gossipLoop, .{self});
    }

    /// Stop the background gossip task
    /// Uses acquire/release ordering for graceful shutdown:
    /// - .acquire on load: check if already stopped
    /// - .release on store: ensure running=false is visible to gossipLoop
    pub fn stop(self: *GossipTask) void {
        if (!self.running.load(.acquire)) {
            return;
        }

        // Release: ensure running=false is visible to gossipLoop thread
        self.running.store(false, .release);

        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }

    /// Main loop for the gossip task.
    /// Runs until the running flag is cleared by stop().
    /// Performs a gossip cycle every interval_ms milliseconds.
    /// Errors during gossip cycles are logged but do not terminate the loop.
    fn gossipLoop(self: *GossipTask) void {
        while (self.running.load(.acquire)) {
            self.performGossipCycle() catch |err| {
                // Log error but continue running
                std.log.err("Cluster gossip cycle failed: {}", .{err});
            };

            // Sleep for the specified interval
            std.Thread.sleep(self.interval_ms * std.time.ns_per_ms);
        }
    }

    /// Perform one gossip cycle: select random node, send PING, update health
    fn performGossipCycle(self: *GossipTask) !void {
        // Skip if cluster has no other nodes
        if (self.cluster.nodes.count() <= 1) {
            return;
        }

        // Select random nodes for gossip
        const gossip_nodes = try self.cluster.selectRandomNodesForGossip(self.allocator, 1);
        defer self.allocator.free(gossip_nodes);

        if (gossip_nodes.len == 0) {
            return;
        }

        // Send PING to the first selected node
        self.cluster.sendPingToNode(gossip_nodes[0].node_id);

        // Update health status for all nodes
        self.cluster.updateNodeHealth();

        // Promote pfail nodes to fail if majority agrees
        self.cluster.promoteFailures();

        // Check if we should start election (we're a replica of a failed master)
        if (self.cluster.shouldStartElection()) {
            // Start election process
            const won = self.cluster.startElection(self.allocator) catch |err| {
                std.log.err("Failed to start election: {}", .{err});
                return;
            };

            if (won) {
                // We won the election, promote to master
                self.cluster.promoteToMaster(self.allocator) catch |err| {
                    std.log.err("Failed to promote to master: {}", .{err});
                };
            }
        }
    }
};

/// TCP server for handling Redis-compatible connections
pub const Server = struct {
    allocator: std.mem.Allocator,
    config: Config,
    databases: []Storage,
    num_databases: u16,
    aof: ?*Aof,
    pubsub: PubSub,
    /// Replication state (always initialised; role depends on config)
    repl: ReplicationState,
    /// Client registry for tracking active connections
    client_registry: ClientRegistry,
    /// Script storage for Lua scripts
    script_store: ScriptStore,
    /// Monotonically increasing connection ID used as subscriber_id.
    next_subscriber_id: u64,
    running: std.atomic.Value(bool),
    /// Shutdown state tracking
    shutdown_state: ShutdownState,
    /// Background gossip task for cluster protocol
    gossip_task: ?GossipTask,
    /// Server statistics tracking (commands processed, connections, uptime)
    stats: ServerStats,

    /// Initialize a new server instance.
    /// If `config.replicaof_host` is set, the server starts as a replica.
    pub fn init(allocator: std.mem.Allocator, config: Config) !*Server {
        const server = try allocator.create(Server);
        errdefer allocator.destroy(server);

        // Initialize databases array (default 16 databases)
        const num_databases: u16 = 16;
        const databases = try allocator.alloc(Storage, num_databases);
        errdefer allocator.free(databases);

        var db_idx: usize = 0;
        errdefer {
            // Free all initialized databases
            for (0..db_idx) |i| {
                databases[i].deinit();
            }
            allocator.free(databases);
        }

        // Initialize each database
        for (0..num_databases) |i| {
            const db_ptr = try Storage.init(allocator, config.port, config.host);
            databases[i] = db_ptr.*;
            db_ptr.allocator.destroy(db_ptr); // Free the pointer wrapper, keep the value
            db_idx += 1;
        }

        // Initialise replication state based on config
        const repl = if (config.replicaof_host != null)
            try ReplicationState.initReplica(allocator, config.replicaof_host.?, config.replicaof_port)
        else
            try ReplicationState.initPrimary(allocator);

        server.* = Server{
            .allocator = allocator,
            .config = config,
            .databases = databases,
            .num_databases = num_databases,
            .aof = null,
            .pubsub = PubSub.init(allocator),
            .repl = repl,
            .client_registry = ClientRegistry.init(allocator),
            .script_store = ScriptStore.init(allocator),
            .next_subscriber_id = 1,
            .running = std.atomic.Value(bool).init(false),
            .shutdown_state = ShutdownState.init(),
            .gossip_task = null,
            .stats = ServerStats.init(),
        };

        // Connect storage instances to pubsub for notifications
        for (server.databases) |*db| {
            db.pubsub_state = &server.pubsub;
        }

        // Initialize gossip task if cluster mode is enabled
        // For now, cluster mode is disabled by default, but we set up the task
        // The task will be started in start() if cluster is enabled
        const GOSSIP_INTERVAL_MS = 100;
        if (databases.len > 0) {
            server.gossip_task = GossipTask.init(allocator, &databases[0].cluster, GOSSIP_INTERVAL_MS);
        }

        return server;
    }

    /// Deinitialize the server and free resources
    pub fn deinit(self: *Server) void {
        // Stop gossip task if running
        if (self.gossip_task) |*task| {
            task.stop();
        }

        if (self.aof) |a| a.close();
        for (self.databases) |*db| {
            db.deinit();
        }
        self.allocator.free(self.databases);
        self.pubsub.deinit();
        self.repl.deinit();
        self.client_registry.deinit();
        self.script_store.deinit();
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
            self.repl.connectToPrimary(&self.databases[0], self.config.port) catch |err| {
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

        // Start gossip task if cluster mode is enabled
        if (self.gossip_task) |*task| {
            if (self.databases.len > 0 and self.databases[0].cluster.enabled) {
                task.start() catch |err| {
                    std.debug.print("Warning: Failed to start gossip task: {any}\n", .{err});
                };
            }
        }

        // Colored startup logs using ANSI escape codes
        const cyan_bold = "\x1b[1;36m";
        const green = "\x1b[32m";
        const magenta = "\x1b[35m";
        const yellow = "\x1b[33m";
        const green_bold = "\x1b[1;32m";
        const reset = "\x1b[0m";

        std.debug.print("{s}Zoltraak{s} server starting...\n", .{ cyan_bold, reset });
        std.debug.print("Listening on {s}{s}:{d}{s}\n", .{ green, self.config.host, self.config.port, reset });
        const role_str: []const u8 = switch (self.repl.role) {
            .primary => "primary",
            .replica => "replica",
        };
        const role_color = switch (self.repl.role) {
            .primary => magenta,
            .replica => yellow,
        };
        std.debug.print("Role: {s}{s}{s}\n", .{ role_color, role_str, reset });

        // Show cluster status if enabled
        if (self.databases.len > 0 and self.databases[0].cluster.enabled) {
            std.debug.print("Cluster: {s}enabled{s} (gossip task running)\n", .{ green, reset });
        }

        std.debug.print("{s}Ready to accept connections.{s}\n", .{ green_bold, reset });

        // Accept connections (single-threaded)
        while (self.running.load(.monotonic)) {
            // Check for shutdown request
            if (self.shutdown_state.isRequested()) {
                try self.performShutdown();
                break;
            }

            // Accept connection with timeout to allow checking running flag
            const connection = listener.accept() catch |err| {
                if (err == error.WouldBlock) continue;
                std.debug.print("Error accepting connection: {any}\n", .{err});
                continue;
            };

            // Record connection statistics
            self.stats.recordConnection();
            self.databases[0].incrementConnectionsReceived();

            // Handle connection
            self.handleConnection(connection) catch |err| {
                std.debug.print("Error handling connection: {any}\n", .{err});
            };
        }
    }

    /// Perform graceful shutdown based on shutdown request
    fn performShutdown(self: *Server) !void {
        const req = self.shutdown_state.getRequest() orelse return;

        std.debug.print("\x1b[1;33mShutdown requested\x1b[0m (save={}, now={}, force={})\n", .{ req.save, req.now, req.force });

        // Save RDB if requested
        if (req.save) {
            std.debug.print("Saving RDB snapshot before shutdown...\n", .{});
            const persistence = @import("storage/persistence.zig");
            persistence.Persistence.save(self.databases, "dump.rdb", self.allocator) catch |err| {
                if (!req.force) {
                    std.debug.print("\x1b[1;31mError saving RDB: {any}\x1b[0m\n", .{err});
                    std.debug.print("Shutdown aborted (use FORCE to override)\n", .{});
                    // Clear shutdown request
                    self.shutdown_state.mutex.lock();
                    self.shutdown_state.request = null;
                    self.shutdown_state.requested.store(false, .release);
                    self.shutdown_state.mutex.unlock();
                    return err;
                }
                std.debug.print("\x1b[1;33mWarning: RDB save failed: {any} (continuing due to FORCE)\x1b[0m\n", .{err});
            };
        }

        // Stop accepting new connections
        self.running.store(false, .monotonic);
        std.debug.print("\x1b[1;32mShutdown complete\x1b[0m\n", .{});
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

        // Format address for client registry
        var addr_buf: [256]u8 = undefined;
        const addr_str = blk: {
            // Extract IP and port from sockaddr_in (network byte order)
            const in = connection.address.in;
            // addr is in network byte order (big-endian), convert to bytes
            const ip_bytes = @as([4]u8, @bitCast(in.sa.addr));
            const port = @byteSwap(in.sa.port); // port is big-endian, swap to little
            const formatted = std.fmt.bufPrint(
                &addr_buf,
                "{d}.{d}.{d}.{d}:{d}",
                .{ ip_bytes[0], ip_bytes[1], ip_bytes[2], ip_bytes[3], port },
            ) catch {
                break :blk "unknown";
            };
            break :blk formatted;
        };

        // Register client connection
        const client_id = try self.client_registry.registerClient(addr_str, connection.stream.handle);
        defer self.client_registry.unregisterClient(client_id);

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

            // Extract command name for client tracking
            const cmd_name = blk: {
                const array = switch (cmd) {
                    .array => |arr| arr,
                    else => break :blk "",
                };
                if (array.len == 0) break :blk "";
                const name = switch (array[0]) {
                    .bulk_string => |s| s,
                    else => break :blk "",
                };
                break :blk name;
            };

            // Get selected database for this client
            const selected_db = self.client_registry.getSelectedDb(client_id);
            const storage = &self.databases[@intCast(selected_db)];

            // Execute command
            const response = commands.executeCommand(
                arena_allocator,
                storage,
                cmd,
                self.aof,
                &self.pubsub,
                subscriber_id,
                &tx,
                &self.repl,
                self.config.port,
                connection.stream,
                this_replica_idx,
                &self.client_registry,
                client_id,
                &self.script_store,
                &self.shutdown_state,
                self.databases,
                self.num_databases,
            ) catch |err| {
                std.debug.print("Command execution error: {any}\n", .{err});
                const error_response = "-ERR Internal server error\r\n";
                _ = connection.stream.write(error_response) catch break;
                continue;
            };

            // Record command execution in statistics
            self.stats.recordCommand();
            self.databases[0].incrementCommandsProcessed();

            // Update last command timestamp
            self.client_registry.updateLastCommand(client_id, cmd_name);

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

            // Deliver any pending RESP3 push invalidation messages that were
            // queued during this command (e.g., a SET/DEL from another connection
            // that this tracking client is subscribed to). In our single-threaded
            // model, pending messages accumulate and are drained here.
            if (self.client_registry.takePendingInvalidations(client_id)) |inv_msgs| {
                defer self.client_registry.allocator.free(inv_msgs);
                for (inv_msgs) |msg| {
                    defer self.client_registry.allocator.free(msg);
                    _ = connection.stream.write(msg) catch |err| {
                        std.debug.print("Invalidation push write error: {any}\n", .{err});
                    };
                }
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
