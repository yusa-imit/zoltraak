const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");
const persistence_mod = @import("../storage/persistence.zig");
const aof_mod = @import("../storage/aof.zig");
const pubsub_mod = @import("../storage/pubsub.zig");
const repl_mod = @import("../storage/replication.zig");
const repl_cmds = @import("replication.zig");
const lists = @import("lists.zig");
const sets = @import("sets.zig");
const hashes = @import("hashes.zig");
pub const hashes_cmds = hashes;
const sorted_sets = @import("sorted_sets.zig");
const streams = @import("streams.zig");
const streams_adv = @import("streams_advanced.zig");
const pubsub_cmds = @import("pubsub.zig");
const tx_mod = @import("transactions.zig");
pub const keys_cmds = @import("keys.zig");
const client_cmds = @import("client.zig");
const config_cmds = @import("config.zig");
const command_cmds = @import("command.zig");
const bits_cmds = @import("bits.zig");
const bitfield_cmds = @import("bitfield.zig");
const geo_cmds = @import("geo.zig");
const hll_cmds = @import("hyperloglog.zig");
const introspection_cmds = @import("introspection.zig");
const info_cmds = @import("info.zig");
const server_cmds = @import("server_commands.zig");
const scripting_cmds = @import("scripting.zig");
const scripting_mod = @import("../storage/scripting.zig");
const acl_cmds = @import("acl.zig");
const auth_cmds = @import("auth.zig");
const cluster_cmds = @import("cluster.zig");
const sentinel_cmds = @import("sentinel.zig");
const function_cmds = @import("functions.zig");
const json_cmds = @import("json.zig");
const search_cmds = @import("search.zig");
const search_agg_cmds = @import("search_aggregate.zig");
const timeseries_cmds = @import("timeseries.zig");
const bloom_cmds = @import("bloom.zig");
const cuckoo_cmds = @import("cuckoo.zig");
const cms_cmds = @import("cms.zig");
const topk_cmds = @import("topk.zig");
const tdigest_cmds = @import("tdigest.zig");
const vector_cmds = @import("vectors.zig");
const utility_cmds = @import("utility.zig");
const hotkeys_cmds = @import("hotkeys.zig");
const modules_cmds = @import("modules.zig");
const acl_storage = @import("../storage/acl.zig");
const ACLStore = acl_storage.ACLStore;
const AclUser = acl_storage.User;
const command_registry = @import("command_registry.zig");
const cluster_mod = @import("../storage/cluster.zig");
const notifications_mod = @import("../storage/notifications.zig");

/// Access mode for key-based command permission checking
pub const AccessMode = enum { read, write };
pub const TxState = tx_mod.TxState;
pub const ReplicationState = repl_mod.ReplicationState;
pub const ScriptStore = scripting_mod.ScriptStore;

const RespValue = protocol.RespValue;
const RespType = protocol.RespType;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;
const Value = storage_mod.Value;
const ConfigValue = @import("../storage/config.zig").ConfigValue;
const Persistence = persistence_mod.Persistence;
pub const Aof = aof_mod.Aof;
pub const PubSub = pubsub_mod.PubSub;
pub const ClientRegistry = client_cmds.ClientRegistry;
const RespProtocol = client_cmds.RespProtocol;
const MonitorMessage = client_cmds.MonitorMessage;

/// Default RDB file path
const DEFAULT_RDB_PATH = "dump.rdb";
/// Default AOF file path
const DEFAULT_AOF_PATH = "appendonly.aof";

/// Get the RESP protocol version for a client (defaults to RESP2 if not found)
fn getClientProtocol(client_registry: *ClientRegistry, client_id: u64) RespProtocol {
    client_registry.mutex.lock();
    defer client_registry.mutex.unlock();

    if (client_registry.clients.get(client_id)) |client_info| {
        return client_info.protocol;
    }
    return .RESP2; // Default to RESP2
}

/// Check ACL key permissions for a command.
/// Returns null if all key permissions are granted, or an error message if denied.
/// Caller must free the returned error message if non-null.
fn checkKeyPermissions(
    allocator: std.mem.Allocator,
    user: *const AclUser,
    cmd_upper: []const u8,
    args: []const RespValue,
) !?[]const u8 {
    // Classify command access mode and extract key positions
    const access_mode = getCommandAccessMode(cmd_upper);
    if (access_mode == null) {
        // Command doesn't operate on keys — no check needed
        return null;
    }

    const key_positions = getCommandKeyPositions(cmd_upper, args);
    if (key_positions.len == 0) {
        // No keys in this command — no check needed
        return null;
    }

    // Check each key against ACL permissions
    for (key_positions) |key_idx| {
        if (key_idx >= args.len) continue; // Safety check
        const key = switch (args[key_idx]) {
            .bulk_string => |s| s,
            else => continue,
        };

        // Check permission using User.hasKeyPermission()
        const mode_str = if (access_mode.? == .read) "read" else "write";
        if (!user.hasKeyPermission(key, mode_str)) {
            // Permission denied — format error message
            const err_msg = try std.fmt.allocPrint(
                allocator,
                "NOPERM this user has no permissions to access key '{s}'",
                .{key},
            );
            return err_msg;
        }
    }

    // All keys passed permission check
    return null;
}

/// Get the access mode for a command (Read or Write).
/// Returns null for commands that don't operate on keys.
fn getCommandAccessMode(cmd_upper: []const u8) ?AccessMode {
    // Read commands
    const read_commands = [_][]const u8{
        "GET", "MGET", "EXISTS", "TTL", "PTTL", "TYPE", "STRLEN",
        "HGET", "HMGET", "HGETALL", "HKEYS", "HVALS", "HLEN", "HEXISTS",
        "LLEN", "LINDEX", "LRANGE", "LPOS",
        "SMEMBERS", "SCARD", "SISMEMBER", "SMISMEMBER", "SRANDMEMBER",
        "ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE", "ZRANGEBYLEX",
        "ZREVRANGEBYLEX", "ZCARD", "ZCOUNT", "ZLEXCOUNT", "ZSCORE", "ZMSCORE",
        "ZRANK", "ZREVRANK", "ZRANDMEMBER", "ZDIFF", "ZINTER", "ZUNION",
        "SDIFF", "SINTER", "SUNION",
        "XLEN", "XRANGE", "XREVRANGE", "XREAD", "XREADGROUP", "XPENDING", "XINFO",
        "GETRANGE", "SUBSTR", "GETBIT", "BITCOUNT", "BITPOS", "BITFIELD_RO",
        "GEODIST", "GEOPOS", "GEORADIUS", "GEORADIUSBYMEMBER", "GEOHASH", "GEOSEARCH",
        "PFCOUNT",
        "DUMP", "OBJECT", "LCS", "SORT_RO", "HRANDFIELD", "HSCAN",
        "TS.GET", "TS.MGET", "TS.RANGE", "TS.REVRANGE", "TS.MRANGE", "TS.MREVRANGE", "TS.QUERYINDEX", "TS.INFO",
        "VCARD", "VDIM", "VEMB", "VISMEMBER", "VRANDMEMBER", "VGETATTR", "VINFO", "VSIM", "VRANGE", "VLINKS",
        "EVAL_RO", "EVALSHA_RO", "FCALL_RO",
    };
    for (read_commands) |rc| {
        if (std.mem.eql(u8, cmd_upper, rc)) return .read;
    }

    // Write commands
    const write_commands = [_][]const u8{
        "SET", "SETEX", "PSETEX", "SETNX", "MSET", "MSETNX", "MSETEX",
        "SETRANGE", "APPEND", "INCR", "DECR", "INCRBY", "DECRBY", "INCRBYFLOAT",
        "GETSET", "GETDEL", "GETEX",
        "DEL", "UNLINK", "RENAME", "RENAMENX", "COPY", "MOVE",
        "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST", "TOUCH",
        "HSET", "HMSET", "HSETNX", "HINCRBY", "HINCRBYFLOAT", "HDEL",
        "HGETDEL", "HGETEX", "HSETEX",
        "LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LPOP", "RPOP", "LSET",
        "LINSERT", "LREM", "LTRIM", "LMOVE", "RPOPLPUSH", "BLPOP", "BRPOP",
        "BLMOVE", "BRPOPLPUSH", "LMPOP", "BLMPOP",
        "SADD", "SREM", "SPOP", "SMOVE", "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE",
        "ZADD", "ZREM", "ZINCRBY", "ZPOPMIN", "ZPOPMAX", "ZMPOP", "BZPOPMIN",
        "BZPOPMAX", "BZMPOP", "ZRANGESTORE", "ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE",
        "XADD", "XDEL", "XTRIM", "XSETID", "XCFGSET", "XGROUP", "XACK", "XCLAIM",
        "XAUTOCLAIM", "XACKDEL", "XDELEX",
        "SETBIT", "BITOP", "BITFIELD",
        "GEOADD", "GEOSEARCHSTORE", "PFADD", "PFMERGE",
        "RESTORE", "SORT", "DELEX", "MIGRATE",
        "TS.CREATE", "TS.ADD", "TS.MADD", "TS.INCRBY", "TS.DECRBY", "TS.DEL", "TS.ALTER", "TS.CREATERULE", "TS.DELETERULE",
        "TOPK.RESERVE", "TOPK.ADD", "TOPK.INCRBY",
        "TDIGEST.CREATE", "TDIGEST.ADD", "TDIGEST.RESET", "TDIGEST.MERGE",
        "VADD", "VREM", "VSETATTR",
    };
    for (write_commands) |wc| {
        if (std.mem.eql(u8, cmd_upper, wc)) return .write;
    }

    // Commands that don't operate on keys
    return null;
}

/// Public wrapper for getCommandAccessMode (for use by scripting layer)
/// Returns AccessMode enum (.read or .write) or null for non-key commands
pub fn classifyCommand(cmd_name: []const u8) ?AccessMode {
    const cmd_upper = std.ascii.allocUpperString(std.heap.page_allocator, cmd_name) catch return null;
    defer std.heap.page_allocator.free(cmd_upper);
    return getCommandAccessMode(cmd_upper);
}

/// Get the argument positions containing keys for a command.
/// Returns a slice of indexes (0-based) into the args array.
/// This is a simplified version — production Redis uses command metadata.
fn getCommandKeyPositions(cmd_upper: []const u8, args: []const RespValue) []const usize {
    // Static array to hold key positions (max 64 keys per command)
    const static = struct {
        var positions: [64]usize = undefined;
    };

    var count: usize = 0;

    // Single-key commands (key at position 1)
    const single_key_commands = [_][]const u8{
        "GET", "SET", "SETEX", "PSETEX", "SETNX", "GETSET", "GETDEL", "GETEX",
        "APPEND", "STRLEN", "SETRANGE", "GETRANGE",
        "INCR", "DECR", "INCRBY", "DECRBY", "INCRBYFLOAT",
        "EXISTS", "DEL", "UNLINK", "TYPE", "TTL", "PTTL",
        "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST", "TOUCH",
        "DUMP", "OBJECT",
        "HSET", "HGET", "HMSET", "HMGET", "HGETALL", "HDEL", "HEXISTS",
        "HINCRBY", "HINCRBYFLOAT", "HKEYS", "HVALS", "HLEN", "HSETNX",
        "HGETDEL", "HGETEX", "HSETEX", "HRANDFIELD", "HSCAN",
        "LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LPOP", "RPOP", "LLEN",
        "LINDEX", "LSET", "LRANGE", "LTRIM", "LREM", "LINSERT", "LPOS", "LMPOP",
        "SADD", "SREM", "SMEMBERS", "SISMEMBER", "SMISMEMBER", "SCARD",
        "SPOP", "SRANDMEMBER",
        "ZADD", "ZREM", "ZCARD", "ZCOUNT", "ZLEXCOUNT", "ZRANGE", "ZREVRANGE",
        "ZRANGEBYSCORE", "ZREVRANGEBYSCORE", "ZRANGEBYLEX", "ZREVRANGEBYLEX",
        "ZSCORE", "ZMSCORE", "ZRANK", "ZREVRANK", "ZINCRBY", "ZPOPMIN", "ZPOPMAX",
        "ZMPOP", "ZRANDMEMBER",
        "XADD", "XDEL", "XTRIM", "XLEN", "XRANGE", "XREVRANGE", "XREAD",
        "XREADGROUP", "XPENDING", "XINFO", "XSETID", "XCFGSET", "XGROUP",
        "XACK", "XCLAIM", "XAUTOCLAIM", "XACKDEL", "XDELEX",
        "SETBIT", "GETBIT", "BITCOUNT", "BITPOS", "BITOP", "BITFIELD", "BITFIELD_RO",
        "GEOADD", "GEODIST", "GEOPOS", "GEORADIUS", "GEORADIUSBYMEMBER",
        "GEOHASH", "GEOSEARCH", "GEOSEARCHSTORE",
        "PFADD", "PFCOUNT", "PFMERGE",
        "LCS", "DELEX", "SORT", "SORT_RO",
    };
    for (single_key_commands) |cmd| {
        if (std.mem.eql(u8, cmd_upper, cmd)) {
            if (args.len > 1) {
                static.positions[0] = 1; // Key is at position 1
                count = 1;
            }
            return static.positions[0..count];
        }
    }

    // Multi-key commands
    if (std.mem.eql(u8, cmd_upper, "MGET") or std.mem.eql(u8, cmd_upper, "DEL") or std.mem.eql(u8, cmd_upper, "UNLINK") or std.mem.eql(u8, cmd_upper, "EXISTS") or std.mem.eql(u8, cmd_upper, "TOUCH")) {
        // All arguments after command name are keys
        for (1..args.len) |i| {
            if (count >= 64) break; // Prevent overflow
            static.positions[count] = i;
            count += 1;
        }
        return static.positions[0..count];
    }

    if (std.mem.eql(u8, cmd_upper, "MSET") or std.mem.eql(u8, cmd_upper, "MSETNX") or std.mem.eql(u8, cmd_upper, "MSETEX")) {
        // Keys at odd positions: 1, 3, 5, ... (key-value pairs)
        var i: usize = 1;
        while (i < args.len) : (i += 2) {
            if (count >= 64) break;
            static.positions[count] = i;
            count += 1;
        }
        return static.positions[0..count];
    }

    if (std.mem.eql(u8, cmd_upper, "RENAME") or std.mem.eql(u8, cmd_upper, "RENAMENX")) {
        // Two keys: source and destination
        if (args.len > 2) {
            static.positions[0] = 1; // source key
            static.positions[1] = 2; // dest key
            count = 2;
        }
        return static.positions[0..count];
    }

    if (std.mem.eql(u8, cmd_upper, "COPY") or std.mem.eql(u8, cmd_upper, "LMOVE") or std.mem.eql(u8, cmd_upper, "BLMOVE") or std.mem.eql(u8, cmd_upper, "BRPOPLPUSH") or std.mem.eql(u8, cmd_upper, "RPOPLPUSH") or std.mem.eql(u8, cmd_upper, "SMOVE")) {
        // Two keys: source and destination
        if (args.len > 2) {
            static.positions[0] = 1;
            static.positions[1] = 2;
            count = 2;
        }
        return static.positions[0..count];
    }

    if (std.mem.eql(u8, cmd_upper, "BLPOP") or std.mem.eql(u8, cmd_upper, "BRPOP") or std.mem.eql(u8, cmd_upper, "BLMPOP") or std.mem.eql(u8, cmd_upper, "BZPOPMIN") or std.mem.eql(u8, cmd_upper, "BZPOPMAX") or std.mem.eql(u8, cmd_upper, "BZMPOP")) {
        // All arguments except last (timeout) are keys
        if (args.len > 2) {
            for (1..args.len - 1) |i| {
                if (count >= 64) break;
                static.positions[count] = i;
                count += 1;
            }
        }
        return static.positions[0..count];
    }

    // Set operations with destination
    if (std.mem.eql(u8, cmd_upper, "SUNIONSTORE") or std.mem.eql(u8, cmd_upper, "SINTERSTORE") or std.mem.eql(u8, cmd_upper, "SDIFFSTORE") or std.mem.eql(u8, cmd_upper, "ZUNIONSTORE") or std.mem.eql(u8, cmd_upper, "ZINTERSTORE") or std.mem.eql(u8, cmd_upper, "ZDIFFSTORE") or std.mem.eql(u8, cmd_upper, "ZRANGESTORE")) {
        // First key is destination, rest are source keys
        // All are keys and need write permission to destination
        for (1..args.len) |i| {
            if (count >= 64) break;
            // Stop at WEIGHTS/AGGREGATE keywords for sorted set operations
            const arg_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => continue,
            };
            if (std.mem.eql(u8, arg_str, "WEIGHTS") or std.mem.eql(u8, arg_str, "AGGREGATE")) break;
            static.positions[count] = i;
            count += 1;
        }
        return static.positions[0..count];
    }

    // Set operations without destination (read-only)
    if (std.mem.eql(u8, cmd_upper, "SUNION") or std.mem.eql(u8, cmd_upper, "SINTER") or std.mem.eql(u8, cmd_upper, "SDIFF") or std.mem.eql(u8, cmd_upper, "ZUNION") or std.mem.eql(u8, cmd_upper, "ZINTER") or std.mem.eql(u8, cmd_upper, "ZDIFF")) {
        // All arguments after numkeys are keys
        // Format: ZUNION numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...]
        if (args.len > 2) {
            // Skip command name and numkeys
            const numkeys_arg = switch (args[1]) {
                .bulk_string => |s| std.fmt.parseInt(usize, s, 10) catch 0,
                else => 0,
            };
            for (2..@min(2 + numkeys_arg, args.len)) |i| {
                if (count >= 64) break;
                static.positions[count] = i;
                count += 1;
            }
        }
        return static.positions[0..count];
    }

    // No keys found
    return static.positions[0..0];
}

/// Publish keyspace notification if enabled.
/// This is a helper to call after successful command execution.
/// - event_flag: the notification type (e.g., .string, .generic, .list)
/// - event_name: the event name (e.g., "set", "del", "lpush")
fn notifyKeyspaceEvent(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub_state: *PubSub,
    db_index: u32,
    key: []const u8,
    event_flag: notifications_mod.NotificationFlag,
    event_name: []const u8,
) void {
    // Get notification flags from config
    const config_value = storage.config.getAsString("notify-keyspace-events") catch return;
    const config_str = config_value orelse return;
    defer allocator.free(config_str);

    const flags = notifications_mod.parseNotificationFlags(config_str);

    // Check if this event type should fire
    if (!notifications_mod.shouldNotify(flags, event_flag)) {
        return;
    }

    // Publish notification (ignore errors — notifications are non-critical)
    notifications_mod.publishNotification(
        allocator,
        pubsub_state,
        db_index,
        key,
        event_name,
        flags,
    ) catch {};
}

/// Execute a RESP command and return the serialized response.
/// Caller owns returned memory and must free it.
/// If `aof` is non-null, write commands are appended to it after successful execution.
/// `ps` and `subscriber_id` are used for SUBSCRIBE / UNSUBSCRIBE / PUBLISH / PUBSUB commands.
/// `tx` holds per-connection transaction state for MULTI/EXEC/DISCARD/WATCH.
/// `repl` holds the replication state; write commands are propagated to replicas.
///   Pass null to disable replication (e.g., during AOF replay or internal use).
/// `my_port` is this server's listen port, sent to primary during REPLCONF handshake.
/// `replica_stream` is set when this connection is from a replica performing PSYNC.
/// `client_registry` and `client_id` are used for CLIENT commands.
/// `shutdown_state` is used for SHUTDOWN command.
pub fn executeCommand(
    allocator: std.mem.Allocator,
    storage: *Storage,
    cmd: RespValue,
    aof: ?*Aof,
    ps: *PubSub,
    subscriber_id: u64,
    tx: *TxState,
    repl: ?*ReplicationState,
    my_port: u16,
    replica_stream: ?std.net.Stream,
    replica_idx: ?usize,
    client_registry: *ClientRegistry,
    client_id: u64,
    script_store: *ScriptStore,
    shutdown_state: ?*@import("../server.zig").ShutdownState,
    databases: []Storage,
    num_databases: u16,
) ![]const u8 {
    const array = switch (cmd) {
        .array => |arr| arr,
        else => {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR expected array");
        },
    };

    if (array.len == 0) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR empty command");
    }

    const cmd_name = switch (array[0]) {
        .bulk_string => |s| s,
        else => {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR invalid command format");
        },
    };

    // Command dispatch (case-insensitive)
    const cmd_upper = try std.ascii.allocUpperString(allocator, cmd_name);
    defer allocator.free(cmd_upper);

    // ── NOAUTH / ACL Permission Check ────────────────────────────────────────
    // Commands always allowed before authentication (per Redis spec)
    const always_allowed = std.mem.eql(u8, cmd_upper, "AUTH") or
        std.mem.eql(u8, cmd_upper, "HELLO") or
        std.mem.eql(u8, cmd_upper, "PING") or
        std.mem.eql(u8, cmd_upper, "QUIT") or
        std.mem.eql(u8, cmd_upper, "RESET");

    if (!always_allowed) {
        // NOAUTH enforcement: if requirepass is configured, clients must authenticate first
        if (!client_registry.isAuthenticated(client_id)) {
            const rp = try storage.config.getAsString("requirepass");
            defer if (rp) |p| allocator.free(p);
            if (rp) |requirepass| {
                if (requirepass.len > 0) {
                    // Log NOAUTH violation
                    if (storage.acl) |acl_store| {
                        const client_addr = try client_registry.getClientAddr(client_id, allocator);
                        defer allocator.free(client_addr);
                        acl_store.addLogEntry(.auth, cmd_upper, "(noauth)", client_addr);
                    }
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    return w.writeError("NOAUTH Authentication required.");
                }
            }
        }

        // Get authenticated user (defaults to "default" if unauthenticated)
        const username = try client_registry.getAuthenticatedUser(client_id, allocator);
        defer allocator.free(username);

        // Get ACL store
        if (storage.acl) |acl_store| {
            // Get user from ACL
            if (acl_store.getUser(username)) |user| {
                // Check if user has permission for this command
                if (!user.hasCommandPermission(cmd_upper)) {
                    const client_addr = try client_registry.getClientAddr(client_id, allocator);
                    defer allocator.free(client_addr);
                    acl_store.addLogEntry(.command, cmd_upper, username, client_addr);
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    return w.writeError("NOPERM this user has no permissions to run this command");
                }

                // ── ACL Key Permission Check ──────────────────────────────────
                // Extract keys from command and check key-level permissions
                const key_check_result = try checkKeyPermissions(
                    allocator,
                    user,
                    cmd_upper,
                    array,
                );
                if (key_check_result) |err_msg| {
                    defer allocator.free(err_msg);
                    // Log key permission violation
                    const client_addr = try client_registry.getClientAddr(client_id, allocator);
                    defer allocator.free(client_addr);
                    acl_store.addLogEntry(.key, cmd_upper, username, client_addr);
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    return w.writeError(err_msg);
                }
            } else {
                // User not found in ACL - deny
                const client_addr = try client_registry.getClientAddr(client_id, allocator);
                defer allocator.free(client_addr);
                acl_store.addLogEntry(.command, cmd_upper, username, client_addr);
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("NOPERM user not found in ACL");
            }
        }
    }

    // ── MONITOR: Broadcast command to monitoring clients ──────────────────────
    // Broadcast to all monitoring clients BEFORE execution (except MONITOR command itself)
    if (!std.mem.eql(u8, cmd_upper, "MONITOR") and !std.mem.eql(u8, cmd_upper, "QUIT")) {
        // Extract command arguments from RespValue array
        var cmd_args = try allocator.alloc([]const u8, array.len);
        defer allocator.free(cmd_args);
        for (array, 0..) |arg, i| {
            cmd_args[i] = switch (arg) {
                .bulk_string => |s| s,
                .simple_string => |s| s,
                else => "",
            };
        }

        // Get current timestamp
        const now = std.time.milliTimestamp();
        const timestamp_sec = @divTrunc(now, 1000);
        const timestamp_usec = @mod(now, 1000) * 1000;

        // Get client address from registry
        const client_addr = client_registry.getClientAddr(client_id, allocator) catch
            try allocator.dupe(u8, "127.0.0.1:0");
        defer allocator.free(client_addr);

        // Get selected database for this client
        const client_db: u8 = @truncate(client_registry.getSelectedDb(client_id));

        // Broadcast to monitoring clients (we don't await responses, fire-and-forget)
        var monitor_messages = client_registry.broadcastToMonitors(
            allocator,
            timestamp_sec,
            timestamp_usec,
            client_db,
            client_addr,
            cmd_args,
        ) catch |err| blk: {
            // Log error but don't fail the command
            std.debug.print("MONITOR broadcast error: {}\n", .{err});
            break :blk std.ArrayList(MonitorMessage){};
        };
        // Clean up monitor messages
        for (monitor_messages.items) |msg| {
            allocator.free(msg.message);
        }
        monitor_messages.deinit(allocator);
    }

    // ── Cluster: Slot redirect check ──────────────────────────────────────────
    // Check if the command should be redirected to another node (ASK or MOVED)
    // Skip redirect check for non-key commands and cluster management commands
    const skip_redirect_cmds = [_][]const u8{
        "PING", "INFO", "AUTH", "HELLO", "CLUSTER", "SENTINEL", "ASKING", "MIGRATE",
        "READONLY", "READWRITE",
        "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH",
        "SUBSCRIBE", "PSUBSCRIBE", "PUBLISH", "PUBSUB",
        "CLIENT", "CONFIG", "COMMAND", "ACL", "SCRIPT", "MODULE",
    };
    var should_check_redirect = true;
    for (skip_redirect_cmds) |skip_cmd| {
        if (std.mem.eql(u8, cmd_upper, skip_cmd)) {
            should_check_redirect = false;
            break;
        }
    }

    if (should_check_redirect and storage.cluster.enabled) {
        // Extract the first key from command arguments
        // Most commands have the key as the second argument (array[1])
        var key_arg: ?[]const u8 = null;
        if (array.len >= 2) {
            key_arg = switch (array[1]) {
                .bulk_string => |s| s,
                else => null,
            };
        }

        if (key_arg) |key| {
            const slot = cluster_mod.keySlot(key);
            const client_has_asking = storage.cluster.hasAsking(client_id);

            // Check for ASK redirect (slot is MIGRATING)
            if (storage.cluster.shouldAskRedirect(slot, client_has_asking)) |dest_node| {
                var w = Writer.init(allocator);
                defer w.deinit();
                var buf: [256]u8 = undefined;
                const redirect_msg = try std.fmt.bufPrint(
                    &buf,
                    "ASK {d} {s}:{d}",
                    .{slot, dest_node.addr, dest_node.port}
                );
                return w.writeError(redirect_msg);
            }

            // Check for MOVED redirect (slot not owned by current node)
            if (storage.cluster.shouldMovedRedirect(slot, client_has_asking)) |dest_node| {
                var w = Writer.init(allocator);
                defer w.deinit();
                var buf: [256]u8 = undefined;
                const redirect_msg = try std.fmt.bufPrint(
                    &buf,
                    "MOVED {d} {s}:{d}",
                    .{slot, dest_node.addr, dest_node.port}
                );
                return w.writeError(redirect_msg);
            }

            // Clear ASKING flag after command execution check
            // (it's a one-time flag that must be reset after each command)
            defer storage.cluster.clearAsking(client_id);
        }
    }

    // ── Replication: read-only guard ──────────────────────────────────────────
    // When this instance is a replica, reject write commands.
    // Replication protocol commands (REPLCONF, PSYNC) are always allowed.
    if (repl) |r| {
        if (r.role == .replica) {
            const replication_cmds = [_][]const u8{
                "REPLCONF", "PSYNC", "PING", "INFO", "REPLICAOF", "SLAVEOF", "WAIT", "FAILOVER", "ROLE",
            };
            var is_repl_cmd = false;
            for (replication_cmds) |rc| {
                if (std.mem.eql(u8, cmd_upper, rc)) {
                    is_repl_cmd = true;
                    break;
                }
            }
            const write_cmds = [_][]const u8{
                "SET",        "DEL",        "LPUSH",      "RPUSH",      "LPOP",
                "RPOP",       "SADD",       "SREM",       "HSET",       "HMSET",      "HDEL",
                "ZADD",       "ZREM",       "FLUSHDB",    "FLUSHALL",
                "EXPIRE",     "PEXPIRE",    "EXPIREAT",   "PEXPIREAT",  "PERSIST",
                "INCR",       "DECR",       "INCRBY",     "DECRBY",     "INCRBYFLOAT",
                "APPEND",     "GETSET",     "GETDEL",     "GETEX",
                "SETNX",      "SETEX",      "PSETEX",
                "MSET",       "MSETNX",     "MSETEX",     "RENAME",     "RENAMENX",
                "UNLINK",
                "DUMP",       "RESTORE",    "COPY",       "TOUCH",      "MOVE",
                "HINCRBY",    "HINCRBYFLOAT", "HSETNX",
                "HGETDEL",    "HGETEX",     "HSETEX",
                "ZINCRBY",    "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE",
                "ZRANGESTORE", "ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE",
                "LSET",       "LTRIM",      "LREM",       "LPUSHX",     "RPUSHX",
                "LINSERT",    "LMOVE",      "RPOPLPUSH",  "BLPOP",      "BRPOP",
                "BLMOVE",     "LMPOP",      "BLMPOP",
                "SPOP",       "SMOVE",      "ZPOPMIN",    "ZPOPMAX",    "ZMPOP",
                "BZPOPMIN",   "BZPOPMAX",   "BZMPOP",     "SETRANGE",
                "SETBIT",     "BITOP",      "BITFIELD",
                "XADD",       "XDEL",       "XTRIM",      "XSETID",     "XCFGSET",
                "XGROUP",     "XACK",       "XCLAIM",     "XAUTOCLAIM",
                "GEOADD",     "PFADD",      "PFMERGE",
                "BF.ADD",     "BF.RESERVE", "BF.MADD",    "BF.INSERT",  "BF.INFO",
                "CF.ADD",     "CF.RESERVE", "CF.ADDNX",   "CF.INSERT",  "CF.INSERTNX", "CF.DEL",
                "CMS.INITBYDIM", "CMS.INITBYPROB", "CMS.INCRBY", "CMS.MERGE",
                "TOPK.RESERVE", "TOPK.ADD",
                "TDIGEST.CREATE", "TDIGEST.ADD", "TDIGEST.RESET", "TDIGEST.MERGE",
            };
            var is_write = false;
            for (write_cmds) |wc| {
                if (std.mem.eql(u8, cmd_upper, wc)) {
                    is_write = true;
                    break;
                }
            }
            if (is_write and !is_repl_cmd) {
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("READONLY You can't write against a read only replica.");
            }
        }
    }

    // ── Subscription mode guard ───────────────────────────────────────────────
    // When a client has active subscriptions, only pub/sub-related commands and
    // PING/RESET/QUIT are allowed. All other commands return an error matching Redis behavior.
    {
        const total_subs = ps.channelCount(subscriber_id) +
            ps.patternCount(subscriber_id) +
            ps.shardedChannelCount(subscriber_id);
        if (total_subs > 0) {
            const allowed_in_sub_mode =
                std.mem.eql(u8, cmd_upper, "SUBSCRIBE") or
                std.mem.eql(u8, cmd_upper, "UNSUBSCRIBE") or
                std.mem.eql(u8, cmd_upper, "PSUBSCRIBE") or
                std.mem.eql(u8, cmd_upper, "PUNSUBSCRIBE") or
                std.mem.eql(u8, cmd_upper, "SSUBSCRIBE") or
                std.mem.eql(u8, cmd_upper, "SUNSUBSCRIBE") or
                std.mem.eql(u8, cmd_upper, "PING") or
                std.mem.eql(u8, cmd_upper, "RESET") or
                std.mem.eql(u8, cmd_upper, "QUIT");
            if (!allowed_in_sub_mode) {
                var w = Writer.init(allocator);
                defer w.deinit();
                var buf: [192]u8 = undefined;
                const msg = std.fmt.bufPrint(
                    &buf,
                    "ERR Can't call '{s}' in subscription mode",
                    .{cmd_name},
                ) catch "ERR Command not allowed in subscription mode";
                return w.writeError(msg);
            }
        }
    }

    // ── Transaction command handling ──────────────────────────────────────────
    // MULTI, EXEC, DISCARD, WATCH, UNWATCH are always handled immediately,
    // even inside a MULTI block.
    if (std.mem.eql(u8, cmd_upper, "MULTI")) {
        const result = tx_mod.cmdMulti(allocator, tx, array);
        // After MULTI, multi_count=0 (transaction started, 0 commands queued)
        if (tx.active) client_registry.updateTxCounts(client_id, 0, @intCast(tx.watched_keys.items.len));
        return result;
    } else if (std.mem.eql(u8, cmd_upper, "DISCARD")) {
        const result = tx_mod.cmdDiscard(allocator, tx, array);
        // After DISCARD, multi_count=-1 (not in transaction), watch_count=0
        client_registry.updateTxCounts(client_id, -1, 0);
        return result;
    } else if (std.mem.eql(u8, cmd_upper, "WATCH")) {
        const result = tx_mod.cmdWatch(allocator, tx, array);
        // After WATCH, update watch_count with real watched key count
        const multi: i32 = if (tx.active) @intCast(tx.queue.items.len) else -1;
        client_registry.updateTxCounts(client_id, multi, @intCast(tx.watched_keys.items.len));
        return result;
    } else if (std.mem.eql(u8, cmd_upper, "UNWATCH")) {
        const result = tx_mod.cmdUnwatch(allocator, tx, array);
        // After UNWATCH, watch_count=0
        const multi: i32 = if (tx.active) @intCast(tx.queue.items.len) else -1;
        client_registry.updateTxCounts(client_id, multi, 0);
        return result;
    } else if (std.mem.eql(u8, cmd_upper, "EXEC")) {
        const result = try cmdExec(allocator, storage, aof, ps, subscriber_id, tx, repl, my_port, client_registry, client_id, script_store, shutdown_state, databases, num_databases);
        // After EXEC, transaction is finished: multi_count=-1, watch_count=0
        client_registry.updateTxCounts(client_id, -1, 0);
        return result;
    }

    // When inside a MULTI block, queue all other commands and return +QUEUED.
    if (tx.active) {
        // Re-encode the parsed command to RESP bytes for queuing.
        // MUST use tx.allocator (not the per-request arena allocator) because
        // qc.data lives until tx.reset() frees it with tx.allocator. Using the
        // arena allocator here causes an "Invalid free" when tx.reset() runs.
        var enc_w = Writer.init(tx.allocator);
        defer enc_w.deinit();
        const encoded = try enc_w.serialize(cmd);
        errdefer tx.allocator.free(encoded);
        try tx.enqueue(encoded);

        // Update multi_count to reflect newly queued command
        client_registry.updateTxCounts(client_id, @intCast(tx.queue.items.len), @intCast(tx.watched_keys.items.len));

        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeSimpleString("QUEUED");
    }

    // Determine if this is a write command that should be AOF-logged
    const is_write_cmd = blk: {
        const write_cmds = [_][]const u8{
            "SET",        "DEL",        "LPUSH",      "RPUSH",      "LPOP",
            "RPOP",       "SADD",       "SREM",       "HSET",       "HMSET",      "HDEL",
            "ZADD",       "ZREM",       "FLUSHDB",    "FLUSHALL",
            "EXPIRE",     "PEXPIRE",    "EXPIREAT",   "PEXPIREAT",  "PERSIST",
            "INCR",       "DECR",       "INCRBY",     "DECRBY",     "INCRBYFLOAT",
            "APPEND",     "GETSET",     "GETDEL",     "GETEX",
            "SETNX",      "SETEX",      "PSETEX",
            "MSET",       "MSETNX",     "MSETEX",     "RENAME",     "RENAMENX",
            "UNLINK",
            "DUMP",       "RESTORE",    "COPY",       "TOUCH",      "MOVE",
            "HINCRBY",    "HINCRBYFLOAT", "HSETNX",
            "HGETDEL",    "HGETEX",     "HSETEX",
            "ZINCRBY",    "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE",
            "ZRANGESTORE", "ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE",
            "LSET",       "LTRIM",      "LREM",       "LPUSHX",     "RPUSHX",
            "LINSERT",    "LMOVE",      "RPOPLPUSH",  "BLPOP",      "BRPOP",
            "BLMOVE",     "LMPOP",      "BLMPOP",
            "SPOP",       "SMOVE",      "ZPOPMIN",    "ZPOPMAX",    "ZMPOP",
            "BZPOPMIN",   "BZPOPMAX",   "BZMPOP",     "SETRANGE",
            "SETBIT",     "BITOP",      "BITFIELD",
            "XADD",       "XDEL",       "XTRIM",      "XSETID",     "XGROUP",
            "XACK",       "XCLAIM",     "XAUTOCLAIM",
            "GEOADD",     "PFADD",      "PFMERGE",
            "BF.ADD",     "BF.RESERVE", "BF.MADD",    "BF.INSERT",  "BF.LOADCHUNK",
            "CF.ADD",     "CF.RESERVE", "CF.ADDNX",   "CF.INSERT",  "CF.INSERTNX", "CF.DEL",
        };
        for (write_cmds) |wc| {
            if (std.mem.eql(u8, cmd_upper, wc)) break :blk true;
        }
        break :blk false;
    };

    // Execute command
    const response = blk: {
        // ── MODULE COMMAND DISPATCH ────────────────────────────────────────────
        // Check for dynamically loaded module commands BEFORE built-in commands
        // This allows modules to override built-in commands (Redis behavior)
        if (storage.module_store.getCommand(cmd_upper)) |module_cmd| {
            // Module command found - execute via module handler
            break :blk try module_cmd.cmdfunc(allocator, storage, array);
        }

        // Server & Auth commands
        if (std.mem.eql(u8, cmd_upper, "AUTH")) {
            break :blk try auth_cmds.cmdAuth(allocator, array, storage, client_registry, client_id);
        } else if (std.mem.eql(u8, cmd_upper, "HELLO")) {
            const args_slice = if (array.len > 1) array[1..] else array[0..0];
            break :blk try server_cmds.cmdHello(allocator, client_registry, client_id, storage, args_slice);
        } else if (std.mem.eql(u8, cmd_upper, "ASKING")) {
            // Convert RespValue array to string args
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            for (array, 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }
            break :blk try cluster_cmds.cmdAsking(allocator, args, storage, null, client_id);
        } else if (std.mem.eql(u8, cmd_upper, "MIGRATE")) {
            // Convert RespValue array to string args
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            for (array, 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }
            break :blk try cluster_cmds.cmdMigrate(allocator, args, storage, null, client_id);
        } else if (std.mem.eql(u8, cmd_upper, "READONLY")) {
            // Convert RespValue array to string args
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            for (array, 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }
            break :blk try cluster_cmds.cmdReadonly(allocator, args, storage, null, client_id);
        } else if (std.mem.eql(u8, cmd_upper, "READWRITE")) {
            // Convert RespValue array to string args
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            for (array, 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }
            break :blk try cluster_cmds.cmdReadwrite(allocator, args, storage, null, client_id);
        }
        // String commands
        else if (std.mem.eql(u8, cmd_upper, "PING")) {
            // In subscription mode, PING returns a push-style 3-element array, not +PONG.
            const sub_count = ps.channelCount(subscriber_id) +
                ps.patternCount(subscriber_id) +
                ps.shardedChannelCount(subscriber_id);
            if (sub_count > 0) {
                const resp_ver: u8 = if (getClientProtocol(client_registry, client_id) == RespProtocol.RESP3) 3 else 2;
                const msg: []const u8 = if (array.len >= 2) switch (array[1]) {
                    .bulk_string => |s| s,
                    else => "",
                } else "";
                break :blk try pubsub_mod.buildPingFrame(allocator, msg, resp_ver);
            }
            break :blk try cmdPing(allocator, array);
        } else if (std.mem.eql(u8, cmd_upper, "SET")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try cmdSet(allocator, storage, array, ps, selected_db, client_registry, client_id);
        } else if (std.mem.eql(u8, cmd_upper, "GET")) {
            break :blk try cmdGet(allocator, storage, array, client_registry, client_id);
        } else if (std.mem.eql(u8, cmd_upper, "DEL")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try cmdDel(allocator, storage, array, ps, selected_db, client_registry, client_id);
        } else if (std.mem.eql(u8, cmd_upper, "EXISTS")) {
            break :blk try cmdExists(allocator, storage, array);
        }
        // String counter commands
        else if (std.mem.eql(u8, cmd_upper, "INCR")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try cmdIncr(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "DECR")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try cmdDecr(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "INCRBY")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try cmdIncrby(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "DECRBY")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try cmdDecrby(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "INCRBYFLOAT")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try cmdIncrbyfloat(allocator, storage, array, protocol_version);
        }
        // String utility commands
        else if (std.mem.eql(u8, cmd_upper, "APPEND")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try cmdAppend(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "STRLEN")) {
            break :blk try cmdStrlen(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "GETSET")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try cmdGetset(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "GETDEL")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try cmdGetdel(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "GETEX")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try cmdGetex(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "SETNX")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try cmdSetnx(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "SETEX")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try cmdSetex(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "PSETEX")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try cmdPsetex(allocator, storage, array, ps, selected_db);
        }
        // Multi-key string commands
        else if (std.mem.eql(u8, cmd_upper, "MGET")) {
            break :blk try cmdMget(allocator, storage, array, client_registry, client_id);
        } else if (std.mem.eql(u8, cmd_upper, "MSET")) {
            break :blk try cmdMset(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "MSETNX")) {
            break :blk try cmdMsetnx(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "MSETEX")) {
            break :blk try cmdMsetex(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LCS")) {
            break :blk try cmdLcs(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "DIGEST")) {
            break :blk try cmdDigest(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "DELEX")) {
            break :blk try cmdDelex(allocator, storage, array);
        }
        // TTL / expiry commands
        else if (std.mem.eql(u8, cmd_upper, "TTL")) {
            break :blk try keys_cmds.cmdTtl(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "PTTL")) {
            break :blk try keys_cmds.cmdPttl(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "EXPIRETIME")) {
            break :blk try keys_cmds.cmdExpiretime(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "PEXPIRETIME")) {
            break :blk try keys_cmds.cmdPexpiretime(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "EXPIRE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try keys_cmds.cmdExpire(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "PEXPIRE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try keys_cmds.cmdPexpire(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "EXPIREAT")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try keys_cmds.cmdExpireat(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "PEXPIREAT")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try keys_cmds.cmdPexpireat(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "PERSIST")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try keys_cmds.cmdPersist(allocator, storage, array, ps, selected_db);
        }
        // Keyspace commands
        else if (std.mem.eql(u8, cmd_upper, "TYPE")) {
            break :blk try keys_cmds.cmdType(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "KEYS")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try keys_cmds.cmdKeys(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "RENAME")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try keys_cmds.cmdRename(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "RENAMENX")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try keys_cmds.cmdRenamenx(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "RANDOMKEY")) {
            break :blk try keys_cmds.cmdRandomkey(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "UNLINK")) {
            break :blk try keys_cmds.cmdUnlink(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "DUMP")) {
            break :blk try keys_cmds.cmdDump(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "RESTORE")) {
            break :blk try keys_cmds.cmdRestore(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "COPY")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try keys_cmds.cmdCopy(allocator, storage, array, databases, num_databases, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "TOUCH")) {
            break :blk try keys_cmds.cmdTouch(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "MOVE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try keys_cmds.cmdMove(allocator, storage, array, databases, num_databases, selected_db);
        }
        // List commands
        else if (std.mem.eql(u8, cmd_upper, "LPUSH")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try lists.cmdLpush(allocator, storage, array, ps, selected_db, client_registry, client_id);
        } else if (std.mem.eql(u8, cmd_upper, "RPUSH")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try lists.cmdRpush(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "LPOP")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try lists.cmdLpop(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "RPOP")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try lists.cmdRpop(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "LRANGE")) {
            break :blk try lists.cmdLrange(allocator, storage, array, client_registry, client_id);
        } else if (std.mem.eql(u8, cmd_upper, "LLEN")) {
            break :blk try lists.cmdLlen(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LINDEX")) {
            break :blk try lists.cmdLindex(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LSET")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try lists.cmdLset(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "LTRIM")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try lists.cmdLtrim(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "LREM")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try lists.cmdLrem(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "LPUSHX")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try lists.cmdLpushx(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "RPUSHX")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try lists.cmdRpushx(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "LINSERT")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try lists.cmdLinsert(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "LPOS")) {
            break :blk try lists.cmdLpos(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LMOVE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try lists.cmdLmove(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "RPOPLPUSH")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try lists.cmdRpoplpush(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "BLPOP")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try lists.cmdBlpop(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "BRPOP")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try lists.cmdBrpop(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "BLMOVE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try lists.cmdBlmove(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "BRPOPLPUSH")) {
            // BRPOPLPUSH is a deprecated alias for BLMOVE source dest RIGHT LEFT timeout
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try lists.cmdBrpoplpush(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "LMPOP")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try lists.cmdLmpop(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "BLMPOP")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try lists.cmdBlmpop(allocator, storage, array, ps, selected_db);
        }
        // Set commands
        else if (std.mem.eql(u8, cmd_upper, "SADD")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sets.cmdSadd(allocator, storage, array, ps, selected_db, client_registry, client_id);
        } else if (std.mem.eql(u8, cmd_upper, "SREM")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sets.cmdSrem(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "SISMEMBER")) {
            break :blk try sets.cmdSismember(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SMEMBERS")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sets.cmdSmembers(allocator, storage, array, protocol_version, client_registry, client_id);
        } else if (std.mem.eql(u8, cmd_upper, "SCARD")) {
            break :blk try sets.cmdScard(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SUNION")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sets.cmdSunion(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "SINTER")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sets.cmdSinter(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "SDIFF")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sets.cmdSdiff(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "SUNIONSTORE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sets.cmdSunionstore(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "SINTERSTORE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sets.cmdSinterstore(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "SDIFFSTORE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sets.cmdSdiffstore(allocator, storage, array, ps, selected_db);
        }
        // Hash commands
        else if (std.mem.eql(u8, cmd_upper, "HSET")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try hashes.cmdHset(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "HMSET")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try hashes.cmdHmset(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "HGET")) {
            break :blk try hashes.cmdHget(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HDEL")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try hashes.cmdHdel(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "HGETALL")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try hashes.cmdHgetall(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "HKEYS")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try hashes.cmdHkeys(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "HVALS")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try hashes.cmdHvals(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "HEXISTS")) {
            break :blk try hashes.cmdHexists(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HLEN")) {
            break :blk try hashes.cmdHlen(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HMGET")) {
            break :blk try hashes.cmdHmget(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HINCRBY")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try hashes.cmdHincrby(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "HINCRBYFLOAT")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try hashes.cmdHincrbyfloat(allocator, storage, array, ps, selected_db, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "HSETNX")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try hashes.cmdHsetnx(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "HSTRLEN")) {
            break :blk try hashes.cmdHstrlen(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HRANDFIELD")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try hashes.cmdHrandfield(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "HEXPIRE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try hashes.cmdHexpire(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "HPEXPIRE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try hashes.cmdHpexpire(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "HEXPIREAT")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try hashes.cmdHexpireat(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "HPEXPIREAT")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try hashes.cmdHpexpireat(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "HPERSIST")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try hashes.cmdHpersist(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "HTTL")) {
            break :blk try hashes.cmdHttpl(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HPTTL")) {
            break :blk try hashes.cmdHpttl(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HEXPIRETIME")) {
            break :blk try hashes.cmdHexpiretime(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HPEXPIRETIME")) {
            break :blk try hashes.cmdHpexpiretime(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HGETDEL")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try hashes.cmdHgetdel(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "HGETEX")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try hashes.cmdHgetex(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "HSETEX")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try hashes.cmdHsetex(allocator, storage, array, ps, selected_db);
        }
        // Sorted set commands
        else if (std.mem.eql(u8, cmd_upper, "ZADD")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sorted_sets.cmdZadd(allocator, storage, array, ps, selected_db, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "ZREM")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sorted_sets.cmdZrem(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "ZRANGE")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sorted_sets.cmdZrange(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "ZRANGEBYSCORE")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sorted_sets.cmdZrangebyscore(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "ZSCORE")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sorted_sets.cmdZscore(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "ZCARD")) {
            break :blk try sorted_sets.cmdZcard(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZRANK")) {
            break :blk try sorted_sets.cmdZrank(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZREVRANK")) {
            break :blk try sorted_sets.cmdZrevrank(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZINCRBY")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sorted_sets.cmdZincrby(allocator, storage, array, ps, selected_db, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "ZCOUNT")) {
            break :blk try sorted_sets.cmdZcount(allocator, storage, array);
        }
        // SCAN family commands
        else if (std.mem.eql(u8, cmd_upper, "SCAN")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try keys_cmds.cmdScan(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "HSCAN")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try keys_cmds.cmdHscan(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "SSCAN")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try keys_cmds.cmdSscan(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "ZSCAN")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try keys_cmds.cmdZscan(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "SORT")) {
            break :blk try keys_cmds.cmdSort(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SORT_RO")) {
            break :blk try keys_cmds.cmdSortRo(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "OBJECT")) {
            break :blk try keys_cmds.cmdObject(allocator, storage, array);
        }
        // Set commands (new)
        else if (std.mem.eql(u8, cmd_upper, "SPOP")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sets.cmdSpop(allocator, storage, array, ps, selected_db, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "SRANDMEMBER")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sets.cmdSrandmember(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "SMOVE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sets.cmdSmove(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "SMISMEMBER")) {
            break :blk try sets.cmdSmismember(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SINTERCARD")) {
            break :blk try sets.cmdSintercard(allocator, storage, array);
        }
        // Sorted set commands (new)
        else if (std.mem.eql(u8, cmd_upper, "ZPOPMIN")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sorted_sets.cmdZpopmin(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "ZPOPMAX")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sorted_sets.cmdZpopmax(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "ZMPOP")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sorted_sets.cmdZmpop(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "BZPOPMIN")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sorted_sets.cmdBzpopmin(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "BZPOPMAX")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sorted_sets.cmdBzpopmax(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "BZMPOP")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sorted_sets.cmdBzmpop(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "ZMSCORE")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sorted_sets.cmdZmscore(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "ZREVRANGE")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sorted_sets.cmdZrevrange(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "ZREVRANGEBYSCORE")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sorted_sets.cmdZrevrangebyscore(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "ZRANDMEMBER")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sorted_sets.cmdZrandmember(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "ZUNION")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sorted_sets.cmdZunion(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "ZINTER")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sorted_sets.cmdZinter(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "ZDIFF")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sorted_sets.cmdZdiff(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "ZUNIONSTORE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sorted_sets.cmdZunionstore(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "ZINTERSTORE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sorted_sets.cmdZinterstore(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "ZDIFFSTORE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sorted_sets.cmdZdiffstore(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "ZREMRANGEBYRANK")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sorted_sets.cmdZremrangebyrank(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "ZREMRANGEBYSCORE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sorted_sets.cmdZremrangebyscore(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "ZREMRANGEBYLEX")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try sorted_sets.cmdZremrangebylex(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "ZRANGEBYLEX")) {
            break :blk try sorted_sets.cmdZrangebylex(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZREVRANGEBYLEX")) {
            break :blk try sorted_sets.cmdZrevrangebylex(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZLEXCOUNT")) {
            break :blk try sorted_sets.cmdZlexcount(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZRANGESTORE")) {
            break :blk try sorted_sets.cmdZrangestore(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZINTERCARD")) {
            break :blk try sorted_sets.cmdZintercard(allocator, storage, array);
        }
        // String range commands
        else if (std.mem.eql(u8, cmd_upper, "GETRANGE")) {
            break :blk try cmdGetrange(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SUBSTR")) {
            break :blk try cmdSubstr(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SETRANGE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try cmdSetrange(allocator, storage, array, ps, selected_db);
        }
        // Bit operations
        else if (std.mem.eql(u8, cmd_upper, "SETBIT")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try cmdSetbit(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "GETBIT")) {
            break :blk try cmdGetbit(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "BITCOUNT")) {
            break :blk try cmdBitcount(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "BITOP")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try cmdBitop(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "BITPOS")) {
            break :blk try cmdBitpos(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "BITFIELD")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try bitfield_cmds.cmdBitfield(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "BITFIELD_RO")) {
            break :blk try bitfield_cmds.cmdBitfieldRo(allocator, storage, array);
        }
        // Stream commands
        else if (std.mem.eql(u8, cmd_upper, "XADD")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try streams.cmdXadd(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "XLEN")) {
            break :blk try streams.cmdXlen(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XRANGE")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try streams.cmdXrange(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "XREVRANGE")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try streams.cmdXrevrange(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "XDEL")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try streams.cmdXdel(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "XTRIM")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try streams.cmdXtrim(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "XGROUP")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try streams_adv.cmdXgroup(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "XREAD")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try streams_adv.cmdXread(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "XREADGROUP")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try streams_adv.cmdXreadgroup(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "XACK")) {
            break :blk try streams_adv.cmdXack(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XCLAIM")) {
            break :blk try streams_adv.cmdXclaim(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XAUTOCLAIM")) {
            break :blk try streams_adv.cmdXautoclaim(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XACKDEL")) {
            break :blk try streams_adv.cmdXackdel(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XDELEX")) {
            break :blk try streams_adv.cmdXdelex(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XPENDING")) {
            break :blk try streams.cmdXpending(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XINFO")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try streams.cmdXinfoStream(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "XSETID")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try streams.cmdXsetid(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "XCFGSET")) {
            break :blk try streams.cmdXcfgset(allocator, storage, array);
        }
        // Pub/Sub commands
        else if (std.mem.eql(u8, cmd_upper, "SUBSCRIBE")) {
            const resp_version: u8 = if (getClientProtocol(client_registry, client_id) == RespProtocol.RESP3) 3 else 2;
            break :blk try pubsub_cmds.cmdSubscribe(allocator, ps, array, subscriber_id, resp_version);
        } else if (std.mem.eql(u8, cmd_upper, "UNSUBSCRIBE")) {
            const resp_version: u8 = if (getClientProtocol(client_registry, client_id) == RespProtocol.RESP3) 3 else 2;
            break :blk try pubsub_cmds.cmdUnsubscribe(allocator, ps, array, subscriber_id, resp_version);
        } else if (std.mem.eql(u8, cmd_upper, "PSUBSCRIBE")) {
            const resp_version: u8 = if (getClientProtocol(client_registry, client_id) == RespProtocol.RESP3) 3 else 2;
            break :blk try pubsub_cmds.cmdPsubscribe(allocator, ps, array, subscriber_id, resp_version);
        } else if (std.mem.eql(u8, cmd_upper, "PUNSUBSCRIBE")) {
            const resp_version: u8 = if (getClientProtocol(client_registry, client_id) == RespProtocol.RESP3) 3 else 2;
            break :blk try pubsub_cmds.cmdPunsubscribe(allocator, ps, array, subscriber_id, resp_version);
        } else if (std.mem.eql(u8, cmd_upper, "PUBLISH")) {
            break :blk try pubsub_cmds.cmdPublish(allocator, ps, array);
        } else if (std.mem.eql(u8, cmd_upper, "PUBSUB")) {
            break :blk try cmdPubsub(allocator, ps, array);
        }
        // Sharded Pub/Sub (Redis 7.0+)
        else if (std.mem.eql(u8, cmd_upper, "SSUBSCRIBE")) {
            const resp_version: u8 = if (getClientProtocol(client_registry, client_id) == RespProtocol.RESP3) 3 else 2;
            break :blk try pubsub_cmds.cmdSsubscribe(allocator, ps, array, subscriber_id, resp_version);
        } else if (std.mem.eql(u8, cmd_upper, "SUNSUBSCRIBE")) {
            const resp_version: u8 = if (getClientProtocol(client_registry, client_id) == RespProtocol.RESP3) 3 else 2;
            break :blk try pubsub_cmds.cmdSunsubscribe(allocator, ps, array, subscriber_id, resp_version);
        } else if (std.mem.eql(u8, cmd_upper, "SPUBLISH")) {
            break :blk try pubsub_cmds.cmdSpublish(allocator, ps, array);
        }
        // Server / persistence commands
        else if (std.mem.eql(u8, cmd_upper, "SAVE")) {
            break :blk try cmdSave(allocator, storage);
        } else if (std.mem.eql(u8, cmd_upper, "BGSAVE")) {
            break :blk try cmdBgsave(allocator, storage);
        } else if (std.mem.eql(u8, cmd_upper, "BGREWRITEAOF")) {
            break :blk try cmdBgrewriteaof(allocator, storage);
        } else if (std.mem.eql(u8, cmd_upper, "DBSIZE")) {
            break :blk try cmdDbsize(allocator, storage);
        } else if (std.mem.eql(u8, cmd_upper, "FLUSHDB") or std.mem.eql(u8, cmd_upper, "FLUSHALL")) {
            break :blk try cmdFlushall(allocator, storage, array);
        }
        // Replication commands
        else if (std.mem.eql(u8, cmd_upper, "REPLICAOF") or std.mem.eql(u8, cmd_upper, "SLAVEOF")) {
            // SLAVEOF is a deprecated alias for REPLICAOF
            if (repl) |r| {
                const str_args = try arrayToStrings(allocator, array);
                defer allocator.free(str_args);
                break :blk try repl_cmds.cmdReplicaof(allocator, storage, r, str_args, my_port);
            }
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR replication not initialized");
        } else if (std.mem.eql(u8, cmd_upper, "REPLCONF")) {
            if (repl) |r| {
                const str_args = try arrayToStrings(allocator, array);
                defer allocator.free(str_args);
                break :blk try repl_cmds.cmdReplconf(allocator, r, str_args, replica_idx);
            }
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeSimpleString("OK");
        } else if (std.mem.eql(u8, cmd_upper, "PSYNC")) {
            if (repl) |r| {
                if (replica_stream) |rs| {
                    const idx = replica_idx orelse 0;
                    break :blk try repl_cmds.cmdPsync(allocator, storage, r, idx, rs);
                }
            }
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR PSYNC not supported in this context");
        } else if (std.mem.eql(u8, cmd_upper, "WAIT")) {
            if (repl) |r| {
                const str_args = try arrayToStrings(allocator, array);
                defer allocator.free(str_args);
                break :blk try repl_cmds.cmdWait(allocator, r, client_registry, client_id, str_args);
            }
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeInteger(0);
        } else if (std.mem.eql(u8, cmd_upper, "WAITAOF")) {
            if (repl) |r| {
                const str_args = try arrayToStrings(allocator, array);
                defer allocator.free(str_args);
                break :blk try repl_cmds.cmdWaitaof(allocator, r, aof, str_args);
            }
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR WAITAOF cannot be used with replica instances");
        } else if (std.mem.eql(u8, cmd_upper, "FAILOVER")) {
            if (repl) |r| {
                const str_args = try arrayToStrings(allocator, array);
                defer allocator.free(str_args);
                break :blk try repl_cmds.cmdFailover(allocator, r, str_args);
            }
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR replication not initialized");
        } else if (std.mem.eql(u8, cmd_upper, "ROLE")) {
            if (repl) |r| {
                const str_args = try arrayToStrings(allocator, array);
                defer allocator.free(str_args);
                break :blk try repl_cmds.cmdRole(allocator, r, str_args);
            }
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR replication not initialized");
        } else if (std.mem.eql(u8, cmd_upper, "INFO")) {
            const str_args = try arrayToStrings(allocator, array);
            defer allocator.free(str_args);

            // Build server config from storage/defaults
            const server_config = info_cmds.ServerConfig{
                .port = 6379,
                .bind = "127.0.0.1",
                .maxmemory = 0,
                .maxmemory_policy = "noeviction",
                .timeout = 0,
                .tcp_keepalive = 300,
                .save = "900 1 300 10 60 10000",
                .appendonly = aof != null,
                .appendfsync = "everysec",
                .databases = @intCast(num_databases),
            };

            // Build server stats using real tracked values from Storage
            const server_stats = info_cmds.ServerStats{
                .client_count = client_registry.count(),
                .tracking_clients = client_registry.countTrackingClients(),
                .total_commands_processed = storage.total_commands_processed.load(.monotonic),
                .total_connections_received = storage.total_connections_received.load(.monotonic),
                .start_time_seconds = storage.server_start_time,
            };

            if (repl) |r| {
                break :blk try info_cmds.cmdInfo(allocator, storage, r, server_config, server_stats, str_args, databases, num_databases);
            }
            // Fallback without replication
            var fallback_repl = try ReplicationState.initPrimary(allocator);
            defer fallback_repl.deinit();
            break :blk try info_cmds.cmdInfo(allocator, storage, &fallback_repl, server_config, server_stats, str_args, databases, num_databases);
        }
        // Client connection commands
        else if (std.mem.eql(u8, cmd_upper, "CLIENT")) {
            // CLIENT subcommand is in array[1]
            const subcmd_array = array[1..];
            break :blk try client_cmds.cmdClient(allocator, client_registry, client_id, subcmd_array, &storage.blocking_queue);
        }
        // Configuration commands
        else if (std.mem.eql(u8, cmd_upper, "CONFIG")) {
            const config_proto = getClientProtocol(client_registry, client_id);
            break :blk try config_cmds.executeConfigCommand(allocator, storage, array, config_proto);
        }
        // Command introspection
        else if (std.mem.eql(u8, cmd_upper, "COMMAND")) {
            if (array.len == 1) {
                // COMMAND - return all commands
                const cmd_proto = getClientProtocol(client_registry, client_id);
                break :blk try command_cmds.cmdCommand(allocator, cmd_proto);
            } else {
                const subcmd = switch (array[1]) {
                    .bulk_string => |s| s,
                    else => {
                        var w = Writer.init(allocator);
                        defer w.deinit();
                        return w.writeError("ERR invalid subcommand format");
                    },
                };
                const subcmd_upper = try std.ascii.allocUpperString(allocator, subcmd);
                defer allocator.free(subcmd_upper);

                if (std.mem.eql(u8, subcmd_upper, "COUNT")) {
                    break :blk try command_cmds.cmdCommandCount(allocator);
                } else if (std.mem.eql(u8, subcmd_upper, "INFO")) {
                    const cmd_args = try extractBulkStrings(allocator, array[2..]);
                    defer allocator.free(cmd_args);
                    const info_proto = getClientProtocol(client_registry, client_id);
                    break :blk try command_cmds.cmdCommandInfo(allocator, cmd_args, info_proto);
                } else if (std.mem.eql(u8, subcmd_upper, "GETKEYS")) {
                    const cmd_args = try extractBulkStrings(allocator, array[2..]);
                    defer allocator.free(cmd_args);
                    break :blk try command_cmds.cmdCommandGetKeys(allocator, cmd_args);
                } else if (std.mem.eql(u8, subcmd_upper, "GETKEYSANDFLAGS")) {
                    const cmd_args = try extractBulkStrings(allocator, array[2..]);
                    defer allocator.free(cmd_args);
                    break :blk try command_cmds.cmdCommandGetKeysAndFlags(allocator, cmd_args);
                } else if (std.mem.eql(u8, subcmd_upper, "LIST")) {
                    // COMMAND LIST [FILTERBY (ACLCAT cat | PATTERN pat | MODULE mod)]
                    // array[0]=COMMAND array[1]=LIST array[2]=FILTERBY array[3]=type array[4]=value
                    var filter_type: ?[]const u8 = null;
                    var filter_value: ?[]const u8 = null;
                    if (array.len > 2) {
                        const filter_opt = switch (array[2]) {
                            .bulk_string => |s| s,
                            else => {
                                var w = Writer.init(allocator);
                                defer w.deinit();
                                return w.writeError("ERR syntax error");
                            },
                        };
                        const filter_upper = try std.ascii.allocUpperString(allocator, filter_opt);
                        defer allocator.free(filter_upper);
                        if (std.mem.eql(u8, filter_upper, "FILTERBY") and array.len > 4) {
                            filter_type = switch (array[3]) {
                                .bulk_string => |s| s,
                                else => {
                                    var w = Writer.init(allocator);
                                    defer w.deinit();
                                    return w.writeError("ERR syntax error");
                                },
                            };
                            filter_value = switch (array[4]) {
                                .bulk_string => |s| s,
                                else => {
                                    var w = Writer.init(allocator);
                                    defer w.deinit();
                                    return w.writeError("ERR syntax error");
                                },
                            };
                        } else if (std.mem.eql(u8, filter_upper, "FILTERBY") and array.len > 3) {
                            // FILTERBY present but no value — treat as empty filter
                            filter_type = switch (array[3]) {
                                .bulk_string => |s| s,
                                else => null,
                            };
                        }
                    }
                    const list_proto = getClientProtocol(client_registry, client_id);
                    break :blk try command_cmds.cmdCommandList(allocator, filter_type, filter_value, list_proto);
                } else if (std.mem.eql(u8, subcmd_upper, "HELP")) {
                    break :blk try command_cmds.cmdCommandHelp(allocator);
                } else if (std.mem.eql(u8, subcmd_upper, "DOCS")) {
                    const cmd_args = try extractBulkStrings(allocator, array[2..]);
                    defer allocator.free(cmd_args);
                    const protocol_version = getClientProtocol(client_registry, client_id);
                    break :blk try command_cmds.cmdCommandDocs(allocator, cmd_args, protocol_version);
                } else {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    var buf: [256]u8 = undefined;
                    const err_msg = try std.fmt.bufPrint(&buf, "ERR unknown COMMAND subcommand '{s}'", .{subcmd});
                    return w.writeError(err_msg);
                }
            }
        }
        // Geospatial commands
        else if (std.mem.eql(u8, cmd_upper, "GEOADD")) {
            break :blk try geo_cmds.cmdGeoadd(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "GEOPOS")) {
            break :blk try geo_cmds.cmdGeopos(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "GEODIST")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try geo_cmds.cmdGeodist(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "GEOHASH")) {
            break :blk try geo_cmds.cmdGeohash(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "GEORADIUS")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try geo_cmds.cmdGeoradius(allocator, storage, array, ps, selected_db, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "GEORADIUS_RO")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try geo_cmds.cmdGeoradiusRo(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "GEORADIUSBYMEMBER")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try geo_cmds.cmdGeoradiusbymember(allocator, storage, array, ps, selected_db, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "GEORADIUSBYMEMBER_RO")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try geo_cmds.cmdGeoradiusbymemberRo(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "GEOSEARCH")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try geo_cmds.cmdGeosearch(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "GEOSEARCHSTORE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try geo_cmds.cmdGeosearchstore(allocator, storage, array, ps, selected_db);
        }
        // HyperLogLog commands
        else if (std.mem.eql(u8, cmd_upper, "PFADD")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try hll_cmds.cmdPfadd(allocator, storage, array, ps, selected_db);
        } else if (std.mem.eql(u8, cmd_upper, "PFCOUNT")) {
            break :blk try hll_cmds.cmdPfcount(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "PFMERGE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            break :blk try hll_cmds.cmdPfmerge(allocator, storage, array, ps, selected_db);
        }
        // Server introspection commands
        else if (std.mem.eql(u8, cmd_upper, "MEMORY")) {
            const args = try extractBulkStrings(allocator, array[1..]);
            defer allocator.free(args);
            break :blk try introspection_cmds.cmdMemory(allocator, storage, args);
        } else if (std.mem.eql(u8, cmd_upper, "SLOWLOG")) {
            const args = try extractBulkStrings(allocator, array[1..]);
            defer allocator.free(args);
            break :blk try introspection_cmds.cmdSlowlog(allocator, storage, args);
        } else if (std.mem.eql(u8, cmd_upper, "LATENCY")) {
            const args = try extractBulkStrings(allocator, array[1..]);
            defer allocator.free(args);
            break :blk try introspection_cmds.cmdLatency(allocator, storage, args);
        } else if (std.mem.eql(u8, cmd_upper, "HOTKEYS")) {
            break :blk try hotkeys_cmds.cmdHotkeys(allocator, storage, array[1..]);
        }
        // Scripting commands
        else if (std.mem.eql(u8, cmd_upper, "EVAL")) {
            const args = try extractBulkStrings(allocator, array[1..]);
            defer allocator.free(args);
            const resp_version = @intFromEnum(getClientProtocol(client_registry, client_id));
            break :blk try scripting_cmds.cmdEval(allocator, storage, script_store, args, resp_version, aof, ps, subscriber_id, tx, repl, my_port, replica_stream, replica_idx, client_registry, client_id, shutdown_state, databases, num_databases);
        } else if (std.mem.eql(u8, cmd_upper, "EVALSHA")) {
            const args = try extractBulkStrings(allocator, array[1..]);
            defer allocator.free(args);
            const resp_version = @intFromEnum(getClientProtocol(client_registry, client_id));
            break :blk try scripting_cmds.cmdEvalSHA(allocator, storage, script_store, args, resp_version, aof, ps, subscriber_id, tx, repl, my_port, replica_stream, replica_idx, client_registry, client_id, shutdown_state, databases, num_databases);
        } else if (std.mem.eql(u8, cmd_upper, "EVAL_RO")) {
            const args = try extractBulkStrings(allocator, array[1..]);
            defer allocator.free(args);
            const resp_version = @intFromEnum(getClientProtocol(client_registry, client_id));
            break :blk try scripting_cmds.cmdEvalRo(allocator, storage, script_store, args, resp_version, aof, ps, subscriber_id, tx, repl, my_port, replica_stream, replica_idx, client_registry, client_id, shutdown_state, databases, num_databases);
        } else if (std.mem.eql(u8, cmd_upper, "EVALSHA_RO")) {
            const args = try extractBulkStrings(allocator, array[1..]);
            defer allocator.free(args);
            const resp_version = @intFromEnum(getClientProtocol(client_registry, client_id));
            break :blk try scripting_cmds.cmdEvalShaRo(allocator, storage, script_store, args, resp_version, aof, ps, subscriber_id, tx, repl, my_port, replica_stream, replica_idx, client_registry, client_id, shutdown_state, databases, num_databases);
        } else if (std.mem.eql(u8, cmd_upper, "SCRIPT")) {
            if (array.len < 2) {
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeError("ERR wrong number of arguments for 'script' command");
            }
            const subcmd = switch (array[1]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR invalid subcommand format");
                },
            };
            const subcmd_upper = try std.ascii.allocUpperString(allocator, subcmd);
            defer allocator.free(subcmd_upper);

            if (std.mem.eql(u8, subcmd_upper, "LOAD")) {
                const args = try extractBulkStrings(allocator, array[2..]);
                defer allocator.free(args);
                break :blk try scripting_cmds.cmdScriptLoad(allocator, script_store, args);
            } else if (std.mem.eql(u8, subcmd_upper, "EXISTS")) {
                const args = try extractBulkStrings(allocator, array[2..]);
                defer allocator.free(args);
                break :blk try scripting_cmds.cmdScriptExists(allocator, script_store, args);
            } else if (std.mem.eql(u8, subcmd_upper, "FLUSH")) {
                const args = try extractBulkStrings(allocator, array[2..]);
                defer allocator.free(args);
                break :blk try scripting_cmds.cmdScriptFlush(allocator, script_store, args);
            } else if (std.mem.eql(u8, subcmd_upper, "KILL")) {
                break :blk try scripting_cmds.cmdScriptKill(allocator, script_store);
            } else if (std.mem.eql(u8, subcmd_upper, "HELP")) {
                break :blk try scripting_cmds.cmdScriptHelp(allocator);
            } else {
                var w = Writer.init(allocator);
                defer w.deinit();
                var buf: [256]u8 = undefined;
                const err_msg = try std.fmt.bufPrint(&buf, "ERR unknown SCRIPT subcommand '{s}'", .{subcmd});
                break :blk try w.writeError(err_msg);
            }
        }
        // Redis Functions commands
        else if (std.mem.eql(u8, cmd_upper, "FUNCTION")) {
            if (array.len < 2) {
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeError("ERR wrong number of arguments for 'function' command");
            }
            const subcmd = switch (array[1]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR invalid subcommand format");
                },
            };
            const subcmd_upper = try std.ascii.allocUpperString(allocator, subcmd);
            defer allocator.free(subcmd_upper);

            // Convert RespValue array to string args
            var args_func = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args_func);
            for (array, 0..) |val, i| {
                args_func[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }

            if (std.mem.eql(u8, subcmd_upper, "LOAD")) {
                const result = try function_cmds.cmdFunctionLoad(allocator, storage, args_func);
                defer switch (result) {
                    .bulk_string => |s| allocator.free(s),
                    .error_string => |s| allocator.free(s),
                    else => {},
                };
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, subcmd_upper, "FLUSH")) {
                const result = try function_cmds.cmdFunctionFlush(allocator, storage, args_func);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, subcmd_upper, "LIST")) {
                const result = try function_cmds.cmdFunctionList(allocator, storage, args_func);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, subcmd_upper, "DELETE")) {
                const result = try function_cmds.cmdFunctionDelete(allocator, storage, args_func);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, subcmd_upper, "DUMP")) {
                const result = try function_cmds.cmdFunctionDump(allocator, storage, args_func);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, subcmd_upper, "RESTORE")) {
                const result = try function_cmds.cmdFunctionRestore(allocator, storage, args_func);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, subcmd_upper, "STATS")) {
                const result = try function_cmds.cmdFunctionStats(allocator, storage, args_func);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, subcmd_upper, "KILL")) {
                const result = try function_cmds.cmdFunctionKill(allocator, storage, args_func);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, subcmd_upper, "HELP")) {
                const result = try function_cmds.cmdFunctionHelp(allocator, args_func);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else {
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeError("ERR unknown FUNCTION subcommand");
            }
        }
        else if (std.mem.eql(u8, cmd_upper, "FCALL")) {
            const args_fcall = try extractBulkStrings(allocator, array[1..]);
            defer allocator.free(args_fcall);
            break :blk try function_cmds.cmdFcall(
                allocator,
                storage,
                script_store,
                args_fcall,
                aof,
                ps,
                subscriber_id,
                tx,
                repl,
                my_port,
                replica_stream,
                replica_idx,
                client_registry,
                client_id,
                shutdown_state,
                databases,
                num_databases,
            );
        }
        else if (std.mem.eql(u8, cmd_upper, "FCALL_RO")) {
            const args_fcall_ro = try extractBulkStrings(allocator, array[1..]);
            defer allocator.free(args_fcall_ro);
            break :blk try function_cmds.cmdFcallRo(
                allocator,
                storage,
                script_store,
                args_fcall_ro,
                aof,
                ps,
                subscriber_id,
                tx,
                repl,
                my_port,
                replica_stream,
                replica_idx,
                client_registry,
                client_id,
                shutdown_state,
                databases,
                num_databases,
            );
        }
        // CLUSTER commands
        else if (std.mem.eql(u8, cmd_upper, "CLUSTER")) {
            if (array.len < 2) {
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeError("ERR wrong number of arguments for 'cluster' command");
            }
            const subcmd = switch (array[1]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR invalid subcommand format");
                },
            };
            const subcmd_upper = try std.ascii.allocUpperString(allocator, subcmd);
            defer allocator.free(subcmd_upper);

            // Convert RespValue array to string args
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            for (array, 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }

            if (std.mem.eql(u8, subcmd_upper, "SLOTS")) {
                break :blk try cluster_cmds.cmdClusterSlots(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "SHARDS")) {
                break :blk try cluster_cmds.cmdClusterShards(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "NODES")) {
                break :blk try cluster_cmds.cmdClusterNodes(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "INFO")) {
                break :blk try cluster_cmds.cmdClusterInfo(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "MYID")) {
                break :blk try cluster_cmds.cmdClusterMyId(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "MYSHARDID")) {
                break :blk try cluster_cmds.cmdClusterMyShardId(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "ADDSLOTS")) {
                break :blk try cluster_cmds.cmdClusterAddSlots(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "ADDSLOTSRANGE")) {
                break :blk try cluster_cmds.cmdClusterAddSlotsRange(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "DELSLOTS")) {
                break :blk try cluster_cmds.cmdClusterDelSlots(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "DELSLOTSRANGE")) {
                break :blk try cluster_cmds.cmdClusterDelSlotsRange(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "FLUSHSLOTS")) {
                break :blk try cluster_cmds.cmdClusterFlushSlots(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "MEET")) {
                break :blk try cluster_cmds.cmdClusterMeet(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "FORGET")) {
                break :blk try cluster_cmds.cmdClusterForget(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "SETSLOT")) {
                break :blk try cluster_cmds.cmdClusterSetslot(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "FAILOVER")) {
                break :blk try cluster_cmds.cmdClusterFailover(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "REPLICAS") or std.mem.eql(u8, subcmd_upper, "SLAVES")) {
                // CLUSTER SLAVES is a deprecated alias for CLUSTER REPLICAS
                break :blk try cluster_cmds.cmdClusterReplicas(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "REPLICATE")) {
                break :blk try cluster_cmds.cmdClusterReplicate(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "SAVECONFIG")) {
                break :blk try cluster_cmds.cmdClusterSaveConfig(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "BUMPEPOCH")) {
                break :blk try cluster_cmds.cmdClusterBumpEpoch(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "SET-CONFIG-EPOCH")) {
                break :blk try cluster_cmds.cmdClusterSetConfigEpoch(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "SLOT-STATS")) {
                break :blk try cluster_cmds.cmdClusterSlotStats(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "COUNTKEYSINSLOT")) {
                break :blk try cluster_cmds.cmdClusterCountkeysInSlot(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "GETKEYSINSLOT")) {
                break :blk try cluster_cmds.cmdClusterGetKeysInSlot(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "KEYSLOT")) {
                break :blk try cluster_cmds.cmdClusterKeyslot(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "LINKS")) {
                break :blk try cluster_cmds.cmdClusterLinks(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "COUNT-FAILURE-REPORTS")) {
                break :blk try cluster_cmds.cmdClusterCountFailureReports(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "RESET")) {
                break :blk try cluster_cmds.cmdClusterReset(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "MIGRATION")) {
                break :blk try cluster_cmds.cmdClusterMigration(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "HELP")) {
                break :blk try cluster_cmds.cmdClusterHelp(allocator, args, storage, null, 0);
            } else {
                var w = Writer.init(allocator);
                defer w.deinit();
                var buf: [256]u8 = undefined;
                const err_msg = try std.fmt.bufPrint(&buf, "ERR unknown CLUSTER subcommand '{s}'", .{subcmd});
                break :blk try w.writeError(err_msg);
            }
        }
        // SENTINEL commands
        else if (std.mem.eql(u8, cmd_upper, "SENTINEL")) {
            if (array.len < 2) {
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeError("ERR wrong number of arguments for 'sentinel' command");
            }
            const subcmd = switch (array[1]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR invalid subcommand format");
                },
            };
            const subcmd_upper = try std.ascii.allocUpperString(allocator, subcmd);
            defer allocator.free(subcmd_upper);

            // Convert RespValue array to string args (same as CLUSTER)
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            for (array, 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }

            if (std.mem.eql(u8, subcmd_upper, "PING")) {
                break :blk try sentinel_cmds.cmdSentinelPing(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "MASTERS")) {
                break :blk try sentinel_cmds.cmdSentinelMasters(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "MASTER")) {
                break :blk try sentinel_cmds.cmdSentinelMaster(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "REPLICAS")) {
                break :blk try sentinel_cmds.cmdSentinelReplicas(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "GET-MASTER-ADDR-BY-NAME")) {
                break :blk try sentinel_cmds.cmdSentinelGetMasterAddrByName(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "MONITOR")) {
                break :blk try sentinel_cmds.cmdSentinelMonitor(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "REMOVE")) {
                break :blk try sentinel_cmds.cmdSentinelRemove(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "SENTINELS")) {
                break :blk try sentinel_cmds.cmdSentinelSentinels(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "IS-MASTER-DOWN-BY-ADDR")) {
                break :blk try sentinel_cmds.cmdSentinelIsMasterDownByAddr(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "RESET")) {
                break :blk try sentinel_cmds.cmdSentinelReset(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "FAILOVER")) {
                break :blk try sentinel_cmds.cmdSentinelFailover(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "CKQUORUM")) {
                break :blk try sentinel_cmds.cmdSentinelCkquorum(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "FLUSHCONFIG")) {
                break :blk try sentinel_cmds.cmdSentinelFlushconfig(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "SET")) {
                break :blk try sentinel_cmds.cmdSentinelSet(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "MYID")) {
                break :blk try sentinel_cmds.cmdSentinelMyid(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "SIMULATE-FAILURE")) {
                break :blk try sentinel_cmds.cmdSentinelSimulateFailure(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "PENDING-SCRIPTS")) {
                break :blk try sentinel_cmds.cmdSentinelPendingScripts(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "INFO-CACHE")) {
                break :blk try sentinel_cmds.cmdSentinelInfoCache(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "CONFIG")) {
                // SENTINEL CONFIG GET/SET subcommands
                if (array.len < 3) {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR wrong number of arguments for 'sentinel|config' command");
                }
                const config_subcmd = switch (array[2]) {
                    .bulk_string => |s| s,
                    else => {
                        var w = Writer.init(allocator);
                        defer w.deinit();
                        break :blk try w.writeError("ERR invalid SENTINEL CONFIG subcommand");
                    },
                };
                var config_subcmd_upper_buf: [64]u8 = undefined;
                const config_subcmd_upper = std.ascii.upperString(&config_subcmd_upper_buf, config_subcmd);

                if (std.mem.eql(u8, config_subcmd_upper, "GET")) {
                    break :blk try sentinel_cmds.cmdSentinelConfigGet(allocator, args, storage, null, 0);
                } else if (std.mem.eql(u8, config_subcmd_upper, "SET")) {
                    break :blk try sentinel_cmds.cmdSentinelConfigSet(allocator, args, storage, null, 0);
                } else {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    var buf: [256]u8 = undefined;
                    const err_msg = try std.fmt.bufPrint(&buf, "ERR unknown SENTINEL CONFIG subcommand '{s}'", .{config_subcmd});
                    break :blk try w.writeError(err_msg);
                }
            } else {
                var w = Writer.init(allocator);
                defer w.deinit();
                var buf: [256]u8 = undefined;
                const err_msg = try std.fmt.bufPrint(&buf, "ERR unknown SENTINEL subcommand '{s}'", .{subcmd});
                break :blk try w.writeError(err_msg);
            }
        }
        // MODULE commands
        else if (std.mem.eql(u8, cmd_upper, "MODULE")) {
            if (array.len < 2) {
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeError("ERR wrong number of arguments for 'module' command");
            }
            const subcmd = switch (array[1]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR invalid subcommand format");
                },
            };
            const subcmd_upper = try std.ascii.allocUpperString(allocator, subcmd);
            defer allocator.free(subcmd_upper);

            // Convert RespValue array to string args (same as CLUSTER/SENTINEL)
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            for (array, 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }

            if (std.mem.eql(u8, subcmd_upper, "HELP")) {
                break :blk try modules_cmds.cmdModuleHelp(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "LOAD")) {
                break :blk try modules_cmds.cmdModuleLoad(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "UNLOAD")) {
                break :blk try modules_cmds.cmdModuleUnload(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "LIST")) {
                break :blk try modules_cmds.cmdModuleList(allocator, args, storage, null, 0);
            } else {
                var w = Writer.init(allocator);
                defer w.deinit();
                var buf: [256]u8 = undefined;
                const err_msg = try std.fmt.bufPrint(&buf, "ERR unknown MODULE subcommand '{s}'", .{subcmd});
                break :blk try w.writeError(err_msg);
            }
        }
        // JSON commands
        else if (std.mem.eql(u8, cmd_upper, "JSON.SET")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const result = try json_cmds.cmdJsonSet(storage, array, allocator, ps, selected_db);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.GET")) {
            const result = try json_cmds.cmdJsonGet(storage, array, allocator);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.DEL")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const result = try json_cmds.cmdJsonDel(storage, array, allocator, ps, selected_db);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.TYPE")) {
            const result = try json_cmds.cmdJsonType(storage, array, allocator);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.MGET")) {
            const result = try json_cmds.cmdJsonMget(storage, array, allocator);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.NUMINCRBY")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const result = try json_cmds.cmdJsonNumincrby(storage, array, allocator, ps, selected_db);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.NUMMULTBY")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const result = try json_cmds.cmdJsonNummultby(storage, array, allocator, ps, selected_db);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.MSET")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const result = try json_cmds.cmdJsonMset(storage, array, allocator, ps, selected_db);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.FORGET")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const result = try json_cmds.cmdJsonForget(storage, array, allocator, ps, selected_db);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.STRAPPEND")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const result = try json_cmds.cmdJsonStrappend(storage, array, allocator, ps, selected_db);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.STRLEN")) {
            const result = try json_cmds.cmdJsonStrlen(storage, array, allocator);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.TOGGLE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const result = try json_cmds.cmdJsonToggle(storage, array, allocator, ps, selected_db);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.CLEAR")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const result = try json_cmds.cmdJsonClear(storage, array, allocator, ps, selected_db);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.ARRINDEX")) {
            const result = try json_cmds.cmdJsonArrindex(storage, array, allocator);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.ARRAPPEND")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const result = try json_cmds.cmdJsonArrappend(storage, array, allocator, ps, selected_db);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.ARRINSERT")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const result = try json_cmds.cmdJsonArrinsert(storage, array, allocator, ps, selected_db);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.ARRLEN")) {
            const result = try json_cmds.cmdJsonArrlen(storage, array, allocator);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.ARRPOP")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const result = try json_cmds.cmdJsonArrpop(storage, array, allocator, ps, selected_db);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.ARRTRIM")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const result = try json_cmds.cmdJsonArrtrim(storage, array, allocator, ps, selected_db);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.OBJKEYS")) {
            const result = try json_cmds.cmdJsonObjkeys(storage, array, allocator);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.OBJLEN")) {
            const result = try json_cmds.cmdJsonObjlen(storage, array, allocator);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.RESP")) {
            const result = try json_cmds.cmdJsonResp(storage, array, allocator);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.MERGE")) {
            const selected_db = client_registry.getSelectedDb(client_id);
            const result = try json_cmds.cmdJsonMerge(storage, array, allocator, ps, selected_db);
            var w = Writer.init(allocator);
            defer w.deinit();
            break :blk try w.writeRespValue(result);
        } else if (std.mem.eql(u8, cmd_upper, "JSON.DEBUG")) {
            // JSON.DEBUG has subcommands: HELP, MEMORY
            if (array.len < 2) {
                // Default to HELP if no subcommand
                const result = try json_cmds.cmdJsonDebugHelp(storage, &[_]protocol.RespValue{array[0]}, allocator);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            }
            const subcmd = switch (array[1]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR invalid subcommand format");
                },
            };
            const subcmd_upper = try std.ascii.allocUpperString(allocator, subcmd);
            defer allocator.free(subcmd_upper);

            if (std.mem.eql(u8, subcmd_upper, "HELP")) {
                const result = try json_cmds.cmdJsonDebugHelp(storage, &[_]protocol.RespValue{array[0]}, allocator);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, subcmd_upper, "MEMORY")) {
                // Remove "DEBUG" and "MEMORY" from args, keep key and optional path
                if (array.len == 2) {
                    // JSON.DEBUG MEMORY -> error (need key)
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR wrong number of arguments for 'json.debug' command");
                }
                const debug_args = if (array.len == 3) blk2: {
                    // JSON.DEBUG MEMORY key
                    break :blk2 &[_]protocol.RespValue{ array[0], array[2] };
                } else blk2: {
                    // JSON.DEBUG MEMORY key path
                    break :blk2 &[_]protocol.RespValue{ array[0], array[2], array[3] };
                };
                const result = try json_cmds.cmdJsonDebugMemory(storage, debug_args, allocator);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else {
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeError("ERR unknown JSON.DEBUG subcommand");
            }
        }
        // Search (FT.*) commands
        else if (std.mem.startsWith(u8, cmd_upper, "FT.")) {
            // Extract command args (skip command name)
            var args = try allocator.alloc([]const u8, array.len - 1);
            defer allocator.free(args);
            for (array[1..], 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }

            if (std.mem.eql(u8, cmd_upper, "FT.CREATE")) {
                const result = try search_cmds.cmdFtCreate(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT._LIST")) {
                const result = try search_cmds.cmdFtList(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.DROPINDEX")) {
                const result = try search_cmds.cmdFtDropindex(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.INFO")) {
                const result = try search_cmds.cmdFtInfo(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.ALTER")) {
                const result = try search_cmds.cmdFtAlter(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.SEARCH")) {
                const result = try search_cmds.cmdFtSearch(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.EXPLAIN")) {
                const result = try search_cmds.cmdFtExplain(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.EXPLAINCLI")) {
                const result = try search_cmds.cmdFtExplaincli(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.AGGREGATE")) {
                const result = try search_agg_cmds.cmdFtAggregate(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.PROFILE")) {
                const result = try search_cmds.cmdFtProfile(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.SPELLCHECK")) {
                const result = try search_cmds.cmdFtSpellcheck(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.CURSOR")) {
                if (args.len == 0) {
                    return "ERR wrong number of arguments for 'FT.CURSOR' command\r\n";
                }
                const subcmd_upper = try std.ascii.allocUpperString(allocator, args[0]);
                defer allocator.free(subcmd_upper);

                const result = if (std.mem.eql(u8, subcmd_upper, "READ"))
                    try search_cmds.cmdFtCursorRead(storage, allocator, args[1..])
                else if (std.mem.eql(u8, subcmd_upper, "DEL"))
                    try search_cmds.cmdFtCursorDel(storage, allocator, args[1..])
                else
                    RespValue{ .error_string = "ERR unknown FT.CURSOR subcommand" };

                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.ALIASADD")) {
                if (args.len < 2) {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR wrong number of arguments for 'FT.ALIASADD' command");
                }
                const result = try search_cmds.cmdFtAliasadd(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.ALIASDEL")) {
                if (args.len < 1) {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR wrong number of arguments for 'FT.ALIASDEL' command");
                }
                const result = try search_cmds.cmdFtAliasdel(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.ALIASUPDATE")) {
                if (args.len < 2) {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR wrong number of arguments for 'FT.ALIASUPDATE' command");
                }
                const result = try search_cmds.cmdFtAliasupdate(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.DICTADD")) {
                if (args.len < 2) {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR wrong number of arguments for 'FT.DICTADD' command");
                }
                const result = try search_cmds.cmdFtDictadd(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.DICTDEL")) {
                if (args.len < 2) {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR wrong number of arguments for 'FT.DICTDEL' command");
                }
                const result = try search_cmds.cmdFtDictdel(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.DICTDUMP")) {
                if (args.len < 1) {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR wrong number of arguments for 'FT.DICTDUMP' command");
                }
                const result = try search_cmds.cmdFtDictdump(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.SYNDUMP")) {
                if (args.len < 1) {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR wrong number of arguments for 'FT.SYNDUMP' command");
                }
                const result = try search_cmds.cmdFtSyndump(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.SYNUPDATE")) {
                if (args.len < 3) {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR wrong number of arguments for 'FT.SYNUPDATE' command");
                }
                const result = try search_cmds.cmdFtSynupdate(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.SUGADD")) {
                if (args.len < 3) {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR wrong number of arguments for 'FT.SUGADD' command");
                }
                const result = try search_cmds.cmdFtSugadd(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.SUGGET")) {
                if (args.len < 2) {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR wrong number of arguments for 'FT.SUGGET' command");
                }
                const result = try search_cmds.cmdFtSugget(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.SUGLEN")) {
                if (args.len != 1) {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR wrong number of arguments for 'FT.SUGLEN' command");
                }
                const result = try search_cmds.cmdFtSuglen(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.SUGDEL")) {
                if (args.len != 2) {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR wrong number of arguments for 'FT.SUGDEL' command");
                }
                const result = try search_cmds.cmdFtSugdel(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.TAGVALS")) {
                if (args.len != 2) {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR wrong number of arguments for 'FT.TAGVALS' command");
                }
                const result = try search_cmds.cmdFtTagvals(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.CONFIG")) {
                if (args.len < 1) {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR wrong number of arguments for 'FT.CONFIG' command");
                }
                const result = try search_cmds.cmdFtConfig(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "FT.HYBRID")) {
                if (args.len < 5) {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR wrong number of arguments for 'FT.HYBRID' command");
                }
                const result = try search_cmds.cmdFtHybrid(storage, allocator, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else {
                var w = Writer.init(allocator);
                defer w.deinit();
                var buf: [256]u8 = undefined;
                const err_msg = try std.fmt.bufPrint(&buf, "ERR unknown command '{s}'", .{cmd_upper});
                break :blk try w.writeError(err_msg);
            }
        }
        // Time Series (TS.*) commands
        else if (std.mem.startsWith(u8, cmd_upper, "TS.")) {
            // Extract command args
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            args[0] = cmd_upper;
            for (array[1..], 1..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }

            if (std.mem.eql(u8, cmd_upper, "TS.CREATE")) {
                break :blk try timeseries_cmds.cmdTsCreate(storage, args, allocator);
            } else if (std.mem.eql(u8, cmd_upper, "TS.INFO")) {
                break :blk try timeseries_cmds.cmdTsInfo(storage, args, allocator);
            } else if (std.mem.eql(u8, cmd_upper, "TS.ADD")) {
                const selected_db = client_registry.getSelectedDb(client_id);
                break :blk try timeseries_cmds.cmdTsAdd(storage, args, allocator, ps, selected_db);
            } else if (std.mem.eql(u8, cmd_upper, "TS.MADD")) {
                const selected_db = client_registry.getSelectedDb(client_id);
                break :blk try timeseries_cmds.cmdTsMadd(storage, args, allocator, ps, selected_db);
            } else if (std.mem.eql(u8, cmd_upper, "TS.INCRBY")) {
                const selected_db = client_registry.getSelectedDb(client_id);
                break :blk try timeseries_cmds.cmdTsIncrby(storage, args, allocator, ps, selected_db);
            } else if (std.mem.eql(u8, cmd_upper, "TS.DECRBY")) {
                const selected_db = client_registry.getSelectedDb(client_id);
                break :blk try timeseries_cmds.cmdTsDecrby(storage, args, allocator, ps, selected_db);
            } else if (std.mem.eql(u8, cmd_upper, "TS.DEL")) {
                const selected_db = client_registry.getSelectedDb(client_id);
                break :blk try timeseries_cmds.cmdTsDel(storage, args, allocator, ps, selected_db);
            } else if (std.mem.eql(u8, cmd_upper, "TS.GET")) {
                break :blk try timeseries_cmds.cmdTsGet(storage, args, allocator);
            } else if (std.mem.eql(u8, cmd_upper, "TS.ALTER")) {
                const selected_db = client_registry.getSelectedDb(client_id);
                break :blk try timeseries_cmds.cmdTsAlter(storage, args, allocator, ps, selected_db);
            } else if (std.mem.eql(u8, cmd_upper, "TS.MGET")) {
                break :blk try timeseries_cmds.cmdTsMget(storage, args, allocator);
            } else if (std.mem.eql(u8, cmd_upper, "TS.RANGE")) {
                break :blk try timeseries_cmds.cmdTsRange(storage, args, allocator);
            } else if (std.mem.eql(u8, cmd_upper, "TS.REVRANGE")) {
                break :blk try timeseries_cmds.cmdTsRevrange(storage, args, allocator);
            } else if (std.mem.eql(u8, cmd_upper, "TS.MRANGE")) {
                break :blk try timeseries_cmds.cmdTsMrange(storage, args, allocator);
            } else if (std.mem.eql(u8, cmd_upper, "TS.MREVRANGE")) {
                break :blk try timeseries_cmds.cmdTsMrevrange(storage, args, allocator);
            } else if (std.mem.eql(u8, cmd_upper, "TS.QUERYINDEX")) {
                break :blk try timeseries_cmds.cmdTsQueryindex(storage, args, allocator);
            } else if (std.mem.eql(u8, cmd_upper, "TS.CREATERULE")) {
                const selected_db = client_registry.getSelectedDb(client_id);
                break :blk try timeseries_cmds.cmdTsCreaterule(storage, args, allocator, ps, selected_db);
            } else if (std.mem.eql(u8, cmd_upper, "TS.DELETERULE")) {
                const selected_db = client_registry.getSelectedDb(client_id);
                break :blk try timeseries_cmds.cmdTsDeleterule(storage, args, allocator, ps, selected_db);
            } else {
                var w = Writer.init(allocator);
                defer w.deinit();
                var buf: [256]u8 = undefined;
                const err_msg = try std.fmt.bufPrint(&buf, "ERR unknown command '{s}'", .{cmd_upper});
                break :blk try w.writeError(err_msg);
            }
        }
        // Bloom Filter (BF.*) commands
        else if (std.mem.startsWith(u8, cmd_upper, "BF.")) {
            // Extract command args (skip command name)
            const args = try allocator.alloc(RespValue, array.len - 1);
            defer allocator.free(args);
            @memcpy(args, array[1..]);

            if (std.mem.eql(u8, cmd_upper, "BF.RESERVE")) {
                const result = try bloom_cmds.cmdBfReserve(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "BF.ADD")) {
                const result = try bloom_cmds.cmdBfAdd(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "BF.EXISTS")) {
                const result = try bloom_cmds.cmdBfExists(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "BF.MADD")) {
                const result = try bloom_cmds.cmdBfMadd(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "BF.MEXISTS")) {
                const result = try bloom_cmds.cmdBfMexists(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "BF.INSERT")) {
                const result = try bloom_cmds.cmdBfInsert(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "BF.INFO")) {
                const result = try bloom_cmds.cmdBfInfo(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "BF.CARD")) {
                const result = try bloom_cmds.cmdBfCard(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "BF.SCANDUMP")) {
                const result = try bloom_cmds.cmdBfScandump(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "BF.LOADCHUNK")) {
                const result = try bloom_cmds.cmdBfLoadchunk(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else {
                var w = Writer.init(allocator);
                defer w.deinit();
                var buf: [256]u8 = undefined;
                const err_msg = try std.fmt.bufPrint(&buf, "ERR unknown command '{s}'", .{cmd_upper});
                break :blk try w.writeError(err_msg);
            }
        }

        // Cuckoo Filter (CF.*) commands
        else if (std.mem.startsWith(u8, cmd_upper, "CF.")) {
            // Extract command args (skip command name)
            const args = try allocator.alloc(RespValue, array.len - 1);
            defer allocator.free(args);
            @memcpy(args, array[1..]);

            if (std.mem.eql(u8, cmd_upper, "CF.RESERVE")) {
                const result = try cuckoo_cmds.cmdCfReserve(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "CF.ADD")) {
                const result = try cuckoo_cmds.cmdCfAdd(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "CF.ADDNX")) {
                const result = try cuckoo_cmds.cmdCfAddnx(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "CF.EXISTS")) {
                const result = try cuckoo_cmds.cmdCfExists(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "CF.DEL")) {
                const result = try cuckoo_cmds.cmdCfDel(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "CF.COUNT")) {
                const result = try cuckoo_cmds.cmdCfCount(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "CF.INFO")) {
                const result = try cuckoo_cmds.cmdCfInfo(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "CF.SCANDUMP")) {
                const result = try cuckoo_cmds.cmdCfScandump(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "CF.LOADCHUNK")) {
                const result = try cuckoo_cmds.cmdCfLoadchunk(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "CF.INSERT")) {
                const result = try cuckoo_cmds.cmdCfInsert(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "CF.INSERTNX")) {
                const result = try cuckoo_cmds.cmdCfInsertnx(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "CF.MEXISTS")) {
                const result = try cuckoo_cmds.cmdCfMexists(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else {
                var w = Writer.init(allocator);
                defer w.deinit();
                var buf: [256]u8 = undefined;
                const err_msg = try std.fmt.bufPrint(&buf, "ERR unknown command '{s}'", .{cmd_upper});
                break :blk try w.writeError(err_msg);
            }
        }

        // Count-Min Sketch (CMS.*) commands
        else if (std.mem.startsWith(u8, cmd_upper, "CMS.")) {
            // Extract command args (skip command name)
            const args = try allocator.alloc(RespValue, array.len - 1);
            defer allocator.free(args);
            @memcpy(args, array[1..]);

            if (std.mem.eql(u8, cmd_upper, "CMS.INITBYDIM")) {
                const result = try cms_cmds.cmdCmsInitByDim(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "CMS.INITBYPROB")) {
                const result = try cms_cmds.cmdCmsInitByProb(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "CMS.INCRBY")) {
                const result = try cms_cmds.cmdCmsIncrBy(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "CMS.QUERY")) {
                const result = try cms_cmds.cmdCmsQuery(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "CMS.MERGE")) {
                const result = try cms_cmds.cmdCmsMerge(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "CMS.INFO")) {
                const result = try cms_cmds.cmdCmsInfo(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TOPK.RESERVE")) {
                const result = try topk_cmds.cmdTopkReserve(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TOPK.ADD")) {
                const result = try topk_cmds.cmdTopkAdd(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TOPK.QUERY")) {
                const protocol_version = getClientProtocol(client_registry, client_id);
                const result = try topk_cmds.cmdTopkQuery(allocator, storage, args, protocol_version);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TOPK.COUNT")) {
                const result = try topk_cmds.cmdTopkCount(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TOPK.INCRBY")) {
                const result = try topk_cmds.cmdTopkIncrby(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TOPK.LIST")) {
                const result = try topk_cmds.cmdTopkList(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TOPK.INFO")) {
                const result = try topk_cmds.cmdTopkInfo(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TDIGEST.CREATE")) {
                const result = try tdigest_cmds.cmdTdigestCreate(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TDIGEST.ADD")) {
                const result = try tdigest_cmds.cmdTdigestAdd(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TDIGEST.RESET")) {
                const result = try tdigest_cmds.cmdTdigestReset(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TDIGEST.MERGE")) {
                const result = try tdigest_cmds.cmdTdigestMerge(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TDIGEST.QUANTILE")) {
                const result = try tdigest_cmds.cmdTdigestQuantile(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TDIGEST.CDF")) {
                const result = try tdigest_cmds.cmdTdigestCdf(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TDIGEST.MIN")) {
                const result = try tdigest_cmds.cmdTdigestMin(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TDIGEST.MAX")) {
                const result = try tdigest_cmds.cmdTdigestMax(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TDIGEST.RANK")) {
                const result = try tdigest_cmds.cmdTdigestRank(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TDIGEST.REVRANK")) {
                const result = try tdigest_cmds.cmdTdigestRevrank(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TDIGEST.BYRANK")) {
                const result = try tdigest_cmds.cmdTdigestByrank(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TDIGEST.BYREVRANK")) {
                const result = try tdigest_cmds.cmdTdigestByrevrank(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TDIGEST.INFO")) {
                const result = try tdigest_cmds.cmdTdigestInfo(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "TDIGEST.TRIMMED_MEAN")) {
                const result = try tdigest_cmds.cmdTdigestTrimmedMean(allocator, storage, args);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else {
                var w = Writer.init(allocator);
                defer w.deinit();
                var buf: [256]u8 = undefined;
                const err_msg = try std.fmt.bufPrint(&buf, "ERR unknown command '{s}'", .{cmd_upper});
                break :blk try w.writeError(err_msg);
            }
        }

        // Vector Set (V*) commands
        else if (std.mem.eql(u8, cmd_upper, "VADD") or
            std.mem.eql(u8, cmd_upper, "VCARD") or
            std.mem.eql(u8, cmd_upper, "VDIM") or
            std.mem.eql(u8, cmd_upper, "VEMB") or
            std.mem.eql(u8, cmd_upper, "VREM") or
            std.mem.eql(u8, cmd_upper, "VISMEMBER") or
            std.mem.eql(u8, cmd_upper, "VRANDMEMBER") or
            std.mem.eql(u8, cmd_upper, "VGETATTR") or
            std.mem.eql(u8, cmd_upper, "VSETATTR") or
            std.mem.eql(u8, cmd_upper, "VINFO") or
            std.mem.eql(u8, cmd_upper, "VSIM") or
            std.mem.eql(u8, cmd_upper, "VRANGE") or
            std.mem.eql(u8, cmd_upper, "VLINKS"))
        {
            // Convert RespValue array to string args
            var vargs = try allocator.alloc([]const u8, array.len);
            defer allocator.free(vargs);
            vargs[0] = cmd_upper;
            for (array[1..], 1..) |val, i| {
                vargs[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }

            if (std.mem.eql(u8, cmd_upper, "VADD")) {
                const result = try vector_cmds.cmdVadd(allocator, storage, vargs, client_id);
                defer vector_cmds.deinitRespValue(allocator, result);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "VCARD")) {
                const result = try vector_cmds.cmdVcard(allocator, storage, vargs, client_id);
                defer vector_cmds.deinitRespValue(allocator, result);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "VDIM")) {
                const result = try vector_cmds.cmdVdim(allocator, storage, vargs, client_id);
                defer vector_cmds.deinitRespValue(allocator, result);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "VEMB")) {
                const result = try vector_cmds.cmdVemb(allocator, storage, vargs, client_id);
                defer vector_cmds.deinitRespValue(allocator, result);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "VREM")) {
                const result = try vector_cmds.cmdVrem(allocator, storage, vargs, client_id);
                defer vector_cmds.deinitRespValue(allocator, result);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "VISMEMBER")) {
                const result = try vector_cmds.cmdVismember(allocator, storage, vargs, client_id);
                defer vector_cmds.deinitRespValue(allocator, result);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "VRANDMEMBER")) {
                const result = try vector_cmds.cmdVrandmember(allocator, storage, vargs, client_id);
                defer vector_cmds.deinitRespValue(allocator, result);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "VGETATTR")) {
                const result = try vector_cmds.cmdVgetattr(allocator, storage, vargs, client_id);
                defer vector_cmds.deinitRespValue(allocator, result);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "VSETATTR")) {
                const result = try vector_cmds.cmdVsetattr(allocator, storage, vargs, client_id);
                defer vector_cmds.deinitRespValue(allocator, result);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "VINFO")) {
                const result = try vector_cmds.cmdVinfo(allocator, storage, vargs, client_id);
                defer vector_cmds.deinitRespValue(allocator, result);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "VSIM")) {
                const result = try vector_cmds.cmdVsim(allocator, storage, vargs, client_id);
                defer vector_cmds.deinitRespValue(allocator, result);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "VRANGE")) {
                const result = try vector_cmds.cmdVrange(allocator, storage, vargs, client_id);
                defer vector_cmds.deinitRespValue(allocator, result);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else if (std.mem.eql(u8, cmd_upper, "VLINKS")) {
                const result = try vector_cmds.cmdVlinks(allocator, storage, vargs, client_id);
                defer vector_cmds.deinitRespValue(allocator, result);
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeRespValue(result);
            } else {
                unreachable;
            }
        }

        // ACL commands
        else if (std.mem.eql(u8, cmd_upper, "ACL")) {
            if (array.len < 2) {
                var w = Writer.init(allocator);
                defer w.deinit();
                break :blk try w.writeError("ERR wrong number of arguments for 'acl' command");
            }
            const subcmd = switch (array[1]) {
                .bulk_string => |s| s,
                else => {
                    var w = Writer.init(allocator);
                    defer w.deinit();
                    break :blk try w.writeError("ERR invalid subcommand format");
                },
            };
            const subcmd_upper = try std.ascii.allocUpperString(allocator, subcmd);
            defer allocator.free(subcmd_upper);

            if (std.mem.eql(u8, subcmd_upper, "WHOAMI")) {
                break :blk try acl_cmds.cmdACLWhoami(allocator, array[1..], client_registry, client_id);
            } else if (std.mem.eql(u8, subcmd_upper, "LIST")) {
                break :blk try acl_cmds.cmdACLList(allocator, array[1..], storage);
            } else if (std.mem.eql(u8, subcmd_upper, "USERS")) {
                break :blk try acl_cmds.cmdACLUsers(allocator, array[1..], storage);
            } else if (std.mem.eql(u8, subcmd_upper, "GETUSER")) {
                break :blk try acl_cmds.cmdACLGetuser(allocator, array[1..], storage);
            } else if (std.mem.eql(u8, subcmd_upper, "SETUSER")) {
                break :blk try acl_cmds.cmdACLSetuser(allocator, storage, array[1..]);
            } else if (std.mem.eql(u8, subcmd_upper, "DELUSER")) {
                break :blk try acl_cmds.cmdACLDeluser(allocator, array[1..]);
            } else if (std.mem.eql(u8, subcmd_upper, "CAT")) {
                break :blk try acl_cmds.cmdACLCat(allocator, array[1..]);
            } else if (std.mem.eql(u8, subcmd_upper, "LOG")) {
                break :blk try acl_cmds.cmdACLLog(allocator, array[1..], storage);
            } else if (std.mem.eql(u8, subcmd_upper, "SAVE")) {
                break :blk try acl_cmds.cmdACLSave(allocator, array[1..]);
            } else if (std.mem.eql(u8, subcmd_upper, "LOAD")) {
                break :blk try acl_cmds.cmdACLLoad(allocator, array[1..]);
            } else if (std.mem.eql(u8, subcmd_upper, "HELP")) {
                break :blk try acl_cmds.cmdACLHelp(allocator, array[1..]);
            } else if (std.mem.eql(u8, subcmd_upper, "GENPASS")) {
                break :blk try acl_cmds.cmdACLGenpass(allocator, array[1..]);
            } else if (std.mem.eql(u8, subcmd_upper, "DRYRUN")) {
                break :blk try acl_cmds.cmdACLDryrun(allocator, storage, array[1..]);
            } else {
                var w = Writer.init(allocator);
                defer w.deinit();
                var buf: [256]u8 = undefined;
                const err_msg = try std.fmt.bufPrint(&buf, "ERR unknown ACL subcommand '{s}'", .{subcmd});
                break :blk try w.writeError(err_msg);
            }
        }
        // Utility commands
        else if (std.mem.eql(u8, cmd_upper, "ECHO")) {
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            for (array, 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }
            const client_protocol = client_registry.getProtocol(client_id);
            break :blk try utility_cmds.cmdEcho(allocator, args, storage, ps, null, client_registry, client_id, storage.config, @intFromEnum(client_protocol));
        } else if (std.mem.eql(u8, cmd_upper, "QUIT")) {
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            for (array, 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }
            const client_protocol = client_registry.getProtocol(client_id);
            break :blk try utility_cmds.cmdQuit(allocator, args, storage, ps, null, client_registry, client_id, storage.config, @intFromEnum(client_protocol));
        } else if (std.mem.eql(u8, cmd_upper, "RESET")) {
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            for (array, 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }
            const client_protocol = client_registry.getProtocol(client_id);
            break :blk try utility_cmds.cmdReset(allocator, args, storage, ps, tx, client_registry, client_id, storage.config, @intFromEnum(client_protocol));
        } else if (std.mem.eql(u8, cmd_upper, "TIME")) {
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            for (array, 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }
            const client_protocol = client_registry.getProtocol(client_id);
            break :blk try utility_cmds.cmdTime(allocator, args, storage, ps, null, client_registry, client_id, storage.config, @intFromEnum(client_protocol));
        } else if (std.mem.eql(u8, cmd_upper, "LASTSAVE")) {
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            for (array, 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }
            const client_protocol = client_registry.getProtocol(client_id);
            break :blk try utility_cmds.cmdLastsave(allocator, args, storage, ps, null, client_registry, client_id, storage.config, @intFromEnum(client_protocol));
        } else if (std.mem.eql(u8, cmd_upper, "SHUTDOWN")) {
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            for (array, 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }
            const client_protocol = client_registry.getProtocol(client_id);
            break :blk try utility_cmds.cmdShutdown(allocator, args, storage, ps, null, client_registry, client_id, storage.config, @intFromEnum(client_protocol), shutdown_state);
        } else if (std.mem.eql(u8, cmd_upper, "MONITOR")) {
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            for (array, 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }
            const client_protocol = client_registry.getProtocol(client_id);
            break :blk try utility_cmds.cmdMonitor(allocator, args, storage, ps, null, client_registry, client_id, storage.config, @intFromEnum(client_protocol));
        } else if (std.mem.eql(u8, cmd_upper, "DEBUG")) {
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            for (array, 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }
            const client_protocol = client_registry.getProtocol(client_id);
            break :blk try utility_cmds.cmdDebug(allocator, args, storage, ps, null, client_registry, client_id, storage.config, @intFromEnum(client_protocol));
        } else if (std.mem.eql(u8, cmd_upper, "LOLWUT")) {
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            for (array, 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }
            const client_protocol = client_registry.getProtocol(client_id);
            break :blk try utility_cmds.cmdLolwut(allocator, args, storage, ps, null, client_registry, client_id, storage.config, @intFromEnum(client_protocol));
        } else if (std.mem.eql(u8, cmd_upper, "SELECT")) {
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            for (array, 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }
            const client_protocol = client_registry.getProtocol(client_id);
            break :blk try utility_cmds.cmdSelect(allocator, args, storage, ps, null, client_registry, client_id, storage.config, @intFromEnum(client_protocol));
        } else if (std.mem.eql(u8, cmd_upper, "SWAPDB")) {
            var args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(args);
            for (array, 0..) |val, i| {
                args[i] = switch (val) {
                    .bulk_string => |s| s,
                    else => "",
                };
            }
            const client_protocol = client_registry.getProtocol(client_id);
            break :blk try utility_cmds.cmdSwapdb(allocator, args, storage, ps, null, client_registry, client_id, storage.config, @intFromEnum(client_protocol), databases, num_databases);
        } else {
            var w = Writer.init(allocator);
            defer w.deinit();
            var buf: [256]u8 = undefined;
            const err_msg = try std.fmt.bufPrint(&buf, "ERR unknown command '{s}'", .{cmd_name});
            return w.writeError(err_msg);
        }
    };

    // Update CLIENT LIST sub/psub/ssub counts after pub/sub command changes subscriptions.
    // These counts are used by CLIENT LIST and CLIENT INFO to report subscription state.
    if (std.mem.eql(u8, cmd_upper, "SUBSCRIBE") or
        std.mem.eql(u8, cmd_upper, "UNSUBSCRIBE") or
        std.mem.eql(u8, cmd_upper, "PSUBSCRIBE") or
        std.mem.eql(u8, cmd_upper, "PUNSUBSCRIBE") or
        std.mem.eql(u8, cmd_upper, "SSUBSCRIBE") or
        std.mem.eql(u8, cmd_upper, "SUNSUBSCRIBE"))
    {
        client_registry.updateSubCounts(
            client_id,
            @intCast(ps.channelCount(subscriber_id)),
            @intCast(ps.patternCount(subscriber_id)),
            @intCast(ps.shardedChannelCount(subscriber_id)),
        );
    }

    // Log write commands to AOF (best-effort, skip on error)
    // Also mark any watched keys dirty so EXEC can detect conflicts.
    if (is_write_cmd) {
        // Mark watched keys dirty if the written key is being watched.
        // The key is always args[1] for single-key write commands.
        if (array.len >= 2) {
            const written_key = switch (array[1]) {
                .bulk_string => |s| s,
                else => "",
            };
            if (written_key.len > 0) {
                tx_mod.markWatchedDirty(tx, written_key);
            }
        }

        if (aof) |a| {
            // Build string slice from the RespValue array
            var aof_args = try allocator.alloc([]const u8, array.len);
            defer allocator.free(aof_args);
            var valid = true;
            for (array, 0..) |arg, i| {
                aof_args[i] = switch (arg) {
                    .bulk_string => |s| s,
                    else => { valid = false; break; },
                };
            }
            if (valid) {
                a.appendCommandDb0(aof_args) catch |err| {
                    std.debug.print("AOF write warning: {any}\n", .{err});
                };
            }
        }

        // Propagate write command to all online replicas
        if (repl) |r| {
            if (r.role == .primary and r.replicas.items.len > 0) {
                // Re-serialize the command as RESP for propagation
                var w = Writer.init(allocator);
                defer w.deinit();
                const resp_bytes = w.serialize(cmd) catch null;
                if (resp_bytes) |bytes| {
                    defer allocator.free(bytes);
                    r.propagate(bytes);
                }
            }

            // Update client's replication offset for WAIT command
            // This records the replication offset after this write command
            if (r.role == .primary) {
                client_registry.updateClientReplOffset(client_id, r.repl_offset);
            }
        }
    }

    return response;
}

/// EXEC — execute all queued commands in the transaction.
/// Returns a null array if WATCH detected a dirty key, otherwise
/// returns an array of results for each queued command.
fn cmdExec(
    allocator: std.mem.Allocator,
    storage: *Storage,
    aof: ?*Aof,
    ps: *PubSub,
    subscriber_id: u64,
    tx: *TxState,
    repl: ?*ReplicationState,
    my_port: u16,
    client_registry: *ClientRegistry,
    client_id: u64,
    script_store: *ScriptStore,
    shutdown_state: ?*@import("../server.zig").ShutdownState,
    databases: []Storage,
    num_databases: u16,
) anyerror![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (!tx.active) {
        return w.writeError("ERR EXEC without MULTI");
    }

    // If a watched key was modified, abort the transaction.
    if (tx.dirty) {
        tx.reset();
        return w.writeArray(null);
    }

    // Execute each queued command.
    const parser_mod = @import("../protocol/parser.zig");
    const queue_len = tx.queue.items.len;
    var results = try allocator.alloc([]const u8, queue_len);
    defer {
        for (results) |r| allocator.free(r);
        allocator.free(results);
    }

    // We need a fresh TxState for recursive execution (EXEC cannot be nested)
    var inner_tx = TxState.init(allocator);
    defer inner_tx.deinit();

    var executed: usize = 0;
    for (tx.queue.items, 0..) |qc, i| {
        var p = parser_mod.Parser.init(allocator);
        defer p.deinit();

        const parsed_cmd = p.parse(qc.data) catch {
            results[i] = try w.writeError("ERR command parse error during EXEC");
            executed += 1;
            continue;
        };
        defer p.freeValue(parsed_cmd);

        results[i] = executeCommand(
            allocator,
            storage,
            parsed_cmd,
            aof,
            ps,
            subscriber_id,
            &inner_tx,
            repl,
            my_port,
            null,
            null,
            client_registry,
            client_id,
            script_store,
            shutdown_state,
            databases,
            num_databases,
        ) catch |err| blk: {
            var buf: [64]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "ERR command error: {any}", .{err}) catch "ERR internal error";
            break :blk try w.writeError(msg);
        };
        executed += 1;
    }

    // Build the response array from raw RESP bytes.
    // We concatenate all individual result bytes into one array response.
    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);

    const count_str = try std.fmt.allocPrint(allocator, "*{d}\r\n", .{executed});
    defer allocator.free(count_str);
    try out.appendSlice(allocator, count_str);

    for (results[0..executed]) |r| {
        try out.appendSlice(allocator, r);
    }

    tx.reset();
    return out.toOwnedSlice(allocator);
}

/// Helper to extract bulk strings from RespValue array
fn extractBulkStrings(allocator: std.mem.Allocator, args: []const RespValue) ![][]const u8 {
    var result = try allocator.alloc([]const u8, args.len);
    for (args, 0..) |arg, i| {
        result[i] = switch (arg) {
            .bulk_string => |s| s,
            else => return error.InvalidArgument,
        };
    }
    return result;
}

/// PING [message]
/// Returns PONG or echoes the message
fn cmdPing(allocator: std.mem.Allocator, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len == 1) {
        // No argument: return +PONG\r\n
        return w.writeSimpleString("PONG");
    } else if (args.len == 2) {
        // With argument: return bulk string
        const message = switch (args[1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR wrong number of arguments for 'ping' command"),
        };
        return w.writeBulkString(message);
    } else {
        return w.writeError("ERR wrong number of arguments for 'ping' command");
    }
}

/// SET key value [EX seconds] [PX milliseconds] [NX|XX]
/// Returns +OK or $-1 if condition not met
fn cmdSet(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32, client_registry: *ClientRegistry, client_id: u64) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Minimum: SET key value
    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'set' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const value = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid value"),
    };

    var expires_at: ?i64 = null;
    var nx = false;
    var xx = false;
    var get_flag = false;
    var keepttl = false;

    // Parse options
    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        const opt_upper = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(opt_upper);

        if (std.mem.eql(u8, opt_upper, "EX")) {
            if (expires_at != null or keepttl) {
                return w.writeError("ERR syntax error");
            }
            i += 1;
            if (i >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const seconds = parseInteger(args[i]) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            if (seconds <= 0) {
                return w.writeError("ERR invalid expire time in 'set' command");
            }
            expires_at = Storage.getCurrentTimestamp() + (seconds * 1000);
        } else if (std.mem.eql(u8, opt_upper, "PX")) {
            if (expires_at != null or keepttl) {
                return w.writeError("ERR syntax error");
            }
            i += 1;
            if (i >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const milliseconds = parseInteger(args[i]) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            if (milliseconds <= 0) {
                return w.writeError("ERR invalid expire time in 'set' command");
            }
            expires_at = Storage.getCurrentTimestamp() + milliseconds;
        } else if (std.mem.eql(u8, opt_upper, "EXAT")) {
            if (expires_at != null or keepttl) {
                return w.writeError("ERR syntax error");
            }
            i += 1;
            if (i >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const unix_sec = parseInteger(args[i]) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            if (unix_sec < 0) {
                return w.writeError("ERR invalid expire time in 'set' command");
            }
            expires_at = unix_sec * 1000; // convert seconds to milliseconds
        } else if (std.mem.eql(u8, opt_upper, "PXAT")) {
            if (expires_at != null or keepttl) {
                return w.writeError("ERR syntax error");
            }
            i += 1;
            if (i >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const unix_ms = parseInteger(args[i]) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            if (unix_ms < 0) {
                return w.writeError("ERR invalid expire time in 'set' command");
            }
            expires_at = unix_ms;
        } else if (std.mem.eql(u8, opt_upper, "KEEPTTL")) {
            if (expires_at != null) {
                return w.writeError("ERR syntax error");
            }
            keepttl = true;
        } else if (std.mem.eql(u8, opt_upper, "NX")) {
            if (xx) {
                return w.writeError("ERR syntax error");
            }
            nx = true;
        } else if (std.mem.eql(u8, opt_upper, "XX")) {
            if (nx) {
                return w.writeError("ERR syntax error");
            }
            xx = true;
        } else if (std.mem.eql(u8, opt_upper, "GET")) {
            get_flag = true;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // GET option: retrieve old value before applying NX/XX/SET logic.
    // Returns WRONGTYPE if key exists as a non-string type.
    var old_value_buf: ?[]u8 = null;
    defer if (old_value_buf) |buf| allocator.free(buf);

    if (get_flag) {
        const entry = storage.getStringWithExpiry(key) catch {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        };
        if (entry) |e| {
            old_value_buf = try allocator.dupe(u8, e.value);
        }
    }

    // Check NX condition (key must NOT exist)
    if (nx and storage.exists(key)) {
        return if (get_flag) w.writeBulkString(old_value_buf) else w.writeNull();
    }

    // Check XX condition (key MUST exist)
    if (xx and !storage.exists(key)) {
        return if (get_flag) w.writeNull() else w.writeNull();
    }

    // Resolve expiry: KEEPTTL preserves existing TTL, otherwise use computed expires_at
    var final_expires_at: ?i64 = expires_at;
    if (keepttl) {
        if (storage.getStringWithExpiry(key) catch null) |e| {
            final_expires_at = e.expires_at;
        }
        // If key doesn't exist or is wrong type, KEEPTTL sets no expiry (new key)
    }

    // Execute SET
    const was_new = !storage.exists(key);
    try storage.set(key, value, final_expires_at);

    // Notify clients about key invalidation (generate messages and cleanup tracking)
    client_cmds.notifyInvalidation(client_registry, key, client_id, allocator) catch |err| {
        std.log.warn("SET: failed to notify invalidation for key '{s}': {}", .{ key, err });
    };

    // Publish keyspace notification
    notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .string, "set");
    if (was_new) {
        notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .new, "new");
    }

    return if (get_flag) w.writeBulkString(old_value_buf) else w.writeOK();
}

/// GET key
/// Returns bulk string value or null
fn cmdGet(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, client_registry: *ClientRegistry, client_id: u64) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'get' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const value = storage.get(key);

    // Track key access for client-side caching
    client_registry.trackKeyAccess(client_id, key) catch |err| {
        std.log.warn("GET: failed to track key '{s}': {}", .{ key, err });
    };

    return w.writeBulkString(value);
}

/// DEL key [key ...]
/// Returns integer count of deleted keys
fn cmdDel(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32, client_registry: *ClientRegistry, client_id: u64) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'del' command");
    }

    // Extract keys
    var keys = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 1);
    defer keys.deinit(allocator);

    for (args[1..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    // Publish notifications for keys that actually exist before deletion
    for (keys.items) |del_key| {
        if (storage.exists(del_key)) {
            notifyKeyspaceEvent(allocator, storage, ps, db_index, del_key, .generic, "del");
        }
    }

    // Check if lazyfree-lazy-user-del is enabled
    const use_async = (storage.config.getConfigValue("lazyfree-lazy-user-del") catch ConfigValue{ .bool = false }).bool;

    const deleted_count = if (use_async)
        try storage.unlinkAsync(keys.items)
    else
        storage.del(keys.items);

    // Notify clients about deleted keys (generate messages and cleanup tracking)
    client_cmds.notifyInvalidationBatch(client_registry, keys.items, client_id, allocator) catch |err| {
        std.log.warn("DEL: failed to notify invalidation: {}", .{err});
    };

    return w.writeInteger(@intCast(deleted_count));
}

/// EXISTS key [key ...]
/// Returns integer count of existing keys (duplicates counted multiple times)
fn cmdExists(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'exists' command");
    }

    var count: i64 = 0;
    for (args[1..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        if (storage.exists(key)) {
            count += 1;
        }
    }

    return w.writeInteger(count);
}

/// SAVE — synchronous RDB snapshot to dump.rdb
/// Returns +OK on success, -ERR on failure
fn cmdSave(allocator: std.mem.Allocator, storage: *Storage) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    Persistence.saveSingleDb(storage, DEFAULT_RDB_PATH, allocator) catch |err| {
        var buf: [128]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "ERR SAVE failed: {any}", .{err}) catch "ERR SAVE failed";
        return w.writeError(msg);
    };

    return w.writeSimpleString("OK");
}

/// BGSAVE — same as SAVE for now (no background fork on Zig)
fn cmdBgsave(allocator: std.mem.Allocator, storage: *Storage) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    Persistence.saveSingleDb(storage, DEFAULT_RDB_PATH, allocator) catch |err| {
        var buf: [128]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "ERR BGSAVE failed: {any}", .{err}) catch "ERR BGSAVE failed";
        return w.writeError(msg);
    };

    return w.writeSimpleString("Background saving started");
}

/// BGREWRITEAOF — rewrite the AOF from current storage state
fn cmdBgrewriteaof(allocator: std.mem.Allocator, storage: *Storage) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    Aof.rewrite(storage, DEFAULT_AOF_PATH, allocator) catch |err| {
        var buf: [128]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "ERR BGREWRITEAOF failed: {any}", .{err}) catch "ERR BGREWRITEAOF failed";
        return w.writeError(msg);
    };

    return w.writeSimpleString("Background append only file rewriting started");
}

/// DBSIZE — return number of keys in current database
fn cmdDbsize(allocator: std.mem.Allocator, storage: *Storage) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    const size: i64 = @intCast(storage.dbSize());
    return w.writeInteger(size);
}

/// FLUSHDB / FLUSHALL — remove all keys
/// Supports optional ASYNC/SYNC mode for lazy freeing
fn cmdFlushall(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Parse optional ASYNC/SYNC argument
    var is_async: ?bool = null;
    if (args.len > 1) {
        const mode_str = switch (args[1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        if (std.ascii.eqlIgnoreCase(mode_str, "ASYNC")) {
            is_async = true;
        } else if (std.ascii.eqlIgnoreCase(mode_str, "SYNC")) {
            is_async = false;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    if (args.len > 2) {
        return w.writeError("ERR syntax error");
    }

    // If no explicit mode, check config for default
    if (is_async == null) {
        is_async = (storage.config.getConfigValue("lazyfree-lazy-user-flush") catch ConfigValue{ .bool = false }).bool;
    }

    if (is_async.?) {
        // Submit flush work to background thread
        try storage.flushAllAsync();
    } else {
        // Synchronous flush
        storage.flushAll();
    }

    return w.writeSimpleString("OK");
}

/// PUBSUB subcommand [args...]
/// Routes to CHANNELS, NUMSUB, NUMPAT, or HELP sub-commands.
fn cmdPubsub(allocator: std.mem.Allocator, ps: *PubSub, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'pubsub' command");
    }

    const sub_name = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid subcommand"),
    };

    const sub_upper = try std.ascii.allocUpperString(allocator, sub_name);
    defer allocator.free(sub_upper);

    if (std.mem.eql(u8, sub_upper, "CHANNELS")) {
        return pubsub_cmds.cmdPubsubChannels(allocator, ps, args);
    } else if (std.mem.eql(u8, sub_upper, "NUMSUB")) {
        return pubsub_cmds.cmdPubsubNumsub(allocator, ps, args);
    } else if (std.mem.eql(u8, sub_upper, "NUMPAT")) {
        return pubsub_cmds.cmdPubsubNumpat(allocator, ps);
    } else if (std.mem.eql(u8, sub_upper, "SHARDCHANNELS")) {
        return pubsub_cmds.cmdPubsubShardchannels(allocator, ps, args);
    } else if (std.mem.eql(u8, sub_upper, "SHARDNUMSUB")) {
        return pubsub_cmds.cmdPubsubShardnumsub(allocator, ps, args);
    } else if (std.mem.eql(u8, sub_upper, "HELP")) {
        return pubsub_cmds.cmdPubsubHelp(allocator);
    } else {
        var buf: [128]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "ERR unknown subcommand '{s}'", .{sub_name}) catch "ERR unknown subcommand";
        return w.writeError(msg);
    }
}

// Helper functions

/// Convert a slice of RespValue to a slice of string slices.
/// Caller must free the returned slice with `allocator.free`.
/// Note: the strings themselves point into the original RespValues and are not copied.
fn arrayToStrings(allocator: std.mem.Allocator, args: []const RespValue) ![]const []const u8 {
    var result = try allocator.alloc([]const u8, args.len);
    for (args, 0..) |arg, i| {
        result[i] = switch (arg) {
            .bulk_string => |s| s,
            else => "",
        };
    }
    return result;
}

fn parseInteger(value: RespValue) !i64 {
    const str = switch (value) {
        .bulk_string => |s| s,
        else => return error.InvalidInteger,
    };
    return std.fmt.parseInt(i64, str, 10);
}

// ── String counter commands ───────────────────────────────────────────────────

/// INCR key
/// Increments the integer value of key by 1.
fn cmdIncr(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'incr' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const new_val = storage.incrby(key, 1) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.NotInteger => return w.writeError("ERR value is not an integer or out of range"),
        error.Overflow => return w.writeError("ERR increment or decrement would overflow"),
        else => return err,
    };

    // Publish keyspace notification
    notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .string, "incr");

    return w.writeInteger(new_val);
}

/// DECR key
/// Decrements the integer value of key by 1.
fn cmdDecr(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'decr' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const new_val = storage.incrby(key, -1) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.NotInteger => return w.writeError("ERR value is not an integer or out of range"),
        error.Overflow => return w.writeError("ERR increment or decrement would overflow"),
        else => return err,
    };

    // Publish keyspace notification
    notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .string, "decr");

    return w.writeInteger(new_val);
}

/// INCRBY key increment
/// Increments the integer value of key by increment.
fn cmdIncrby(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'incrby' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const delta = parseInteger(args[2]) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    const new_val = storage.incrby(key, delta) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.NotInteger => return w.writeError("ERR value is not an integer or out of range"),
        error.Overflow => return w.writeError("ERR increment or decrement would overflow"),
        else => return err,
    };

    // Publish keyspace notification
    notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .string, "incrby");

    return w.writeInteger(new_val);
}

/// DECRBY key decrement
/// Decrements the integer value of key by decrement.
fn cmdDecrby(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'decrby' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const delta = parseInteger(args[2]) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    // Negate and add (DECRBY x n == INCRBY x -n)
    const neg_delta = std.math.negate(delta) catch {
        return w.writeError("ERR increment or decrement would overflow");
    };

    const new_val = storage.incrby(key, neg_delta) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.NotInteger => return w.writeError("ERR value is not an integer or out of range"),
        error.Overflow => return w.writeError("ERR increment or decrement would overflow"),
        else => return err,
    };

    // Publish keyspace notification
    notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .string, "decrby");

    return w.writeInteger(new_val);
}

/// INCRBYFLOAT key increment
/// Increment the float value of key by increment.
fn cmdIncrbyfloat(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, protocol_version: RespProtocol) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'incrbyfloat' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const delta_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not a valid float"),
    };

    const delta = std.fmt.parseFloat(f64, delta_str) catch {
        return w.writeError("ERR value is not a valid float");
    };

    // Reject NaN and infinity before calling storage (gives better error message)
    if (std.math.isNan(delta) or std.math.isInf(delta)) {
        return w.writeError("ERR increment would produce NaN or Infinity");
    }

    const new_val = storage.incrbyfloat(key, delta) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.NotFloat => return w.writeError("ERR value is not a valid float"),
        error.NanOrInfinity => return w.writeError("ERR increment would produce NaN or Infinity"),
        else => return err,
    };

    // RESP3: return native double type
    if (protocol_version == .RESP3) {
        return w.writeDouble(new_val);
    }

    // Format matching storage's formatFloat (decimal, no trailing zeros)
    var buf: [64]u8 = undefined;
    const raw = std.fmt.bufPrint(&buf, "{d}", .{new_val}) catch return error.OutOfMemory;
    const formatted = blk: {
        if (std.mem.indexOf(u8, raw, ".") == null) break :blk raw;
        var end = raw.len;
        while (end > 0 and raw[end - 1] == '0') end -= 1;
        if (end > 0 and raw[end - 1] == '.') end -= 1;
        break :blk raw[0..end];
    };
    return w.writeBulkString(formatted);
}

// ── String utility commands ───────────────────────────────────────────────────

/// APPEND key value
/// Appends value to the string stored at key. Returns new length.
fn cmdAppend(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'append' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const suffix = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid value"),
    };

    const new_len = storage.appendString(key, suffix) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.StringTooLarge => return w.writeError("ERR string exceeds maximum allowed size (512mb)"),
        else => return err,
    };

    // Publish keyspace notification
    notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .string, "append");

    return w.writeInteger(@intCast(new_len));
}

/// STRLEN key
/// Returns the length of the string value stored at key.
fn cmdStrlen(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'strlen' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Check type first
    const vtype = storage.getType(key);
    if (vtype != null and vtype.? != .string) {
        return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    const val = storage.get(key);
    const len: i64 = if (val) |v| @intCast(v.len) else 0;
    return w.writeInteger(len);
}

/// GETSET key value
///
/// DEPRECATED: This command is deprecated since Redis 6.2.0.
/// Use `SET key value GET` instead. GETSET is provided for backwards compatibility only.
///
/// Sets key to value and returns the old value.
/// Functionally equivalent to: SET key value GET
pub fn cmdGetset(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'getset' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const value = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid value"),
    };

    // Check type
    const vtype = storage.getType(key);
    if (vtype != null and vtype.? != .string) {
        return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    // Get old value before overwriting
    const old_val = storage.get(key);
    const old_copy: ?[]u8 = if (old_val) |v| try allocator.dupe(u8, v) else null;
    defer if (old_copy) |c| allocator.free(c);

    try storage.set(key, value, null);

    // Fire "set" keyspace notification (GETSET always overwrites)
    notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .string, "set");

    return w.writeBulkString(old_copy);
}

/// GETDEL key
/// Gets the value and deletes the key.
pub fn cmdGetdel(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'getdel' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const val = storage.getdel(key) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => return err,
    };
    defer if (val) |v| allocator.free(v);

    // Fire "del" keyspace notification only when the key actually existed and was deleted
    if (val != null) {
        notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .generic, "del");
    }

    return w.writeBulkString(val);
}

/// GETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-ms | PERSIST]
/// Gets the value and optionally updates the expiry.
fn cmdGetex(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'getex' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    var expires_at: ?i64 = null;
    var persist = false;

    if (args.len >= 3) {
        const opt = switch (args[2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const opt_upper = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(opt_upper);

        if (std.mem.eql(u8, opt_upper, "PERSIST")) {
            persist = true;
        } else if (std.mem.eql(u8, opt_upper, "EX")) {
            if (args.len < 4) return w.writeError("ERR syntax error");
            const secs = parseInteger(args[3]) catch return w.writeError("ERR value is not an integer or out of range");
            if (secs <= 0) return w.writeError("ERR invalid expire time in 'getex' command");
            expires_at = Storage.getCurrentTimestamp() + (secs * 1000);
        } else if (std.mem.eql(u8, opt_upper, "PX")) {
            if (args.len < 4) return w.writeError("ERR syntax error");
            const ms = parseInteger(args[3]) catch return w.writeError("ERR value is not an integer or out of range");
            if (ms <= 0) return w.writeError("ERR invalid expire time in 'getex' command");
            expires_at = Storage.getCurrentTimestamp() + ms;
        } else if (std.mem.eql(u8, opt_upper, "EXAT")) {
            if (args.len < 4) return w.writeError("ERR syntax error");
            const ts = parseInteger(args[3]) catch return w.writeError("ERR value is not an integer or out of range");
            if (ts < 0) return w.writeError("ERR invalid expire time in 'getex' command");
            expires_at = ts * 1000;
        } else if (std.mem.eql(u8, opt_upper, "PXAT")) {
            if (args.len < 4) return w.writeError("ERR syntax error");
            const ts = parseInteger(args[3]) catch return w.writeError("ERR value is not an integer or out of range");
            if (ts < 0) return w.writeError("ERR invalid expire time in 'getex' command");
            expires_at = ts;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    const val = storage.getex(key, expires_at, persist) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        else => return err,
    };
    defer if (val) |v| allocator.free(v);

    // Publish "getex" notification (for expiry modification)
    if (val != null and (expires_at != null or persist)) {
        notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .string, "getex");
    }

    return w.writeBulkString(val);
}

/// SETNX key value
///
/// DEPRECATED: This command is deprecated since Redis 2.6.12.
/// Use `SET key value NX` instead. SETNX is provided for backwards compatibility only.
///
/// Set key to value only if key does not exist. Returns 1 if set, 0 if not.
/// Functionally equivalent to: SET key value NX
pub fn cmdSetnx(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'setnx' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const value = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid value"),
    };

    if (storage.exists(key)) {
        return w.writeInteger(0);
    }

    try storage.set(key, value, null);
    // Fire "set" notification only when key was actually set
    notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .string, "set");
    return w.writeInteger(1);
}

/// SETEX key seconds value
///
/// DEPRECATED: This command is deprecated since Redis 2.6.12.
/// Use `SET key value EX seconds` instead. SETEX is provided for backwards compatibility only.
///
/// Set key to value with expiry in seconds.
/// Functionally equivalent to: SET key value EX seconds
fn cmdSetex(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'setex' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const seconds = parseInteger(args[2]) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    if (seconds <= 0) {
        return w.writeError("ERR invalid expire time in 'setex' command");
    }

    const value = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid value"),
    };

    const expires_at = Storage.getCurrentTimestamp() + (seconds * 1000);
    const was_new = !storage.exists(key);
    try storage.set(key, value, expires_at);

    // Publish keyspace notification
    notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .string, "setex");
    if (was_new) {
        notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .new, "new");
    }

    return w.writeOK();
}

/// PSETEX key milliseconds value
///
/// DEPRECATED: This command is deprecated since Redis 2.6.12.
/// Use `SET key value PX milliseconds` instead. PSETEX is provided for backwards compatibility only.
///
/// Set key to value with expiry in milliseconds.
/// Functionally equivalent to: SET key value PX milliseconds
fn cmdPsetex(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'psetex' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const milliseconds = parseInteger(args[2]) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    if (milliseconds <= 0) {
        return w.writeError("ERR invalid expire time in 'psetex' command");
    }

    const value = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid value"),
    };

    const expires_at = Storage.getCurrentTimestamp() + milliseconds;
    const was_new = !storage.exists(key);
    try storage.set(key, value, expires_at);

    // Publish keyspace notification
    notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .string, "psetex");
    if (was_new) {
        notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .new, "new");
    }

    return w.writeOK();
}

// ── Multi-key string commands ─────────────────────────────────────────────────

/// MGET key [key ...]
/// Returns values for each key (null bulk string for missing/wrong-type keys).
fn cmdMget(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, client_registry: *ClientRegistry, client_id: u64) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'mget' command");
    }

    const count = args.len - 1;
    var buf = std.ArrayList(u8){ .items = &.{}, .capacity = 0 };
    defer buf.deinit(allocator);

    const header = try std.fmt.allocPrint(allocator, "*{d}\r\n", .{count});
    defer allocator.free(header);
    try buf.appendSlice(allocator, header);

    for (args[1..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => {
                try buf.appendSlice(allocator, "$-1\r\n");
                continue;
            },
        };

        // Only return string values; wrong-type keys return null
        const vtype = storage.getType(key);
        if (vtype != null and vtype.? != .string) {
            try buf.appendSlice(allocator, "$-1\r\n");
            continue;
        }

        const val = storage.get(key);
        if (val) |v| {
            const elem = try std.fmt.allocPrint(allocator, "${d}\r\n{s}\r\n", .{ v.len, v });
            defer allocator.free(elem);
            try buf.appendSlice(allocator, elem);
        } else {
            try buf.appendSlice(allocator, "$-1\r\n");
        }

        // Track key access for client-side caching
        client_registry.trackKeyAccess(client_id, key) catch {};
    }

    return buf.toOwnedSlice(allocator);
}

/// MSET key value [key value ...]
/// Sets multiple keys to multiple values.
fn cmdMset(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3 or (args.len - 1) % 2 != 0) {
        return w.writeError("ERR wrong number of arguments for 'mset' command");
    }

    var i: usize = 1;
    while (i < args.len) : (i += 2) {
        const key = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        const value = switch (args[i + 1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid value"),
        };

        try storage.set(key, value, null);
    }

    return w.writeOK();
}

/// MSETNX key value [key value ...]
/// Sets multiple keys to multiple values, only if none of the keys exist.
/// Returns 1 if all keys were set, 0 if none were set (atomic).
fn cmdMsetnx(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3 or (args.len - 1) % 2 != 0) {
        return w.writeError("ERR wrong number of arguments for 'msetnx' command");
    }

    // Check if any key already exists
    var i: usize = 1;
    while (i < args.len) : (i += 2) {
        const key = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        if (storage.exists(key)) {
            return w.writeInteger(0);
        }
    }

    // All keys are new — set them all
    i = 1;
    while (i < args.len) : (i += 2) {
        const key = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };

        const value = switch (args[i + 1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid value"),
        };

        try storage.set(key, value, null);
    }

    return w.writeInteger(1);
}

/// MSETEX numkeys key value [key value ...] [NX | XX] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
/// Atomically sets multiple string keys with optional shared expiration.
/// - numkeys: number of key-value pairs to set
/// - NX: only set if NONE of the keys exist
/// - XX: only set if ALL of the keys exist
/// - EX/PX/EXAT/PXAT/KEEPTTL: expiration options (mutually exclusive)
/// Returns 1 if all keys were set, 0 if none were set.
fn cmdMsetex(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'msetex' command");
    }

    // Parse numkeys
    const numkeys_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR numkeys must be a number"),
    };
    const numkeys = std.fmt.parseInt(usize, numkeys_str, 10) catch {
        return w.writeError("ERR numkeys must be a valid integer");
    };

    if (numkeys == 0) {
        return w.writeError("ERR numkeys must be positive");
    }

    // Must have at least numkeys*2 arguments for key-value pairs
    const expected_min_args = 2 + numkeys * 2;
    if (args.len < expected_min_args) {
        return w.writeError("ERR wrong number of arguments for 'msetex' command");
    }

    // Parse key-value pairs
    var keys = try std.ArrayList([]const u8).initCapacity(allocator, numkeys);
    defer keys.deinit(allocator);
    var values = try std.ArrayList([]const u8).initCapacity(allocator, numkeys);
    defer values.deinit(allocator);

    var i: usize = 2;
    var count: usize = 0;
    while (count < numkeys) : (count += 1) {
        if (i >= args.len) {
            return w.writeError("ERR wrong number of arguments for 'msetex' command");
        }

        const key = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        const value = switch (args[i + 1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid value"),
        };

        try keys.append(allocator, key);
        try values.append(allocator, value);
        i += 2;
    }

    // Parse options
    var nx_flag = false;
    var xx_flag = false;
    var expires_at: ?i64 = null;
    var keepttl_flag = false;

    while (i < args.len) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        const opt_upper = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(opt_upper);

        if (std.mem.eql(u8, opt_upper, "NX")) {
            if (xx_flag) {
                return w.writeError("ERR NX and XX options at the same time are not compatible");
            }
            nx_flag = true;
            i += 1;
        } else if (std.mem.eql(u8, opt_upper, "XX")) {
            if (nx_flag) {
                return w.writeError("ERR NX and XX options at the same time are not compatible");
            }
            xx_flag = true;
            i += 1;
        } else if (std.mem.eql(u8, opt_upper, "KEEPTTL")) {
            if (expires_at != null) {
                return w.writeError("ERR KEEPTTL and expiration options at the same time are not compatible");
            }
            keepttl_flag = true;
            i += 1;
        } else if (std.mem.eql(u8, opt_upper, "EX")) {
            if (keepttl_flag or expires_at != null) {
                return w.writeError("ERR syntax error");
            }
            i += 1;
            if (i >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const seconds_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR value is not an integer or out of range"),
            };
            const seconds = std.fmt.parseInt(i64, seconds_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            if (seconds <= 0) {
                return w.writeError("ERR invalid expire time in 'msetex' command");
            }
            expires_at = std.time.milliTimestamp() + (seconds * 1000);
            i += 1;
        } else if (std.mem.eql(u8, opt_upper, "PX")) {
            if (keepttl_flag or expires_at != null) {
                return w.writeError("ERR syntax error");
            }
            i += 1;
            if (i >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const millis_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR value is not an integer or out of range"),
            };
            const millis = std.fmt.parseInt(i64, millis_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            if (millis <= 0) {
                return w.writeError("ERR invalid expire time in 'msetex' command");
            }
            expires_at = std.time.milliTimestamp() + millis;
            i += 1;
        } else if (std.mem.eql(u8, opt_upper, "EXAT")) {
            if (keepttl_flag or expires_at != null) {
                return w.writeError("ERR syntax error");
            }
            i += 1;
            if (i >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const timestamp_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR value is not an integer or out of range"),
            };
            const timestamp = std.fmt.parseInt(i64, timestamp_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            expires_at = timestamp * 1000;
            i += 1;
        } else if (std.mem.eql(u8, opt_upper, "PXAT")) {
            if (keepttl_flag or expires_at != null) {
                return w.writeError("ERR syntax error");
            }
            i += 1;
            if (i >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const timestamp_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR value is not an integer or out of range"),
            };
            const timestamp = std.fmt.parseInt(i64, timestamp_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
            expires_at = timestamp;
            i += 1;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Execute msetex
    const success = try storage.msetex(
        keys.items,
        values.items,
        expires_at,
        nx_flag,
        xx_flag,
        keepttl_flag,
    );

    return w.writeInteger(if (success) 1 else 0);
}

/// LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]
/// Find the longest common subsequence between two strings.
/// Returns the LCS string by default, or metadata based on options:
/// - LEN: return only the length of the LCS
/// - IDX: return match positions as arrays
/// - MINMATCHLEN: minimum match length for IDX mode (default 0)
/// - WITHMATCHLEN: include match lengths in IDX output
fn cmdLcs(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3) {
        return w.writeError("ERR wrong number of arguments for 'lcs' command");
    }

    const key1 = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const key2 = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse options
    var len_only = false;
    var with_idx = false;
    var min_match_len: usize = 0;
    var with_match_len = false;

    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        const opt = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };

        const opt_upper = try std.ascii.allocUpperString(allocator, opt);
        defer allocator.free(opt_upper);

        if (std.mem.eql(u8, opt_upper, "LEN")) {
            len_only = true;
        } else if (std.mem.eql(u8, opt_upper, "IDX")) {
            with_idx = true;
        } else if (std.mem.eql(u8, opt_upper, "MINMATCHLEN")) {
            // Parse the value
            i += 1;
            if (i >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const val_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR syntax error"),
            };
            min_match_len = std.fmt.parseInt(usize, val_str, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            };
        } else if (std.mem.eql(u8, opt_upper, "WITHMATCHLEN")) {
            with_match_len = true;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Get the two strings
    const str1 = storage.get(key1) orelse "";
    const str2 = storage.get(key2) orelse "";

    // Compute LCS using dynamic programming
    const lcs_result = try computeLcs(allocator, str1, str2);
    defer lcs_result.deinit(allocator);

    if (len_only) {
        // Return only the length
        return w.writeInteger(@intCast(lcs_result.length));
    } else if (with_idx) {
        // IDX mode: return match positions
        var matches = try findLcsMatches(allocator, str1, str2, min_match_len);
        defer {
            for (matches.items) |match| {
                allocator.free(match.key1_range);
                allocator.free(match.key2_range);
            }
            matches.deinit(allocator);
        }

        // Format: map with "matches" key containing array of match arrays, and "len" key
        // Redis format:
        // 1) "matches"
        // 2) 1) 1) 1) 4  (key1 start)
        //          2) 7  (key1 end)
        //       2) 1) 5  (key2 start)
        //          2) 8  (key2 end)
        //       3) 4     (match len - only if WITHMATCHLEN)
        // 3) "len"
        // 4) 11 (total LCS length)

        var result = std.ArrayList(u8){};
        errdefer result.deinit(allocator);

        // Start with a map containing "matches" and "len"
        try result.appendSlice(allocator, "*2\r\n"); // 2 elements in outer array

        // First element: "matches" key
        try result.appendSlice(allocator, "$7\r\nmatches\r\n");

        // Second element: array of matches
        const match_count_str = try std.fmt.allocPrint(allocator, "*{d}\r\n", .{matches.items.len});
        defer allocator.free(match_count_str);
        try result.appendSlice(allocator, match_count_str);

        // Each match is an array of 2 or 3 elements (key1_range, key2_range, optional match_len)
        const elements: usize = if (with_match_len) 3 else 2;
        for (matches.items) |match| {
            const elem_str = try std.fmt.allocPrint(allocator, "*{d}\r\n", .{elements});
            defer allocator.free(elem_str);
            try result.appendSlice(allocator, elem_str);

            // key1_range: [start, end]
            try result.appendSlice(allocator, "*2\r\n");
            const start1_str = try std.fmt.allocPrint(allocator, ":{d}\r\n", .{match.key1_range[0]});
            defer allocator.free(start1_str);
            try result.appendSlice(allocator, start1_str);
            const end1_str = try std.fmt.allocPrint(allocator, ":{d}\r\n", .{match.key1_range[1]});
            defer allocator.free(end1_str);
            try result.appendSlice(allocator, end1_str);

            // key2_range: [start, end]
            try result.appendSlice(allocator, "*2\r\n");
            const start2_str = try std.fmt.allocPrint(allocator, ":{d}\r\n", .{match.key2_range[0]});
            defer allocator.free(start2_str);
            try result.appendSlice(allocator, start2_str);
            const end2_str = try std.fmt.allocPrint(allocator, ":{d}\r\n", .{match.key2_range[1]});
            defer allocator.free(end2_str);
            try result.appendSlice(allocator, end2_str);

            // Optional match_len
            if (with_match_len) {
                const len_str = try std.fmt.allocPrint(allocator, ":{d}\r\n", .{match.match_len});
                defer allocator.free(len_str);
                try result.appendSlice(allocator, len_str);
            }
        }

        // "len" key
        try result.appendSlice(allocator, "$3\r\nlen\r\n");

        // Total LCS length value
        const len_str = try std.fmt.allocPrint(allocator, ":{d}\r\n", .{lcs_result.length});
        defer allocator.free(len_str);
        try result.appendSlice(allocator, len_str);

        return try result.toOwnedSlice(allocator);
    } else {
        // Return the LCS string
        return w.writeBulkString(lcs_result.string);
    }
}

const LcsResult = struct {
    string: []const u8,
    length: usize,

    fn deinit(self: *const LcsResult, allocator: std.mem.Allocator) void {
        allocator.free(self.string);
    }
};

fn computeLcs(allocator: std.mem.Allocator, str1: []const u8, str2: []const u8) !LcsResult {
    const m = str1.len;
    const n = str2.len;

    if (m == 0 or n == 0) {
        return LcsResult{
            .string = try allocator.dupe(u8, ""),
            .length = 0,
        };
    }

    // Create DP table
    const table = try allocator.alloc([]usize, m + 1);
    defer {
        for (table) |row| {
            allocator.free(row);
        }
        allocator.free(table);
    }

    for (table) |*row| {
        row.* = try allocator.alloc(usize, n + 1);
        @memset(row.*, 0);
    }

    // Fill DP table
    for (1..m + 1) |i| {
        for (1..n + 1) |j| {
            if (str1[i - 1] == str2[j - 1]) {
                table[i][j] = table[i - 1][j - 1] + 1;
            } else {
                table[i][j] = @max(table[i - 1][j], table[i][j - 1]);
            }
        }
    }

    // Backtrack to build the LCS string
    var lcs = std.ArrayList(u8){};
    errdefer lcs.deinit(allocator);

    var i = m;
    var j = n;
    while (i > 0 and j > 0) {
        if (str1[i - 1] == str2[j - 1]) {
            try lcs.append(allocator, str1[i - 1]);
            i -= 1;
            j -= 1;
        } else if (table[i - 1][j] > table[i][j - 1]) {
            i -= 1;
        } else {
            j -= 1;
        }
    }

    // Reverse the LCS string
    const lcs_str = try lcs.toOwnedSlice(allocator);
    std.mem.reverse(u8, lcs_str);

    return LcsResult{
        .string = lcs_str,
        .length = lcs_str.len,
    };
}

const LcsMatch = struct {
    key1_range: []const usize,
    key2_range: []const usize,
    match_len: usize,
};

fn findLcsMatches(allocator: std.mem.Allocator, str1: []const u8, str2: []const u8, min_match_len: usize) !std.ArrayList(LcsMatch) {
    var matches = std.ArrayList(LcsMatch){};
    errdefer {
        for (matches.items) |match| {
            allocator.free(match.key1_range);
            allocator.free(match.key2_range);
        }
        matches.deinit(allocator);
    }

    const m = str1.len;
    const n = str2.len;

    if (m == 0 or n == 0) {
        return matches;
    }

    // Create DP table for LCS
    const table = try allocator.alloc([]usize, m + 1);
    defer {
        for (table) |row| {
            allocator.free(row);
        }
        allocator.free(table);
    }

    for (table) |*row| {
        row.* = try allocator.alloc(usize, n + 1);
        @memset(row.*, 0);
    }

    // Fill DP table
    for (1..m + 1) |i| {
        for (1..n + 1) |j| {
            if (str1[i - 1] == str2[j - 1]) {
                table[i][j] = table[i - 1][j - 1] + 1;
            } else {
                table[i][j] = @max(table[i - 1][j], table[i][j - 1]);
            }
        }
    }

    // Backtrack to find matching ranges
    var i = m;
    var j = n;
    var match_start_i: ?usize = null;
    var match_start_j: ?usize = null;
    var match_len: usize = 0;

    while (i > 0 and j > 0) {
        if (str1[i - 1] == str2[j - 1]) {
            if (match_start_i == null) {
                match_start_i = i - 1;
                match_start_j = j - 1;
                match_len = 0;
            }
            match_len += 1;
            i -= 1;
            j -= 1;
        } else {
            // End of a match
            if (match_start_i != null and match_len >= min_match_len) {
                const range1 = try allocator.alloc(usize, 2);
                range1[0] = match_start_i.?;
                range1[1] = match_start_i.? + match_len - 1;

                const range2 = try allocator.alloc(usize, 2);
                range2[0] = match_start_j.?;
                range2[1] = match_start_j.? + match_len - 1;

                try matches.append(allocator, LcsMatch{
                    .key1_range = range1,
                    .key2_range = range2,
                    .match_len = match_len,
                });
            }
            match_start_i = null;
            match_start_j = null;
            match_len = 0;

            if (table[i - 1][j] > table[i][j - 1]) {
                i -= 1;
            } else {
                j -= 1;
            }
        }
    }

    // Handle the last match if exists
    if (match_start_i != null and match_len >= min_match_len) {
        const range1 = try allocator.alloc(usize, 2);
        range1[0] = match_start_i.?;
        range1[1] = match_start_i.? + match_len - 1;

        const range2 = try allocator.alloc(usize, 2);
        range2[0] = match_start_j.?;
        range2[1] = match_start_j.? + match_len - 1;

        try matches.append(allocator, LcsMatch{
            .key1_range = range1,
            .key2_range = range2,
            .match_len = match_len,
        });
    }

    // Reverse matches (we built them backward)
    std.mem.reverse(LcsMatch, matches.items);

    return matches;
}

/// DIGEST key
/// Get the hash digest for the value stored in the specified key.
/// Returns a hexadecimal string representation of the XXH3 hash.
/// Redis 8.4+ — uses XXH3 hash algorithm for efficient comparison.
/// Time complexity: O(N) where N is the length of the string value.
pub fn cmdDigest(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 2) {
        return w.writeError("ERR wrong number of arguments for 'digest' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Get the value (borrowed, no need to free)
    const value_result = storage.get(key);

    if (value_result == null) {
        return w.writeNull();
    }

    // Compute XXH3-64 hash digest as per Redis 8.4 specification
    const hash_value = std.hash.XxHash3.hash(0, value_result.?);

    // Convert to hex string (16 lowercase hex characters for 64-bit hash)
    const hex_str = try std.fmt.allocPrint(allocator, "{x:0>16}", .{hash_value});
    defer allocator.free(hex_str);

    return w.writeBulkString(hex_str);
}

/// DELEX key [IFEQ value | IFNE value | IFDEQ digest | IFDNE digest]
/// Conditionally delete a key based on value or digest comparison.
/// Redis 8.4+ — atomic compare-and-delete for optimistic concurrency control.
/// Time complexity: O(1) for IFEQ/IFNE, O(N) for IFDEQ/IFDNE where N is value length.
pub fn cmdDelex(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'delex' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse conditional flags
    var condition: ?enum { ifeq, ifne, ifdeq, ifdne } = null;
    var condition_value: ?[]const u8 = null;

    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const flag = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid argument"),
        };

        const flag_upper = try std.ascii.allocUpperString(allocator, flag);
        defer allocator.free(flag_upper);

        if (std.mem.eql(u8, flag_upper, "IFEQ")) {
            if (condition != null) {
                return w.writeError("ERR multiple conditions specified");
            }
            if (i + 1 >= args.len) {
                return w.writeError("ERR IFEQ requires a value");
            }
            condition = .ifeq;
            condition_value = switch (args[i + 1]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid value for IFEQ"),
            };
            i += 1;
        } else if (std.mem.eql(u8, flag_upper, "IFNE")) {
            if (condition != null) {
                return w.writeError("ERR multiple conditions specified");
            }
            if (i + 1 >= args.len) {
                return w.writeError("ERR IFNE requires a value");
            }
            condition = .ifne;
            condition_value = switch (args[i + 1]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid value for IFNE"),
            };
            i += 1;
        } else if (std.mem.eql(u8, flag_upper, "IFDEQ")) {
            if (condition != null) {
                return w.writeError("ERR multiple conditions specified");
            }
            if (i + 1 >= args.len) {
                return w.writeError("ERR IFDEQ requires a digest");
            }
            condition = .ifdeq;
            condition_value = switch (args[i + 1]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid digest for IFDEQ"),
            };
            i += 1;
        } else if (std.mem.eql(u8, flag_upper, "IFDNE")) {
            if (condition != null) {
                return w.writeError("ERR multiple conditions specified");
            }
            if (i + 1 >= args.len) {
                return w.writeError("ERR IFDNE requires a digest");
            }
            condition = .ifdne;
            condition_value = switch (args[i + 1]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid digest for IFDNE"),
            };
            i += 1;
        } else {
            return w.writeError("ERR unknown option");
        }
    }

    // No condition means unconditional delete (same as DEL)
    if (condition == null) {
        const deleted = storage.del(&[_][]const u8{key});
        return w.writeInteger(@intCast(deleted));
    }

    // Get current value (borrowed, no need to free)
    const current_value = storage.get(key);

    if (current_value == null) {
        // Key doesn't exist - no deletion
        return w.writeInteger(0);
    }

    // Check condition
    const should_delete = switch (condition.?) {
        .ifeq => std.mem.eql(u8, current_value.?, condition_value.?),
        .ifne => !std.mem.eql(u8, current_value.?, condition_value.?),
        .ifdeq => blk: {
            // Compute digest of current value
            const current_hash = std.hash.Wyhash.hash(0, current_value.?);
            const current_hex = try std.fmt.allocPrint(allocator, "{x:0>16}", .{current_hash});
            defer allocator.free(current_hex);
            break :blk std.mem.eql(u8, current_hex, condition_value.?);
        },
        .ifdne => blk: {
            // Compute digest of current value
            const current_hash = std.hash.Wyhash.hash(0, current_value.?);
            const current_hex = try std.fmt.allocPrint(allocator, "{x:0>16}", .{current_hash});
            defer allocator.free(current_hex);
            break :blk !std.mem.eql(u8, current_hex, condition_value.?);
        },
    };

    if (should_delete) {
        const deleted = storage.del(&[_][]const u8{key});
        return w.writeInteger(@intCast(deleted));
    } else {
        return w.writeInteger(0);
    }
}

// Embedded unit tests

test "commands - PING no argument" {
    const allocator = std.testing.allocator;

    const args = [_]RespValue{
        RespValue{ .bulk_string = "PING" },
    };

    const result = try cmdPing(allocator, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+PONG\r\n", result);
}

test "commands - PING with message" {
    const allocator = std.testing.allocator;

    const args = [_]RespValue{
        RespValue{ .bulk_string = "PING" },
        RespValue{ .bulk_string = "hello" },
    };

    const result = try cmdPing(allocator, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$5\r\nhello\r\n", result);
}

test "commands - PING too many arguments" {
    const allocator = std.testing.allocator;

    const args = [_]RespValue{
        RespValue{ .bulk_string = "PING" },
        RespValue{ .bulk_string = "arg1" },
        RespValue{ .bulk_string = "arg2" },
    };

    const result = try cmdPing(allocator, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("-ERR wrong number of arguments for 'ping' command\r\n", result);
}

test "commands - SET basic" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
    };

    const result = try cmdSet(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqualStrings("value1", storage.get("key1").?);
}

test "commands - SET with EX option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "EX" },
        RespValue{ .bulk_string = "60" },
    };

    const result = try cmdSet(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqualStrings("value1", storage.get("key1").?);
}

test "commands - SET with PX option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "PX" },
        RespValue{ .bulk_string = "5000" },
    };

    var ps = PubSub.init(allocator);


    defer ps.deinit();



    var registry = ClientRegistry.init(allocator);


    defer registry.deinit();



    const result = try cmdSet(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqualStrings("value1", storage.get("key1").?);
}

test "commands - SET with NX when key doesn't exist" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "NX" },
    };

    var ps = PubSub.init(allocator);


    defer ps.deinit();



    var registry = ClientRegistry.init(allocator);


    defer registry.deinit();



    const result = try cmdSet(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "commands - SET with NX when key exists" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "existing", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "NX" },
    };

    var ps = PubSub.init(allocator);


    defer ps.deinit();



    var registry = ClientRegistry.init(allocator);


    defer registry.deinit();



    const result = try cmdSet(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
    try std.testing.expectEqualStrings("existing", storage.get("key1").?);
}

test "commands - SET with XX when key exists" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "existing", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "new_value" },
        RespValue{ .bulk_string = "XX" },
    };

    var ps = PubSub.init(allocator);


    defer ps.deinit();



    var registry = ClientRegistry.init(allocator);


    defer registry.deinit();



    const result = try cmdSet(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqualStrings("new_value", storage.get("key1").?);
}

test "commands - SET with XX when key doesn't exist" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "XX" },
    };

    var ps = PubSub.init(allocator);


    defer ps.deinit();



    var registry = ClientRegistry.init(allocator);


    defer registry.deinit();



    const result = try cmdSet(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
    try std.testing.expect(storage.get("key1") == null);
}

test "commands - SET with both NX and XX returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "NX" },
        RespValue{ .bulk_string = "XX" },
    };

    var ps = PubSub.init(allocator);


    defer ps.deinit();



    var registry = ClientRegistry.init(allocator);


    defer registry.deinit();



    const result = try cmdSet(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("-ERR syntax error\r\n", result);
}

test "commands - SET with negative expiration" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "EX" },
        RespValue{ .bulk_string = "-1" },
    };

    var ps = PubSub.init(allocator);


    defer ps.deinit();



    var registry = ClientRegistry.init(allocator);


    defer registry.deinit();



    const result = try cmdSet(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("-ERR invalid expire time in 'set' command\r\n", result);
}

test "commands - SET with GET returns nil when key missing" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "newkey" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "GET" },
    };
    const result = try cmdSet(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);
    // SET succeeds but old value was nil
    try std.testing.expectEqualStrings("$-1\r\n", result);
    // Key was actually set
    try std.testing.expectEqualStrings("value1", storage.get("newkey").?);
}

test "commands - SET with GET returns old value" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    try storage.set("mykey", "old", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "new" },
        RespValue{ .bulk_string = "GET" },
    };
    const result = try cmdSet(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$3\r\nold\r\n", result);
    try std.testing.expectEqualStrings("new", storage.get("mykey").?);
}

test "commands - SET with NX GET returns old value without setting" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    try storage.set("mykey", "existing", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "new" },
        RespValue{ .bulk_string = "NX" },
        RespValue{ .bulk_string = "GET" },
    };
    const result = try cmdSet(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);
    // Returns old value, but SET is blocked by NX
    try std.testing.expectEqualStrings("$8\r\nexisting\r\n", result);
    // Key unchanged
    try std.testing.expectEqualStrings("existing", storage.get("mykey").?);
}

test "commands - SET with KEEPTTL preserves expiry" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const future = Storage.getCurrentTimestamp() + 5000;
    try storage.set("mykey", "old", future);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "new" },
        RespValue{ .bulk_string = "KEEPTTL" },
    };
    const result = try cmdSet(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("+OK\r\n", result);
    // Value updated
    try std.testing.expectEqualStrings("new", storage.get("mykey").?);
    // Expiry preserved
    const entry = try storage.getStringWithExpiry("mykey");
    try std.testing.expect(entry != null);
    try std.testing.expectEqual(future, entry.?.expires_at.?);
}

test "commands - SET with EXAT sets absolute expiry" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const future_sec: i64 = @divTrunc(Storage.getCurrentTimestamp(), 1000) + 3600;
    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "val" },
        RespValue{ .bulk_string = "EXAT" },
        RespValue{ .bulk_string = try std.fmt.allocPrint(allocator, "{d}", .{future_sec}) },
    };
    defer allocator.free(args[4].bulk_string);
    const result = try cmdSet(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("+OK\r\n", result);
    const entry = try storage.getStringWithExpiry("mykey");
    try std.testing.expect(entry != null);
    try std.testing.expectEqual(future_sec * 1000, entry.?.expires_at.?);
}

test "commands - SET with PXAT sets absolute ms expiry" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const future_ms: i64 = Storage.getCurrentTimestamp() + 3_600_000;
    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "val" },
        RespValue{ .bulk_string = "PXAT" },
        RespValue{ .bulk_string = try std.fmt.allocPrint(allocator, "{d}", .{future_ms}) },
    };
    defer allocator.free(args[4].bulk_string);
    const result = try cmdSet(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("+OK\r\n", result);
    const entry = try storage.getStringWithExpiry("mykey");
    try std.testing.expect(entry != null);
    try std.testing.expectEqual(future_ms, entry.?.expires_at.?);
}

test "commands - SET KEEPTTL with EX returns syntax error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "val" },
        RespValue{ .bulk_string = "EX" },
        RespValue{ .bulk_string = "100" },
        RespValue{ .bulk_string = "KEEPTTL" },
    };
    const result = try cmdSet(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("-ERR syntax error\r\n", result);
}

test "commands - GET existing key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "hello", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "key1" },
    };

    var registry = ClientRegistry.init(allocator);


    defer registry.deinit();



    const result = try cmdGet(allocator, storage, &args, &registry, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$5\r\nhello\r\n", result);
}

test "commands - GET non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    var registry = ClientRegistry.init(allocator);


    defer registry.deinit();



    const result = try cmdGet(allocator, storage, &args, &registry, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "commands - GET wrong number of arguments" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GET" },
    };

    var registry = ClientRegistry.init(allocator);


    defer registry.deinit();



    const result = try cmdGet(allocator, storage, &args, &registry, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("-ERR wrong number of arguments for 'get' command\r\n", result);
}

test "commands - DEL single key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DEL" },
        RespValue{ .bulk_string = "key1" },
    };

    var ps = PubSub.init(allocator);


    defer ps.deinit();



    var registry = ClientRegistry.init(allocator);


    defer registry.deinit();



    const result = try cmdDel(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
    try std.testing.expect(storage.get("key1") == null);
}

test "commands - DEL multiple keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);
    try storage.set("key2", "value2", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DEL" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key2" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    var ps = PubSub.init(allocator);


    defer ps.deinit();



    var registry = ClientRegistry.init(allocator);


    defer registry.deinit();



    const result = try cmdDel(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":2\r\n", result);
}

test "commands - DEL non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DEL" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    var ps = PubSub.init(allocator);


    defer ps.deinit();



    var registry = ClientRegistry.init(allocator);


    defer registry.deinit();



    const result = try cmdDel(allocator, storage, &args, &ps, 0, &registry, 0);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "commands - EXISTS single existing key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "EXISTS" },
        RespValue{ .bulk_string = "key1" },
    };

    const result = try cmdExists(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
}

test "commands - EXISTS non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "EXISTS" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    const result = try cmdExists(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "commands - EXISTS multiple keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);
    try storage.set("key2", "value2", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "EXISTS" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key2" },
        RespValue{ .bulk_string = "nosuchkey" },
    };

    const result = try cmdExists(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":2\r\n", result);
}

test "commands - EXISTS with duplicate keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "EXISTS" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key1" },
    };

    const result = try cmdExists(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":3\r\n", result);
}

test "commands - executeCommand dispatches PING" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init(allocator);
    defer tx.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "PING" },
    };
    const cmd = RespValue{ .array = &args };

    const result = try executeCommand(allocator, storage, cmd, null, &ps, 0, &tx, null, 6379, null, null);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+PONG\r\n", result);
}

test "commands - executeCommand case insensitive" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init(allocator);
    defer tx.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "ping" },
    };
    const cmd = RespValue{ .array = &args };

    const result = try executeCommand(allocator, storage, cmd, null, &ps, 0, &tx, null, 6379, null, null);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+PONG\r\n", result);
}

test "commands - executeCommand unknown command" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();
    var ps = PubSub.init(allocator);
    defer ps.deinit();
    var tx = TxState.init(allocator);
    defer tx.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "UNKNOWN" },
    };
    const cmd = RespValue{ .array = &args };

    const result = try executeCommand(allocator, storage, cmd, null, &ps, 0, &tx, null, 6379, null, null);
    defer allocator.free(result);

    const expected = "-ERR unknown command 'UNKNOWN'\r\n";
    try std.testing.expectEqualStrings(expected, result);
}

// ── String range commands ─────────────────────────────────────────────────────

/// GETRANGE key start end (also aliased as SUBSTR)
/// Returns substring of string value. Negative indices supported.
/// Returns empty string if key doesn't exist.
pub fn cmdGetrange(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'getrange' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };
    const start_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not an integer or out of range"),
    };
    const end_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not an integer or out of range"),
    };

    const start = std.fmt.parseInt(i64, start_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };
    const end = std.fmt.parseInt(i64, end_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };

    const result = storage.getrange(allocator, key, start, end) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };
    defer allocator.free(result);

    return w.writeBulkString(result);
}

/// SUBSTR key start end
///
/// DEPRECATED: This command is deprecated since Redis 2.0.0.
/// Use GETRANGE instead. SUBSTR is provided for backwards compatibility only.
///
/// Returns the substring of the string value stored at key.
/// Identical behavior to GETRANGE - negative indices supported.
///
/// Returns:
///   - Bulk string: Substring at specified range
///   - Empty string: If key doesn't exist or range is out of bounds
///   - Error: WRONGTYPE if key is not a string
pub fn cmdSubstr(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    // SUBSTR is a direct alias to GETRANGE with identical arguments
    return cmdGetrange(allocator, storage, args);
}

/// SETRANGE key offset value
/// Overwrite bytes at offset in string. Zero-pads if necessary.
/// Returns new total length.
pub fn cmdSetrange(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'setrange' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };
    const offset_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR value is not an integer or out of range"),
    };
    const value = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid value"),
    };

    const offset = std.fmt.parseInt(i64, offset_str, 10) catch {
        return w.writeError("ERR value is not an integer or out of range");
    };
    if (offset < 0) {
        return w.writeError("ERR offset is out of range");
    }

    // Redis enforces a 512MB maximum string size
    const MAX_STRING_BYTES: usize = 512 * 1024 * 1024;
    const required_len = @as(usize, @intCast(offset)) + value.len;
    if (required_len > MAX_STRING_BYTES) {
        return w.writeError("ERR string exceeds maximum allowed size (512mb)");
    }

    const new_len = storage.setrange(key, @as(usize, @intCast(offset)), value) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    // Publish keyspace notification
    notifyKeyspaceEvent(allocator, storage, ps, db_index, key, .string, "setrange");

    return w.writeInteger(@intCast(new_len));
}

// ── Bit operations ────────────────────────────────────────────────────────

/// Helper to publish bitmap/bitfield notifications
fn notifyBitmapEvent(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub_state: *PubSub,
    db_index: u32,
    key: []const u8,
    event_flag: notifications_mod.NotificationFlag,
    event_name: []const u8,
) void {
    const config_value = storage.config.getAsString("notify-keyspace-events") catch return;
    const config_str = config_value orelse return;
    const flags = notifications_mod.parseNotificationFlags(config_str);

    if (!notifications_mod.shouldNotify(flags, event_flag)) return;

    notifications_mod.publishNotification(allocator, pubsub_state, db_index, key, event_name, flags) catch {};
}

pub fn cmdSetbit(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'setbit' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };
    const offset_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR bit offset is not an integer or out of range"),
    };
    const value_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR bit is not an integer or out of range"),
    };

    const offset = std.fmt.parseInt(usize, offset_str, 10) catch {
        return w.writeError("ERR bit offset is not an integer or out of range");
    };

    const value_int = std.fmt.parseInt(u8, value_str, 10) catch {
        return w.writeError("ERR bit is not an integer or out of range");
    };

    if (value_int > 1) {
        return w.writeError("ERR bit is not an integer or out of range");
    }

    const value: u1 = @intCast(value_int);

    const original_bit = storage.setbit(key, offset, value) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    // Fire notification only if bit changed
    if (original_bit != value) {
        notifyBitmapEvent(allocator, storage, ps, db_index, key, .string, "setbit");
    }

    return w.writeInteger(original_bit);
}

pub fn cmdGetbit(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len != 3) {
        return w.writeError("ERR wrong number of arguments for 'getbit' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };
    const offset_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR bit offset is not an integer or out of range"),
    };

    const offset = std.fmt.parseInt(usize, offset_str, 10) catch {
        return w.writeError("ERR bit offset is not an integer or out of range");
    };

    const bit = storage.getbit(key, offset) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(bit);
}

pub fn cmdBitcount(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Valid forms: BITCOUNT key / BITCOUNT key start end / BITCOUNT key start end BYTE|BIT
    if (args.len != 2 and args.len != 4 and args.len != 5) {
        return w.writeError("ERR wrong number of arguments for 'bitcount' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const start: ?i64 = if (args.len >= 4) blk: {
        const start_str = switch (args[2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR value is not an integer or out of range"),
        };
        break :blk std.fmt.parseInt(i64, start_str, 10) catch {
            return w.writeError("ERR value is not an integer or out of range");
        };
    } else null;

    const end: ?i64 = if (args.len >= 4) blk: {
        const end_str = switch (args[3]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR value is not an integer or out of range"),
        };
        break :blk std.fmt.parseInt(i64, end_str, 10) catch {
            return w.writeError("ERR value is not an integer or out of range");
        };
    } else null;

    // Parse optional BYTE|BIT unit (Redis 7.0+)
    const unit: storage_mod.RangeUnit = if (args.len == 5) blk: {
        const unit_str = switch (args[4]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        if (std.ascii.eqlIgnoreCase(unit_str, "BYTE")) {
            break :blk .byte;
        } else if (std.ascii.eqlIgnoreCase(unit_str, "BIT")) {
            break :blk .bit;
        } else {
            return w.writeError("ERR syntax error");
        }
    } else .byte;

    const count = storage.bitcount(key, start, end, unit) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(count);
}

pub fn cmdBitop(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue, ps: *PubSub, db_index: u32) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'bitop' command");
    }

    const op_str = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };

    const operation: Storage.BitOp = if (std.ascii.eqlIgnoreCase(op_str, "AND"))
        .AND
    else if (std.ascii.eqlIgnoreCase(op_str, "OR"))
        .OR
    else if (std.ascii.eqlIgnoreCase(op_str, "XOR"))
        .XOR
    else if (std.ascii.eqlIgnoreCase(op_str, "NOT"))
        .NOT
    else {
        return w.writeError("ERR syntax error");
    };

    const destkey = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Extract source keys
    var srckeys = std.ArrayList([]const u8){};
    defer srckeys.deinit(allocator);

    for (args[3..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try srckeys.append(allocator, key);
    }

    if (operation == .NOT and srckeys.items.len != 1) {
        return w.writeError("ERR BITOP NOT must be called with a single source key");
    }

    const result_len = storage.bitop(operation, destkey, srckeys.items) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    // Fire appropriate notification based on result
    if (result_len > 0) {
        // Result has content, fire "set" event
        notifyBitmapEvent(allocator, storage, ps, db_index, destkey, .string, "set");
    } else {
        // Result is empty, check if destkey existed and fire "del" if needed
        const destkey_existed = storage.get(destkey) != null;
        if (destkey_existed) {
            notifyBitmapEvent(allocator, storage, ps, db_index, destkey, .generic, "del");
        }
    }

    return w.writeInteger(@intCast(result_len));
}

pub fn cmdBitpos(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3 or args.len > 6) {
        return w.writeError("ERR wrong number of arguments for 'bitpos' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const bit_int = switch (args[2]) {
        .bulk_string => |s| std.fmt.parseInt(u8, s, 10) catch {
            return w.writeError("ERR bit must be 0 or 1");
        },
        else => return w.writeError("ERR bit must be 0 or 1"),
    };

    if (bit_int > 1) {
        return w.writeError("ERR bit must be 0 or 1");
    }

    const bit: u1 = @intCast(bit_int);

    const start: ?i64 = if (args.len >= 4) blk: {
        break :blk switch (args[3]) {
            .bulk_string => |s| std.fmt.parseInt(i64, s, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            },
            else => return w.writeError("ERR value is not an integer or out of range"),
        };
    } else null;

    const end: ?i64 = if (args.len >= 5) blk: {
        break :blk switch (args[4]) {
            .bulk_string => |s| std.fmt.parseInt(i64, s, 10) catch {
                return w.writeError("ERR value is not an integer or out of range");
            },
            else => return w.writeError("ERR value is not an integer or out of range"),
        };
    } else null;

    const unit: storage_mod.RangeUnit = if (args.len == 6) blk: {
        const unit_str = switch (args[5]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        if (std.ascii.eqlIgnoreCase(unit_str, "BYTE")) {
            break :blk .byte;
        } else if (std.ascii.eqlIgnoreCase(unit_str, "BIT")) {
            break :blk .bit;
        } else {
            return w.writeError("ERR syntax error");
        }
    } else .byte;

    const position = storage.bitpos(key, bit, start, end, unit) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(position);
}

// ── New command unit tests ─────────────────────────────────────────────────

test "commands - INCR basic" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "INCR" },
        RespValue{ .bulk_string = "counter" },
    };
    const r1 = try cmdIncr(allocator, storage, &args);
    defer allocator.free(r1);
    try std.testing.expectEqualStrings(":1\r\n", r1);

    const r2 = try cmdIncr(allocator, storage, &args);
    defer allocator.free(r2);
    try std.testing.expectEqualStrings(":2\r\n", r2);
}

test "commands - INCR on non-integer returns error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key", "notanint", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "INCR" },
        RespValue{ .bulk_string = "key" },
    };
    const result = try cmdIncr(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expect(result[0] == '-');
}

test "commands - DECR basic" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("counter", "10", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DECR" },
        RespValue{ .bulk_string = "counter" },
    };
    const result = try cmdDecr(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":9\r\n", result);
}

test "commands - INCRBY basic" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "INCRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "5" },
    };
    const result = try cmdIncrby(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":5\r\n", result);
}

test "commands - DECRBY basic" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("counter", "20", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DECRBY" },
        RespValue{ .bulk_string = "counter" },
        RespValue{ .bulk_string = "7" },
    };
    const result = try cmdDecrby(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":13\r\n", result);
}

test "commands - INCRBYFLOAT basic" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key", "10.5", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "INCRBYFLOAT" },
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "0.1" },
    };
    const result = try cmdIncrbyfloat(allocator, storage, &args, .RESP2);
    defer allocator.free(result);
    // Result should be a bulk string starting with $
    try std.testing.expect(result[0] == '$');
}

test "commands - INCRBYFLOAT non-float error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "INCRBYFLOAT" },
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "notafloat" },
    };
    const result = try cmdIncrbyfloat(allocator, storage, &args, .RESP2);
    defer allocator.free(result);
    try std.testing.expect(result[0] == '-');
}

test "commands - APPEND creates key and returns length" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "APPEND" },
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "Hello" },
    };
    const r1 = try cmdAppend(allocator, storage, &args);
    defer allocator.free(r1);
    try std.testing.expectEqualStrings(":5\r\n", r1);

    const args2 = [_]RespValue{
        RespValue{ .bulk_string = "APPEND" },
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = " World" },
    };
    const r2 = try cmdAppend(allocator, storage, &args2);
    defer allocator.free(r2);
    try std.testing.expectEqualStrings(":11\r\n", r2);

    try std.testing.expectEqualStrings("Hello World", storage.get("key").?);
}

test "commands - STRLEN returns correct length" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key", "Hello", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "STRLEN" },
        RespValue{ .bulk_string = "key" },
    };
    const result = try cmdStrlen(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":5\r\n", result);
}

test "commands - STRLEN on missing key returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "STRLEN" },
        RespValue{ .bulk_string = "missing" },
    };
    const result = try cmdStrlen(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "commands - GETSET returns old value" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    try storage.set("key", "old", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GETSET" },
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "new" },
    };
    const result = try cmdGetset(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$3\r\nold\r\n", result);

    try std.testing.expectEqualStrings("new", storage.get("key").?);
}

test "commands - GETDEL returns value and removes key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    try storage.set("key", "value", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GETDEL" },
        RespValue{ .bulk_string = "key" },
    };
    const result = try cmdGetdel(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$5\r\nvalue\r\n", result);

    try std.testing.expect(storage.get("key") == null);
}

test "commands - GETDEL on missing key returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GETDEL" },
        RespValue{ .bulk_string = "missing" },
    };
    const result = try cmdGetdel(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "commands - SETNX sets only when key missing" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    var ps = PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SETNX" },
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "value" },
    };
    const r1 = try cmdSetnx(allocator, storage, &args, &ps, 0);
    defer allocator.free(r1);
    try std.testing.expectEqualStrings(":1\r\n", r1);

    const r2 = try cmdSetnx(allocator, storage, &args, &ps, 0);
    defer allocator.free(r2);
    try std.testing.expectEqualStrings(":0\r\n", r2);
}

test "commands - SETEX sets key with expiry" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SETEX" },
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "100" },
        RespValue{ .bulk_string = "value" },
    };
    const result = try cmdSetex(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("+OK\r\n", result);

    // TTL should be positive
    const ttl = storage.getTtlMs("key");
    try std.testing.expect(ttl > 0);
}

test "commands - SETEX rejects non-positive expiry" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SETEX" },
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "value" },
    };
    const result = try cmdSetex(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expect(result[0] == '-');
}

test "commands - PSETEX sets key with ms expiry" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "PSETEX" },
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "100000" },
        RespValue{ .bulk_string = "value" },
    };
    const result = try cmdPsetex(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "commands - MGET multiple keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("k1", "v1", null);
    try storage.set("k2", "v2", null);

    var registry = ClientRegistry.init(allocator);
    defer registry.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "MGET" },
        RespValue{ .bulk_string = "k1" },
        RespValue{ .bulk_string = "k2" },
        RespValue{ .bulk_string = "missing" },
    };
    const result = try cmdMget(allocator, storage, &args, &registry, 0);
    defer allocator.free(result);

    // Should be *3\r\n$2\r\nv1\r\n$2\r\nv2\r\n$-1\r\n
    try std.testing.expect(std.mem.startsWith(u8, result, "*3\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, result, "$-1\r\n") != null);
}

test "commands - MSET sets all keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "MSET" },
        RespValue{ .bulk_string = "k1" },
        RespValue{ .bulk_string = "v1" },
        RespValue{ .bulk_string = "k2" },
        RespValue{ .bulk_string = "v2" },
    };
    const result = try cmdMset(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("+OK\r\n", result);

    try std.testing.expectEqualStrings("v1", storage.get("k1").?);
    try std.testing.expectEqualStrings("v2", storage.get("k2").?);
}

test "commands - MSETNX sets all or none" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args1 = [_]RespValue{
        RespValue{ .bulk_string = "MSETNX" },
        RespValue{ .bulk_string = "k1" },
        RespValue{ .bulk_string = "v1" },
        RespValue{ .bulk_string = "k2" },
        RespValue{ .bulk_string = "v2" },
    };
    const r1 = try cmdMsetnx(allocator, storage, &args1);
    defer allocator.free(r1);
    try std.testing.expectEqualStrings(":1\r\n", r1);

    // k1 already exists — should fail
    const args2 = [_]RespValue{
        RespValue{ .bulk_string = "MSETNX" },
        RespValue{ .bulk_string = "k1" },
        RespValue{ .bulk_string = "new1" },
        RespValue{ .bulk_string = "k3" },
        RespValue{ .bulk_string = "v3" },
    };
    const r2 = try cmdMsetnx(allocator, storage, &args2);
    defer allocator.free(r2);
    try std.testing.expectEqualStrings(":0\r\n", r2);

    // k1 should still have old value; k3 should not exist
    try std.testing.expectEqualStrings("v1", storage.get("k1").?);
    try std.testing.expect(storage.get("k3") == null);
}

test "commands - MSETEX basic operation" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "MSETEX" },
        RespValue{ .bulk_string = "3" }, // numkeys
        RespValue{ .bulk_string = "k1" },
        RespValue{ .bulk_string = "v1" },
        RespValue{ .bulk_string = "k2" },
        RespValue{ .bulk_string = "v2" },
        RespValue{ .bulk_string = "k3" },
        RespValue{ .bulk_string = "v3" },
    };
    const result = try cmdMsetex(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":1\r\n", result);

    // Verify all keys were set
    try std.testing.expectEqualStrings("v1", storage.get("k1").?);
    try std.testing.expectEqualStrings("v2", storage.get("k2").?);
    try std.testing.expectEqualStrings("v3", storage.get("k3").?);
}

test "commands - MSETEX with EX option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "MSETEX" },
        RespValue{ .bulk_string = "2" }, // numkeys
        RespValue{ .bulk_string = "k1" },
        RespValue{ .bulk_string = "v1" },
        RespValue{ .bulk_string = "k2" },
        RespValue{ .bulk_string = "v2" },
        RespValue{ .bulk_string = "EX" },
        RespValue{ .bulk_string = "100" },
    };
    const result = try cmdMsetex(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":1\r\n", result);

    // Keys should exist
    try std.testing.expect(storage.get("k1") != null);
    try std.testing.expect(storage.get("k2") != null);
}

test "commands - MSETEX with NX flag succeeds" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "MSETEX" },
        RespValue{ .bulk_string = "2" }, // numkeys
        RespValue{ .bulk_string = "k1" },
        RespValue{ .bulk_string = "v1" },
        RespValue{ .bulk_string = "k2" },
        RespValue{ .bulk_string = "v2" },
        RespValue{ .bulk_string = "NX" },
    };
    const result = try cmdMsetex(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":1\r\n", result);

    // Both keys should be set
    try std.testing.expectEqualStrings("v1", storage.get("k1").?);
    try std.testing.expectEqualStrings("v2", storage.get("k2").?);
}

test "commands - MSETEX with NX flag fails when key exists" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Pre-existing key
    try storage.set("k1", "old", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "MSETEX" },
        RespValue{ .bulk_string = "2" }, // numkeys
        RespValue{ .bulk_string = "k1" },
        RespValue{ .bulk_string = "new1" },
        RespValue{ .bulk_string = "k2" },
        RespValue{ .bulk_string = "v2" },
        RespValue{ .bulk_string = "NX" },
    };
    const result = try cmdMsetex(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":0\r\n", result);

    // k1 should still have old value; k2 should not exist
    try std.testing.expectEqualStrings("old", storage.get("k1").?);
    try std.testing.expect(storage.get("k2") == null);
}

test "commands - MSETEX with XX flag succeeds when all keys exist" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Pre-existing keys
    try storage.set("k1", "old1", null);
    try storage.set("k2", "old2", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "MSETEX" },
        RespValue{ .bulk_string = "2" }, // numkeys
        RespValue{ .bulk_string = "k1" },
        RespValue{ .bulk_string = "new1" },
        RespValue{ .bulk_string = "k2" },
        RespValue{ .bulk_string = "new2" },
        RespValue{ .bulk_string = "XX" },
    };
    const result = try cmdMsetex(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":1\r\n", result);

    // Both keys should have new values
    try std.testing.expectEqualStrings("new1", storage.get("k1").?);
    try std.testing.expectEqualStrings("new2", storage.get("k2").?);
}

test "commands - MSETEX with XX flag fails when any key missing" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Only k1 exists
    try storage.set("k1", "old", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "MSETEX" },
        RespValue{ .bulk_string = "2" }, // numkeys
        RespValue{ .bulk_string = "k1" },
        RespValue{ .bulk_string = "new1" },
        RespValue{ .bulk_string = "k2" },
        RespValue{ .bulk_string = "v2" },
        RespValue{ .bulk_string = "XX" },
    };
    const result = try cmdMsetex(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":0\r\n", result);

    // k1 should still have old value
    try std.testing.expectEqualStrings("old", storage.get("k1").?);
}

test "commands - MSETEX with KEEPTTL preserves existing TTL" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set k1 with expiry
    const future = std.time.milliTimestamp() + 100000;
    try storage.set("k1", "old", future);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "MSETEX" },
        RespValue{ .bulk_string = "2" }, // numkeys
        RespValue{ .bulk_string = "k1" },
        RespValue{ .bulk_string = "new1" },
        RespValue{ .bulk_string = "k2" },
        RespValue{ .bulk_string = "v2" },
        RespValue{ .bulk_string = "KEEPTTL" },
    };
    const result = try cmdMsetex(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":1\r\n", result);

    // k1 should have new value and still have TTL
    try std.testing.expectEqualStrings("new1", storage.get("k1").?);
    const ttl = storage.getTtlMs("k1");
    try std.testing.expect(ttl > 0);
}

test "commands - LCS basic string comparison" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set two strings
    try storage.set("key1", "ohmytext", null);
    try storage.set("key2", "mynewtext", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LCS" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key2" },
    };
    const result = try cmdLcs(allocator, storage, &args);
    defer allocator.free(result);

    // Expected LCS is "mytext"
    try std.testing.expect(std.mem.indexOf(u8, result, "mytext") != null);
}

test "commands - LCS with LEN option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "ohmytext", null);
    try storage.set("key2", "mynewtext", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LCS" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key2" },
        RespValue{ .bulk_string = "LEN" },
    };
    const result = try cmdLcs(allocator, storage, &args);
    defer allocator.free(result);

    // Expected length is 6 (mytext)
    try std.testing.expectEqualStrings(":6\r\n", result);
}

test "commands - LCS with empty strings" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "hello", null);
    try storage.set("key2", "", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LCS" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key2" },
    };
    const result = try cmdLcs(allocator, storage, &args);
    defer allocator.free(result);

    // Empty string
    try std.testing.expectEqualStrings("$0\r\n\r\n", result);
}

test "commands - LCS with nonexistent keys" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LCS" },
        RespValue{ .bulk_string = "missing1" },
        RespValue{ .bulk_string = "missing2" },
    };
    const result = try cmdLcs(allocator, storage, &args);
    defer allocator.free(result);

    // Both keys missing, empty LCS
    try std.testing.expectEqualStrings("$0\r\n\r\n", result);
}

test "commands - LCS with IDX option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set two strings with a clear pattern
    try storage.set("key1", "ohmytext", null);
    try storage.set("key2", "mynewtext", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LCS" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key2" },
        RespValue{ .bulk_string = "IDX" },
    };
    const result = try cmdLcs(allocator, storage, &args);
    defer allocator.free(result);

    // Should return array with "matches" and "len" keys
    try std.testing.expect(std.mem.indexOf(u8, result, "matches") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "len") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, ":6\r\n") != null); // LCS length is 6
}

test "commands - LCS with IDX and WITHMATCHLEN options" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "ohmytext", null);
    try storage.set("key2", "mynewtext", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LCS" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key2" },
        RespValue{ .bulk_string = "IDX" },
        RespValue{ .bulk_string = "WITHMATCHLEN" },
    };
    const result = try cmdLcs(allocator, storage, &args);
    defer allocator.free(result);

    // Should include match lengths in the output
    try std.testing.expect(std.mem.indexOf(u8, result, "matches") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "len") != null);
    // Should have 3 elements per match (key1_range, key2_range, match_len)
    try std.testing.expect(std.mem.indexOf(u8, result, "*3\r\n") != null);
}

test "commands - LCS with IDX and MINMATCHLEN options" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "ohmytext", null);
    try storage.set("key2", "mynewtext", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "LCS" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "key2" },
        RespValue{ .bulk_string = "IDX" },
        RespValue{ .bulk_string = "MINMATCHLEN" },
        RespValue{ .bulk_string = "3" },
    };
    const result = try cmdLcs(allocator, storage, &args);
    defer allocator.free(result);

    // Should filter out matches shorter than 3 characters
    try std.testing.expect(std.mem.indexOf(u8, result, "matches") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "len") != null);
}

test "commands - DIGEST returns hash for existing key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "myvalue", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DIGEST" },
        RespValue{ .bulk_string = "mykey" },
    };
    const result = try cmdDigest(allocator, storage, &args);
    defer allocator.free(result);

    // Should return a bulk string with hex digest (16 hex chars for Wyhash)
    try std.testing.expect(std.mem.startsWith(u8, result, "$"));
    try std.testing.expect(result.len > 10); // At least some hex digits
}

test "commands - DIGEST returns null for non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DIGEST" },
        RespValue{ .bulk_string = "nosuchkey" },
    };
    const result = try cmdDigest(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "commands - DIGEST wrong argument count" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DIGEST" },
    };
    const result = try cmdDigest(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR wrong number") != null);
}

test "commands - DELEX unconditional delete" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DELEX" },
        RespValue{ .bulk_string = "key1" },
    };
    const result = try cmdDelex(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
    try std.testing.expect(storage.get("key1") == null);
}

test "commands - DELEX with IFEQ matches" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DELEX" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "IFEQ" },
        RespValue{ .bulk_string = "value1" },
    };
    const result = try cmdDelex(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
    try std.testing.expect(storage.get("key1") == null);
}

test "commands - DELEX with IFEQ no match" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DELEX" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "IFEQ" },
        RespValue{ .bulk_string = "wrongvalue" },
    };
    const result = try cmdDelex(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":0\r\n", result);
    const val = storage.get("key1");
    try std.testing.expect(val != null);
}

test "commands - DELEX with IFNE matches" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DELEX" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "IFNE" },
        RespValue{ .bulk_string = "wrongvalue" },
    };
    const result = try cmdDelex(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
    try std.testing.expect(storage.get("key1") == null);
}

test "commands - DELEX with IFDEQ matches" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);

    // First get the digest
    const digest_args = [_]RespValue{
        RespValue{ .bulk_string = "DIGEST" },
        RespValue{ .bulk_string = "key1" },
    };
    const digest_result = try cmdDigest(allocator, storage, &digest_args);
    defer allocator.free(digest_result);

    // Extract hex digest from RESP bulk string ($N\r\nHEX\r\n)
    const digest_start = std.mem.indexOf(u8, digest_result, "\r\n").? + 2;
    const digest_end = std.mem.lastIndexOf(u8, digest_result, "\r\n").?;
    const digest_hex = digest_result[digest_start..digest_end];

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DELEX" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "IFDEQ" },
        RespValue{ .bulk_string = digest_hex },
    };
    const result = try cmdDelex(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
    try std.testing.expect(storage.get("key1") == null);
}

test "commands - DELEX with IFDNE matches" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DELEX" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "IFDNE" },
        RespValue{ .bulk_string = "0000000000000000" }, // Wrong digest
    };
    const result = try cmdDelex(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings(":1\r\n", result);
    try std.testing.expect(storage.get("key1") == null);
}

test "commands - DELEX multiple conditions error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key1", "value1", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "DELEX" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "IFEQ" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "IFNE" },
        RespValue{ .bulk_string = "value2" },
    };
    const result = try cmdDelex(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR multiple conditions") != null);
}

// SUBSTR tests (deprecated alias for GETRANGE)

test "commands - SUBSTR basic substring" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "Hello World", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SUBSTR" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "4" },
    };
    const result = try cmdSubstr(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$5\r\nHello\r\n", result);
}

test "commands - SUBSTR negative indices" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "Hello World", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SUBSTR" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "-5" },
        RespValue{ .bulk_string = "-1" },
    };
    const result = try cmdSubstr(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$5\r\nWorld\r\n", result);
}

test "commands - SUBSTR out of range" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("mykey", "Hello", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SUBSTR" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "10" },
        RespValue{ .bulk_string = "20" },
    };
    const result = try cmdSubstr(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$0\r\n\r\n", result);
}

test "commands - SUBSTR non-existent key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SUBSTR" },
        RespValue{ .bulk_string = "nonexistent" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "5" },
    };
    const result = try cmdSubstr(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("$0\r\n\r\n", result);
}

test "commands - SUBSTR WRONGTYPE error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set a non-string value (list)
    try storage.lpush("mylist", &[_][]const u8{"value"});

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SUBSTR" },
        RespValue{ .bulk_string = "mylist" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "5" },
    };
    const result = try cmdSubstr(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "WRONGTYPE") != null);
}

test "commands - SUBSTR wrong number of arguments" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SUBSTR" },
        RespValue{ .bulk_string = "mykey" },
    };
    const result = try cmdSubstr(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR wrong number of arguments") != null);
}

// ─────────────────────────────────────────────────────────────────────────────
// BITCOUNT BIT mode tests (Redis 7.0 — Iteration 281)
// ─────────────────────────────────────────────────────────────────────────────

test "BITCOUNT - non-existent key returns 0" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "BITCOUNT" },
        RespValue{ .bulk_string = "nosuchkey" },
    };
    const result = try cmdBitcount(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "BITCOUNT - full string popcount" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // 0xFF has 8 set bits
    try storage.set("k", "\xFF", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "BITCOUNT" },
        RespValue{ .bulk_string = "k" },
    };
    const result = try cmdBitcount(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":8\r\n", result);
}

test "BITCOUNT BYTE mode - byte range" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // "\xFF\x00": byte 0 = 8 bits, byte 1 = 0 bits
    try storage.set("k", "\xFF\x00", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "BITCOUNT" },
        RespValue{ .bulk_string = "k" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "BYTE" },
    };
    const result = try cmdBitcount(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":8\r\n", result);
}

test "BITCOUNT BIT mode - bit range equals byte range for full byte" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // "\xFF": bits 0-7 should equal byte 0 BYTE mode
    try storage.set("k", "\xFF", null);

    // BYTE mode: BITCOUNT k 0 0 BYTE
    const byte_args = [_]RespValue{
        RespValue{ .bulk_string = "BITCOUNT" },
        RespValue{ .bulk_string = "k" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "BYTE" },
    };
    const byte_result = try cmdBitcount(allocator, storage, &byte_args);
    defer allocator.free(byte_result);

    // BIT mode: BITCOUNT k 0 7 BIT (same 8 bits)
    const bit_args = [_]RespValue{
        RespValue{ .bulk_string = "BITCOUNT" },
        RespValue{ .bulk_string = "k" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "7" },
        RespValue{ .bulk_string = "BIT" },
    };
    const bit_result = try cmdBitcount(allocator, storage, &bit_args);
    defer allocator.free(bit_result);

    try std.testing.expectEqualStrings(":8\r\n", byte_result);
    try std.testing.expectEqualStrings(":8\r\n", bit_result);
}

test "BITCOUNT BIT mode - single bit" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // 0x80 = 1000 0000: only bit 0 (MSB) is set
    try storage.set("k", "\x80", null);

    // BITCOUNT k 0 0 BIT — only bit 0, which is set → 1
    const args_set = [_]RespValue{
        RespValue{ .bulk_string = "BITCOUNT" },
        RespValue{ .bulk_string = "k" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "BIT" },
    };
    const r_set = try cmdBitcount(allocator, storage, &args_set);
    defer allocator.free(r_set);
    try std.testing.expectEqualStrings(":1\r\n", r_set);

    // BITCOUNT k 1 7 BIT — bits 1-7 of 0x80 are all 0 → 0
    const args_clear = [_]RespValue{
        RespValue{ .bulk_string = "BITCOUNT" },
        RespValue{ .bulk_string = "k" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "7" },
        RespValue{ .bulk_string = "BIT" },
    };
    const r_clear = try cmdBitcount(allocator, storage, &args_clear);
    defer allocator.free(r_clear);
    try std.testing.expectEqualStrings(":0\r\n", r_clear);
}

test "BITCOUNT BIT mode - negative indices" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // "\xF0" = 1111 0000, 8 bits; bit -1 is bit 7 (last bit), which is 0
    try storage.set("k", "\xF0", null);

    // BITCOUNT k -4 -1 BIT — bits 4-7 (last 4 bits) of 0xF0 are 0 → 0
    const args = [_]RespValue{
        RespValue{ .bulk_string = "BITCOUNT" },
        RespValue{ .bulk_string = "k" },
        RespValue{ .bulk_string = "-4" },
        RespValue{ .bulk_string = "-1" },
        RespValue{ .bulk_string = "BIT" },
    };
    const result = try cmdBitcount(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":0\r\n", result);
}

test "BITCOUNT BIT mode - cross-byte range" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // "\xFF\xFF" = all bits set, 16 bits total
    try storage.set("k", "\xFF\xFF", null);

    // BITCOUNT k 4 11 BIT — bits 4-11 (8 bits, all set) → 8
    const args = [_]RespValue{
        RespValue{ .bulk_string = "BITCOUNT" },
        RespValue{ .bulk_string = "k" },
        RespValue{ .bulk_string = "4" },
        RespValue{ .bulk_string = "11" },
        RespValue{ .bulk_string = "BIT" },
    };
    const result = try cmdBitcount(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":8\r\n", result);
}

test "BITCOUNT - WRONGTYPE error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.lpush("mylist", &[_][]const u8{"value"});

    const args = [_]RespValue{
        RespValue{ .bulk_string = "BITCOUNT" },
        RespValue{ .bulk_string = "mylist" },
    };
    const result = try cmdBitcount(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "WRONGTYPE") != null);
}

test "BITCOUNT - invalid unit string" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("k", "hello", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "BITCOUNT" },
        RespValue{ .bulk_string = "k" },
        RespValue{ .bulk_string = "0" },
        RespValue{ .bulk_string = "1" },
        RespValue{ .bulk_string = "INVALID" },
    };
    const result = try cmdBitcount(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR syntax error") != null);
}

test "BITCOUNT - wrong argument count" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // 3 args: BITCOUNT key start (end missing)
    const args = [_]RespValue{
        RespValue{ .bulk_string = "BITCOUNT" },
        RespValue{ .bulk_string = "k" },
        RespValue{ .bulk_string = "0" },
    };
    const result = try cmdBitcount(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR wrong number of arguments") != null);
}

test "commands - GETEX EXAT 0 expires key immediately (not an error)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Set a key first
    _ = try storage.set(allocator, "mykey", "hello", null, false, false, false, false);

    // GETEX with EXAT 0 (epoch) should return the value, not an error
    const ps = try @import("../storage/pubsub.zig").PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GETEX" },
        RespValue{ .bulk_string = "mykey" },
        RespValue{ .bulk_string = "EXAT" },
        RespValue{ .bulk_string = "0" },
    };
    const result = try cmdGetex(allocator, storage, &args, ps, 0);
    defer allocator.free(result);

    // Should return the value, not an error
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") == null);
    try std.testing.expect(std.mem.indexOf(u8, result, "hello") != null);
}

test "commands - GETEX PXAT 0 expires key immediately (not an error)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.set(allocator, "mykey2", "world", null, false, false, false, false);

    const ps = try @import("../storage/pubsub.zig").PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GETEX" },
        RespValue{ .bulk_string = "mykey2" },
        RespValue{ .bulk_string = "PXAT" },
        RespValue{ .bulk_string = "0" },
    };
    const result = try cmdGetex(allocator, storage, &args, ps, 0);
    defer allocator.free(result);

    // Should return the value, not an error
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR") == null);
    try std.testing.expect(std.mem.indexOf(u8, result, "world") != null);
}

test "commands - GETEX EXAT negative is still an error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    _ = try storage.set(allocator, "mykey3", "data", null, false, false, false, false);

    const ps = try @import("../storage/pubsub.zig").PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GETEX" },
        RespValue{ .bulk_string = "mykey3" },
        RespValue{ .bulk_string = "EXAT" },
        RespValue{ .bulk_string = "-1" },
    };
    const result = try cmdGetex(allocator, storage, &args, ps, 0);
    defer allocator.free(result);

    // Negative timestamp should return an error
    try std.testing.expect(std.mem.indexOf(u8, result, "ERR invalid expire time") != null);
}

test "commands - SET EXAT 0 expires key immediately (not an error)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const ps = try @import("../storage/pubsub.zig").PubSub.init(allocator);
    defer ps.deinit();

    const cr = try @import("../server.zig").ClientRegistry.init(allocator);
    defer cr.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "setexatkey" },
        RespValue{ .bulk_string = "value" },
        RespValue{ .bulk_string = "EXAT" },
        RespValue{ .bulk_string = "0" },
    };
    const result = try cmdSet(allocator, storage, &args, ps, cr, 0, .resp2);
    defer allocator.free(result);

    // timestamp=0 (epoch, past) is valid — key should be set and immediately expired
    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "commands - SET PXAT 0 expires key immediately (not an error)" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const ps = try @import("../storage/pubsub.zig").PubSub.init(allocator);
    defer ps.deinit();

    const cr = try @import("../server.zig").ClientRegistry.init(allocator);
    defer cr.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "setpxatkey" },
        RespValue{ .bulk_string = "value" },
        RespValue{ .bulk_string = "PXAT" },
        RespValue{ .bulk_string = "0" },
    };
    const result = try cmdSet(allocator, storage, &args, ps, cr, 0, .resp2);
    defer allocator.free(result);

    // timestamp=0 (epoch, past) is valid — key should be set and immediately expired
    try std.testing.expectEqualStrings("+OK\r\n", result);
}

test "commands - SET EXAT negative is still an error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const ps = try @import("../storage/pubsub.zig").PubSub.init(allocator);
    defer ps.deinit();

    const cr = try @import("../server.zig").ClientRegistry.init(allocator);
    defer cr.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "setexatneg" },
        RespValue{ .bulk_string = "value" },
        RespValue{ .bulk_string = "EXAT" },
        RespValue{ .bulk_string = "-1" },
    };
    const result = try cmdSet(allocator, storage, &args, ps, cr, 0, .resp2);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR invalid expire time") != null);
}

test "SETRANGE - rejects offset that would exceed 512MB" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    var ps = pubsub_mod.PubSub.init(allocator);
    defer ps.deinit();

    // offset 536870912 + 1 byte = 536870913 > 512MB → error
    const args = [_]RespValue{
        RespValue{ .bulk_string = "SETRANGE" },
        RespValue{ .bulk_string = "bigkey" },
        RespValue{ .bulk_string = "536870912" },
        RespValue{ .bulk_string = "x" },
    };
    const result = try cmdSetrange(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "ERR string exceeds maximum allowed size") != null);
}

test "APPEND - normal operation works correctly" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    var ps = pubsub_mod.PubSub.init(allocator);
    defer ps.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "APPEND" },
        RespValue{ .bulk_string = "appendkey" },
        RespValue{ .bulk_string = "hello" },
    };
    const result = try cmdAppend(allocator, storage, &args, &ps, 0);
    defer allocator.free(result);
    try std.testing.expectEqualStrings(":5\r\n", result);
}
