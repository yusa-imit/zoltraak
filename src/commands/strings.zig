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
const cluster_cmds = @import("cluster.zig");
const utility_cmds = @import("utility.zig");
pub const TxState = tx_mod.TxState;
pub const ReplicationState = repl_mod.ReplicationState;
pub const ScriptStore = scripting_mod.ScriptStore;

const RespValue = protocol.RespValue;
const RespType = protocol.RespType;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;
const Persistence = persistence_mod.Persistence;
pub const Aof = aof_mod.Aof;
pub const PubSub = pubsub_mod.PubSub;
pub const ClientRegistry = client_cmds.ClientRegistry;
const RespProtocol = client_cmds.RespProtocol;

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

    // ── Replication: read-only guard ──────────────────────────────────────────
    // When this instance is a replica, reject write commands.
    // Replication protocol commands (REPLCONF, PSYNC) are always allowed.
    if (repl) |r| {
        if (r.role == .replica) {
            const replication_cmds = [_][]const u8{
                "REPLCONF", "PSYNC", "PING", "INFO", "REPLICAOF", "WAIT",
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
                "RPOP",       "SADD",       "SREM",       "HSET",       "HDEL",
                "ZADD",       "ZREM",       "FLUSHDB",    "FLUSHALL",
                "EXPIRE",     "PEXPIRE",    "EXPIREAT",   "PEXPIREAT",  "PERSIST",
                "INCR",       "DECR",       "INCRBY",     "DECRBY",     "INCRBYFLOAT",
                "APPEND",     "GETSET",     "GETDEL",     "GETEX",
                "SETNX",      "SETEX",      "PSETEX",
                "MSET",       "MSETNX",     "MSETEX",     "RENAME",     "RENAMENX",
                "UNLINK",
                "DUMP",       "RESTORE",    "COPY",       "TOUCH",      "MOVE",
                "HINCRBY",    "HINCRBYFLOAT", "HSETNX",
                "ZINCRBY",    "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE",
                "LSET",       "LTRIM",      "LREM",       "LPUSHX",     "RPUSHX",
                "LINSERT",    "LMOVE",      "RPOPLPUSH",  "BLPOP",      "BRPOP",
                "BLMOVE",     "BLMPOP",
                "SPOP",       "SMOVE",      "ZPOPMIN",    "ZPOPMAX",    "BZPOPMIN",
                "BZPOPMAX",   "SETRANGE",
                "SETBIT",     "BITOP",      "BITFIELD",
                "XADD",       "XDEL",       "XTRIM",      "XGROUP",     "XACK",
                "XCLAIM",     "XAUTOCLAIM",
                "GEOADD",     "PFADD",      "PFMERGE",
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

    // ── Transaction command handling ──────────────────────────────────────────
    // MULTI, EXEC, DISCARD, WATCH, UNWATCH are always handled immediately,
    // even inside a MULTI block.
    if (std.mem.eql(u8, cmd_upper, "MULTI")) {
        return tx_mod.cmdMulti(allocator, tx, array);
    } else if (std.mem.eql(u8, cmd_upper, "DISCARD")) {
        return tx_mod.cmdDiscard(allocator, tx, array);
    } else if (std.mem.eql(u8, cmd_upper, "WATCH")) {
        return tx_mod.cmdWatch(allocator, tx, array);
    } else if (std.mem.eql(u8, cmd_upper, "UNWATCH")) {
        return tx_mod.cmdUnwatch(allocator, tx, array);
    } else if (std.mem.eql(u8, cmd_upper, "EXEC")) {
        return try cmdExec(allocator, storage, aof, ps, subscriber_id, tx, repl, my_port, client_registry, client_id, script_store);
    }

    // When inside a MULTI block, queue all other commands and return +QUEUED.
    if (tx.active) {
        // We need to re-encode the parsed command back to RESP bytes for queuing.
        // Use the raw input: we store the RESP bytes via the parser's raw slice trick.
        // Since we don't have the raw bytes here, serialize the RespValue instead.
        var w = Writer.init(allocator);
        defer w.deinit();
        const encoded = try w.serialize(cmd);
        errdefer allocator.free(encoded);
        try tx.enqueue(encoded);
        return w.writeSimpleString("QUEUED");
    }

    // Determine if this is a write command that should be AOF-logged
    const is_write_cmd = blk: {
        const write_cmds = [_][]const u8{
            "SET",        "DEL",        "LPUSH",      "RPUSH",      "LPOP",
            "RPOP",       "SADD",       "SREM",       "HSET",       "HDEL",
            "ZADD",       "ZREM",       "FLUSHDB",    "FLUSHALL",
            "EXPIRE",     "PEXPIRE",    "EXPIREAT",   "PEXPIREAT",  "PERSIST",
            "INCR",       "DECR",       "INCRBY",     "DECRBY",     "INCRBYFLOAT",
            "APPEND",     "GETSET",     "GETDEL",     "GETEX",
            "SETNX",      "SETEX",      "PSETEX",
            "MSET",       "MSETNX",     "MSETEX",     "RENAME",     "RENAMENX",
            "UNLINK",
            "DUMP",       "RESTORE",    "COPY",       "TOUCH",      "MOVE",
            "HINCRBY",    "HINCRBYFLOAT", "HSETNX",
            "ZINCRBY",    "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE",
            "LSET",       "LTRIM",      "LREM",       "LPUSHX",     "RPUSHX",
            "LINSERT",    "LMOVE",      "RPOPLPUSH",  "BLPOP",      "BRPOP",
            "BLMOVE",     "BLMPOP",
            "SPOP",       "SMOVE",      "ZPOPMIN",    "ZPOPMAX",    "BZPOPMIN",
            "BZPOPMAX",   "SETRANGE",
            "SETBIT",     "BITOP",      "BITFIELD",
            "XADD",       "XDEL",       "XTRIM",      "XGROUP",     "XACK",
            "XCLAIM",     "XAUTOCLAIM",
            "GEOADD",     "PFADD",      "PFMERGE",
        };
        for (write_cmds) |wc| {
            if (std.mem.eql(u8, cmd_upper, wc)) break :blk true;
        }
        break :blk false;
    };

    // Execute command
    const response = blk: {
        // Server commands
        if (std.mem.eql(u8, cmd_upper, "HELLO")) {
            const args_slice = if (array.len > 1) array[1..] else array[0..0];
            break :blk try server_cmds.cmdHello(allocator, client_registry, client_id, args_slice);
        }
        // String commands
        else if (std.mem.eql(u8, cmd_upper, "PING")) {
            break :blk try cmdPing(allocator, array);
        } else if (std.mem.eql(u8, cmd_upper, "SET")) {
            break :blk try cmdSet(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "GET")) {
            break :blk try cmdGet(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "DEL")) {
            break :blk try cmdDel(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "EXISTS")) {
            break :blk try cmdExists(allocator, storage, array);
        }
        // String counter commands
        else if (std.mem.eql(u8, cmd_upper, "INCR")) {
            break :blk try cmdIncr(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "DECR")) {
            break :blk try cmdDecr(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "INCRBY")) {
            break :blk try cmdIncrby(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "DECRBY")) {
            break :blk try cmdDecrby(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "INCRBYFLOAT")) {
            break :blk try cmdIncrbyfloat(allocator, storage, array);
        }
        // String utility commands
        else if (std.mem.eql(u8, cmd_upper, "APPEND")) {
            break :blk try cmdAppend(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "STRLEN")) {
            break :blk try cmdStrlen(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "GETSET")) {
            break :blk try cmdGetset(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "GETDEL")) {
            break :blk try cmdGetdel(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "GETEX")) {
            break :blk try cmdGetex(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SETNX")) {
            break :blk try cmdSetnx(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SETEX")) {
            break :blk try cmdSetex(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "PSETEX")) {
            break :blk try cmdPsetex(allocator, storage, array);
        }
        // Multi-key string commands
        else if (std.mem.eql(u8, cmd_upper, "MGET")) {
            break :blk try cmdMget(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "MSET")) {
            break :blk try cmdMset(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "MSETNX")) {
            break :blk try cmdMsetnx(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "MSETEX")) {
            break :blk try cmdMsetex(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LCS")) {
            break :blk try cmdLcs(allocator, storage, array);
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
            break :blk try keys_cmds.cmdExpire(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "PEXPIRE")) {
            break :blk try keys_cmds.cmdPexpire(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "EXPIREAT")) {
            break :blk try keys_cmds.cmdExpireat(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "PEXPIREAT")) {
            break :blk try keys_cmds.cmdPexpireat(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "PERSIST")) {
            break :blk try keys_cmds.cmdPersist(allocator, storage, array);
        }
        // Keyspace commands
        else if (std.mem.eql(u8, cmd_upper, "TYPE")) {
            break :blk try keys_cmds.cmdType(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "KEYS")) {
            break :blk try keys_cmds.cmdKeys(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "RENAME")) {
            break :blk try keys_cmds.cmdRename(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "RENAMENX")) {
            break :blk try keys_cmds.cmdRenamenx(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "RANDOMKEY")) {
            break :blk try keys_cmds.cmdRandomkey(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "UNLINK")) {
            break :blk try keys_cmds.cmdUnlink(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "DUMP")) {
            break :blk try keys_cmds.cmdDump(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "RESTORE")) {
            break :blk try keys_cmds.cmdRestore(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "COPY")) {
            break :blk try keys_cmds.cmdCopy(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "TOUCH")) {
            break :blk try keys_cmds.cmdTouch(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "MOVE")) {
            break :blk try keys_cmds.cmdMove(allocator, storage, array);
        }
        // List commands
        else if (std.mem.eql(u8, cmd_upper, "LPUSH")) {
            break :blk try lists.cmdLpush(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "RPUSH")) {
            break :blk try lists.cmdRpush(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LPOP")) {
            break :blk try lists.cmdLpop(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "RPOP")) {
            break :blk try lists.cmdRpop(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LRANGE")) {
            break :blk try lists.cmdLrange(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LLEN")) {
            break :blk try lists.cmdLlen(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LINDEX")) {
            break :blk try lists.cmdLindex(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LSET")) {
            break :blk try lists.cmdLset(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LTRIM")) {
            break :blk try lists.cmdLtrim(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LREM")) {
            break :blk try lists.cmdLrem(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LPUSHX")) {
            break :blk try lists.cmdLpushx(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "RPUSHX")) {
            break :blk try lists.cmdRpushx(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LINSERT")) {
            break :blk try lists.cmdLinsert(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LPOS")) {
            break :blk try lists.cmdLpos(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "LMOVE")) {
            break :blk try lists.cmdLmove(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "RPOPLPUSH")) {
            break :blk try lists.cmdRpoplpush(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "BLPOP")) {
            break :blk try lists.cmdBlpop(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "BRPOP")) {
            break :blk try lists.cmdBrpop(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "BLMOVE")) {
            break :blk try lists.cmdBlmove(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "BLMPOP")) {
            break :blk try lists.cmdBlmpop(allocator, storage, array);
        }
        // Set commands
        else if (std.mem.eql(u8, cmd_upper, "SADD")) {
            break :blk try sets.cmdSadd(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SREM")) {
            break :blk try sets.cmdSrem(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SISMEMBER")) {
            break :blk try sets.cmdSismember(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SMEMBERS")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sets.cmdSmembers(allocator, storage, array, protocol_version);
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
            break :blk try sets.cmdSunionstore(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SINTERSTORE")) {
            break :blk try sets.cmdSinterstore(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SDIFFSTORE")) {
            break :blk try sets.cmdSdiffstore(allocator, storage, array);
        }
        // Hash commands
        else if (std.mem.eql(u8, cmd_upper, "HSET")) {
            break :blk try hashes.cmdHset(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HGET")) {
            break :blk try hashes.cmdHget(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HDEL")) {
            break :blk try hashes.cmdHdel(allocator, storage, array);
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
            break :blk try hashes.cmdHincrby(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HINCRBYFLOAT")) {
            break :blk try hashes.cmdHincrbyfloat(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HSETNX")) {
            break :blk try hashes.cmdHsetnx(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HSTRLEN")) {
            break :blk try hashes.cmdHstrlen(allocator, storage, array);
        }
        // Sorted set commands
        else if (std.mem.eql(u8, cmd_upper, "ZADD")) {
            break :blk try sorted_sets.cmdZadd(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZREM")) {
            break :blk try sorted_sets.cmdZrem(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZRANGE")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sorted_sets.cmdZrange(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "ZRANGEBYSCORE")) {
            break :blk try sorted_sets.cmdZrangebyscore(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZSCORE")) {
            break :blk try sorted_sets.cmdZscore(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZCARD")) {
            break :blk try sorted_sets.cmdZcard(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZRANK")) {
            break :blk try sorted_sets.cmdZrank(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZREVRANK")) {
            break :blk try sorted_sets.cmdZrevrank(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZINCRBY")) {
            break :blk try sorted_sets.cmdZincrby(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZCOUNT")) {
            break :blk try sorted_sets.cmdZcount(allocator, storage, array);
        }
        // SCAN family commands
        else if (std.mem.eql(u8, cmd_upper, "SCAN")) {
            break :blk try keys_cmds.cmdScan(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "HSCAN")) {
            break :blk try keys_cmds.cmdHscan(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SSCAN")) {
            break :blk try keys_cmds.cmdSscan(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZSCAN")) {
            break :blk try keys_cmds.cmdZscan(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "OBJECT")) {
            break :blk try keys_cmds.cmdObject(allocator, storage, array);
        }
        // Set commands (new)
        else if (std.mem.eql(u8, cmd_upper, "SPOP")) {
            break :blk try sets.cmdSpop(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SRANDMEMBER")) {
            break :blk try sets.cmdSrandmember(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SMOVE")) {
            break :blk try sets.cmdSmove(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SMISMEMBER")) {
            break :blk try sets.cmdSmismember(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SINTERCARD")) {
            break :blk try sets.cmdSintercard(allocator, storage, array);
        }
        // Sorted set commands (new)
        else if (std.mem.eql(u8, cmd_upper, "ZPOPMIN")) {
            break :blk try sorted_sets.cmdZpopmin(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZPOPMAX")) {
            break :blk try sorted_sets.cmdZpopmax(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "BZPOPMIN")) {
            break :blk try sorted_sets.cmdBzpopmin(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "BZPOPMAX")) {
            break :blk try sorted_sets.cmdBzpopmax(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZMSCORE")) {
            break :blk try sorted_sets.cmdZmscore(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZREVRANGE")) {
            const protocol_version = getClientProtocol(client_registry, client_id);
            break :blk try sorted_sets.cmdZrevrange(allocator, storage, array, protocol_version);
        } else if (std.mem.eql(u8, cmd_upper, "ZREVRANGEBYSCORE")) {
            break :blk try sorted_sets.cmdZrevrangebyscore(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZRANDMEMBER")) {
            break :blk try sorted_sets.cmdZrandmember(allocator, storage, array);
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
            break :blk try sorted_sets.cmdZunionstore(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZINTERSTORE")) {
            break :blk try sorted_sets.cmdZinterstore(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZDIFFSTORE")) {
            break :blk try sorted_sets.cmdZdiffstore(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZREMRANGEBYRANK")) {
            break :blk try sorted_sets.cmdZremrangebyrank(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZREMRANGEBYSCORE")) {
            break :blk try sorted_sets.cmdZremrangebyscore(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZREMRANGEBYLEX")) {
            break :blk try sorted_sets.cmdZremrangebylex(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZRANGEBYLEX")) {
            break :blk try sorted_sets.cmdZrangebylex(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZREVRANGEBYLEX")) {
            break :blk try sorted_sets.cmdZrevrangebylex(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "ZLEXCOUNT")) {
            break :blk try sorted_sets.cmdZlexcount(allocator, storage, array);
        }
        // String range commands
        else if (std.mem.eql(u8, cmd_upper, "GETRANGE") or std.mem.eql(u8, cmd_upper, "SUBSTR")) {
            break :blk try cmdGetrange(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "SETRANGE")) {
            break :blk try cmdSetrange(allocator, storage, array);
        }
        // Bit operations
        else if (std.mem.eql(u8, cmd_upper, "SETBIT")) {
            break :blk try cmdSetbit(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "GETBIT")) {
            break :blk try cmdGetbit(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "BITCOUNT")) {
            break :blk try cmdBitcount(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "BITOP")) {
            break :blk try cmdBitop(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "BITFIELD")) {
            break :blk try bitfield_cmds.cmdBitfield(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "BITFIELD_RO")) {
            break :blk try bitfield_cmds.cmdBitfieldRo(allocator, storage, array);
        }
        // Stream commands
        else if (std.mem.eql(u8, cmd_upper, "XADD")) {
            break :blk try streams.cmdXadd(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XLEN")) {
            break :blk try streams.cmdXlen(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XRANGE")) {
            break :blk try streams.cmdXrange(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XREVRANGE")) {
            break :blk try streams.cmdXrevrange(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XDEL")) {
            break :blk try streams.cmdXdel(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XTRIM")) {
            break :blk try streams.cmdXtrim(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XGROUP")) {
            break :blk try streams_adv.cmdXgroup(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XREAD")) {
            break :blk try streams_adv.cmdXread(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XREADGROUP")) {
            break :blk try streams_adv.cmdXreadgroup(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XACK")) {
            break :blk try streams_adv.cmdXack(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XCLAIM")) {
            break :blk try streams_adv.cmdXclaim(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XAUTOCLAIM")) {
            break :blk try streams_adv.cmdXautoclaim(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XPENDING")) {
            break :blk try streams.cmdXpending(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "XINFO")) {
            break :blk try streams.cmdXinfoStream(allocator, storage, array);
        }
        // Pub/Sub commands
        else if (std.mem.eql(u8, cmd_upper, "SUBSCRIBE")) {
            break :blk try pubsub_cmds.cmdSubscribe(allocator, ps, array, subscriber_id);
        } else if (std.mem.eql(u8, cmd_upper, "UNSUBSCRIBE")) {
            break :blk try pubsub_cmds.cmdUnsubscribe(allocator, ps, array, subscriber_id);
        } else if (std.mem.eql(u8, cmd_upper, "PUBLISH")) {
            break :blk try pubsub_cmds.cmdPublish(allocator, ps, array);
        } else if (std.mem.eql(u8, cmd_upper, "PUBSUB")) {
            break :blk try cmdPubsub(allocator, ps, array);
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
            break :blk try cmdFlushall(allocator, storage);
        }
        // Replication commands
        else if (std.mem.eql(u8, cmd_upper, "REPLICAOF")) {
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
                break :blk try repl_cmds.cmdWait(allocator, r, str_args);
            }
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeInteger(0);
        } else if (std.mem.eql(u8, cmd_upper, "INFO")) {
            const str_args = try arrayToStrings(allocator, array);
            defer allocator.free(str_args);

            // Build server config from storage/defaults
            const server_config = info_cmds.ServerConfig{
                .port = 6379,  // TODO: pass actual port
                .bind = "127.0.0.1",  // TODO: pass actual bind address
                .maxmemory = 0,
                .maxmemory_policy = "noeviction",
                .timeout = 0,
                .tcp_keepalive = 300,
                .save = "900 1 300 10 60 10000",
                .appendonly = aof != null,
                .appendfsync = "everysec",
                .databases = 1,
            };

            // Build server stats
            const server_stats = info_cmds.ServerStats{
                .client_count = 1,  // TODO: track actual client count
                .total_commands_processed = 0,  // TODO: track command count
                .total_connections_received = 0,  // TODO: track connection count
                .start_time_seconds = 0,  // TODO: track server start time
            };

            if (repl) |r| {
                break :blk try info_cmds.cmdInfo(allocator, storage, r, server_config, server_stats, str_args);
            }
            // Fallback without replication
            var fallback_repl = try ReplicationState.initPrimary(allocator);
            defer fallback_repl.deinit();
            break :blk try info_cmds.cmdInfo(allocator, storage, &fallback_repl, server_config, server_stats, str_args);
        }
        // Client connection commands
        else if (std.mem.eql(u8, cmd_upper, "CLIENT")) {
            // CLIENT subcommand is in array[1]
            const subcmd_array = array[1..];
            break :blk try client_cmds.cmdClient(allocator, client_registry, client_id, subcmd_array);
        }
        // Configuration commands
        else if (std.mem.eql(u8, cmd_upper, "CONFIG")) {
            break :blk try config_cmds.executeConfigCommand(allocator, storage, array);
        }
        // Command introspection
        else if (std.mem.eql(u8, cmd_upper, "COMMAND")) {
            if (array.len == 1) {
                // COMMAND - return all commands
                break :blk try command_cmds.cmdCommand(allocator);
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
                    break :blk try command_cmds.cmdCommandInfo(allocator, cmd_args);
                } else if (std.mem.eql(u8, subcmd_upper, "GETKEYS")) {
                    const cmd_args = try extractBulkStrings(allocator, array[2..]);
                    defer allocator.free(cmd_args);
                    break :blk try command_cmds.cmdCommandGetKeys(allocator, cmd_args);
                } else if (std.mem.eql(u8, subcmd_upper, "LIST")) {
                    var filter_by: ?[]const u8 = null;
                    if (array.len > 2) {
                        const filter_opt = switch (array[2]) {
                            .bulk_string => |s| s,
                            else => {
                                var w = Writer.init(allocator);
                                defer w.deinit();
                                return w.writeError("ERR invalid filter format");
                            },
                        };
                        const filter_upper = try std.ascii.allocUpperString(allocator, filter_opt);
                        defer allocator.free(filter_upper);
                        if (std.mem.eql(u8, filter_upper, "FILTERBY") and array.len > 3) {
                            filter_by = switch (array[3]) {
                                .bulk_string => |s| s,
                                else => {
                                    var w = Writer.init(allocator);
                                    defer w.deinit();
                                    return w.writeError("ERR invalid filter value");
                                },
                            };
                        }
                    }
                    break :blk try command_cmds.cmdCommandList(allocator, filter_by);
                } else if (std.mem.eql(u8, subcmd_upper, "HELP")) {
                    break :blk try command_cmds.cmdCommandHelp(allocator);
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
            break :blk try geo_cmds.cmdGeodist(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "GEOHASH")) {
            break :blk try geo_cmds.cmdGeohash(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "GEORADIUS")) {
            break :blk try geo_cmds.cmdGeoradius(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "GEOSEARCH")) {
            break :blk try geo_cmds.cmdGeosearch(allocator, storage, array);
        }
        // HyperLogLog commands
        else if (std.mem.eql(u8, cmd_upper, "PFADD")) {
            break :blk try hll_cmds.cmdPfadd(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "PFCOUNT")) {
            break :blk try hll_cmds.cmdPfcount(allocator, storage, array);
        } else if (std.mem.eql(u8, cmd_upper, "PFMERGE")) {
            break :blk try hll_cmds.cmdPfmerge(allocator, storage, array);
        }
        // Server introspection commands
        else if (std.mem.eql(u8, cmd_upper, "MEMORY")) {
            const args = try extractBulkStrings(allocator, array[1..]);
            defer allocator.free(args);
            break :blk try introspection_cmds.cmdMemory(allocator, storage, args);
        } else if (std.mem.eql(u8, cmd_upper, "SLOWLOG")) {
            const args = try extractBulkStrings(allocator, array[1..]);
            defer allocator.free(args);
            break :blk try introspection_cmds.cmdSlowlogStub(allocator, args);
        }
        // Scripting commands
        else if (std.mem.eql(u8, cmd_upper, "EVAL")) {
            const args = try extractBulkStrings(allocator, array[1..]);
            defer allocator.free(args);
            const resp_version = @intFromEnum(getClientProtocol(client_registry, client_id));
            break :blk try scripting_cmds.cmdEval(allocator, storage, script_store, args, resp_version);
        } else if (std.mem.eql(u8, cmd_upper, "EVALSHA")) {
            const args = try extractBulkStrings(allocator, array[1..]);
            defer allocator.free(args);
            const resp_version = @intFromEnum(getClientProtocol(client_registry, client_id));
            break :blk try scripting_cmds.cmdEvalSHA(allocator, storage, script_store, args, resp_version);
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
            } else if (std.mem.eql(u8, subcmd_upper, "NODES")) {
                break :blk try cluster_cmds.cmdClusterNodes(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "INFO")) {
                break :blk try cluster_cmds.cmdClusterInfo(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "MYID")) {
                break :blk try cluster_cmds.cmdClusterMyId(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "KEYSLOT")) {
                break :blk try cluster_cmds.cmdClusterKeyslot(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "COUNTKEYSINSLOT")) {
                break :blk try cluster_cmds.cmdClusterCountKeysInSlot(allocator, args, storage, null, 0);
            } else if (std.mem.eql(u8, subcmd_upper, "GETKEYSINSLOT")) {
                break :blk try cluster_cmds.cmdClusterGetKeysInSlot(allocator, args, storage, null, 0);
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
                break :blk try acl_cmds.cmdACLWhoami(allocator, array[1..]);
            } else if (std.mem.eql(u8, subcmd_upper, "LIST")) {
                break :blk try acl_cmds.cmdACLList(allocator, array[1..]);
            } else if (std.mem.eql(u8, subcmd_upper, "USERS")) {
                break :blk try acl_cmds.cmdACLUsers(allocator, array[1..]);
            } else if (std.mem.eql(u8, subcmd_upper, "GETUSER")) {
                break :blk try acl_cmds.cmdACLGetuser(allocator, array[1..]);
            } else if (std.mem.eql(u8, subcmd_upper, "SETUSER")) {
                break :blk try acl_cmds.cmdACLSetuser(allocator, array[1..]);
            } else if (std.mem.eql(u8, subcmd_upper, "DELUSER")) {
                break :blk try acl_cmds.cmdACLDeluser(allocator, array[1..]);
            } else if (std.mem.eql(u8, subcmd_upper, "CAT")) {
                break :blk try acl_cmds.cmdACLCat(allocator, array[1..]);
            } else if (std.mem.eql(u8, subcmd_upper, "HELP")) {
                break :blk try acl_cmds.cmdACLHelp(allocator, array[1..]);
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
            break :blk try utility_cmds.cmdShutdown(allocator, args, storage, ps, null, client_registry, client_id, storage.config, @intFromEnum(client_protocol));
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
            break :blk try utility_cmds.cmdSwapdb(allocator, args, storage, ps, null, client_registry, client_id, storage.config, @intFromEnum(client_protocol));
        } else {
            var w = Writer.init(allocator);
            defer w.deinit();
            var buf: [256]u8 = undefined;
            const err_msg = try std.fmt.bufPrint(&buf, "ERR unknown command '{s}'", .{cmd_name});
            return w.writeError(err_msg);
        }
    };

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
                a.appendCommand(aof_args) catch |err| {
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
fn cmdSet(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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
            if (expires_at != null) {
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
            if (expires_at != null) {
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
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Check NX condition
    if (nx and storage.exists(key)) {
        return w.writeNull(); // $-1\r\n
    }

    // Check XX condition
    if (xx and !storage.exists(key)) {
        return w.writeNull(); // $-1\r\n
    }

    // Execute SET
    try storage.set(key, value, expires_at);
    return w.writeOK();
}

/// GET key
/// Returns bulk string value or null
fn cmdGet(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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
    return w.writeBulkString(value);
}

/// DEL key [key ...]
/// Returns integer count of deleted keys
fn cmdDel(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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

    const deleted_count = storage.del(keys.items);
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

    Persistence.save(storage, DEFAULT_RDB_PATH, allocator) catch |err| {
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

    Persistence.save(storage, DEFAULT_RDB_PATH, allocator) catch |err| {
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
fn cmdFlushall(allocator: std.mem.Allocator, storage: *Storage) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    storage.flushAll();
    return w.writeSimpleString("OK");
}

/// PUBSUB subcommand [args...]
/// Routes to CHANNELS or NUMSUB sub-commands.
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
fn cmdIncr(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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

    return w.writeInteger(new_val);
}

/// DECR key
/// Decrements the integer value of key by 1.
fn cmdDecr(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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

    return w.writeInteger(new_val);
}

/// INCRBY key increment
/// Increments the integer value of key by increment.
fn cmdIncrby(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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

    return w.writeInteger(new_val);
}

/// DECRBY key decrement
/// Decrements the integer value of key by decrement.
fn cmdDecrby(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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

    return w.writeInteger(new_val);
}

/// INCRBYFLOAT key increment
/// Increment the float value of key by increment.
fn cmdIncrbyfloat(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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

    const new_val = storage.incrbyfloat(key, delta) catch |err| switch (err) {
        error.WrongType => return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        error.NotFloat => return w.writeError("ERR value is not a valid float"),
        else => return err,
    };

    // Format response: strip unnecessary trailing zeros but keep at least one decimal
    var buf: [64]u8 = undefined;
    const formatted = std.fmt.bufPrint(&buf, "{d}", .{new_val}) catch unreachable;
    return w.writeBulkString(formatted);
}

// ── String utility commands ───────────────────────────────────────────────────

/// APPEND key value
/// Appends value to the string stored at key. Returns new length.
fn cmdAppend(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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
        else => return err,
    };

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
/// Sets key to value and returns the old value.
fn cmdGetset(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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

    return w.writeBulkString(old_copy);
}

/// GETDEL key
/// Gets the value and deletes the key.
fn cmdGetdel(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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

    return w.writeBulkString(val);
}

/// GETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-ms | PERSIST]
/// Gets the value and optionally updates the expiry.
fn cmdGetex(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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
            if (ts <= 0) return w.writeError("ERR invalid expire time in 'getex' command");
            expires_at = ts * 1000;
        } else if (std.mem.eql(u8, opt_upper, "PXAT")) {
            if (args.len < 4) return w.writeError("ERR syntax error");
            const ts = parseInteger(args[3]) catch return w.writeError("ERR value is not an integer or out of range");
            if (ts <= 0) return w.writeError("ERR invalid expire time in 'getex' command");
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

    return w.writeBulkString(val);
}

/// SETNX key value
/// Set key to value only if key does not exist. Returns 1 if set, 0 if not.
fn cmdSetnx(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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
    return w.writeInteger(1);
}

/// SETEX key seconds value
/// Set key to value with expiry in seconds.
fn cmdSetex(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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
    try storage.set(key, value, expires_at);
    return w.writeOK();
}

/// PSETEX key milliseconds value
/// Set key to value with expiry in milliseconds.
fn cmdPsetex(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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
    try storage.set(key, value, expires_at);
    return w.writeOK();
}

// ── Multi-key string commands ─────────────────────────────────────────────────

/// MGET key [key ...]
/// Returns values for each key (null bulk string for missing/wrong-type keys).
fn cmdMget(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
    };

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("+OK\r\n", result);
    try std.testing.expectEqualStrings("value1", storage.get("key1").?);
}

test "commands - SET with EX option" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "key1" },
        RespValue{ .bulk_string = "value1" },
        RespValue{ .bulk_string = "EX" },
        RespValue{ .bulk_string = "60" },
    };

    const result = try cmdSet(allocator, storage, &args);
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

    const result = try cmdSet(allocator, storage, &args);
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

    const result = try cmdSet(allocator, storage, &args);
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

    const result = try cmdSet(allocator, storage, &args);
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

    const result = try cmdSet(allocator, storage, &args);
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

    const result = try cmdSet(allocator, storage, &args);
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

    const result = try cmdSet(allocator, storage, &args);
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

    const result = try cmdSet(allocator, storage, &args);
    defer allocator.free(result);

    try std.testing.expectEqualStrings("-ERR invalid expire time in 'set' command\r\n", result);
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

    const result = try cmdGet(allocator, storage, &args);
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

    const result = try cmdGet(allocator, storage, &args);
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

    const result = try cmdGet(allocator, storage, &args);
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

    const result = try cmdDel(allocator, storage, &args);
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

    const result = try cmdDel(allocator, storage, &args);
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

    const result = try cmdDel(allocator, storage, &args);
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

/// SETRANGE key offset value
/// Overwrite bytes at offset in string. Zero-pads if necessary.
/// Returns new total length.
pub fn cmdSetrange(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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

    const new_len = storage.setrange(key, @as(usize, @intCast(offset)), value) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(@intCast(new_len));
}

// ── Bit operations ────────────────────────────────────────────────────────

pub fn cmdSetbit(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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

    if (args.len != 2 and args.len != 4) {
        return w.writeError("ERR wrong number of arguments for 'bitcount' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    const start: ?i64 = if (args.len == 4) blk: {
        const start_str = switch (args[2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR value is not an integer or out of range"),
        };
        break :blk std.fmt.parseInt(i64, start_str, 10) catch {
            return w.writeError("ERR value is not an integer or out of range");
        };
    } else null;

    const end: ?i64 = if (args.len == 4) blk: {
        const end_str = switch (args[3]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR value is not an integer or out of range"),
        };
        break :blk std.fmt.parseInt(i64, end_str, 10) catch {
            return w.writeError("ERR value is not an integer or out of range");
        };
    } else null;

    const count = storage.bitcount(key, start, end) catch |err| {
        if (err == error.WrongType) {
            return w.writeError("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return err;
    };

    return w.writeInteger(count);
}

pub fn cmdBitop(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
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

    return w.writeInteger(@intCast(result_len));
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
    const result = try cmdIncrbyfloat(allocator, storage, &args);
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
    const result = try cmdIncrbyfloat(allocator, storage, &args);
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

    try storage.set("key", "old", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GETSET" },
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "new" },
    };
    const result = try cmdGetset(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$3\r\nold\r\n", result);

    try std.testing.expectEqualStrings("new", storage.get("key").?);
}

test "commands - GETDEL returns value and removes key" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("key", "value", null);

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GETDEL" },
        RespValue{ .bulk_string = "key" },
    };
    const result = try cmdGetdel(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$5\r\nvalue\r\n", result);

    try std.testing.expect(storage.get("key") == null);
}

test "commands - GETDEL on missing key returns null" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "GETDEL" },
        RespValue{ .bulk_string = "missing" },
    };
    const result = try cmdGetdel(allocator, storage, &args);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("$-1\r\n", result);
}

test "commands - SETNX sets only when key missing" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "SETNX" },
        RespValue{ .bulk_string = "key" },
        RespValue{ .bulk_string = "value" },
    };
    const r1 = try cmdSetnx(allocator, storage, &args);
    defer allocator.free(r1);
    try std.testing.expectEqualStrings(":1\r\n", r1);

    const r2 = try cmdSetnx(allocator, storage, &args);
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

    const args = [_]RespValue{
        RespValue{ .bulk_string = "MGET" },
        RespValue{ .bulk_string = "k1" },
        RespValue{ .bulk_string = "k2" },
        RespValue{ .bulk_string = "missing" },
    };
    const result = try cmdMget(allocator, storage, &args);
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
