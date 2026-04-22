const std = @import("std");
const memory = @import("memory.zig");

const Storage = memory.Storage;
const Value = memory.Value;
const ValueType = memory.ValueType;

/// RDB file format constants
/// Magic bytes: "ZOLTRAAK" followed by version u8
const RDB_MAGIC = "ZOLTRAAK";
const RDB_VERSION: u8 = 1;

/// Type tags written to RDB file
const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_HASH: u8 = 3;
const RDB_TYPE_SORTED_SET: u8 = 4;
const RDB_TYPE_EOF: u8 = 0xFF;
const RDB_DB_SELECTOR: u8 = 0xFE;

/// Redis length encoding format (used for database selector):
/// - 0-63: Single byte with value (top 2 bits = 00)
/// - 64-16383: Two bytes LE (top 2 bits = 01)
/// - 16384+: Four bytes LE (top 2 bits = 10)

/// RDB Persistence: save and load snapshots of Storage to/from disk.
///
/// Binary format (little-endian):
///   MAGIC[8] VERSION[1]
///   [For each database with keys:]
///     DB_SELECTOR[1] = 0xFE
///     DB_INDEX[1-4] (Redis length encoding)
///     [For each key in database:]
///       TYPE[1]
///       EXPIRES_FLAG[1]  (0 = no expiry, 1 = has expiry)
///       if EXPIRES_FLAG == 1: EXPIRES_AT[8] (i64 LE)
///       KEY_LEN[4] KEY[KEY_LEN]
///       <type-specific payload>
///   EOF[1] = 0xFF
///   CHECKSUM[4] (CRC32 of everything before)
pub const Persistence = struct {
    /// Save all databases to RDB file.
    /// The file is written atomically: first to a temp file, then renamed.
    /// Accepts array of Storage pointers (one per database).
    pub fn save(databases: []Storage, path: []const u8, allocator: std.mem.Allocator) !void {
        // Build data in memory first
        var buf = std.ArrayList(u8){};
        defer buf.deinit(allocator);

        const w = buf.writer(allocator);

        // Magic + version
        try w.writeAll(RDB_MAGIC);
        try w.writeByte(RDB_VERSION);

        const now = Storage.getCurrentTimestamp();

        // Iterate all databases
        for (databases, 0..) |*storage, db_idx| {
            // Lock storage and check if it has keys
            storage.mutex.lock();
            defer storage.mutex.unlock();

            var it = storage.data.iterator();
            var has_keys = false;

            // First pass: check if database has non-expired keys
            while (it.next()) |entry| {
                if (!entry.value_ptr.isExpired(now)) {
                    has_keys = true;
                    break;
                }
            }

            // Skip empty databases
            if (!has_keys) continue;

            // Write database selector
            try w.writeByte(RDB_DB_SELECTOR);
            try writeLengthEncoded(w, @intCast(db_idx));

            // Reset iterator for second pass: write keys
            it = storage.data.iterator();
            while (it.next()) |entry| {
                // Skip expired keys
                if (entry.value_ptr.isExpired(now)) continue;

                const key = entry.key_ptr.*;
                const value = entry.value_ptr.*;

                // Write type byte
                const type_byte: u8 = switch (value) {
                    .string => RDB_TYPE_STRING,
                    .list => RDB_TYPE_LIST,
                    .set => RDB_TYPE_SET,
                    .hash => RDB_TYPE_HASH,
                    .sorted_set => RDB_TYPE_SORTED_SET,
                    .stream => 0xFF, // Placeholder - streams not yet serialized
                    .hyperloglog => 0xFE, // HyperLogLog type
                    .json => 0x0F, // JSON type
                    .timeseries => 0xFD, // Time Series type
                    .bloom => 0xFC, // Bloom Filter type
                    .cuckoo => 0xFB, // Cuckoo Filter type
                    .count_min_sketch => 0xFA, // Count-Min Sketch type
                };
                try w.writeByte(type_byte);

                // Write expiration
                const expires_at = value.getExpiration();
                if (expires_at) |exp| {
                    try w.writeByte(1);
                    try w.writeInt(i64, exp, .little);
                } else {
                    try w.writeByte(0);
                }

                // Write key
                try writeBlob(w, key);

                // Write value payload
                switch (value) {
                    .string => |s| try writeBlob(w, s.data),
                    .list => |l| {
                        try w.writeInt(u32, @intCast(l.data.items.len), .little);
                        for (l.data.items) |elem| {
                            try writeBlob(w, elem);
                        }
                    },
                    .set => |s| {
                        try w.writeInt(u32, @intCast(s.data.count()), .little);
                        var kit = s.data.keyIterator();
                        while (kit.next()) |k| {
                            try writeBlob(w, k.*);
                        }
                    },
                    .hash => |h| {
                        try w.writeInt(u32, @intCast(h.data.count()), .little);
                        var hit = h.data.iterator();
                        while (hit.next()) |e| {
                            try writeBlob(w, e.key_ptr.*);
                            try writeBlob(w, e.value_ptr.*.data);
                        }
                    },
                    .sorted_set => |z| {
                        try w.writeInt(u32, @intCast(z.sorted_list.items.len), .little);
                        for (z.sorted_list.items) |scored| {
                            // Write score as f64 (8 bytes IEEE 754 LE)
                            const score_bits = @as(u64, @bitCast(scored.score));
                            try w.writeInt(u64, score_bits, .little);
                            try writeBlob(w, scored.member);
                        }
                    },
                    .stream => {
                        // Streams not yet fully implemented in persistence
                        try w.writeInt(u32, 0, .little);
                    },
                    .hyperloglog => |hll| {
                        try writeBlob(w, &hll.registers);
                    },
                    .json => |j| {
                        // Serialize JSON to string
                        const json_str = try j.root.stringify(j.allocator);
                        defer j.allocator.free(json_str);
                        try writeBlob(w, json_str);
                    },
                    .timeseries => {
                        // Time series not yet implemented in persistence
                        try w.writeInt(u32, 0, .little);
                    },
                    .bloom => {
                        // Bloom filter not yet implemented in persistence
                        try w.writeInt(u32, 0, .little);
                    },
                    .cuckoo => {
                        // Cuckoo filter not yet implemented in persistence
                        try w.writeInt(u32, 0, .little);
                    },
                    .count_min_sketch => {
                        // Count-Min Sketch not yet implemented in persistence
                        try w.writeInt(u32, 0, .little);
                    },
                }
            }
        }

        // EOF marker
        try w.writeByte(RDB_TYPE_EOF);

        // Compute CRC32 of payload so far, then append it
        const crc = std.hash.Crc32.hash(buf.items);
        var crc_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &crc_bytes, crc, .little);
        try buf.appendSlice(allocator, &crc_bytes);

        // Write to tmp file then rename atomically
        var tmp_buf: [512]u8 = undefined;
        const tmp_path = try std.fmt.bufPrint(&tmp_buf, "{s}.tmp", .{path});

        try std.fs.cwd().writeFile(.{ .sub_path = tmp_path, .data = buf.items });
        try std.fs.cwd().rename(tmp_path, path);

        // Update last save time on successful save for all databases
        for (databases) |*storage| {
            storage.updateLastSaveTime();
        }
    }

    /// Load a snapshot from the given file path into all databases.
    /// Accepts array of Storage pointers (one per database).
    /// Returns total number of keys loaded. Returns 0 if file not found.
    pub fn load(databases: []Storage, path: []const u8, allocator: std.mem.Allocator) !usize {
        const file = std.fs.cwd().openFile(path, .{}) catch |err| {
            if (err == error.FileNotFound) return 0;
            return err;
        };
        defer file.close();

        const data = try file.readToEndAlloc(allocator, 512 * 1024 * 1024); // 512MB max
        defer allocator.free(data);

        if (data.len < RDB_MAGIC.len + 1 + 4) return error.InvalidRdbFile;

        // Verify checksum (last 4 bytes)
        const payload = data[0 .. data.len - 4];
        const stored_crc = std.mem.readInt(u32, data[data.len - 4 ..][0..4], .little);
        const computed_crc = std.hash.Crc32.hash(payload);
        if (stored_crc != computed_crc) return error.RdbChecksumMismatch;

        var pos: usize = 0;

        // Check magic
        if (!std.mem.eql(u8, payload[pos .. pos + RDB_MAGIC.len], RDB_MAGIC)) {
            return error.InvalidRdbFile;
        }
        pos += RDB_MAGIC.len;

        // Check version
        const version = payload[pos];
        pos += 1;
        if (version != RDB_VERSION) return error.UnsupportedRdbVersion;

        const now = Storage.getCurrentTimestamp();
        var keys_loaded: usize = 0;
        var current_db: usize = 0; // Default to database 0 for backward compatibility

        while (pos < payload.len) {
            if (pos >= payload.len) break;
            const type_byte = payload[pos];
            pos += 1;

            // Check for database selector
            if (type_byte == RDB_DB_SELECTOR) {
                const db_idx = try readLengthEncoded(payload, &pos);
                if (db_idx < 0 or db_idx >= databases.len) {
                    return error.InvalidDatabaseIndex;
                }
                current_db = @intCast(db_idx);
                continue;
            }

            if (type_byte == RDB_TYPE_EOF) break;

            // Read expiration flag
            if (pos >= payload.len) return error.InvalidRdbFile;
            const expires_flag = payload[pos];
            pos += 1;
            var expires_at: ?i64 = null;
            if (expires_flag == 1) {
                if (pos + 8 > payload.len) return error.InvalidRdbFile;
                const exp = std.mem.readInt(i64, payload[pos..][0..8], .little);
                pos += 8;
                // Skip keys that are already expired
                if (exp <= now) {
                    // Skip key blob
                    if (pos + 4 > payload.len) return error.InvalidRdbFile;
                    const key_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
                    pos += 4 + key_len;
                    try skipValue(payload, &pos, type_byte);
                    continue;
                }
                expires_at = exp;
            }

            // Read key
            const key = try readBlob(payload, &pos, allocator);
            defer allocator.free(key);

            // Read and insert value into current_db
            const storage = &databases[current_db];
            switch (type_byte) {
                RDB_TYPE_STRING => {
                    const val = try readBlob(payload, &pos, allocator);
                    defer allocator.free(val);
                    try storage.set(key, val, expires_at);
                },
                RDB_TYPE_LIST => {
                    if (pos + 4 > payload.len) return error.InvalidRdbFile;
                    const count = std.mem.readInt(u32, payload[pos..][0..4], .little);
                    pos += 4;
                    var elems = try allocator.alloc([]const u8, count);
                    defer {
                        for (elems) |e| allocator.free(e);
                        allocator.free(elems);
                    }
                    for (0..count) |i| {
                        elems[i] = try readBlob(payload, &pos, allocator);
                    }
                    _ = try storage.rpush(key, elems, expires_at);
                },
                RDB_TYPE_SET => {
                    if (pos + 4 > payload.len) return error.InvalidRdbFile;
                    const count = std.mem.readInt(u32, payload[pos..][0..4], .little);
                    pos += 4;
                    var members = try allocator.alloc([]const u8, count);
                    defer {
                        for (members) |m| allocator.free(m);
                        allocator.free(members);
                    }
                    for (0..count) |i| {
                        members[i] = try readBlob(payload, &pos, allocator);
                    }
                    _ = try storage.sadd(key, members, expires_at);
                },
                RDB_TYPE_HASH => {
                    if (pos + 4 > payload.len) return error.InvalidRdbFile;
                    const count = std.mem.readInt(u32, payload[pos..][0..4], .little);
                    pos += 4;
                    var i: u32 = 0;
                    while (i < count) : (i += 1) {
                        const field = try readBlob(payload, &pos, allocator);
                        defer allocator.free(field);
                        const hval = try readBlob(payload, &pos, allocator);
                        defer allocator.free(hval);
                        var fields_arr = [_][]const u8{field};
                        var values_arr = [_][]const u8{hval};
                        _ = try storage.hset(key, &fields_arr, &values_arr, expires_at);
                    }
                },
                RDB_TYPE_SORTED_SET => {
                    if (pos + 4 > payload.len) return error.InvalidRdbFile;
                    const count = std.mem.readInt(u32, payload[pos..][0..4], .little);
                    pos += 4;
                    var i: u32 = 0;
                    while (i < count) : (i += 1) {
                        if (pos + 8 > payload.len) return error.InvalidRdbFile;
                        const score_bits = std.mem.readInt(u64, payload[pos..][0..8], .little);
                        const score: f64 = @bitCast(score_bits);
                        pos += 8;
                        const member = try readBlob(payload, &pos, allocator);
                        defer allocator.free(member);
                        var scores_arr = [_]f64{score};
                        var members_arr = [_][]const u8{member};
                        _ = try storage.zadd(key, &scores_arr, &members_arr, 0, expires_at);
                    }
                },
                0xFE => { // HyperLogLog
                    const registers_data = try readBlob(payload, &pos, allocator);
                    defer allocator.free(registers_data);

                    if (registers_data.len != 16384) return error.InvalidRdbFile;

                    // Create HyperLogLog value directly
                    var hll = Value.HyperLogLogValue.init();
                    hll.expires_at = expires_at;
                    @memcpy(&hll.registers, registers_data);

                    storage.mutex.lock();
                    defer storage.mutex.unlock();

                    const key_copy = try storage.allocator.dupe(u8, key);
                    errdefer storage.allocator.free(key_copy);

                    try storage.data.put(key_copy, Value{ .hyperloglog = hll });
                },
                0x0F => { // JSON
                    const json_str = try readBlob(payload, &pos, allocator);
                    defer allocator.free(json_str);

                    // Parse JSON
                    const json_node_mod = @import("json_value.zig");
                    const json_root = try json_node_mod.JsonNode.parse(storage.allocator, json_str);

                    // Create JSON value
                    const json_val = Value{
                        .json = .{
                            .root = json_root,
                            .expires_at = expires_at,
                            .allocator = storage.allocator,
                        },
                    };

                    storage.mutex.lock();
                    defer storage.mutex.unlock();

                    const owned_key = try storage.allocator.dupe(u8, key);
                    errdefer storage.allocator.free(owned_key);

                    try storage.data.put(owned_key, json_val);
                },
                else => return error.InvalidRdbFile,
            }

            keys_loaded += 1;
        }

        return keys_loaded;
    }

    /// Backward-compatible save for single database (wraps multi-DB version).
    pub fn saveSingleDb(storage: *Storage, path: []const u8, allocator: std.mem.Allocator) !void {
        var databases: [1]Storage = undefined;
        databases[0] = storage.*;
        try save(&databases, path, allocator);
    }

    /// Backward-compatible load for single database (wraps multi-DB version).
    pub fn loadSingleDb(storage: *Storage, path: []const u8, allocator: std.mem.Allocator) !usize {
        var databases: [1]Storage = undefined;
        databases[0] = storage.*;
        return load(&databases, path, allocator);
    }

    /// Backward-compatible saveToBytes for single database (wraps multi-DB version).
    pub fn saveToBytesSingleDb(storage: *Storage, allocator: std.mem.Allocator) ![]u8 {
        var databases: [1]Storage = undefined;
        databases[0] = storage.*;
        return saveToBytes(&databases, allocator);
    }

    /// Backward-compatible loadFromBytes for single database (wraps multi-DB version).
    pub fn loadFromBytesSingleDb(storage: *Storage, data: []const u8, allocator: std.mem.Allocator) !usize {
        var databases: [1]Storage = undefined;
        databases[0] = storage.*;
        return loadFromBytes(&databases, data, allocator);
    }

    /// Write a Redis-compatible length-encoded value.
    /// Uses 6-bit, 14-bit, or 32-bit format depending on value size.
    fn writeLengthEncoded(w: anytype, value: u32) !void {
        if (value < 64) {
            // 6-bit format (1 byte): 00XXXXXX
            try w.writeByte(@intCast(value));
        } else if (value < 16384) {
            // 14-bit format (2 bytes LE): 01XXXXXX XXXXXXXX
            const v = (0x40 << 8) | (value >> 8);
            try w.writeInt(u16, @intCast(v), .little);
            try w.writeByte(@intCast(value & 0xFF));
        } else {
            // 32-bit format (4 bytes LE): 10000000 XXXXXXXX XXXXXXXX XXXXXXXX XXXXXXXX
            try w.writeByte(0x80);
            try w.writeInt(u32, value, .little);
        }
    }

    /// Read a Redis-compatible length-encoded value.
    fn readLengthEncoded(data: []const u8, pos: *usize) !i64 {
        if (pos.* >= data.len) return error.InvalidRdbFile;
        const first_byte = data[pos.*];
        pos.* += 1;

        const top_bits = first_byte >> 6;
        switch (top_bits) {
            0 => {
                // 6-bit: value is in lower 6 bits
                return first_byte & 0x3F;
            },
            1 => {
                // 14-bit: lower 6 bits + next byte
                if (pos.* >= data.len) return error.InvalidRdbFile;
                const next_byte = data[pos.*];
                pos.* += 1;
                const high = @as(u16, first_byte & 0x3F) << 8;
                return high | next_byte;
            },
            2 => {
                // 32-bit: next 4 bytes LE
                if (pos.* + 4 > data.len) return error.InvalidRdbFile;
                const value = std.mem.readInt(u32, data[pos.*..][0..4], .little);
                pos.* += 4;
                return value;
            },
            else => {
                // Invalid format
                return error.InvalidRdbFile;
            },
        }
    }

    /// Write a length-prefixed blob (4-byte LE length + data)
    fn writeBlob(w: anytype, data: []const u8) !void {
        try w.writeInt(u32, @intCast(data.len), .little);
        try w.writeAll(data);
    }

    /// Read a length-prefixed blob; caller owns returned memory
    fn readBlob(data: []const u8, pos: *usize, allocator: std.mem.Allocator) ![]u8 {
        if (pos.* + 4 > data.len) return error.InvalidRdbFile;
        const len = std.mem.readInt(u32, data[pos.*..][0..4], .little);
        pos.* += 4;
        if (pos.* + len > data.len) return error.InvalidRdbFile;
        const blob = try allocator.dupe(u8, data[pos.* .. pos.* + len]);
        pos.* += len;
        return blob;
    }

    /// Skip over a value payload without allocating
    fn skipValue(data: []const u8, pos: *usize, type_byte: u8) !void {
        switch (type_byte) {
            RDB_TYPE_STRING => {
                if (pos.* + 4 > data.len) return error.InvalidRdbFile;
                const len = std.mem.readInt(u32, data[pos.*..][0..4], .little);
                pos.* += 4 + len;
            },
            RDB_TYPE_LIST, RDB_TYPE_SET => {
                if (pos.* + 4 > data.len) return error.InvalidRdbFile;
                const count = std.mem.readInt(u32, data[pos.*..][0..4], .little);
                pos.* += 4;
                for (0..count) |_| {
                    if (pos.* + 4 > data.len) return error.InvalidRdbFile;
                    const len = std.mem.readInt(u32, data[pos.*..][0..4], .little);
                    pos.* += 4 + len;
                }
            },
            RDB_TYPE_HASH => {
                if (pos.* + 4 > data.len) return error.InvalidRdbFile;
                const count = std.mem.readInt(u32, data[pos.*..][0..4], .little);
                pos.* += 4;
                for (0..count) |_| {
                    if (pos.* + 4 > data.len) return error.InvalidRdbFile;
                    const flen = std.mem.readInt(u32, data[pos.*..][0..4], .little);
                    pos.* += 4 + flen;
                    if (pos.* + 4 > data.len) return error.InvalidRdbFile;
                    const vlen = std.mem.readInt(u32, data[pos.*..][0..4], .little);
                    pos.* += 4 + vlen;
                }
            },
            RDB_TYPE_SORTED_SET => {
                if (pos.* + 4 > data.len) return error.InvalidRdbFile;
                const count = std.mem.readInt(u32, data[pos.*..][0..4], .little);
                pos.* += 4;
                for (0..count) |_| {
                    pos.* += 8; // score f64
                    if (pos.* + 4 > data.len) return error.InvalidRdbFile;
                    const mlen = std.mem.readInt(u32, data[pos.*..][0..4], .little);
                    pos.* += 4 + mlen;
                }
            },
            else => return error.InvalidRdbFile,
        }
    }

    /// Save all databases to an in-memory byte buffer.
    /// The caller owns the returned slice and must free it with `allocator`.
    pub fn saveToBytes(databases: []Storage, allocator: std.mem.Allocator) ![]u8 {
        var buf = std.ArrayList(u8){};
        errdefer buf.deinit(allocator);

        const w = buf.writer(allocator);

        try w.writeAll(RDB_MAGIC);
        try w.writeByte(RDB_VERSION);

        const now = Storage.getCurrentTimestamp();

        // Iterate all databases
        for (databases, 0..) |*storage, db_idx| {
            storage.mutex.lock();
            defer storage.mutex.unlock();

            var it = storage.data.iterator();
            var has_keys = false;

            // First pass: check if database has non-expired keys
            while (it.next()) |entry| {
                if (!entry.value_ptr.isExpired(now)) {
                    has_keys = true;
                    break;
                }
            }

            // Skip empty databases
            if (!has_keys) continue;

            // Write database selector
            try w.writeByte(RDB_DB_SELECTOR);
            try writeLengthEncoded(w, @intCast(db_idx));

            // Reset iterator for second pass: write keys
            it = storage.data.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.isExpired(now)) continue;

                const key = entry.key_ptr.*;
                const value = entry.value_ptr.*;

                const type_byte: u8 = switch (value) {
                    .string => RDB_TYPE_STRING,
                    .list => RDB_TYPE_LIST,
                    .set => RDB_TYPE_SET,
                    .hash => RDB_TYPE_HASH,
                    .sorted_set => RDB_TYPE_SORTED_SET,
                    .stream => 0xFF, // Streams not yet serialized
                    .hyperloglog => 0xFE, // HyperLogLog
                    .json => 0x0F, // JSON type
                    .timeseries => 0xFD, // Time Series type
                    .bloom => 0xFC, // Bloom Filter type
                    .cuckoo => 0xFB, // Cuckoo Filter type
                    .count_min_sketch => 0xFA, // Count-Min Sketch type
                };
                try w.writeByte(type_byte);

                const expires_at = value.getExpiration();
                if (expires_at) |exp| {
                    try w.writeByte(1);
                    try w.writeInt(i64, exp, .little);
                } else {
                    try w.writeByte(0);
                }

                try writeBlob(w, key);

                switch (value) {
                    .string => |s| try writeBlob(w, s.data),
                    .list => |l| {
                        try w.writeInt(u32, @intCast(l.data.items.len), .little);
                        for (l.data.items) |elem| try writeBlob(w, elem);
                    },
                    .set => |s| {
                        try w.writeInt(u32, @intCast(s.data.count()), .little);
                        var kit = s.data.keyIterator();
                        while (kit.next()) |k| try writeBlob(w, k.*);
                    },
                    .hash => |h| {
                        try w.writeInt(u32, @intCast(h.data.count()), .little);
                        var hit = h.data.iterator();
                        while (hit.next()) |e| {
                            try writeBlob(w, e.key_ptr.*);
                            try writeBlob(w, e.value_ptr.*.data);
                        }
                    },
                    .sorted_set => |z| {
                        try w.writeInt(u32, @intCast(z.sorted_list.items.len), .little);
                        for (z.sorted_list.items) |scored| {
                            const score_bits = @as(u64, @bitCast(scored.score));
                            try w.writeInt(u64, score_bits, .little);
                            try writeBlob(w, scored.member);
                        }
                    },
                    .stream => {
                        // Streams not yet implemented - write empty marker
                        try w.writeInt(u32, 0, .little);
                    },
                    .hyperloglog => |hll| {
                        try writeBlob(w, &hll.registers);
                    },
                    .json => |j| {
                        // Serialize JSON to string
                        const json_str = try j.root.stringify(j.allocator);
                        defer j.allocator.free(json_str);
                        try writeBlob(w, json_str);
                    },
                    .timeseries => {
                        // Time series not yet implemented in persistence
                        try w.writeInt(u32, 0, .little);
                    },
                    .bloom => {
                        // Bloom filter not yet implemented in persistence
                        try w.writeInt(u32, 0, .little);
                    },
                    .cuckoo => {
                        // Cuckoo filter not yet implemented in persistence
                        try w.writeInt(u32, 0, .little);
                    },
                    .count_min_sketch => {
                        // Count-Min Sketch not yet implemented in persistence
                        try w.writeInt(u32, 0, .little);
                    },
                }
            }
        }

        try w.writeByte(RDB_TYPE_EOF);

        const crc = std.hash.Crc32.hash(buf.items);
        var crc_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &crc_bytes, crc, .little);
        try buf.appendSlice(allocator, &crc_bytes);

        return buf.toOwnedSlice(allocator);
    }

    /// Load a snapshot from raw bytes into all databases.
    /// Returns number of keys loaded.
    pub fn loadFromBytes(databases: []Storage, data: []const u8, allocator: std.mem.Allocator) !usize {
        if (data.len < RDB_MAGIC.len + 1 + 4) return error.InvalidRdbFile;

        // Verify checksum (last 4 bytes)
        const payload = data[0 .. data.len - 4];
        const stored_crc = std.mem.readInt(u32, data[data.len - 4 ..][0..4], .little);
        const computed_crc = std.hash.Crc32.hash(payload);
        if (stored_crc != computed_crc) return error.RdbChecksumMismatch;

        var pos: usize = 0;

        if (!std.mem.eql(u8, payload[pos .. pos + RDB_MAGIC.len], RDB_MAGIC)) {
            return error.InvalidRdbFile;
        }
        pos += RDB_MAGIC.len;

        const version = payload[pos];
        pos += 1;
        if (version != RDB_VERSION) return error.UnsupportedRdbVersion;

        const now = Storage.getCurrentTimestamp();
        var keys_loaded: usize = 0;
        var current_db: usize = 0;

        while (pos < payload.len) {
            if (pos >= payload.len) break;
            const type_byte = payload[pos];
            pos += 1;

            // Check for database selector
            if (type_byte == RDB_DB_SELECTOR) {
                const db_idx = try readLengthEncoded(payload, &pos);
                if (db_idx < 0 or db_idx >= databases.len) {
                    return error.InvalidDatabaseIndex;
                }
                current_db = @intCast(db_idx);
                continue;
            }

            if (type_byte == RDB_TYPE_EOF) break;

            if (pos >= payload.len) return error.InvalidRdbFile;
            const expires_flag = payload[pos];
            pos += 1;
            var expires_at: ?i64 = null;
            if (expires_flag == 1) {
                if (pos + 8 > payload.len) return error.InvalidRdbFile;
                const exp = std.mem.readInt(i64, payload[pos..][0..8], .little);
                pos += 8;
                if (exp <= now) {
                    if (pos + 4 > payload.len) return error.InvalidRdbFile;
                    const key_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
                    pos += 4 + key_len;
                    try skipValue(payload, &pos, type_byte);
                    continue;
                }
                expires_at = exp;
            }

            const key = try readBlob(payload, &pos, allocator);
            defer allocator.free(key);

            const storage = &databases[current_db];
            switch (type_byte) {
                RDB_TYPE_STRING => {
                    const val = try readBlob(payload, &pos, allocator);
                    defer allocator.free(val);
                    try storage.set(key, val, expires_at);
                },
                RDB_TYPE_LIST => {
                    if (pos + 4 > payload.len) return error.InvalidRdbFile;
                    const count = std.mem.readInt(u32, payload[pos..][0..4], .little);
                    pos += 4;
                    var elems = try allocator.alloc([]const u8, count);
                    defer {
                        for (elems) |e| allocator.free(e);
                        allocator.free(elems);
                    }
                    for (0..count) |i| {
                        elems[i] = try readBlob(payload, &pos, allocator);
                    }
                    _ = try storage.rpush(key, elems, expires_at);
                },
                RDB_TYPE_SET => {
                    if (pos + 4 > payload.len) return error.InvalidRdbFile;
                    const count = std.mem.readInt(u32, payload[pos..][0..4], .little);
                    pos += 4;
                    var members = try allocator.alloc([]const u8, count);
                    defer {
                        for (members) |m| allocator.free(m);
                        allocator.free(members);
                    }
                    for (0..count) |i| {
                        members[i] = try readBlob(payload, &pos, allocator);
                    }
                    _ = try storage.sadd(key, members, expires_at);
                },
                RDB_TYPE_HASH => {
                    if (pos + 4 > payload.len) return error.InvalidRdbFile;
                    const count = std.mem.readInt(u32, payload[pos..][0..4], .little);
                    pos += 4;
                    var i: u32 = 0;
                    while (i < count) : (i += 1) {
                        const field = try readBlob(payload, &pos, allocator);
                        defer allocator.free(field);
                        const hval = try readBlob(payload, &pos, allocator);
                        defer allocator.free(hval);
                        var fields_arr = [_][]const u8{field};
                        var values_arr = [_][]const u8{hval};
                        _ = try storage.hset(key, &fields_arr, &values_arr, expires_at);
                    }
                },
                RDB_TYPE_SORTED_SET => {
                    if (pos + 4 > payload.len) return error.InvalidRdbFile;
                    const count = std.mem.readInt(u32, payload[pos..][0..4], .little);
                    pos += 4;
                    var i: u32 = 0;
                    while (i < count) : (i += 1) {
                        if (pos + 8 > payload.len) return error.InvalidRdbFile;
                        const score_bits = std.mem.readInt(u64, payload[pos..][0..8], .little);
                        const score: f64 = @bitCast(score_bits);
                        pos += 8;
                        const member = try readBlob(payload, &pos, allocator);
                        defer allocator.free(member);
                        var scores_arr = [_]f64{score};
                        var members_arr = [_][]const u8{member};
                        _ = try storage.zadd(key, &scores_arr, &members_arr, 0, expires_at);
                    }
                },
                else => return error.InvalidRdbFile,
            }

            keys_loaded += 1;
        }

        return keys_loaded;
    }
};

// ============================================================
// Unit Tests
// ============================================================

test "persistence - save and load strings" {
    const allocator = std.testing.allocator;

    const storage = try Storage.init(allocator);
    defer storage.deinit();

    try storage.set("hello", "world", null);
    try storage.set("foo", "bar", null);

    const tmp_path = "/tmp/zoltraak_test_rdb.rdb";
    try Persistence.saveSingleDb(storage, tmp_path, allocator);

    const storage2 = try Storage.init(allocator);
    defer storage2.deinit();

    const count = try Persistence.loadSingleDb(storage2, tmp_path, allocator);
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectEqualStrings("world", storage2.get("hello").?);
    try std.testing.expectEqualStrings("bar", storage2.get("foo").?);

    std.fs.cwd().deleteFile(tmp_path) catch {};
}

test "persistence - save and load with expiration" {
    const allocator = std.testing.allocator;

    const storage = try Storage.init(allocator);
    defer storage.deinit();

    // Key with future expiration
    const future_exp = Storage.getCurrentTimestamp() + 60000; // 60s from now
    try storage.set("expiring", "value", future_exp);

    // Key already expired (should be skipped on save)
    const past_exp = Storage.getCurrentTimestamp() - 1000;
    try storage.set("expired", "gone", past_exp);

    const tmp_path = "/tmp/zoltraak_test_rdb_exp.rdb";
    try Persistence.saveSingleDb(storage, tmp_path, allocator);

    const storage2 = try Storage.init(allocator);
    defer storage2.deinit();

    const count = try Persistence.loadSingleDb(storage2, tmp_path, allocator);
    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqualStrings("value", storage2.get("expiring").?);
    try std.testing.expect(storage2.get("expired") == null);

    std.fs.cwd().deleteFile(tmp_path) catch {};
}

test "persistence - load nonexistent file returns 0" {
    const allocator = std.testing.allocator;

    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const count = try Persistence.loadSingleDb(storage, "/tmp/does_not_exist_zoltraak.rdb", allocator);
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "persistence - save and load list" {
    const allocator = std.testing.allocator;

    const storage = try Storage.init(allocator);
    defer storage.deinit();

    const elems = [_][]const u8{ "a", "b", "c" };
    _ = try storage.rpush("mylist", &elems, null);

    const tmp_path = "/tmp/zoltraak_test_rdb_list.rdb";
    try Persistence.saveSingleDb(storage, tmp_path, allocator);

    const storage2 = try Storage.init(allocator);
    defer storage2.deinit();

    const count = try Persistence.loadSingleDb(storage2, tmp_path, allocator);
    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(usize, 3), storage2.llen("mylist").?);

    std.fs.cwd().deleteFile(tmp_path) catch {};
}

test "persistence - save and load hash" {
    const allocator = std.testing.allocator;

    const storage = try Storage.init(allocator);
    defer storage.deinit();

    var fields = [_][]const u8{"name"};
    var values = [_][]const u8{"Alice"};
    _ = try storage.hset("user:1", &fields, &values, null);

    const tmp_path = "/tmp/zoltraak_test_rdb_hash.rdb";
    try Persistence.saveSingleDb(storage, tmp_path, allocator);

    const storage2 = try Storage.init(allocator);
    defer storage2.deinit();

    const count = try Persistence.loadSingleDb(storage2, tmp_path, allocator);
    try std.testing.expectEqual(@as(usize, 1), count);
    const loaded_val = storage2.hget("user:1", "name");
    try std.testing.expect(loaded_val != null);
    try std.testing.expectEqualStrings("Alice", loaded_val.?);

    std.fs.cwd().deleteFile(tmp_path) catch {};
}

test "persistence - save and load set" {
    const allocator = std.testing.allocator;

    const storage = try Storage.init(allocator);
    defer storage.deinit();

    var members = [_][]const u8{ "x", "y", "z" };
    _ = try storage.sadd("myset", &members, null);

    const tmp_path = "/tmp/zoltraak_test_rdb_set.rdb";
    try Persistence.saveSingleDb(storage, tmp_path, allocator);

    const storage2 = try Storage.init(allocator);
    defer storage2.deinit();

    const count = try Persistence.loadSingleDb(storage2, tmp_path, allocator);
    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(usize, 3), storage2.scard("myset").?);

    std.fs.cwd().deleteFile(tmp_path) catch {};
}

test "persistence - save and load sorted set" {
    const allocator = std.testing.allocator;

    const storage = try Storage.init(allocator);
    defer storage.deinit();

    var scores = [_]f64{ 1.0, 2.0, 3.0 };
    var members = [_][]const u8{ "one", "two", "three" };
    _ = try storage.zadd("myzset", &scores, &members, 0, null);

    const tmp_path = "/tmp/zoltraak_test_rdb_zset.rdb";
    try Persistence.saveSingleDb(storage, tmp_path, allocator);

    const storage2 = try Storage.init(allocator);
    defer storage2.deinit();

    const count = try Persistence.loadSingleDb(storage2, tmp_path, allocator);
    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(@as(usize, 3), storage2.zcard("myzset").?);
    try std.testing.expect(storage2.zscore("myzset", "two") != null);
    try std.testing.expectEqual(@as(f64, 2.0), storage2.zscore("myzset", "two").?);

    std.fs.cwd().deleteFile(tmp_path) catch {};
}
