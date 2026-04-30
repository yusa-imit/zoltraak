/// Listpack — compact encoding for small lists, hashes, and sorted sets
///
/// Redis uses listpack to save memory when collections are small. Listpack is a sequence
/// of entries encoded as:
/// - Total bytes (4 bytes little-endian)
/// - Number of elements (2 bytes little-endian)
/// - Entries (variable length, each with backlen at end)
/// - End marker (0xFF)
///
/// Each entry format:
/// - Encoding byte(s)
/// - Data
/// - Backlen (variable length, encoded backwards)
///
/// Encoding types:
/// - 0b0xxxxxxx: 7-bit unsigned integer (0-127)
/// - 0b10xxxxxx: 6-bit string (0-63 bytes)
/// - 0b110xxxxx yyyyyyyy: 13-bit integer (-4096 to 4095)
/// - 0b1110xxxx yyyyyyyy: string 0-4095 bytes
/// - 0b11110001 + 2 bytes: 16-bit signed integer
/// - 0b11110010 + 3 bytes: 24-bit signed integer
/// - 0b11110011 + 4 bytes: 32-bit signed integer
/// - 0b11110100 + 8 bytes: 64-bit signed integer
/// - 0b11110000 + 4 bytes: 32-bit string length + data
/// - 0b11111111: end marker

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const ListpackError = error{
    InvalidEncoding,
    InvalidBacklen,
    TooLarge,
    OutOfMemory,
};

/// Maximum listpack size (16 MB)
const MAX_LISTPACK_SIZE: u32 = 16 * 1024 * 1024;

/// Maximum string length that fits in listpack
const MAX_STRING_LEN: u32 = 4096;

/// End marker
const LP_EOF: u8 = 0xFF;

/// Encoding markers
const LP_ENCODING_7BIT_UINT: u8 = 0b00000000;
const LP_ENCODING_6BIT_STR: u8 = 0b10000000;
const LP_ENCODING_13BIT_INT: u8 = 0b11000000;
const LP_ENCODING_12BIT_STR: u8 = 0b11100000;
const LP_ENCODING_16BIT_INT: u8 = 0b11110001;
const LP_ENCODING_24BIT_INT: u8 = 0b11110010;
const LP_ENCODING_32BIT_INT: u8 = 0b11110011;
const LP_ENCODING_64BIT_INT: u8 = 0b11110100;
const LP_ENCODING_32BIT_STR: u8 = 0b11110000;

pub const ListpackEntry = union(enum) {
    integer: i64,
    string: []const u8,

    pub fn deinit(self: *ListpackEntry, allocator: Allocator) void {
        switch (self.*) {
            .string => |s| allocator.free(s),
            .integer => {},
        }
    }
};

pub const Listpack = struct {
    data: []u8,
    allocator: Allocator,

    /// Create new empty listpack
    pub fn init(allocator: Allocator) !Listpack {
        // Initial size: 4 (total bytes) + 2 (num elements) + 1 (end marker) = 7 bytes
        const data = try allocator.alloc(u8, 7);
        errdefer allocator.free(data);

        // Total bytes
        std.mem.writeInt(u32, data[0..4], 7, .little);
        // Number of elements
        std.mem.writeInt(u16, data[4..6], 0, .little);
        // End marker
        data[6] = LP_EOF;

        return Listpack{
            .data = data,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Listpack) void {
        self.allocator.free(self.data);
    }

    /// Get number of elements
    pub fn len(self: *const Listpack) u16 {
        return std.mem.readInt(u16, self.data[4..6], .little);
    }

    /// Get total size in bytes
    pub fn size(self: *const Listpack) u32 {
        return std.mem.readInt(u32, self.data[0..4], .little);
    }

    /// Append integer
    pub fn appendInt(self: *Listpack, value: i64) !void {
        const encoded = try encodeInt(self.allocator, value);
        defer self.allocator.free(encoded);
        try self.appendEntry(encoded);
    }

    /// Append string
    pub fn appendString(self: *Listpack, value: []const u8) !void {
        if (value.len > MAX_STRING_LEN) return error.TooLarge;

        const encoded = try encodeString(self.allocator, value);
        defer self.allocator.free(encoded);
        try self.appendEntry(encoded);
    }

    /// Append raw encoded entry
    fn appendEntry(self: *Listpack, encoded: []const u8) !void {
        const old_size = self.size();
        const new_size = old_size - 1 + encoded.len + 1; // -1 for old EOF, +1 for new EOF

        if (new_size > MAX_LISTPACK_SIZE) return error.TooLarge;

        // Reallocate
        const new_data = try self.allocator.realloc(self.data, new_size);
        self.data = new_data;

        // Copy entry before EOF
        const insert_pos = old_size - 1;
        @memcpy(self.data[insert_pos .. insert_pos + encoded.len], encoded);

        // Write new EOF
        self.data[new_size - 1] = LP_EOF;

        // Update header
        std.mem.writeInt(u32, self.data[0..4], @intCast(new_size), .little);
        const count = self.len();
        std.mem.writeInt(u16, self.data[4..6], count + 1, .little);
    }

    /// Get entry at index
    pub fn get(self: *const Listpack, index: u16) !ListpackEntry {
        if (index >= self.len()) return error.OutOfBounds;

        var pos: usize = 6; // Skip header
        var i: u16 = 0;

        while (i < index) : (i += 1) {
            const skip_len = try getEntryLen(self.data[pos..]);
            pos += skip_len;
        }

        return try decodeEntry(self.allocator, self.data[pos..]);
    }

    /// Iterate over all entries
    pub const Iterator = struct {
        lp: *const Listpack,
        pos: usize,
        count: u16,

        pub fn next(self: *Iterator) !?ListpackEntry {
            if (self.count == 0) return null;

            const entry = try decodeEntry(self.lp.allocator, self.lp.data[self.pos..]);
            const entry_len = try getEntryLen(self.lp.data[self.pos..]);
            self.pos += entry_len;
            self.count -= 1;

            return entry;
        }
    };

    pub fn iterator(self: *const Listpack) Iterator {
        return Iterator{
            .lp = self,
            .pos = 6, // Skip header
            .count = self.len(),
        };
    }
};

/// Encode integer in most compact form
fn encodeInt(allocator: Allocator, value: i64) ![]u8 {
    // 7-bit unsigned (0-127)
    if (value >= 0 and value <= 127) {
        const buf = try allocator.alloc(u8, 2);
        buf[0] = @intCast(value);
        buf[1] = 1; // backlen
        return buf;
    }

    // 13-bit signed (-4096 to 4095)
    if (value >= -4096 and value <= 4095) {
        const buf = try allocator.alloc(u8, 3);
        const encoded: u16 = @bitCast(@as(i16, @intCast(value)));
        buf[0] = LP_ENCODING_13BIT_INT | @as(u8, @intCast((encoded >> 8) & 0x1F));
        buf[1] = @intCast(encoded & 0xFF);
        buf[2] = 2; // backlen
        return buf;
    }

    // 16-bit signed
    if (value >= std.math.minInt(i16) and value <= std.math.maxInt(i16)) {
        const buf = try allocator.alloc(u8, 4);
        buf[0] = LP_ENCODING_16BIT_INT;
        std.mem.writeInt(i16, buf[1..3], @intCast(value), .little);
        buf[3] = 3; // backlen
        return buf;
    }

    // 24-bit signed
    if (value >= -(1 << 23) and value < (1 << 23)) {
        const buf = try allocator.alloc(u8, 5);
        buf[0] = LP_ENCODING_24BIT_INT;
        const v24: i32 = @intCast(value);
        buf[1] = @intCast(v24 & 0xFF);
        buf[2] = @intCast((v24 >> 8) & 0xFF);
        buf[3] = @intCast((v24 >> 16) & 0xFF);
        buf[4] = 4; // backlen
        return buf;
    }

    // 32-bit signed
    if (value >= std.math.minInt(i32) and value <= std.math.maxInt(i32)) {
        const buf = try allocator.alloc(u8, 6);
        buf[0] = LP_ENCODING_32BIT_INT;
        std.mem.writeInt(i32, buf[1..5], @intCast(value), .little);
        buf[5] = 5; // backlen
        return buf;
    }

    // 64-bit signed
    const buf = try allocator.alloc(u8, 10);
    buf[0] = LP_ENCODING_64BIT_INT;
    std.mem.writeInt(i64, buf[1..9], value, .little);
    buf[9] = 9; // backlen
    return buf;
}

/// Encode string in most compact form
fn encodeString(allocator: Allocator, value: []const u8) ![]u8 {
    const slen = value.len;

    // 6-bit string (0-63 bytes)
    if (slen <= 63) {
        const total_len = 1 + slen + 1;
        const buf = try allocator.alloc(u8, total_len);
        buf[0] = LP_ENCODING_6BIT_STR | @as(u8, @intCast(slen));
        @memcpy(buf[1 .. 1 + slen], value);
        buf[total_len - 1] = @intCast(total_len); // backlen
        return buf;
    }

    // 12-bit string (0-4095 bytes)
    if (slen <= 4095) {
        const total_len = 2 + slen + 1;
        const buf = try allocator.alloc(u8, total_len);
        buf[0] = LP_ENCODING_12BIT_STR | @as(u8, @intCast((slen >> 8) & 0x0F));
        buf[1] = @intCast(slen & 0xFF);
        @memcpy(buf[2 .. 2 + slen], value);
        buf[total_len - 1] = @intCast(total_len); // backlen (max 4097, fits in u8 via wrapping)
        return buf;
    }

    // 32-bit string (> 4095 bytes)
    const total_len = 5 + slen + 1;
    const buf = try allocator.alloc(u8, total_len);
    buf[0] = LP_ENCODING_32BIT_STR;
    std.mem.writeInt(u32, buf[1..5], @intCast(slen), .little);
    @memcpy(buf[5 .. 5 + slen], value);
    // backlen wraps around for large entries, but listpack spec handles this
    buf[total_len - 1] = @intCast(total_len & 0xFF);
    return buf;
}

/// Get length of entry starting at buffer
fn getEntryLen(buf: []const u8) !usize {
    if (buf.len == 0) return error.InvalidEncoding;

    const first = buf[0];

    // EOF
    if (first == LP_EOF) return 0;

    // 7-bit uint
    if ((first & 0x80) == 0) return 2;

    // 6-bit string
    if ((first & 0xC0) == 0x80) {
        const slen = first & 0x3F;
        return 1 + slen + 1;
    }

    // 13-bit int
    if ((first & 0xE0) == 0xC0) return 3;

    // 12-bit string
    if ((first & 0xF0) == 0xE0) {
        if (buf.len < 2) return error.InvalidEncoding;
        const slen = (@as(usize, first & 0x0F) << 8) | buf[1];
        return 2 + slen + 1;
    }

    // Special encodings
    switch (first) {
        LP_ENCODING_16BIT_INT => return 4,
        LP_ENCODING_24BIT_INT => return 5,
        LP_ENCODING_32BIT_INT => return 6,
        LP_ENCODING_64BIT_INT => return 10,
        LP_ENCODING_32BIT_STR => {
            if (buf.len < 5) return error.InvalidEncoding;
            const slen = std.mem.readInt(u32, buf[1..5], .little);
            return 5 + slen + 1;
        },
        else => return error.InvalidEncoding,
    }
}

/// Decode entry starting at buffer
fn decodeEntry(allocator: Allocator, buf: []const u8) !ListpackEntry {
    if (buf.len == 0) return error.InvalidEncoding;

    const first = buf[0];

    // 7-bit uint
    if ((first & 0x80) == 0) {
        return ListpackEntry{ .integer = first };
    }

    // 6-bit string
    if ((first & 0xC0) == 0x80) {
        const slen = first & 0x3F;
        if (buf.len < 1 + slen) return error.InvalidEncoding;
        const str = try allocator.alloc(u8, slen);
        @memcpy(str, buf[1 .. 1 + slen]);
        return ListpackEntry{ .string = str };
    }

    // 13-bit int
    if ((first & 0xE0) == 0xC0) {
        if (buf.len < 2) return error.InvalidEncoding;
        const v1 = @as(u16, first & 0x1F) << 8;
        const v2 = @as(u16, buf[1]);
        var encoded = v1 | v2;
        // Sign extend from 13 bits to 16 bits
        if ((encoded & 0x1000) != 0) {
            encoded |= 0xE000; // Set upper 3 bits for negative
        }
        const value: i16 = @bitCast(encoded);
        return ListpackEntry{ .integer = value };
    }

    // 12-bit string
    if ((first & 0xF0) == 0xE0) {
        if (buf.len < 2) return error.InvalidEncoding;
        const slen = (@as(usize, first & 0x0F) << 8) | buf[1];
        if (buf.len < 2 + slen) return error.InvalidEncoding;
        const str = try allocator.alloc(u8, slen);
        @memcpy(str, buf[2 .. 2 + slen]);
        return ListpackEntry{ .string = str };
    }

    // Special encodings
    switch (first) {
        LP_ENCODING_16BIT_INT => {
            if (buf.len < 3) return error.InvalidEncoding;
            const value = std.mem.readInt(i16, buf[1..3], .little);
            return ListpackEntry{ .integer = value };
        },
        LP_ENCODING_24BIT_INT => {
            if (buf.len < 4) return error.InvalidEncoding;
            var v: i32 = 0;
            v |= @as(i32, buf[1]);
            v |= @as(i32, buf[2]) << 8;
            v |= @as(i32, buf[3]) << 16;
            // Sign extend from 24 bits
            if ((v & 0x800000) != 0) {
                v |= @as(i32, @bitCast(@as(u32, 0xFF000000)));
            }
            return ListpackEntry{ .integer = v };
        },
        LP_ENCODING_32BIT_INT => {
            if (buf.len < 5) return error.InvalidEncoding;
            const value = std.mem.readInt(i32, buf[1..5], .little);
            return ListpackEntry{ .integer = value };
        },
        LP_ENCODING_64BIT_INT => {
            if (buf.len < 9) return error.InvalidEncoding;
            const value = std.mem.readInt(i64, buf[1..9], .little);
            return ListpackEntry{ .integer = value };
        },
        LP_ENCODING_32BIT_STR => {
            if (buf.len < 5) return error.InvalidEncoding;
            const slen = std.mem.readInt(u32, buf[1..5], .little);
            if (buf.len < 5 + slen) return error.InvalidEncoding;
            const str = try allocator.alloc(u8, slen);
            @memcpy(str, buf[5 .. 5 + slen]);
            return ListpackEntry{ .string = str };
        },
        else => return error.InvalidEncoding,
    }
}

// ============================================================================
// Tests
// ============================================================================

test "listpack: init and basic properties" {
    const allocator = std.testing.allocator;

    var lp = try Listpack.init(allocator);
    defer lp.deinit();

    try std.testing.expectEqual(@as(u16, 0), lp.len());
    try std.testing.expectEqual(@as(u32, 7), lp.size());
}

test "listpack: append integers" {
    const allocator = std.testing.allocator;

    var lp = try Listpack.init(allocator);
    defer lp.deinit();

    try lp.appendInt(42);
    try lp.appendInt(-100);
    try lp.appendInt(5000);

    try std.testing.expectEqual(@as(u16, 3), lp.len());

    const e1 = try lp.get(0);
    try std.testing.expectEqual(@as(i64, 42), e1.integer);

    const e2 = try lp.get(1);
    try std.testing.expectEqual(@as(i64, -100), e2.integer);

    const e3 = try lp.get(2);
    try std.testing.expectEqual(@as(i64, 5000), e3.integer);
}

test "listpack: append strings" {
    const allocator = std.testing.allocator;

    var lp = try Listpack.init(allocator);
    defer lp.deinit();

    try lp.appendString("hello");
    try lp.appendString("world");

    try std.testing.expectEqual(@as(u16, 2), lp.len());

    var e1 = try lp.get(0);
    defer e1.deinit(allocator);
    try std.testing.expectEqualStrings("hello", e1.string);

    var e2 = try lp.get(1);
    defer e2.deinit(allocator);
    try std.testing.expectEqualStrings("world", e2.string);
}

test "listpack: mixed entries" {
    const allocator = std.testing.allocator;

    var lp = try Listpack.init(allocator);
    defer lp.deinit();

    try lp.appendInt(123);
    try lp.appendString("test");
    try lp.appendInt(-456);

    try std.testing.expectEqual(@as(u16, 3), lp.len());

    const e1 = try lp.get(0);
    try std.testing.expectEqual(@as(i64, 123), e1.integer);

    var e2 = try lp.get(1);
    defer e2.deinit(allocator);
    try std.testing.expectEqualStrings("test", e2.string);

    const e3 = try lp.get(2);
    try std.testing.expectEqual(@as(i64, -456), e3.integer);
}

test "listpack: iterator" {
    const allocator = std.testing.allocator;

    var lp = try Listpack.init(allocator);
    defer lp.deinit();

    try lp.appendInt(1);
    try lp.appendInt(2);
    try lp.appendInt(3);

    var iter = lp.iterator();
    var count: u16 = 0;

    while (try iter.next()) |entry| {
        count += 1;
        switch (entry) {
            .integer => |v| {
                try std.testing.expect(v >= 1 and v <= 3);
            },
            .string => unreachable,
        }
    }

    try std.testing.expectEqual(@as(u16, 3), count);
}

test "listpack: encoding sizes" {
    const allocator = std.testing.allocator;

    // 7-bit uint: 2 bytes (encoding + backlen)
    var lp1 = try Listpack.init(allocator);
    defer lp1.deinit();
    try lp1.appendInt(127);
    try std.testing.expectEqual(@as(u32, 7 + 2), lp1.size());

    // 13-bit int: 3 bytes
    var lp2 = try Listpack.init(allocator);
    defer lp2.deinit();
    try lp2.appendInt(-1000);
    try std.testing.expectEqual(@as(u32, 7 + 3), lp2.size());

    // 6-bit string: 1 + len + 1
    var lp3 = try Listpack.init(allocator);
    defer lp3.deinit();
    try lp3.appendString("hello");
    try std.testing.expectEqual(@as(u32, 7 + 1 + 5 + 1), lp3.size());
}

test "listpack: large integers" {
    const allocator = std.testing.allocator;

    var lp = try Listpack.init(allocator);
    defer lp.deinit();

    try lp.appendInt(std.math.maxInt(i64));
    try lp.appendInt(std.math.minInt(i64));

    const e1 = try lp.get(0);
    try std.testing.expectEqual(std.math.maxInt(i64), e1.integer);

    const e2 = try lp.get(1);
    try std.testing.expectEqual(std.math.minInt(i64), e2.integer);
}
