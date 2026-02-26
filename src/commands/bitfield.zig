const std = @import("std");
const Allocator = std.mem.Allocator;
const Storage = @import("../storage/memory.zig").Storage;
const writer_mod = @import("../protocol/writer.zig");
const Writer = writer_mod.Writer;
const RespValue = @import("../protocol/parser.zig").RespValue;

/// Bitfield type encoding
const BitfieldType = struct {
    signed: bool,
    bits: u6, // 1-64 bits
};

/// Bitfield overflow behavior
const OverflowBehavior = enum {
    wrap, // Wrap around on overflow (default)
    sat, // Saturate at min/max values
    fail, // Return nil on overflow
};

/// Parse bitfield type string (e.g., "i8", "u16", "i63")
fn parseBitfieldType(type_str: []const u8) !BitfieldType {
    if (type_str.len < 2) return error.InvalidType;

    const signed = switch (type_str[0]) {
        'i' => true,
        'u' => false,
        else => return error.InvalidType,
    };

    const bits_str = type_str[1..];
    const bits = std.fmt.parseInt(u6, bits_str, 10) catch return error.InvalidType;

    if (bits == 0 or bits > 64) return error.InvalidType;

    return BitfieldType{ .signed = signed, .bits = bits };
}

/// Get bytes needed to access bitfield at offset with given width
fn getBytesNeeded(offset: usize, bits: u6) usize {
    const bit_end = offset + bits;
    const byte_end = (bit_end + 7) / 8;
    return byte_end;
}

/// Read a bitfield value from a byte slice
fn readBitfield(data: []const u8, offset: usize, bf_type: BitfieldType) i64 {
    var result: u64 = 0;
    const bit_end = offset + bf_type.bits;

    var bit_idx = offset;
    while (bit_idx < bit_end) : (bit_idx += 1) {
        const byte_idx = bit_idx / 8;
        const bit_pos: u3 = @intCast(bit_idx % 8);

        if (byte_idx < data.len) {
            const byte_val = data[byte_idx];
            const bit_val: u1 = @intCast((byte_val >> @intCast(7 - bit_pos)) & 1);
            result = (result << 1) | bit_val;
        } else {
            result = result << 1; // Out of bounds bits are 0
        }
    }

    // Handle signed conversion
    if (bf_type.signed and bf_type.bits > 0 and bf_type.bits < 64) {
        const sign_bit_mask: u64 = @as(u64, 1) << @intCast(bf_type.bits - 1);
        if (result & sign_bit_mask != 0) {
            // Negative: sign-extend
            const shift_amt: u7 = @intCast(@as(u8, 64) - @as(u8, bf_type.bits));
            const extend_mask = (@as(u64, 1) << @intCast(shift_amt)) - 1;
            result |= extend_mask << @intCast(bf_type.bits);
        }
    }

    return @bitCast(result);
}

/// Write a bitfield value to a byte slice
fn writeBitfield(data: []u8, offset: usize, bf_type: BitfieldType, value: i64) void {
    const uvalue: u64 = @bitCast(value);
    const bit_end = offset + bf_type.bits;

    var bit_idx = offset;
    while (bit_idx < bit_end) : (bit_idx += 1) {
        const byte_idx = bit_idx / 8;
        const bit_pos: u3 = @intCast(bit_idx % 8);

        if (byte_idx < data.len) {
            const shift_amount = @as(u6, @intCast(bf_type.bits - 1 - (bit_idx - offset)));
            const bit_val: u1 = @intCast((uvalue >> shift_amount) & 1);

            const mask: u8 = ~(@as(u8, 1) << @intCast(7 - bit_pos));
            data[byte_idx] = (data[byte_idx] & mask) | (@as(u8, bit_val) << @intCast(7 - bit_pos));
        }
    }
}

/// Check if incrementing would overflow
fn checkOverflow(current: i64, delta: i64, bf_type: BitfieldType, overflow: OverflowBehavior) struct { value: i64, overflow_occurred: bool } {
    const max_val: i64 = if (bf_type.signed)
        (@as(i64, 1) << @intCast(bf_type.bits - 1)) - 1
    else
        (@as(i64, 1) << @intCast(bf_type.bits)) - 1;

    const min_val: i64 = if (bf_type.signed)
        -(@as(i64, 1) << @intCast(bf_type.bits - 1))
    else
        0;

    // Check for overflow
    const result_checked = @addWithOverflow(current, delta);
    var result = result_checked[0];
    const did_overflow = result_checked[1] != 0 or result > max_val or result < min_val;

    if (did_overflow) {
        switch (overflow) {
            .fail => return .{ .value = 0, .overflow_occurred = true },
            .sat => {
                if (result > max_val) result = max_val;
                if (result < min_val) result = min_val;
            },
            .wrap => {
                // Mask to bitfield size
                const mask: u64 = (@as(u64, 1) << @intCast(bf_type.bits)) - 1;
                result = @bitCast(@as(u64, @bitCast(result)) & mask);

                // Sign-extend if signed
                if (bf_type.signed and bf_type.bits < 64) {
                    const sign_bit_mask: u64 = @as(u64, 1) << @intCast(bf_type.bits - 1);
                    if ((@as(u64, @bitCast(result)) & sign_bit_mask) != 0) {
                        const shift_amt: u7 = @intCast(@as(u8, 64) - @as(u8, bf_type.bits));
                        const extend_mask = (@as(u64, 1) << @intCast(shift_amt)) - 1;
                        result = @bitCast(@as(u64, @bitCast(result)) | (extend_mask << @intCast(bf_type.bits)));
                    }
                }
            },
        }
    }

    return .{ .value = result, .overflow_occurred = false };
}

/// BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW WRAP|SAT|FAIL]
/// Perform arbitrary bitfield integer operations on strings
pub fn cmdBitfield(
    allocator: Allocator,
    storage: *Storage,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'bitfield' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse operations
    var operations = std.ArrayList(Operation){};
    defer operations.deinit(allocator);

    var overflow = OverflowBehavior.wrap;
    var i: usize = 2;

    while (i < args.len) {
        const op_str = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid operation"),
        };

        const op_upper = try std.ascii.allocUpperString(allocator, op_str);
        defer allocator.free(op_upper);

        if (std.mem.eql(u8, op_upper, "OVERFLOW")) {
            i += 1;
            if (i >= args.len) return w.writeError("ERR OVERFLOW requires an argument");

            const overflow_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid OVERFLOW argument"),
            };

            const overflow_upper = try std.ascii.allocUpperString(allocator, overflow_str);
            defer allocator.free(overflow_upper);

            if (std.mem.eql(u8, overflow_upper, "WRAP")) {
                overflow = .wrap;
            } else if (std.mem.eql(u8, overflow_upper, "SAT")) {
                overflow = .sat;
            } else if (std.mem.eql(u8, overflow_upper, "FAIL")) {
                overflow = .fail;
            } else {
                return w.writeError("ERR OVERFLOW must be WRAP, SAT, or FAIL");
            }
            i += 1;
        } else if (std.mem.eql(u8, op_upper, "GET")) {
            i += 1;
            if (i + 1 >= args.len) return w.writeError("ERR GET requires type and offset");

            const type_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid type"),
            };
            i += 1;

            const offset_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid offset"),
            };
            i += 1;

            const bf_type = parseBitfieldType(type_str) catch {
                return w.writeError("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 and i64 are supported as well.");
            };

            const offset = std.fmt.parseInt(usize, offset_str, 10) catch {
                return w.writeError("ERR bit offset is not an integer or out of range");
            };

            try operations.append(allocator, Operation{
                .get = .{ .bf_type = bf_type, .offset = offset },
            });
        } else if (std.mem.eql(u8, op_upper, "SET")) {
            i += 1;
            if (i + 2 >= args.len) return w.writeError("ERR SET requires type, offset, and value");

            const type_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid type"),
            };
            i += 1;

            const offset_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid offset"),
            };
            i += 1;

            const value_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid value"),
            };
            i += 1;

            const bf_type = parseBitfieldType(type_str) catch {
                return w.writeError("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 and i64 are supported as well.");
            };

            const offset = std.fmt.parseInt(usize, offset_str, 10) catch {
                return w.writeError("ERR bit offset is not an integer or out of range");
            };

            const value = std.fmt.parseInt(i64, value_str, 10) catch {
                return w.writeError("ERR bit value is not an integer or out of range");
            };

            try operations.append(allocator, Operation{
                .set = .{ .bf_type = bf_type, .offset = offset, .value = value, .overflow = overflow },
            });
        } else if (std.mem.eql(u8, op_upper, "INCRBY")) {
            i += 1;
            if (i + 2 >= args.len) return w.writeError("ERR INCRBY requires type, offset, and increment");

            const type_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid type"),
            };
            i += 1;

            const offset_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid offset"),
            };
            i += 1;

            const increment_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid increment"),
            };
            i += 1;

            const bf_type = parseBitfieldType(type_str) catch {
                return w.writeError("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 and i64 are supported as well.");
            };

            const offset = std.fmt.parseInt(usize, offset_str, 10) catch {
                return w.writeError("ERR bit offset is not an integer or out of range");
            };

            const increment = std.fmt.parseInt(i64, increment_str, 10) catch {
                return w.writeError("ERR bit increment is not an integer or out of range");
            };

            try operations.append(allocator, Operation{
                .incrby = .{ .bf_type = bf_type, .offset = offset, .increment = increment, .overflow = overflow },
            });
        } else {
            return w.writeError("ERR unknown BITFIELD subcommand or wrong number of arguments");
        }
    }

    if (operations.items.len == 0) {
        return w.writeError("ERR wrong number of arguments for 'bitfield' command");
    }

    // Execute operations
    var results = std.ArrayList(?i64){};
    defer results.deinit(allocator);

    // Get current value
    const current_val = storage.get(key) orelse "";

    // Determine max bytes needed
    var max_bytes: usize = current_val.len;
    for (operations.items) |op| {
        const needed = switch (op) {
            .get => |g| getBytesNeeded(g.offset, g.bf_type.bits),
            .set => |s| getBytesNeeded(s.offset, s.bf_type.bits),
            .incrby => |inc| getBytesNeeded(inc.offset, inc.bf_type.bits),
        };
        if (needed > max_bytes) max_bytes = needed;
    }

    // Create working buffer
    var working_data = try allocator.alloc(u8, max_bytes);
    defer allocator.free(working_data);
    @memset(working_data, 0);
    if (current_val.len > 0) {
        @memcpy(working_data[0..current_val.len], current_val);
    }

    var modified = false;

    for (operations.items) |op| {
        switch (op) {
            .get => |g| {
                const value = readBitfield(working_data, g.offset, g.bf_type);
                try results.append(allocator, value);
            },
            .set => |s| {
                const old_value = readBitfield(working_data, s.offset, s.bf_type);
                writeBitfield(working_data, s.offset, s.bf_type, s.value);
                try results.append(allocator, old_value);
                modified = true;
            },
            .incrby => |inc| {
                const current = readBitfield(working_data, inc.offset, inc.bf_type);
                const result = checkOverflow(current, inc.increment, inc.bf_type, inc.overflow);

                if (result.overflow_occurred) {
                    try results.append(allocator, null);
                } else {
                    writeBitfield(working_data, inc.offset, inc.bf_type, result.value);
                    try results.append(allocator, result.value);
                    modified = true;
                }
            },
        }
    }

    // Save modified data
    if (modified) {
        try storage.set(key, working_data, null);
    }

    // Build response array
    var buffer = std.ArrayList(u8){};
    errdefer buffer.deinit(allocator);

    try buffer.append(allocator, '*');
    var buf: [32]u8 = undefined;
    const len_str = std.fmt.bufPrint(&buf, "{d}\r\n", .{results.items.len}) catch unreachable;
    try buffer.appendSlice(allocator, len_str);

    for (results.items) |maybe_val| {
        if (maybe_val) |val| {
            const val_str = std.fmt.bufPrint(&buf, ":{d}\r\n", .{val}) catch unreachable;
            try buffer.appendSlice(allocator, val_str);
        } else {
            try buffer.appendSlice(allocator, "$-1\r\n"); // nil
        }
    }

    return try buffer.toOwnedSlice(allocator);
}

/// BITFIELD_RO key [GET type offset] [GET type offset] ...
/// Read-only variant of BITFIELD (only GET operations allowed)
pub fn cmdBitfieldRo(
    allocator: Allocator,
    storage: *Storage,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'bitfield_ro' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse GET operations only
    var operations = std.ArrayList(GetOp){};
    defer operations.deinit(allocator);

    var i: usize = 2;
    while (i < args.len) {
        const op_str = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid operation"),
        };

        const op_upper = try std.ascii.allocUpperString(allocator, op_str);
        defer allocator.free(op_upper);

        if (std.mem.eql(u8, op_upper, "GET")) {
            i += 1;
            if (i + 1 >= args.len) return w.writeError("ERR GET requires type and offset");

            const type_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid type"),
            };
            i += 1;

            const offset_str = switch (args[i]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid offset"),
            };
            i += 1;

            const bf_type = parseBitfieldType(type_str) catch {
                return w.writeError("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 and i64 are supported as well.");
            };

            const offset = std.fmt.parseInt(usize, offset_str, 10) catch {
                return w.writeError("ERR bit offset is not an integer or out of range");
            };

            try operations.append(allocator, GetOp{ .bf_type = bf_type, .offset = offset });
        } else {
            return w.writeError("ERR BITFIELD_RO only supports the GET subcommand");
        }
    }

    if (operations.items.len == 0) {
        return w.writeError("ERR wrong number of arguments for 'bitfield_ro' command");
    }

    // Get current value
    const current_val = storage.get(key) orelse "";

    // Execute GET operations
    var results = std.ArrayList(i64){};
    defer results.deinit(allocator);

    for (operations.items) |op| {
        const value = readBitfield(current_val, op.offset, op.bf_type);
        try results.append(allocator, value);
    }

    // Build response array
    var buffer = std.ArrayList(u8){};
    errdefer buffer.deinit(allocator);

    try buffer.append(allocator, '*');
    var buf: [32]u8 = undefined;
    const len_str = std.fmt.bufPrint(&buf, "{d}\r\n", .{results.items.len}) catch unreachable;
    try buffer.appendSlice(allocator, len_str);

    for (results.items) |val| {
        const val_str = std.fmt.bufPrint(&buf, ":{d}\r\n", .{val}) catch unreachable;
        try buffer.appendSlice(allocator, val_str);
    }

    return try buffer.toOwnedSlice(allocator);
}

// Operation types
const Operation = union(enum) {
    get: GetOp,
    set: SetOp,
    incrby: IncrbyOp,
};

const GetOp = struct {
    bf_type: BitfieldType,
    offset: usize,
};

const SetOp = struct {
    bf_type: BitfieldType,
    offset: usize,
    value: i64,
    overflow: OverflowBehavior,
};

const IncrbyOp = struct {
    bf_type: BitfieldType,
    offset: usize,
    increment: i64,
    overflow: OverflowBehavior,
};

// ─── Unit Tests ────────────────────────────────────────────────────────────

test "parseBitfieldType" {
    const t1 = try parseBitfieldType("i8");
    try std.testing.expect(t1.signed == true);
    try std.testing.expect(t1.bits == 8);

    const t2 = try parseBitfieldType("u16");
    try std.testing.expect(t2.signed == false);
    try std.testing.expect(t2.bits == 16);

    const t3 = try parseBitfieldType("i63");
    try std.testing.expect(t3.signed == true);
    try std.testing.expect(t3.bits == 63);

    // Invalid types
    try std.testing.expectError(error.InvalidType, parseBitfieldType("x8"));
    try std.testing.expectError(error.InvalidType, parseBitfieldType("i0"));
    try std.testing.expectError(error.InvalidType, parseBitfieldType("i65"));
}

test "readBitfield and writeBitfield" {
    const allocator = std.testing.allocator;

    const data = try allocator.alloc(u8, 8);
    defer allocator.free(data);
    @memset(data, 0);

    // Test unsigned 8-bit at offset 0
    const t1 = BitfieldType{ .signed = false, .bits = 8 };
    writeBitfield(data, 0, t1, 42);
    const v1 = readBitfield(data, 0, t1);
    try std.testing.expectEqual(@as(i64, 42), v1);

    // Test signed 8-bit at offset 8
    const t2 = BitfieldType{ .signed = true, .bits = 8 };
    writeBitfield(data, 8, t2, -5);
    const v2 = readBitfield(data, 8, t2);
    try std.testing.expectEqual(@as(i64, -5), v2);

    // Test unsigned 16-bit at offset 16
    const t3 = BitfieldType{ .signed = false, .bits = 16 };
    writeBitfield(data, 16, t3, 1000);
    const v3 = readBitfield(data, 16, t3);
    try std.testing.expectEqual(@as(i64, 1000), v3);
}

test "checkOverflow wrap" {
    const bf_type = BitfieldType{ .signed = true, .bits = 8 };

    const r1 = checkOverflow(127, 1, bf_type, .wrap);
    try std.testing.expectEqual(@as(i64, -128), r1.value);
    try std.testing.expectEqual(false, r1.overflow_occurred);

    const r2 = checkOverflow(-128, -1, bf_type, .wrap);
    try std.testing.expectEqual(@as(i64, 127), r2.value);
    try std.testing.expectEqual(false, r2.overflow_occurred);
}

test "checkOverflow sat" {
    const bf_type = BitfieldType{ .signed = true, .bits = 8 };

    const r1 = checkOverflow(127, 1, bf_type, .sat);
    try std.testing.expectEqual(@as(i64, 127), r1.value);
    try std.testing.expectEqual(false, r1.overflow_occurred);

    const r2 = checkOverflow(-128, -1, bf_type, .sat);
    try std.testing.expectEqual(@as(i64, -128), r2.value);
    try std.testing.expectEqual(false, r2.overflow_occurred);
}

test "checkOverflow fail" {
    const bf_type = BitfieldType{ .signed = true, .bits = 8 };

    const r1 = checkOverflow(127, 1, bf_type, .fail);
    try std.testing.expectEqual(true, r1.overflow_occurred);

    const r2 = checkOverflow(-128, -1, bf_type, .fail);
    try std.testing.expectEqual(true, r2.overflow_occurred);
}
