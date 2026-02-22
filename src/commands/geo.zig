const std = @import("std");
const protocol = @import("../protocol/parser.zig");
const writer_mod = @import("../protocol/writer.zig");
const storage_mod = @import("../storage/memory.zig");

const RespValue = protocol.RespValue;
const Writer = writer_mod.Writer;
const Storage = storage_mod.Storage;

// Geospatial constants
const EARTH_RADIUS_METERS = 6372797.560856; // Earth radius in meters (WGS84)
const MIN_LAT = -85.05112878;
const MAX_LAT = 85.05112878;
const MIN_LON = -180.0;
const MAX_LON = 180.0;

/// GeoHash encoding parameters (52-bit precision like Redis)
const GEOHASH_STEP_MAX = 26; // 26 steps = 52 bits
const GEOHASH_LAT_RANGE = [2]f64{ -90.0, 90.0 };
const GEOHASH_LON_RANGE = [2]f64{ -180.0, 180.0 };

/// Encodes latitude and longitude into a 52-bit geohash
fn encodeGeohash(latitude: f64, longitude: f64) !u64 {
    if (latitude < MIN_LAT or latitude > MAX_LAT) return error.InvalidLatitude;
    if (longitude < MIN_LON or longitude > MAX_LON) return error.InvalidLongitude;

    var lat_range = GEOHASH_LAT_RANGE;
    var lon_range = GEOHASH_LON_RANGE;
    var hash: u64 = 0;
    var bit: u6 = 0;

    while (bit < GEOHASH_STEP_MAX * 2) : (bit += 1) {
        if (bit % 2 == 0) {
            // Even bit: longitude
            const mid = (lon_range[0] + lon_range[1]) / 2.0;
            if (longitude > mid) {
                hash |= @as(u64, 1) << @intCast(51 - bit);
                lon_range[0] = mid;
            } else {
                lon_range[1] = mid;
            }
        } else {
            // Odd bit: latitude
            const mid = (lat_range[0] + lat_range[1]) / 2.0;
            if (latitude > mid) {
                hash |= @as(u64, 1) << @intCast(51 - bit);
                lat_range[0] = mid;
            } else {
                lat_range[1] = mid;
            }
        }
    }

    return hash;
}

const Coords = struct { lat: f64, lon: f64 };

/// Decodes a 52-bit geohash into latitude and longitude
fn decodeGeohash(hash: u64) Coords {
    var lat_range = GEOHASH_LAT_RANGE;
    var lon_range = GEOHASH_LON_RANGE;
    var bit: u6 = 0;

    while (bit < GEOHASH_STEP_MAX * 2) : (bit += 1) {
        const is_set = (hash & (@as(u64, 1) << @intCast(51 - bit))) != 0;

        if (bit % 2 == 0) {
            // Even bit: longitude
            const mid = (lon_range[0] + lon_range[1]) / 2.0;
            if (is_set) {
                lon_range[0] = mid;
            } else {
                lon_range[1] = mid;
            }
        } else {
            // Odd bit: latitude
            const mid = (lat_range[0] + lat_range[1]) / 2.0;
            if (is_set) {
                lat_range[0] = mid;
            } else {
                lat_range[1] = mid;
            }
        }
    }

    return .{
        .lat = (lat_range[0] + lat_range[1]) / 2.0,
        .lon = (lon_range[0] + lon_range[1]) / 2.0,
    };
}

/// Calculates distance between two points using Haversine formula
fn haversineDistance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) f64 {
    const lat1_rad = lat1 * std.math.pi / 180.0;
    const lat2_rad = lat2 * std.math.pi / 180.0;
    const dlat = (lat2 - lat1) * std.math.pi / 180.0;
    const dlon = (lon2 - lon1) * std.math.pi / 180.0;

    const a = @sin(dlat / 2.0) * @sin(dlat / 2.0) +
        @cos(lat1_rad) * @cos(lat2_rad) *
        @sin(dlon / 2.0) * @sin(dlon / 2.0);

    const c = 2.0 * std.math.atan2(@sqrt(a), @sqrt(1.0 - a));
    return EARTH_RADIUS_METERS * c;
}

/// Encodes a geohash into base32 string (11 character precision)
fn encodeGeohashBase32(hash: u64, buf: []u8) ![]const u8 {
    const base32 = "0123456789bcdefghjkmnpqrstuvwxyz";
    var result: [11]u8 = undefined;
    var h = hash;
    var i: usize = 0;

    while (i < 11) : (i += 1) {
        const idx = h & 0x1F; // Get lowest 5 bits
        result[10 - i] = base32[@intCast(idx)];
        h >>= 5;
    }

    @memcpy(buf[0..11], &result);
    return buf[0..11];
}

/// GEOADD key longitude latitude member [longitude latitude member ...]
pub fn cmdGeoadd(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4 or (args.len - 1) % 3 != 0) {
        return w.writeError("ERR wrong number of arguments for 'geoadd' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    var added_count: usize = 0;
    var i: usize = 2;
    while (i < args.len) : (i += 3) {
        const lon_str = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid longitude"),
        };
        const lat_str = switch (args[i + 1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid latitude"),
        };
        const member = switch (args[i + 2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid member"),
        };

        const lon = std.fmt.parseFloat(f64, lon_str) catch {
            return w.writeError("ERR invalid longitude");
        };
        const lat = std.fmt.parseFloat(f64, lat_str) catch {
            return w.writeError("ERR invalid latitude");
        };

        // Encode coordinates as geohash
        const geohash = encodeGeohash(lat, lon) catch {
            return w.writeError("ERR invalid coordinates");
        };

        // Store as sorted set with geohash as score
        const score = @as(f64, @floatFromInt(geohash));
        const result = try storage.zadd(key, &[_]f64{score}, &[_][]const u8{member}, 0, null);
        added_count += result.added;
    }

    return w.writeInteger(@intCast(added_count));
}

/// GEOPOS key member [member ...]
pub fn cmdGeopos(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    if (args.len < 2) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'geopos' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR invalid key");
        },
    };

    // Build response array manually
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    const member_count = args.len - 2;
    try std.fmt.format(buf.writer(allocator), "*{d}\r\n", .{member_count});

    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const member = switch (args[i]) {
            .bulk_string => |s| s,
            else => {
                buf.deinit(allocator);
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR invalid member");
            },
        };

        if (storage.zscore(key, member)) |score| {
            const geohash: u64 = @intFromFloat(score);
            const coords = decodeGeohash(geohash);

            try buf.appendSlice(allocator, "*2\r\n");

            var lon_buf: [64]u8 = undefined;
            const lon_str = try std.fmt.bufPrint(&lon_buf, "{d:.6}", .{coords.lon});
            try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ lon_str.len, lon_str });

            var lat_buf: [64]u8 = undefined;
            const lat_str = try std.fmt.bufPrint(&lat_buf, "{d:.6}", .{coords.lat});
            try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ lat_str.len, lat_str });
        } else {
            try buf.appendSlice(allocator, "*-1\r\n");
        }
    }

    return buf.toOwnedSlice(allocator);
}

/// GEODIST key member1 member2 [unit]
pub fn cmdGeodist(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4 or args.len > 5) {
        return w.writeError("ERR wrong number of arguments for 'geodist' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };
    const member1 = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid member"),
    };
    const member2 = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid member"),
    };
    const unit = if (args.len == 5) switch (args[4]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid unit"),
    } else "m";

    const score1 = storage.zscore(key, member1) orelse {
        return w.writeNull();
    };

    const score2 = storage.zscore(key, member2) orelse {
        return w.writeNull();
    };

    const geohash1: u64 = @intFromFloat(score1);
    const geohash2: u64 = @intFromFloat(score2);
    const coords1 = decodeGeohash(geohash1);
    const coords2 = decodeGeohash(geohash2);

    var distance = haversineDistance(coords1.lat, coords1.lon, coords2.lat, coords2.lon);

    // Convert to requested unit
    if (std.mem.eql(u8, unit, "m")) {
        // meters (default)
    } else if (std.mem.eql(u8, unit, "km")) {
        distance /= 1000.0;
    } else if (std.mem.eql(u8, unit, "mi")) {
        distance /= 1609.34;
    } else if (std.mem.eql(u8, unit, "ft")) {
        distance /= 0.3048;
    } else {
        return w.writeError("ERR unsupported unit provided. please use m, km, ft, mi");
    }

    var dist_buf: [64]u8 = undefined;
    const dist_str = try std.fmt.bufPrint(&dist_buf, "{d:.4}", .{distance});
    return w.writeBulkString(dist_str);
}

/// GEOHASH key member [member ...]
pub fn cmdGeohash(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    if (args.len < 2) {
        var w = Writer.init(allocator);
        defer w.deinit();
        return w.writeError("ERR wrong number of arguments for 'geohash' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => {
            var w = Writer.init(allocator);
            defer w.deinit();
            return w.writeError("ERR invalid key");
        },
    };

    // Build response
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    const member_count = args.len - 2;
    try std.fmt.format(buf.writer(allocator), "*{d}\r\n", .{member_count});

    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const member = switch (args[i]) {
            .bulk_string => |s| s,
            else => {
                buf.deinit(allocator);
                var w = Writer.init(allocator);
                defer w.deinit();
                return w.writeError("ERR invalid member");
            },
        };

        if (storage.zscore(key, member)) |score| {
            const geohash: u64 = @intFromFloat(score);
            var hash_buf: [32]u8 = undefined;
            const hash_str = try encodeGeohashBase32(geohash, &hash_buf);
            try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ hash_str.len, hash_str });
        } else {
            try buf.appendSlice(allocator, "$-1\r\n");
        }
    }

    return buf.toOwnedSlice(allocator);
}

/// Result struct for georadius queries
const GeoResult = struct {
    member: []const u8,
    distance: f64,
    geohash: u64,
    coords: Coords,
};

/// GEORADIUS key longitude latitude radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC]
pub fn cmdGeoradius(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 6) {
        return w.writeError("ERR wrong number of arguments for 'georadius' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };
    const lon_str = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid longitude"),
    };
    const lat_str = switch (args[3]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid latitude"),
    };
    const radius_str = switch (args[4]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid radius"),
    };
    const unit = switch (args[5]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid unit"),
    };

    const lon = std.fmt.parseFloat(f64, lon_str) catch {
        return w.writeError("ERR invalid longitude");
    };
    const lat = std.fmt.parseFloat(f64, lat_str) catch {
        return w.writeError("ERR invalid latitude");
    };
    var radius = std.fmt.parseFloat(f64, radius_str) catch {
        return w.writeError("ERR invalid radius");
    };

    // Convert radius to meters
    if (std.mem.eql(u8, unit, "m")) {
        // meters (default)
    } else if (std.mem.eql(u8, unit, "km")) {
        radius *= 1000.0;
    } else if (std.mem.eql(u8, unit, "mi")) {
        radius *= 1609.34;
    } else if (std.mem.eql(u8, unit, "ft")) {
        radius *= 0.3048;
    } else {
        return w.writeError("ERR unsupported unit provided. please use m, km, ft, mi");
    }

    // Parse options
    var with_coord = false;
    var with_dist = false;
    var with_hash = false;
    var count_limit: ?usize = null;
    var sort_asc = false;
    var sort_desc = false;

    var i: usize = 6;
    while (i < args.len) {
        const arg_str = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const arg_upper = try std.ascii.allocUpperString(allocator, arg_str);
        defer allocator.free(arg_upper);

        if (std.mem.eql(u8, arg_upper, "WITHCOORD")) {
            with_coord = true;
            i += 1;
        } else if (std.mem.eql(u8, arg_upper, "WITHDIST")) {
            with_dist = true;
            i += 1;
        } else if (std.mem.eql(u8, arg_upper, "WITHHASH")) {
            with_hash = true;
            i += 1;
        } else if (std.mem.eql(u8, arg_upper, "COUNT")) {
            if (i + 1 >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const count_str = switch (args[i + 1]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid count"),
            };
            count_limit = std.fmt.parseInt(usize, count_str, 10) catch {
                return w.writeError("ERR invalid count");
            };
            i += 2;
        } else if (std.mem.eql(u8, arg_upper, "ASC")) {
            sort_asc = true;
            i += 1;
        } else if (std.mem.eql(u8, arg_upper, "DESC")) {
            sort_desc = true;
            i += 1;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Get all members and filter by radius
    const all_members = try storage.zrange(allocator, key, 0, -1, false) orelse {
        // Key doesn't exist or is not a sorted set
        var buf = std.ArrayList(u8){};
        defer buf.deinit(allocator);
        try buf.appendSlice(allocator, "*0\r\n");
        return buf.toOwnedSlice(allocator);
    };
    defer {
        for (all_members) |member| {
            allocator.free(member);
        }
        allocator.free(all_members);
    }

    var results = std.ArrayList(GeoResult){};
    defer results.deinit(allocator);

    for (all_members) |member| {
        if (storage.zscore(key, member)) |score| {
            const geohash: u64 = @intFromFloat(score);
            const coords = decodeGeohash(geohash);
            const distance = haversineDistance(lat, lon, coords.lat, coords.lon);

            if (distance <= radius) {
                try results.append(allocator, .{
                    .member = member,
                    .distance = distance,
                    .geohash = geohash,
                    .coords = coords,
                });
            }
        }
    }

    // Sort by distance if requested
    if (sort_asc or sort_desc) {
        const Context = struct {
            fn lessThan(_: void, a: GeoResult, b: GeoResult) bool {
                return a.distance < b.distance;
            }
            fn greaterThan(_: void, a: GeoResult, b: GeoResult) bool {
                return a.distance > b.distance;
            }
        };

        if (sort_asc) {
            std.mem.sort(GeoResult, results.items, {}, Context.lessThan);
        } else {
            std.mem.sort(GeoResult, results.items, {}, Context.greaterThan);
        }
    }

    const result_count = if (count_limit) |limit| @min(limit, results.items.len) else results.items.len;

    // Build response
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    try std.fmt.format(buf.writer(allocator), "*{d}\r\n", .{result_count});

    for (results.items[0..result_count]) |result| {
        var field_count: usize = 1;
        if (with_dist) field_count += 1;
        if (with_hash) field_count += 1;
        if (with_coord) field_count += 1;

        if (field_count > 1) {
            try std.fmt.format(buf.writer(allocator), "*{d}\r\n", .{field_count});
        }

        // Member name
        try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ result.member.len, result.member });

        // Distance
        if (with_dist) {
            var dist = result.distance;
            if (std.mem.eql(u8, unit, "km")) {
                dist /= 1000.0;
            } else if (std.mem.eql(u8, unit, "mi")) {
                dist /= 1609.34;
            } else if (std.mem.eql(u8, unit, "ft")) {
                dist /= 0.3048;
            }
            var dist_buf: [64]u8 = undefined;
            const dist_str = try std.fmt.bufPrint(&dist_buf, "{d:.4}", .{dist});
            try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ dist_str.len, dist_str });
        }

        // Geohash
        if (with_hash) {
            try std.fmt.format(buf.writer(allocator), ":{d}\r\n", .{result.geohash});
        }

        // Coordinates
        if (with_coord) {
            try buf.appendSlice(allocator, "*2\r\n");
            var lon_buf: [64]u8 = undefined;
            const lon_str_result = try std.fmt.bufPrint(&lon_buf, "{d:.6}", .{result.coords.lon});
            try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ lon_str_result.len, lon_str_result });

            var lat_buf: [64]u8 = undefined;
            const lat_str_result = try std.fmt.bufPrint(&lat_buf, "{d:.6}", .{result.coords.lat});
            try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ lat_str_result.len, lat_str_result });
        }
    }

    return buf.toOwnedSlice(allocator);
}

/// GEOSEARCH - simplified implementation (BYRADIUS only)
pub fn cmdGeosearch(allocator: std.mem.Allocator, storage: *Storage, args: []const RespValue) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 4) {
        return w.writeError("ERR wrong number of arguments for 'geosearch' command");
    }

    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Parse FROM clause
    var lat: f64 = undefined;
    var lon: f64 = undefined;
    var idx: usize = 2;

    if (idx >= args.len) {
        return w.writeError("ERR syntax error");
    }

    const from_clause = switch (args[idx]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };
    const from_upper = try std.ascii.allocUpperString(allocator, from_clause);
    defer allocator.free(from_upper);

    if (std.mem.eql(u8, from_upper, "FROMMEMBER")) {
        if (idx + 1 >= args.len) {
            return w.writeError("ERR syntax error");
        }
        const member = switch (args[idx + 1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid member"),
        };
        const score = storage.zscore(key, member) orelse {
            // Return empty array if member not found
            var buf = std.ArrayList(u8){};
            defer buf.deinit(allocator);
            try buf.appendSlice(allocator, "*0\r\n");
            return buf.toOwnedSlice(allocator);
        };
        const geohash: u64 = @intFromFloat(score);
        const coords = decodeGeohash(geohash);
        lat = coords.lat;
        lon = coords.lon;
        idx += 2;
    } else if (std.mem.eql(u8, from_upper, "FROMLONLAT")) {
        if (idx + 2 >= args.len) {
            return w.writeError("ERR syntax error");
        }
        const lon_str = switch (args[idx + 1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid longitude"),
        };
        const lat_str = switch (args[idx + 2]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid latitude"),
        };
        lon = std.fmt.parseFloat(f64, lon_str) catch {
            return w.writeError("ERR invalid longitude");
        };
        lat = std.fmt.parseFloat(f64, lat_str) catch {
            return w.writeError("ERR invalid latitude");
        };
        idx += 3;
    } else {
        return w.writeError("ERR syntax error");
    }

    // Parse BY clause (BYRADIUS only)
    if (idx >= args.len) {
        return w.writeError("ERR syntax error");
    }
    const by_clause = switch (args[idx]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR syntax error"),
    };
    const by_upper = try std.ascii.allocUpperString(allocator, by_clause);
    defer allocator.free(by_upper);

    if (!std.mem.eql(u8, by_upper, "BYRADIUS")) {
        return w.writeError("ERR syntax error, BYRADIUS required (BYBOX not implemented)");
    }

    if (idx + 2 >= args.len) {
        return w.writeError("ERR syntax error");
    }

    const radius_str = switch (args[idx + 1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid radius"),
    };
    const unit = switch (args[idx + 2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid unit"),
    };

    var radius = std.fmt.parseFloat(f64, radius_str) catch {
        return w.writeError("ERR invalid radius");
    };
    idx += 3;

    // Convert radius to meters
    if (std.mem.eql(u8, unit, "m")) {
        // meters (default)
    } else if (std.mem.eql(u8, unit, "km")) {
        radius *= 1000.0;
    } else if (std.mem.eql(u8, unit, "mi")) {
        radius *= 1609.34;
    } else if (std.mem.eql(u8, unit, "ft")) {
        radius *= 0.3048;
    } else {
        return w.writeError("ERR unsupported unit provided. please use m, km, ft, mi");
    }

    // Parse options (same as GEORADIUS)
    var with_coord = false;
    var with_dist = false;
    var with_hash = false;
    var count_limit: ?usize = null;
    var sort_asc = false;
    var sort_desc = false;

    while (idx < args.len) {
        const arg_str = switch (args[idx]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR syntax error"),
        };
        const arg_upper = try std.ascii.allocUpperString(allocator, arg_str);
        defer allocator.free(arg_upper);

        if (std.mem.eql(u8, arg_upper, "WITHCOORD")) {
            with_coord = true;
            idx += 1;
        } else if (std.mem.eql(u8, arg_upper, "WITHDIST")) {
            with_dist = true;
            idx += 1;
        } else if (std.mem.eql(u8, arg_upper, "WITHHASH")) {
            with_hash = true;
            idx += 1;
        } else if (std.mem.eql(u8, arg_upper, "COUNT")) {
            if (idx + 1 >= args.len) {
                return w.writeError("ERR syntax error");
            }
            const count_str = switch (args[idx + 1]) {
                .bulk_string => |s| s,
                else => return w.writeError("ERR invalid count"),
            };
            count_limit = std.fmt.parseInt(usize, count_str, 10) catch {
                return w.writeError("ERR invalid count");
            };
            idx += 2;
        } else if (std.mem.eql(u8, arg_upper, "ASC")) {
            sort_asc = true;
            idx += 1;
        } else if (std.mem.eql(u8, arg_upper, "DESC")) {
            sort_desc = true;
            idx += 1;
        } else {
            return w.writeError("ERR syntax error");
        }
    }

    // Reuse GEORADIUS logic
    const all_members = try storage.zrange(allocator, key, 0, -1, false) orelse {
        // Key doesn't exist or is not a sorted set
        var buf = std.ArrayList(u8){};
        defer buf.deinit(allocator);
        try buf.appendSlice(allocator, "*0\r\n");
        return buf.toOwnedSlice(allocator);
    };
    defer {
        for (all_members) |member| {
            allocator.free(member);
        }
        allocator.free(all_members);
    }

    var results = std.ArrayList(GeoResult){};
    defer results.deinit(allocator);

    for (all_members) |member| {
        if (storage.zscore(key, member)) |score| {
            const geohash: u64 = @intFromFloat(score);
            const coords = decodeGeohash(geohash);
            const distance = haversineDistance(lat, lon, coords.lat, coords.lon);

            if (distance <= radius) {
                try results.append(allocator, .{
                    .member = member,
                    .distance = distance,
                    .geohash = geohash,
                    .coords = coords,
                });
            }
        }
    }

    // Sort and output (same as GEORADIUS)
    if (sort_asc or sort_desc) {
        const Context = struct {
            fn lessThan(_: void, a: GeoResult, b: GeoResult) bool {
                return a.distance < b.distance;
            }
            fn greaterThan(_: void, a: GeoResult, b: GeoResult) bool {
                return a.distance > b.distance;
            }
        };

        if (sort_asc) {
            std.mem.sort(GeoResult, results.items, {}, Context.lessThan);
        } else {
            std.mem.sort(GeoResult, results.items, {}, Context.greaterThan);
        }
    }

    const result_count = if (count_limit) |limit| @min(limit, results.items.len) else results.items.len;

    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    try std.fmt.format(buf.writer(allocator), "*{d}\r\n", .{result_count});

    for (results.items[0..result_count]) |result| {
        var field_count: usize = 1;
        if (with_dist) field_count += 1;
        if (with_hash) field_count += 1;
        if (with_coord) field_count += 1;

        if (field_count > 1) {
            try std.fmt.format(buf.writer(allocator), "*{d}\r\n", .{field_count});
        }

        try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ result.member.len, result.member });

        if (with_dist) {
            var dist = result.distance;
            if (std.mem.eql(u8, unit, "km")) {
                dist /= 1000.0;
            } else if (std.mem.eql(u8, unit, "mi")) {
                dist /= 1609.34;
            } else if (std.mem.eql(u8, unit, "ft")) {
                dist /= 0.3048;
            }
            var dist_buf: [64]u8 = undefined;
            const dist_str = try std.fmt.bufPrint(&dist_buf, "{d:.4}", .{dist});
            try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ dist_str.len, dist_str });
        }

        if (with_hash) {
            try std.fmt.format(buf.writer(allocator), ":{d}\r\n", .{result.geohash});
        }

        if (with_coord) {
            try buf.appendSlice(allocator, "*2\r\n");
            var lon_buf: [64]u8 = undefined;
            const lon_str_result = try std.fmt.bufPrint(&lon_buf, "{d:.6}", .{result.coords.lon});
            try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ lon_str_result.len, lon_str_result });

            var lat_buf: [64]u8 = undefined;
            const lat_str_result = try std.fmt.bufPrint(&lat_buf, "{d:.6}", .{result.coords.lat});
            try std.fmt.format(buf.writer(allocator), "${d}\r\n{s}\r\n", .{ lat_str_result.len, lat_str_result });
        }
    }

    return buf.toOwnedSlice(allocator);
}

// Unit tests
test "geohash encoding and decoding" {
    const testing = std.testing;

    // Test San Francisco coordinates
    const lat: f64 = 37.7749;
    const lon: f64 = -122.4194;

    const hash = try encodeGeohash(lat, lon);
    const decoded = decodeGeohash(hash);

    // Should be within ~1 meter precision
    try testing.expect(@abs(decoded.lat - lat) < 0.0001);
    try testing.expect(@abs(decoded.lon - lon) < 0.0001);
}

test "haversine distance calculation" {
    const testing = std.testing;

    // Distance from San Francisco to Los Angeles
    const sf_lat = 37.7749;
    const sf_lon = -122.4194;
    const la_lat = 34.0522;
    const la_lon = -118.2437;

    const distance = haversineDistance(sf_lat, sf_lon, la_lat, la_lon);

    // Should be approximately 559 km (559000 meters)
    try testing.expect(distance > 550000.0 and distance < 570000.0);
}

test "geohash base32 encoding" {
    const testing = std.testing;

    var buf: [32]u8 = undefined;
    const hash: u64 = 3471157880845414; // Example geohash
    const encoded = try encodeGeohashBase32(hash, &buf);

    try testing.expect(encoded.len == 11);
}
