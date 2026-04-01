const std = @import("std");

/// Information about a monitored master instance
pub const MasterInfo = struct {
    /// Master name (unique identifier)
    name: []const u8,
    /// IP address of master
    ip: []const u8,
    /// Port of master
    port: u16,
    /// Number of Sentinels that need to agree for failover
    quorum: u8,
    /// Milliseconds before considering master as down
    down_after_milliseconds: u64,
    /// Last time we sent PING to master (Unix timestamp ms)
    last_ping_time: i64,
    /// Last time we received PONG from master (Unix timestamp ms)
    last_pong_time: i64,
    /// Whether master is currently considered down
    is_down: bool,

    pub fn deinit(self: *MasterInfo, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.ip);
    }
};

/// Redis Sentinel state
/// Manages monitoring and failover of Redis masters
pub const SentinelState = struct {
    /// Whether Sentinel mode is enabled
    enabled: bool,
    /// Sentinel's unique ID (40-char hex, similar to cluster node ID)
    myid: [40]u8,
    /// Map of monitored masters: name → MasterInfo
    monitored_masters: std.StringHashMap(MasterInfo),
    /// Current configuration epoch
    current_epoch: u64,
    /// Allocator for dynamic memory
    allocator: std.mem.Allocator,

    /// Initialize a new Sentinel state
    pub fn init(allocator: std.mem.Allocator) SentinelState {
        return SentinelState{
            .enabled = false,
            .myid = generateNodeId(),
            .monitored_masters = std.StringHashMap(MasterInfo).init(allocator),
            .current_epoch = 0,
            .allocator = allocator,
        };
    }

    /// Free all resources
    pub fn deinit(self: *SentinelState) void {
        // Free all MasterInfo resources
        var it = self.monitored_masters.iterator();
        while (it.next()) |entry| {
            var master = entry.value_ptr;
            master.deinit(self.allocator);
        }
        // Free the map itself
        self.monitored_masters.deinit();
    }

    /// Generate a random 40-character hex ID (similar to cluster node ID)
    fn generateNodeId() [40]u8 {
        var node_id: [40]u8 = undefined;
        var prng = std.Random.DefaultPrng.init(@intCast(std.time.microTimestamp()));
        const random = prng.random();
        const hex_chars = "0123456789abcdef";
        for (&node_id) |*c| {
            c.* = hex_chars[random.intRangeLessThan(u8, 0, 16)];
        }
        return node_id;
    }

    /// Add a master to the monitoring list
    /// Returns error if master with same name already exists
    pub fn monitorMaster(
        self: *SentinelState,
        name: []const u8,
        ip: []const u8,
        port: u16,
        quorum: u8,
    ) !void {
        // Check if master already exists
        if (self.monitored_masters.contains(name)) {
            return error.MasterAlreadyExists;
        }

        // Duplicate strings for ownership
        const name_copy = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(name_copy);
        const ip_copy = try self.allocator.dupe(u8, ip);
        errdefer self.allocator.free(ip_copy);

        const now = std.time.milliTimestamp();
        const master = MasterInfo{
            .name = name_copy,
            .ip = ip_copy,
            .port = port,
            .quorum = quorum,
            .down_after_milliseconds = 30000, // Default 30 seconds
            .last_ping_time = now,
            .last_pong_time = now,
            .is_down = false,
        };

        try self.monitored_masters.put(name_copy, master);
    }

    /// Remove a master from the monitoring list
    /// Returns error if master doesn't exist
    pub fn removeMaster(self: *SentinelState, name: []const u8) !void {
        // Get the master entry
        const kv = self.monitored_masters.fetchRemove(name) orelse return error.MasterNotFound;

        // Free the master's resources
        var master = kv.value;
        master.deinit(self.allocator);
    }

    /// Get all monitored masters as a slice
    /// Caller is responsible for freeing the returned slice (not the MasterInfo contents)
    pub fn getMasters(self: *SentinelState) ![]const MasterInfo {
        var list = try std.ArrayList(MasterInfo).initCapacity(self.allocator, self.monitored_masters.count());
        errdefer list.deinit(self.allocator);

        var it = self.monitored_masters.valueIterator();
        while (it.next()) |master| {
            try list.append(self.allocator, master.*);
        }

        return try list.toOwnedSlice(self.allocator);
    }

    /// Get a specific master by name
    /// Returns null if master doesn't exist
    pub fn getMaster(self: *SentinelState, name: []const u8) ?*MasterInfo {
        return self.monitored_masters.getPtr(name);
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "SentinelState.init: creates disabled by default" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    // Sentinel should be disabled by default
    try std.testing.expectEqual(false, sentinel.enabled);
}

test "SentinelState.init: generates unique 40-char hex myid" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    // myid should be 40 characters
    try std.testing.expectEqual(@as(usize, 40), sentinel.myid.len);

    // All characters should be valid hex (0-9, a-f)
    for (sentinel.myid) |char| {
        const is_hex = (char >= '0' and char <= '9') or (char >= 'a' and char <= 'f');
        try std.testing.expect(is_hex);
    }
}

test "SentinelState.init: generates different IDs for different instances" {
    const allocator = std.testing.allocator;

    var sentinel1 = SentinelState.init(allocator);
    defer sentinel1.deinit();

    var sentinel2 = SentinelState.init(allocator);
    defer sentinel2.deinit();

    // IDs should be different
    const same = std.mem.eql(u8, &sentinel1.myid, &sentinel2.myid);
    try std.testing.expect(!same);
}

test "SentinelState.init: initializes empty monitored_masters map" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    // monitored_masters should be empty initially
    try std.testing.expectEqual(@as(usize, 0), sentinel.monitored_masters.count());
}

test "SentinelState.init: initializes current_epoch to 0" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    // current_epoch should start at 0
    try std.testing.expectEqual(@as(u64, 0), sentinel.current_epoch);
}

test "SentinelState.monitorMaster: adds a new master" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    try sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    // Verify master was added
    try std.testing.expectEqual(@as(usize, 1), sentinel.monitored_masters.count());

    // Verify master details
    const master = sentinel.getMaster("mymaster").?;
    try std.testing.expectEqualStrings("mymaster", master.name);
    try std.testing.expectEqualStrings("127.0.0.1", master.ip);
    try std.testing.expectEqual(@as(u16, 6379), master.port);
    try std.testing.expectEqual(@as(u8, 2), master.quorum);
    try std.testing.expectEqual(@as(u64, 30000), master.down_after_milliseconds);
    try std.testing.expectEqual(false, master.is_down);
}

test "SentinelState.monitorMaster: rejects duplicate master name" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    try sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    // Attempt to add duplicate
    const result = sentinel.monitorMaster("mymaster", "127.0.0.2", 6380, 3);
    try std.testing.expectError(error.MasterAlreadyExists, result);

    // Only one master should exist
    try std.testing.expectEqual(@as(usize, 1), sentinel.monitored_masters.count());
}

test "SentinelState.removeMaster: removes existing master" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    try sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);
    try std.testing.expectEqual(@as(usize, 1), sentinel.monitored_masters.count());

    try sentinel.removeMaster("mymaster");

    // Master should be removed
    try std.testing.expectEqual(@as(usize, 0), sentinel.monitored_masters.count());
    try std.testing.expect(sentinel.getMaster("mymaster") == null);
}

test "SentinelState.removeMaster: rejects non-existent master" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    const result = sentinel.removeMaster("nonexistent");
    try std.testing.expectError(error.MasterNotFound, result);
}

test "SentinelState.getMasters: returns all monitored masters" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    try sentinel.monitorMaster("master1", "127.0.0.1", 6379, 2);
    try sentinel.monitorMaster("master2", "127.0.0.2", 6380, 3);

    const masters = try sentinel.getMasters();
    defer allocator.free(masters);

    try std.testing.expectEqual(@as(usize, 2), masters.len);
}

test "SentinelState.getMaster: returns null for non-existent master" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    const master = sentinel.getMaster("nonexistent");
    try std.testing.expect(master == null);
}

test "SentinelState.deinit: frees all resources" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);

    // Add a monitored master to verify cleanup
    const name = try allocator.dupe(u8, "mymaster");
    const ip = try allocator.dupe(u8, "127.0.0.1");
    const master = MasterInfo{
        .name = name,
        .ip = ip,
        .port = 6379,
        .quorum = 2,
        .down_after_milliseconds = 30000,
        .last_ping_time = 0,
        .last_pong_time = 0,
        .is_down = false,
    };
    try sentinel.monitored_masters.put(name, master);

    // deinit should free all resources without leaks
    sentinel.deinit();

    // If there's a leak, std.testing.allocator will catch it
}

test "SentinelState: allocator stored correctly" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    // Verify allocator is stored
    try std.testing.expectEqual(allocator, sentinel.allocator);
}
