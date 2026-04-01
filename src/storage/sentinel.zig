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
