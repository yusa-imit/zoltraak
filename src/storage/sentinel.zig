const std = @import("std");
const zuda = @import("zuda");

/// Information about another Sentinel instance
pub const SentinelInfo = struct {
    /// Sentinel's unique ID (40-char hex)
    id: []const u8,
    /// IP address of Sentinel
    ip: []const u8,
    /// Port of Sentinel
    port: u16,
    /// Last time we heard from this Sentinel (Unix timestamp ms)
    last_hello_time: i64,
    /// Whether this Sentinel considers the master down
    is_master_down: bool,

    pub fn deinit(self: *SentinelInfo, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.ip);
    }
};

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
    /// Other Sentinels monitoring this master: sentinel_id → SentinelInfo
    sentinels: std.StringHashMap(SentinelInfo),

    pub fn deinit(self: *MasterInfo, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.ip);
        // Free all SentinelInfo resources
        var it = self.sentinels.iterator();
        while (it.next()) |entry| {
            var sentinel = entry.value_ptr;
            sentinel.deinit(allocator);
        }
        self.sentinels.deinit();
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
            .sentinels = std.StringHashMap(SentinelInfo).init(self.allocator),
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

    /// Register or update a Sentinel for a master
    /// Used when receiving HELLO messages from other Sentinels
    pub fn registerSentinel(
        self: *SentinelState,
        master_name: []const u8,
        sentinel_id: []const u8,
        ip: []const u8,
        port: u16,
    ) !void {
        const master = self.monitored_masters.getPtr(master_name) orelse return error.MasterNotFound;

        // Check if Sentinel already exists
        if (master.sentinels.getPtr(sentinel_id)) |existing| {
            // Update existing Sentinel
            existing.last_hello_time = std.time.milliTimestamp();
            return;
        }

        // Add new Sentinel
        const id_copy = try self.allocator.dupe(u8, sentinel_id);
        errdefer self.allocator.free(id_copy);
        const ip_copy = try self.allocator.dupe(u8, ip);
        errdefer self.allocator.free(ip_copy);

        const sentinel = SentinelInfo{
            .id = id_copy,
            .ip = ip_copy,
            .port = port,
            .last_hello_time = std.time.milliTimestamp(),
            .is_master_down = false,
        };

        try master.sentinels.put(id_copy, sentinel);
    }

    /// Get all Sentinels monitoring a specific master
    /// Returns null if master doesn't exist
    /// Caller is responsible for freeing the returned slice
    pub fn getSentinels(self: *SentinelState, master_name: []const u8) !?[]const SentinelInfo {
        const master = self.monitored_masters.getPtr(master_name) orelse return null;

        var list = try std.ArrayList(SentinelInfo).initCapacity(self.allocator, master.sentinels.count());
        errdefer list.deinit(self.allocator);

        var it = master.sentinels.valueIterator();
        while (it.next()) |sentinel| {
            try list.append(self.allocator, sentinel.*);
        }

        return try list.toOwnedSlice(self.allocator);
    }

    /// Check if a master at the given IP:port is down according to this Sentinel
    /// Returns a tuple: (is_known: bool, is_down: bool, leader_runid: ?[]const u8)
    /// leader_runid is null if not in failover, otherwise the Sentinel's ID leading the failover
    pub fn isMasterDownByAddr(
        self: *SentinelState,
        ip: []const u8,
        port: u16,
    ) struct { is_known: bool, is_down: bool, leader_runid: ?[]const u8 } {
        // Search for a master with matching IP and port
        var it = self.monitored_masters.valueIterator();
        while (it.next()) |master| {
            if (std.mem.eql(u8, master.ip, ip) and master.port == port) {
                // Found the master
                return .{
                    .is_known = true,
                    .is_down = master.is_down,
                    .leader_runid = null, // TODO: implement failover leader election
                };
            }
        }

        // Master not known
        return .{
            .is_known = false,
            .is_down = false,
            .leader_runid = null,
        };
    }

    /// Reset master(s) by glob pattern
    /// Clears all sentinels, resets timestamps to current time, sets is_down = false
    /// Preserves the master entry (does not remove it)
    /// Returns the count of masters reset
    pub fn resetMaster(self: *SentinelState, pattern: []const u8) !usize {
        var count: usize = 0;
        const now = std.time.milliTimestamp();

        // Iterate through all monitored masters
        var it = self.monitored_masters.iterator();
        while (it.next()) |entry| {
            const master_name = entry.key_ptr.*;
            const master = entry.value_ptr;

            // Check if master name matches glob pattern
            if (zuda.algorithms.string.globMatch(pattern, master_name)) {
                // Clear all sentinels for this master
                // Note: HashMap key is the same pointer as sentinel.id, so we only free sentinel.ip
                var sentinel_it = master.sentinels.iterator();
                while (sentinel_it.next()) |sentinel_entry| {
                    const sentinel_info = sentinel_entry.value_ptr;
                    // Only free ip, NOT id (id is owned by HashMap key and will be freed by clearAndFree)
                    self.allocator.free(sentinel_info.ip);
                }
                // clearAndFree() frees both keys and values (keys are sentinel.id strings)
                master.sentinels.clearAndFree();

                // Reset timestamps to current time
                master.last_ping_time = now;
                master.last_pong_time = now;

                // Clear is_down flag
                master.is_down = false;

                count += 1;
            }
        }

        return count;
    }

    /// Force failover for a specific master
    /// This is a stub implementation - actual failover logic will be implemented in future iterations
    pub fn forceFailover(self: *SentinelState, master_name: []const u8) !void {
        // Verify master exists
        _ = self.monitored_masters.getPtr(master_name) orelse return error.MasterNotFound;

        // TODO: Implement actual failover logic in future iterations
        // For now, this is a stub that just validates the master exists
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
        .sentinels = std.StringHashMap(SentinelInfo).init(allocator),
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

test "SentinelState.getMaster: returns null for nonexistent master" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    // Attempt to get nonexistent master
    const master = sentinel.getMaster("nonexistent");
    try std.testing.expect(master == null);
}

test "SentinelState.getMaster: returns existing master" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    try sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    // Get the master
    const master = sentinel.getMaster("mymaster").?;
    try std.testing.expectEqualStrings("mymaster", master.name);
    try std.testing.expectEqualStrings("127.0.0.1", master.ip);
    try std.testing.expectEqual(@as(u16, 6379), master.port);
    try std.testing.expectEqual(@as(u8, 2), master.quorum);
}

test "SentinelState.getMaster: allows modification of returned master" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    try sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    // Get mutable pointer to master
    const master = sentinel.getMaster("mymaster").?;

    // Modify master state
    master.is_down = true;
    master.last_pong_time = 123456789;

    // Verify modifications persist
    const master2 = sentinel.getMaster("mymaster").?;
    try std.testing.expectEqual(true, master2.is_down);
    try std.testing.expectEqual(@as(i64, 123456789), master2.last_pong_time);
}

test "SentinelState.registerSentinel: adds new sentinel to master" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    try sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);
    try sentinel.registerSentinel("mymaster", "abc123", "127.0.0.2", 26379);

    const master = sentinel.getMaster("mymaster").?;
    try std.testing.expectEqual(@as(usize, 1), master.sentinels.count());

    const other_sentinel = master.sentinels.get("abc123").?;
    try std.testing.expectEqualStrings("abc123", other_sentinel.id);
    try std.testing.expectEqualStrings("127.0.0.2", other_sentinel.ip);
    try std.testing.expectEqual(@as(u16, 26379), other_sentinel.port);
}

test "SentinelState.registerSentinel: updates existing sentinel" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    try sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);
    try sentinel.registerSentinel("mymaster", "abc123", "127.0.0.2", 26379);

    const master = sentinel.getMaster("mymaster").?;
    const old_time = master.sentinels.get("abc123").?.last_hello_time;

    // Register again (update) - timestamp will be >= old_time (usually same if very fast)
    try sentinel.registerSentinel("mymaster", "abc123", "127.0.0.2", 26379);

    // Should still have 1 sentinel
    try std.testing.expectEqual(@as(usize, 1), master.sentinels.count());

    // Timestamp should be >= old_time (milliTimestamp may be same if very fast)
    const new_time = master.sentinels.get("abc123").?.last_hello_time;
    try std.testing.expect(new_time >= old_time);
}

test "SentinelState.registerSentinel: rejects unknown master" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    const result = sentinel.registerSentinel("nonexistent", "abc123", "127.0.0.2", 26379);
    try std.testing.expectError(error.MasterNotFound, result);
}

test "SentinelState.getSentinels: returns all sentinels for master" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    try sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);
    try sentinel.registerSentinel("mymaster", "abc123", "127.0.0.2", 26379);
    try sentinel.registerSentinel("mymaster", "def456", "127.0.0.3", 26380);

    const sentinels = (try sentinel.getSentinels("mymaster")).?;
    defer allocator.free(sentinels);

    try std.testing.expectEqual(@as(usize, 2), sentinels.len);
}

test "SentinelState.getSentinels: returns null for unknown master" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    const sentinels = try sentinel.getSentinels("nonexistent");
    try std.testing.expect(sentinels == null);
}

test "SentinelState.isMasterDownByAddr: returns is_known=false for unknown master" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    const result = sentinel.isMasterDownByAddr("127.0.0.1", 6379);
    try std.testing.expectEqual(false, result.is_known);
    try std.testing.expectEqual(false, result.is_down);
    try std.testing.expect(result.leader_runid == null);
}

test "SentinelState.isMasterDownByAddr: returns is_down=false for healthy master" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    try sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    const result = sentinel.isMasterDownByAddr("127.0.0.1", 6379);
    try std.testing.expectEqual(true, result.is_known);
    try std.testing.expectEqual(false, result.is_down);
    try std.testing.expect(result.leader_runid == null);
}

test "SentinelState.isMasterDownByAddr: returns is_down=true for down master" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    try sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    // Mark master as down
    const master = sentinel.getMaster("mymaster").?;
    master.is_down = true;

    const result = sentinel.isMasterDownByAddr("127.0.0.1", 6379);
    try std.testing.expectEqual(true, result.is_known);
    try std.testing.expectEqual(true, result.is_down);
}

test "SentinelState.isMasterDownByAddr: matches by IP and port" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    try sentinel.monitorMaster("master1", "127.0.0.1", 6379, 2);
    try sentinel.monitorMaster("master2", "127.0.0.2", 6380, 2);

    // Check first master
    const result1 = sentinel.isMasterDownByAddr("127.0.0.1", 6379);
    try std.testing.expectEqual(true, result1.is_known);

    // Check second master
    const result2 = sentinel.isMasterDownByAddr("127.0.0.2", 6380);
    try std.testing.expectEqual(true, result2.is_known);

    // Wrong port
    const result3 = sentinel.isMasterDownByAddr("127.0.0.1", 9999);
    try std.testing.expectEqual(false, result3.is_known);
}

// ============================================================================
// SENTINEL RESET Tests
// ============================================================================

test "SentinelState.resetMaster: resets single master by exact name" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    // Add master with some sentinels and state
    try sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);
    try sentinel.registerSentinel("mymaster", "sentinel1", "127.0.0.2", 26379);
    try sentinel.registerSentinel("mymaster", "sentinel2", "127.0.0.3", 26380);

    const master = sentinel.getMaster("mymaster").?;
    master.is_down = true;
    master.last_ping_time = 1000;
    master.last_pong_time = 2000;

    // Reset by exact name
    const count = try sentinel.resetMaster("mymaster");

    // Should return count of 1
    try std.testing.expectEqual(@as(usize, 1), count);

    // Master should still exist
    const reset_master = sentinel.getMaster("mymaster").?;
    try std.testing.expectEqualStrings("mymaster", reset_master.name);

    // Sentinels should be cleared
    try std.testing.expectEqual(@as(usize, 0), reset_master.sentinels.count());

    // is_down should be cleared
    try std.testing.expectEqual(false, reset_master.is_down);

    // Timestamps should be reset to current time (not old values)
    const now = std.time.milliTimestamp();
    try std.testing.expect(reset_master.last_ping_time >= now - 100); // Within 100ms
    try std.testing.expect(reset_master.last_pong_time >= now - 100);
}

test "SentinelState.resetMaster: resets multiple masters by glob pattern" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    // Add multiple masters with "my" prefix
    try sentinel.monitorMaster("mymaster1", "127.0.0.1", 6379, 2);
    try sentinel.monitorMaster("mymaster2", "127.0.0.2", 6380, 2);
    try sentinel.monitorMaster("other", "127.0.0.3", 6381, 2);

    // Add sentinels and state to all
    try sentinel.registerSentinel("mymaster1", "s1", "127.0.0.4", 26379);
    try sentinel.registerSentinel("mymaster2", "s2", "127.0.0.5", 26380);
    try sentinel.registerSentinel("other", "s3", "127.0.0.6", 26381);

    sentinel.getMaster("mymaster1").?.is_down = true;
    sentinel.getMaster("mymaster2").?.is_down = true;
    sentinel.getMaster("other").?.is_down = true;

    // Reset with glob pattern "my*"
    const count = try sentinel.resetMaster("my*");

    // Should reset 2 masters
    try std.testing.expectEqual(@as(usize, 2), count);

    // mymaster1 and mymaster2 should be reset
    try std.testing.expectEqual(@as(usize, 0), sentinel.getMaster("mymaster1").?.sentinels.count());
    try std.testing.expectEqual(@as(usize, 0), sentinel.getMaster("mymaster2").?.sentinels.count());
    try std.testing.expectEqual(false, sentinel.getMaster("mymaster1").?.is_down);
    try std.testing.expectEqual(false, sentinel.getMaster("mymaster2").?.is_down);

    // "other" should NOT be reset
    try std.testing.expectEqual(@as(usize, 1), sentinel.getMaster("other").?.sentinels.count());
    try std.testing.expectEqual(true, sentinel.getMaster("other").?.is_down);
}

test "SentinelState.resetMaster: resets all masters with wildcard" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    // Add multiple masters
    try sentinel.monitorMaster("master1", "127.0.0.1", 6379, 2);
    try sentinel.monitorMaster("master2", "127.0.0.2", 6380, 2);
    try sentinel.monitorMaster("master3", "127.0.0.3", 6381, 2);

    // Add state
    try sentinel.registerSentinel("master1", "s1", "127.0.0.4", 26379);
    try sentinel.registerSentinel("master2", "s2", "127.0.0.5", 26380);
    try sentinel.registerSentinel("master3", "s3", "127.0.0.6", 26381);

    sentinel.getMaster("master1").?.is_down = true;
    sentinel.getMaster("master2").?.is_down = true;
    sentinel.getMaster("master3").?.is_down = true;

    // Reset with wildcard "*"
    const count = try sentinel.resetMaster("*");

    // Should reset all 3 masters
    try std.testing.expectEqual(@as(usize, 3), count);

    // All should be reset
    try std.testing.expectEqual(@as(usize, 0), sentinel.getMaster("master1").?.sentinels.count());
    try std.testing.expectEqual(@as(usize, 0), sentinel.getMaster("master2").?.sentinels.count());
    try std.testing.expectEqual(@as(usize, 0), sentinel.getMaster("master3").?.sentinels.count());
    try std.testing.expectEqual(false, sentinel.getMaster("master1").?.is_down);
    try std.testing.expectEqual(false, sentinel.getMaster("master2").?.is_down);
    try std.testing.expectEqual(false, sentinel.getMaster("master3").?.is_down);
}

test "SentinelState.resetMaster: returns 0 for no matches" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    try sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    // Pattern that doesn't match
    const count = try sentinel.resetMaster("nonexistent*");

    // Should return 0
    try std.testing.expectEqual(@as(usize, 0), count);

    // Master should be unchanged
    const master = sentinel.getMaster("mymaster").?;
    try std.testing.expectEqualStrings("mymaster", master.name);
}

test "SentinelState.resetMaster: preserves master entry (does not remove)" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    try sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);
    try sentinel.registerSentinel("mymaster", "s1", "127.0.0.2", 26379);

    // Reset
    const count = try sentinel.resetMaster("mymaster");
    try std.testing.expectEqual(@as(usize, 1), count);

    // Master should still exist with same IP/port/quorum
    const master = sentinel.getMaster("mymaster").?;
    try std.testing.expectEqualStrings("mymaster", master.name);
    try std.testing.expectEqualStrings("127.0.0.1", master.ip);
    try std.testing.expectEqual(@as(u16, 6379), master.port);
    try std.testing.expectEqual(@as(u8, 2), master.quorum);

    // Total master count should not change
    try std.testing.expectEqual(@as(usize, 1), sentinel.monitored_masters.count());
}

test "SentinelState.resetMaster: clears sentinel list" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    try sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);
    try sentinel.registerSentinel("mymaster", "s1", "127.0.0.2", 26379);
    try sentinel.registerSentinel("mymaster", "s2", "127.0.0.3", 26380);
    try sentinel.registerSentinel("mymaster", "s3", "127.0.0.4", 26381);

    // Should have 3 sentinels
    try std.testing.expectEqual(@as(usize, 3), sentinel.getMaster("mymaster").?.sentinels.count());

    // Reset
    _ = try sentinel.resetMaster("mymaster");

    // Sentinels should be cleared
    try std.testing.expectEqual(@as(usize, 0), sentinel.getMaster("mymaster").?.sentinels.count());
}

test "SentinelState.resetMaster: resets timestamps to current time" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    try sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    // Set old timestamps
    const master = sentinel.getMaster("mymaster").?;
    master.last_ping_time = 1000;
    master.last_pong_time = 2000;

    const before_reset = std.time.milliTimestamp();

    // Reset
    _ = try sentinel.resetMaster("mymaster");

    const after_reset = std.time.milliTimestamp();

    // Timestamps should be current (between before and after)
    const reset_master = sentinel.getMaster("mymaster").?;
    try std.testing.expect(reset_master.last_ping_time >= before_reset);
    try std.testing.expect(reset_master.last_ping_time <= after_reset + 10); // Small margin
    try std.testing.expect(reset_master.last_pong_time >= before_reset);
    try std.testing.expect(reset_master.last_pong_time <= after_reset + 10);
}

// ============================================================================
// SENTINEL FAILOVER Tests
// ============================================================================

test "SentinelState.forceFailover: forces failover for existing master" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    try sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    // Should succeed without error
    try sentinel.forceFailover("mymaster");

    // Master should still exist (failover is async operation)
    const master = sentinel.getMaster("mymaster").?;
    try std.testing.expectEqualStrings("mymaster", master.name);
}

test "SentinelState.forceFailover: returns error for unknown master" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    // Attempt to force failover on nonexistent master
    const result = sentinel.forceFailover("nonexistent");
    try std.testing.expectError(error.MasterNotFound, result);
}

test "SentinelState.forceFailover: can be called multiple times" {
    const allocator = std.testing.allocator;
    var sentinel = SentinelState.init(allocator);
    defer sentinel.deinit();

    try sentinel.monitorMaster("mymaster", "127.0.0.1", 6379, 2);

    // Multiple calls should all succeed
    try sentinel.forceFailover("mymaster");
    try sentinel.forceFailover("mymaster");
    try sentinel.forceFailover("mymaster");

    // Master should still exist
    try std.testing.expect(sentinel.getMaster("mymaster") != null);
}
