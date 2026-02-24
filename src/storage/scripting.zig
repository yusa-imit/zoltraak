const std = @import("std");
const Allocator = std.mem.Allocator;

/// Script storage for Lua scripts
pub const ScriptStore = struct {
    /// Map of SHA1 hash to script source
    scripts: std.StringHashMap([]const u8),
    allocator: Allocator,

    pub fn init(allocator: Allocator) ScriptStore {
        return .{
            .scripts = std.StringHashMap([]const u8).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ScriptStore) void {
        var it = self.scripts.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.scripts.deinit();
    }

    /// Load a script and return its SHA1 hash
    pub fn loadScript(self: *ScriptStore, script: []const u8) ![]const u8 {
        const sha1_hash = try computeSHA1(self.allocator, script);
        errdefer self.allocator.free(sha1_hash);

        // Check if script already exists
        if (self.scripts.get(sha1_hash)) |_| {
            return sha1_hash;
        }

        // Store the script
        const script_copy = try self.allocator.dupe(u8, script);
        errdefer self.allocator.free(script_copy);

        try self.scripts.put(sha1_hash, script_copy);
        return sha1_hash;
    }

    /// Check if a script exists
    pub fn exists(self: *ScriptStore, sha1: []const u8) bool {
        return self.scripts.contains(sha1);
    }

    /// Get a script by SHA1
    pub fn getScript(self: *ScriptStore, sha1: []const u8) ?[]const u8 {
        return self.scripts.get(sha1);
    }

    /// Flush all scripts
    pub fn flush(self: *ScriptStore) void {
        var it = self.scripts.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.scripts.clearRetainingCapacity();
    }
};

/// Compute SHA1 hash of input and return as hex string
fn computeSHA1(allocator: Allocator, input: []const u8) ![]const u8 {
    var hasher = std.crypto.hash.Sha1.init(.{});
    hasher.update(input);
    var hash: [20]u8 = undefined;
    hasher.final(&hash);

    // Convert to hex string using formatInt for each byte
    const hex = try allocator.alloc(u8, 40);
    for (hash, 0..) |byte, i| {
        _ = try std.fmt.bufPrint(hex[i * 2 .. i * 2 + 2], "{x:0>2}", .{byte});
    }
    return hex;
}

test "ScriptStore basic operations" {
    const allocator = std.testing.allocator;
    var store = ScriptStore.init(allocator);
    defer store.deinit();

    const script = "return redis.call('get', KEYS[1])";
    const sha1 = try store.loadScript(script);
    defer allocator.free(sha1);

    try std.testing.expect(store.exists(sha1));
    try std.testing.expectEqualStrings(script, store.getScript(sha1).?);
}

test "ScriptStore flush" {
    const allocator = std.testing.allocator;
    var store = ScriptStore.init(allocator);
    defer store.deinit();

    const script = "return 42";
    const sha1 = try store.loadScript(script);
    defer allocator.free(sha1);

    try std.testing.expect(store.exists(sha1));
    store.flush();
    try std.testing.expect(!store.exists(sha1));
}

test "ScriptStore duplicate load" {
    const allocator = std.testing.allocator;
    var store = ScriptStore.init(allocator);
    defer store.deinit();

    const script = "return ARGV[1]";
    const sha1_1 = try store.loadScript(script);
    defer allocator.free(sha1_1);
    const sha1_2 = try store.loadScript(script);
    defer allocator.free(sha1_2);

    try std.testing.expectEqualStrings(sha1_1, sha1_2);
}
