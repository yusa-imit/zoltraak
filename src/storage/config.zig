const std = @import("std");

/// Configuration parameter value types
pub const ConfigValue = union(enum) {
    string: []const u8,
    int: i64,
    bool: bool,

    pub fn deinit(self: *ConfigValue, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .string => |s| allocator.free(s),
            .int, .bool => {},
        }
    }

    pub fn clone(self: ConfigValue, allocator: std.mem.Allocator) !ConfigValue {
        return switch (self) {
            .string => |s| ConfigValue{ .string = try allocator.dupe(u8, s) },
            .int => |i| ConfigValue{ .int = i },
            .bool => |b| ConfigValue{ .bool = b },
        };
    }

    /// Format value as string for CONFIG GET response
    pub fn format(self: ConfigValue, allocator: std.mem.Allocator) ![]const u8 {
        return switch (self) {
            .string => |s| try allocator.dupe(u8, s),
            .int => |i| try std.fmt.allocPrint(allocator, "{d}", .{i}),
            .bool => |b| try allocator.dupe(u8, if (b) "yes" else "no"),
        };
    }
};

/// Configuration parameter metadata
pub const ConfigParam = struct {
    name: []const u8,
    default_value: ConfigValue,
    read_only: bool,

    pub fn deinit(self: *ConfigParam, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        var val = self.default_value;
        val.deinit(allocator);
    }
};

/// Runtime configuration for Zoltraak server
/// Thread-safe configuration management with parameter validation
pub const Config = struct {
    allocator: std.mem.Allocator,
    /// Parameter name (case-insensitive) -> current value
    params: std.StringHashMap(ConfigValue),
    /// Parameter metadata (name, default, read-only flag)
    metadata: std.ArrayList(ConfigParam),
    mutex: std.Thread.Mutex,

    /// Initialize configuration with Redis-compatible defaults
    pub fn init(allocator: std.mem.Allocator, port: u16, bind: []const u8) !*Config {
        const config = try allocator.create(Config);
        errdefer allocator.destroy(config);

        config.* = Config{
            .allocator = allocator,
            .params = std.StringHashMap(ConfigValue).init(allocator),
            .metadata = std.ArrayList(ConfigParam){},
            .mutex = std.Thread.Mutex{},
        };

        errdefer {
            config.params.deinit();
            for (config.metadata.items) |*meta| {
                meta.deinit(allocator);
            }
            config.metadata.deinit(allocator);
        }

        // Define default parameters
        const defaults = [_]struct {
            name: []const u8,
            value: ConfigValue,
            read_only: bool,
        }{
            // Memory management
            .{ .name = "maxmemory", .value = .{ .int = 0 }, .read_only = false }, // 0 = unlimited
            .{ .name = "maxmemory-policy", .value = .{ .string = "noeviction" }, .read_only = false },

            // Networking
            .{ .name = "timeout", .value = .{ .int = 0 }, .read_only = false }, // 0 = no timeout
            .{ .name = "tcp-keepalive", .value = .{ .int = 300 }, .read_only = false },
            .{ .name = "port", .value = .{ .int = @as(i64, port) }, .read_only = true },
            .{ .name = "bind", .value = .{ .string = bind }, .read_only = true },

            // Persistence
            .{ .name = "save", .value = .{ .string = "900 1 300 10 60 10000" }, .read_only = false },
            .{ .name = "appendonly", .value = .{ .bool = false }, .read_only = false },
            .{ .name = "appendfsync", .value = .{ .string = "everysec" }, .read_only = false },

            // Database
            .{ .name = "databases", .value = .{ .int = 1 }, .read_only = true }, // Zoltraak uses single DB
        };

        for (defaults) |def| {
            const name_lower = try std.ascii.allocLowerString(allocator, def.name);
            errdefer allocator.free(name_lower);

            const value = try def.value.clone(allocator);
            errdefer {
                var v = value;
                v.deinit(allocator);
            }

            try config.params.put(name_lower, value);

            // Create a separate copy of name_lower for metadata
            const meta_name = try allocator.dupe(u8, name_lower);
            errdefer allocator.free(meta_name);

            const meta = ConfigParam{
                .name = meta_name,
                .default_value = try def.value.clone(allocator),
                .read_only = def.read_only,
            };
            try config.metadata.append(allocator, meta);
        }

        return config;
    }

    /// Deinitialize configuration and free all memory
    pub fn deinit(self: *Config) void {
        self.mutex.lock();

        // Free parameter values
        var it = self.params.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var value = entry.value_ptr.*;
            value.deinit(self.allocator);
        }
        self.params.deinit();

        // Free metadata
        for (self.metadata.items) |*meta| {
            meta.deinit(self.allocator);
        }
        self.metadata.deinit(self.allocator);

        const allocator = self.allocator;
        self.mutex.unlock();
        allocator.destroy(self);
    }

    /// Get configuration parameter value
    /// Returns owned string representation, caller must free
    pub fn get(self: *Config, param_name: []const u8) !?[]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const name_lower = try std.ascii.allocLowerString(self.allocator, param_name);
        defer self.allocator.free(name_lower);

        const value = self.params.get(name_lower) orelse return null;
        return try value.format(self.allocator);
    }

    /// Set configuration parameter value at runtime
    /// Returns error if parameter is read-only or value is invalid
    pub fn set(self: *Config, param_name: []const u8, value_str: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const name_lower = try std.ascii.allocLowerString(self.allocator, param_name);
        defer self.allocator.free(name_lower);

        // Find metadata for validation
        var meta: ?*ConfigParam = null;
        for (self.metadata.items) |*m| {
            if (std.mem.eql(u8, m.name, name_lower)) {
                meta = m;
                break;
            }
        }

        const m = meta orelse return error.UnknownParameter;

        // Check read-only
        if (m.read_only) {
            return error.ReadOnlyParameter;
        }

        // Get current value to determine type
        const current = self.params.get(name_lower) orelse return error.UnknownParameter;

        // Parse new value based on current type
        const new_value = switch (current) {
            .string => blk: {
                const dup = try self.allocator.dupe(u8, value_str);

                // Validate specific string parameters
                if (std.mem.eql(u8, name_lower, "maxmemory-policy")) {
                    const valid_policies = [_][]const u8{
                        "noeviction",   "allkeys-lru",    "volatile-lru",
                        "allkeys-lfu",  "volatile-lfu",   "allkeys-random",
                        "volatile-random", "volatile-ttl",
                    };
                    var valid = false;
                    for (valid_policies) |policy| {
                        if (std.mem.eql(u8, dup, policy)) {
                            valid = true;
                            break;
                        }
                    }
                    if (!valid) {
                        self.allocator.free(dup);
                        return error.InvalidValue;
                    }
                } else if (std.mem.eql(u8, name_lower, "appendfsync")) {
                    const valid_modes = [_][]const u8{ "always", "everysec", "no" };
                    var valid = false;
                    for (valid_modes) |mode| {
                        if (std.mem.eql(u8, dup, mode)) {
                            valid = true;
                            break;
                        }
                    }
                    if (!valid) {
                        self.allocator.free(dup);
                        return error.InvalidValue;
                    }
                }

                break :blk ConfigValue{ .string = dup };
            },
            .int => blk: {
                const parsed = std.fmt.parseInt(i64, value_str, 10) catch return error.InvalidValue;
                // Validate range for specific parameters
                if (std.mem.eql(u8, name_lower, "maxmemory") or
                    std.mem.eql(u8, name_lower, "timeout") or
                    std.mem.eql(u8, name_lower, "tcp-keepalive"))
                {
                    if (parsed < 0) return error.InvalidValue;
                }
                break :blk ConfigValue{ .int = parsed };
            },
            .bool => blk: {
                const lower = try std.ascii.allocLowerString(self.allocator, value_str);
                defer self.allocator.free(lower);

                const parsed = if (std.mem.eql(u8, lower, "yes") or std.mem.eql(u8, lower, "true") or std.mem.eql(u8, lower, "1"))
                    true
                else if (std.mem.eql(u8, lower, "no") or std.mem.eql(u8, lower, "false") or std.mem.eql(u8, lower, "0"))
                    false
                else
                    return error.InvalidValue;

                break :blk ConfigValue{ .bool = parsed };
            },
        };

        // Replace the value in the HashMap
        // The HashMap already has the key, so we just update the value
        const gop = try self.params.getOrPut(name_lower);
        if (gop.found_existing) {
            // Free old value
            var old_val = gop.value_ptr.*;
            old_val.deinit(self.allocator);
            // Store new value
            gop.value_ptr.* = new_value;
        } else {
            // This shouldn't happen since we validated the parameter exists
            return error.UnknownParameter;
        }
    }

    /// Get all parameter names matching glob pattern
    /// Returns owned array of owned strings, caller must free all
    pub fn getMatching(self: *Config, pattern: []const u8) ![][]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var matches = std.ArrayList([]const u8){};
        errdefer {
            for (matches.items) |match| {
                self.allocator.free(match);
            }
            matches.deinit(self.allocator);
        }

        var it = self.params.keyIterator();
        while (it.next()) |key| {
            if (try globMatch(pattern, key.*)) {
                const dup = try self.allocator.dupe(u8, key.*);
                try matches.append(self.allocator, dup);
            }
        }

        return matches.toOwnedSlice(self.allocator);
    }

    /// Reset all statistics (placeholder for future stats implementation)
    pub fn resetStats(self: *Config) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        // In a full implementation, this would reset INFO stats like:
        // - total_commands_processed
        // - total_connections_received
        // - keyspace_hits/misses
        // For now, this is a no-op as stats are tracked elsewhere
    }
};

/// Glob pattern matching for CONFIG GET
/// Supports *, ?, and [abc] patterns
fn globMatch(pattern: []const u8, text: []const u8) !bool {
    var p_idx: usize = 0;
    var t_idx: usize = 0;

    while (p_idx < pattern.len) {
        const p_char = pattern[p_idx];

        if (p_char == '*') {
            // Wildcard - match zero or more characters
            if (p_idx == pattern.len - 1) {
                // Star at end matches rest of string
                return true;
            }
            // Try matching rest of pattern at each position
            while (t_idx <= text.len) : (t_idx += 1) {
                if (try globMatch(pattern[p_idx + 1 ..], text[t_idx..])) {
                    return true;
                }
            }
            return false;
        } else if (p_char == '?') {
            // Single character wildcard
            if (t_idx >= text.len) return false;
            p_idx += 1;
            t_idx += 1;
        } else if (p_char == '[') {
            // Character class [abc]
            if (t_idx >= text.len) return false;

            const close = std.mem.indexOfScalarPos(u8, pattern, p_idx, ']') orelse return false;
            const char_class = pattern[p_idx + 1 .. close];
            const t_char = text[t_idx];

            var matched = false;
            for (char_class) |cc| {
                if (cc == t_char) {
                    matched = true;
                    break;
                }
            }
            if (!matched) return false;

            p_idx = close + 1;
            t_idx += 1;
        } else {
            // Literal character
            if (t_idx >= text.len or pattern[p_idx] != text[t_idx]) return false;
            p_idx += 1;
            t_idx += 1;
        }
    }

    return t_idx == text.len;
}

// ── Tests ────────────────────────────────────────────────────────────────────

test "Config.init creates default parameters" {
    const allocator = std.testing.allocator;

    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Check default values
    const maxmemory = try config.get("maxmemory");
    defer if (maxmemory) |m| allocator.free(m);
    try std.testing.expect(maxmemory != null);
    try std.testing.expectEqualStrings("0", maxmemory.?);

    const port = try config.get("port");
    defer if (port) |p| allocator.free(p);
    try std.testing.expect(port != null);
    try std.testing.expectEqualStrings("6379", port.?);

    const bind = try config.get("bind");
    defer if (bind) |b| allocator.free(b);
    try std.testing.expect(bind != null);
    try std.testing.expectEqualStrings("127.0.0.1", bind.?);
}

test "Config.set updates writable parameters" {
    const allocator = std.testing.allocator;

    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Set maxmemory
    try config.set("maxmemory", "1073741824");

    const maxmemory = try config.get("maxmemory");
    defer if (maxmemory) |m| allocator.free(m);
    try std.testing.expectEqualStrings("1073741824", maxmemory.?);

    // Set boolean parameter
    try config.set("appendonly", "yes");

    const appendonly = try config.get("appendonly");
    defer if (appendonly) |a| allocator.free(a);
    try std.testing.expectEqualStrings("yes", appendonly.?);
}

test "Config.set rejects read-only parameters" {
    const allocator = std.testing.allocator;

    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Attempt to set read-only port
    const result = config.set("port", "8080");
    try std.testing.expectError(error.ReadOnlyParameter, result);

    // Verify port unchanged
    const port = try config.get("port");
    defer if (port) |p| allocator.free(p);
    try std.testing.expectEqualStrings("6379", port.?);
}

test "Config.set validates parameter values" {
    const allocator = std.testing.allocator;

    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Invalid integer
    const bad_int = config.set("maxmemory", "not-a-number");
    try std.testing.expectError(error.InvalidValue, bad_int);

    // Invalid boolean
    const bad_bool = config.set("appendonly", "maybe");
    try std.testing.expectError(error.InvalidValue, bad_bool);

    // Invalid policy
    const bad_policy = config.set("maxmemory-policy", "delete-everything");
    try std.testing.expectError(error.InvalidValue, bad_policy);

    // Valid policy
    try config.set("maxmemory-policy", "allkeys-lru");
    const policy = try config.get("maxmemory-policy");
    defer if (policy) |p| allocator.free(p);
    try std.testing.expectEqualStrings("allkeys-lru", policy.?);
}

test "Config.getMatching with glob patterns" {
    const allocator = std.testing.allocator;

    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Match all
    {
        const matches = try config.getMatching("*");
        defer {
            for (matches) |match| {
                allocator.free(match);
            }
            allocator.free(matches);
        }
        try std.testing.expect(matches.len > 0);
    }

    // Match maxmemory*
    {
        const matches = try config.getMatching("maxmemory*");
        defer {
            for (matches) |match| {
                allocator.free(match);
            }
            allocator.free(matches);
        }
        try std.testing.expect(matches.len >= 2); // maxmemory, maxmemory-policy
    }

    // Match append*
    {
        const matches = try config.getMatching("append*");
        defer {
            for (matches) |match| {
                allocator.free(match);
            }
            allocator.free(matches);
        }
        try std.testing.expect(matches.len >= 2); // appendonly, appendfsync
    }

    // No match
    {
        const matches = try config.getMatching("nonexistent*");
        defer allocator.free(matches);
        try std.testing.expectEqual(@as(usize, 0), matches.len);
    }
}

test "globMatch patterns" {
    // Wildcard *
    try std.testing.expect(try globMatch("*", "anything"));
    try std.testing.expect(try globMatch("max*", "maxmemory"));
    try std.testing.expect(try globMatch("*memory", "maxmemory"));
    try std.testing.expect(try globMatch("max*policy", "maxmemory-policy"));

    // Single character ?
    try std.testing.expect(try globMatch("p?rt", "port"));
    try std.testing.expect(try globMatch("p?rt", "part")); // ? matches any single char
    try std.testing.expect(!try globMatch("p?rt", "ports")); // too long
    try std.testing.expect(!try globMatch("p?rt", "prt")); // too short

    // Character class []
    try std.testing.expect(try globMatch("[abc]ind", "bind"));
    try std.testing.expect(!try globMatch("[abc]ind", "find"));
    try std.testing.expect(try globMatch("[pP]ort", "port"));
    try std.testing.expect(try globMatch("[pP]ort", "Port"));

    // Combined patterns
    try std.testing.expect(try globMatch("max*-*", "maxmemory-policy"));
    try std.testing.expect(!try globMatch("max*-*", "maxmemory"));

    // Exact match
    try std.testing.expect(try globMatch("port", "port"));
    try std.testing.expect(!try globMatch("port", "ports"));
}

test "Config.set case insensitive" {
    const allocator = std.testing.allocator;

    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    // Set with mixed case
    try config.set("MaxMemory", "2048");

    // Get with different case
    const val1 = try config.get("maxmemory");
    defer if (val1) |v| allocator.free(v);
    try std.testing.expectEqualStrings("2048", val1.?);

    const val2 = try config.get("MAXMEMORY");
    defer if (val2) |v| allocator.free(v);
    try std.testing.expectEqualStrings("2048", val2.?);
}

test "Config.get returns null for unknown parameter" {
    const allocator = std.testing.allocator;

    const config = try Config.init(allocator, 6379, "127.0.0.1");
    defer config.deinit();

    const result = try config.get("nonexistent");
    try std.testing.expect(result == null);
}

test "ConfigValue.format conversions" {
    const allocator = std.testing.allocator;

    // String value
    {
        var val = ConfigValue{ .string = try allocator.dupe(u8, "test") };
        defer val.deinit(allocator);

        const formatted = try val.format(allocator);
        defer allocator.free(formatted);
        try std.testing.expectEqualStrings("test", formatted);
    }

    // Integer value
    {
        const val = ConfigValue{ .int = 12345 };

        const formatted = try val.format(allocator);
        defer allocator.free(formatted);
        try std.testing.expectEqualStrings("12345", formatted);
    }

    // Boolean values
    {
        const val_true = ConfigValue{ .bool = true };
        const val_false = ConfigValue{ .bool = false };

        const fmt_true = try val_true.format(allocator);
        defer allocator.free(fmt_true);
        try std.testing.expectEqualStrings("yes", fmt_true);

        const fmt_false = try val_false.format(allocator);
        defer allocator.free(fmt_false);
        try std.testing.expectEqualStrings("no", fmt_false);
    }
}
