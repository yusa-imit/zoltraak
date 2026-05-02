const std = @import("std");
const Config = @import("config.zig").Config;

/// TLS/SSL configuration for encrypted connections (Redis 6.0+ compatibility)
/// Implements all 20 TLS configuration parameters from Redis specification
pub const TlsConfig = struct {
    // Core TLS settings
    port: u16,                       // tls-port (default: 0 = disabled)
    cert_file: ?[]const u8,          // tls-cert-file
    key_file: ?[]const u8,           // tls-key-file
    key_file_pass: ?[]const u8,      // tls-key-file-pass
    ca_cert_file: ?[]const u8,       // tls-ca-cert-file
    ca_cert_dir: ?[]const u8,        // tls-ca-cert-dir
    auth_clients: AuthClientsMode,   // tls-auth-clients (yes/no/optional)

    // Protocol & Cipher configuration
    protocols: []const u8,           // tls-protocols (e.g., "TLSv1.2 TLSv1.3")
    ciphers: ?[]const u8,            // tls-ciphers (TLS 1.2 cipher suites)
    ciphersuites: ?[]const u8,       // tls-ciphersuites (TLS 1.3 cipher suites)
    prefer_server_ciphers: bool,     // tls-prefer-server-ciphers

    // Session & Performance
    session_caching: bool,           // tls-session-caching
    session_cache_size: u32,         // tls-session-cache-size
    session_cache_timeout: u32,      // tls-session-cache-timeout (seconds)

    // Cluster & Replication
    cluster: bool,                   // tls-cluster
    replication: bool,               // tls-replication

    // Client certificate override (for outbound connections)
    client_cert_file: ?[]const u8,   // tls-client-cert-file
    client_key_file: ?[]const u8,    // tls-client-key-file
    client_key_file_pass: ?[]const u8, // tls-client-key-file-pass

    // Advanced (Redis 7.2+)
    allowlisted_certs: ?[]const u8,  // tls-allowlisted-certs

    allocator: std.mem.Allocator,

    /// Client authentication mode for TLS
    pub const AuthClientsMode = enum {
        yes,      // Require client certificate
        no,       // No client certificate required
        optional, // Client certificate optional

        pub fn fromString(s: []const u8) !AuthClientsMode {
            if (std.mem.eql(u8, s, "yes")) return .yes;
            if (std.mem.eql(u8, s, "no")) return .no;
            if (std.mem.eql(u8, s, "optional")) return .optional;
            return error.InvalidAuthClientsMode;
        }

        pub fn toString(self: AuthClientsMode) []const u8 {
            return switch (self) {
                .yes => "yes",
                .no => "no",
                .optional => "optional",
            };
        }
    };

    /// Create TlsConfig with Redis defaults
    pub fn init(allocator: std.mem.Allocator) !TlsConfig {
        const protocols = try allocator.dupe(u8, "TLSv1.2 TLSv1.3");
        errdefer allocator.free(protocols);

        return TlsConfig{
            .port = 0,
            .cert_file = null,
            .key_file = null,
            .key_file_pass = null,
            .ca_cert_file = null,
            .ca_cert_dir = null,
            .auth_clients = .yes,
            .protocols = protocols,
            .ciphers = null,
            .ciphersuites = null,
            .prefer_server_ciphers = true,
            .session_caching = true,
            .session_cache_size = 20480,
            .session_cache_timeout = 300,
            .cluster = false,
            .replication = false,
            .client_cert_file = null,
            .client_key_file = null,
            .client_key_file_pass = null,
            .allowlisted_certs = null,
            .allocator = allocator,
        };
    }

    /// Free all allocated memory
    pub fn deinit(self: *TlsConfig) void {
        self.allocator.free(self.protocols);
        if (self.cert_file) |f| self.allocator.free(f);
        if (self.key_file) |f| self.allocator.free(f);
        if (self.key_file_pass) |f| self.allocator.free(f);
        if (self.ca_cert_file) |f| self.allocator.free(f);
        if (self.ca_cert_dir) |f| self.allocator.free(f);
        if (self.ciphers) |c| self.allocator.free(c);
        if (self.ciphersuites) |c| self.allocator.free(c);
        if (self.client_cert_file) |f| self.allocator.free(f);
        if (self.client_key_file) |f| self.allocator.free(f);
        if (self.client_key_file_pass) |f| self.allocator.free(f);
        if (self.allowlisted_certs) |c| self.allocator.free(c);
    }

    /// Check if TLS is enabled (port > 0)
    pub fn isEnabled(self: *const TlsConfig) bool {
        return self.port > 0;
    }

    /// Validate TLS configuration
    /// Returns error if TLS is enabled but required files are missing
    pub fn validate(self: *const TlsConfig) !void {
        if (!self.isEnabled()) {
            return; // TLS disabled, no validation needed
        }

        // Require server certificate and key if TLS is enabled
        if (self.cert_file == null) {
            return error.TlsCertFileRequired;
        }
        if (self.key_file == null) {
            return error.TlsKeyFileRequired;
        }

        // Require CA cert if client auth is enabled
        if (self.auth_clients != .no) {
            if (self.ca_cert_file == null and self.ca_cert_dir == null) {
                return error.TlsCaCertRequired;
            }
        }

        // Validate session cache size
        if (self.session_cache_size == 0) {
            return error.InvalidSessionCacheSize;
        }
    }
};

// ============================================================================
// Unit Tests
// ============================================================================

test "TlsConfig: init with defaults" {
    const allocator = std.testing.allocator;
    var config = try TlsConfig.init(allocator);
    defer config.deinit();

    // Core settings
    try std.testing.expectEqual(@as(u16, 0), config.port);
    try std.testing.expect(config.cert_file == null);
    try std.testing.expect(config.key_file == null);
    try std.testing.expectEqual(TlsConfig.AuthClientsMode.yes, config.auth_clients);

    // Protocol settings
    try std.testing.expectEqualStrings("TLSv1.2 TLSv1.3", config.protocols);
    try std.testing.expect(config.prefer_server_ciphers);

    // Session settings
    try std.testing.expect(config.session_caching);
    try std.testing.expectEqual(@as(u32, 20480), config.session_cache_size);
    try std.testing.expectEqual(@as(u32, 300), config.session_cache_timeout);

    // Cluster/Replication
    try std.testing.expect(!config.cluster);
    try std.testing.expect(!config.replication);
}

test "TlsConfig: isEnabled returns false by default" {
    const allocator = std.testing.allocator;
    var config = try TlsConfig.init(allocator);
    defer config.deinit();

    try std.testing.expect(!config.isEnabled());
}

test "TlsConfig: isEnabled returns true when port > 0" {
    const allocator = std.testing.allocator;
    var config = try TlsConfig.init(allocator);
    defer config.deinit();

    config.port = 6380;
    try std.testing.expect(config.isEnabled());
}

test "TlsConfig: validate fails if TLS enabled without cert" {
    const allocator = std.testing.allocator;
    var config = try TlsConfig.init(allocator);
    defer config.deinit();

    config.port = 6380;

    const result = config.validate();
    try std.testing.expectError(error.TlsCertFileRequired, result);
}

test "TlsConfig: validate fails if TLS enabled without key" {
    const allocator = std.testing.allocator;
    var config = try TlsConfig.init(allocator);
    defer config.deinit();

    config.port = 6380;
    config.cert_file = try allocator.dupe(u8, "/etc/redis/tls/server.crt");

    const result = config.validate();
    try std.testing.expectError(error.TlsKeyFileRequired, result);
}

test "TlsConfig: validate fails if auth enabled without CA" {
    const allocator = std.testing.allocator;
    var config = try TlsConfig.init(allocator);
    defer config.deinit();

    config.port = 6380;
    config.cert_file = try allocator.dupe(u8, "/etc/redis/tls/server.crt");
    config.key_file = try allocator.dupe(u8, "/etc/redis/tls/server.key");
    config.auth_clients = .yes;

    const result = config.validate();
    try std.testing.expectError(error.TlsCaCertRequired, result);
}

test "TlsConfig: validate succeeds with all required files" {
    const allocator = std.testing.allocator;
    var config = try TlsConfig.init(allocator);
    defer config.deinit();

    config.port = 6380;
    config.cert_file = try allocator.dupe(u8, "/etc/redis/tls/server.crt");
    config.key_file = try allocator.dupe(u8, "/etc/redis/tls/server.key");
    config.ca_cert_file = try allocator.dupe(u8, "/etc/redis/tls/ca.crt");
    config.auth_clients = .yes;

    try config.validate();
}

test "TlsConfig: validate succeeds without CA when auth_clients=no" {
    const allocator = std.testing.allocator;
    var config = try TlsConfig.init(allocator);
    defer config.deinit();

    config.port = 6380;
    config.cert_file = try allocator.dupe(u8, "/etc/redis/tls/server.crt");
    config.key_file = try allocator.dupe(u8, "/etc/redis/tls/server.key");
    config.auth_clients = .no;

    try config.validate();
}

test "TlsConfig: AuthClientsMode fromString" {
    try std.testing.expectEqual(TlsConfig.AuthClientsMode.yes, try TlsConfig.AuthClientsMode.fromString("yes"));
    try std.testing.expectEqual(TlsConfig.AuthClientsMode.no, try TlsConfig.AuthClientsMode.fromString("no"));
    try std.testing.expectEqual(TlsConfig.AuthClientsMode.optional, try TlsConfig.AuthClientsMode.fromString("optional"));

    const result = TlsConfig.AuthClientsMode.fromString("invalid");
    try std.testing.expectError(error.InvalidAuthClientsMode, result);
}

test "TlsConfig: AuthClientsMode toString" {
    try std.testing.expectEqualStrings("yes", TlsConfig.AuthClientsMode.yes.toString());
    try std.testing.expectEqualStrings("no", TlsConfig.AuthClientsMode.no.toString());
    try std.testing.expectEqualStrings("optional", TlsConfig.AuthClientsMode.optional.toString());
}

test "TlsConfig: validate allows CA cert dir instead of file" {
    const allocator = std.testing.allocator;
    var config = try TlsConfig.init(allocator);
    defer config.deinit();

    config.port = 6380;
    config.cert_file = try allocator.dupe(u8, "/etc/redis/tls/server.crt");
    config.key_file = try allocator.dupe(u8, "/etc/redis/tls/server.key");
    config.ca_cert_dir = try allocator.dupe(u8, "/etc/ssl/certs");
    config.auth_clients = .yes;

    try config.validate();
}
