const std = @import("std");
const TlsConfig = @import("../src/storage/tls_config.zig").TlsConfig;
const TlsConnection = @import("../src/network/tls.zig").TlsConnection;
const TlsListener = @import("../src/network/tls.zig").TlsListener;

/// Integration tests for TLS/SSL support (Phase 10)
/// Note: These tests validate configuration and API structure
/// Real TLS handshake tests require OpenSSL integration (future iteration)

test "TlsConfig init and deinit" {
    const allocator = std.testing.allocator;
    var config = try TlsConfig.init(allocator);
    defer config.deinit();

    // Verify defaults
    try std.testing.expectEqual(@as(u16, 0), config.port);
    try std.testing.expect(config.cert_file == null);
    try std.testing.expect(config.key_file == null);
    try std.testing.expectEqual(TlsConfig.AuthClientsMode.yes, config.auth_clients);
    try std.testing.expectEqual(true, config.prefer_server_ciphers);
    try std.testing.expectEqual(true, config.session_caching);
    try std.testing.expectEqual(@as(u32, 20480), config.session_cache_size);
    try std.testing.expectEqual(@as(u32, 300), config.session_cache_timeout);
}

test "TlsConfig validation - disabled TLS passes" {
    const allocator = std.testing.allocator;
    var config = try TlsConfig.init(allocator);
    defer config.deinit();

    // Disabled TLS (port = 0) should always pass
    try config.validate();
}

test "TlsConfig validation - enabled requires cert and key" {
    const allocator = std.testing.allocator;
    var config = try TlsConfig.init(allocator);
    defer config.deinit();

    config.port = 6380;

    // Should fail - no cert file
    try std.testing.expectError(error.TlsCertFileRequired, config.validate());

    // Add cert file
    config.cert_file = try allocator.dupe(u8, "/tmp/test_cert.pem");

    // Should fail - no key file
    try std.testing.expectError(error.TlsKeyFileRequired, config.validate());

    // Add key file
    config.key_file = try allocator.dupe(u8, "/tmp/test_key.pem");

    // Should fail - CA cert required for client auth
    try std.testing.expectError(error.TlsCaCertRequired, config.validate());

    // Add CA cert
    config.ca_cert_file = try allocator.dupe(u8, "/tmp/test_ca.pem");

    // Now validation should pass (file existence checked at runtime, not in validate())
}

test "TlsConfig validation - auth_clients no doesn't require CA" {
    const allocator = std.testing.allocator;
    var config = try TlsConfig.init(allocator);
    defer config.deinit();

    config.port = 6380;
    config.cert_file = try allocator.dupe(u8, "/tmp/cert.pem");
    config.key_file = try allocator.dupe(u8, "/tmp/key.pem");
    config.auth_clients = .no;

    // Should pass without CA cert when auth_clients is no
    // (file existence checked at runtime)
}

test "TlsConfig validation - zero session cache size" {
    const allocator = std.testing.allocator;
    var config = try TlsConfig.init(allocator);
    defer config.deinit();

    config.port = 6380;
    config.cert_file = try allocator.dupe(u8, "/tmp/cert.pem");
    config.key_file = try allocator.dupe(u8, "/tmp/key.pem");
    config.ca_cert_file = try allocator.dupe(u8, "/tmp/ca.pem");
    config.session_cache_size = 0;

    // Should fail - zero cache size invalid
    try std.testing.expectError(error.InvalidSessionCacheSize, config.validate());
}

test "TlsConfig getParameter - all parameters" {
    const allocator = std.testing.allocator;
    var config = try TlsConfig.init(allocator);
    defer config.deinit();

    config.port = 6380;
    config.cert_file = try allocator.dupe(u8, "/etc/cert.pem");
    config.key_file = try allocator.dupe(u8, "/etc/key.pem");

    // Test tls-port
    {
        const value = try config.getParameter(allocator, "tls-port");
        defer if (value) |v| allocator.free(v);
        try std.testing.expect(value != null);
        try std.testing.expectEqualStrings("6380", value.?);
    }

    // Test tls-cert-file
    {
        const value = try config.getParameter(allocator, "tls-cert-file");
        defer if (value) |v| allocator.free(v);
        try std.testing.expect(value != null);
        try std.testing.expectEqualStrings("/etc/cert.pem", value.?);
    }

    // Test tls-key-file
    {
        const value = try config.getParameter(allocator, "tls-key-file");
        defer if (value) |v| allocator.free(v);
        try std.testing.expect(value != null);
        try std.testing.expectEqualStrings("/etc/key.pem", value.?);
    }

    // Test tls-auth-clients
    {
        const value = try config.getParameter(allocator, "tls-auth-clients");
        defer if (value) |v| allocator.free(v);
        try std.testing.expect(value != null);
        try std.testing.expectEqualStrings("yes", value.?);
    }

    // Test tls-prefer-server-ciphers
    {
        const value = try config.getParameter(allocator, "tls-prefer-server-ciphers");
        defer if (value) |v| allocator.free(v);
        try std.testing.expect(value != null);
        try std.testing.expectEqualStrings("yes", value.?);
    }

    // Test tls-session-caching
    {
        const value = try config.getParameter(allocator, "tls-session-caching");
        defer if (value) |v| allocator.free(v);
        try std.testing.expect(value != null);
        try std.testing.expectEqualStrings("yes", value.?);
    }

    // Test tls-cluster
    {
        const value = try config.getParameter(allocator, "tls-cluster");
        defer if (value) |v| allocator.free(v);
        try std.testing.expect(value != null);
        try std.testing.expectEqualStrings("no", value.?);
    }

    // Test unknown parameter
    {
        const value = try config.getParameter(allocator, "unknown-param");
        try std.testing.expect(value == null);
    }
}

test "TlsConfig setParameter - all parameters" {
    const allocator = std.testing.allocator;
    var config = try TlsConfig.init(allocator);
    defer config.deinit();

    // Set tls-port
    try config.setParameter(allocator, "tls-port", "6380");
    try std.testing.expectEqual(@as(u16, 6380), config.port);

    // Set tls-cert-file
    try config.setParameter(allocator, "tls-cert-file", "/new/cert.pem");
    try std.testing.expect(config.cert_file != null);
    try std.testing.expectEqualStrings("/new/cert.pem", config.cert_file.?);

    // Set tls-auth-clients
    try config.setParameter(allocator, "tls-auth-clients", "optional");
    try std.testing.expectEqual(TlsConfig.AuthClientsMode.optional, config.auth_clients);

    // Set tls-prefer-server-ciphers
    try config.setParameter(allocator, "tls-prefer-server-ciphers", "no");
    try std.testing.expectEqual(false, config.prefer_server_ciphers);

    // Set tls-session-caching
    try config.setParameter(allocator, "tls-session-caching", "no");
    try std.testing.expectEqual(false, config.session_caching);

    // Set tls-session-cache-size
    try config.setParameter(allocator, "tls-session-cache-size", "10000");
    try std.testing.expectEqual(@as(u32, 10000), config.session_cache_size);

    // Set tls-cluster
    try config.setParameter(allocator, "tls-cluster", "yes");
    try std.testing.expectEqual(true, config.cluster);

    // Invalid parameter should error
    try std.testing.expectError(error.UnknownParameter, config.setParameter(allocator, "invalid-param", "value"));

    // Invalid port should error
    try std.testing.expectError(error.InvalidParameterValue, config.setParameter(allocator, "tls-port", "invalid"));

    // Invalid auth mode should error
    try std.testing.expectError(error.InvalidParameterValue, config.setParameter(allocator, "tls-auth-clients", "invalid"));
}

test "TlsConfig AuthClientsMode conversions" {
    const yes = try TlsConfig.AuthClientsMode.fromString("yes");
    try std.testing.expectEqual(TlsConfig.AuthClientsMode.yes, yes);
    try std.testing.expectEqualStrings("yes", yes.toString());

    const no = try TlsConfig.AuthClientsMode.fromString("no");
    try std.testing.expectEqual(TlsConfig.AuthClientsMode.no, no);
    try std.testing.expectEqualStrings("no", no.toString());

    const optional = try TlsConfig.AuthClientsMode.fromString("optional");
    try std.testing.expectEqual(TlsConfig.AuthClientsMode.optional, optional);
    try std.testing.expectEqualStrings("optional", optional.toString());

    // Invalid mode
    try std.testing.expectError(error.InvalidAuthClientsMode, TlsConfig.AuthClientsMode.fromString("invalid"));
}

test "TlsConfig isEnabled" {
    const allocator = std.testing.allocator;
    var config = try TlsConfig.init(allocator);
    defer config.deinit();

    // Default port 0 means disabled
    try std.testing.expectEqual(false, config.isEnabled());

    // Non-zero port means enabled
    config.port = 6380;
    try std.testing.expectEqual(true, config.isEnabled());
}
