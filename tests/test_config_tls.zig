const std = @import("std");
const protocol = @import("../src/protocol/parser.zig");
const storage_mod = @import("../src/storage/memory.zig");
const config_cmd = @import("../src/commands/config.zig");

const RespValue = protocol.RespValue;
const Storage = storage_mod.Storage;

// ============================================================================
// Integration Tests for CONFIG GET/SET with TLS Parameters
// ============================================================================

test "CONFIG GET tls-port returns default value" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "tls-port" },
    };

    const response = try config_cmd.executeConfigCommand(allocator, storage, &args);
    defer allocator.free(response);

    // Should return *2\r\n$8\r\ntls-port\r\n$1\r\n0\r\n
    try std.testing.expect(std.mem.indexOf(u8, response, "tls-port") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "$1\r\n0\r\n") != null);
}

test "CONFIG GET tls-cert-file returns empty string by default" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "tls-cert-file" },
    };

    const response = try config_cmd.executeConfigCommand(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "tls-cert-file") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "$0\r\n\r\n") != null);
}

test "CONFIG GET tls-auth-clients returns yes by default" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "tls-auth-clients" },
    };

    const response = try config_cmd.executeConfigCommand(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "tls-auth-clients") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "yes") != null);
}

test "CONFIG GET tls-protocols returns default" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "tls-protocols" },
    };

    const response = try config_cmd.executeConfigCommand(allocator, storage, &args);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "tls-protocols") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "TLSv1.2 TLSv1.3") != null);
}

test "CONFIG GET tls-* (wildcard) returns all TLS parameters" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "tls-*" },
    };

    const response = try config_cmd.executeConfigCommand(allocator, storage, &args);
    defer allocator.free(response);

    // Should return all 20 TLS parameters (40 elements: name + value for each)
    try std.testing.expect(std.mem.indexOf(u8, response, "tls-port") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "tls-cert-file") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "tls-auth-clients") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "tls-cluster") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "tls-replication") != null);
}

test "CONFIG SET tls-port updates value" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "tls-port" },
        RespValue{ .bulk_string = "6380" },
    };

    const set_response = try config_cmd.executeConfigCommand(allocator, storage, &set_args);
    defer allocator.free(set_response);

    try std.testing.expect(std.mem.indexOf(u8, set_response, "+OK") != null);

    // Verify value was updated
    try std.testing.expectEqual(@as(u16, 6380), storage.tls_config.port);

    // Verify via CONFIG GET
    const get_args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "GET" },
        RespValue{ .bulk_string = "tls-port" },
    };

    const get_response = try config_cmd.executeConfigCommand(allocator, storage, &get_args);
    defer allocator.free(get_response);

    try std.testing.expect(std.mem.indexOf(u8, get_response, "6380") != null);
}

test "CONFIG SET tls-cert-file updates value" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "tls-cert-file" },
        RespValue{ .bulk_string = "/etc/redis/server.crt" },
    };

    const set_response = try config_cmd.executeConfigCommand(allocator, storage, &set_args);
    defer allocator.free(set_response);

    try std.testing.expect(std.mem.indexOf(u8, set_response, "+OK") != null);

    // Verify value was updated
    try std.testing.expectEqualStrings("/etc/redis/server.crt", storage.tls_config.cert_file.?);
}

test "CONFIG SET tls-auth-clients with valid value" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "tls-auth-clients" },
        RespValue{ .bulk_string = "optional" },
    };

    const set_response = try config_cmd.executeConfigCommand(allocator, storage, &set_args);
    defer allocator.free(set_response);

    try std.testing.expect(std.mem.indexOf(u8, set_response, "+OK") != null);

    // Verify value was updated
    const TlsConfig = @import("../src/storage/tls_config.zig").TlsConfig;
    try std.testing.expectEqual(TlsConfig.AuthClientsMode.optional, storage.tls_config.auth_clients);
}

test "CONFIG SET tls-auth-clients with invalid value returns error" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "tls-auth-clients" },
        RespValue{ .bulk_string = "invalid" },
    };

    const set_response = try config_cmd.executeConfigCommand(allocator, storage, &set_args);
    defer allocator.free(set_response);

    try std.testing.expect(std.mem.indexOf(u8, set_response, "-ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, set_response, "tls-auth-clients") != null);
}

test "CONFIG SET tls-cluster boolean value" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "tls-cluster" },
        RespValue{ .bulk_string = "yes" },
    };

    const set_response = try config_cmd.executeConfigCommand(allocator, storage, &set_args);
    defer allocator.free(set_response);

    try std.testing.expect(std.mem.indexOf(u8, set_response, "+OK") != null);

    // Verify value was updated
    try std.testing.expect(storage.tls_config.cluster);
}

test "CONFIG SET tls-session-cache-size integer value" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "tls-session-cache-size" },
        RespValue{ .bulk_string = "40960" },
    };

    const set_response = try config_cmd.executeConfigCommand(allocator, storage, &set_args);
    defer allocator.free(set_response);

    try std.testing.expect(std.mem.indexOf(u8, set_response, "+OK") != null);

    // Verify value was updated
    try std.testing.expectEqual(@as(u32, 40960), storage.tls_config.session_cache_size);
}

test "CONFIG SET multiple TLS parameters" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "tls-port" },
        RespValue{ .bulk_string = "6443" },
        RespValue{ .bulk_string = "tls-replication" },
        RespValue{ .bulk_string = "yes" },
    };

    const set_response = try config_cmd.executeConfigCommand(allocator, storage, &set_args);
    defer allocator.free(set_response);

    try std.testing.expect(std.mem.indexOf(u8, set_response, "+OK") != null);

    // Verify both values were updated
    try std.testing.expectEqual(@as(u16, 6443), storage.tls_config.port);
    try std.testing.expect(storage.tls_config.replication);
}

test "CONFIG SET tls-cert-file empty string clears value" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // First set a value
    storage.tls_config.cert_file = try allocator.dupe(u8, "/old/cert.pem");

    // Then clear it
    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "tls-cert-file" },
        RespValue{ .bulk_string = "" },
    };

    const set_response = try config_cmd.executeConfigCommand(allocator, storage, &set_args);
    defer allocator.free(set_response);

    try std.testing.expect(std.mem.indexOf(u8, set_response, "+OK") != null);

    // Verify value was cleared
    try std.testing.expect(storage.tls_config.cert_file == null);
}

test "CONFIG SET case-insensitive TLS parameter names" {
    const allocator = std.testing.allocator;

    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const set_args = [_]RespValue{
        RespValue{ .bulk_string = "CONFIG" },
        RespValue{ .bulk_string = "SET" },
        RespValue{ .bulk_string = "TLS-PORT" }, // uppercase
        RespValue{ .bulk_string = "6443" },
    };

    const set_response = try config_cmd.executeConfigCommand(allocator, storage, &set_args);
    defer allocator.free(set_response);

    try std.testing.expect(std.mem.indexOf(u8, set_response, "+OK") != null);

    // Verify value was updated (case-insensitive)
    try std.testing.expectEqual(@as(u16, 6443), storage.tls_config.port);
}
