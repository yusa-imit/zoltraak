# Iteration 250 Plan — Phase 10 TLS/SSL Foundation

**Goal**: Implement minimum viable TLS/SSL support for Redis-compatible encrypted connections

**Scope**: TLS 1.2+ server support with basic certificate authentication

---

## 1. Redis TLS Specification Analysis

### 1.1 TLS Command-Line Options

Redis server supports the following TLS-related command-line options (compile-time enabled feature since Redis 6.0):

| Option | Description | Example |
|--------|-------------|---------|
| `--tls-port` | Port for TLS connections | `--tls-port 6380` |
| `--tls-cert-file` | Server certificate file | `--tls-cert-file ./redis.crt` |
| `--tls-key-file` | Server private key file | `--tls-key-file ./redis.key` |
| `--tls-ca-cert-file` | CA certificate file (for client auth) | `--tls-ca-cert-file ./ca.crt` |
| `--tls-ca-cert-dir` | CA certificate directory | `--tls-ca-cert-dir /etc/ssl/certs` |
| `--tls-auth-clients` | Require client certificates (yes/no/optional) | `--tls-auth-clients yes` |

**Source**: [Redis TLS Documentation](https://redis.io/docs/latest/operate/oss_and_stack/management/security/encryption/)

### 1.2 TLS CONFIG Parameters (redis.conf)

Redis supports 20 TLS configuration parameters for runtime configuration:

#### Core TLS Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tls-port` | integer | 0 (disabled) | Port for TLS connections |
| `tls-cert-file` | string | - | Path to server certificate (PEM format) |
| `tls-key-file` | string | - | Path to server private key (PEM format) |
| `tls-key-file-pass` | string | - | Password for encrypted private key |
| `tls-ca-cert-file` | string | - | Path to CA certificate bundle |
| `tls-ca-cert-dir` | string | - | Directory containing CA certificates |
| `tls-auth-clients` | enum | yes | Client certificate requirement (yes/no/optional) |

#### Protocol & Cipher Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tls-protocols` | string | TLSv1.2 TLSv1.3 | Allowed TLS protocol versions |
| `tls-ciphers` | string | (OpenSSL default) | TLS 1.2 cipher suites |
| `tls-ciphersuites` | string | (OpenSSL default) | TLS 1.3 cipher suites |
| `tls-prefer-server-ciphers` | bool | yes | Prefer server cipher order |

**Recommended TLS 1.2 ciphers**: `ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256`

**Recommended TLS 1.3 ciphersuites**: `TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256`

**Source**: [Redis TLS Protocol Configuration](https://redis.io/docs/latest/operate/rs/security/encryption/tls/tls-protocols/)

#### Session & Performance

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tls-session-caching` | bool | yes | Enable session resumption for faster reconnects |
| `tls-session-cache-size` | integer | 20480 | Number of cached TLS sessions |
| `tls-session-cache-timeout` | integer | 300 | Session cache TTL (seconds) |

#### Cluster & Replication TLS

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tls-cluster` | bool | no | Enable TLS for cluster bus (port+10000) |
| `tls-replication` | bool | no | Enable TLS for replica-to-master connections |

#### Client Certificate Override (for outbound connections)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tls-client-cert-file` | string | - | Client certificate for outbound TLS (replication/cluster) |
| `tls-client-key-file` | string | - | Client private key for outbound TLS |
| `tls-client-key-file-pass` | string | - | Password for client key |

**Source**: [Redis TLS Configuration Settings](https://redis.io/docs/latest/operate/oss_and_stack/management/security/encryption/)

### 1.3 TLS Connection Handshake Flow

Redis TLS follows standard TLS 1.2/1.3 handshake protocol:

```
Client                                Server
  |                                      |
  |--- ClientHello ---------------------->| (supported versions, ciphersuites, SNI)
  |                                      |
  |<-- ServerHello, Certificate ---------| (selected version, cipher, server cert)
  |<-- CertificateRequest (optional) ----| (if tls-auth-clients yes/optional)
  |<-- ServerHelloDone ------------------|
  |                                      |
  |--- Certificate (if requested) ------>| Client cert for mutual TLS
  |--- ClientKeyExchange --------------->| RSA/ECDHE key exchange
  |--- CertificateVerify (if cert sent)->| Proves client owns private key
  |--- ChangeCipherSpec ---------------->|
  |--- Finished ------------------------>|
  |                                      |
  |<-- ChangeCipherSpec -----------------|
  |<-- Finished -------------------------|
  |                                      |
  |=== Encrypted RESP Protocol ========>| Normal Redis commands over TLS
```

**Key validation steps**:
1. **Server certificate validation** (client-side): Verify server cert against CA bundle (`tls-ca-cert-file`)
2. **Client certificate validation** (server-side, if `tls-auth-clients yes`): Verify client cert against CA bundle
3. **Hostname verification**: Optional SNI (Server Name Indication) check
4. **Cipher negotiation**: Must match configured `tls-ciphers` or `tls-ciphersuites`

**Source**: [Redis TLS Handshake Troubleshooting](https://oneuptime.com/blog/post/2026-03-31-redis-troubleshoot-redis-tls-handshake-failures/view)

### 1.4 Certificate Validation Requirements

#### Minimum Requirements for TLS 1.2+ Compliance

1. **Certificate Format**: PEM (Privacy Enhanced Mail) format, X.509 v3 certificates
2. **Key Types**: RSA (2048+ bits), ECDSA (P-256/P-384), Ed25519
3. **Signature Algorithms**: SHA256/SHA384/SHA512 (no MD5/SHA1)
4. **Certificate Chain**: Server must present full chain (leaf → intermediate → root CA)
5. **Validity Period**: Must check `notBefore` and `notAfter` dates
6. **Revocation Check**: Optional CRL/OCSP (not implemented in Redis OSS)

#### Mutual TLS (mTLS) Requirements

When `tls-auth-clients yes`:
- Client MUST present a valid certificate during handshake
- Client certificate MUST be signed by a CA in `tls-ca-cert-file` or `tls-ca-cert-dir`
- Client MUST prove ownership via CertificateVerify message

When `tls-auth-clients optional`:
- Client MAY present a certificate
- If presented, it MUST be valid
- If not presented, connection is still allowed

**Source**: [Redis Certificate-Based Authentication](https://redis.io/docs/latest/operate/rs/security/certificates/certificate-based-authentication/)

---

## 2. Zig TLS Implementation Analysis

### 2.1 Standard Library Support

Zig's standard library provides `std.crypto.tls` with the following capabilities:

| Feature | Status | Notes |
|---------|--------|-------|
| TLS 1.3 Client | ✅ Available | `std.crypto.tls.Client` fully functional |
| TLS 1.2 Client | ⚠️ Partial | Supported but with known edge case issues |
| TLS 1.3 Server | ❌ Missing | [Issue #14171](https://github.com/ziglang/zig/issues/14171) — not yet implemented |
| TLS 1.2 Server | ❌ Missing | No server implementation in stdlib |
| Zero-heap allocation | ✅ Yes | Client implementation has zero heap allocations |
| Cipher suites | ⚠️ Limited | Small subset of modern ciphersuites |

**Supported Cipher Suites in std.crypto.tls** (as of Zig 0.15.2):
- `TLS_AES_128_GCM_SHA256` (TLS 1.3)
- `TLS_AES_256_GCM_SHA384` (TLS 1.3)
- `TLS_CHACHA20_POLY1305_SHA256` (TLS 1.3)
- `TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256` (TLS 1.2, partial support)

**Source**: [Zig TLS Implementation Status](https://github.com/ziglang/zig/blob/master/lib/std/crypto/tls.zig)

### 2.2 Alternative Approaches

Since `std.crypto.tls.Server` does not exist, we have three options:

#### Option A: Third-Party Zig TLS Library

**Project**: [ianic/tls.zig](https://github.com/ianic/tls.zig)
- TLS 1.3/1.2 client ✅
- TLS 1.3 server ✅
- Pure Zig implementation
- Tested against 6270+ domains (4844 TLS 1.3, 1426 TLS 1.2)
- **Pros**: Pure Zig, no C dependencies, server support
- **Cons**: Not in stdlib, less battle-tested than OpenSSL

#### Option B: OpenSSL/BoringSSL via C Interop

Use Zig's `@cImport` to link against system OpenSSL/LibreSSL:
- Full TLS 1.2/1.3 support ✅
- Battle-tested implementation ✅
- All Redis cipher suites supported ✅
- **Pros**: Production-grade, Redis uses OpenSSL
- **Cons**: External dependency, C API complexity, harder to cross-compile

**Existing Zig wrapper**: [glingy/openssl-zig](https://github.com/glingy/openssl-zig)

#### Option C: Hybrid Approach (Recommended for Iteration 250)

**Stub implementation** for Phase 10 foundation:
1. Parse all TLS CONFIG parameters ✅
2. Accept `--tls-port` flag and bind TLS listener ✅
3. Return `-ERR TLS not yet implemented` on TLS connections ✅
4. Provide extension points for future TLS integration ✅
5. Document TLS implementation strategy in code comments ✅

Then choose Option A or B for full implementation in future iterations.

**Source**: [Zig TLS Client Examples](https://sheran.sg/blog/figured-out-zig-tls-client/)

---

## 3. Minimum Viable Implementation (Iteration 250)

### 3.1 Architecture Design

```
┌─────────────────────────────────────────────────┐
│              Server Struct                      │
├─────────────────────────────────────────────────┤
│  - tcp_listener (port)                          │
│  - tls_listener (tls-port) ← NEW               │
│  - tls_config: TlsConfig   ← NEW               │
└─────────────────────────────────────────────────┘
                    │
        ┌───────────┴──────────┐
        ▼                      ▼
┌──────────────┐      ┌──────────────────┐
│ TCP Accept   │      │ TLS Accept       │ ← NEW
│ (port 6379)  │      │ (tls-port 6380)  │
└──────────────┘      └──────────────────┘
        │                      │
        └───────────┬──────────┘
                    ▼
        ┌───────────────────────┐
        │ handleConnection()    │
        │ (unified handler)     │
        └───────────────────────┘
```

### 3.2 File Structure

```
src/
├── server.zig              # Add tls_listener field, tls_config integration
├── storage/
│   └── tls_config.zig      # NEW: TLS configuration state
└── commands/
    └── config.zig          # Add CONFIG GET/SET for TLS parameters
```

### 3.3 Implementation Tasks (Iteration 250 Scope)

#### Task 1: TLS Configuration Storage (`src/storage/tls_config.zig`)

```zig
/// TLS/SSL configuration for Redis server
/// Corresponds to redis.conf tls-* parameters
pub const TlsConfig = struct {
    allocator: std.mem.Allocator,

    // Core TLS settings
    port: u16 = 0,                          // 0 = TLS disabled
    cert_file: ?[]const u8 = null,
    key_file: ?[]const u8 = null,
    key_file_pass: ?[]const u8 = null,
    ca_cert_file: ?[]const u8 = null,
    ca_cert_dir: ?[]const u8 = null,
    auth_clients: AuthClientsMode = .yes,

    // Protocol configuration
    protocols: []const u8 = "TLSv1.2 TLSv1.3",
    ciphers: ?[]const u8 = null,            // TLS 1.2 cipher list
    ciphersuites: ?[]const u8 = null,       // TLS 1.3 cipher list
    prefer_server_ciphers: bool = true,

    // Session management
    session_caching: bool = true,
    session_cache_size: u32 = 20480,
    session_cache_timeout: u32 = 300,

    // Cluster & replication
    cluster: bool = false,
    replication: bool = false,

    // Client certificate override (for outbound)
    client_cert_file: ?[]const u8 = null,
    client_key_file: ?[]const u8 = null,
    client_key_file_pass: ?[]const u8 = null,

    pub const AuthClientsMode = enum {
        yes,        // Require client cert (mutual TLS)
        no,         // No client cert required
        optional,   // Client cert optional but validated if present
    };

    pub fn init(allocator: std.mem.Allocator) TlsConfig {
        return TlsConfig{ .allocator = allocator };
    }

    pub fn deinit(self: *TlsConfig) void {
        // Free all owned strings (cert paths, cipher lists, etc.)
        // TODO: Implement in full iteration
    }

    pub fn isEnabled(self: TlsConfig) bool {
        return self.port > 0;
    }

    pub fn validateConfig(self: TlsConfig) !void {
        if (!self.isEnabled()) return;

        // Validate required files are present
        if (self.cert_file == null) return error.TlsCertFileRequired;
        if (self.key_file == null) return error.TlsKeyFileRequired;

        // Validate files exist and are readable
        // TODO: Implement in full iteration
    }
};
```

#### Task 2: Server Integration (`src/server.zig`)

Add TLS listener alongside existing TCP listener:

```zig
pub const Server = struct {
    // ... existing fields ...
    tls_config: storage_mod.TlsConfig,
    tls_listener: ?std.net.Server = null,

    pub fn init(allocator: std.mem.Allocator, config: Config) !Server {
        // ... existing initialization ...

        // Initialize TLS config from CONFIG defaults
        var tls_config = storage_mod.TlsConfig.init(allocator);

        return Server{
            // ... existing fields ...
            .tls_config = tls_config,
            .tls_listener = null,
        };
    }

    pub fn start(self: *Server) !void {
        // ... existing TCP listener setup ...

        // Start TLS listener if enabled
        if (self.tls_config.isEnabled()) {
            try self.tls_config.validateConfig();

            const tls_address = try std.net.Address.parseIp(
                self.config.host,
                self.tls_config.port
            );

            self.tls_listener = try tls_address.listen(.{
                .reuse_address = true,
            });

            std.debug.print("TLS listener bound to {s}:{d}\n", .{
                self.config.host,
                self.tls_config.port
            });

            // TODO: Spawn separate thread for TLS accept loop
            // For now, TLS connections will be rejected with error
        }

        // ... existing accept loop ...
    }

    fn handleTlsConnection(self: *Server, connection: std.net.Server.Connection) !void {
        defer connection.stream.close();

        // Stub implementation for Iteration 250
        const error_msg = "-ERR TLS connections not yet supported. " ++
                          "Use non-TLS port for now.\r\n";
        _ = connection.stream.write(error_msg) catch {};

        // TODO: Implement TLS handshake in future iteration:
        // 1. Create TLS context from tls_config
        // 2. Perform TLS handshake (ClientHello, ServerHello, etc.)
        // 3. Verify client certificate if auth_clients=yes
        // 4. Wrap connection.stream in TLS wrapper
        // 5. Call handleConnection() with wrapped stream
    }
};
```

#### Task 3: CONFIG Command Integration (`src/commands/config.zig`)

Add TLS parameters to CONFIG GET/SET:

```zig
// In cmdConfigGet()
if (std.mem.eql(u8, param_lower, "tls-port")) {
    const port_str = try std.fmt.allocPrint(arena.allocator(), "{d}", .{storage.tls_config.port});
    results.append(port_str) catch return error.OutOfMemory;
    return;
}

if (std.mem.eql(u8, param_lower, "tls-auth-clients")) {
    const mode = switch (storage.tls_config.auth_clients) {
        .yes => "yes",
        .no => "no",
        .optional => "optional",
    };
    results.append(mode) catch return error.OutOfMemory;
    return;
}

// Add all 20 TLS parameters similarly...
```

#### Task 4: Command-Line Argument Parsing (`src/main.zig`)

```zig
// Add TLS flags to argument parser
if (std.mem.eql(u8, arg, "--tls-port")) {
    const port_arg = args.next() orelse {
        std.debug.print("Error: --tls-port requires a value\n", .{});
        return;
    };
    server_config.tls_config.port = std.fmt.parseInt(u16, port_arg, 10) catch {
        std.debug.print("Error: Invalid TLS port\n", .{});
        return;
    };
}

if (std.mem.eql(u8, arg, "--tls-cert-file")) {
    server_config.tls_config.cert_file = args.next();
}

// Add --tls-key-file, --tls-ca-cert-file, --tls-auth-clients, etc.
```

### 3.4 Testing Strategy

#### Unit Tests (`tests/test_tls_config.zig`)

```zig
test "TlsConfig init/deinit" {
    const allocator = std.testing.allocator;
    var config = TlsConfig.init(allocator);
    defer config.deinit();

    try std.testing.expect(!config.isEnabled()); // port=0 by default
}

test "TlsConfig enabled detection" {
    const allocator = std.testing.allocator;
    var config = TlsConfig.init(allocator);
    defer config.deinit();

    config.port = 6380;
    try std.testing.expect(config.isEnabled());
}

test "TlsConfig validation requires cert and key" {
    const allocator = std.testing.allocator;
    var config = TlsConfig.init(allocator);
    defer config.deinit();

    config.port = 6380;
    try std.testing.expectError(error.TlsCertFileRequired, config.validateConfig());

    config.cert_file = "/tmp/cert.pem";
    try std.testing.expectError(error.TlsKeyFileRequired, config.validateConfig());
}
```

#### Integration Tests (`tests/test_tls_integration.zig`)

```zig
test "CONFIG GET tls-port" {
    // Start server with TLS port configured
    // Send: CONFIG GET tls-port
    // Expect: ["tls-port", "6380"]
}

test "CONFIG SET tls-auth-clients" {
    // Send: CONFIG SET tls-auth-clients optional
    // Expect: +OK
    // Send: CONFIG GET tls-auth-clients
    // Expect: ["tls-auth-clients", "optional"]
}

test "TLS connection returns error message" {
    // Start server with tls-port 6380
    // Connect to port 6380
    // Expect: -ERR TLS connections not yet supported
}
```

### 3.5 Documentation Updates

#### README.md

```markdown
## TLS/SSL Support

**Status**: Phase 10 foundation implemented (Iteration 250) — Configuration parsing only

Zoltraak supports TLS configuration via redis.conf parameters and command-line flags:

```bash
./zoltraak --tls-port 6380 --tls-cert-file ./cert.pem --tls-key-file ./key.pem
```

**Available TLS CONFIG parameters**:
- `tls-port` — Port for TLS connections (0 = disabled)
- `tls-cert-file` — Server certificate path
- `tls-key-file` — Server private key path
- `tls-auth-clients` — Client certificate requirement (yes/no/optional)
- ... (all 20 parameters)

**Current limitations**:
- TLS handshake not yet implemented (returns error message)
- Full TLS 1.2/1.3 support planned for future iterations
- Consider using stunnel for TLS in production until full implementation
```

#### CLAUDE.md

```markdown
## Phase 10 TLS/SSL Status

- **Iteration 250** (foundation): TLS config parsing, command-line flags, CONFIG GET/SET integration ✅
- **Future iterations**: TLS handshake, certificate validation, session caching, cluster TLS

Implementation approach TBD:
- Option A: ianic/tls.zig (pure Zig, TLS 1.3 server support)
- Option B: OpenSSL via C interop (battle-tested, full cipher suite support)
```

---

## 4. Future Iterations Roadmap

### Iteration 251: TLS Handshake Implementation

**Scope**: Implement full TLS 1.2/1.3 server handshake

**Tasks**:
1. Choose TLS backend (ianic/tls.zig vs OpenSSL)
2. Implement `TlsContext` wrapper
3. Server certificate loading and validation
4. TLS handshake state machine
5. Encrypted stream wrapper for RESP protocol
6. Integration tests with `redis-cli --tls`

**Estimated complexity**: Large (3-4 days)

### Iteration 252: Client Certificate Validation (mTLS)

**Scope**: Mutual TLS with client certificate authentication

**Tasks**:
1. CA certificate loading and trust store
2. Client certificate request during handshake
3. Certificate chain validation
4. CertificateVerify message handling
5. `tls-auth-clients` mode support (yes/no/optional)
6. Integration with ACL system (cert-based user mapping)

**Estimated complexity**: Medium (2-3 days)

### Iteration 253: TLS for Cluster & Replication

**Scope**: Encrypted inter-node communication

**Tasks**:
1. `tls-cluster yes` — TLS on cluster bus (port+10000)
2. `tls-replication yes` — TLS for replica connections
3. Outbound TLS client support (for CLUSTER MEET, REPLICAOF)
4. Client certificate override (`tls-client-cert-file`)
5. Integration tests with multi-node setup

**Estimated complexity**: Medium (2 days)

### Iteration 254: Session Caching & Performance

**Scope**: TLS session resumption for faster reconnects

**Tasks**:
1. Session cache implementation (LRU with size limit)
2. Session ticket generation and validation
3. `tls-session-caching` / `tls-session-cache-size` integration
4. Performance benchmarks (handshake latency, throughput)
5. Memory usage profiling

**Estimated complexity**: Small (1-2 days)

---

## 5. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| **std.crypto.tls.Server missing** | High — No stdlib server support | Use ianic/tls.zig or OpenSSL binding |
| **Cross-compilation with OpenSSL** | Medium — OpenSSL linking complexity | Provide static builds or document system OpenSSL requirements |
| **Cipher suite compatibility** | Medium — Limited cipher support in pure Zig | Document supported ciphers, recommend OpenSSL for production |
| **Certificate validation bugs** | High — Security vulnerability | Extensive testing with invalid certs, fuzzing, audit by security expert |
| **Performance overhead** | Low — TLS adds latency | Implement session caching, benchmark against Redis |

---

## 6. Success Criteria (Iteration 250)

- [ ] `TlsConfig` struct with all 20 TLS parameters
- [ ] CONFIG GET/SET support for all `tls-*` parameters
- [ ] Command-line flags: `--tls-port`, `--tls-cert-file`, `--tls-key-file`, `--tls-auth-clients`
- [ ] TLS listener binds to `tls-port` (if enabled)
- [ ] TLS connections return error message (stub)
- [ ] All unit tests pass (10+ tests)
- [ ] All integration tests pass (5+ tests)
- [ ] Zero memory leaks
- [ ] Documentation updated (README.md, CLAUDE.md, PRD.md)
- [ ] Quality reviews pass (zig-quality-reviewer, code-reviewer)

---

## 7. References

### Official Redis Documentation
- [TLS/SSL Overview](https://redis.io/docs/latest/operate/oss_and_stack/management/security/encryption/)
- [TLS Configuration Parameters](https://redis.io/docs/latest/operate/rs/security/encryption/tls/enable-tls/)
- [TLS Protocol & Cipher Configuration](https://redis.io/docs/latest/operate/rs/security/encryption/tls/tls-protocols/)
- [TLS Cipher Suites](https://redis.io/docs/latest/operate/rs/security/encryption/tls/ciphers/)
- [Certificate-Based Authentication](https://redis.io/docs/latest/operate/rs/security/certificates/certificate-based-authentication/)

### Zig TLS Resources
- [Zig std.crypto.tls Source Code](https://github.com/ziglang/zig/blob/master/lib/std/crypto/tls.zig)
- [Zig TLS Server Issue #14171](https://github.com/ziglang/zig/issues/14171)
- [ianic/tls.zig — TLS 1.3/1.2 Server](https://github.com/ianic/tls.zig)
- [openssl-zig — OpenSSL Bindings](https://github.com/glingy/openssl-zig)
- [Zig TLS Client Tutorial](https://sheran.sg/blog/figured-out-zig-tls-client/)

### Technical Specifications
- [RFC 5246 — TLS 1.2](https://datatracker.ietf.org/doc/html/rfc5246)
- [RFC 8446 — TLS 1.3](https://datatracker.ietf.org/doc/html/rfc8446)
- [RFC 7396 — JSON Merge Patch](https://datatracker.ietf.org/doc/html/rfc7396) (for comparison)

---

## 8. Estimated Effort

**Iteration 250 (Foundation)**: 1 day
- Config parsing: 3 hours
- Server integration: 2 hours
- Testing: 2 hours
- Documentation: 1 hour

**Phase 10 Total (Iterations 250-254)**: 8-12 days
- Foundation: 1 day
- Full handshake: 3-4 days
- Client cert validation: 2-3 days
- Cluster/replication TLS: 2 days
- Session caching: 1-2 days

**Risk buffer**: +2 days for TLS backend selection and integration issues

---

**End of Iteration 250 Plan**
