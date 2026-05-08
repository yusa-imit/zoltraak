# Phase 10 — TLS/SSL Implementation Specification

**Status:** Planning Phase
**Target:** Redis 8.x TLS compatibility
**Estimated Iterations:** 3-4
**Documentation References:**
- [Redis TLS Documentation](https://redis.io/docs/latest/operate/oss_and_stack/management/security/encryption/)
- [Configure TLS in Redis Cluster](https://dev.to/hedgehog/configure-tls-in-redis-cluster-50ec)
- [Redis TLS Troubleshooting](https://oneuptime.com/blog/post/2026-03-31-redis-troubleshoot-redis-tls-handshake-failures/view)

---

## 1. Overview

Implement TLS 1.2+ support for secure Redis connections, including:
- Server-side TLS socket handling with certificate authentication
- 20 TLS configuration parameters (already defined in `src/storage/config.zig`)
- TLS for cluster bus (port + 10000)
- TLS for replication connections (primary-replica)
- Mutual TLS (client certificate authentication)
- Session caching for performance

**Current State:**
- TLS config parameters are defined in `src/storage/config.zig` (lines 134-155)
- No actual TLS socket implementation exists
- Server only listens on plain TCP (see `src/server.zig`)

---

## 2. TLS Configuration Parameters

All 20 parameters are already defined in `src/storage/config.zig`. Here's the complete specification:

### 2.1 Core Certificate Configuration

| Parameter | Type | Default | Read-Only | Description |
|-----------|------|---------|-----------|-------------|
| `tls-port` | int | 0 | No | Port for TLS connections (0 = disabled). Works in addition to `port` unless `port 0` is set |
| `tls-cert-file` | string | "" | No | Path to X.509 certificate file for server authentication |
| `tls-key-file` | string | "" | No | Path to private key file corresponding to certificate |
| `tls-key-file-pass` | string | "" | No | Passphrase for encrypted private key files (optional) |
| `tls-ca-cert-file` | string | "" | No | Path to CA certificate bundle for validating client certificates |
| `tls-ca-cert-dir` | string | "" | No | Directory containing CA certificates (alternative to file) |
| `tls-dh-params-file` | string | "" | No | Path to DH parameters file for DH-based ciphers (optional, OpenSSL < 3.0) |

### 2.2 Client Authentication

| Parameter | Type | Default | Read-Only | Description |
|-----------|------|---------|-----------|-------------|
| `tls-auth-clients` | string | "yes" | No | Require client certificate authentication. Values: "yes" (required), "no" (disabled), "optional" (request but don't require) |
| `tls-auth-clients-user` | string | "off" | No | Map TLS clients to Redis users based on certificate CN. Values: "off", "CN" |

### 2.3 Protocol & Cipher Configuration

| Parameter | Type | Default | Read-Only | Description |
|-----------|------|---------|-----------|-------------|
| `tls-protocols` | string | "TLSv1.2 TLSv1.3" | No | Space-separated list of allowed TLS versions. Values: "TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3" |
| `tls-ciphers` | string | "" | No | OpenSSL cipher list for TLS 1.2 and earlier (empty = use OpenSSL defaults) |
| `tls-ciphersuites` | string | "" | No | OpenSSL cipher suite list for TLS 1.3 (empty = use OpenSSL defaults). Example: "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256" |
| `tls-prefer-server-ciphers` | bool | true | No | Use server's cipher preference order instead of client's |

### 2.4 Session Caching

| Parameter | Type | Default | Read-Only | Description |
|-----------|------|---------|-----------|-------------|
| `tls-session-caching` | bool | true | No | Enable TLS session caching for faster reconnections |
| `tls-session-cache-size` | int | 20480 | No | Maximum number of cached TLS sessions |
| `tls-session-cache-timeout` | int | 300 | No | Session cache timeout in seconds |

### 2.5 Cluster & Replication TLS

| Parameter | Type | Default | Read-Only | Description |
|-----------|------|---------|-----------|-------------|
| `tls-cluster` | bool | false | No | Enable TLS for cluster bus and inter-node communication (port + 10000) |
| `tls-replication` | bool | false | No | Enable TLS for outgoing replication connections to primary |

### 2.6 Outbound Client Certificates

| Parameter | Type | Default | Read-Only | Description |
|-----------|------|---------|-----------|-------------|
| `tls-client-cert-file` | string | "" | No | Client certificate for outbound connections (replica, cluster) |
| `tls-client-key-file` | string | "" | No | Client private key for outbound connections |
| `tls-client-key-file-pass` | string | "" | No | Passphrase for client key file (optional) |

### 2.7 Certificate Allowlist (Redis 7.2+)

| Parameter | Type | Default | Read-Only | Description |
|-----------|------|---------|-----------|-------------|
| `tls-allowlisted-certs` | string | "" | No | Space-separated paths to allowlisted certificate files (skip CA validation for these) |

---

## 3. TLS Connection Flow

### 3.1 Server Initialization

```
1. Check if tls-port > 0
2. If yes:
   a. Load server certificate (tls-cert-file)
   b. Load server private key (tls-key-file)
   c. Decrypt key if tls-key-file-pass is set
   d. Load CA certificate bundle (tls-ca-cert-file or tls-ca-cert-dir)
   e. Load DH params if tls-dh-params-file is set
   f. Configure allowed TLS versions (tls-protocols)
   g. Configure cipher suites (tls-ciphers, tls-ciphersuites)
   h. Set cipher preference mode (tls-prefer-server-ciphers)
   i. Configure session caching (tls-session-caching, size, timeout)
   j. Set client authentication mode (tls-auth-clients: yes/no/optional)
3. Create TLS listener socket on tls-port
4. Optionally disable plain TCP listener (if port = 0)
```

### 3.2 TLS Handshake Protocol

**For each incoming connection on tls-port:**

```
1. Accept TCP connection
2. Perform TLS handshake:
   a. Server sends: ServerHello, Certificate, ServerKeyExchange (if needed), ServerHelloDone
   b. If tls-auth-clients = "yes" or "optional":
      - Server sends CertificateRequest
      - Client must send Certificate, CertificateVerify
   c. Client sends: ClientKeyExchange, ChangeCipherSpec, Finished
   d. Server validates client certificate against CA bundle
   e. If tls-auth-clients = "yes" and validation fails → reject connection
   f. If tls-auth-clients = "optional" and validation fails → allow but mark as unauthenticated
   g. Server sends: ChangeCipherSpec, Finished
3. TLS session established → proceed with RESP protocol
4. If tls-session-caching = true:
   - Cache session ID for resume
   - Respect tls-session-cache-size and timeout
```

### 3.3 RESP Protocol over TLS

- **No changes to RESP protocol itself**
- All RESP2/RESP3 messages are transmitted over the encrypted TLS channel
- Existing command handlers work unchanged
- Connection state (HELLO, AUTH, SELECT) operates normally

### 3.4 Cluster Bus TLS (if tls-cluster = true)

```
1. Cluster bus port = base_port + 10000
2. If tls-cluster = true:
   a. Create TLS listener on tls-port + 10000
   b. Use tls-client-cert-file / tls-client-key-file for outbound cluster connections
   c. All cluster gossip messages (PING, PONG, MEET, FAIL) encrypted
   d. Slot migration data encrypted
3. All cluster nodes MUST have consistent CA chain for handshake success
```

### 3.5 Replication TLS (if tls-replication = true)

```
1. Replica establishes outbound connection to primary
2. If tls-replication = true:
   a. Replica initiates TLS handshake with primary
   b. Replica uses tls-client-cert-file / tls-client-key-file
   c. Primary validates replica certificate against CA bundle
   d. PSYNC, RDB transfer, and command stream all encrypted
3. Primary must have tls-port enabled to accept TLS replica connections
```

---

## 4. TLS Error Conditions

### 4.1 Configuration Errors (at startup)

| Condition | Error Response | Action |
|-----------|----------------|--------|
| `tls-port > 0` but `tls-cert-file` empty | Fatal error | Log error, exit |
| `tls-port > 0` but `tls-key-file` empty | Fatal error | Log error, exit |
| Certificate file not found | Fatal error | Log "ERR certificate file not found: <path>", exit |
| Private key file not found | Fatal error | Log "ERR private key file not found: <path>", exit |
| Private key passphrase incorrect | Fatal error | Log "ERR failed to decrypt private key", exit |
| Certificate and key mismatch | Fatal error | Log "ERR certificate and private key do not match", exit |
| CA cert file not found (if tls-auth-clients = yes/optional) | Fatal error | Log "ERR CA certificate file not found: <path>", exit |
| Invalid TLS protocol version in `tls-protocols` | Fatal error | Log "ERR invalid TLS protocol version: <version>", exit |
| Failed to load DH params | Warning | Log warning, continue (DH ciphers disabled) |

### 4.2 Handshake Errors (per connection)

| Condition | Error Response | Action |
|-----------|----------------|--------|
| Client uses unsupported TLS version | TLS alert: protocol_version | Close connection, log warning |
| Client certificate validation fails (tls-auth-clients = yes) | TLS alert: certificate_verify_failed | Close connection, log warning |
| Client doesn't provide certificate (tls-auth-clients = yes) | TLS alert: certificate_required | Close connection |
| Client certificate expired | TLS alert: certificate_expired | Close connection, log warning |
| Client certificate not yet valid | TLS alert: certificate_unknown | Close connection, log warning |
| Wrong certificate type (client cert used as server cert) | TLS alert: unsupported_certificate | Close connection, log warning |
| No common cipher suite | TLS alert: handshake_failure | Close connection, log warning |
| Cipher suite negotiation fails | TLS alert: insufficient_security | Close connection |

### 4.3 Runtime Errors

| Condition | Error Response | Action |
|-----------|----------------|--------|
| TLS read error during RESP parsing | Connection error | Close connection, log error |
| TLS write error during response | Connection error | Close connection, log error |
| Session cache full | No error | Evict oldest session (LRU) |
| Certificate expires while server running | No immediate error | Log warning, continue (renew and reload required) |

### 4.4 Cluster-Specific TLS Errors

| Condition | Error Response | Action |
|-----------|----------------|--------|
| Cluster node CA chain mismatch | Handshake failure | Log "ERR cluster node <node_id> CA mismatch", mark node as failed |
| Cluster bus TLS port unreachable | Connection timeout | Retry with backoff, mark node as PFAIL |
| Outbound cluster connection cert missing (tls-client-cert-file empty) | Fatal error | Log error, refuse to join cluster |

---

## 5. Command-Level Changes

**No new commands required.** TLS is transparent to Redis commands.

### 5.1 Affected Commands (behavior remains identical)

All existing commands work unchanged over TLS. However, these commands return TLS-aware information:

#### CLIENT LIST
```
Output includes TLS status per client:
id=1 addr=127.0.0.1:52301 ... ssl=yes ssl_version=TLSv1.3 ssl_cipher=TLS_AES_256_GCM_SHA384 ssl_cert_subject=/CN=client.example.com
```

**New fields:**
- `ssl=yes/no` — whether connection is TLS-encrypted
- `ssl_version=<version>` — negotiated TLS version (TLSv1.2, TLSv1.3, etc.)
- `ssl_cipher=<cipher>` — negotiated cipher suite
- `ssl_cert_subject=<DN>` — client certificate subject (if mutual TLS)
- `ssl_cert_issuer=<DN>` — client certificate issuer (if mutual TLS)

#### INFO SERVER
```
# Server
...
tls_enabled:1
tls_port:6379
tls_cert_file:/path/to/redis.crt
tls_key_file:/path/to/redis.key
tls_ca_cert_file:/path/to/ca.crt
tls_protocols:TLSv1.2 TLSv1.3
tls_ciphers:
tls_ciphersuites:TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256
tls_prefer_server_ciphers:yes
tls_auth_clients:yes
tls_session_caching:yes
tls_session_cache_size:20480
tls_session_cache_timeout:300
tls_cluster:no
tls_replication:no
```

#### INFO STATS
```
# Stats
...
total_tls_connections_received:1523
current_tls_connections:42
tls_handshake_errors:17
tls_session_cache_hits:891
tls_session_cache_misses:632
```

#### CONFIG GET tls-*
All 20 TLS parameters are accessible via `CONFIG GET tls-*` (already implemented in `src/storage/config.zig`).

#### CONFIG SET tls-*
Dynamic changes to these parameters require connection reload:
- `tls-auth-clients` — can be changed at runtime
- `tls-session-caching`, `tls-session-cache-size`, `tls-session-cache-timeout` — can be changed at runtime
- `tls-prefer-server-ciphers` — affects new connections only
- All certificate/key files — **READ-ONLY at runtime** (require restart)
- `tls-port` — **READ-ONLY at runtime** (require restart)

---

## 6. Implementation Architecture

### 6.1 Technology Choice: OpenSSL vs std.crypto.tls

**Recommended: OpenSSL via C bindings**

| Factor | OpenSSL | std.crypto.tls |
|--------|---------|----------------|
| TLS version support | 1.0, 1.1, 1.2, 1.3 | 1.3 only |
| Redis compatibility | 100% (Redis uses OpenSSL) | Partial (no TLS 1.2) |
| Cipher suite control | Full (via tls-ciphers, tls-ciphersuites) | Limited |
| Session caching | Full support | Not documented |
| Certificate validation | Full X.509 chain validation | Basic |
| Mutual TLS | Full support | Basic |
| DH params | Supported | N/A |
| Community packages | [openssl-zig](https://github.com/kassane/openssl-zig) | Stdlib |
| Build complexity | Requires OpenSSL lib | No external deps |

**Decision:** Use OpenSSL for full Redis compatibility. Already have LuaJIT as system dependency, adding OpenSSL is acceptable.

### 6.2 File Structure

```
src/
├── tls/
│   ├── context.zig          # TLS context initialization (SSL_CTX)
│   ├── connection.zig       # TLS connection wrapper (SSL)
│   ├── config.zig           # TLS config loader (certificate, key, CA)
│   ├── session_cache.zig    # Session caching implementation
│   └── errors.zig           # TLS error handling
├── server.zig               # Modified: support dual plain/TLS listeners
├── storage/
│   └── config.zig           # Already has TLS config parameters
└── commands/
    ├── client.zig           # Modified: add TLS fields to CLIENT LIST
    └── info.zig             # Modified: add TLS stats to INFO
```

### 6.3 Key Components

#### 6.3.1 TLS Context (`src/tls/context.zig`)

```zig
/// TLS context for server and client connections
pub const TlsContext = struct {
    ssl_ctx: *OpenSSL.SSL_CTX,
    config: *Config,
    session_cache: ?*SessionCache,

    /// Initialize TLS context from config
    pub fn init(allocator: Allocator, config: *Config) !*TlsContext;

    /// Load server certificate and key
    pub fn loadServerCert(self: *TlsContext, cert_file: []const u8, key_file: []const u8, key_pass: ?[]const u8) !void;

    /// Load CA certificate bundle
    pub fn loadCACert(self: *TlsContext, ca_file: ?[]const u8, ca_dir: ?[]const u8) !void;

    /// Configure allowed TLS versions
    pub fn setProtocols(self: *TlsContext, protocols: []const u8) !void;

    /// Configure cipher suites
    pub fn setCiphers(self: *TlsContext, ciphers: []const u8, ciphersuites: []const u8) !void;

    /// Set client authentication mode
    pub fn setAuthMode(self: *TlsContext, mode: AuthMode) void;

    pub fn deinit(self: *TlsContext) void;
};
```

#### 6.3.2 TLS Connection (`src/tls/connection.zig`)

```zig
/// TLS connection wrapper for SSL socket
pub const TlsConnection = struct {
    ssl: *OpenSSL.SSL,
    socket: std.posix.socket_t,

    /// Create TLS connection from accepted socket
    pub fn accept(ctx: *TlsContext, socket: std.posix.socket_t) !*TlsConnection;

    /// Connect to remote host with TLS (for cluster/replication)
    pub fn connect(ctx: *TlsContext, host: []const u8, port: u16) !*TlsConnection;

    /// Perform TLS handshake
    pub fn handshake(self: *TlsConnection) !void;

    /// Read RESP data from TLS socket
    pub fn read(self: *TlsConnection, buffer: []u8) !usize;

    /// Write RESP data to TLS socket
    pub fn write(self: *TlsConnection, data: []const u8) !void;

    /// Get client certificate info (if mutual TLS)
    pub fn getClientCert(self: *TlsConnection) ?CertificateInfo;

    pub fn deinit(self: *TlsConnection) void;
};
```

#### 6.3.3 Session Cache (`src/tls/session_cache.zig`)

```zig
/// TLS session cache for faster reconnections
pub const SessionCache = struct {
    allocator: Allocator,
    sessions: std.AutoHashMap([32]u8, CachedSession), // session_id -> session
    lru_list: std.TailQueue(CachedSession),
    max_size: usize,
    timeout_sec: u64,
    mutex: std.Thread.Mutex,

    pub fn init(allocator: Allocator, max_size: usize, timeout_sec: u64) !*SessionCache;

    /// Store session after successful handshake
    pub fn put(self: *SessionCache, session_id: [32]u8, session: *OpenSSL.SSL_SESSION) !void;

    /// Retrieve session for resume
    pub fn get(self: *SessionCache, session_id: [32]u8) ?*OpenSSL.SSL_SESSION;

    /// Evict expired sessions
    pub fn evictExpired(self: *SessionCache) void;

    pub fn deinit(self: *SessionCache) void;
};
```

### 6.4 Server Modifications

**In `src/server.zig`:**

```zig
pub const Server = struct {
    // Existing fields...
    plain_listener: ?std.net.Server,  // Plain TCP listener (if port > 0)
    tls_listener: ?std.net.Server,    // TLS listener (if tls-port > 0)
    tls_context: ?*TlsContext,        // TLS context (if TLS enabled)

    pub fn init(...) !*Server {
        // ...existing initialization...

        // Initialize TLS if tls-port > 0
        if (try config.get("tls-port")) |port_str| {
            const tls_port = try std.fmt.parseInt(u16, port_str, 10);
            if (tls_port > 0) {
                server.tls_context = try TlsContext.init(allocator, config);
                // Load certificates, configure protocols, etc.
                server.tls_listener = try createTlsListener(tls_port);
            }
        }
    }

    pub fn run(self: *Server) !void {
        // Accept connections from both plain and TLS listeners
        // Spawn handler for each accepted connection
    }
};
```

---

## 7. Testing Requirements

### 7.1 Unit Tests

1. **TLS context initialization**
   - Valid certificate/key pair loading
   - Invalid certificate error handling
   - Key passphrase decryption
   - CA bundle loading

2. **TLS handshake**
   - Successful TLS 1.2 handshake
   - Successful TLS 1.3 handshake
   - Protocol version negotiation
   - Cipher suite negotiation
   - Client certificate validation (mutual TLS)
   - Certificate chain validation

3. **Session caching**
   - Session storage and retrieval
   - LRU eviction when cache full
   - Session timeout expiration
   - Cache hit/miss counting

4. **Configuration**
   - All 20 TLS parameters via CONFIG GET/SET
   - Parameter validation (e.g., invalid protocol version)
   - Runtime parameter changes (auth-clients, session-caching)

### 7.2 Integration Tests

1. **redis-cli with TLS**
```bash
redis-cli --tls \
  --cert ./tests/tls/client.crt \
  --key ./tests/tls/client.key \
  --cacert ./tests/tls/ca.crt \
  -p 6379 PING
```

2. **Dual listener (plain + TLS)**
```bash
# Plain connection
redis-cli -p 6379 PING
# TLS connection
redis-cli --tls --cacert ./tests/tls/ca.crt -p 6380 PING
```

3. **Mutual TLS (client cert required)**
```bash
# With valid client cert → success
redis-cli --tls --cert client.crt --key client.key --cacert ca.crt PING
# Without client cert → handshake failure
redis-cli --tls --cacert ca.crt PING  # Expect: connection closed by server
```

4. **Cluster bus TLS**
- Create 3-node cluster with tls-cluster=yes
- Verify gossip messages encrypted
- Verify slot migration over TLS

5. **Replication TLS**
- Configure replica with tls-replication=yes
- Verify PSYNC handshake over TLS
- Verify RDB transfer encrypted
- Verify command stream encrypted

6. **Session caching**
- Connect, disconnect, reconnect → verify cache hit
- Measure handshake time: first connection vs. resumed connection
- Verify session eviction when cache full

### 7.3 Compatibility Tests

1. **TLS protocol versions**
   - TLSv1.2 only (tls-protocols="TLSv1.2")
   - TLSv1.3 only (tls-protocols="TLSv1.3")
   - Both (tls-protocols="TLSv1.2 TLSv1.3")
   - Client requests TLSv1.1 → reject

2. **Cipher suite restrictions**
   - Weak cipher disabled (e.g., RC4, MD5)
   - Strong cipher only (e.g., AES-256-GCM)
   - Client cipher vs. server cipher preference

3. **Certificate validation**
   - Self-signed certificate
   - CA-signed certificate
   - Expired certificate → reject
   - Not-yet-valid certificate → reject
   - Certificate with wrong hostname → accept (Redis doesn't validate hostname by default)

4. **Error conditions**
   - Missing certificate file → startup failure
   - Certificate/key mismatch → startup failure
   - Client cert validation failure → connection closed
   - Protocol version mismatch → alert + close

### 7.4 Performance Tests

1. **Throughput impact**
```bash
# Baseline (plain TCP)
redis-benchmark -p 6379 -t set,get -n 1000000 -q

# TLS enabled
redis-benchmark --tls --cacert ca.crt -p 6380 -t set,get -n 1000000 -q

# Expected: 10-30% throughput reduction due to encryption overhead
```

2. **Latency impact**
```bash
# Measure p50, p99, p999 latency for plain vs. TLS
redis-benchmark --csv ... (parse latency columns)
```

3. **Session caching benefit**
```bash
# Measure handshake time with/without session resume
# Expected: 50-70% reduction in handshake time on resume
```

---

## 8. Edge Cases & Special Behaviors

### 8.1 Certificate Rotation

**Problem:** Server certificate expires while running.

**Redis behavior:** Server continues running with expired cert. Clients connecting after expiration will fail handshake.

**Zoltraak behavior:** Same as Redis. Log warning when cert expires. Require manual restart with new cert.

**Future enhancement:** Support SIGHUP reload (not in v1.0 scope).

### 8.2 Allowlisted Certificates (tls-allowlisted-certs)

**Redis 7.2+ feature:** Skip CA validation for specific client certificates.

**Use case:** Client has self-signed cert not issued by trusted CA.

**Implementation:**
```
1. Parse tls-allowlisted-certs as space-separated file paths
2. Load each certificate
3. During handshake, if client cert matches allowlist → skip CA validation
4. If not in allowlist → perform standard CA validation
```

### 8.3 Cluster Node Certificate Consistency

**Critical requirement:** All cluster nodes MUST share consistent CA chain.

**Problem:** If node A's CA differs from node B's CA → handshake fails → cluster partition.

**Zoltraak behavior:**
- Log error: "ERR cluster node <node_id> CA mismatch"
- Mark node as FAIL in cluster state
- Do NOT attempt automatic recovery (operator must fix certificates)

### 8.4 Replication with Mixed TLS/Plain

**Scenario:** Primary has TLS enabled, replica connects via plain TCP.

**Redis behavior:** Replica must use tls-replication=yes to connect to TLS-enabled primary.

**Zoltraak behavior:**
- If primary tls-port > 0 and port = 0 (TLS-only) → replica MUST use TLS
- If primary has both ports → replica can use plain or TLS (based on tls-replication setting)
- If replica attempts plain connection to TLS-only primary → connection refused

### 8.5 Client Certificate to ACL User Mapping (tls-auth-clients-user)

**Redis 7.2+ feature:** Map client certificate CN to Redis ACL user.

**Example:**
```
tls-auth-clients-user CN
```

When client presents cert with `CN=alice`, automatically authenticate as user `alice`.

**Implementation:**
1. Extract CN from client certificate subject
2. Look up ACL user with matching name
3. If found → authenticate as that user (skip AUTH command)
4. If not found → connection rejected

**Edge cases:**
- Certificate has no CN → reject connection
- ACL user disabled → reject connection
- Multiple CNs in certificate → use first CN

---

## 9. Implementation Phases

### Iteration 1: TLS Foundation (3-4 days)
**Goal:** Basic TLS server socket with OpenSSL bindings.

**Tasks:**
1. Add OpenSSL dependency to `build.zig` (link libssl, libcrypto)
2. Implement `src/tls/context.zig`:
   - Initialize SSL_CTX
   - Load server cert/key
   - Configure TLS versions (tls-protocols)
3. Implement `src/tls/connection.zig`:
   - SSL_accept for incoming connections
   - SSL_read/SSL_write wrappers
4. Modify `src/server.zig`:
   - Create TLS listener if tls-port > 0
   - Accept TLS connections
   - Pass to existing command handler
5. Test: `redis-cli --tls --cacert ca.crt -p 6379 PING`

**Acceptance criteria:**
- TLS server listens on tls-port
- redis-cli can connect over TLS
- RESP protocol works over TLS channel
- No client cert required yet (tls-auth-clients=no)

### Iteration 2: Mutual TLS & Configuration (2-3 days)
**Goal:** Client certificate authentication and full config parameter support.

**Tasks:**
1. Implement CA certificate loading in `src/tls/context.zig`
2. Implement client cert validation (tls-auth-clients: yes/no/optional)
3. Add TLS fields to CLIENT LIST output
4. Add TLS stats to INFO SERVER/STATS
5. Test mutual TLS with redis-cli --cert / --key
6. Test CONFIG GET/SET for all TLS parameters

**Acceptance criteria:**
- tls-auth-clients=yes enforces client cert
- tls-auth-clients=optional allows connections with/without cert
- CLIENT LIST shows ssl=yes, ssl_version, ssl_cipher, ssl_cert_subject
- INFO shows all TLS configuration and stats

### Iteration 3: Session Caching & Performance (2-3 days)
**Goal:** TLS session caching for performance optimization.

**Tasks:**
1. Implement `src/tls/session_cache.zig`:
   - LRU cache with configurable size/timeout
   - Session storage/retrieval via OpenSSL callbacks
2. Add session cache hit/miss tracking
3. Benchmark: measure handshake time improvement
4. Test: cache eviction, timeout, full cache

**Acceptance criteria:**
- Session resume works (measured via handshake time)
- Cache hit/miss stats in INFO STATS
- Cache size/timeout configurable via CONFIG SET
- Cache eviction when full (LRU)

### Iteration 4: Cluster & Replication TLS (3-4 days)
**Goal:** TLS for cluster bus and replication connections.

**Tasks:**
1. Modify cluster bus listener to use TLS if tls-cluster=yes
2. Implement outbound TLS connections for cluster gossip
3. Load tls-client-cert-file / tls-client-key-file for outbound
4. Modify replication code to use TLS if tls-replication=yes
5. Test: 3-node cluster with tls-cluster=yes
6. Test: primary-replica with tls-replication=yes

**Acceptance criteria:**
- Cluster nodes communicate over TLS
- CLUSTER NODES shows TLS connections
- Replica connects to primary over TLS
- PSYNC, RDB transfer, command stream all encrypted

---

## 10. Dependencies & Prerequisites

### 10.1 Build Dependencies

**OpenSSL 1.1.1+ or 3.0+**
- macOS: `brew install openssl@3`
- Linux: `apt-get install libssl-dev` or `yum install openssl-devel`
- Zig binding: Use [openssl-zig](https://github.com/kassane/openssl-zig) package

**Update `build.zig`:**
```zig
exe.linkSystemLibrary("ssl");
exe.linkSystemLibrary("crypto");
exe.linkLibC();

// macOS Homebrew OpenSSL path
exe.addIncludePath(.{ .cwd_relative = "/opt/homebrew/opt/openssl@3/include" });
exe.addLibraryPath(.{ .cwd_relative = "/opt/homebrew/opt/openssl@3/lib" });
```

**Update `build.zig.zon`:**
```zig
.dependencies = .{
    .sailor = .{ ... },
    .zuda = .{ ... },
    .openssl = .{
        .url = "https://github.com/kassane/openssl-zig/archive/<commit>.tar.gz",
        .hash = "<hash>",
    },
},
```

### 10.2 Test Certificates

**Generate test certificates for integration tests:**

```bash
# Create tests/tls directory
mkdir -p tests/tls

# Generate CA key and cert
openssl genrsa -out tests/tls/ca.key 4096
openssl req -new -x509 -days 3650 -key tests/tls/ca.key -out tests/tls/ca.crt \
  -subj "/CN=Test CA"

# Generate server key and cert
openssl genrsa -out tests/tls/server.key 4096
openssl req -new -key tests/tls/server.key -out tests/tls/server.csr \
  -subj "/CN=localhost"
openssl x509 -req -days 3650 -in tests/tls/server.csr -CA tests/tls/ca.crt \
  -CAkey tests/tls/ca.key -CAcreateserial -out tests/tls/server.crt

# Generate client key and cert
openssl genrsa -out tests/tls/client.key 4096
openssl req -new -key tests/tls/client.key -out tests/tls/client.csr \
  -subj "/CN=test-client"
openssl x509 -req -days 3650 -in tests/tls/client.csr -CA tests/tls/ca.crt \
  -CAkey tests/tls/ca.key -CAcreateserial -out tests/tls/client.crt

# Generate DH params (optional)
openssl dhparam -out tests/tls/redis.dh 2048
```

### 10.3 Configuration Dependencies

**No changes to existing config structure.** All TLS parameters already defined in `src/storage/config.zig` lines 134-155.

---

## 11. Compatibility Matrix

### 11.1 Redis Compatibility

| Feature | Redis Behavior | Zoltraak Target |
|---------|----------------|-----------------|
| TLS 1.2 support | Yes | Yes |
| TLS 1.3 support | Yes | Yes |
| TLS 1.0/1.1 support | Deprecated (disabled by default) | No (security risk) |
| Mutual TLS | Yes (tls-auth-clients) | Yes |
| Session caching | Yes | Yes |
| Cluster bus TLS | Yes (port + 10000) | Yes |
| Replication TLS | Yes | Yes |
| Certificate allowlist | Yes (Redis 7.2+) | Yes |
| ACL user mapping (CN) | Yes (Redis 7.2+) | Yes |
| Cipher suite control | Yes | Yes |
| Protocol version control | Yes | Yes |
| DH params | Yes (OpenSSL < 3.0) | Yes |

### 11.2 Client Library Compatibility

All major Redis clients support TLS. No changes needed for client compatibility.

**Verified clients:**
- redis-cli (--tls, --cert, --key, --cacert flags)
- redis-py (connection_kwargs={'ssl_ca_certs': ...})
- ioredis (tls: { ca: fs.readFileSync(...) })
- go-redis (TLSConfig: &tls.Config{...})
- Jedis (SSLSocketFactory)
- redis-rb (ssl_params: { ca_file: ... })
- Lettuce (SslOptions)
- StackExchange.Redis (ssl=true, sslHost=...)

---

## 12. Security Considerations

### 12.1 Secure Defaults

**Zoltraak TLS defaults prioritize security:**

| Parameter | Default | Rationale |
|-----------|---------|-----------|
| `tls-protocols` | "TLSv1.2 TLSv1.3" | Disable insecure TLS 1.0/1.1 |
| `tls-auth-clients` | "yes" | Mutual TLS by default (defense in depth) |
| `tls-prefer-server-ciphers` | true | Server enforces strong cipher preference |
| `tls-session-caching` | true | Performance optimization (safe) |
| `tls-ciphers` | "" (OpenSSL defaults) | Use OpenSSL's secure defaults for TLS 1.2 |
| `tls-ciphersuites` | "" (OpenSSL defaults) | Use OpenSSL's secure defaults for TLS 1.3 |

### 12.2 Weak Cipher Protection

**Automatically disable weak ciphers:**
- NULL ciphers (no encryption)
- Export ciphers (40/56-bit keys)
- RC4 (stream cipher vulnerabilities)
- MD5-based ciphers
- DES/3DES (insufficient key size)

**If user explicitly sets `tls-ciphers`:** Log warning if weak cipher detected, but allow (user override).

### 12.3 Certificate Validation

**Strict validation by default:**
- Certificate must be valid X.509 format
- Certificate must not be expired
- Certificate must be issued by trusted CA (if tls-auth-clients=yes)
- Certificate chain must be valid
- **No hostname validation** (Redis doesn't validate hostname by default)

**Hostname validation:** Not implemented in v1.0 (Redis doesn't do it either).

### 12.4 Private Key Protection

**Key file permissions:**
- Check that tls-key-file has restricted permissions (mode 0600 or 0400)
- Log warning if key file is world-readable
- Do NOT fail startup (user may have different security model)

**Key passphrase:**
- If tls-key-file-pass is set, store in memory securely (no logging)
- Zero memory after use
- Never expose passphrase in CONFIG GET

---

## 13. Open Questions & Future Work

### 13.1 Out of Scope for v1.0

1. **SIGHUP certificate reload** — Redis supports runtime cert reload via SIGHUP signal. Zoltraak v1.0 requires restart.
2. **OCSP stapling** — Online Certificate Status Protocol for real-time revocation checking.
3. **Certificate pinning** — Pin specific certificates or public keys.
4. **Hostname validation** — Validate server hostname matches certificate CN/SAN.
5. **TLS 1.1 support** — Deprecated protocol, not implemented for security reasons.
6. **Client-side TLS for Lua http.request** — Lua scripting doesn't support HTTPS in v1.0.

### 13.2 Implementation Questions

**Q1:** Should `tls-port = 0` disable TLS even if other tls-* parameters are set?
**A1:** Yes. `tls-port = 0` means TLS disabled, ignore all other TLS config.

**Q2:** Should we support TLS on the default port (6379) if both `port` and `tls-port` are set to same value?
**A2:** No. Redis doesn't support this. TLS and plain TCP must use different ports.

**Q3:** Should session cache be shared across server restarts (persist to disk)?
**A3:** No. Session cache is in-memory only (same as Redis).

**Q4:** Should we validate certificate expiration at startup or only during handshake?
**A4:** Both. Validate at startup and log warning. Also validate during handshake.

**Q5:** Should we support TLS-PSK (pre-shared key) authentication?
**A5:** No. Redis doesn't support PSK. Out of scope for v1.0.

---

## 14. Success Metrics

### 14.1 Functionality

- [ ] TLS 1.2 connections work with redis-cli --tls
- [ ] TLS 1.3 connections work with redis-cli --tls
- [ ] Mutual TLS (client cert) works
- [ ] Session caching reduces handshake time by >= 50%
- [ ] Cluster bus TLS works (3-node cluster)
- [ ] Replication TLS works (primary-replica)
- [ ] All 20 TLS config parameters accessible via CONFIG GET/SET
- [ ] CLIENT LIST shows TLS connection details
- [ ] INFO shows TLS stats

### 14.2 Performance

- [ ] TLS throughput >= 70% of plain TCP throughput
- [ ] TLS latency p99 <= 1.5x plain TCP latency
- [ ] Session cache hit rate >= 80% under normal load
- [ ] No memory leaks in OpenSSL integration (valgrind clean)

### 14.3 Security

- [ ] Weak ciphers disabled by default
- [ ] TLS 1.0/1.1 disabled by default
- [ ] Certificate validation enforced (no self-signed by default unless in CA bundle)
- [ ] Private key file permission warning if world-readable
- [ ] No passphrase logging or exposure via CONFIG GET

### 14.4 Compatibility

- [ ] redis-cli --tls works identically to Redis
- [ ] redis-py with ssl=True works
- [ ] ioredis with tls: {...} works
- [ ] All CLIENT LIST TLS fields match Redis output format
- [ ] All INFO TLS fields match Redis output format

---

## 15. References

### 15.1 Redis Documentation
- [TLS | Docs](https://redis.io/docs/latest/operate/oss_and_stack/management/security/encryption/)
- [Configure TLS in Redis Cluster](https://dev.to/hedgehog/configure-tls-in-redis-cluster-50ec)
- [Configure TLS protocol](https://redis.io/docs/latest/operate/rs/security/encryption/tls/tls-protocols/)
- [Configure cipher suites](https://redis.io/docs/latest/operate/rs/security/encryption/tls/ciphers/)

### 15.2 Troubleshooting Guides
- [How to Troubleshoot Redis TLS Handshake Failures](https://oneuptime.com/blog/post/2026-03-31-redis-troubleshoot-redis-tls-handshake-failures/view)
- [Troubleshooting TLS Failures – Redis Knowledge Base](https://support.redislabs.com/hc/en-us/articles/26867190871314-Troubleshooting-TLS-Failures)

### 15.3 Zig & OpenSSL Resources
- [openssl-zig](https://github.com/kassane/openssl-zig) — OpenSSL bindings for Zig
- [std.crypto.tls](https://github.com/ziglang/zig/blob/master/lib/std/crypto/tls.zig) — Zig standard library TLS (TLS 1.3 only)
- [Redis cluster port configuration](https://www.hostiserver.com/community/articles/cracking-the-redis-port-puzzle-a-down-to-earth-guide-to-setup-and-function)

---

## Appendix A: TLS Configuration Examples

### A.1 Basic TLS Server (no client cert required)

```conf
port 0                              # Disable plain TCP
tls-port 6379                       # TLS on default port
tls-cert-file /etc/redis/server.crt
tls-key-file /etc/redis/server.key
tls-ca-cert-file /etc/redis/ca.crt
tls-auth-clients no                 # No client cert required
```

### A.2 Mutual TLS (client cert required)

```conf
port 6379                           # Plain TCP on 6379
tls-port 6380                       # TLS on 6380
tls-cert-file /etc/redis/server.crt
tls-key-file /etc/redis/server.key
tls-ca-cert-file /etc/redis/ca.crt
tls-auth-clients yes                # Require client cert
```

### A.3 TLS-Only Cluster

```conf
port 0                              # Disable plain TCP
tls-port 7000                       # TLS on 7000
cluster-enabled yes
tls-cluster yes                     # Cluster bus TLS (port 17000)
tls-replication yes                 # Replication TLS
tls-cert-file /etc/redis/server.crt
tls-key-file /etc/redis/server.key
tls-ca-cert-file /etc/redis/ca.crt
tls-client-cert-file /etc/redis/client.crt
tls-client-key-file /etc/redis/client.key
```

### A.4 Cipher Suite Restriction

```conf
tls-port 6379
tls-cert-file /etc/redis/server.crt
tls-key-file /etc/redis/server.key
tls-ca-cert-file /etc/redis/ca.crt
tls-protocols "TLSv1.3"             # TLS 1.3 only
tls-ciphersuites "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256"
tls-prefer-server-ciphers yes
```

---

## Appendix B: Error Message Reference

### B.1 Startup Errors

| Error | Message | Cause | Solution |
|-------|---------|-------|----------|
| Missing cert | `ERR certificate file not found: /path/to/cert.crt` | tls-cert-file path invalid | Check file path, ensure file exists |
| Missing key | `ERR private key file not found: /path/to/key.key` | tls-key-file path invalid | Check file path, ensure file exists |
| Key passphrase | `ERR failed to decrypt private key` | Incorrect passphrase in tls-key-file-pass | Verify passphrase is correct |
| Cert/key mismatch | `ERR certificate and private key do not match` | Public key in cert != public key derived from private key | Regenerate cert from key, or vice versa |
| Invalid protocol | `ERR invalid TLS protocol version: TLSv1.0` | Unsupported protocol in tls-protocols | Use TLSv1.2 or TLSv1.3 |
| Missing CA | `ERR CA certificate file not found: /path/to/ca.crt` | tls-ca-cert-file path invalid (when tls-auth-clients=yes) | Check file path |

### B.2 Handshake Errors (TLS Alerts)

| Alert | Numeric Code | Cause | Client Sees |
|-------|--------------|-------|-------------|
| protocol_version | 70 | Client requested unsupported TLS version | `SSL_connect() error: wrong version number` |
| certificate_required | 116 | Client didn't provide cert (tls-auth-clients=yes) | `SSL_connect() error: peer did not return a certificate` |
| certificate_expired | 45 | Client cert expired | `SSL_connect() error: certificate verify failed` |
| certificate_unknown | 46 | Client cert not yet valid or other issue | `SSL_connect() error: certificate verify failed` |
| unsupported_certificate | 43 | Wrong cert type (e.g., server cert used as client cert) | `SSL_connect() error: unsupported certificate` |
| handshake_failure | 40 | Generic handshake error | `SSL_connect() error: handshake failure` |
| insufficient_security | 71 | No common cipher suite | `SSL_connect() error: no shared cipher` |

---

**End of Specification**
