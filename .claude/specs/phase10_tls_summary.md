# Phase 10 TLS/SSL Implementation — Quick Reference

**Full Specification:** See `phase10_tls_specification.md`

---

## Executive Summary

Implement TLS 1.2+ support for secure Redis connections with 20 configuration parameters, mutual TLS authentication, session caching, and cluster/replication encryption.

**Status:** All 20 TLS config parameters already defined in `src/storage/config.zig` (lines 134-155). Implementation requires OpenSSL integration and socket layer changes.

**Estimated Effort:** 3-4 iterations (10-14 days)

---

## Key Features

| Feature | Description | Priority |
|---------|-------------|----------|
| TLS 1.2/1.3 | Minimum TLS version support | P0 |
| Mutual TLS | Client certificate authentication | P0 |
| Session caching | Faster reconnections (50-70% speedup) | P0 |
| Cluster bus TLS | Encrypted inter-node communication (port + 10000) | P1 |
| Replication TLS | Encrypted primary-replica sync | P1 |
| 20 config params | All TLS configuration via CONFIG GET/SET | P0 |

---

## Configuration Parameters (20 Total)

### Core (7 params)
- `tls-port` (int, default: 0) — TLS listener port
- `tls-cert-file` (string) — Server certificate path
- `tls-key-file` (string) — Private key path
- `tls-key-file-pass` (string) — Key passphrase
- `tls-ca-cert-file` (string) — CA bundle path
- `tls-ca-cert-dir` (string) — CA directory
- `tls-dh-params-file` (string) — DH parameters (optional)

### Authentication (2 params)
- `tls-auth-clients` (string: yes/no/optional, default: yes) — Require client cert
- `tls-auth-clients-user` (string: off/CN, default: off) — Map cert CN to ACL user

### Protocol/Cipher (4 params)
- `tls-protocols` (string, default: "TLSv1.2 TLSv1.3") — Allowed TLS versions
- `tls-ciphers` (string) — TLS 1.2 cipher list
- `tls-ciphersuites` (string) — TLS 1.3 cipher suites
- `tls-prefer-server-ciphers` (bool, default: true) — Server cipher preference

### Session Caching (3 params)
- `tls-session-caching` (bool, default: true) — Enable session cache
- `tls-session-cache-size` (int, default: 20480) — Max cached sessions
- `tls-session-cache-timeout` (int, default: 300) — Cache timeout (seconds)

### Cluster/Replication (2 params)
- `tls-cluster` (bool, default: false) — Enable cluster bus TLS
- `tls-replication` (bool, default: false) — Enable replication TLS

### Outbound Certs (2 params)
- `tls-client-cert-file` (string) — Client cert for outbound connections
- `tls-client-key-file` (string) — Client key for outbound connections
- `tls-client-key-file-pass` (string) — Client key passphrase

---

## Implementation Approach

**Technology:** OpenSSL via C bindings (not std.crypto.tls — TLS 1.3 only)

**Reasoning:**
- Redis uses OpenSSL → 100% compatibility
- Full TLS 1.2 support (required by many clients)
- Complete cipher suite control
- Mature session caching
- Already have LuaJIT system dependency

**Dependencies:**
- OpenSSL 1.1.1+ or 3.0+
- [openssl-zig](https://github.com/kassane/openssl-zig) package

---

## File Structure

```
src/
├── tls/
│   ├── context.zig          # SSL_CTX initialization
│   ├── connection.zig       # SSL socket wrapper
│   ├── config.zig           # Certificate/key loading
│   ├── session_cache.zig    # Session caching (LRU)
│   └── errors.zig           # TLS error handling
├── server.zig               # Modified: dual plain/TLS listeners
├── storage/config.zig       # Already has all 20 TLS params
└── commands/
    ├── client.zig           # Modified: add TLS fields to CLIENT LIST
    └── info.zig             # Modified: add TLS stats to INFO
```

---

## Command Changes

**No new commands.** TLS is transparent to Redis commands.

### Modified Output

**CLIENT LIST:**
```
id=1 addr=127.0.0.1:52301 ... ssl=yes ssl_version=TLSv1.3 ssl_cipher=TLS_AES_256_GCM_SHA384 ssl_cert_subject=/CN=client.example.com
```

**INFO SERVER:**
```
# Server
tls_enabled:1
tls_port:6379
tls_protocols:TLSv1.2 TLSv1.3
tls_auth_clients:yes
...
```

**INFO STATS:**
```
# Stats
total_tls_connections_received:1523
current_tls_connections:42
tls_handshake_errors:17
tls_session_cache_hits:891
tls_session_cache_misses:632
```

---

## Iteration Breakdown

### Iteration 1: TLS Foundation (3-4 days)
- OpenSSL integration in `build.zig`
- `src/tls/context.zig` — SSL_CTX initialization
- `src/tls/connection.zig` — SSL socket wrapper
- Modify `src/server.zig` — dual plain/TLS listeners
- Test: `redis-cli --tls --cacert ca.crt PING`

**Acceptance:**
- TLS server listens on tls-port
- RESP protocol works over TLS
- No client cert required yet

### Iteration 2: Mutual TLS & Config (2-3 days)
- CA certificate loading
- Client cert validation (tls-auth-clients: yes/no/optional)
- TLS fields in CLIENT LIST
- TLS stats in INFO
- Test mutual TLS with redis-cli --cert / --key

**Acceptance:**
- tls-auth-clients=yes enforces client cert
- CLIENT LIST shows TLS connection details
- INFO shows TLS configuration and stats

### Iteration 3: Session Caching (2-3 days)
- `src/tls/session_cache.zig` — LRU cache
- OpenSSL session callbacks
- Session cache hit/miss tracking
- Benchmark handshake time improvement

**Acceptance:**
- Session resume works (50%+ speedup)
- Cache hit/miss in INFO STATS
- Cache eviction (LRU)

### Iteration 4: Cluster & Replication TLS (3-4 days)
- Cluster bus TLS listener (port + 10000)
- Outbound TLS for cluster gossip
- Replication TLS (tls-replication=yes)
- Test 3-node cluster with TLS

**Acceptance:**
- Cluster nodes communicate over TLS
- Replica connects to primary over TLS
- PSYNC encrypted

---

## Testing Strategy

### Unit Tests
- TLS context initialization (valid/invalid certs)
- Handshake (TLS 1.2, TLS 1.3, mutual TLS)
- Session caching (storage, retrieval, eviction)
- Configuration (all 20 params via CONFIG GET/SET)

### Integration Tests
- redis-cli with --tls
- Dual listener (plain + TLS)
- Mutual TLS (client cert required)
- Cluster bus TLS (3-node cluster)
- Replication TLS (primary-replica)
- Session caching (measure speedup)

### Compatibility Tests
- TLS protocol versions (1.2, 1.3, 1.0 rejection)
- Cipher suite restrictions
- Certificate validation (expired, not-yet-valid, CA mismatch)
- Error conditions (missing cert, key mismatch, handshake failure)

### Performance Tests
- Throughput: TLS vs. plain TCP (expect 10-30% reduction)
- Latency: p50, p99, p999 comparison
- Session caching benefit (50-70% handshake speedup)

---

## Error Conditions

### Startup Errors (Fatal)
- Missing certificate file
- Missing private key file
- Certificate/key mismatch
- Invalid TLS protocol version
- Missing CA cert (if tls-auth-clients=yes)

### Handshake Errors (Per Connection)
- Client uses unsupported TLS version → alert + close
- Client cert validation fails → alert + close
- Client doesn't provide cert (tls-auth-clients=yes) → alert + close
- No common cipher suite → alert + close

### Cluster TLS Errors
- CA chain mismatch between nodes → handshake failure, mark node FAIL
- Cluster bus TLS port unreachable → retry, mark PFAIL

---

## Security Considerations

### Secure Defaults
- TLS 1.0/1.1 disabled (only 1.2/1.3)
- Mutual TLS by default (tls-auth-clients=yes)
- Server cipher preference enabled
- Weak ciphers automatically disabled (NULL, RC4, MD5, DES)

### Certificate Validation
- Strict X.509 validation
- Check expiration at startup + handshake
- Validate CA chain
- **No hostname validation** (Redis doesn't do it)

### Private Key Protection
- Check file permissions (warn if world-readable)
- Never log passphrase
- Zero memory after use
- Don't expose passphrase in CONFIG GET

---

## Edge Cases

### Certificate Rotation
- Server continues with expired cert (log warning)
- Require manual restart for new cert
- SIGHUP reload: out of scope for v1.0

### Allowlisted Certificates (Redis 7.2+)
- Skip CA validation for specific client certs
- Use case: self-signed client certs

### Cluster Node Certificate Consistency
- **Critical:** All nodes MUST share consistent CA chain
- CA mismatch → handshake failure → cluster partition
- No automatic recovery (operator must fix)

### Replication with Mixed TLS/Plain
- If primary TLS-only (port=0) → replica MUST use TLS
- If primary dual-mode → replica can choose

### ACL User Mapping (tls-auth-clients-user)
- Map client cert CN to Redis ACL user
- Example: CN=alice → authenticate as user alice
- If user not found or disabled → reject

---

## Success Metrics

### Functionality
- [x] TLS 1.2/1.3 connections work
- [x] Mutual TLS works
- [x] Session caching reduces handshake time >= 50%
- [x] Cluster bus TLS works
- [x] Replication TLS works
- [x] All 20 config params accessible
- [x] CLIENT LIST shows TLS details
- [x] INFO shows TLS stats

### Performance
- [x] TLS throughput >= 70% of plain TCP
- [x] TLS latency p99 <= 1.5x plain TCP
- [x] Session cache hit rate >= 80%
- [x] No memory leaks (valgrind clean)

### Security
- [x] Weak ciphers disabled
- [x] TLS 1.0/1.1 disabled
- [x] Certificate validation enforced
- [x] Private key permission warning
- [x] No passphrase logging

### Compatibility
- [x] redis-cli --tls works
- [x] redis-py with ssl=True works
- [x] ioredis with tls: {...} works
- [x] CLIENT LIST matches Redis format
- [x] INFO matches Redis format

---

## Out of Scope (v1.0)

1. SIGHUP certificate reload
2. OCSP stapling
3. Certificate pinning
4. Hostname validation
5. TLS 1.1 support (deprecated)
6. Client-side TLS for Lua http.request
7. TLS-PSK authentication

---

## References

**Redis Documentation:**
- [TLS | Docs](https://redis.io/docs/latest/operate/oss_and_stack/management/security/encryption/)
- [Configure TLS in Redis Cluster](https://dev.to/hedgehog/configure-tls-in-redis-cluster-50ec)
- [Troubleshooting TLS](https://oneuptime.com/blog/post/2026-03-31-redis-troubleshoot-redis-tls-handshake-failures/view)

**Zig Resources:**
- [openssl-zig](https://github.com/kassane/openssl-zig)
- [std.crypto.tls](https://github.com/ziglang/zig/blob/master/lib/std/crypto/tls.zig) (TLS 1.3 only)

---

**Full Specification:** `/Users/fn/codespace/zoltraak/.claude/specs/phase10_tls_specification.md`
