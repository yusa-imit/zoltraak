# Redis TLS Quick Reference

**For Zoltraak Phase 10 Implementation**

---

## TL;DR — Minimum Viable TLS

**Iteration 250 Scope (Foundation)**:
- Parse 20 TLS config parameters
- Bind TLS listener on `tls-port`
- Return error stub on TLS connections
- Full handshake deferred to Iteration 251+

**Iteration 251+ Scope (Full Implementation)**:
- TLS 1.2/1.3 server handshake
- Certificate validation
- Mutual TLS (client certs)
- Session caching

---

## Essential Config Parameters (Top 7)

| Parameter | Type | Required | Example |
|-----------|------|----------|---------|
| `tls-port` | u16 | YES | 6380 |
| `tls-cert-file` | path | YES | `/etc/redis/tls/server.crt` |
| `tls-key-file` | path | YES | `/etc/redis/tls/server.key` |
| `tls-ca-cert-file` | path | If mTLS | `/etc/redis/tls/ca.crt` |
| `tls-auth-clients` | enum | NO | `yes` / `no` / `optional` (default: `yes`) |
| `tls-protocols` | string | NO | `"TLSv1.2 TLSv1.3"` |
| `tls-session-caching` | bool | NO | `yes` (default) |

**Minimal TLS config**:
```conf
tls-port 6380
tls-cert-file /etc/redis/tls/server.crt
tls-key-file /etc/redis/tls/server.key
tls-auth-clients no
```

---

## CONFIG GET/SET Mapping

### CONFIG GET tls-port
**Response**: `["tls-port", "6380"]`

### CONFIG SET tls-port 6380
**Response**: `+OK`
**Side effect**: Requires server restart (runtime TLS port changes not supported)

### CONFIG GET tls-auth-clients
**Response**: `["tls-auth-clients", "yes"]`

### CONFIG SET tls-auth-clients optional
**Response**: `+OK`
**Validation**: Must be `yes`, `no`, or `optional` (case-insensitive)

---

## Command-Line Flags

```bash
./zoltraak \
  --port 6379 \
  --tls-port 6380 \
  --tls-cert-file /etc/redis/tls/server.crt \
  --tls-key-file /etc/redis/tls/server.key \
  --tls-ca-cert-file /etc/redis/tls/ca.crt \
  --tls-auth-clients yes
```

**Dual-stack mode**: Both `--port` and `--tls-port` set (plain TCP + TLS)

---

## TLS Handshake Flow (Visual)

```
Client --> [ClientHello] --> Server
Client <-- [ServerHello, Certificate] <-- Server
Client <-- [CertificateRequest] <-- Server (if tls-auth-clients yes)
Client --> [Certificate, ClientKeyExchange] --> Server
Client --> [CertificateVerify] --> Server
Client --> [Finished] --> Server
Client <-- [Finished] <-- Server
Client <==> [Encrypted RESP Commands] <==> Server
```

**TLS 1.3 optimization**: 1-RTT handshake instead of 2-RTT

---

## Error Messages

| Condition | Error Response |
|-----------|----------------|
| TLS connection to plain port | `ERR TLS is not configured` |
| Plain connection to TLS port | `ERR expected TLS connection` |
| Missing server cert | `ERR Server certificate not configured` |
| Invalid cert file | `ERR Failed to load certificate` |
| Missing client cert (when required) | `ERR Client authentication required` |
| Untrusted client cert | `ERR Invalid client certificate` |
| Cipher mismatch | TLS alert 40 (handshake_failure) |
| Expired certificate | TLS alert 45 (certificate_expired) |

---

## Cipher Suites (Recommended)

**TLS 1.3** (use `tls-ciphersuites`):
```
TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256
```

**TLS 1.2** (use `tls-ciphers`):
```
ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256
```

**Prohibited ciphers**: `RC4`, `DES`, `3DES`, `MD5`, `SHA1`, `EXPORT`, `NULL`, `anon`

---

## Zig Implementation (std.crypto.tls)

**Current Status**:
- ✅ TLS 1.3 Client (`std.crypto.tls.Client`)
- ⚠️ TLS 1.2 Client (partial, edge case bugs)
- ❌ TLS 1.3 Server (not in stdlib)
- ❌ TLS 1.2 Server (not in stdlib)

**Alternatives**:
1. **ianic/tls.zig** — Pure Zig, TLS 1.3/1.2 client + TLS 1.3 server
2. **openssl-zig** — Bindings to OpenSSL (C dependency)

**Recommendation**: Use `ianic/tls.zig` for MVP, evaluate OpenSSL for production.

---

## Testing with redis-cli

**Connect with TLS (server cert only)**:
```bash
redis-cli -h localhost -p 6380 --tls \
  --cacert /etc/redis/tls/ca.crt
```

**Connect with mutual TLS (client cert)**:
```bash
redis-cli -h localhost -p 6380 --tls \
  --cacert /etc/redis/tls/ca.crt \
  --cert /etc/redis/tls/client.crt \
  --key /etc/redis/tls/client.key
```

**Skip verification (INSECURE, testing only)**:
```bash
redis-cli -h localhost -p 6380 --tls --insecure
```

---

## Certificate Generation (Quick Test Setup)

**1. Generate CA certificate**:
```bash
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 3650 -key ca.key -out ca.crt \
  -subj "/CN=Redis Test CA"
```

**2. Generate server certificate**:
```bash
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr \
  -subj "/CN=localhost"
openssl x509 -req -days 365 -in server.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out server.crt
```

**3. Generate client certificate**:
```bash
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr \
  -subj "/CN=redis-client"
openssl x509 -req -days 365 -in client.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out client.crt
```

**4. Set permissions**:
```bash
chmod 600 server.key client.key ca.key
chmod 644 server.crt client.crt ca.crt
```

---

## Performance Metrics

| Metric | Plain TCP | TLS 1.2 | TLS 1.3 |
|--------|-----------|---------|---------|
| Handshake RTT | 0 | 2-RTT | 1-RTT |
| Latency overhead | 0% | +5-10% | +3-5% |
| Throughput overhead | 0% | -10-15% | -5-10% |
| CPU overhead | 0% | +10-20% | +5-15% |

**With AES-NI hardware acceleration**: Overhead reduces to <5% for AES-GCM ciphers.

---

## Security Checklist

- [ ] Use TLS 1.2 minimum (`tls-protocols "TLSv1.2 TLSv1.3"`)
- [ ] Disable weak ciphers (no RC4/DES/3DES)
- [ ] Enable mutual TLS (`tls-auth-clients yes`)
- [ ] Use strong keys (RSA 4096 or ECDSA P-384)
- [ ] Rotate certificates every 90 days
- [ ] Restrict private key permissions (`chmod 600`)
- [ ] Monitor certificate expiration (alert 30 days before)
- [ ] Enable session caching for performance
- [ ] Use separate certs for server and client
- [ ] Enable TLS for cluster bus and replication in production

---

## Iteration 250 Implementation Checklist

**Storage layer** (`src/storage/tls_config.zig`):
- [ ] `TlsConfig` struct with 20 fields
- [ ] `init()` / `deinit()` methods
- [ ] `isEnabled()` helper (returns `port > 0`)
- [ ] `validateConfig()` method (check cert files exist)

**Server layer** (`src/server.zig`):
- [ ] `tls_listener: ?std.net.Server` field
- [ ] Bind TLS listener in `start()` if `tls_config.isEnabled()`
- [ ] `handleTlsConnection()` stub (return error message)

**Command layer** (`src/commands/config.zig`):
- [ ] CONFIG GET support for all 20 `tls-*` parameters
- [ ] CONFIG SET support for all 20 `tls-*` parameters
- [ ] Case-insensitive parameter matching
- [ ] Validation for enum types (`tls-auth-clients`)

**CLI layer** (`src/main.zig`):
- [ ] Parse `--tls-port` flag
- [ ] Parse `--tls-cert-file` flag
- [ ] Parse `--tls-key-file` flag
- [ ] Parse `--tls-auth-clients` flag
- [ ] Parse other TLS flags as needed

**Tests**:
- [ ] Unit tests for `TlsConfig` (init/deinit, validation)
- [ ] Integration tests for CONFIG GET/SET
- [ ] Integration test: TLS connection returns error stub
- [ ] Manual test: `redis-cli --tls` to port 6380 (expect error)

**Documentation**:
- [ ] Update README.md with TLS section
- [ ] Update CLAUDE.md with Phase 10 status
- [ ] Update docs/PRD.md with Iteration 250 completion

---

## Future Iterations

**Iteration 251** — TLS Handshake Implementation
- Choose TLS backend (ianic/tls.zig vs OpenSSL)
- Implement server handshake state machine
- Load server certificate and private key
- Accept TLS connections and perform handshake
- Wrap connection stream in TLS context

**Iteration 252** — Mutual TLS (Client Certificates)
- Load CA certificate bundle
- Request client certificate in handshake
- Verify client certificate chain
- Implement `tls-auth-clients` modes (yes/no/optional)
- Integration with ACL system

**Iteration 253** — Cluster & Replication TLS
- Enable TLS for cluster bus (`tls-cluster yes`)
- Enable TLS for replication (`tls-replication yes`)
- Implement outbound TLS client for CLUSTER MEET and REPLICAOF
- Use `tls-client-cert-file` for outbound connections

**Iteration 254** — Session Caching & Performance
- Implement TLS session cache (LRU)
- Session resumption (TLS 1.2 session IDs)
- 0-RTT resumption (TLS 1.3 session tickets)
- Performance benchmarks vs plain TCP

---

## Key Resources

- [Redis TLS Documentation](https://redis.io/docs/latest/operate/oss_and_stack/management/security/encryption/)
- [RFC 8446 — TLS 1.3](https://datatracker.ietf.org/doc/html/rfc8446)
- [ianic/tls.zig](https://github.com/ianic/tls.zig)
- [openssl-zig](https://github.com/glingy/openssl-zig)
- [Zig TLS Client Tutorial](https://sheran.sg/blog/figured-out-zig-tls-client/)

---

**Last Updated**: 2026-05-03
**For**: Zoltraak Iteration 250 (Phase 10 TLS Foundation)
