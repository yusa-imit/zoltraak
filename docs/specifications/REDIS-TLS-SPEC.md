# Redis TLS/SSL Specification

**Version**: Redis 6.0+ (TLS introduced as optional compile-time feature)
**Target Compatibility**: Redis 7.x/8.x TLS behavior
**Document Purpose**: Specification reference for Zoltraak Phase 10 TLS implementation

---

## 1. Overview

Redis supports TLS/SSL encryption for:
- Client-to-server connections (standard data port)
- Cluster bus communication (inter-node gossip, slot migration)
- Replication connections (primary-to-replica sync)

TLS support is **optional** and **compile-time enabled** in Redis. When enabled, Redis can operate in three modes:
1. **Plain TCP only** (`tls-port 0`)
2. **TLS only** (`port 0`, `tls-port 6380`)
3. **Dual-stack** (both `port 6379` and `tls-port 6380` active)

---

## 2. Configuration Parameters

### 2.1 Core TLS Settings

| Parameter | Type | Required | Default | Description | Redis Version |
|-----------|------|----------|---------|-------------|---------------|
| `tls-port` | integer | No | 0 | Port for TLS connections (0 = disabled) | 6.0+ |
| `tls-cert-file` | path | Yes* | - | Server certificate file (PEM format) | 6.0+ |
| `tls-key-file` | path | Yes* | - | Server private key file (PEM format) | 6.0+ |
| `tls-key-file-pass` | string | No | - | Password for encrypted private key | 6.0+ |
| `tls-ca-cert-file` | path | No** | - | CA certificate bundle for client verification | 6.0+ |
| `tls-ca-cert-dir` | path | No** | - | Directory containing CA certificates | 6.0+ |
| `tls-auth-clients` | enum | No | yes | Client certificate requirement (yes/no/optional) | 6.0+ |

**\*Required** if `tls-port > 0`
**\*\*Required** if `tls-auth-clients yes` or `tls-auth-clients optional`

**tls-auth-clients modes**:
- `yes` — Client MUST present a valid certificate (mutual TLS)
- `no` — Client certificate NOT required (server-only TLS)
- `optional` — Client MAY present a certificate; if presented, it MUST be valid

### 2.2 Protocol & Cipher Configuration

| Parameter | Type | Default | Description | Redis Version |
|-----------|------|---------|-------------|---------------|
| `tls-protocols` | string | "TLSv1.2 TLSv1.3" | Space-separated allowed TLS versions | 6.0+ |
| `tls-ciphers` | string | (OpenSSL default) | Colon-separated TLS 1.2 cipher list | 6.0+ |
| `tls-ciphersuites` | string | (OpenSSL default) | Colon-separated TLS 1.3 cipher suites | 6.0+ |
| `tls-prefer-server-ciphers` | bool | yes | Prefer server cipher order over client | 6.0+ |

**Recommended TLS 1.2 ciphers** (Redis documentation):
```
ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256
```

**Recommended TLS 1.3 ciphersuites** (Redis documentation):
```
TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256
```

**Deprecated ciphers** (MUST NOT be supported):
- 3DES (weak, removed in Redis 7.0)
- RC4 (broken, removed in Redis 6.2)
- Any cipher using MD5 or SHA1 signatures

### 2.3 Session Management

| Parameter | Type | Default | Description | Redis Version |
|-----------|------|---------|-------------|---------------|
| `tls-session-caching` | bool | yes | Enable TLS session resumption | 6.0+ |
| `tls-session-cache-size` | integer | 20480 | Maximum number of cached sessions | 6.0+ |
| `tls-session-cache-timeout` | integer | 300 | Session cache TTL (seconds) | 6.0+ |

**Session resumption** reduces handshake latency for reconnecting clients by caching:
- Session ID (TLS 1.2)
- Session tickets (TLS 1.3)

### 2.4 Cluster & Replication TLS

| Parameter | Type | Default | Description | Redis Version |
|-----------|------|---------|-------------|---------------|
| `tls-cluster` | bool | no | Enable TLS for cluster bus (port+10000) | 6.0+ |
| `tls-replication` | bool | no | Enable TLS for replica-to-primary connections | 6.0+ |

When `tls-cluster yes`:
- Cluster bus listens on `tls-port + 10000` with TLS
- All CLUSTER MEET messages use TLS
- Gossip protocol (PING/PONG/MEET) encrypted

When `tls-replication yes`:
- REPLICAOF command initiates TLS handshake before PSYNC
- Full resync (RDB transfer) encrypted
- Incremental sync (replication backlog) encrypted

### 2.5 Client Certificate Override (Outbound Connections)

| Parameter | Type | Default | Description | Redis Version |
|-----------|------|---------|-------------|---------------|
| `tls-client-cert-file` | path | - | Client certificate for outbound TLS | 6.0+ |
| `tls-client-key-file` | path | - | Client private key for outbound TLS | 6.0+ |
| `tls-client-key-file-pass` | string | - | Password for client key | 6.0+ |

Used when Redis acts as a **TLS client** (not server):
- REPLICAOF to a TLS-enabled primary
- CLUSTER MEET to a TLS-enabled node
- MIGRATE to a TLS-enabled destination

If not set, Redis uses `tls-cert-file` and `tls-key-file` for outbound connections.

### 2.6 Additional Options (Redis 7.2+)

| Parameter | Type | Default | Description | Redis Version |
|-----------|------|---------|-------------|---------------|
| `tls-allowlisted-certs` | path | - | File containing allowlisted client cert fingerprints | 7.2+ |

**Certificate pinning**: Only clients with certificates matching SHA256 fingerprints in this file are allowed (stricter than CA-based validation).

---

## 3. TLS Handshake Protocol

### 3.1 Standard TLS 1.2 Handshake

```
Client                                      Server
  |                                            |
  |--- ClientHello --------------------------->| (TLS version, ciphers, SNI)
  |                                            |
  |<-- ServerHello, Certificate ---------------|
  |<-- ServerKeyExchange (if ECDHE) -----------|
  |<-- CertificateRequest (if auth required) --|
  |<-- ServerHelloDone ------------------------|
  |                                            |
  |--- Certificate (if requested) ------------>|
  |--- ClientKeyExchange --------------------->|
  |--- CertificateVerify (if cert sent) ------>|
  |--- ChangeCipherSpec ---------------------->|
  |--- Finished ------------------------------>|
  |                                            |
  |<-- ChangeCipherSpec -----------------------|
  |<-- Finished -------------------------------|
  |                                            |
  |=== Encrypted Application Data ============>| (RESP protocol)
```

### 3.2 TLS 1.3 Handshake (Optimized)

```
Client                                      Server
  |                                            |
  |--- ClientHello, KeyShare ----------------->|
  |                                            |
  |<-- ServerHello, KeyShare ------------------|
  |<-- {EncryptedExtensions} ------------------|
  |<-- {CertificateRequest} (if auth) ---------|
  |<-- {Certificate, CertificateVerify} -------|
  |<-- {Finished} -----------------------------|
  |                                            |
  |--- {Certificate, CertificateVerify} ------>|
  |--- {Finished} ----------------------------->|
  |                                            |
  |=== Encrypted Application Data ============>|
```

**Key differences**:
- **1-RTT handshake** (vs 2-RTT in TLS 1.2)
- **0-RTT resumption** (session tickets with early data)
- **Always encrypted** (handshake messages after ServerHello)

### 3.3 Certificate Validation Steps

#### Server Certificate Validation (Client-Side)

1. **Chain of trust**: Verify certificate chain from leaf to root CA
2. **Signature verification**: Check CA signatures using public keys
3. **Validity period**: Ensure `notBefore <= current_time <= notAfter`
4. **Hostname verification** (if SNI provided): Match CN or SAN against server hostname
5. **Revocation check** (optional): Check CRL or OCSP (Redis does not implement this)

#### Client Certificate Validation (Server-Side, if `tls-auth-clients yes`)

1. **Chain of trust**: Verify client cert against CA in `tls-ca-cert-file` or `tls-ca-cert-dir`
2. **Signature verification**: Check CA signature
3. **Validity period**: Ensure certificate not expired
4. **CertificateVerify**: Verify client owns private key (signature over handshake transcript)
5. **Allowlist check** (if `tls-allowlisted-certs` set): Match SHA256 fingerprint

**Failure behavior**:
- Invalid server cert → Client closes connection with TLS alert
- Invalid client cert → Server sends TLS alert `certificate_unknown` (42) or `bad_certificate` (42)

---

## 4. Certificate Format & Requirements

### 4.1 File Formats

| Format | Extension | Encoding | Description |
|--------|-----------|----------|-------------|
| PEM | `.pem`, `.crt`, `.cer` | Base64 ASCII | Most common, human-readable |
| DER | `.der`, `.cer` | Binary | Not supported in Redis |
| PKCS#12 | `.p12`, `.pfx` | Binary | Not supported in Redis |

**PEM format example**:
```
-----BEGIN CERTIFICATE-----
MIIDXTCCAkWgAwIBAgIJAKZ... (base64 data)
-----END CERTIFICATE-----
```

**Certificate chain** (multiple certs in one file):
```
-----BEGIN CERTIFICATE-----
... (leaf certificate)
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
... (intermediate CA)
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
... (root CA)
-----END CERTIFICATE-----
```

### 4.2 Certificate Requirements

| Field | Requirement |
|-------|-------------|
| Version | X.509 v3 |
| Key Type | RSA (≥2048 bits), ECDSA (P-256/P-384), Ed25519 |
| Signature Algorithm | SHA256withRSA, SHA384withECDSA, SHA512withECDSA (NO MD5/SHA1) |
| Validity Period | Must be within `notBefore` and `notAfter` |
| Subject | Must have CN (Common Name) or SAN (Subject Alternative Name) |
| Key Usage | `digitalSignature`, `keyEncipherment` (for TLS server) |
| Extended Key Usage | `serverAuth` (for server cert), `clientAuth` (for client cert) |

**Prohibited**:
- Self-signed certificates (unless CA is in `tls-ca-cert-file`)
- Expired certificates
- Weak keys (RSA <2048 bits, MD5/SHA1 signatures)

### 4.3 Private Key Requirements

- **Format**: PEM (PKCS#1 or PKCS#8)
- **Encryption**: Optional (use `tls-key-file-pass` if encrypted)
- **Permissions**: File must be readable only by Redis process (chmod 600)

**PKCS#1 format** (RSA-specific):
```
-----BEGIN RSA PRIVATE KEY-----
...
-----END RSA PRIVATE KEY-----
```

**PKCS#8 format** (generic):
```
-----BEGIN PRIVATE KEY-----
...
-----END PRIVATE KEY-----
```

---

## 5. Command-Line Arguments

Redis server supports TLS configuration via command-line flags (overrides redis.conf):

| Flag | Argument | Example |
|------|----------|---------|
| `--tls-port` | `<port>` | `--tls-port 6380` |
| `--tls-cert-file` | `<path>` | `--tls-cert-file /etc/redis/tls/redis.crt` |
| `--tls-key-file` | `<path>` | `--tls-key-file /etc/redis/tls/redis.key` |
| `--tls-key-file-pass` | `<password>` | `--tls-key-file-pass secret123` |
| `--tls-ca-cert-file` | `<path>` | `--tls-ca-cert-file /etc/redis/tls/ca.crt` |
| `--tls-ca-cert-dir` | `<path>` | `--tls-ca-cert-dir /etc/ssl/certs` |
| `--tls-auth-clients` | `yes\|no\|optional` | `--tls-auth-clients yes` |
| `--tls-protocols` | `"TLSv1.2 TLSv1.3"` | `--tls-protocols "TLSv1.3"` |
| `--tls-ciphers` | `<cipher-list>` | `--tls-ciphers "ECDHE-RSA-AES256-GCM-SHA384"` |
| `--tls-prefer-server-ciphers` | `yes\|no` | `--tls-prefer-server-ciphers yes` |
| `--tls-session-caching` | `yes\|no` | `--tls-session-caching yes` |
| `--tls-cluster` | `yes\|no` | `--tls-cluster yes` |
| `--tls-replication` | `yes\|no` | `--tls-replication yes` |

**Example: Dual-stack server with mutual TLS**:
```bash
redis-server \
  --port 6379 \
  --tls-port 6380 \
  --tls-cert-file /etc/redis/tls/server.crt \
  --tls-key-file /etc/redis/tls/server.key \
  --tls-ca-cert-file /etc/redis/tls/ca.crt \
  --tls-auth-clients yes
```

---

## 6. Client Connection Examples

### 6.1 redis-cli with TLS

**Server-only TLS** (no client cert):
```bash
redis-cli -h localhost -p 6380 --tls \
  --cacert /etc/redis/tls/ca.crt
```

**Mutual TLS** (with client cert):
```bash
redis-cli -h localhost -p 6380 --tls \
  --cacert /etc/redis/tls/ca.crt \
  --cert /etc/redis/tls/client.crt \
  --key /etc/redis/tls/client.key
```

**Skip server verification** (INSECURE, for testing only):
```bash
redis-cli -h localhost -p 6380 --tls --insecure
```

### 6.2 Client Library Examples

**Python (redis-py)**:
```python
import redis

r = redis.StrictRedis(
    host='localhost',
    port=6380,
    ssl=True,
    ssl_ca_certs='/etc/redis/tls/ca.crt',
    ssl_certfile='/etc/redis/tls/client.crt',
    ssl_keyfile='/etc/redis/tls/client.key'
)
```

**Node.js (node-redis)**:
```javascript
const redis = require('redis');
const fs = require('fs');

const client = redis.createClient({
  socket: {
    host: 'localhost',
    port: 6380,
    tls: true,
    ca: fs.readFileSync('/etc/redis/tls/ca.crt'),
    cert: fs.readFileSync('/etc/redis/tls/client.crt'),
    key: fs.readFileSync('/etc/redis/tls/client.key')
  }
});
```

---

## 7. Error Codes & Diagnostics

### 7.1 TLS Alert Codes (RFC 5246/8446)

| Alert Code | Name | Description | Common Cause |
|------------|------|-------------|--------------|
| 40 | handshake_failure | Generic handshake error | Cipher mismatch |
| 42 | bad_certificate | Invalid client certificate | Expired or untrusted cert |
| 43 | unsupported_certificate | Certificate type not supported | Wrong key type |
| 44 | certificate_revoked | Certificate on CRL | (Redis doesn't check CRL) |
| 45 | certificate_expired | Certificate expired | Check notAfter date |
| 46 | certificate_unknown | Generic cert error | CA not trusted |
| 48 | unknown_ca | CA not in trust store | Missing CA cert |
| 70 | protocol_version | TLS version mismatch | Client uses TLS 1.1, server requires 1.2+ |
| 71 | insufficient_security | Weak cipher/key | Client proposes weak cipher |

### 7.2 Redis Error Messages

| Error Message | Cause | Fix |
|---------------|-------|-----|
| `ERR TLS is not configured` | TLS connection to non-TLS port | Use `tls-port` instead |
| `ERR Server certificate not configured` | `tls-cert-file` missing | Set `tls-cert-file` and `tls-key-file` |
| `ERR Failed to load certificate` | Invalid PEM file | Check file format and permissions |
| `ERR Client authentication required` | Missing client cert when `tls-auth-clients yes` | Provide client cert |
| `ERR Invalid client certificate` | Untrusted client cert | Check CA certificate |

### 7.3 OpenSSL Debugging

Enable verbose TLS logging (OpenSSL-based implementations):
```bash
redis-server --loglevel verbose --tls-port 6380 ...
```

Use `openssl s_client` to test handshake:
```bash
openssl s_client -connect localhost:6380 \
  -CAfile /etc/redis/tls/ca.crt \
  -cert /etc/redis/tls/client.crt \
  -key /etc/redis/tls/client.key \
  -state -debug
```

---

## 8. Performance Considerations

### 8.1 Handshake Latency

| Scenario | Latency (typical) | Mitigation |
|----------|-------------------|------------|
| Full TLS 1.2 handshake | 2-3 RTT (4-6ms on LAN) | Enable session caching |
| TLS 1.3 handshake | 1 RTT (2-3ms on LAN) | Use TLS 1.3 when possible |
| Session resumption (TLS 1.2) | 1 RTT | `tls-session-caching yes` |
| 0-RTT resumption (TLS 1.3) | 0 RTT (no extra latency) | Requires session tickets |

### 8.2 Throughput Impact

- **Encryption overhead**: 5-15% CPU increase vs plain TCP
- **AES-NI acceleration**: <5% overhead with hardware support
- **ChaCha20-Poly1305**: 10-15% overhead on CPUs without AES-NI

**Recommendation**: Use TLS 1.3 with AES-GCM on modern CPUs for minimal overhead.

### 8.3 Memory Usage

| Component | Memory per Connection |
|-----------|----------------------|
| TLS context (shared) | ~50 KB |
| Session state | ~1 KB |
| Handshake buffers | ~16 KB (temporary) |
| Session cache entry | ~512 bytes |

**Session cache sizing**: `tls-session-cache-size * 512 bytes`
- Default 20480 sessions = ~10 MB cache

---

## 9. Security Best Practices

### 9.1 Certificate Management

- **Rotate certificates** every 90 days (Let's Encrypt default)
- **Use strong keys**: RSA 4096 or ECDSA P-384
- **Separate keys** for server and client certificates
- **Restrict permissions**: `chmod 600` for private keys
- **Monitor expiration**: Alert 30 days before expiry

### 9.2 Cipher Configuration

**Modern cipher list** (TLS 1.2+):
```
ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256
```

**Prohibited ciphers**:
- Anything with `RC4`, `DES`, `3DES`, `MD5`, `EXPORT`, `NULL`, `anon`

### 9.3 Mutual TLS Recommendations

- **Always use** `tls-auth-clients yes` for production
- **Pin certificates** via `tls-allowlisted-certs` for high-security environments
- **Rotate client certs** annually
- **Revoke compromised certs** immediately (requires CRL/OCSP in future)

### 9.4 Network Security

- **Firewall TLS port** to known IP ranges
- **Use separate networks** for cluster bus vs client connections
- **Enable TLS for all connections** in zero-trust environments
- **Monitor TLS failures** for potential attacks

---

## 10. Compliance & Standards

### 10.1 Regulatory Requirements

| Standard | Requirement | Redis TLS Support |
|----------|-------------|-------------------|
| PCI DSS 4.0 | TLS 1.2+ required | ✅ Supported |
| HIPAA | Encryption in transit | ✅ Supported (with TLS 1.2+) |
| GDPR | Data protection in transit | ✅ Supported |
| FedRAMP | TLS 1.2+ with approved ciphers | ✅ Configurable |
| SOC 2 | Encryption controls | ✅ Supported |

### 10.2 Industry Standards

- **NIST SP 800-52 Rev. 2**: TLS 1.2 minimum, TLS 1.3 recommended
- **OWASP**: Mutual TLS for service-to-service communication
- **Mozilla SSL Configuration Generator**: Intermediate profile = TLS 1.2+

---

## 11. Testing Checklist

### 11.1 Functional Tests

- [ ] TLS connection with server cert only
- [ ] Mutual TLS with client cert
- [ ] `tls-auth-clients optional` mode
- [ ] Session resumption (TLS 1.2)
- [ ] 0-RTT resumption (TLS 1.3)
- [ ] Dual-stack server (plain TCP + TLS)
- [ ] Cluster bus TLS (`tls-cluster yes`)
- [ ] Replication TLS (`tls-replication yes`)
- [ ] CONFIG GET/SET for all TLS parameters
- [ ] Certificate expiration handling

### 11.2 Negative Tests

- [ ] Expired server certificate → handshake failure
- [ ] Untrusted CA → handshake failure
- [ ] Missing client cert (when required) → error
- [ ] Invalid client cert → error
- [ ] Cipher mismatch → handshake failure
- [ ] TLS version mismatch (client TLS 1.1, server requires 1.2) → error
- [ ] Plain TCP to TLS port → error

### 11.3 Performance Tests

- [ ] Handshake latency vs plain TCP
- [ ] Throughput with AES-GCM vs ChaCha20
- [ ] Session cache hit rate
- [ ] Memory usage per TLS connection
- [ ] CPU overhead under load

### 11.4 Security Tests

- [ ] Man-in-the-middle attack (expect failure)
- [ ] Certificate pinning bypass (expect failure)
- [ ] Downgrade attack TLS 1.3 → 1.2 (expect detection)
- [ ] Weak cipher negotiation (expect rejection)

---

## 12. Implementation Notes for Zoltraak

### 12.1 Zig TLS Backend Selection

**Option A: ianic/tls.zig** (Recommended for MVP)
- Pure Zig implementation
- TLS 1.3 server support ✅
- TLS 1.2 client support ✅
- Zero C dependencies
- Lighter weight than OpenSSL

**Option B: OpenSSL via C interop** (For production-grade)
- Battle-tested, widely used
- Full cipher suite support
- CRL/OCSP support (future)
- Requires system OpenSSL or static linking

**Recommendation**: Start with Option A for Iteration 250-252, evaluate Option B for Iteration 253+ if needed.

### 12.2 Architecture Notes

```
Server.zig
├── tcp_listener (port 6379)
├── tls_listener (tls-port 6380)
└── tls_config (TlsConfig struct)

TlsConfig.zig (storage/tls_config.zig)
├── All 20 TLS parameters
├── validateConfig() — check cert files exist
└── isEnabled() — tls-port > 0

handleConnection() — unified for TCP + TLS
├── Detect connection type (TCP vs TLS)
├── Perform TLS handshake if TLS
├── Wrap stream in TLS context
└── Execute RESP commands
```

### 12.3 Stub Implementation (Iteration 250)

Phase 10 foundation (Iteration 250) will:
1. Parse all TLS CONFIG parameters ✅
2. Accept `--tls-port` flag ✅
3. Bind TLS listener (but not accept connections) ✅
4. Return error message on TLS connection attempts ✅
5. Provide extension points for future TLS handshake ✅

Full TLS handshake deferred to Iteration 251-252.

---

## References

1. [Redis TLS Overview](https://redis.io/docs/latest/operate/oss_and_stack/management/security/encryption/)
2. [Redis TLS Configuration](https://redis.io/docs/latest/operate/rs/security/encryption/tls/enable-tls/)
3. [Redis Cipher Suites](https://redis.io/docs/latest/operate/rs/security/encryption/tls/ciphers/)
4. [RFC 5246 — TLS 1.2](https://datatracker.ietf.org/doc/html/rfc5246)
5. [RFC 8446 — TLS 1.3](https://datatracker.ietf.org/doc/html/rfc8446)
6. [NIST SP 800-52 Rev. 2](https://csrc.nist.gov/publications/detail/sp/800-52/rev-2/final)
7. [Mozilla SSL Configuration Generator](https://ssl-config.mozilla.org/)
8. [Zig std.crypto.tls](https://github.com/ziglang/zig/blob/master/lib/std/crypto/tls.zig)
9. [ianic/tls.zig](https://github.com/ianic/tls.zig)
10. [openssl-zig](https://github.com/glingy/openssl-zig)

---

**Document Version**: 1.0
**Last Updated**: 2026-05-03
**Maintainer**: Zoltraak Project
