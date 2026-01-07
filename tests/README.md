# Zoltraak Integration Tests

This directory contains comprehensive integration tests for the Zoltraak Redis-compatible server.

## Test Structure

### 1. Integration Tests (Zig-based)

**File:** `test_integration.zig`

Comprehensive Zig-based integration tests that:
- Spawn the Zoltraak server process
- Connect via TCP using RESP2 protocol
- Test all commands end-to-end
- Verify correct behavior and error handling

**Test Coverage:**
- **PING Command Tests** (4 tests)
  - Basic PING without arguments
  - PING with message echo
  - Case insensitivity
  - Error handling for too many arguments

- **SET Command Tests** (13 tests)
  - Basic key-value storage
  - Overwriting existing values
  - EX option (expiration in seconds)
  - PX option (expiration in milliseconds)
  - NX option (only set if not exists)
  - XX option (only set if exists)
  - Multiple option combinations
  - Error conditions (negative expiration, wrong arguments, etc.)
  - Empty keys and values

- **GET Command Tests** (5 tests)
  - Retrieving existing keys
  - Non-existent keys return null
  - Expired keys return null
  - Empty value retrieval
  - Wrong number of arguments

- **DEL Command Tests** (5 tests)
  - Deleting single keys
  - Deleting multiple keys
  - Non-existent keys
  - Duplicate key handling
  - Wrong number of arguments

- **EXISTS Command Tests** (6 tests)
  - Single existing key
  - Non-existent keys
  - Multiple keys
  - Duplicate key counting
  - Expired keys
  - Wrong number of arguments

- **Error Handling Tests** (2 tests)
  - Unknown command errors
  - Case insensitive command dispatch

- **Complex Integration Tests** (3 tests)
  - Full workflow with multiple commands
  - Multiple SET options combined
  - Large value storage and retrieval

**Total: 38+ integration tests**

### 2. Shell Script Tests (redis-cli)

**File:** `integration_test.sh`

Shell-based integration tests using `redis-cli` for manual verification and CI/CD pipelines.

**Test Categories:**
- PING command tests
- SET command tests (all options)
- GET command tests
- DEL command tests
- EXISTS command tests
- TTL expiration tests
- Case insensitivity tests
- Error handling tests
- Complex workflow tests
- Large value tests
- Concurrent operation tests

## Running Tests

### Prerequisites

1. **Build the server:**
   ```bash
   zig build
   ```

2. **For shell script tests, install redis-cli:**
   ```bash
   # macOS
   brew install redis

   # Ubuntu/Debian
   sudo apt-get install redis-tools

   # Arch Linux
   sudo pacman -S redis
   ```

### Running Zig Integration Tests

**Run all tests (unit + integration):**
```bash
zig build test
```

**Run only integration tests:**
```bash
zig build test-integration
```

**Note:** Integration tests will automatically start and stop the server process.

### Running Shell Script Tests

1. **Start the server manually:**
   ```bash
   ./zig-out/bin/zoltraak
   ```

2. **In another terminal, run the test script:**
   ```bash
   ./tests/integration_test.sh
   ```

The script will:
- Check if the server is running
- Execute all test cases
- Display colored output (green = pass, red = fail)
- Show a summary of results
- Clean up test keys after completion

### Expected Output

**Successful test run:**
```
==========================================
Zoltraak Integration Tests (redis-cli)
==========================================

Server is running

=== PING Command Tests ===
Testing: PING without argument... PASS
Testing: PING with message... PASS

=== SET Command Tests ===
Testing: SET basic key-value... PASS
...

==========================================
Test Summary
==========================================
Passed: 50
Failed: 0
Total:  50

All tests passed!
```

## Test Implementation Details

### RESP2 Protocol Testing

The integration tests verify full RESP2 protocol compliance:

- **Simple Strings:** `+OK\r\n`
- **Errors:** `-ERR message\r\n`
- **Integers:** `:42\r\n`
- **Bulk Strings:** `$5\r\nhello\r\n`
- **Null Bulk Strings:** `$-1\r\n`
- **Arrays:** `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`

### RespClient Helper

The `test_integration.zig` file includes a `RespClient` helper that:
- Establishes TCP connections
- Serializes commands to RESP format
- Sends commands over the socket
- Receives and parses responses
- Handles connection cleanup

Example usage:
```zig
var client = try RespClient.init(allocator, "127.0.0.1", 6379);
defer client.deinit();

const response = try client.sendCommand(&[_][]const u8{"PING"});
defer allocator.free(response);

try testing.expectEqualStrings("+PONG\r\n", response);
```

### TestServer Helper

Spawns the Zoltraak server as a subprocess for testing:
```zig
var server = try TestServer.start(allocator);
defer server.stop();

// Run tests...
```

## Adding New Tests

### Adding Zig Integration Tests

1. Open `tests/test_integration.zig`
2. Add a new test function:
   ```zig
   test "CommandName - test description" {
       var server = try TestServer.start(testing.allocator);
       defer server.stop();

       var client = try RespClient.init(testing.allocator, "127.0.0.1", 6379);
       defer client.deinit();

       const response = try client.sendCommand(&[_][]const u8{"COMMAND", "arg1", "arg2"});
       defer testing.allocator.free(response);

       try testing.expectEqualStrings("expected_response", response);
   }
   ```

### Adding Shell Script Tests

1. Open `tests/integration_test.sh`
2. Add test commands to the appropriate section:
   ```bash
   test_command "Test description" "$REDIS_CLI COMMAND arg1 arg2" "expected_output"
   ```

## Troubleshooting

### Server Doesn't Start

If integration tests fail with connection errors:
1. Check that the server builds successfully: `zig build`
2. Try running the server manually: `./zig-out/bin/zoltraak`
3. Verify no other process is using port 6379: `lsof -i :6379`

### Port Already in Use

If you get "address already in use" errors:
```bash
# Find and kill the process using port 6379
lsof -i :6379
kill -9 <PID>
```

### Shell Script Tests Fail

If shell script tests fail:
1. Ensure `redis-cli` is installed: `which redis-cli`
2. Verify the server is running: `nc -z 127.0.0.1 6379`
3. Check for conflicting Redis instances: `ps aux | grep redis`

## Continuous Integration

For CI/CD pipelines, use the shell script tests:

```yaml
# Example GitHub Actions workflow
- name: Build Zoltraak
  run: zig build

- name: Start Zoltraak Server
  run: ./zig-out/bin/zoltraak &

- name: Wait for Server
  run: sleep 1

- name: Run Integration Tests
  run: ./tests/integration_test.sh
```

## Test Specification Compliance

These tests verify compliance with:
- **ITERATION_1_SPEC.md** - Complete iteration 1 specifications
- **Redis RESP2 Protocol** - Full protocol compliance
- **Redis Command Behavior** - Exact command semantics

All tests follow the acceptance criteria defined in the specification document.

## Performance Testing

While not part of the standard test suite, you can use `redis-benchmark` for performance testing:

```bash
# Start server
./zig-out/bin/zoltraak

# Run benchmark (in another terminal)
redis-benchmark -p 6379 -t set,get -n 100000 -q
```

Expected output:
```
SET: XXXXX requests per second
GET: XXXXX requests per second
```

## Memory Testing

To verify no memory leaks, Zig's test runner automatically checks for leaks:

```bash
zig build test
```

Any memory leaks will cause tests to fail with detailed diagnostics.
