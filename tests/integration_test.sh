#!/bin/bash

# Zoltraak Integration Test Script
# Tests the server using redis-cli to verify Redis protocol compatibility

set -e  # Exit on error

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

REDIS_CLI="redis-cli -p 6379"
PASSED=0
FAILED=0

# Function to run a test
test_command() {
    local description="$1"
    local command="$2"
    local expected="$3"

    echo -n "Testing: $description... "

    result=$(eval "$command" 2>&1 || true)

    if [[ "$result" == "$expected" ]]; then
        echo -e "${GREEN}PASS${NC}"
        ((PASSED++))
    else
        echo -e "${RED}FAIL${NC}"
        echo "  Expected: $expected"
        echo "  Got:      $result"
        ((FAILED++))
    fi
}

# Function to test if output contains substring
test_command_contains() {
    local description="$1"
    local command="$2"
    local expected_substring="$3"

    echo -n "Testing: $description... "

    result=$(eval "$command" 2>&1 || true)

    if [[ "$result" == *"$expected_substring"* ]]; then
        echo -e "${GREEN}PASS${NC}"
        ((PASSED++))
    else
        echo -e "${RED}FAIL${NC}"
        echo "  Expected to contain: $expected_substring"
        echo "  Got:                 $result"
        ((FAILED++))
    fi
}

echo "=========================================="
echo "Zoltraak Integration Tests (redis-cli)"
echo "=========================================="
echo ""

# Check if server is running
if ! nc -z 127.0.0.1 6379 2>/dev/null; then
    echo -e "${RED}Error: Zoltraak server is not running on port 6379${NC}"
    echo "Please start the server first: ./zig-out/bin/zoltraak"
    exit 1
fi

echo -e "${GREEN}Server is running${NC}"
echo ""

# Clean up any existing test keys
$REDIS_CLI DEL testkey key1 key2 key3 mykey expkey session:123 user:1 > /dev/null 2>&1 || true

echo "=== PING Command Tests ==="
test_command "PING without argument" "$REDIS_CLI PING" "PONG"
test_command "PING with message" "$REDIS_CLI PING 'hello world'" "hello world"
echo ""

echo "=== SET Command Tests ==="
test_command "SET basic key-value" "$REDIS_CLI SET mykey myvalue" "OK"
test_command "SET overwrites existing" "$REDIS_CLI SET mykey newvalue" "OK"
test_command "SET with EX option" "$REDIS_CLI SET expkey value EX 60" "OK"
test_command "SET with PX option" "$REDIS_CLI SET expkey value PX 5000" "OK"
test_command "SET with NX (new key)" "$REDIS_CLI SET newkey value NX" "OK"
test_command "SET with NX (existing key)" "$REDIS_CLI SET newkey value2 NX" "(nil)"
test_command "SET with XX (existing key)" "$REDIS_CLI SET newkey updated XX" "OK"
test_command "SET with XX (non-existent)" "$REDIS_CLI SET nonexistent value XX" "(nil)"
test_command_contains "SET with NX and XX (error)" "$REDIS_CLI SET key value NX XX" "ERR"
test_command_contains "SET negative expiration" "$REDIS_CLI SET key value EX -1" "ERR"
test_command "SET empty value" "$REDIS_CLI SET emptykey ''" "OK"
echo ""

echo "=== GET Command Tests ==="
test_command "GET existing key" "$REDIS_CLI GET mykey" "newvalue"
test_command "GET non-existent key" "$REDIS_CLI GET nosuchkey" "(nil)"
test_command "GET empty value" "$REDIS_CLI GET emptykey" ""
test_command_contains "GET wrong arguments" "$REDIS_CLI GET" "ERR"
echo ""

echo "=== DEL Command Tests ==="
$REDIS_CLI SET key1 value1 > /dev/null
$REDIS_CLI SET key2 value2 > /dev/null
$REDIS_CLI SET key3 value3 > /dev/null
test_command "DEL single key" "$REDIS_CLI DEL key1" "(integer) 1"
test_command "DEL non-existent key" "$REDIS_CLI DEL nosuchkey" "(integer) 0"
test_command "DEL multiple keys" "$REDIS_CLI DEL key2 key3 nosuchkey" "(integer) 2"
test_command "DEL duplicate keys" "$REDIS_CLI SET dupkey value && $REDIS_CLI DEL dupkey dupkey" "(integer) 1"
test_command_contains "DEL no arguments" "$REDIS_CLI DEL" "ERR"
echo ""

echo "=== EXISTS Command Tests ==="
$REDIS_CLI SET existkey value > /dev/null
test_command "EXISTS existing key" "$REDIS_CLI EXISTS existkey" "(integer) 1"
test_command "EXISTS non-existent key" "$REDIS_CLI EXISTS nosuchkey" "(integer) 0"
$REDIS_CLI SET key1 value1 > /dev/null
$REDIS_CLI SET key2 value2 > /dev/null
test_command "EXISTS multiple keys" "$REDIS_CLI EXISTS key1 key2 nosuchkey" "(integer) 2"
test_command "EXISTS duplicate keys" "$REDIS_CLI EXISTS existkey existkey existkey" "(integer) 3"
test_command_contains "EXISTS no arguments" "$REDIS_CLI EXISTS" "ERR"
echo ""

echo "=== TTL Expiration Tests ==="
test_command "SET with short expiration" "$REDIS_CLI SET ttlkey value PX 100" "OK"
test_command "GET before expiration" "$REDIS_CLI GET ttlkey" "value"
echo -n "Waiting for expiration (200ms)... "
sleep 0.2
echo "done"
test_command "GET after expiration" "$REDIS_CLI GET ttlkey" "(nil)"
test_command "EXISTS after expiration" "$REDIS_CLI EXISTS ttlkey" "(integer) 0"
echo ""

echo "=== Case Insensitivity Tests ==="
test_command "lowercase ping" "$REDIS_CLI ping" "PONG"
test_command "lowercase set" "$REDIS_CLI set casekey value" "OK"
test_command "MiXeD CaSe get" "$REDIS_CLI GeT casekey" "value"
echo ""

echo "=== Error Handling Tests ==="
test_command_contains "Unknown command" "$REDIS_CLI UNKNOWNCMD" "ERR"
test_command_contains "SET wrong arguments" "$REDIS_CLI SET onlykey" "ERR"
test_command_contains "GET too many arguments" "$REDIS_CLI GET key1 key2" "ERR"
echo ""

echo "=== Complex Workflow Test ==="
$REDIS_CLI SET user:1:name Alice > /dev/null
$REDIS_CLI SET user:1:email alice@example.com > /dev/null
$REDIS_CLI SET user:2:name Bob > /dev/null
$REDIS_CLI SET session:abc123 active EX 3600 > /dev/null
test_command "Workflow: GET user name" "$REDIS_CLI GET user:1:name" "Alice"
test_command "Workflow: EXISTS multiple users" "$REDIS_CLI EXISTS user:1:name user:2:name" "(integer) 2"
test_command "Workflow: DEL user" "$REDIS_CLI DEL user:1:name user:1:email" "(integer) 2"
test_command "Workflow: EXISTS after delete" "$REDIS_CLI EXISTS user:1:name user:2:name" "(integer) 1"
echo ""

echo "=== Large Value Test ==="
# Create a 1KB value
large_value=$(head -c 1024 /dev/urandom | base64)
test_command "SET large value" "$REDIS_CLI SET largekey '$large_value'" "OK"
result=$($REDIS_CLI GET largekey)
if [[ "$result" == "$large_value" ]]; then
    echo -e "Testing: GET large value... ${GREEN}PASS${NC}"
    ((PASSED++))
else
    echo -e "Testing: GET large value... ${RED}FAIL${NC}"
    ((FAILED++))
fi
echo ""

echo "=== Concurrent Operations Test ==="
# Run multiple SET/GET operations in parallel
for i in {1..10}; do
    ($REDIS_CLI SET concurrent:$i value$i > /dev/null && $REDIS_CLI GET concurrent:$i > /dev/null) &
done
wait
echo -n "Testing: Concurrent operations... "
all_exist=true
for i in {1..10}; do
    result=$($REDIS_CLI GET concurrent:$i)
    if [[ "$result" != "value$i" ]]; then
        all_exist=false
        break
    fi
done
if [ "$all_exist" = true ]; then
    echo -e "${GREEN}PASS${NC}"
    ((PASSED++))
else
    echo -e "${RED}FAIL${NC}"
    ((FAILED++))
fi
echo ""

# Clean up test keys
echo "Cleaning up test keys..."
$REDIS_CLI DEL mykey newkey emptykey existkey key1 key2 key3 dupkey casekey largekey > /dev/null 2>&1 || true
$REDIS_CLI DEL user:1:name user:1:email user:2:name session:abc123 > /dev/null 2>&1 || true
for i in {1..10}; do
    $REDIS_CLI DEL concurrent:$i > /dev/null 2>&1 || true
done
echo ""

echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo -e "Passed: ${GREEN}$PASSED${NC}"
echo -e "Failed: ${RED}$FAILED${NC}"
echo "Total:  $((PASSED + FAILED))"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed.${NC}"
    exit 1
fi
