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
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}FAIL${NC}"
        echo "  Expected: $expected"
        echo "  Got:      $result"
        FAILED=$((FAILED + 1))
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
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}FAIL${NC}"
        echo "  Expected to contain: $expected_substring"
        echo "  Got:                 $result"
        FAILED=$((FAILED + 1))
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
    PASSED=$((PASSED + 1))
else
    echo -e "Testing: GET large value... ${RED}FAIL${NC}"
    FAILED=$((FAILED + 1))
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
    PASSED=$((PASSED + 1))
else
    echo -e "${RED}FAIL${NC}"
    FAILED=$((FAILED + 1))
fi
echo ""

# ============================================================================
# LIST Command Tests
# ============================================================================

echo "=== LPUSH Command Tests ==="
test_command "LPUSH single element" "$REDIS_CLI LPUSH testlist 'hello'" "(integer) 1"
test_command "LPUSH multiple elements" "$REDIS_CLI LPUSH testlist2 a b c" "(integer) 3"
$REDIS_CLI LPUSH testlist3 first > /dev/null
test_command "LPUSH append to existing" "$REDIS_CLI LPUSH testlist3 second third" "(integer) 3"
$REDIS_CLI SET stringkey value > /dev/null
test_command_contains "LPUSH on string key" "$REDIS_CLI LPUSH stringkey elem" "WRONGTYPE"
test_command_contains "LPUSH wrong arguments" "$REDIS_CLI LPUSH" "ERR"
echo ""

echo "=== RPUSH Command Tests ==="
test_command "RPUSH single element" "$REDIS_CLI RPUSH rlist 'hello'" "(integer) 1"
test_command "RPUSH multiple elements" "$REDIS_CLI RPUSH rlist2 a b c" "(integer) 3"
$REDIS_CLI RPUSH rlist3 first > /dev/null
test_command "RPUSH append to existing" "$REDIS_CLI RPUSH rlist3 second third" "(integer) 3"
$REDIS_CLI SET stringkey2 value > /dev/null
test_command_contains "RPUSH on string key" "$REDIS_CLI RPUSH stringkey2 elem" "WRONGTYPE"
echo ""

echo "=== LPOP Command Tests ==="
$REDIS_CLI RPUSH poplist a b c d e > /dev/null
test_command "LPOP without count" "$REDIS_CLI LPOP poplist" "a"
test_command "LPOP with count parameter" "$REDIS_CLI LPOP poplist 2 | head -n1" "(empty array)"
test_command "LPOP on non-existent key" "$REDIS_CLI LPOP nosuchlist" "(nil)"
test_command "LPOP with count on non-existent" "$REDIS_CLI LPOP nosuchlist 5" "(empty array)"
$REDIS_CLI RPUSH smalllist x y > /dev/null
test_command "LPOP count > length" "$REDIS_CLI LPOP smalllist 10 | head -n1" "(empty array)"
echo ""

echo "=== RPOP Command Tests ==="
$REDIS_CLI RPUSH rpoplist a b c d e > /dev/null
test_command "RPOP without count" "$REDIS_CLI RPOP rpoplist" "e"
test_command "RPOP with count parameter" "$REDIS_CLI RPOP rpoplist 2 | head -n1" "(empty array)"
test_command "RPOP on non-existent key" "$REDIS_CLI RPOP nosuchlist" "(nil)"
echo ""

echo "=== LRANGE Command Tests ==="
$REDIS_CLI RPUSH rangelist a b c d e > /dev/null
test_command "LRANGE full list (0 -1)" "$REDIS_CLI LRANGE rangelist 0 -1 | wc -l | tr -d ' '" "5"
test_command "LRANGE positive indices" "$REDIS_CLI LRANGE rangelist 1 3 | wc -l | tr -d ' '" "3"
test_command "LRANGE negative indices" "$REDIS_CLI LRANGE rangelist -3 -1 | wc -l | tr -d ' '" "3"
test_command "LRANGE out of bounds" "$REDIS_CLI LRANGE rangelist -100 100 | wc -l | tr -d ' '" "5"
test_command "LRANGE invalid range" "$REDIS_CLI LRANGE rangelist 5 1" "(empty array)"
test_command "LRANGE non-existent key" "$REDIS_CLI LRANGE nosuchlist 0 -1" "(empty array)"
test_command "LRANGE single element" "$REDIS_CLI LRANGE rangelist 1 1" "b"
echo ""

echo "=== LLEN Command Tests ==="
$REDIS_CLI RPUSH lenlist a b c d > /dev/null
test_command "LLEN returns length" "$REDIS_CLI LLEN lenlist" "(integer) 4"
test_command "LLEN non-existent key" "$REDIS_CLI LLEN nosuchlist" "(integer) 0"
$REDIS_CLI SET lenstring value > /dev/null
test_command_contains "LLEN on string key" "$REDIS_CLI LLEN lenstring" "WRONGTYPE"
test_command_contains "LLEN wrong arguments" "$REDIS_CLI LLEN" "ERR"
echo ""

echo "=== List Workflow Tests ==="

# Stack behavior (LPUSH + LPOP)
$REDIS_CLI LPUSH stack first second third > /dev/null
test_command "Stack: LPOP first element" "$REDIS_CLI LPOP stack" "third"
test_command "Stack: LPOP second element" "$REDIS_CLI LPOP stack" "second"
test_command "Stack: LPOP third element" "$REDIS_CLI LPOP stack" "first"
test_command "Stack: LPOP empty list" "$REDIS_CLI LPOP stack" "(nil)"
test_command "Stack: EXISTS after empty" "$REDIS_CLI EXISTS stack" "(integer) 0"

# Queue behavior (RPUSH + LPOP)
$REDIS_CLI RPUSH queue job1 job2 job3 > /dev/null
test_command "Queue: LPOP first job" "$REDIS_CLI LPOP queue" "job1"
test_command "Queue: LPOP second job" "$REDIS_CLI LPOP queue" "job2"
test_command "Queue: LPOP third job" "$REDIS_CLI LPOP queue" "job3"

# Combined operations
$REDIS_CLI RPUSH combined a b > /dev/null
$REDIS_CLI LPUSH combined x y > /dev/null
test_command "Combined: LLEN after RPUSH+LPUSH" "$REDIS_CLI LLEN combined" "(integer) 4"
test_command "Combined: First element is y" "$REDIS_CLI LRANGE combined 0 0" "y"
test_command "Combined: Last element is b" "$REDIS_CLI LRANGE combined -1 -1" "b"

# Auto-deletion test
$REDIS_CLI RPUSH templist single > /dev/null
test_command "Auto-delete: EXISTS before pop" "$REDIS_CLI EXISTS templist" "(integer) 1"
$REDIS_CLI LPOP templist > /dev/null
test_command "Auto-delete: EXISTS after pop" "$REDIS_CLI EXISTS templist" "(integer) 0"

# Complex workflow
$REDIS_CLI RPUSH workflow 1 2 3 > /dev/null
$REDIS_CLI LPUSH workflow 0 > /dev/null
$REDIS_CLI RPUSH workflow 4 5 > /dev/null
test_command "Workflow: LLEN after mixed ops" "$REDIS_CLI LLEN workflow" "(integer) 6"
test_command "Workflow: LRANGE middle" "$REDIS_CLI LRANGE workflow 1 4 | wc -l | tr -d ' '" "4"
$REDIS_CLI LPOP workflow 2 > /dev/null
$REDIS_CLI RPOP workflow > /dev/null
test_command "Workflow: LLEN after pops" "$REDIS_CLI LLEN workflow" "(integer) 3"

echo ""

# ============================================================================
# SET Command Tests
# ============================================================================

echo "=== SADD Command Tests ==="
test_command "SADD single member" "$REDIS_CLI SADD myset hello" "(integer) 1"
test_command "SADD multiple members" "$REDIS_CLI SADD myset2 one two three" "(integer) 3"
test_command "SADD duplicate returns 0" "$REDIS_CLI SADD myset hello" "(integer) 0"
$REDIS_CLI SADD myset3 a b > /dev/null
test_command "SADD mixed new and existing" "$REDIS_CLI SADD myset3 b c d" "(integer) 2"
$REDIS_CLI SET stringkey value > /dev/null
test_command_contains "SADD on string key" "$REDIS_CLI SADD stringkey member" "WRONGTYPE"
$REDIS_CLI LPUSH listkey elem > /dev/null
test_command_contains "SADD on list key" "$REDIS_CLI SADD listkey member" "WRONGTYPE"
test_command_contains "SADD wrong arguments" "$REDIS_CLI SADD onlykey" "ERR"
echo ""

echo "=== SREM Command Tests ==="
$REDIS_CLI SADD remset one two three > /dev/null
test_command "SREM single member" "$REDIS_CLI SREM remset one" "(integer) 1"
$REDIS_CLI SADD remset2 a b c d > /dev/null
test_command "SREM multiple members" "$REDIS_CLI SREM remset2 a c" "(integer) 2"
$REDIS_CLI SADD remset3 x y > /dev/null
test_command "SREM non-existent member" "$REDIS_CLI SREM remset3 z" "(integer) 0"
test_command "SREM on non-existent key" "$REDIS_CLI SREM nosuchset member" "(integer) 0"
$REDIS_CLI SADD remset4 single > /dev/null
$REDIS_CLI SREM remset4 single > /dev/null
test_command "SREM auto-deletion" "$REDIS_CLI EXISTS remset4" "(integer) 0"
$REDIS_CLI SET stringkey2 value > /dev/null
test_command_contains "SREM on string key" "$REDIS_CLI SREM stringkey2 member" "WRONGTYPE"
test_command_contains "SREM wrong arguments" "$REDIS_CLI SREM onlykey" "ERR"
echo ""

echo "=== SISMEMBER Command Tests ==="
$REDIS_CLI SADD checkset hello world > /dev/null
test_command "SISMEMBER existing member" "$REDIS_CLI SISMEMBER checkset hello" "(integer) 1"
test_command "SISMEMBER non-existent member" "$REDIS_CLI SISMEMBER checkset foo" "(integer) 0"
test_command "SISMEMBER non-existent key" "$REDIS_CLI SISMEMBER nosuchset member" "(integer) 0"
$REDIS_CLI SET stringkey3 value > /dev/null
test_command_contains "SISMEMBER on string key" "$REDIS_CLI SISMEMBER stringkey3 member" "WRONGTYPE"
test_command_contains "SISMEMBER wrong arguments" "$REDIS_CLI SISMEMBER onlykey" "ERR"
echo ""

echo "=== SMEMBERS Command Tests ==="
$REDIS_CLI SADD membersset one two three > /dev/null
# Count the number of lines returned (should be 3 members)
test_command "SMEMBERS returns all members" "$REDIS_CLI SMEMBERS membersset | grep -E '^(one|two|three)$' | wc -l | tr -d ' '" "3"
test_command "SMEMBERS non-existent key" "$REDIS_CLI SMEMBERS nosuchset" "(empty array)"
$REDIS_CLI SET stringkey4 value > /dev/null
test_command_contains "SMEMBERS on string key" "$REDIS_CLI SMEMBERS stringkey4" "WRONGTYPE"
test_command_contains "SMEMBERS wrong arguments" "$REDIS_CLI SMEMBERS" "ERR"
echo ""

echo "=== SCARD Command Tests ==="
$REDIS_CLI SADD cardset a b c d > /dev/null
test_command "SCARD returns cardinality" "$REDIS_CLI SCARD cardset" "(integer) 4"
test_command "SCARD non-existent key" "$REDIS_CLI SCARD nosuchset" "(integer) 0"
$REDIS_CLI SET stringkey5 value > /dev/null
test_command_contains "SCARD on string key" "$REDIS_CLI SCARD stringkey5" "WRONGTYPE"
test_command_contains "SCARD wrong arguments" "$REDIS_CLI SCARD" "ERR"
echo ""

echo "=== Set Workflow Tests ==="

# Tag system
$REDIS_CLI SADD article:1:tags redis database nosql > /dev/null
test_command "Tag system: Check tag membership" "$REDIS_CLI SISMEMBER article:1:tags redis" "(integer) 1"
test_command "Tag system: Count tags" "$REDIS_CLI SCARD article:1:tags" "(integer) 3"
test_command "Tag system: Non-existent tag" "$REDIS_CLI SISMEMBER article:1:tags sql" "(integer) 0"
$REDIS_CLI SREM article:1:tags nosql > /dev/null
test_command "Tag system: After remove" "$REDIS_CLI SCARD article:1:tags" "(integer) 2"

# Unique visitors
$REDIS_CLI SADD page:visitors user:123 > /dev/null
$REDIS_CLI SADD page:visitors user:456 > /dev/null
$REDIS_CLI SADD page:visitors user:123 > /dev/null
test_command "Unique visitors: Count" "$REDIS_CLI SCARD page:visitors" "(integer) 2"
test_command "Unique visitors: Check visitor" "$REDIS_CLI SISMEMBER page:visitors user:456" "(integer) 1"

# Duplicate handling
test_command "Duplicate handling in SADD" "$REDIS_CLI SADD dupset a b a c b d" "(integer) 4"
test_command "Duplicate handling: Verify cardinality" "$REDIS_CLI SCARD dupset" "(integer) 4"

# Auto-deletion after removing all members
$REDIS_CLI SADD tempset one two three > /dev/null
test_command "Before remove: Key exists" "$REDIS_CLI EXISTS tempset" "(integer) 1"
$REDIS_CLI SREM tempset one two three > /dev/null
test_command "After remove: Key deleted" "$REDIS_CLI EXISTS tempset" "(integer) 0"
test_command "After remove: SCARD returns 0" "$REDIS_CLI SCARD tempset" "(integer) 0"
test_command "After remove: SMEMBERS returns empty" "$REDIS_CLI SMEMBERS tempset" "(empty array)"

# Multiple sets
$REDIS_CLI SADD set1 a b c > /dev/null
$REDIS_CLI SADD set2 c d e > /dev/null
test_command "Multiple sets: set1 cardinality" "$REDIS_CLI SCARD set1" "(integer) 3"
test_command "Multiple sets: set2 cardinality" "$REDIS_CLI SCARD set2" "(integer) 3"
test_command "Multiple sets: Common element in set1" "$REDIS_CLI SISMEMBER set1 c" "(integer) 1"
test_command "Multiple sets: Common element in set2" "$REDIS_CLI SISMEMBER set2 c" "(integer) 1"
$REDIS_CLI SREM set1 c > /dev/null
test_command "Multiple sets: After remove from set1" "$REDIS_CLI SISMEMBER set1 c" "(integer) 0"
test_command "Multiple sets: Still in set2" "$REDIS_CLI SISMEMBER set2 c" "(integer) 1"

echo ""

# Clean up test keys
echo "Cleaning up test keys..."
$REDIS_CLI DEL mykey newkey emptykey existkey key1 key2 key3 dupkey casekey largekey > /dev/null 2>&1 || true
$REDIS_CLI DEL user:1:name user:1:email user:2:name session:abc123 > /dev/null 2>&1 || true
$REDIS_CLI DEL testlist testlist2 testlist3 stringkey rlist rlist2 rlist3 stringkey2 > /dev/null 2>&1 || true
$REDIS_CLI DEL poplist smalllist rpoplist rangelist lenlist lenstring > /dev/null 2>&1 || true
$REDIS_CLI DEL stack queue combined templist workflow > /dev/null 2>&1 || true
$REDIS_CLI DEL myset myset2 myset3 remset remset2 remset3 remset4 checkset membersset cardset > /dev/null 2>&1 || true
$REDIS_CLI DEL stringkey stringkey2 stringkey3 stringkey4 stringkey5 listkey > /dev/null 2>&1 || true
$REDIS_CLI DEL article:1:tags page:visitors dupset tempset set1 set2 > /dev/null 2>&1 || true
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
