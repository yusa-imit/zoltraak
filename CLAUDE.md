# CLAUDE.md

Zoltraak — Redis-compatible in-memory data store written in Zig.

## Project Status

**Current: v0.1.0 — Iterations 1-97 complete (187+ Redis commands)**
**Target: v1.0 — 100% Redis compatibility (500+ commands)**
**Roadmap: [docs/PRD.md](docs/PRD.md)**

97 iterations complete (187+ Redis commands). See `docs/milestones.md` for detailed breakdown, `docs/PRD.md` for full roadmap.

### Known stubs (need real implementation for 1.0)

Lua scripting (EVAL returns nil), ACL (no enforcement), Cluster (single-node), SELECT (DB 0 only). All blocking commands have true polling-based semantics. SLOWLOG, MONITOR, LATENCY, MEMORY, DEBUG, SHUTDOWN, FAILOVER have real implementations. Phase 6 server management 90% complete (ROLE pending).

---

## Build & Run

```bash
zig build                          # Build
zig build -Doptimize=ReleaseFast   # Release build
zig build run                      # Build and run server
zig build test                     # All tests (unit + integration)
zig build test-integration         # Integration tests only
./zig-out/bin/zoltraak             # Start server (default 127.0.0.1:6379)
./zig-out/bin/zoltraak --host 0.0.0.0 --port 6380
./zig-out/bin/zoltraak-cli         # REPL client
redis-cli -p 6379                  # Connect with standard redis-cli
```

---

## Project Structure

```
zoltraak/
├── src/
│   ├── main.zig                 # Server entry point
│   ├── server.zig               # TCP server, command routing
│   ├── cli.zig                  # REPL client (zoltraak-cli)
│   ├── protocol/
│   │   ├── parser.zig           # RESP2/RESP3 parser
│   │   └── writer.zig           # RESP2/RESP3 response writer
│   ├── commands/                # Command handlers (26 files)
│   │   ├── strings.zig          # String commands
│   │   ├── lists.zig            # List commands
│   │   ├── sets.zig             # Set commands
│   │   ├── hashes.zig           # Hash commands
│   │   ├── sorted_sets.zig      # Sorted set commands
│   │   ├── streams.zig          # Stream commands
│   │   ├── streams_advanced.zig # Consumer groups, XCLAIM, XAUTOCLAIM
│   │   ├── geo.zig              # Geospatial commands
│   │   ├── hyperloglog.zig      # HyperLogLog commands
│   │   ├── bits.zig             # SETBIT, GETBIT, BITCOUNT, BITOP
│   │   ├── bitfield.zig         # BITFIELD, BITFIELD_RO
│   │   ├── keys.zig             # Key management (TTL, EXPIRE, SCAN, etc.)
│   │   ├── transactions.zig     # MULTI/EXEC/WATCH
│   │   ├── pubsub.zig           # Pub/Sub commands
│   │   ├── replication.zig      # REPLICAOF, REPLCONF, PSYNC
│   │   ├── client.zig           # CLIENT subcommands
│   │   ├── config.zig           # CONFIG subcommands
│   │   ├── command.zig          # COMMAND introspection
│   │   ├── info.zig             # INFO command
│   │   ├── scripting.zig        # EVAL/EVALSHA stubs
│   │   ├── acl.zig              # ACL stubs
│   │   ├── cluster.zig          # Cluster stubs
│   │   ├── utility.zig          # ECHO, QUIT, SELECT, TIME, etc.
│   │   ├── server_commands.zig  # SAVE, BGSAVE, FLUSHDB, etc.
│   │   └── introspection.zig    # OBJECT, DEBUG
│   └── storage/                 # Data layer
│       ├── memory.zig           # Core storage engine (tagged union Value)
│       ├── persistence.zig      # RDB persistence
│       ├── aof.zig              # AOF logging
│       ├── config.zig           # Runtime configuration
│       ├── replication.zig      # Replication state
│       ├── pubsub.zig           # Pub/Sub state
│       ├── acl.zig              # ACL storage (stub)
│       ├── scripting.zig        # Script cache
│       └── blocking.zig         # Blocking queue for XREAD/XREADGROUP BLOCK
├── tests/                       # Integration tests
│   ├── test_integration.zig     # Main integration suite
│   ├── test_key_management.zig
│   ├── test_hash_field_ttl.zig
│   ├── test_resp3.zig
│   ├── test_bitfield.zig
│   ├── test_client.zig
│   ├── test_utility.zig
│   └── integration_test.sh      # Shell-based tests (redis-cli)
├── docs/
│   ├── PRD.md                   # 1.0 product requirements (18 phases)
│   └── milestones.md            # Completed iterations, dependency tracking
├── .claude/
│   └── agents/                  # 9 specialized agents
├── build.zig                    # Build config (sailor dependency)
├── build.zig.zon                # Package manifest (Zig 0.15+, sailor v0.4.0)
└── README.md                    # User-facing documentation
```

> **Note**: 파일 구조는 참고안. 실제 구현에 따라 변경 가능하며, 소스 코드가 기준.

---

## Zig Guidelines

### Conventions
- `std.mem.Allocator` for all allocations
- `errdefer` for cleanup on error paths
- `comptime` for compile-time computations where beneficial
- snake_case for functions/variables, PascalCase for types

### Zig 0.15 API Notes
- `ArrayListUnmanaged` — mutation methods take allocator as first arg
- `std.io.getStdOut().writer(&buf)` + `.interface.print()` — flush before exit
- `std.builtin.Type` tags are lowercase: `.int`, `.@"struct"`, etc.
- `b.createModule()` for build system execution/test targets

### Memory Management
- Arena allocator for per-request allocations
- General purpose allocator for long-lived data
- Always free in same scope or defer cleanup
- `std.testing.allocator` in tests for leak detection

### Error Handling
- Use Zig error unions (`!`) for fallible operations
- Provide meaningful error types — no `anyerror`
- Log errors at appropriate levels before returning

---

## Redis Command Implementation Pattern

1. **Storage layer** (`src/storage/memory.zig`): Add data type variant to `Value` tagged union, implement operations
2. **Command handlers** (`src/commands/<type>.zig`): Parse args → validate → execute against storage → format RESP response
3. **Command routing** (`src/server.zig`): Register in dispatch table
4. **WRONGTYPE errors**: Always check type match before operating
5. **RESP3 awareness**: Return native RESP3 types (maps, sets) when protocol version is 3

---

## Development Cycle (8 Phases)

Each iteration follows this workflow. One iteration = one feature/command group. No scope creep.

### Phase 1 — Planning
**Agent**: `redis-spec-analyzer`
- Analyze Redis spec for target commands
- Document command syntax, return types, error conditions, edge cases
- Cross-reference with redis.io/commands

### Phase 2 — Implementation + Unit Tests
**Agents**: `zig-implementor` + `unit-test-writer` (parallel)
- Implement in `src/commands/`, `src/storage/`
- Embed unit tests at end of source files (Zig convention)
- All tests pass with `zig build test`
- **Gate**: compiles, unit tests pass, no memory leaks

### Phase 3 — Code Quality Review
**Agents**: `zig-quality-reviewer` → `code-reviewer` (sequential)
- Zig review: memory safety, error handling, idioms, doc comments
- Architecture review: separation of concerns, API design, maintainability
- **Gate**: Critical issues → BLOCK. Important issues → FIX or BLOCK.

### Phase 4 — Integration Testing
**Agent**: `integration-test-orchestrator`
- Create E2E RESP protocol tests in `tests/` directory
- Test full request-response cycles and command interactions
- **Gate**: all integration tests pass

### Phase 5 — Validation
**Agents**: `redis-compatibility-validator` + `performance-validator` (parallel)
- Differential testing vs real Redis (byte-by-byte RESP comparison)
- redis-benchmark throughput/latency comparison
- **Gate**: compatibility >= 95%, performance >= 70% of Redis, regression < 10%

### Phase 6 — Documentation
- Update README.md command tables
- Update CLAUDE.md if new patterns emerged

### Phase 7 — Cleanup
- Delete temporary spec/summary documents
- Remove debug prints, TODO comments
- **Kill all background processes**: `pkill -f zoltraak 2>/dev/null; sleep 1`
- Verify port 6379 is free: `lsof -ti :6379 | xargs kill 2>/dev/null`

### Phase 8 — Commit
**Agent**: `git-commit-push`
- Commit format: `feat(<scope>): implement Iteration N — <description>`
- Push to origin/main

### Failure Handling

| Phase | Action |
|-------|--------|
| 1 (Planning) | Request clarification, retry with more context |
| 2 (Implementation) | Review errors, attempt fix, retry |
| 3 (Quality) | Apply fixes, re-run review. Never skip. |
| 4 (Integration) | Fix tests or implementation, retry |
| 5 (Validation) | Fix compatibility/performance, re-run. Never skip. |
| 6-8 | Investigate and escalate if needed |

### Agent Coordination

| Phase | Agent(s) | Execution |
|-------|----------|-----------|
| 1 | redis-spec-analyzer | Solo |
| 2 | zig-implementor + unit-test-writer | Parallel |
| 3 | zig-quality-reviewer → code-reviewer | Sequential |
| 4 | integration-test-orchestrator | Solo |
| 5 | redis-compatibility-validator + performance-validator | Parallel |
| 6-7 | Orchestrator direct | Solo |
| 8 | git-commit-push | Solo |

### Phase 2 Agents (Planned)

- `documentation-writer`: Automated doc updates
- `zig-build-maintainer`: Safe build.zig management
- `redis-subsystem-planner`: Plans complex subsystems
- `redis-command-validator`: Validates command arguments
- `ci-validator`: CI/CD integration
- `bug-investigator`: Systematic debugging on test failures
- `resp-protocol-specialist`: RESP3 protocol edge cases

---

## Autonomous Session Protocol

자동화 세션(cron job 등)에서는 다음 프로토콜을 실행한다.

**컨텍스트 복원** — 세션 시작 시 읽을 파일:
1. `CLAUDE.md` — 프로젝트 규칙, 개발 사이클, 에이전트 아키텍처
2. `README.md` — 지원 명령어, 프로젝트 상태
3. `docs/PRD.md` — 1.0 로드맵, 다음 구현 대상 확인
4. `docs/milestones.md` — 완료 이터레이션 요약, 의존성 마이그레이션 상태

**10단계 실행 사이클**:

| Phase | 내용 | 비고 |
|-------|------|------|
| 1. 상태 파악 | git log, 빌드, 테스트 상태 점검 | PRD + `docs/milestones.md`에서 다음 미완료 이터레이션 식별 |
| 2. 이슈 확인 | `gh issue list --state open --limit 10` | 아래 **이슈 우선순위 프로토콜** 참조 |
| 3. 계획 | 구현 전략을 내부적으로 수립 (텍스트 출력) | 비대화형 세션에서 plan mode 도구 사용 금지 |
| 4. 구현 | Development Cycle 8 Phases 수행 (Planning → Commit) | 사이클당 하나의 이터레이션만 |
| 5. 검증 | `zig build test` 전체 통과 확인 | 실패 시 수정 후 재시도 |
| 6. 코드 리뷰 | 메모리 안전성, Redis 호환성, 테스트 커버리지 확인 | 이슈 발견 시 수정 후 재커밋 |
| 7. 커밋 & 푸시 | `feat(<scope>): implement Iteration N — <description>` | `git add -A` 금지 |
| 8. 릴리즈 판단 | 아래 **릴리즈 판단 기준** 확인 | 조건 충족 시 자율 릴리즈 수행 |
| 9. 프로세스 정리 | `pkill -f zoltraak`, 포트 확인 | 백그라운드 프로세스 전부 종료 |
| 10. 세션 요약 | 구조화된 요약 출력 | 아래 템플릿 참조 |

### 이슈 우선순위 프로토콜

세션 시작 시 GitHub Issues를 확인하고 우선순위를 결정한다:

```bash
gh issue list --state open --limit 10 --json number,title,labels,createdAt
```

| 우선순위 | 조건 | 행동 |
|---------|------|------|
| 1 (최우선) | `bug` 라벨 | 다른 작업보다 항상 우선 처리 |
| 2 (높음) | `migration` 라벨 (`from:sailor`, `from:zuda` 등) | 의존성 마이그레이션 — 현재 작업보다 우선 처리 |
| 3 (보통) | `feature-request` + 현재 이터레이션 범위 내 | 현재 작업과 병행 |
| 4 (낮음) | `feature-request` + 미래 범위 | 적어두고 넘어감 |

- 이슈 처리 후: `gh issue close <number> --comment "Fixed in <commit-hash>"`
- bug 라벨 이슈가 있으면 새 이터레이션 구현보다 반드시 먼저 수정한다
- bug 수정은 별도 커밋: `fix(<scope>): <description>`

**작업 선택 규칙**:
- bug 이슈가 있으면 → 버그 수정 우선
- bug 없으면 → PRD.md의 Phase 순서를 따름 (Phase 1 → Phase 2 → ...)
- 사이클당 하나의 이터레이션만 구현
- 이전 세션의 미완료 작업(빌드 실패, 테스트 실패)이 있으면 먼저 수정

**안전 규칙**:
- Force push 및 파괴적 git 명령어 금지
- 모든 작업은 `main` 브랜치에서 (단일 브랜치 워크플로우)
- 동일 에러 3회 시도 후 지속 시 문서화하고 다음 항목으로 이동
- 스코프 크리프 금지: 사이클당 하나의 이터레이션만 구현
- 기존 테스트가 반드시 계속 통과해야 함
- `zig build test`가 60초 이상 걸리면 hang으로 간주하고 강제 종료

**프로세스 정리 (필수)**:
- 사이클 중 시작한 모든 백그라운드 프로세스는 사이클 종료 전에 반드시 종료
- 커밋 전 마지막 단계: `pkill -f zoltraak 2>/dev/null; sleep 1`
- 포트 확인: `lsof -ti :6379 | xargs kill 2>/dev/null`

**세션 요약 템플릿**:

    ## Session Summary
    ### Iteration
    - [완료한 이터레이션]
    ### Commands Implemented
    - [추가된 Redis 명령어 목록]
    ### Files Changed
    - [생성/수정된 파일 목록]
    ### Tests
    - Unit tests: [수, 상태]
    - Integration tests: [수, 상태]
    ### Redis Compatibility
    - [호환성 검증 결과]
    ### Issues Resolved
    - [처리한 GitHub Issues (번호, 제목)]
    ### Release
    - [릴리즈 수행 여부 및 버전]
    ### Next Priority
    - [다음 사이클에서 구현할 내용]
    ### Issues / Blockers
    - [발생한 문제 또는 미해결 이슈]

---

## Release & Patch Policy

세션 사이클의 Phase 8(릴리즈 판단)에서 아래 조건을 확인하고, 충족 시 자율적으로 릴리즈를 수행한다.

### 릴리즈 판단 기준

**패치 릴리즈 (v0.1.X)** — 다음 중 하나라도 해당하면 즉시 발행:
- 사용자 보고 버그를 수정한 커밋이 마지막 릴리즈 태그 이후에 존재
- 빌드/테스트 실패 수정
- Redis 호환성 깨짐 수정

**마이너 릴리즈 (v0.X.0)** — 다음 조건을 모두 충족 시 발행:
1. 마지막 릴리즈 이후 새로운 Redis 명령어 그룹이 구현됨 (최소 20개 이상의 새 명령어)
2. 해당 명령어에 대한 테스트가 작성되어 있음
3. `zig build test` — 전체 통과, 0 failures
4. `bug` 라벨 이슈가 0개 (open)

**메이저 릴리즈 (v1.0.0)** — 500+ Redis 명령어 구현 완료 + 사용자 승인

### 릴리즈 조건 확인 방법

```bash
# 마지막 태그 이후 커밋 확인
LAST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "")
if [ -n "$LAST_TAG" ]; then
  git log ${LAST_TAG}..HEAD --oneline
else
  git log --oneline -20
fi

# open bug 이슈 확인
gh issue list --state open --label bug --limit 5
```

### 릴리즈 절차

1. `build.zig.zon`의 version 업데이트
2. 커밋: `chore: bump version to v0.X.0`
3. 태그: `git tag -a v0.X.0 -m "Release v0.X.0: <릴리즈 요약>"`
4. 푸시: `git push && git push origin v0.X.0`
5. GitHub Release: `gh release create v0.X.0 --title "v0.X.0: <요약>" --notes "<릴리즈 노트>"`
6. 관련 이슈 닫기: `gh issue close <number> --comment "Resolved in v0.X.0"`
7. Discord 알림: `openclaw message send --channel discord --target user:264745080709971968 --message "[zoltraak] Released v0.X.0 — <요약>"`

### 패치 릴리즈 절차

버그 수정 시 패치 릴리즈를 즉시 발행한다. PATCH 번호만 증가. 기능 커밋을 패치에 포함하지 않음.

1. 버그 수정 커밋 식별
2. `zig build test` 통과 확인
3. 태그: `git tag -a v0.X.Y <commit-hash> -m "Release v0.X.Y: <수정 요약>"`
4. 푸시: `git push origin v0.X.Y`
5. GitHub Release: `gh release create v0.X.Y --title "v0.X.Y: <요약>" --notes "<릴리즈 노트>"`
6. 관련 이슈에 릴리즈 코멘트 추가
7. Discord 알림

---

## Sailor Migration

**Current in zoltraak**: v1.13.0 — All versions through v1.13.0 migrated. See `docs/milestones.md` for full version history.
**Latest available**: v1.13.1 (patch — integer overflow fix, not yet applied)

**마이그레이션 프로토콜**:
1. 세션 시작 시 `docs/milestones.md`의 Sailor 섹션을 확인
2. `status: READY`인 미완료 마이그레이션이 있으면 현재 작업보다 우선 수행
3. 마이그레이션 완료 후 `docs/milestones.md`에서 `DONE`으로 변경하고 커밋
4. `zig build test` 통과 확인 필수

**sailor 이슈 발행**:
```bash
# Bug
gh issue create --repo yusa-imit/sailor --title "bug: <설명>" --label "bug,from:zoltraak" \
  --body "## 증상\n<문제>\n## 재현 방법\n<코드>\n## 환경\n- sailor: <ver>\n- zig: 0.15.2"
# Feature request
gh issue create --repo yusa-imit/sailor --title "feat: <기능>" --label "feature-request,from:zoltraak" \
  --body "## 필요한 이유\n<이유>\n## 제안 API\n<사용 예시>\n## 현재 워크어라운드\n없음"
```

**로컬 워크어라운드 금지 (CRITICAL)**: sailor에 버그가 있으면 로컬 우회 금지. 반드시 이슈 발행 후 수정을 기다린다. sailor 에이전트가 `from:*` 라벨 이슈를 최우선 처리한다.

---

## Resources

- [Redis Commands](https://redis.io/commands/)
- [RESP Protocol Spec](https://redis.io/docs/latest/develop/reference/protocol-spec/)
- [Zig Documentation](https://ziglang.org/documentation/master/)
- [Zoltraak 1.0 PRD](docs/PRD.md)
- [Milestones & Dependencies](docs/milestones.md)

---

## zuda Migration

**Current**: Not yet integrated — all modules PENDING. See `docs/milestones.md` for migration targets.

**마이그레이션 프로토콜**:
1. zuda에서 `from:zuda` 라벨 이슈가 도착하면 status를 `READY`로 변경
2. `build.zig.zon`에 zuda 의존성 추가, 자체 구현을 zuda import로 교체
3. Sorted Set 마이그레이션 시: SkipList의 `(score, member)` 복합 정렬 + rank/score range query 지원 확인 필수
4. `zig build test` + `tests/integration_test.sh` 전체 통과 확인
5. status를 `DONE`으로 변경하고 커밋

**로컬 워크어라운드 금지 (CRITICAL)**: zuda에 버그가 있으면 자체 구현으로 우회하지 않고, `gh issue create --repo yusa-imit/zuda --label "bug,from:zoltraak"` 발행 후 수정 대기. zuda 에이전트가 `from:*` 라벨 이슈를 최우선 처리한다.

