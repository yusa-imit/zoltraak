# CLAUDE.md

This file provides guidance for Claude Code when working with the Zoltraak codebase.

## Project Overview

Zoltraak is a Redis-compatible in-memory data store written in Zig. The goal is to provide a drop-in replacement for Redis with improved performance and memory efficiency.

## Claude Team Features

This project can leverage Claude Team capabilities for enhanced collaboration:

- **Shared Projects**: This codebase can be shared across team members in Claude Team, allowing multiple developers to collaborate with consistent context and project knowledge
- **Persistent Memory**: Claude Code maintains project-specific memory across sessions, learning from patterns, conventions, and decisions made during development
- **Team Knowledge Base**: Architectural decisions, implementation patterns, and project-specific conventions are preserved and accessible to all team members
- **Collaborative Workflows**: Team members can build on each other's work with shared context about the codebase structure, coding standards, and development practices
- **Consistent Code Quality**: Shared understanding of project guidelines ensures consistent code style and architecture across all contributions

When working with Claude Code on this project, the AI will maintain awareness of project patterns, previous decisions, and team preferences to provide more contextual and consistent assistance.

## Development Status

**완료된 이터레이션**:
- Iteration 1: String commands (PING, SET, GET, DEL, EXISTS) + key expiration
- Iteration 2: List commands (LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN)
- Iteration 3: Set commands (SADD, SREM, SISMEMBER, SMEMBERS, SCARD)
- Iteration 4: Hash commands (HSET, HGET, HDEL, HGETALL, HKEYS, HVALS, HEXISTS, HLEN)

**남은 로드맵** (우선순위 순):
1. Sorted Set commands (ZADD, ZREM, ZRANGE, ZRANGEBYSCORE, ZSCORE, ZCARD)
2. Sorted Set commands (ZADD, ZREM, ZRANGE, ZRANGEBYSCORE, ZSCORE, ZCARD)
3. Persistence — RDB snapshots
4. Persistence — AOF logging
5. Pub/Sub messaging
6. Transactions (MULTI/EXEC)
7. Replication

## Build Commands

```bash
# Build the project
zig build

# Run tests
zig build test

# Build with optimizations
zig build -Doptimize=ReleaseFast

# Build and run
zig build run

# Clean build artifacts
rm -rf zig-out .zig-cache
```

## Project Structure

```
zoltraak/
├── src/
│   ├── main.zig          # Entry point
│   ├── server.zig        # TCP server implementation
│   ├── protocol/         # RESP protocol handling
│   │   ├── parser.zig    # RESP parser
│   │   └── writer.zig    # RESP response writer
│   ├── commands/         # Command implementations
│   │   ├── strings.zig   # String commands (GET, SET, etc.)
│   │   ├── lists.zig     # List commands
│   │   ├── sets.zig      # Set commands
│   │   ├── hashes.zig    # Hash commands
│   │   └── sorted_sets.zig
│   ├── storage/          # Data storage layer
│   │   ├── memory.zig    # In-memory storage
│   │   └── persistence.zig
│   └── utils/            # Utility functions
├── build.zig             # Build configuration
├── build.zig.zon         # Package dependencies
└── tests/                # Integration tests
```

## Development Guidelines

### Zig Conventions
- Use `std.mem.Allocator` for all allocations
- Prefer `errdefer` for cleanup on error paths
- Use `comptime` for compile-time computations where beneficial
- Follow Zig's naming conventions: snake_case for functions and variables, PascalCase for types

**Zig 0.15 API 참고사항**:
- `ArrayListUnmanaged` 사용 — mutation 메서드가 allocator를 첫 번째 인자로 받음
- `std.io.getStdOut().writer(&buf)` + `.interface.print()` — 종료 전 flush 필수
- `std.builtin.Type` 태그는 소문자: `.int`, `.@"struct"` 등
- `b.createModule()` 로 빌드 시스템 실행/테스트 타겟 생성

### Code Style
- Keep functions focused and small
- Document public APIs with doc comments (`///`)
- Use meaningful variable names
- Avoid `anytype` unless necessary for generic code

### Memory Management
- The server uses an arena allocator for per-request allocations
- Long-lived data uses a general purpose allocator
- Always free allocations in the same scope or defer cleanup

### Error Handling
- Use Zig's error unions (`!`) for fallible operations
- Provide meaningful error types in `error` sets
- Log errors at appropriate levels before returning

## Key Components

### RESP Protocol
The Redis Serialization Protocol is implemented in `src/protocol/`. Key types:
- Simple Strings: `+OK\r\n`
- Errors: `-ERR message\r\n`
- Integers: `:1000\r\n`
- Bulk Strings: `$6\r\nfoobar\r\n`
- Arrays: `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`

### Command Processing
1. Parse incoming RESP data
2. Route to appropriate command handler
3. Execute command against storage
4. Return RESP-formatted response

### Storage Engine
- Hash map based key-value store
- Type-tagged values for different data structures
- Expiration handled via lazy deletion + active expiration

## Testing

```bash
# Run all tests (unit + integration)
zig build test

# Run only integration tests
zig build test-integration

# Run specific test file
zig test src/protocol/parser.zig

# Run with verbose output
zig build test -- --verbose

# Run shell-based integration tests (requires server running)
./tests/integration_test.sh
```

## Debugging

```bash
# Build with debug info
zig build -Doptimize=Debug

# Use LLDB for debugging
lldb ./zig-out/bin/zoltraak
```

## Performance Considerations

- Use `std.ArrayList` with pre-allocated capacity for known sizes
- Avoid unnecessary allocations in hot paths
- Use `@prefetch` for predictable memory access patterns
- Profile with `zig build -Doptimize=ReleaseFast` for accurate benchmarks

## Common Tasks

### Adding a New Command
1. Create handler function in appropriate `src/commands/*.zig` file
2. Register command in the command router
3. Add tests for the new command
4. Update documentation

### Implementing a Data Structure
1. Define the type in `src/storage/`
2. Implement required operations
3. Add type tag to value enum
4. Create command handlers

## Dependencies

This project aims for minimal external dependencies, relying primarily on Zig's standard library.

## Agent Architecture

Zoltraak uses a sophisticated multi-agent development workflow with specialized agents for planning, implementation, quality review, testing, and validation. This architecture ensures high code quality, Redis compatibility, and performance standards.

### Development Workflow (8 Phases)

The `dev-cycle-orchestrator` coordinates complete development cycles through 8 phases:

1. **Planning**: `redis-spec-analyzer` analyzes Redis specifications and validates compliance
2. **Implementation + Unit Tests**: `zig-implementor` + `unit-test-writer` create code with embedded tests
3. **Code Quality Review**: `zig-quality-reviewer` + `code-reviewer` ensure quality at technical and architectural levels
4. **Integration Testing**: `integration-test-orchestrator` creates end-to-end RESP protocol tests
5. **Validation**: `redis-compatibility-validator` + `performance-validator` verify Redis compatibility and performance
6. **Documentation**: Documentation updates
7. **Cleanup**: Remove temporary files
8. **Commit**: `git-commit-push` commits and pushes changes

### Phase 1 Agents (Current)

**Planning & Analysis:**
- `redis-spec-analyzer`: Plans iterations with deep Redis spec validation

**Implementation:**
- `zig-implementor`: Implements features following Zig conventions (renamed from spec-implementor)
- `unit-test-writer`: Embeds unit tests in source files (Zig convention)

**Quality Assurance:**
- `zig-quality-reviewer`: Reviews Zig code for memory safety, error handling, and idioms
- `code-reviewer`: Reviews architectural design and maintainability

**Testing:**
- `integration-test-orchestrator`: Creates end-to-end integration tests (refocused from integration-test-generator)

**Validation:**
- `redis-compatibility-validator`: Differential testing against real Redis
- `performance-validator`: Zig profiling + redis-benchmark comparison (merged from 3 agents)

**Version Control:**
- `git-commit-push`: Manages commits and pushes

**Orchestration:**
- `dev-cycle-orchestrator`: Coordinates the 8-phase workflow

### Phase 2 Agents (Planned)

The following agents are approved for Phase 2 implementation:
- `documentation-writer`: Automated documentation updates
- `zig-build-maintainer`: Safe build.zig management
- `redis-subsystem-planner`: Plans complex subsystems (Pub/Sub, Transactions, Persistence)
- `redis-command-validator`: Validates command arguments and options
- `ci-validator`: CI/CD integration and pre-commit checks
- `bug-investigator`: Systematic debugging when tests fail
- `resp-protocol-specialist`: RESP3 protocol implementation

### Quality Gates

The workflow includes multiple quality gates to prevent issues:

**Phase 3 Gate (Code Quality)**:
- Blocks on critical Zig issues (memory leaks, anyerror usage)
- Blocks on important architectural problems
- Must pass before testing

**Phase 5 Gate (Validation)**:
- Blocks if Redis compatibility < 95%
- Blocks if performance < 70% of Redis
- Blocks if performance regression > 10%
- Must pass before commit

### Testing Strategy

**Unit Tests (Embedded)**:
- Written by `unit-test-writer`
- Embedded in source files (Zig convention)
- Test individual functions in isolation
- Use `std.testing.allocator` for leak detection

**Integration Tests (Separate Files)**:
- Written by `integration-test-orchestrator`
- Located in `tests/` directory
- Test end-to-end RESP protocol flows
- Verify command interactions

**Compatibility Tests**:
- Performed by `redis-compatibility-validator`
- Differential testing against real Redis
- Byte-by-byte RESP comparison
- Client library compatibility (redis-py, node-redis, etc.)

**Performance Tests**:
- Performed by `performance-validator`
- redis-benchmark comparison
- Zig memory profiling
- Hot path analysis

### Agent Invocation

To use an agent for a task:
```
<Use the redis-spec-analyzer agent to plan the next iteration>
<Use the zig-implementor agent to implement the ZADD command>
<Use the redis-compatibility-validator agent to verify HSET compatibility>
```

Or invoke the orchestrator for complete workflows:
```
<Use the dev-cycle-orchestrator to implement the INCR command end-to-end>
```

### Agent Development Guidelines

When working with agents:
- Agents have specific responsibilities - don't overlap
- Orchestrator coordinates sequential and parallel execution
- Quality gates must pass - never skip reviews or validation
- Unit tests are embedded, integration tests are separate
- Always validate against Redis specifications

## Autonomous Session Protocol

자동화 세션(cron job 등)에서는 다음 프로토콜을 실행한다.

**컨텍스트 복원** — 세션 시작 시 읽을 파일:
1. `CLAUDE.md` — 프로젝트 규칙, 코딩 표준, 에이전트 아키텍처
2. `ORCHESTRATOR.md` — 8단계 개발 사이클 오케스트레이션 프로토콜
3. `README.md` — 현재 로드맵, 지원 명령어, 프로젝트 상태

**작업 선택 규칙**:
- 로드맵 순서를 엄격히 따름
- 사이클당 하나의 데이터 구조 또는 기능만 구현
- 이전 세션의 미완료 작업(빌드 실패, 테스트 실패)이 있으면 먼저 수정

**8단계 실행 사이클**:

| Phase | 내용 |
|-------|------|
| 1. 상태 파악 & 계획 | git log, `zig build test` 확인 → Redis 명령어 사양 분석 → 구현 계획 |
| 2. 구현 + 유닛 테스트 | Storage layer → Command handlers → Command routing → 유닛 테스트 |
| 3. 코드 품질 리뷰 | 메모리 안전성, 에러 처리, 아키텍처 일관성 검사 |
| 4. 통합 테스트 | `tests/test_integration.zig`에 RESP 프로토콜 E2E 테스트 추가 |
| 5. 호환성 검증 | RESP 응답 포맷·에러 메시지가 Redis와 일치하는지 확인 |
| 6. 문서화 | README.md 지원 명령어 테이블 업데이트 |
| 7. 정리 | 임시 파일 제거, 디버그 프린트·TODO 주석 정리 |
| 8. 커밋 & 푸시 | `feat(<scope>): implement Redis <Type> commands` 형식으로 커밋 후 `git push` 실행 |

**Redis 명령어 구현 패턴**:
1. **Storage layer** (`src/storage/memory.zig`): `Value` tagged union에 새 데이터 타입 추가, 저장 연산 구현
2. **Command handlers** (`src/commands/<type>.zig`): args 파싱 → 검증 → storage 실행 → RESP 응답 포맷
3. **Command routing** (`src/server.zig`): 명령어 디스패치 테이블에 등록
4. **WRONGTYPE 에러**: 타입 불일치 시 반드시 처리

**안전 규칙**:
- Force push 및 파괴적 git 명령어 금지
- 모든 작업은 `main` 브랜치에서 (단일 브랜치 워크플로우)
- 동일 에러 3회 시도 후 지속 시 문서화하고 다음 항목으로 이동
- 스코프 크리프 금지: 사이클당 하나의 이터레이션만 구현
- 기존 테스트가 반드시 계속 통과해야 함
- 외부 의존성 없음: Zig 표준 라이브러리만 사용

**세션 요약 템플릿**:

    ## Session Summary
    ### Iteration
    - [완료한 이터레이션, 예: "Iteration 4: Hash Commands"]
    ### Commands Implemented
    - [추가된 Redis 명령어 목록]
    ### Files Changed
    - [생성/수정된 파일 목록]
    ### Tests
    - Unit tests: [수, 상태]
    - Integration tests: [수, 상태]
    ### Redis Compatibility
    - [호환성 검증 결과]
    ### Next Priority
    - [다음 사이클에서 구현할 내용]
    ### Issues / Blockers
    - [발생한 문제 또는 미해결 이슈]

## Resources

- [Zig Documentation](https://ziglang.org/documentation/master/)
- [Redis Commands](https://redis.io/commands/)
- [RESP Protocol Specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)
