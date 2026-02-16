# Zoltraak Development Cycle Prompt

> This prompt is executed by an automated cron job. Claude Code runs this as a fully autonomous development session.

---

## Context

You are working on **Zoltraak**, a Redis-compatible in-memory data store written in Zig. The goal is a drop-in Redis replacement with improved performance and memory efficiency.

Before doing anything, read these files to restore full project context:

1. `CLAUDE.md` — Project overview, coding standards, agent architecture, 8-phase workflow
2. `ORCHESTRATOR.md` — Detailed 8-phase dev cycle orchestration protocol
3. `README.md` — Current roadmap, supported commands, project status

---

## Current State

**Completed iterations**:
- Iteration 1: String commands (PING, SET, GET, DEL, EXISTS) + key expiration
- Iteration 2: List commands (LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN)
- Iteration 3: Set commands (SADD, SREM, SISMEMBER, SMEMBERS, SCARD)

**Remaining roadmap** (in priority order):
1. Hash commands (HSET, HGET, HDEL, HGETALL, HKEYS, HVALS, HEXISTS, HLEN)
2. Sorted Set commands (ZADD, ZREM, ZRANGE, ZRANGEBYSCORE, ZSCORE, ZCARD)
3. Persistence — RDB snapshots
4. Persistence — AOF logging
5. Pub/Sub messaging
6. Transactions (MULTI/EXEC)
7. Replication

---

## Execution Protocol

Execute the following phases in strict order. This follows the 8-phase workflow defined in `ORCHESTRATOR.md`.

### Phase 1: Status Assessment & Planning

**Assess current state**:
- Run `git log --oneline -10` to see recent work
- Run `zig build` to verify the project compiles
- Run `zig build test` to check test health
- Check which source files exist: `src/commands/`, `src/storage/`, `src/protocol/`
- Read `README.md` roadmap to identify the **next uncompleted item**

**Pick the next iteration**:
- Follow the roadmap order strictly
- Pick ONE data structure or feature per cycle
- If the previous session left incomplete work (build failure, failing tests), fix it first

**Plan the iteration** (acts as `redis-spec-analyzer`):
- Research the Redis command specifications for the target commands
  - Exact command syntax and arguments
  - Return types and RESP encoding
  - Error conditions and edge cases
  - Behavior with wrong types (WRONGTYPE error)
- Identify which files need to be created/modified:
  - `src/commands/<type>.zig` — command handlers
  - `src/storage/memory.zig` — storage layer additions
  - `src/server.zig` — command routing
  - `tests/test_integration.zig` — integration tests
- Document the plan before proceeding

### Phase 2: Implementation + Unit Tests

Implement the feature following the established patterns in the codebase.

**Implementation pattern** (follow existing code):
1. **Storage layer** (`src/storage/memory.zig`):
   - Add the new data type to the `Value` tagged union
   - Implement storage operations (add, remove, get, etc.)

2. **Command handlers** (`src/commands/<type>.zig`):
   - Create command handler functions matching the existing pattern
   - Each handler: parse args -> validate -> execute on storage -> format RESP response
   - Handle WRONGTYPE errors for type mismatches

3. **Command routing** (`src/server.zig`):
   - Register new commands in the command dispatch table

4. **Unit tests** (embedded in source files, Zig convention):
   - Test each command handler
   - Test success cases, error cases, edge cases
   - Use `std.testing.allocator` for leak detection

**Verification**:
```bash
zig build          # Must compile
zig build test     # All tests must pass
```

If anything fails, fix and re-verify before proceeding.

### Phase 3: Code Quality Review

Self-review the implementation:

**Zig quality checks**:
- Memory safety: proper allocator usage, `defer`/`errdefer` for cleanup
- Error handling: no `anyerror`, proper error propagation with explicit error sets
- No undefined behavior or memory leaks
- Follows Zig naming conventions (snake_case functions, PascalCase types)

**Architecture checks**:
- Consistent with existing command handler patterns
- Proper separation between protocol, commands, and storage
- No code duplication across command files
- Clean API boundaries

If critical issues found, fix them and re-verify (go back to Phase 2 verification).

### Phase 4: Integration Testing

Add integration tests in `tests/test_integration.zig`:

- Test full RESP protocol request-response cycles
- Test command interactions (e.g., SET then GET, type conflicts)
- Test edge cases (empty keys, large values, concurrent operations)
- Follow the existing integration test patterns in the file

```bash
zig build test     # All tests including integration must pass
```

### Phase 5: Compatibility Validation

Validate Redis compatibility:

- Verify RESP response format matches Redis exactly
- Check error messages match Redis conventions (`-ERR`, `-WRONGTYPE`)
- Verify integer replies, bulk string replies, array replies are correct
- Test type conflict behavior (e.g., string command on list key)

**Note**: If a real Redis instance is not available, validate by:
- Cross-referencing with Redis documentation (https://redis.io/commands/)
- Checking RESP encoding matches the spec
- Verifying error message formats

### Phase 6: Documentation

Update project documentation:

1. **README.md**: Add the new commands to the "Supported Commands" table and update "Project Status"
2. **CLAUDE.md**: Update if new patterns or conventions emerged
3. **Source files**: Ensure doc comments on new public APIs

### Phase 7: Cleanup

- Remove any temporary planning documents or spec files
- Ensure no debug prints or TODO comments left in committed code
- Clean workspace

### Phase 8: Commit

Commit all changes with descriptive messages:

```
feat(<scope>): implement Redis <Type> commands

Add <COMMAND1>, <COMMAND2>, ... commands with full RESP compatibility.
Includes storage layer support, command routing, unit tests, and
integration tests.

Co-Authored-By: Claude <noreply@anthropic.com>
```

- Use appropriate commit types: `feat`, `fix`, `test`, `docs`, `refactor`
- Commit incrementally if changes are large (storage, commands, tests, docs separately)
- Stage specific files, never use `git add -A`

---

## Safety Rules

- **Never force push** or run destructive git commands
- **All work on `main` branch** (this project uses a single-branch workflow)
- **Stop if stuck**: If the same error persists after 3 fix attempts, document the issue and move on to the next iteration item
- **No scope creep**: Implement ONE iteration per cycle. Do not start the next data structure.
- **Preserve existing functionality**: All existing tests must continue to pass
- **No external dependencies**: Rely only on Zig's standard library

---

## Zig Conventions (Quick Reference)

- Zig 0.15.x APIs (see `CLAUDE.md` or zr project's `.claude/memory/zig-0.15-migration.md` for breaking changes from 0.14)
- `ArrayListUnmanaged` instead of `ArrayList` — mutation methods take allocator as first arg
- `std.io.getStdOut().writer(&buf)` with `.interface.print()` — must flush before exit
- `std.builtin.Type` tags are lowercase: `.int`, `.@"struct"`, etc.
- `b.createModule()` for build system executable/test targets

---

## Session Summary Template

Output this at session end:

```
## Session Summary

### Iteration
- [Which iteration was completed, e.g., "Iteration 4: Hash Commands"]

### Commands Implemented
- [List of Redis commands added]

### Files Changed
- [List of files created/modified]

### Tests
- Unit tests: [count and status]
- Integration tests: [count and status]

### Redis Compatibility
- [Notes on compatibility verification]

### Next Priority
- [What the next cycle should implement]

### Issues / Blockers
- [Any problems encountered or unresolved issues]
```
