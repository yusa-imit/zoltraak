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

## Resources

- [Zig Documentation](https://ziglang.org/documentation/master/)
- [Redis Commands](https://redis.io/commands/)
- [RESP Protocol Specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)
