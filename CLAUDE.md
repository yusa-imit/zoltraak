# CLAUDE.md

This file provides guidance for Claude Code when working with the Zoltraak codebase.

## Project Overview

Zoltraak is a Redis-compatible in-memory data store written in Zig. The goal is to provide a drop-in replacement for Redis with improved performance and memory efficiency.

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

## Resources

- [Zig Documentation](https://ziglang.org/documentation/master/)
- [Redis Commands](https://redis.io/commands/)
- [RESP Protocol Specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)
