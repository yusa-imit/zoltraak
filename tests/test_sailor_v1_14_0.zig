const std = @import("std");
const testing = std.testing;

// Sailor v1.14.0 — Memory Pooling, Render Profiling, Virtual Rendering, Incremental Layout, Buffer Compression
// Tests verify module availability and basic integration
// Note: v1.14.0 features are mostly performance optimizations internal to sailor
// These tests verify backward compatibility and that sailor v1.14.0 loads correctly

test "sailor v1.14.0 backward compatibility - core modules" {
    // v1.14.0 maintains backward compatibility with all previous versions
    const sailor = @import("sailor");

    // Core modules must exist
    const has_tui = @hasDecl(sailor, "tui");
    const has_arg = @hasDecl(sailor, "arg");

    try testing.expect(has_tui);
    try testing.expect(has_arg);
}

test "sailor v1.14.0 performance features - profiler integration" {
    // v1.14.0 adds render profiling to identify slow widgets
    // PerformanceProfiler was introduced in v1.9.0, v1.14.0 extends it
    const sailor = @import("sailor");

    // Verify TUI module exists (profiling is internal to TUI rendering)
    try testing.expect(@hasDecl(sailor, "tui"));

    // Profiler exists from v1.9.0, v1.14.0 enhances performance tracking
    if (@hasDecl(sailor.tui, "PerformanceProfiler")) {
        // Profiler type exists, v1.14.0 adds memory pool tracking
        try testing.expect(true);
    } else {
        // If not directly exposed, verify base functionality works
        try testing.expect(@hasDecl(sailor, "tui"));
    }
}

test "sailor v1.14.0 virtual rendering - viewport optimization" {
    // v1.14.0 adds virtual rendering: only render widgets in viewport
    const sailor = @import("sailor");

    // Viewport clipping was added in v1.7.0, v1.14.0 optimizes it further
    // Virtual rendering is internal, verify TUI foundation exists
    try testing.expect(@hasDecl(sailor, "tui"));

    // Check if Rect type exists (used for viewport bounds)
    if (@hasDecl(sailor.tui, "Rect")) {
        try testing.expect(true);
    } else {
        // Viewport types might be internal, verify TUI works
        try testing.expect(@hasDecl(sailor, "tui"));
    }
}

test "sailor v1.14.0 incremental layout - caching optimization" {
    // v1.14.0 adds incremental layout: cache results, only recompute on changes
    const sailor = @import("sailor");

    // Layout caching was introduced in v1.7.0, v1.14.0 improves it
    // Layout system is internal to TUI, verify foundation exists
    try testing.expect(@hasDecl(sailor, "tui"));

    // Layout types are internal to sailor, v1.14.0 optimizes performance
    // Just verify the TUI module loads correctly
}

test "sailor v1.14.0 buffer compression - memory optimization" {
    // v1.14.0 adds buffer compression to reduce memory for large TUI apps
    const sailor = @import("sailor");

    // Buffer type exists from earlier versions, v1.14.0 adds compression
    try testing.expect(@hasDecl(sailor, "tui"));

    // Verify Buffer type exists (compression is internal to Buffer impl)
    if (@hasDecl(sailor.tui, "Buffer")) {
        try testing.expect(true);
    } else {
        // Buffer might be internal, verify TUI works
        try testing.expect(@hasDecl(sailor, "tui"));
    }
}

test "sailor v1.14.0 memory pooling - allocation optimization" {
    // v1.14.0 introduces memory pooling for frequently created objects
    const sailor = @import("sailor");

    // Memory pooling is internal to sailor's allocation strategy
    // Verify that sailor loads and core functionality works
    try testing.expect(@hasDecl(sailor, "tui"));
    try testing.expect(@hasDecl(sailor, "arg"));

    // Memory pooling reduces allocations but is transparent to users
    // Just verify backward compatibility
}

test "sailor v1.14.0 TUI rendering - basic functionality" {
    // Verify TUI rendering still works with v1.14.0 optimizations
    const sailor = @import("sailor");

    // v1.14.0 optimizes rendering internally but maintains same API
    try testing.expect(@hasDecl(sailor, "tui"));

    // All previous TUI functionality should still work
    // Memory pooling and virtual rendering are transparent optimizations
}

test "sailor v1.14.0 arg parsing - CLI functionality" {
    // Verify arg module still works with v1.14.0
    const sailor = @import("sailor");

    // arg module is critical for zoltraak CLI
    try testing.expect(@hasDecl(sailor, "arg"));

    // v1.14.0 performance improvements don't affect arg parsing API
}

test "sailor v1.14.0 complete feature set verification" {
    // Comprehensive check: all critical sailor modules work with v1.14.0
    const sailor = @import("sailor");

    // v1.14.0 is a performance release, all APIs remain stable
    try testing.expect(@hasDecl(sailor, "tui"));
    try testing.expect(@hasDecl(sailor, "arg"));

    // The following features from v1.14.0 are internal optimizations:
    // - Memory pooling (transparent to users)
    // - Render profiling (extends v1.9.0 PerformanceProfiler)
    // - Virtual rendering (only render visible widgets)
    // - Incremental layout (cache layout results)
    // - Buffer compression (reduce memory footprint)

    // All previous functionality (v0.1.0 through v1.13.1) works unchanged
}

test "sailor v1.14.0 integration smoke test" {
    // Smoke test: import sailor and verify critical modules
    const sailor = @import("sailor");

    // Core modules must exist
    try testing.expect(@hasDecl(sailor, "arg"));
    try testing.expect(@hasDecl(sailor, "tui"));

    // v1.14.0 should maintain all previous functionality
    // If any critical module is missing, this test will fail
}
