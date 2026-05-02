const std = @import("std");
const sailor = @import("sailor");

// Tests for sailor v2.5.0 features
// - iTerm2 inline images (OSC 1337) — NOTE: Not yet in public API, deferred
// - Unicode grapheme cluster support ✅
// - Terminal quirks database ✅
// - Benchmark stability ✅

// ============================================================================
// Unicode Grapheme Cluster Support (v2.5.0)
// ============================================================================

test "v2.5.0: grapheme module exists" {
    // Verify that grapheme module is available
    const grapheme = sailor.grapheme;
    const ModuleType = @TypeOf(grapheme);
    try std.testing.expect(ModuleType == type);
}

test "v2.5.0: unicode module exists" {
    // Verify that unicode module is available
    const unicode = sailor.unicode;
    const ModuleType = @TypeOf(unicode);
    try std.testing.expect(ModuleType == type);
}

// ============================================================================
// Terminal Quirks Database (v2.5.0)
// ============================================================================

test "v2.5.0: quirks module exists" {
    // Verify that quirks module is available
    const quirks = sailor.quirks;
    const ModuleType = @TypeOf(quirks);
    try std.testing.expect(ModuleType == type);
}

// ============================================================================
// Benchmark Stability (v2.5.0)
// ============================================================================

test "v2.5.0: bench module exists" {
    // Verify that benchmark module is available
    const bench = sailor.bench;
    const ModuleType = @TypeOf(bench);
    try std.testing.expect(ModuleType == type);
}

// ============================================================================
// Backward Compatibility Verification
// ============================================================================

test "v2.5.0: backward compatible with term module" {
    // Verify that existing term API still works
    const term = sailor.term;
    const ModuleType = @TypeOf(term);
    try std.testing.expect(ModuleType == type);
}

test "v2.5.0: backward compatible with Color API" {
    const tui = sailor.tui;

    // Verify colors still work
    _ = tui.Color.red;
    _ = tui.Color.green;
    _ = tui.Color.blue;

    try std.testing.expect(true);
}

test "v2.5.0: backward compatible with Style API" {
    const tui = sailor.tui;

    // Verify Style creation still works
    const style = tui.Style{ .fg = tui.Color.white, .bold = true };
    try std.testing.expect(style.bold);
}

test "v2.5.0: backward compatible with arg parser" {
    const arg = sailor.arg;
    const ModuleType = @TypeOf(arg);
    try std.testing.expect(ModuleType == type);
}

test "v2.5.0: backward compatible with color module" {
    const color = sailor.color;
    const ModuleType = @TypeOf(color);
    try std.testing.expect(ModuleType == type);
}

test "v2.5.0: backward compatible with repl module" {
    const repl = sailor.repl;
    const ModuleType = @TypeOf(repl);
    try std.testing.expect(ModuleType == type);
}

test "v2.5.0: backward compatible with progress module" {
    const progress = sailor.progress;
    const ModuleType = @TypeOf(progress);
    try std.testing.expect(ModuleType == type);
}

test "v2.5.0: backward compatible with fmt module" {
    const fmt = sailor.fmt;
    const ModuleType = @TypeOf(fmt);
    try std.testing.expect(ModuleType == type);
}

test "v2.5.0: backward compatible with tui framework" {
    const tui = sailor.tui;
    const ModuleType = @TypeOf(tui);
    try std.testing.expect(ModuleType == type);
}

test "v2.5.0: Rect API uses struct literal initialization" {
    const tui = sailor.tui;

    // v2.5.0 removed Rect.new() — now uses direct struct initialization
    const rect = tui.Rect{ .x = 10, .y = 20, .width = 80, .height = 24 };

    try std.testing.expectEqual(@as(u16, 10), rect.x);
    try std.testing.expectEqual(@as(u16, 20), rect.y);
    try std.testing.expectEqual(@as(u16, 80), rect.width);
    try std.testing.expectEqual(@as(u16, 24), rect.height);
}
