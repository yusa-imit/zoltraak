const std = @import("std");
const testing = std.testing;
const sailor = @import("sailor");

// Test suite for sailor v1.16.0 features
// New features:
// 1. Terminal capability database (termcap module)
// 2. Bracketed paste mode (term.BracketedPaste)
// 3. Synchronized output protocol (term.SynchronizedOutput)
// 4. Hyperlink support (term.writeHyperlink)
// 5. Focus tracking (focus module)

test "sailor v1.16.0 - module availability" {
    // Verify sailor v1.16.0 modules are available
    _ = sailor;
}

test "sailor v1.16.0 - focus module exists" {
    // Verify focus module is available
    const T = @TypeOf(sailor.focus);
    try testing.expect(T == type);
}

test "sailor v1.16.0 - termcap module exists" {
    // Verify termcap module is available
    const T = @TypeOf(sailor.termcap);
    try testing.expect(T == type);
}

test "sailor v1.16.0 - FocusManager type exists" {
    // Verify FocusManager type is available in focus module
    const T = @TypeOf(sailor.focus.FocusManager);
    try testing.expect(T == type);
}

test "sailor v1.16.0 - TermInfo type exists" {
    // Verify TermInfo type is available in termcap module
    const T = @TypeOf(sailor.termcap.TermInfo);
    try testing.expect(T == type);
}

test "sailor v1.16.0 - BracketedPaste in term module" {
    // Verify BracketedPaste is available in term module
    const has_bracketed_paste = @hasDecl(sailor.term, "BracketedPaste");
    try testing.expect(has_bracketed_paste);
}

test "sailor v1.16.0 - SynchronizedOutput in term module" {
    // Verify SynchronizedOutput is available in term module
    const has_sync_output = @hasDecl(sailor.term, "SynchronizedOutput");
    try testing.expect(has_sync_output);
}

test "sailor v1.16.0 - writeHyperlink function exists" {
    // Verify writeHyperlink function is available in term module
    const has_hyperlink = @hasDecl(sailor.term, "writeHyperlink");
    try testing.expect(has_hyperlink);
}

test "sailor v1.16.0 - writeHyperlinkWithParams function exists" {
    // Verify writeHyperlinkWithParams function is available in term module
    const has_hyperlink_params = @hasDecl(sailor.term, "writeHyperlinkWithParams");
    try testing.expect(has_hyperlink_params);
}

test "sailor v1.16.0 - FocusManager creation" {
    // Create a FocusManager instance to verify API
    const allocator = testing.allocator;

    // FocusManager.init returns a non-error value
    var manager = sailor.focus.FocusManager.init(allocator);
    defer manager.deinit();

    // Basic sanity check - should not crash
    try testing.expect(true);
}

test "sailor v1.16.0 - TermInfo load method exists" {
    // Verify TermInfo.load method exists
    const has_load = @hasDecl(sailor.termcap.TermInfo, "load");
    try testing.expect(has_load);

    // Verify TermInfo.parse method exists
    const has_parse = @hasDecl(sailor.termcap.TermInfo, "parse");
    try testing.expect(has_parse);
}

test "sailor v1.16.0 - backward compatibility with v1.15.0" {
    // Verify existing sailor v1.15.0 types still exist
    // Terminal type should still be available
    const T1 = @TypeOf(sailor.tui.Terminal);
    try testing.expect(T1 == type);

    // Buffer type should still be available
    const T2 = @TypeOf(sailor.tui.Buffer);
    try testing.expect(T2 == type);

    // Style type should still be available (v1.5.0+)
    const T3 = @TypeOf(sailor.tui.Style);
    try testing.expect(T3 == type);

    // Color type should still be available
    const T4 = @TypeOf(sailor.color.Color);
    try testing.expect(T4 == type);
}
