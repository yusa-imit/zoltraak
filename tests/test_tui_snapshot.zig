const std = @import("std");
const sailor = @import("sailor");
const tui = sailor.tui;

// Test basic TUI operations with mock terminal (sailor v1.5.0)
// Note: Terminal.init requires actual terminal, so we test the types and styles instead
test "TUI Style creation" {
    // Test various style combinations
    const bold_style = tui.Style{ .fg = tui.Color.white, .bold = true };
    try std.testing.expect(bold_style.bold);
    try std.testing.expectEqual(tui.Color.white, bold_style.fg);

    const colored_style = tui.Style{ .fg = tui.Color.red, .bg = tui.Color.yellow };
    try std.testing.expectEqual(tui.Color.red, colored_style.fg);
    try std.testing.expectEqual(tui.Color.yellow, colored_style.bg);

    const combined_style = tui.Style{
        .fg = tui.Color.cyan,
        .bg = tui.Color.blue,
        .bold = true,
        .italic = true,
        .underline = true,
    };
    try std.testing.expect(combined_style.bold);
    try std.testing.expect(combined_style.italic);
    try std.testing.expect(combined_style.underline);
}

// Test TUI Color enum
test "TUI Color values" {
    // Test that all colors are accessible
    _ = tui.Color.black;
    _ = tui.Color.red;
    _ = tui.Color.green;
    _ = tui.Color.yellow;
    _ = tui.Color.blue;
    _ = tui.Color.magenta;
    _ = tui.Color.cyan;
    _ = tui.Color.white;

    // Verify colors are not equal
    try std.testing.expect(tui.Color.red != tui.Color.blue);
    try std.testing.expect(tui.Color.green != tui.Color.yellow);
}

// Test TUI Rect geometry
test "TUI Rect creation and properties" {
    const rect = tui.Rect.new(10, 20, 80, 24);

    try std.testing.expectEqual(10, rect.x);
    try std.testing.expectEqual(20, rect.y);
    try std.testing.expectEqual(80, rect.width);
    try std.testing.expectEqual(24, rect.height);
}

// Test Buffer creation and deinit
test "TUI Buffer init and deinit" {
    const allocator = std.testing.allocator;

    var buffer = try tui.Buffer.init(allocator, 80, 24);
    defer buffer.deinit();

    try std.testing.expectEqual(80, buffer.width);
    try std.testing.expectEqual(24, buffer.height);
}

// Test that sailor v1.5.0 testing utilities are available
test "Sailor v1.5.0 testing utilities exist" {
    // This test verifies that the v1.5.0 module is correctly loaded
    // by checking that the tui module has the expected types

    // Verify Frame type exists
    const FrameType = @TypeOf(tui.Frame);
    try std.testing.expect(FrameType == type);

    // Verify Buffer type exists
    const BufferType = @TypeOf(tui.Buffer);
    try std.testing.expect(BufferType == type);

    // Verify Style type exists
    const StyleType = @TypeOf(tui.Style);
    try std.testing.expect(StyleType == type);

    // Verify Color type exists
    const ColorType = @TypeOf(tui.Color);
    try std.testing.expect(ColorType == type);

    // Verify Rect type exists
    const RectType = @TypeOf(tui.Rect);
    try std.testing.expect(RectType == type);
}
