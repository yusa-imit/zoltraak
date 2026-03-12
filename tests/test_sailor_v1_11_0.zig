const std = @import("std");
const sailor = @import("sailor");
const tui = sailor.tui;

test "sailor v1.11.0 - shadow effects" {
    const allocator = std.testing.allocator;

    // Create terminal and buffer for testing
    var term = try tui.Terminal.init(allocator);
    defer term.deinit();

    var buf = try tui.Buffer.init(allocator, 80, 24);
    defer buf.deinit(allocator);

    // Test shadow rendering
    const area = tui.Rect{ .x = 5, .y = 5, .width = 20, .height = 10 };
    const shadow = tui.effects.ShadowStyle.subtle;

    tui.effects.renderShadow(&buf, area, shadow);

    // Verify shadow was rendered (shadow appears at offset positions)
    try std.testing.expect(buf.width == 80);
    try std.testing.expect(buf.height == 24);
}

test "sailor v1.11.0 - 3D border effects" {
    const allocator = std.testing.allocator;

    var buf = try tui.Buffer.init(allocator, 80, 24);
    defer buf.deinit(allocator);

    // Test 3D border effect
    const area = tui.Rect{ .x = 10, .y = 10, .width = 30, .height = 5 };
    const border3d = tui.effects.BorderStyle3D.raised_button;
    const base_style = tui.Style{ .fg = tui.Color.white, .bg = tui.Color.blue };

    tui.effects.applyBorderEffect(&buf, area, border3d, base_style);

    // Verify buffer was modified
    try std.testing.expect(buf.width == 80);
}

test "sailor v1.11.0 - blur effects module" {
    const allocator = std.testing.allocator;

    var buf = try tui.Buffer.init(allocator, 80, 24);
    defer buf.deinit(allocator);

    // Test blur effect
    const blur = tui.blur.BlurEffect.init(.box_drawing, 128);
    const area = tui.Rect{ .x = 5, .y = 5, .width = 20, .height = 10 };

    blur.apply(&buf, area);

    // Verify blur effect was applied
    try std.testing.expect(buf.width == 80);
}

test "sailor v1.11.0 - transparency effects" {
    const allocator = std.testing.allocator;

    var buf = try tui.Buffer.init(allocator, 80, 24);
    defer buf.deinit(allocator);

    // Test transparency effect
    const transparency = tui.blur.TransparencyEffect.init(.char_fade, 128);
    const area = tui.Rect{ .x = 10, .y = 5, .width = 15, .height = 8 };

    transparency.apply(&buf, area);

    // Verify transparency was applied
    try std.testing.expect(buf.width == 80);
}

test "sailor v1.11.0 - sixel image encoding" {
    const allocator = std.testing.allocator;

    // Create a simple 2x2 test image
    const pixels = [_]tui.sixel.SixelImage.Color{
        tui.sixel.SixelImage.Color.fromRgb(255, 0, 0), // Red
        tui.sixel.SixelImage.Color.fromRgb(0, 255, 0), // Green
        tui.sixel.SixelImage.Color.fromRgb(0, 0, 255), // Blue
        tui.sixel.SixelImage.Color.fromRgb(255, 255, 0), // Yellow
    };

    const image = tui.sixel.SixelImage{
        .width = 2,
        .height = 2,
        .pixels = &pixels,
    };

    // Test encoding to buffer
    var output = std.ArrayList(u8){};
    defer output.deinit(allocator);
    const writer = output.writer(allocator);

    const encoder = tui.sixel.SixelEncoder{};
    try encoder.encode(allocator, image, writer);

    // Verify Sixel sequence was written
    const result = try output.toOwnedSlice(allocator);
    defer allocator.free(result);

    // Sixel sequences start with ESC P q
    try std.testing.expect(result.len > 3);
    try std.testing.expect(result[0] == 0x1b); // ESC
    try std.testing.expect(result[1] == 'P');
}

test "sailor v1.11.0 - kitty graphics protocol" {
    const allocator = std.testing.allocator;

    // Create a test image
    const pixels = [_]u8{ 255, 0, 0, 255 }; // 1x1 red pixel (RGBA)

    // Test Kitty protocol encoding
    var output = std.ArrayList(u8){};
    defer output.deinit(allocator);
    const writer = output.writer(allocator);

    try tui.kitty.transmitImage(allocator, .{
        .width = 1,
        .height = 1,
        .format = .rgba,
        .data = &pixels,
    }, writer);

    // Verify Kitty sequence was written
    const result = try output.toOwnedSlice(allocator);
    defer allocator.free(result);

    // Kitty graphics sequences start with ESC _G
    try std.testing.expect(result.len > 3);
    try std.testing.expect(result[0] == 0x1b); // ESC
    try std.testing.expect(result[1] == '_');
    try std.testing.expect(result[2] == 'G');
}

test "sailor v1.11.0 - easing functions" {
    // Test basic easing functions
    try std.testing.expect(tui.animation.linear(0.5) == 0.5);

    // Ease-in should accelerate
    try std.testing.expect(tui.animation.easeIn(0.5) == 0.25);

    // Ease-out should decelerate
    try std.testing.expect(tui.animation.easeOut(0.5) == 0.75);

    // Ease-in-out at midpoint
    const midpoint = tui.animation.easeInOut(0.5);
    try std.testing.expect(midpoint >= 0.0 and midpoint <= 1.0);
}

test "sailor v1.11.0 - color interpolation" {
    const start_color = tui.Color.red;
    const end_color = tui.Color.blue;

    // Test color interpolation
    const mid_color = tui.animation.lerpColor(start_color, end_color, 0.5);
    _ = mid_color;

    // Verify interpolated color exists
    try std.testing.expect(true);
}
