const std = @import("std");
const sailor = @import("sailor");

test "sailor v1.22.0 - SpanBuilder basic functionality" {
    const allocator = std.testing.allocator;

    // Create a span with bold and color
    var builder = sailor.text.SpanBuilder.init(allocator);
    defer builder.deinit();

    try builder.addText("Hello", .{ .bold = true, .fg_color = .red });
    try builder.addText(" ", .{});
    try builder.addText("World", .{ .italic = true, .fg_color = .blue });

    const spans = try builder.build();
    defer allocator.free(spans);

    // Verify we got 3 spans
    try std.testing.expectEqual(@as(usize, 3), spans.len);

    // Verify first span
    try std.testing.expectEqualStrings("Hello", spans[0].text);
    try std.testing.expect(spans[0].style.bold);
    try std.testing.expect(spans[0].style.fg_color == .red);

    // Verify second span (space)
    try std.testing.expectEqualStrings(" ", spans[1].text);

    // Verify third span
    try std.testing.expectEqualStrings("World", spans[2].text);
    try std.testing.expect(spans[2].style.italic);
    try std.testing.expect(spans[2].style.fg_color == .blue);
}

test "sailor v1.22.0 - LineBuilder multi-style line" {
    const allocator = std.testing.allocator;

    // Create a line with multiple styled segments
    var builder = sailor.text.LineBuilder.init(allocator);
    defer builder.deinit();

    try builder.append("Error: ", .{ .bold = true, .fg_color = .red });
    try builder.append("File not found", .{ .fg_color = .yellow });

    const line = try builder.build();
    defer line.deinit(allocator);

    // Verify line has expected spans
    try std.testing.expect(line.spans.len >= 2);
}

test "sailor v1.22.0 - RichTextParser markdown conversion" {
    const allocator = std.testing.allocator;

    const markdown_text = "**bold** and *italic* text";

    var parser = sailor.text.RichTextParser.init(allocator);
    defer parser.deinit();

    const parsed = try parser.parseLine(markdown_text);
    defer {
        for (parsed) |span| {
            allocator.free(span.text);
        }
        allocator.free(parsed);
    }

    // Should have at least 3 spans: bold, normal, italic
    try std.testing.expect(parsed.len >= 3);

    // Find bold span
    var found_bold = false;
    var found_italic = false;
    for (parsed) |span| {
        if (span.style.bold and std.mem.eql(u8, span.text, "bold")) {
            found_bold = true;
        }
        if (span.style.italic and std.mem.eql(u8, span.text, "italic")) {
            found_italic = true;
        }
    }

    try std.testing.expect(found_bold);
    try std.testing.expect(found_italic);
}

test "sailor v1.22.0 - LineBreaker word wrapping" {
    const allocator = std.testing.allocator;

    const long_text = "This is a very long line that should be wrapped to fit within the maximum width constraint";

    var breaker = sailor.text.LineBreaker.init(allocator, .{
        .max_width = 20,
        .hyphenate = false,
    });
    defer breaker.deinit();

    const lines = try breaker.breakText(long_text);
    defer {
        for (lines) |line| {
            allocator.free(line);
        }
        allocator.free(lines);
    }

    // Should have wrapped into multiple lines
    try std.testing.expect(lines.len > 1);

    // Each line should be <= max_width
    for (lines) |line| {
        const width = sailor.text.measureWidth(line);
        try std.testing.expect(width <= 20);
    }
}

test "sailor v1.22.0 - text width measurement with Unicode" {
    // Test ASCII text
    const ascii_width = sailor.text.measureWidth("Hello");
    try std.testing.expectEqual(@as(usize, 5), ascii_width);

    // Test with CJK characters (each typically 2 cells wide)
    const cjk_width = sailor.text.measureWidth("你好");
    try std.testing.expect(cjk_width >= 4); // Each CJK char is 2 cells

    // Test with emoji (can be 1-2 cells)
    const emoji_width = sailor.text.measureWidth("👍");
    try std.testing.expect(emoji_width >= 1);
}

test "sailor v1.22.0 - multiline text height" {
    const text = "Line 1\nLine 2\nLine 3";

    const height = sailor.text.measureHeight(text);
    try std.testing.expectEqual(@as(usize, 3), height);
}

test "sailor v1.22.0 - RichTextParser multi-line markdown" {
    const allocator = std.testing.allocator;

    const markdown =
        \\# Heading
        \\This is **bold** text
        \\And this is *italic*
    ;

    var parser = sailor.text.RichTextParser.init(allocator);
    defer parser.deinit();

    const lines = try parser.parseMultiLine(markdown);
    defer {
        for (lines) |line| {
            for (line) |span| {
                allocator.free(span.text);
            }
            allocator.free(line);
        }
        allocator.free(lines);
    }

    // Should have 3 lines
    try std.testing.expectEqual(@as(usize, 3), lines.len);

    // First line should be heading (bold and larger perhaps)
    try std.testing.expect(lines[0].len > 0);
}

test "sailor v1.22.0 - SpanBuilder with strikethrough and code" {
    const allocator = std.testing.allocator;

    var builder = sailor.text.SpanBuilder.init(allocator);
    defer builder.deinit();

    try builder.addText("deleted", .{ .strikethrough = true });
    try builder.addText(" ", .{});
    try builder.addText("code", .{ .code = true });

    const spans = try builder.build();
    defer allocator.free(spans);

    try std.testing.expectEqual(@as(usize, 3), spans.len);
    try std.testing.expect(spans[0].style.strikethrough);
    try std.testing.expect(spans[2].style.code);
}
