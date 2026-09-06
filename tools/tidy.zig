//! Tiger Style mechanical tidy checks over `src/`: line length, function length (against a
//! shrink-only baseline), a ban list (`std.debug.print`, `std.time.*`, `catch unreachable`
//! without a proof comment, `usize` in wire-format files), and module `//!` headers.
//!
//! Modes: `tidy check <src_dir> <baseline_path>` gates `zig build test`; `tidy record
//! <src_dir> <baseline_path>` regenerates the baseline from the current tree — a maintainer
//! runs this after fixing violations so the checked-in baseline may only shrink.

const std = @import("std");
const Allocator = std.mem.Allocator;
const assert = std.debug.assert;

pub const line_len_max = 100;
pub const function_lines_max = 70;
pub const function_lines_grace_max = 72;

const wire_format_markers = [_][]const u8{ "protocol/", "persistence", "aof", "rdb" };

pub const FileMetrics = struct {
    file: []const u8,
    long_lines: u32 = 0,
    debug_print: u32 = 0,
    time_calls: u32 = 0,
    catch_unreachable: u32 = 0,
    usize_wire: u32 = 0,
    header_missing: bool = false,
};

pub const FunctionMetrics = struct {
    file: []const u8,
    name: []const u8,
    lines: u32,
};

pub const Baseline = struct {
    files: []const FileMetrics = &.{},
    functions: []const FunctionMetrics = &.{},
};

pub const FunctionSpan = struct {
    name: []const u8,
    start_line: u32,
    lines: u32,
};

// ---- pure, unit-testable helpers ----

pub fn isWireFormatFile(path: []const u8) bool {
    assert(path.len > 0);
    for (wire_format_markers) |marker| {
        if (std.mem.indexOf(u8, path, marker) != null) return true;
    }
    return false;
}

pub fn hasModuleHeader(source: []const u8) bool {
    var lines = std.mem.splitScalar(u8, source, '\n');
    const first = lines.next() orelse return false;
    return std.mem.startsWith(u8, std.mem.trim(u8, first, " \t\r"), "//!");
}

pub const LongLines = struct { count: u32, first_line: u32 };

/// Counts lines over `max` columns; `first_line` is 1-indexed, 0 when `count == 0`.
pub fn scanLongLines(source: []const u8, max: usize) LongLines {
    assert(max > 0);
    var count: u32 = 0;
    var first_line: u32 = 0;
    var lines = std.mem.splitScalar(u8, source, '\n');
    var line_no: u32 = 1;
    while (lines.next()) |line| : (line_no += 1) {
        const trimmed = std.mem.trimRight(u8, line, "\r");
        if (trimmed.len > max) {
            count += 1;
            if (first_line == 0) first_line = line_no;
        }
    }
    return .{ .count = count, .first_line = first_line };
}

pub fn countSubstring(source: []const u8, needle: []const u8) u32 {
    assert(needle.len > 0);
    var count: u32 = 0;
    var index: usize = 0;
    while (std.mem.indexOfPos(u8, source, index, needle)) |found| {
        count += 1;
        index = found + needle.len;
    }
    return count;
}

pub fn countUnprovenCatchUnreachable(source: []const u8) u32 {
    var count: u32 = 0;
    var lines = std.mem.splitScalar(u8, source, '\n');
    while (lines.next()) |line| {
        if (std.mem.indexOf(u8, line, "catch unreachable") == null) continue;
        if (std.mem.indexOf(u8, line, "//") != null) continue;
        count += 1;
    }
    return count;
}

/// Blanks string/char literal bodies and line comments so brace-counting sees only real code
/// braces. Zig double-quoted strings and char literals cannot contain a literal newline, so
/// no quoting state needs to carry across lines.
pub fn stripLineForBraces(buf: []u8, line: []const u8) []u8 {
    assert(buf.len >= line.len);
    var out_len: usize = 0;
    var i: usize = 0;
    while (i < line.len) {
        const c = line[i];
        if (c == '/' and i + 1 < line.len and line[i + 1] == '/') break;
        if (c == '"' or c == '\'') {
            const quote = c;
            buf[out_len] = ' ';
            out_len += 1;
            i += 1;
            while (i < line.len and line[i] != quote) {
                if (line[i] == '\\' and i + 1 < line.len) i += 1;
                buf[out_len] = ' ';
                out_len += 1;
                i += 1;
            }
            if (i < line.len) {
                buf[out_len] = ' ';
                out_len += 1;
                i += 1;
            }
            continue;
        }
        buf[out_len] = c;
        out_len += 1;
        i += 1;
    }
    assert(out_len <= line.len);
    return buf[0..out_len];
}

fn findFnName(stripped: []const u8) ?[]const u8 {
    const marker = "fn ";
    const at = std.mem.indexOf(u8, stripped, marker) orelse return null;
    var start = at + marker.len;
    while (start < stripped.len and stripped[start] == ' ') start += 1;
    var end = start;
    while (end < stripped.len and (std.ascii.isAlphanumeric(stripped[end]) or stripped[end] == '_')) : (end += 1) {}
    if (end == start) return null;
    return stripped[start..end];
}

const line_buf_max = 4096;

const Pending = struct {
    name: []const u8,
    start_line: u32,
    base_depth: i32,
    opened: bool,
};

/// Best-effort function-span scan: a `fn` keyword starts a span; the span ends when brace
/// depth returns to the level it started at, having opened at least one brace (so bodyless
/// function-pointer type declarations in struct fields are skipped, not misdetected).
pub fn scanFunctionSpans(arena: Allocator, source: []const u8) ![]FunctionSpan {
    var spans: std.ArrayListUnmanaged(FunctionSpan) = .empty;
    var strip_buf: [line_buf_max]u8 = undefined;
    var depth: i32 = 0;
    var pending: ?Pending = null;

    var lines = std.mem.splitScalar(u8, source, '\n');
    var line_no: u32 = 1;
    while (lines.next()) |raw_line| : (line_no += 1) {
        const line = raw_line[0..@min(raw_line.len, line_buf_max)];
        const stripped = stripLineForBraces(&strip_buf, line);

        if (pending == null) {
            if (findFnName(stripped)) |name| {
                pending = .{
                    .name = try arena.dupe(u8, name),
                    .start_line = line_no,
                    .base_depth = depth,
                    .opened = false,
                };
            }
        }

        for (stripped) |ch| {
            if (ch == '{') {
                depth += 1;
                if (pending) |*p| {
                    if (depth > p.base_depth) p.opened = true;
                }
            } else if (ch == '}') {
                depth -= 1;
                if (pending) |p| {
                    if (p.opened and depth == p.base_depth) {
                        try spans.append(arena, .{
                            .name = p.name,
                            .start_line = p.start_line,
                            .lines = line_no - p.start_line + 1,
                        });
                        pending = null;
                    }
                }
            }
        }

        if (pending) |p| {
            if (!p.opened) {
                const trimmed = std.mem.trimRight(u8, stripped, " \t\r");
                if (trimmed.len > 0 and (trimmed[trimmed.len - 1] == ';' or trimmed[trimmed.len - 1] == ',')) {
                    pending = null;
                }
            }
        }
    }

    return spans.toOwnedSlice(arena);
}

// ---- tree scan and reporting ----

pub const FileReport = struct {
    file: []const u8,
    metrics: FileMetrics,
    functions_over_limit: []const FunctionSpan,
    first_long_line: u32,
};

pub fn scanFile(arena: Allocator, rel_path: []const u8, source: []const u8) !FileReport {
    var metrics: FileMetrics = .{ .file = rel_path };
    const long_lines = scanLongLines(source, line_len_max);
    metrics.long_lines = long_lines.count;
    metrics.debug_print = countSubstring(source, "std.debug.print(");
    metrics.time_calls = countSubstring(source, "std.time.");
    metrics.catch_unreachable = countUnprovenCatchUnreachable(source);
    metrics.header_missing = !hasModuleHeader(source);
    if (isWireFormatFile(rel_path)) metrics.usize_wire = countSubstring(source, ": usize");

    const spans = try scanFunctionSpans(arena, source);
    var over_limit: std.ArrayListUnmanaged(FunctionSpan) = .empty;
    for (spans) |span| {
        if (span.lines > function_lines_max) try over_limit.append(arena, span);
    }

    return .{
        .file = rel_path,
        .metrics = metrics,
        .functions_over_limit = try over_limit.toOwnedSlice(arena),
        .first_long_line = long_lines.first_line,
    };
}

fn findFileBaseline(baseline: Baseline, file: []const u8) FileMetrics {
    for (baseline.files) |fm| {
        if (std.mem.eql(u8, fm.file, file)) return fm;
    }
    return .{ .file = file };
}

fn findFunctionBaselineLines(baseline: Baseline, file: []const u8, name: []const u8) ?u32 {
    for (baseline.functions) |fnm| {
        if (std.mem.eql(u8, fnm.file, file) and std.mem.eql(u8, fnm.name, name)) return fnm.lines;
    }
    return null;
}

/// Compares one file's current metrics against the baseline; writes a violation line per
/// regression and returns whether the file passed. The baseline may only shrink: a current
/// count above the recorded baseline is a regression, a lower count is a silent improvement.
pub fn checkReport(report: FileReport, baseline: Baseline, out: *std.Io.Writer) !bool {
    var ok = true;
    const base = findFileBaseline(baseline, report.file);

    if (report.metrics.long_lines > base.long_lines) {
        try out.print(
            "{s}:{d}: {d} lines exceed {d} columns, exceeds baseline {d}\n",
            .{ report.file, report.first_long_line, report.metrics.long_lines, line_len_max, base.long_lines },
        );
        ok = false;
    }

    const counters = .{
        .{ "std.debug.print", report.metrics.debug_print, base.debug_print },
        .{ "std.time.*", report.metrics.time_calls, base.time_calls },
        .{ "unproven catch-unreachable", report.metrics.catch_unreachable, base.catch_unreachable },
        .{ "usize in wire format", report.metrics.usize_wire, base.usize_wire },
    };
    inline for (counters) |c| {
        if (c[1] > c[2]) {
            try out.print("{s}: {s} count {d} exceeds baseline {d}\n", .{ report.file, c[0], c[1], c[2] });
            ok = false;
        }
    }

    if (report.metrics.header_missing and !base.header_missing) {
        try out.print("{s}: missing //! module header\n", .{report.file});
        ok = false;
    }

    for (report.functions_over_limit) |span| {
        if (span.lines <= function_lines_grace_max) continue;
        const baseline_lines = findFunctionBaselineLines(baseline, report.file, span.name);
        if (baseline_lines == null or baseline_lines.? < span.lines) {
            try out.print(
                "{s}:{d}: fn {s} is {d} lines, exceeds grace max {d} and baseline\n",
                .{ report.file, span.start_line, span.name, span.lines, function_lines_grace_max },
            );
            ok = false;
        }
    }
    return ok;
}

fn collectReports(arena: Allocator, src_dir_path: []const u8) ![]FileReport {
    var src_dir = try std.fs.cwd().openDir(src_dir_path, .{ .iterate = true });
    defer src_dir.close();

    var reports: std.ArrayListUnmanaged(FileReport) = .empty;
    var walker = try src_dir.walk(arena);
    defer walker.deinit();
    while (try walker.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, ".zig")) continue;

        const source = try src_dir.readFileAlloc(arena, entry.path, 16 * 1024 * 1024);
        const rel_path = try arena.dupe(u8, entry.path);
        try reports.append(arena, try scanFile(arena, rel_path, source));
    }
    return reports.toOwnedSlice(arena);
}

fn writeBaseline(arena: Allocator, reports: []const FileReport, baseline_path: []const u8) !void {
    var files: std.ArrayListUnmanaged(FileMetrics) = .empty;
    var functions: std.ArrayListUnmanaged(FunctionMetrics) = .empty;
    for (reports) |report| {
        const m = report.metrics;
        if (m.long_lines > 0 or m.debug_print > 0 or m.time_calls > 0 or m.catch_unreachable > 0 or
            m.usize_wire > 0 or m.header_missing)
        {
            try files.append(arena, m);
        }
        for (report.functions_over_limit) |span| {
            if (span.lines <= function_lines_grace_max) continue;
            try functions.append(arena, .{ .file = report.file, .name = span.name, .lines = span.lines });
        }
    }

    const baseline: Baseline = .{
        .files = try files.toOwnedSlice(arena),
        .functions = try functions.toOwnedSlice(arena),
    };

    var allocating = std.Io.Writer.Allocating.init(arena);
    try std.zon.stringify.serialize(baseline, .{}, &allocating.writer);
    const bytes = try allocating.toOwnedSlice();
    try std.fs.cwd().writeFile(.{ .sub_path = baseline_path, .data = bytes });
}

fn fatal(comptime msg: []const u8) noreturn {
    std.debug.print(msg ++ "\n", .{});
    std.process.exit(2);
}

pub fn main() !void {
    var arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const args = try std.process.argsAlloc(arena);
    if (args.len < 4) fatal("usage: tidy <check|record> <src_dir> <baseline_path>");
    const mode = args[1];
    const src_dir_path = args[2];
    const baseline_path = args[3];

    const reports = try collectReports(arena, src_dir_path);

    if (std.mem.eql(u8, mode, "record")) {
        try writeBaseline(arena, reports, baseline_path);
        return;
    }
    if (!std.mem.eql(u8, mode, "check")) fatal("unknown mode: expected check or record");

    const baseline_source = std.fs.cwd().readFileAllocOptions(
        arena,
        baseline_path,
        16 * 1024 * 1024,
        null,
        .of(u8),
        0,
    ) catch |err| switch (err) {
        error.FileNotFound => "",
        else => return err,
    };
    const baseline: Baseline = if (baseline_source.len == 0) .{} else try std.zon.parse.fromSlice(
        Baseline,
        arena,
        baseline_source,
        null,
        .{},
    );

    var stderr_buf: [4096]u8 = undefined;
    var stderr_writer = std.fs.File.stderr().writer(&stderr_buf);
    const out = &stderr_writer.interface;

    var all_ok = true;
    for (reports) |report| {
        if (!try checkReport(report, baseline, out)) all_ok = false;
    }
    try out.flush();

    if (!all_ok) std.process.exit(1);
}

// ── Unit tests ──────────────────────────────────────────────────────────────

test "hasModuleHeader: present vs missing vs later in file" {
    try std.testing.expect(hasModuleHeader("//! doc\nconst std = @import(\"std\");\n"));
    try std.testing.expect(!hasModuleHeader("const std = @import(\"std\");\n//! doc\n"));
    try std.testing.expect(!hasModuleHeader(""));
}

test "scanLongLines: flags a 101-column line, allows 100" {
    const ok_line = "x" ** line_len_max;
    try std.testing.expectEqual(@as(u32, 0), scanLongLines(ok_line, line_len_max).count);

    const bad_line = "x" ** (line_len_max + 1);
    const result = scanLongLines(bad_line, line_len_max);
    try std.testing.expectEqual(@as(u32, 1), result.count);
    try std.testing.expectEqual(@as(u32, 1), result.first_line);
}

test "countSubstring: counts non-overlapping occurrences" {
    try std.testing.expectEqual(@as(u32, 2), countSubstring("std.debug.print(a); std.debug.print(b);", "std.debug.print("));
    try std.testing.expectEqual(@as(u32, 0), countSubstring("nothing here", "std.debug.print("));
}

test "countUnprovenCatchUnreachable: comment on the line proves it" {
    const proven = "const v = f() catch unreachable; // f() never fails for valid input\n";
    const unproven = "const v = f() catch unreachable;\n";
    try std.testing.expectEqual(@as(u32, 0), countUnprovenCatchUnreachable(proven));
    try std.testing.expectEqual(@as(u32, 1), countUnprovenCatchUnreachable(unproven));
}

test "stripLineForBraces: braces inside strings and comments do not count" {
    var buf: [256]u8 = undefined;
    const stripped = stripLineForBraces(&buf, "std.log.err(\"{s} { }\", .{}); // trailing { comment");
    try std.testing.expectEqual(@as(usize, 1), countSubstring(stripped, "{"));
}

test "isWireFormatFile: matches known wire-format paths only" {
    try std.testing.expect(isWireFormatFile("protocol/parser.zig"));
    try std.testing.expect(isWireFormatFile("storage/persistence.zig"));
    try std.testing.expect(isWireFormatFile("storage/aof.zig"));
    try std.testing.expect(!isWireFormatFile("commands/strings.zig"));
}

test "scanFunctionSpans: measures a simple function body, ignores fn-typed fields" {
    const source =
        \\const Callback = struct {
        \\    handler: fn (u8) void,
        \\};
        \\
        \\pub fn add(a: u32, b: u32) u32 {
        \\    return a + b;
        \\}
        \\
    ;
    const arena_gpa = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(arena_gpa);
    defer arena_state.deinit();
    const spans = try scanFunctionSpans(arena_state.allocator(), source);

    try std.testing.expectEqual(@as(usize, 1), spans.len);
    try std.testing.expectEqualStrings("add", spans[0].name);
    try std.testing.expectEqual(@as(u32, 3), spans[0].lines);
}

test "checkReport: new violation fails, baselined-and-shrunk violation passes" {
    var buf: [1024]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buf);

    const clean: FileReport = .{
        .file = "a.zig",
        .metrics = .{ .file = "a.zig" },
        .functions_over_limit = &.{},
        .first_long_line = 0,
    };
    try std.testing.expect(try checkReport(clean, .{}, &writer));

    const regressed: FileReport = .{
        .file = "a.zig",
        .metrics = .{ .file = "a.zig", .debug_print = 1 },
        .functions_over_limit = &.{},
        .first_long_line = 0,
    };
    try std.testing.expect(!try checkReport(regressed, .{}, &writer));

    const baseline: Baseline = .{ .files = &.{.{ .file = "a.zig", .debug_print = 2 }} };
    try std.testing.expect(try checkReport(regressed, baseline, &writer));
}
