const std = @import("std");
const sailor = @import("sailor");
const net = std.net;
const tui = sailor.tui;

/// TUI Dashboard with advanced widgets from sailor v0.5.0
pub const Dashboard = struct {
    allocator: std.mem.Allocator,
    terminal: *tui.Terminal,
    stream: net.Stream,

    // State
    selected_index: usize,
    show_delete_dialog: bool,
    notification_text: ?[]const u8,
    notification_timer: u64,

    // Data
    keys_tree: KeysTree,
    memory_stats: MemoryStats,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, terminal: *tui.Terminal, stream: net.Stream) !Self {
        return Self{
            .allocator = allocator,
            .terminal = terminal,
            .stream = stream,
            .selected_index = 0,
            .show_delete_dialog = false,
            .notification_text = null,
            .notification_timer = 0,
            .keys_tree = try KeysTree.init(allocator),
            .memory_stats = MemoryStats{},
        };
    }

    pub fn deinit(self: *Self) void {
        self.keys_tree.deinit();
    }

    /// Refresh keys from Redis server and populate tree
    pub fn refreshKeys(self: *Self) !void {
        // Send KEYS * command
        try self.stream.writeAll("*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n");

        // Parse response
        var read_buffer: [8192]u8 = undefined;
        const n = try self.stream.read(&read_buffer);
        const data = read_buffer[0..n];

        // Clear existing tree
        self.keys_tree.clear();

        // Parse RESP array response and build tree
        if (data.len > 0 and data[0] == '*') {
            // Skip "*N\r\n" to get to elements
            var pos: usize = 1;
            while (pos < data.len and data[pos] != '\r') : (pos += 1) {}
            pos += 2; // Skip \r\n

            // Read each key
            while (pos < data.len) {
                if (data[pos] == '$') {
                    // Bulk string
                    pos += 1;
                    const len_end = std.mem.indexOfPos(u8, data, pos, "\r\n") orelse break;
                    const len_str = data[pos..len_end];
                    const len = try std.fmt.parseInt(usize, len_str, 10);
                    pos = len_end + 2;

                    if (pos + len <= data.len) {
                        const key = data[pos .. pos + len];
                        try self.keys_tree.addKey(key);
                        pos += len + 2; // Skip key + \r\n
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        // Show notification
        self.showNotification("Keys refreshed");
    }

    /// Refresh memory stats via INFO MEMORY
    pub fn refreshMemoryStats(self: *Self) !void {
        // Send INFO MEMORY command
        try self.stream.writeAll("*2\r\n$4\r\nINFO\r\n$6\r\nMEMORY\r\n");

        var read_buffer: [8192]u8 = undefined;
        const n = try self.stream.read(&read_buffer);
        _ = read_buffer[0..n]; // Parse later

        // For now, simulate with placeholder values
        self.memory_stats.used_memory = 1024 * 1024; // 1MB
        self.memory_stats.peak_memory = 2 * 1024 * 1024; // 2MB
        self.memory_stats.num_keys = self.keys_tree.totalKeys();
    }

    /// Show notification toast
    pub fn showNotification(self: *Self, text: []const u8) void {
        self.notification_text = text;
        self.notification_timer = 3; // Show for 3 seconds
    }

    /// Handle delete key confirmation
    pub fn showDeleteDialog(self: *Self) void {
        self.show_delete_dialog = true;
    }

    pub fn closeDeleteDialog(self: *Self) void {
        self.show_delete_dialog = false;
    }

    pub fn deleteSelectedKey(self: *Self) !void {
        const selected_key = self.keys_tree.getSelectedKey(self.selected_index) orelse return;

        // Send DEL command
        const del_cmd = try std.fmt.allocPrint(self.allocator, "*2\r\n$3\r\nDEL\r\n${}\r\n{s}\r\n", .{ selected_key.len, selected_key });
        defer self.allocator.free(del_cmd);

        try self.stream.writeAll(del_cmd);

        // Read response
        var read_buffer: [256]u8 = undefined;
        _ = try self.stream.read(&read_buffer);

        // Refresh keys
        try self.refreshKeys();
        self.showNotification("Key deleted");
        self.closeDeleteDialog();
    }
};

/// Hierarchical tree structure for keys
const KeysTree = struct {
    allocator: std.mem.Allocator,
    root: TreeNode,

    const TreeNode = struct {
        name: []const u8,
        children: std.ArrayList(*TreeNode),
        is_leaf: bool,

        fn init(allocator: std.mem.Allocator, name: []const u8, is_leaf: bool) !*TreeNode {
            const node = try allocator.create(TreeNode);
            node.* = .{
                .name = try allocator.dupe(u8, name),
                .children = std.ArrayList(*TreeNode){},
                .is_leaf = is_leaf,
            };
            return node;
        }

        fn deinit(self: *TreeNode, allocator: std.mem.Allocator) void {
            allocator.free(self.name);
            for (self.children.items) |child| {
                child.deinit(allocator);
                allocator.destroy(child);
            }
            self.children.deinit(allocator);
        }
    };

    pub fn init(allocator: std.mem.Allocator) !KeysTree {
        const root = try TreeNode.init(allocator, "keys", false);
        return KeysTree{
            .allocator = allocator,
            .root = root.*,
        };
    }

    pub fn deinit(self: *KeysTree) void {
        self.root.deinit(self.allocator);
    }

    pub fn clear(self: *KeysTree) void {
        for (self.root.children.items) |child| {
            child.deinit(self.allocator);
            self.allocator.destroy(child);
        }
        self.root.children.clearRetainingCapacity();
    }

    pub fn addKey(self: *KeysTree, key: []const u8) !void {
        // Split key by ':' to create hierarchy
        var parts = std.mem.splitSequence(u8, key, ":");
        var current = &self.root;

        var is_last = false;
        var part_buf: [256]u8 = undefined;

        while (parts.next()) |part| {
            // Check if this is the last part
            const peek = parts.peek();
            is_last = (peek == null);

            // Copy part to buffer
            @memcpy(part_buf[0..part.len], part);
            const part_copy = part_buf[0..part.len];

            // Find or create child node
            var found = false;
            for (current.children.items) |child| {
                if (std.mem.eql(u8, child.name, part_copy)) {
                    current = child;
                    found = true;
                    break;
                }
            }

            if (!found) {
                const new_node = try TreeNode.init(self.allocator, part_copy, is_last);
                try current.children.append(self.allocator, new_node);
                current = new_node;
            }
        }
    }

    pub fn totalKeys(self: *KeysTree) usize {
        return self.countLeaves(&self.root);
    }

    fn countLeaves(self: *const KeysTree, node: *const TreeNode) usize {
        var count: usize = 0;
        if (node.is_leaf) {
            count += 1;
        }
        for (node.children.items) |child| {
            count += self.countLeaves(child);
        }
        return count;
    }

    pub fn getSelectedKey(self: *KeysTree, index: usize) ?[]const u8 {
        var current_index: usize = 0;
        return self.findKeyAtIndex(&self.root, index, &current_index);
    }

    fn findKeyAtIndex(self: *const KeysTree, node: *const TreeNode, target: usize, current: *usize) ?[]const u8 {
        if (node.is_leaf) {
            if (current.* == target) {
                return node.name;
            }
            current.* += 1;
        }

        for (node.children.items) |child| {
            if (self.findKeyAtIndex(child, target, current)) |key| {
                return key;
            }
        }

        return null;
    }
};

const MemoryStats = struct {
    used_memory: usize = 0,
    peak_memory: usize = 0,
    num_keys: usize = 0,
};

/// Render Tree widget for hierarchical key browsing
pub fn renderTree(
    frame: *tui.Frame,
    area: tui.Rect,
    tree: *const KeysTree,
    selected_index: usize,
) !void {
    // Use sailor's Tree widget from v0.5.0
    // For now, render a simple hierarchical view manually

    var y: u16 = area.y;
    const max_y = area.y + area.height;

    try renderTreeNode(frame, &y, max_y, area.x, area.width, &tree.root, 0, selected_index);
}

fn renderTreeNode(
    frame: *tui.Frame,
    y: *u16,
    max_y: u16,
    x: u16,
    width: u16,
    node: *const KeysTree.TreeNode,
    depth: u16,
    selected_index: usize,
) !void {
    if (y.* >= max_y) return;

    // Indent based on depth
    const indent = depth * 2;
    if (indent >= width) return;

    // Render node name
    const prefix = if (node.is_leaf) "• " else "▸ ";
    const name_start = x + indent;
    const available_width = width -| indent;

    const style = if (selected_index == 0) tui.Style{ .fg = tui.Color.cyan } else tui.Style{};

    if (available_width > prefix.len) {
        frame.setString(name_start, y.*, prefix, style);

        const name_max_len = @min(node.name.len, available_width - prefix.len);
        const name = node.name[0..name_max_len];
        frame.setString(name_start + @as(u16, @intCast(prefix.len)), y.*, name, style);
    }

    y.* += 1;

    // Render children
    for (node.children.items) |child| {
        try renderTreeNode(frame, y, max_y, x, width, child, depth + 1, selected_index);
    }
}

/// Render LineChart widget for memory/connection metrics
pub fn renderLineChart(
    frame: *tui.Frame,
    area: tui.Rect,
    stats: *const MemoryStats,
) !void {
    // Use sailor's LineChart widget from v0.5.0
    // For now, render simple bar chart

    const title = "Memory Usage";
    frame.setString(area.x, area.y, title, tui.Style{});

    // Calculate percentages
    const used_pct = if (stats.peak_memory > 0) (stats.used_memory * 100) / stats.peak_memory else 0;

    // Render bar
    const bar_y = area.y + 2;
    const bar_width = @min(area.width, 50);
    const filled_width = (bar_width * used_pct) / 100;

    var i: u16 = 0;
    while (i < filled_width) : (i += 1) {
        frame.setString(area.x + i, bar_y, "█", tui.Style{ .fg = tui.Color.green });
    }

    // Show stats
    var stats_buf: [128]u8 = undefined;
    const stats_text = try std.fmt.bufPrint(&stats_buf, "Used: {} bytes | Peak: {} bytes | Keys: {}", .{ stats.used_memory, stats.peak_memory, stats.num_keys });
    frame.setString(area.x, bar_y + 2, stats_text, tui.Style{});
}

/// Cache hit/miss statistics for funnel visualization
pub const CacheStats = struct {
    total_commands: usize = 0,
    keyspace_hits: usize = 0,
    keyspace_misses: usize = 0,
    connected_clients: usize = 0,
};

/// Render FunnelChart widget for cache hit-rate visualization using sailor v2.76.0.
/// Displays the conversion funnel: Total Commands → Keyspace Hits → Cache Hit %.
pub fn renderFunnelChart(
    frame: *tui.Frame,
    area: tui.Rect,
    stats: *const CacheStats,
) void {
    if (area.width == 0 or area.height == 0) return;

    const stages = [_]tui.widgets.FunnelStage{
        .{
            .label = "Commands",
            .value = @floatFromInt(stats.total_commands),
            .style = tui.Style{ .fg = tui.Color.cyan },
        },
        .{
            .label = "Hits",
            .value = @floatFromInt(stats.keyspace_hits),
            .style = tui.Style{ .fg = tui.Color.green },
        },
        .{
            .label = "Misses",
            .value = @floatFromInt(stats.keyspace_misses),
            .style = tui.Style{ .fg = tui.Color.yellow },
        },
        .{
            .label = "Clients",
            .value = @floatFromInt(stats.connected_clients),
            .style = tui.Style{ .fg = tui.Color.magenta },
        },
    };

    const chart = tui.widgets.FunnelChart.init()
        .withStages(&stages)
        .withShowValues(true)
        .withShowPercentages(true);

    chart.render(frame.buffer, area);
}

/// Render Dialog widget for DEL command confirmation
pub fn renderDialog(
    frame: *tui.Frame,
    area: tui.Rect,
    key_name: []const u8,
) !void {
    // Center dialog
    const dialog_width: u16 = 50;
    const dialog_height: u16 = 7;
    const dialog_x = (area.width -| dialog_width) / 2;
    const dialog_y = (area.height -| dialog_height) / 2;

    // Draw border
    const border_style = tui.Style{ .fg = tui.Color.yellow };

    // Top border
    var i: u16 = 0;
    while (i < dialog_width) : (i += 1) {
        const ch = if (i == 0) "┌" else if (i == dialog_width - 1) "┐" else "─";
        frame.setString(dialog_x + i, dialog_y, ch, border_style);
    }

    // Bottom border
    i = 0;
    while (i < dialog_width) : (i += 1) {
        const ch = if (i == 0) "└" else if (i == dialog_width - 1) "┘" else "─";
        frame.setString(dialog_x + i, dialog_y + dialog_height - 1, ch, border_style);
    }

    // Side borders
    var j: u16 = 1;
    while (j < dialog_height - 1) : (j += 1) {
        frame.setString(dialog_x, dialog_y + j, "│", border_style);
        frame.setString(dialog_x + dialog_width - 1, dialog_y + j, "│", border_style);
    }

    // Title
    const title = " Confirm Delete ";
    frame.setString(dialog_x + 2, dialog_y, title, tui.Style{ .fg = tui.Color.red });

    // Message
    var msg_buf: [64]u8 = undefined;
    const msg = try std.fmt.bufPrint(&msg_buf, "Delete key: {s}?", .{key_name});
    const msg_truncated = if (msg.len > dialog_width - 4) msg[0 .. dialog_width - 4] else msg;
    frame.setString(dialog_x + 2, dialog_y + 2, msg_truncated, tui.Style{});

    // Buttons
    const yes_text = "[Y] Yes";
    const no_text = "[N] No";
    frame.setString(dialog_x + 2, dialog_y + 4, yes_text, tui.Style{ .fg = tui.Color.green });
    frame.setString(dialog_x + 15, dialog_y + 4, no_text, tui.Style{ .fg = tui.Color.red });
}

/// Render Notification toast
/// Latency distribution data for dot plot visualization.
pub const LatencyStats = struct {
    /// Latency buckets: label + microsecond value
    command_name: []const u8 = "GET",
    p50_us: f32 = 0.0,
    p95_us: f32 = 0.0,
    p99_us: f32 = 0.0,
    p999_us: f32 = 0.0,
};

/// Render DotPlot widget for command latency distribution using sailor v2.77.0.
/// Displays per-percentile latency as dots on a horizontal axis (Cleveland dot plot).
pub fn renderDotPlot(
    frame: *tui.Frame,
    area: tui.Rect,
    stats: *const LatencyStats,
) void {
    if (area.width == 0 or area.height == 0) return;

    const max_lat = @max(stats.p999_us, 1.0);
    const items = [_]tui.widgets.DotPlotItem{
        .{
            .label = "p50",
            .value = stats.p50_us,
            .style = tui.Style{ .fg = tui.Color.green },
        },
        .{
            .label = "p95",
            .value = stats.p95_us,
            .style = tui.Style{ .fg = tui.Color.yellow },
        },
        .{
            .label = "p99",
            .value = stats.p99_us,
            .style = tui.Style{ .fg = tui.Color.red },
        },
        .{
            .label = "p999",
            .value = stats.p999_us,
            .style = tui.Style{ .fg = tui.Color.magenta },
        },
    };

    const plot = tui.widgets.DotPlot.init()
        .withItems(&items)
        .withXMin(0.0)
        .withXMax(max_lat)
        .withShowLabels(true)
        .withShowValues(true);

    plot.render(frame.buffer, area);
}

/// Server resource utilization metrics for radial bar visualization.
pub const ServerMetrics = struct {
    /// CPU utilization (0.0–1.0)
    cpu_usage: f32 = 0.0,
    /// Memory utilization (0.0–1.0)
    memory_usage: f32 = 0.0,
    /// Network bandwidth utilization (0.0–1.0)
    network_usage: f32 = 0.0,
    /// Disk I/O utilization (0.0–1.0)
    disk_usage: f32 = 0.0,
};

/// Render RadialBar widget for server resource utilization using sailor v2.78.0.
/// Displays CPU, memory, network, and disk usage as concentric arcs.
pub fn renderRadialBar(
    frame: *tui.Frame,
    area: tui.Rect,
    metrics: *const ServerMetrics,
) void {
    if (area.width == 0 or area.height == 0) return;

    const arcs = [_]tui.widgets.RadialArc{
        .{
            .label = "CPU",
            .value = metrics.cpu_usage,
            .style = tui.Style{ .fg = tui.Color.green },
        },
        .{
            .label = "MEM",
            .value = metrics.memory_usage,
            .style = tui.Style{ .fg = tui.Color.cyan },
        },
        .{
            .label = "NET",
            .value = metrics.network_usage,
            .style = tui.Style{ .fg = tui.Color.yellow },
        },
        .{
            .label = "DISK",
            .value = metrics.disk_usage,
            .style = tui.Style{ .fg = tui.Color.magenta },
        },
    };

    const bar = tui.widgets.RadialBar.init()
        .withArcs(&arcs)
        .withShowLabels(true)
        .withShowValues(true);

    bar.render(frame.buffer, area);
}

/// Per-command throughput history for stream graph visualization.
/// Each layer holds a rolling series of samples (e.g. ops/sec) for one command.
pub const CommandThroughputHistory = struct {
    get_samples: []const f32 = &.{},
    set_samples: []const f32 = &.{},
    del_samples: []const f32 = &.{},
};

/// Render StreamGraph widget for command throughput over time using sailor v2.79.0.
/// Displays per-command throughput history as a theme-river-style stacked silhouette,
/// stacked symmetrically around a vertically centered baseline.
pub fn renderStreamGraph(
    frame: *tui.Frame,
    area: tui.Rect,
    history: *const CommandThroughputHistory,
) void {
    if (area.width == 0 or area.height == 0) return;

    const layers = [_]tui.widgets.StreamLayer{
        .{
            .label = "GET",
            .values = history.get_samples,
            .style = tui.Style{ .fg = tui.Color.green },
        },
        .{
            .label = "SET",
            .values = history.set_samples,
            .style = tui.Style{ .fg = tui.Color.cyan },
        },
        .{
            .label = "DEL",
            .values = history.del_samples,
            .style = tui.Style{ .fg = tui.Color.red },
        },
    };

    const graph = tui.widgets.StreamGraph.init()
        .withLayers(&layers)
        .withShowLabels(true)
        .withFocused(0);

    graph.render(frame.buffer, area);
}

/// Per-data-type value size samples for density-distribution visualization.
/// Each series holds raw sizes (bytes) observed for keys of that Redis type.
pub const KeySizeDistribution = struct {
    string_sizes: []const f32 = &.{},
    list_sizes: []const f32 = &.{},
    hash_sizes: []const f32 = &.{},
};

/// Render ViolinPlot widget for per-type key size distribution using sailor v2.80.0.
/// Displays value sizes for string/list/hash keys as mirrored density silhouettes,
/// sharing a global min/max scale so shapes are directly comparable across types.
pub fn renderViolinPlot(
    frame: *tui.Frame,
    area: tui.Rect,
    dist: *const KeySizeDistribution,
) void {
    if (area.width == 0 or area.height == 0) return;

    const series = [_]tui.widgets.ViolinSeries{
        .{
            .label = "STR",
            .values = dist.string_sizes,
            .style = tui.Style{ .fg = tui.Color.green },
        },
        .{
            .label = "LIST",
            .values = dist.list_sizes,
            .style = tui.Style{ .fg = tui.Color.cyan },
        },
        .{
            .label = "HASH",
            .values = dist.hash_sizes,
            .style = tui.Style{ .fg = tui.Color.yellow },
        },
    };

    const plot = tui.widgets.ViolinPlot.init()
        .withSeries(&series)
        .withShowLabels(true)
        .withFocused(0);

    plot.render(frame.buffer, area);
}

/// Render BoxPlot widget for per-type key size quartile summary using sailor v2.82.0.
/// Companion view to renderViolinPlot: shares the same KeySizeDistribution samples
/// but displays five-number-summary statistics (min/Q1/median/Q3/max) with outlier
/// markers instead of a density silhouette, for a quick at-a-glance size comparison.
pub fn renderBoxPlot(
    frame: *tui.Frame,
    area: tui.Rect,
    dist: *const KeySizeDistribution,
) void {
    if (area.width == 0 or area.height == 0) return;

    const series = [_]tui.widgets.BoxPlotSeries{
        .{
            .label = "STR",
            .values = dist.string_sizes,
            .style = tui.Style{ .fg = tui.Color.green },
        },
        .{
            .label = "LIST",
            .values = dist.list_sizes,
            .style = tui.Style{ .fg = tui.Color.cyan },
        },
        .{
            .label = "HASH",
            .values = dist.hash_sizes,
            .style = tui.Style{ .fg = tui.Color.yellow },
        },
    };

    const plot = tui.widgets.BoxPlot.init()
        .withSeries(&series)
        .withShowLabels(true)
        .withShowOutliers(true)
        .withFocused(0);

    plot.render(frame.buffer, area);
}

/// Per-database key-type counts for hierarchical keyspace visualization.
/// Each database holds counts of keys broken down by Redis data type.
pub const DatabaseKeyTypeCounts = struct {
    db_index: u16 = 0,
    string_count: f32 = 0,
    list_count: f32 = 0,
    hash_count: f32 = 0,
    set_count: f32 = 0,
    zset_count: f32 = 0,
};

/// Render SunburstChart widget for hierarchical keyspace breakdown using sailor v2.81.0.
/// Root ring shows databases; second ring shows per-database key-type distribution
/// (STRING/LIST/HASH/SET/ZSET), letting an operator see at a glance where keys live
/// across both database and data-type dimensions.
pub fn renderSunburstChart(
    frame: *tui.Frame,
    area: tui.Rect,
    databases: []const DatabaseKeyTypeCounts,
) void {
    if (area.width == 0 or area.height == 0) return;

    var children_buf: [8][5]tui.widgets.SunburstNode = undefined;
    var nodes_buf: [8]tui.widgets.SunburstNode = undefined;
    const n = @min(databases.len, tui.widgets.SunburstChart.MAX_NODES);

    var label_buf: [8][16]u8 = undefined;
    for (0..n) |i| {
        const db = databases[i];
        children_buf[i] = [_]tui.widgets.SunburstNode{
            .{ .label = "STR", .value = db.string_count, .style = tui.Style{ .fg = tui.Color.green } },
            .{ .label = "LIST", .value = db.list_count, .style = tui.Style{ .fg = tui.Color.cyan } },
            .{ .label = "HASH", .value = db.hash_count, .style = tui.Style{ .fg = tui.Color.yellow } },
            .{ .label = "SET", .value = db.set_count, .style = tui.Style{ .fg = tui.Color.magenta } },
            .{ .label = "ZSET", .value = db.zset_count, .style = tui.Style{ .fg = tui.Color.blue } },
        };

        const label = std.fmt.bufPrint(&label_buf[i], "db{d}", .{db.db_index}) catch "db?";
        const total = db.string_count + db.list_count + db.hash_count + db.set_count + db.zset_count;
        nodes_buf[i] = .{ .label = label, .value = total, .children = children_buf[i][0..] };
    }

    const chart = tui.widgets.SunburstChart.init()
        .withNodes(nodes_buf[0..n])
        .withShowLabels(true)
        .withShowValues(true)
        .withFocused(0);

    chart.render(frame.buffer, area);
}

/// Memory usage OHLC (open/high/low/close) sample for one sampling window,
/// summarizing used_memory (bytes) fluctuation observed within that period.
pub const MemoryUsagePeriod = struct {
    label: []const u8 = "",
    open_bytes: f32 = 0,
    high_bytes: f32 = 0,
    low_bytes: f32 = 0,
    close_bytes: f32 = 0,
};

/// Render CandlestickChart widget for memory usage OHLC per period using sailor v2.83.0.
/// Each candle summarizes used_memory (bytes) fluctuation within a sampling window —
/// bullish (green) candles show net growth over the window, bearish (red) show net
/// shrink — letting an operator spot memory growth trends and volatility at a glance,
/// complementing the point-in-time gauges from renderRadialBar with a time-series view.
pub fn renderMemoryUsageCandles(
    frame: *tui.Frame,
    area: tui.Rect,
    periods: []const MemoryUsagePeriod,
) void {
    if (area.width == 0 or area.height == 0) return;

    var candles_buf: [tui.widgets.CandlestickChart.MAX_CANDLES]tui.widgets.Candle = undefined;
    const n = @min(periods.len, tui.widgets.CandlestickChart.MAX_CANDLES);
    for (0..n) |i| {
        const p = periods[i];
        candles_buf[i] = .{
            .label = p.label,
            .open = p.open_bytes,
            .high = p.high_bytes,
            .low = p.low_bytes,
            .close = p.close_bytes,
        };
    }

    const chart = tui.widgets.CandlestickChart.init()
        .withCandles(candles_buf[0..n])
        .withShowLabels(true)
        .withUpStyle(tui.Style{ .fg = tui.Color.green })
        .withDownStyle(tui.Style{ .fg = tui.Color.red })
        .withFocused(0);

    chart.render(frame.buffer, area);
}

/// Memory budget for a single tracked resource, expressed as an actual value
/// against a target ceiling (e.g. used_memory vs maxmemory).
pub const MemoryBudget = struct {
    label: []const u8 = "MEM",
    used_bytes: f32 = 0,
    limit_bytes: f32 = 0,
};

/// Render BulletChart widget for used_memory-vs-maxmemory KPI using sailor v2.84.0.
/// Each bullet shows the current usage as a value bar, maxmemory as a target tick,
/// and safe/warn/critical qualitative bands (60%/85%/100% of the limit) so an operator
/// can see at a glance how close a database is to its configured memory ceiling —
/// a compact single-row complement to the multi-arc renderRadialBar gauge.
pub fn renderMemoryBudgetBullet(
    frame: *tui.Frame,
    area: tui.Rect,
    budgets: []const MemoryBudget,
) void {
    if (area.width == 0 or area.height == 0) return;

    var ranges_buf: [tui.widgets.BulletChart.MAX_BULLETS][3]f32 = undefined;
    var bullets_buf: [tui.widgets.BulletChart.MAX_BULLETS]tui.widgets.Bullet = undefined;
    const n = @min(budgets.len, tui.widgets.BulletChart.MAX_BULLETS);

    var max_value: f32 = 1.0;
    for (0..n) |i| {
        max_value = @max(max_value, budgets[i].limit_bytes);
    }

    for (0..n) |i| {
        const b = budgets[i];
        ranges_buf[i] = .{ b.limit_bytes * 0.6, b.limit_bytes * 0.85, b.limit_bytes };
        bullets_buf[i] = .{
            .label = b.label,
            .value = b.used_bytes,
            .target = b.limit_bytes,
            .ranges = ranges_buf[i][0..],
            .style = tui.Style{ .fg = tui.Color.cyan },
        };
    }

    const chart = tui.widgets.BulletChart.init()
        .withBullets(bullets_buf[0..n])
        .withMaxValue(max_value)
        .withShowLabels(true)
        .withShowValues(true)
        .withFocused(0);

    chart.render(frame.buffer, area);
}

/// Per-database key count sampled at two points in time (e.g. before/after
/// a BGSAVE, FLUSHDB, or migration window), used to visualize keyspace churn.
pub const DatabaseKeyCountChange = struct {
    db_index: u16 = 0,
    before_count: f32 = 0,
    after_count: f32 = 0,
};

/// Render SlopeChart widget for per-database keyspace change using sailor v2.87.0.
/// Each database draws a diagonal line from its before_count to after_count,
/// with direction-based coloring (increase/decrease/flat) so an operator can
/// see at a glance which databases grew or shrank across a sampling window —
/// a two-point-per-category complement to the single-point renderDotPlot.
pub fn renderKeyspaceChangeSlope(
    frame: *tui.Frame,
    area: tui.Rect,
    databases: []const DatabaseKeyCountChange,
) void {
    if (area.width == 0 or area.height == 0) return;

    var items_buf: [tui.widgets.SlopeChart.MAX_ITEMS]tui.widgets.SlopeItem = undefined;
    const n = @min(databases.len, tui.widgets.SlopeChart.MAX_ITEMS);

    var label_buf: [tui.widgets.SlopeChart.MAX_ITEMS][16]u8 = undefined;
    var max_value: f32 = 1.0;
    for (0..n) |i| {
        const db = databases[i];
        max_value = @max(max_value, @max(db.before_count, db.after_count));
    }

    for (0..n) |i| {
        const db = databases[i];
        const label = std.fmt.bufPrint(&label_buf[i], "db{d}", .{db.db_index}) catch "db?";
        items_buf[i] = .{
            .label = label,
            .left_value = db.before_count,
            .right_value = db.after_count,
        };
    }

    const chart = tui.widgets.SlopeChart.init()
        .withItems(items_buf[0..n])
        .withMinValue(0.0)
        .withMaxValue(max_value)
        .withLeftLabel("before")
        .withRightLabel("after")
        .withShowLabels(true)
        .withShowValues(true)
        .withFocused(0);

    chart.render(frame.buffer, area);
}

/// Per-command latency histogram — pre-binned frequency counts sampled from
/// LATENCY HISTORY / commandstats, one bucket-count series per command.
pub const CommandLatencyHistogram = struct {
    command: []const u8 = "",
    /// Frequency count per latency bucket (e.g. <1ms, <2ms, <4ms, ... doubling).
    bucket_counts: []const f32 = &.{},
};

/// Render RidgelinePlot widget comparing per-command latency distribution
/// shape using sailor v2.88.0. Each command draws a stacked density
/// silhouette from its LATENCY HISTORY / commandstats bucket counts, letting
/// an operator spot which commands have wide/bimodal/skewed latency spread
/// at a glance — a many-category distribution-shape complement to the
/// single-line renderKeyspaceChangeSlope.
pub fn renderCommandLatencyRidgeline(
    frame: *tui.Frame,
    area: tui.Rect,
    histograms: []const CommandLatencyHistogram,
) void {
    if (area.width == 0 or area.height == 0) return;

    const n = @min(histograms.len, tui.widgets.RidgelinePlot.MAX_SERIES);

    var series_buf: [tui.widgets.RidgelinePlot.MAX_SERIES]tui.widgets.RidgelineSeries = undefined;
    for (0..n) |i| {
        series_buf[i] = .{
            .label = histograms[i].command,
            .values = histograms[i].bucket_counts,
        };
    }

    const plot = tui.widgets.RidgelinePlot.init()
        .withSeries(series_buf[0..n])
        .withSharedScale(true)
        .withOverlap(1)
        .withReverse(false)
        .withLabelColumnWidth(10)
        .withFocused(0);

    plot.render(frame.buffer, area);
}

/// Per-command popularity rank sampled at successive time points, e.g. one
/// rank per periodic COMMAND STATS / INFO commandstats snapshot ordered by
/// call count (rank 1 = most-called command in that snapshot).
pub const CommandRankSnapshot = struct {
    command: []const u8 = "",
    /// Rank at each sampled time point (1-based, 1 = most popular).
    ranks: []const u32 = &.{},
};

/// Render BumpChart widget comparing per-command call-count rank over
/// successive sampling intervals using sailor v2.89.0. Each command draws a
/// polyline connecting its ranks across time points, with '/' and '\\'
/// glyphs marking rank improvement/decline between samples — a
/// leaderboard/standings-style complement to the distribution-shape
/// renderCommandLatencyRidgeline.
pub fn renderCommandPopularityBump(
    frame: *tui.Frame,
    area: tui.Rect,
    snapshots: []const CommandRankSnapshot,
    timepoint_labels: []const []const u8,
) void {
    if (area.width == 0 or area.height == 0) return;

    const n = @min(snapshots.len, tui.widgets.BumpChart.MAX_SERIES);

    var series_buf: [tui.widgets.BumpChart.MAX_SERIES]tui.widgets.BumpSeries = undefined;
    for (0..n) |i| {
        series_buf[i] = .{
            .label = snapshots[i].command,
            .ranks = snapshots[i].ranks,
        };
    }

    const chart = tui.widgets.BumpChart.init()
        .withSeries(series_buf[0..n])
        .withTimepointLabels(timepoint_labels)
        .withShowTimepointLabels(timepoint_labels.len > 0)
        .withShowLabels(true)
        .withFocused(0);

    chart.render(frame.buffer, area);
}

/// Per-database memory usage broken down by value type (bytes), one entry
/// per database. Used as the two-dimensional (database x type) input to
/// renderMemoryByTypeMosaic.
pub const DatabaseMemoryByType = struct {
    db_index: u16 = 0,
    string_bytes: f32 = 0,
    list_bytes: f32 = 0,
    hash_bytes: f32 = 0,
    set_bytes: f32 = 0,
    zset_bytes: f32 = 0,
};

/// Render MosaicPlot widget for two-dimensional memory-usage breakdown using
/// sailor v2.90.0. Column widths are proportional to each database's total
/// memory footprint; segment heights within a column are proportional to
/// that database's per-value-type memory share (STRING/LIST/HASH/SET/ZSET) —
/// letting an operator see both which database dominates memory usage and
/// which data type dominates within it in a single view, complementing the
/// ring-based renderSunburstChart (key counts) with an area-based encoding
/// of memory bytes.
pub fn renderMemoryByTypeMosaic(
    frame: *tui.Frame,
    area: tui.Rect,
    databases: []const DatabaseMemoryByType,
) void {
    if (area.width == 0 or area.height == 0) return;

    var segments_buf: [tui.widgets.MosaicPlot.MAX_COLUMNS][5]tui.widgets.MosaicSegment = undefined;
    var columns_buf: [tui.widgets.MosaicPlot.MAX_COLUMNS]tui.widgets.MosaicColumn = undefined;
    const n = @min(databases.len, tui.widgets.MosaicPlot.MAX_COLUMNS);

    var label_buf: [tui.widgets.MosaicPlot.MAX_COLUMNS][16]u8 = undefined;
    for (0..n) |i| {
        const db = databases[i];
        segments_buf[i] = [_]tui.widgets.MosaicSegment{
            .{ .label = "STR", .value = db.string_bytes, .style = tui.Style{ .fg = tui.Color.green } },
            .{ .label = "LIST", .value = db.list_bytes, .style = tui.Style{ .fg = tui.Color.cyan } },
            .{ .label = "HASH", .value = db.hash_bytes, .style = tui.Style{ .fg = tui.Color.yellow } },
            .{ .label = "SET", .value = db.set_bytes, .style = tui.Style{ .fg = tui.Color.magenta } },
            .{ .label = "ZSET", .value = db.zset_bytes, .style = tui.Style{ .fg = tui.Color.blue } },
        };

        const label = std.fmt.bufPrint(&label_buf[i], "db{d}", .{db.db_index}) catch "db?";
        columns_buf[i] = .{ .label = label, .segments = segments_buf[i][0..] };
    }

    const plot = tui.widgets.MosaicPlot.init()
        .withColumns(columns_buf[0..n])
        .withShowColumnLabels(true)
        .withShowSegmentLabels(true)
        .withFocusedColumn(0)
        .withFocusedSegment(0);

    plot.render(frame.buffer, area);
}

/// Per-command call count within a command category, e.g. one entry per
/// command reported by COMMAND STATS / INFO commandstats grouped under its
/// owning category (string/list/hash/set/zset/generic/...).
pub const CommandCallCount = struct {
    command: []const u8 = "",
    calls: u64 = 0,
};

/// A command category and its per-command call counts, e.g. "string" with
/// SET/GET/INCR call counts. Used as input to renderCommandStatsIcicle.
pub const CommandCategoryStats = struct {
    category: []const u8 = "",
    commands: []const CommandCallCount = &.{},
};

/// Render IcicleChart widget for a two-level command-statistics breakdown
/// using sailor v2.91.0. The root band spans the full width; the second
/// row divides it into category bands proportional to each category's
/// total call count; the third row divides each category band into
/// per-command bands proportional to that command's call count — a
/// rectangular drill-down complement to the ring-based renderSunburstChart
/// and the area-based renderMemoryByTypeMosaic, better suited to a
/// depth-first "which commands dominate this category" reading.
pub fn renderCommandStatsIcicle(
    frame: *tui.Frame,
    area: tui.Rect,
    categories: []const CommandCategoryStats,
) void {
    if (area.width == 0 or area.height == 0) return;

    const cat_n = @min(categories.len, tui.widgets.IcicleChart.MAX_CHILDREN_PER_NODE);

    var cmd_nodes_buf: [tui.widgets.IcicleChart.MAX_CHILDREN_PER_NODE][tui.widgets.IcicleChart.MAX_CHILDREN_PER_NODE]tui.widgets.IcicleNode = undefined;
    var cat_nodes: [tui.widgets.IcicleChart.MAX_CHILDREN_PER_NODE]tui.widgets.IcicleNode = undefined;

    var root_total: f32 = 0;
    for (0..cat_n) |i| {
        const cat = categories[i];
        const cmd_n = @min(cat.commands.len, tui.widgets.IcicleChart.MAX_CHILDREN_PER_NODE);

        var cat_total: f32 = 0;
        for (0..cmd_n) |j| {
            const calls_f: f32 = @floatFromInt(cat.commands[j].calls);
            cmd_nodes_buf[i][j] = .{ .label = cat.commands[j].command, .value = calls_f };
            cat_total += calls_f;
        }

        cat_nodes[i] = .{ .label = cat.category, .value = cat_total, .children = cmd_nodes_buf[i][0..cmd_n] };
        root_total += cat_total;
    }

    const root = tui.widgets.IcicleNode{ .label = "ALL", .value = root_total, .children = cat_nodes[0..cat_n] };

    const chart = tui.widgets.IcicleChart.init()
        .withRoot(root)
        .withShowLabels(true)
        .withShowValues(false)
        .withFocused(&.{});

    chart.render(frame.buffer, area);
}

/// A single boolean server setting (e.g. a CONFIG GET yes/no value) to be
/// displayed as a toggle switch, e.g. `appendonly`, `stop-writes-on-bgsave-error`,
/// `rdbcompression`. Used as input to renderServerFlagsPanel.
pub const ServerBooleanFlag = struct {
    name: []const u8 = "",
    enabled: bool = false,
};

/// Render a panel of server boolean CONFIG settings as ToggleSwitch rows
/// using sailor v2.92.0's ToggleSwitchGroup — a slider-style on/off complement
/// to the checkbox-style widgets used elsewhere, better suited to settings
/// that are inherently binary (on vs off) rather than multi-select. Each
/// flag renders as `[◯    ] name` when disabled or `[    ◉] name` when
/// enabled; the first flag is shown focused since this is a read-only status
/// snapshot rather than an interactive form.
pub fn renderServerFlagsPanel(
    frame: *tui.Frame,
    area: tui.Rect,
    flags: []const ServerBooleanFlag,
    buf: []tui.widgets.ToggleSwitch,
) void {
    if (area.width == 0 or area.height == 0) return;

    const n = @min(flags.len, buf.len);
    for (0..n) |i| {
        buf[i] = tui.widgets.ToggleSwitch.init(flags[i].name).withChecked(flags[i].enabled);
    }

    const group = tui.widgets.ToggleSwitchGroup.init(buf[0..n]).withHelp(false);
    group.render(frame.buffer, area);
}

/// Daily command activity count for a single day, e.g. total commands
/// processed or keys expired on that date. Used as input to
/// renderKeyActivityHeatmap, one entry per day starting at a given
/// start date (day 0 = start_date, day 1 = start_date + 1, ...).
pub const DailyActivityCount = struct {
    count: u64 = 0,
};

/// Render a CalendarHeatmap widget for daily command/expiry activity using
/// sailor v2.93.0's CalendarHeatmap — a GitHub-style contribution grid
/// mapping each day's activity count to a 5-level intensity glyph. Useful
/// for visualizing INFO commandstats or expired-keys history over a rolling
/// window (e.g. the last 90 days) at a glance. `start_date` anchors day 0 of
/// `daily_counts`; `focused_day` optionally highlights a single day index
/// (e.g. "today") within the grid.
pub fn renderKeyActivityHeatmap(
    frame: *tui.Frame,
    area: tui.Rect,
    start_date: tui.widgets.Calendar.Date,
    daily_counts: []const DailyActivityCount,
    focused_day: ?usize,
    values_buf: []f32,
) void {
    if (area.width == 0 or area.height == 0) return;

    const n = @min(daily_counts.len, values_buf.len, tui.widgets.CalendarHeatmap.MAX_ENTRIES);
    for (0..n) |i| {
        values_buf[i] = @floatFromInt(daily_counts[i].count);
    }

    const heatmap = tui.widgets.CalendarHeatmap.init(start_date)
        .withValues(values_buf[0..n])
        .withShowMonthLabels(true)
        .withShowWeekdayLabels(true)
        .withFocused(focused_day);

    heatmap.render(frame.buffer, area);
}

pub fn renderNotification(
    frame: *tui.Frame,
    area: tui.Rect,
    text: []const u8,
) !void {
    // Bottom-right corner notification
    const notif_width: u16 = @min(@as(u16, @intCast(text.len)) + 4, area.width);
    const notif_height: u16 = 3;
    const notif_x = area.width -| notif_width;
    const notif_y = area.height -| notif_height;

    // Background
    const bg_style = tui.Style{ .bg = tui.Color.blue, .fg = tui.Color.white };

    var i: u16 = 0;
    while (i < notif_width) : (i += 1) {
        var j: u16 = 0;
        while (j < notif_height) : (j += 1) {
            frame.setString(notif_x + i, notif_y + j, " ", bg_style);
        }
    }

    // Text
    const text_truncated = if (text.len > notif_width - 4) text[0 .. notif_width - 4] else text;
    frame.setString(notif_x + 2, notif_y + 1, text_truncated, bg_style);
}
