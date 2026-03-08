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
