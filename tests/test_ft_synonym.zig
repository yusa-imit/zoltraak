const std = @import("std");
const Server = @import("../src/server.zig").Server;
const Parser = @import("../src/protocol/parser.zig").Parser;
const Writer = @import("../src/protocol/writer.zig").Writer;

test "FT.SYNUPDATE and FT.SYNDUMP basic flow" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 6379, .{});
    defer server.deinit();

    // Create index
    {
        const input = "*3\r\n$9\r\nFT.CREATE\r\n$6\r\nmyidx1\r\n$6\r\nSCHEMA\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer {
            var w = Writer.init(allocator);
            defer w.deinit();
            allocator.free(response);
        }

        try std.testing.expect(std.mem.indexOf(u8, response, "+OK") != null);
    }

    // Add synonym group 1 with terms: "hello", "hi", "hey"
    {
        const input = "*6\r\n$11\r\nFT.SYNUPDATE\r\n$6\r\nmyidx1\r\n$1\r\n1\r\n$5\r\nhello\r\n$2\r\nhi\r\n$3\r\nhey\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer {
            var w = Writer.init(allocator);
            defer w.deinit();
            allocator.free(response);
        }

        try std.testing.expect(std.mem.indexOf(u8, response, "+OK") != null);
    }

    // Add synonym group 2 with terms: "good", "great"
    {
        const input = "*5\r\n$11\r\nFT.SYNUPDATE\r\n$6\r\nmyidx1\r\n$1\r\n2\r\n$4\r\ngood\r\n$5\r\ngreat\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer {
            var w = Writer.init(allocator);
            defer w.deinit();
            allocator.free(response);
        }

        try std.testing.expect(std.mem.indexOf(u8, response, "+OK") != null);
    }

    // Dump synonyms
    {
        const input = "*2\r\n$10\r\nFT.SYNDUMP\r\n$6\r\nmyidx1\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer {
            var w = Writer.init(allocator);
            defer w.deinit();
            allocator.free(response);
        }

        // Response should be: *4\r\n$1\r\n1\r\n*3\r\n...\r\n$1\r\n2\r\n*2\r\n...
        try std.testing.expect(std.mem.indexOf(u8, response, "$1\r\n1\r\n") != null); // group id 1
        try std.testing.expect(std.mem.indexOf(u8, response, "$1\r\n2\r\n") != null); // group id 2
        try std.testing.expect(std.mem.indexOf(u8, response, "$5\r\nhello") != null);
        try std.testing.expect(std.mem.indexOf(u8, response, "$2\r\nhi") != null);
        try std.testing.expect(std.mem.indexOf(u8, response, "$3\r\nhey") != null);
        try std.testing.expect(std.mem.indexOf(u8, response, "$4\r\ngood") != null);
        try std.testing.expect(std.mem.indexOf(u8, response, "$5\r\ngreat") != null);
    }
}

test "FT.SYNUPDATE replaces existing group" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 6379, .{});
    defer server.deinit();

    // Create index
    {
        const input = "*3\r\n$9\r\nFT.CREATE\r\n$6\r\nmyidx2\r\n$6\r\nSCHEMA\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer {
            var w = Writer.init(allocator);
            defer w.deinit();
            allocator.free(response);
        }

        try std.testing.expect(std.mem.indexOf(u8, response, "+OK") != null);
    }

    // Add synonym group
    {
        const input = "*5\r\n$11\r\nFT.SYNUPDATE\r\n$6\r\nmyidx2\r\n$1\r\n1\r\n$3\r\nold\r\n$4\r\nterm\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "+OK") != null);
    }

    // Replace group 1 with new terms
    {
        const input = "*5\r\n$11\r\nFT.SYNUPDATE\r\n$6\r\nmyidx2\r\n$1\r\n1\r\n$3\r\nnew\r\n$5\r\nterms\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "+OK") != null);
    }

    // Dump should only show new terms
    {
        const input = "*2\r\n$10\r\nFT.SYNDUMP\r\n$6\r\nmyidx2\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "$3\r\nnew") != null);
        try std.testing.expect(std.mem.indexOf(u8, response, "$5\r\nterms") != null);
        try std.testing.expect(std.mem.indexOf(u8, response, "old") == null);
        try std.testing.expect(std.mem.indexOf(u8, response, "term") == null);
    }
}

test "FT.SYNUPDATE with SKIPINITIALSCAN flag" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 6379, .{});
    defer server.deinit();

    // Create index
    {
        const input = "*3\r\n$9\r\nFT.CREATE\r\n$6\r\nmyidx3\r\n$6\r\nSCHEMA\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "+OK") != null);
    }

    // Add synonym with SKIPINITIALSCAN flag
    {
        const input = "*6\r\n$11\r\nFT.SYNUPDATE\r\n$6\r\nmyidx3\r\n$1\r\n1\r\n$16\r\nSKIPINITIALSCAN\r\n$4\r\nfast\r\n$5\r\nquick\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "+OK") != null);
    }

    // Dump should show the terms
    {
        const input = "*2\r\n$10\r\nFT.SYNDUMP\r\n$6\r\nmyidx3\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "$4\r\nfast") != null);
        try std.testing.expect(std.mem.indexOf(u8, response, "$5\r\nquick") != null);
    }
}

test "FT.SYNDUMP on nonexistent index" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 6379, .{});
    defer server.deinit();

    const input = "*2\r\n$10\r\nFT.SYNDUMP\r\n$12\r\nnonexistent1\r\n";
    var parser = Parser.init(allocator);
    defer parser.deinit();
    const result = try parser.parse(input);
    defer parser.reset();

    const response = try server.handleCommand(allocator, result.value.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "-ERR Unknown Index name") != null);
}

test "FT.SYNUPDATE on nonexistent index" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 6379, .{});
    defer server.deinit();

    const input = "*5\r\n$11\r\nFT.SYNUPDATE\r\n$12\r\nnonexistent2\r\n$1\r\n1\r\n$4\r\nterm\r\n$5\r\nother\r\n";
    var parser = Parser.init(allocator);
    defer parser.deinit();
    const result = try parser.parse(input);
    defer parser.reset();

    const response = try server.handleCommand(allocator, result.value.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "-ERR Unknown Index name") != null);
}

test "FT.SYNUPDATE with invalid group id" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 6379, .{});
    defer server.deinit();

    // Create index
    {
        const input = "*3\r\n$9\r\nFT.CREATE\r\n$6\r\nmyidx4\r\n$6\r\nSCHEMA\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "+OK") != null);
    }

    // Try to add with invalid group id
    {
        const input = "*5\r\n$11\r\nFT.SYNUPDATE\r\n$6\r\nmyidx4\r\n$3\r\nabc\r\n$4\r\nterm\r\n$5\r\nother\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "-ERR invalid synonym group id") != null);
    }
}

test "FT.SYNDUMP on empty index" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 6379, .{});
    defer server.deinit();

    // Create index
    {
        const input = "*3\r\n$9\r\nFT.CREATE\r\n$6\r\nmyidx5\r\n$6\r\nSCHEMA\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "+OK") != null);
    }

    // Dump should return empty array
    {
        const input = "*2\r\n$10\r\nFT.SYNDUMP\r\n$6\r\nmyidx5\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "*0\r\n") != null);
    }
}

test "FT.SYNUPDATE arity errors" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 6379, .{});
    defer server.deinit();

    // Too few arguments
    {
        const input = "*2\r\n$11\r\nFT.SYNUPDATE\r\n$5\r\nindex\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "-ERR wrong number of arguments") != null);
    }

    // No terms after SKIPINITIALSCAN
    {
        const input = "*4\r\n$11\r\nFT.SYNUPDATE\r\n$5\r\nindex\r\n$1\r\n1\r\n$16\r\nSKIPINITIALSCAN\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "-ERR wrong number of arguments") != null);
    }
}

test "FT.SYNDUMP arity error" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 6379, .{});
    defer server.deinit();

    const input = "*1\r\n$10\r\nFT.SYNDUMP\r\n";
    var parser = Parser.init(allocator);
    defer parser.deinit();
    const result = try parser.parse(input);
    defer parser.reset();

    const response = try server.handleCommand(allocator, result.value.array);
    defer allocator.free(response);

    try std.testing.expect(std.mem.indexOf(u8, response, "-ERR wrong number of arguments") != null);
}

test "FT.SYNUPDATE with alias" {
    const allocator = std.testing.allocator;

    var server = try Server.init(allocator, "127.0.0.1", 6379, .{});
    defer server.deinit();

    // Create index
    {
        const input = "*3\r\n$9\r\nFT.CREATE\r\n$6\r\nmyidx6\r\n$6\r\nSCHEMA\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "+OK") != null);
    }

    // Create alias
    {
        const input = "*3\r\n$11\r\nFT.ALIASADD\r\n$7\r\nmyalias\r\n$6\r\nmyidx6\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "+OK") != null);
    }

    // Add synonym via alias
    {
        const input = "*5\r\n$11\r\nFT.SYNUPDATE\r\n$7\r\nmyalias\r\n$1\r\n1\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "+OK") != null);
    }

    // Dump via alias
    {
        const input = "*2\r\n$10\r\nFT.SYNDUMP\r\n$7\r\nmyalias\r\n";
        var parser = Parser.init(allocator);
        defer parser.deinit();
        const result = try parser.parse(input);
        defer parser.reset();

        const response = try server.handleCommand(allocator, result.value.array);
        defer allocator.free(response);

        try std.testing.expect(std.mem.indexOf(u8, response, "$3\r\nfoo") != null);
        try std.testing.expect(std.mem.indexOf(u8, response, "$3\r\nbar") != null);
    }
}
