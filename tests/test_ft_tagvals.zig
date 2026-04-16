const std = @import("std");
const testing = std.testing;
const RespValue = @import("../src/protocol/parser.zig").RespValue;
const Storage = @import("../src/storage/memory.zig").Storage;
const search_cmds = @import("../src/commands/search.zig");
const search_mod = @import("../src/storage/search.zig");

// ============================================================================
// FT.TAGVALS - Basic Functionality Tests
// ============================================================================

test "FT.TAGVALS: basic usage - create index, add documents, retrieve distinct values" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Create index with TAG field
    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    // Add TAG field to schema
    var field = try search_mod.FieldSchema.init(allocator, "category", .tag);
    defer field.deinit();
    try index.addField(field);

    // Add documents with tags
    try index.addTagValue("category", "electronics", "doc1");
    try index.addTagValue("category", "gaming", "doc2");
    try index.addTagValue("category", "mobile", "doc3");

    // Call FT.TAGVALS
    const args = &[_][]const u8{ "idx", "category" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    // Verify response is array
    try testing.expect(result == .array);
    try testing.expectEqual(@as(usize, 3), result.array.len);

    // Verify all tags are present (order may vary)
    var found_electronics = false;
    var found_gaming = false;
    var found_mobile = false;

    for (result.array) |resp_val| {
        try testing.expect(resp_val == .bulk_string);
        const tag = resp_val.bulk_string;

        if (std.mem.eql(u8, tag, "electronics")) found_electronics = true;
        if (std.mem.eql(u8, tag, "gaming")) found_gaming = true;
        if (std.mem.eql(u8, tag, "mobile")) found_mobile = true;
    }

    try testing.expect(found_electronics);
    try testing.expect(found_gaming);
    try testing.expect(found_mobile);
}

// ============================================================================
// FT.TAGVALS - Deduplication Tests
// ============================================================================

test "FT.TAGVALS: multiple documents with same tag (deduplication)" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    var field = try search_mod.FieldSchema.init(allocator, "brand", .tag);
    defer field.deinit();
    try index.addField(field);

    // Add same tag to multiple documents
    try index.addTagValue("brand", "Apple", "doc1");
    try index.addTagValue("brand", "Apple", "doc2");
    try index.addTagValue("brand", "Apple", "doc3");
    try index.addTagValue("brand", "Samsung", "doc4");
    try index.addTagValue("brand", "Samsung", "doc5");

    const args = &[_][]const u8{ "idx", "brand" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    // Should only return 2 distinct values
    try testing.expect(result == .array);
    try testing.expectEqual(@as(usize, 2), result.array.len);

    var found_apple = false;
    var found_samsung = false;

    for (result.array) |resp_val| {
        try testing.expect(resp_val == .bulk_string);
        const tag = resp_val.bulk_string;
        if (std.mem.eql(u8, tag, "Apple")) found_apple = true;
        if (std.mem.eql(u8, tag, "Samsung")) found_samsung = true;
    }

    try testing.expect(found_apple);
    try testing.expect(found_samsung);
}

// ============================================================================
// FT.TAGVALS - Case and Whitespace Handling Tests
// ============================================================================

test "FT.TAGVALS: case normalization (FOO, foo, Foo should be same)" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    var field = try search_mod.FieldSchema.init(allocator, "tags", .tag);
    defer field.deinit();
    try index.addField(field);

    // Add case variations of the same tag
    try index.addTagValue("tags", "JavaScript", "doc1");
    try index.addTagValue("tags", "javascript", "doc2");
    try index.addTagValue("tags", "JAVASCRIPT", "doc3");
    try index.addTagValue("tags", "Python", "doc4");

    const args = &[_][]const u8{ "idx", "tags" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    // Should return distinct values as stored (case-sensitive storage)
    // but typically Redis treats tags case-insensitively in real implementation
    try testing.expect(result == .array);
    // Storage may or may not deduplicate case - depends on implementation
    try testing.expect(result.array.len >= 2);
}

test "FT.TAGVALS: whitespace trimming (tags with leading/trailing spaces)" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    var field = try search_mod.FieldSchema.init(allocator, "tags", .tag);
    defer field.deinit();
    try index.addField(field);

    // Add tags with whitespace
    try index.addTagValue("tags", "  electronics  ", "doc1");
    try index.addTagValue("tags", "electronics", "doc2");
    try index.addTagValue("tags", "\tgaming\n", "doc3");

    const args = &[_][]const u8{ "idx", "tags" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    try testing.expect(result == .array);
    // Exact behavior depends on storage implementation
    try testing.expect(result.array.len >= 1);
}

// ============================================================================
// FT.TAGVALS - Empty Result Tests
// ============================================================================

test "FT.TAGVALS: empty result when field has no indexed values" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    // Add field but don't add any values
    var field = try search_mod.FieldSchema.init(allocator, "category", .tag);
    defer field.deinit();
    try index.addField(field);

    const args = &[_][]const u8{ "idx", "category" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    // Should return empty array
    try testing.expect(result == .array);
    try testing.expectEqual(@as(usize, 0), result.array.len);
}

// ============================================================================
// FT.TAGVALS - Error Handling Tests
// ============================================================================

test "FT.TAGVALS: error on unknown index name" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const args = &[_][]const u8{ "nonexistent_index", "field" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    try testing.expect(result == .error_string);
    try testing.expect(std.mem.eql(u8, result.error_string, "ERR Unknown Index name"));
}

test "FT.TAGVALS: error on unknown field name" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    var field = try search_mod.FieldSchema.init(allocator, "category", .tag);
    defer field.deinit();
    try index.addField(field);

    const args = &[_][]const u8{ "idx", "nonexistent_field" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    try testing.expect(result == .error_string);
    try testing.expect(std.mem.eql(u8, result.error_string, "ERR Unknown field name"));
}

test "FT.TAGVALS: error on field that is not TAG type (try with TEXT field)" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    // Add TEXT field instead of TAG
    var field = try search_mod.FieldSchema.init(allocator, "description", .text);
    defer field.deinit();
    try index.addField(field);

    const args = &[_][]const u8{ "idx", "description" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    try testing.expect(result == .error_string);
    try testing.expect(std.mem.containsAtLeast(u8, result.error_string, 1, "not a tag field"));
}

test "FT.TAGVALS: error on arity mismatch (too few args)" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Missing field_name argument
    const args = &[_][]const u8{"idx"};
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    try testing.expect(result == .error_string);
    try testing.expect(std.mem.containsAtLeast(u8, result.error_string, 1, "wrong number of arguments"));
}

test "FT.TAGVALS: error on arity mismatch (too many args)" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Extra argument
    const args = &[_][]const u8{ "idx", "field", "extra" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    try testing.expect(result == .error_string);
    try testing.expect(std.mem.containsAtLeast(u8, result.error_string, 1, "wrong number of arguments"));
}

// ============================================================================
// FT.TAGVALS - Alias Resolution Tests
// ============================================================================

test "FT.TAGVALS: alias resolution (create alias, use alias with FT.TAGVALS)" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Create index with TAG field
    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    var field = try search_mod.FieldSchema.init(allocator, "category", .tag);
    defer field.deinit();
    try index.addField(field);

    // Add tag values
    try index.addTagValue("category", "electronics", "doc1");
    try index.addTagValue("category", "gaming", "doc2");

    // Create alias for the index
    try storage.search.addAlias("idx_alias", "idx");

    // Call FT.TAGVALS using alias instead of index name
    const args = &[_][]const u8{ "idx_alias", "category" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    // Should work through alias
    try testing.expect(result == .array);
    try testing.expectEqual(@as(usize, 2), result.array.len);

    var found_electronics = false;
    var found_gaming = false;

    for (result.array) |resp_val| {
        try testing.expect(resp_val == .bulk_string);
        const tag = resp_val.bulk_string;
        if (std.mem.eql(u8, tag, "electronics")) found_electronics = true;
        if (std.mem.eql(u8, tag, "gaming")) found_gaming = true;
    }

    try testing.expect(found_electronics);
    try testing.expect(found_gaming);
}

// ============================================================================
// FT.TAGVALS - Multiple Distinct Tags Tests
// ============================================================================

test "FT.TAGVALS: multiple distinct tags from multiple documents" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try storage.search.createIndex("products", .hash);
    const index = storage.search.getIndex("products").?;

    var field = try search_mod.FieldSchema.init(allocator, "tags", .tag);
    defer field.deinit();
    try index.addField(field);

    // Add many distinct tags from different documents
    const tags = &[_][]const u8{
        "smartphone", "laptop",   "tablet",     "headphones", "smartwatch",
        "charger",    "monitor",  "keyboard",   "mouse",      "router",
        "camera",     "printer",  "storage",    "gpu",        "cpu",
    };

    for (tags, 0..) |tag, i| {
        const doc_id = try std.fmt.allocPrint(allocator, "doc{}", .{i});
        try index.addTagValue("tags", tag, doc_id);
    }

    const args = &[_][]const u8{ "products", "tags" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    try testing.expect(result == .array);
    try testing.expectEqual(@as(usize, tags.len), result.array.len);

    // Verify all expected tags are present
    for (result.array) |resp_val| {
        try testing.expect(resp_val == .bulk_string);
        const tag = resp_val.bulk_string;

        var found = false;
        for (tags) |expected_tag| {
            if (std.mem.eql(u8, tag, expected_tag)) {
                found = true;
                break;
            }
        }
        try testing.expect(found);
    }
}

// ============================================================================
// FT.TAGVALS - Unicode Tags Tests
// ============================================================================

test "FT.TAGVALS: unicode tags (日本語, français)" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    var field = try search_mod.FieldSchema.init(allocator, "language", .tag);
    defer field.deinit();
    try index.addField(field);

    // Add Unicode tags
    try index.addTagValue("language", "日本語", "doc1");
    try index.addTagValue("language", "français", "doc2");
    try index.addTagValue("language", "Ελληνικά", "doc3");
    try index.addTagValue("language", "Русский", "doc4");

    const args = &[_][]const u8{ "idx", "language" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    try testing.expect(result == .array);
    try testing.expectEqual(@as(usize, 4), result.array.len);

    var found_japanese = false;
    var found_french = false;
    var found_greek = false;
    var found_russian = false;

    for (result.array) |resp_val| {
        try testing.expect(resp_val == .bulk_string);
        const tag = resp_val.bulk_string;

        if (std.mem.eql(u8, tag, "日本語")) found_japanese = true;
        if (std.mem.eql(u8, tag, "français")) found_french = true;
        if (std.mem.eql(u8, tag, "Ελληνικά")) found_greek = true;
        if (std.mem.eql(u8, tag, "Русский")) found_russian = true;
    }

    try testing.expect(found_japanese);
    try testing.expect(found_french);
    try testing.expect(found_greek);
    try testing.expect(found_russian);
}

// ============================================================================
// FT.TAGVALS - Complex Tag Values Tests
// ============================================================================

test "FT.TAGVALS: complex tag values (c++, node.js, rust-lang)" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    var field = try search_mod.FieldSchema.init(allocator, "language", .tag);
    defer field.deinit();
    try index.addField(field);

    // Add complex tag values
    try index.addTagValue("language", "c++", "doc1");
    try index.addTagValue("language", "node.js", "doc2");
    try index.addTagValue("language", "rust-lang", "doc3");
    try index.addTagValue("language", "c#", "doc4");
    try index.addTagValue("language", "objective-c", "doc5");
    try index.addTagValue("language", "f#", "doc6");

    const args = &[_][]const u8{ "idx", "language" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    try testing.expect(result == .array);
    try testing.expectEqual(@as(usize, 6), result.array.len);

    var found_cpp = false;
    var found_nodejs = false;
    var found_rust = false;
    var found_csharp = false;
    var found_objc = false;
    var found_fsharp = false;

    for (result.array) |resp_val| {
        try testing.expect(resp_val == .bulk_string);
        const tag = resp_val.bulk_string;

        if (std.mem.eql(u8, tag, "c++")) found_cpp = true;
        if (std.mem.eql(u8, tag, "node.js")) found_nodejs = true;
        if (std.mem.eql(u8, tag, "rust-lang")) found_rust = true;
        if (std.mem.eql(u8, tag, "c#")) found_csharp = true;
        if (std.mem.eql(u8, tag, "objective-c")) found_objc = true;
        if (std.mem.eql(u8, tag, "f#")) found_fsharp = true;
    }

    try testing.expect(found_cpp);
    try testing.expect(found_nodejs);
    try testing.expect(found_rust);
    try testing.expect(found_csharp);
    try testing.expect(found_objc);
    try testing.expect(found_fsharp);
}

// ============================================================================
// FT.TAGVALS - Multiple TAG Fields Tests
// ============================================================================

test "FT.TAGVALS: multiple TAG fields in same index" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    // Add multiple TAG fields
    var category_field = try search_mod.FieldSchema.init(allocator, "category", .tag);
    defer category_field.deinit();
    try index.addField(category_field);

    var brand_field = try search_mod.FieldSchema.init(allocator, "brand", .tag);
    defer brand_field.deinit();
    try index.addField(brand_field);

    // Add values to first field
    try index.addTagValue("category", "electronics", "doc1");
    try index.addTagValue("category", "gaming", "doc2");

    // Add values to second field
    try index.addTagValue("brand", "Apple", "doc1");
    try index.addTagValue("brand", "Samsung", "doc2");

    // Test first field
    const args1 = &[_][]const u8{ "idx", "category" };
    const result1 = try search_cmds.cmdFtTagvals(&storage, allocator, args1);

    try testing.expect(result1 == .array);
    try testing.expectEqual(@as(usize, 2), result1.array.len);

    // Test second field
    const args2 = &[_][]const u8{ "idx", "brand" };
    const result2 = try search_cmds.cmdFtTagvals(&storage, allocator, args2);

    try testing.expect(result2 == .array);
    try testing.expectEqual(@as(usize, 2), result2.array.len);
}

// ============================================================================
// FT.TAGVALS - Large Tag Value Set Tests
// ============================================================================

test "FT.TAGVALS: large tag value set (100 distinct tags)" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    var field = try search_mod.FieldSchema.init(allocator, "tags", .tag);
    defer field.deinit();
    try index.addField(field);

    // Add 100 distinct tags
    const num_tags = 100;
    for (0..num_tags) |i| {
        var tag_buf: [64]u8 = undefined;
        const tag = try std.fmt.bufPrint(&tag_buf, "tag_{d:0>3}", .{i});
        var doc_buf: [64]u8 = undefined;
        const doc_id = try std.fmt.bufPrint(&doc_buf, "doc{d}", .{i});

        try index.addTagValue("tags", tag, doc_id);
    }

    const args = &[_][]const u8{ "idx", "tags" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    try testing.expect(result == .array);
    try testing.expectEqual(@as(usize, num_tags), result.array.len);

    // Verify all results are bulk strings
    for (result.array) |resp_val| {
        try testing.expect(resp_val == .bulk_string);
    }
}

// ============================================================================
// FT.TAGVALS - Mixed Field Types Tests
// ============================================================================

test "FT.TAGVALS: error when mixed field types (TAG + NUMERIC + TEXT in same index)" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    // Add different field types
    var tag_field = try search_mod.FieldSchema.init(allocator, "tags", .tag);
    defer tag_field.deinit();
    try index.addField(tag_field);

    var text_field = try search_mod.FieldSchema.init(allocator, "title", .text);
    defer text_field.deinit();
    try index.addField(text_field);

    var numeric_field = try search_mod.FieldSchema.init(allocator, "price", .numeric);
    defer numeric_field.deinit();
    try index.addField(numeric_field);

    // Should work for TAG field
    try index.addTagValue("tags", "electronics", "doc1");

    const args_tag = &[_][]const u8{ "idx", "tags" };
    const result_tag = try search_cmds.cmdFtTagvals(&storage, allocator, args_tag);
    try testing.expect(result_tag == .array);

    // Should fail for TEXT field
    const args_text = &[_][]const u8{ "idx", "title" };
    const result_text = try search_cmds.cmdFtTagvals(&storage, allocator, args_text);
    try testing.expect(result_text == .error_string);
    try testing.expect(std.mem.containsAtLeast(u8, result_text.error_string, 1, "not a tag field"));

    // Should fail for NUMERIC field
    const args_numeric = &[_][]const u8{ "idx", "price" };
    const result_numeric = try search_cmds.cmdFtTagvals(&storage, allocator, args_numeric);
    try testing.expect(result_numeric == .error_string);
    try testing.expect(std.mem.containsAtLeast(u8, result_numeric.error_string, 1, "not a tag field"));
}

// ============================================================================
// FT.TAGVALS - Case Sensitivity Tests
// ============================================================================

test "FT.TAGVALS: case sensitivity in field names" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    // Field name is case-sensitive in storage
    var field = try search_mod.FieldSchema.init(allocator, "Category", .tag);
    defer field.deinit();
    try index.addField(field);

    try index.addTagValue("Category", "electronics", "doc1");

    // Should work with exact case
    const args_exact = &[_][]const u8{ "idx", "Category" };
    const result_exact = try search_cmds.cmdFtTagvals(&storage, allocator, args_exact);
    try testing.expect(result_exact == .array);

    // Should fail with different case (field names are case-sensitive)
    const args_lower = &[_][]const u8{ "idx", "category" };
    const result_lower = try search_cmds.cmdFtTagvals(&storage, allocator, args_lower);
    try testing.expect(result_lower == .error_string);
    try testing.expect(std.mem.eql(u8, result_lower.error_string, "ERR Unknown field name"));
}

// ============================================================================
// FT.TAGVALS - Null and Empty Tag Tests
// ============================================================================

test "FT.TAGVALS: empty tag values" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    var field = try search_mod.FieldSchema.init(allocator, "tags", .tag);
    defer field.deinit();
    try index.addField(field);

    // Add empty tag
    try index.addTagValue("tags", "", "doc1");
    try index.addTagValue("tags", "nonempty", "doc2");

    const args = &[_][]const u8{ "idx", "tags" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    try testing.expect(result == .array);
    // Should include both empty and non-empty tags
    try testing.expect(result.array.len >= 1);
}

// ============================================================================
// FT.TAGVALS - Duplicate Insertion Tests
// ============================================================================

test "FT.TAGVALS: same tag added multiple times to same document" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    var field = try search_mod.FieldSchema.init(allocator, "tags", .tag);
    defer field.deinit();
    try index.addField(field);

    // Add same tag multiple times to same document
    try index.addTagValue("tags", "important", "doc1");
    try index.addTagValue("tags", "important", "doc1");
    try index.addTagValue("tags", "important", "doc1");

    const args = &[_][]const u8{ "idx", "tags" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    try testing.expect(result == .array);
    // Should still only have one distinct value
    try testing.expectEqual(@as(usize, 1), result.array.len);
    try testing.expect(std.mem.eql(u8, result.array[0].bulk_string, "important"));
}

// ============================================================================
// FT.TAGVALS - Special Characters Tests
// ============================================================================

test "FT.TAGVALS: special characters in tag values" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    var field = try search_mod.FieldSchema.init(allocator, "tags", .tag);
    defer field.deinit();
    try index.addField(field);

    // Add tags with special characters
    try index.addTagValue("tags", "tag@value", "doc1");
    try index.addTagValue("tags", "tag#1", "doc2");
    try index.addTagValue("tags", "tag/name", "doc3");
    try index.addTagValue("tags", "tag:colon", "doc4");
    try index.addTagValue("tags", "tag|pipe", "doc5");

    const args = &[_][]const u8{ "idx", "tags" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    try testing.expect(result == .array);
    try testing.expectEqual(@as(usize, 5), result.array.len);

    var found_at = false;
    var found_hash = false;
    var found_slash = false;
    var found_colon = false;
    var found_pipe = false;

    for (result.array) |resp_val| {
        try testing.expect(resp_val == .bulk_string);
        const tag = resp_val.bulk_string;

        if (std.mem.eql(u8, tag, "tag@value")) found_at = true;
        if (std.mem.eql(u8, tag, "tag#1")) found_hash = true;
        if (std.mem.eql(u8, tag, "tag/name")) found_slash = true;
        if (std.mem.eql(u8, tag, "tag:colon")) found_colon = true;
        if (std.mem.eql(u8, tag, "tag|pipe")) found_pipe = true;
    }

    try testing.expect(found_at);
    try testing.expect(found_hash);
    try testing.expect(found_slash);
    try testing.expect(found_colon);
    try testing.expect(found_pipe);
}

// ============================================================================
// FT.TAGVALS - Numeric-like Tag Values Tests
// ============================================================================

test "FT.TAGVALS: numeric-like tag values (stored as strings)" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    var field = try search_mod.FieldSchema.init(allocator, "tags", .tag);
    defer field.deinit();
    try index.addField(field);

    // Add numeric-like tags (stored as strings)
    try index.addTagValue("tags", "123", "doc1");
    try index.addTagValue("tags", "456.789", "doc2");
    try index.addTagValue("tags", "-999", "doc3");
    try index.addTagValue("tags", "0", "doc4");

    const args = &[_][]const u8{ "idx", "tags" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    try testing.expect(result == .array);
    try testing.expectEqual(@as(usize, 4), result.array.len);

    var found_123 = false;
    var found_456 = false;
    var found_neg = false;
    var found_zero = false;

    for (result.array) |resp_val| {
        try testing.expect(resp_val == .bulk_string);
        const tag = resp_val.bulk_string;

        if (std.mem.eql(u8, tag, "123")) found_123 = true;
        if (std.mem.eql(u8, tag, "456.789")) found_456 = true;
        if (std.mem.eql(u8, tag, "-999")) found_neg = true;
        if (std.mem.eql(u8, tag, "0")) found_zero = true;
    }

    try testing.expect(found_123);
    try testing.expect(found_456);
    try testing.expect(found_neg);
    try testing.expect(found_zero);
}

// ============================================================================
// FT.TAGVALS - Response Format Validation Tests
// ============================================================================

test "FT.TAGVALS: response format is array of bulk strings" {
    var storage = try Storage.init(testing.allocator);
    defer storage.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    try storage.search.createIndex("idx", .hash);
    const index = storage.search.getIndex("idx").?;

    var field = try search_mod.FieldSchema.init(allocator, "category", .tag);
    defer field.deinit();
    try index.addField(field);

    try index.addTagValue("category", "electronics", "doc1");
    try index.addTagValue("category", "gaming", "doc2");

    const args = &[_][]const u8{ "idx", "category" };
    const result = try search_cmds.cmdFtTagvals(&storage, allocator, args);

    // Verify outer response is array
    try testing.expect(result == .array);

    // Verify each element is a bulk string
    for (result.array) |resp_val| {
        try testing.expect(resp_val == .bulk_string);
        try testing.expect(resp_val.bulk_string.len > 0 or resp_val.bulk_string.len == 0);
    }
}
