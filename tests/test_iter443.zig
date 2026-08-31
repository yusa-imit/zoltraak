const std = @import("std");
const zoltraak = @import("zoltraak");
const storage_mod = zoltraak.storage;
const cuckoo_mod = zoltraak.cuckoo_storage;

const Storage = storage_mod.Storage;

// Iteration 443: Storage.deinit() freed the LoadContext for an interrupted
// CF.LOADCHUNK sequence (cuckoo_load_contexts) but never freed the duped key
// string used as the map key, leaking it whenever a chunked restore is left
// incomplete at shutdown. The sibling bloom_load_contexts map already frees
// both the key and the value correctly; this test drives the same shape of
// leak directly against the map (the way CF.LOADCHUNK populates it) so
// std.testing.allocator's leak detector catches the missing free.
test "Storage.deinit frees cuckoo_load_contexts key on interrupted CF.LOADCHUNK" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    const ctx = try storage.allocator.create(cuckoo_mod.CuckooFilterValue.LoadContext);
    ctx.* = .{
        .allocator = storage.allocator,
        .buffer = try std.ArrayList(u8).initCapacity(storage.allocator, 8),
        .expected_iterator = 0,
    };
    const ctx_key = try storage.allocator.dupe(u8, "interrupted-cuckoo-key");
    try storage.cuckoo_load_contexts.put(ctx_key, ctx);

    // storage.deinit() (deferred above) must free both the LoadContext and
    // the duped key string, or std.testing.allocator reports a leak.
}
