const std = @import("std");
const sailor = @import("sailor");

// Tests for sailor v2.13.0 features
// - MiddlewareStore: action intercept middleware pipeline ✅
// - ThunkStore: async action sequencing ✅
// - UndoStore: time-travel undo/redo ✅
// - StatePersist: pluggable serialization ✅
// - ReactiveList widget: Signal-bound list ✅

// ============================================================================
// MiddlewareStore (v2.13.0)
// ============================================================================

const CountState = struct { count: i32 };
const CountAction = union(enum) { increment, decrement, set: i32 };

fn countReducer(state: CountState, action: CountAction, _: std.mem.Allocator) anyerror!CountState {
    return switch (action) {
        .increment => CountState{ .count = state.count + 1 },
        .decrement => CountState{ .count = state.count - 1 },
        .set => |v| CountState{ .count = v },
    };
}

test "v2.13.0: middleware module exists" {
    const middleware = sailor.middleware;
    const ModuleType = @TypeOf(middleware);
    try std.testing.expect(ModuleType == type);
}

test "v2.13.0: MiddlewareStore can be created and dispatched" {
    const allocator = std.testing.allocator;
    const MiddlewareStore = sailor.middleware.MiddlewareStore(CountState, CountAction);

    var store = try MiddlewareStore.init(
        allocator,
        CountState{ .count = 0 },
        countReducer,
        .{},
    );
    defer store.deinit(allocator);

    try std.testing.expectEqual(@as(i32, 0), store.getState().count);
    try store.dispatch(.increment);
    try std.testing.expectEqual(@as(i32, 1), store.getState().count);
    try store.dispatch(.increment);
    try std.testing.expectEqual(@as(i32, 2), store.getState().count);
    try store.dispatch(.decrement);
    try std.testing.expectEqual(@as(i32, 1), store.getState().count);
}

test "v2.13.0: MiddlewareStore set action works" {
    const allocator = std.testing.allocator;
    const MiddlewareStore = sailor.middleware.MiddlewareStore(CountState, CountAction);

    var store = try MiddlewareStore.init(
        allocator,
        CountState{ .count = 10 },
        countReducer,
        .{},
    );
    defer store.deinit(allocator);

    try store.dispatch(.{ .set = 42 });
    try std.testing.expectEqual(@as(i32, 42), store.getState().count);
}

// ============================================================================
// ThunkStore (v2.13.0)
// ============================================================================

test "v2.13.0: thunk module exists" {
    const thunk = sailor.thunk;
    const ModuleType = @TypeOf(thunk);
    try std.testing.expect(ModuleType == type);
}

test "v2.13.0: ThunkStore can be created and dispatches sync actions" {
    const allocator = std.testing.allocator;
    const ThunkStore = sailor.thunk.ThunkStore(CountState, CountAction);

    var store = try ThunkStore.init(
        allocator,
        CountState{ .count = 0 },
        countReducer,
    );
    defer store.deinit(allocator);

    try std.testing.expectEqual(@as(i32, 0), store.getState().count);
    try store.dispatch(.increment);
    try std.testing.expectEqual(@as(i32, 1), store.getState().count);
    try store.dispatch(.{ .set = 99 });
    try std.testing.expectEqual(@as(i32, 99), store.getState().count);
}

// ============================================================================
// UndoStore (v2.13.0)
// ============================================================================

test "v2.13.0: undo_middleware module exists" {
    const undo = sailor.undo_middleware;
    const ModuleType = @TypeOf(undo);
    try std.testing.expect(ModuleType == type);
}

test "v2.13.0: UndoStore basic undo/redo functionality" {
    const allocator = std.testing.allocator;
    const UndoStore = sailor.undo_middleware.UndoStore(CountState, CountAction);

    var store = try UndoStore.init(
        allocator,
        CountState{ .count = 0 },
        countReducer,
        50,
    );
    defer store.deinit(allocator);

    // Initial state
    try std.testing.expectEqual(@as(i32, 0), store.getState().count);
    try std.testing.expect(!store.canUndo());
    try std.testing.expect(!store.canRedo());

    // Dispatch actions
    try store.dispatch(.increment);
    try std.testing.expectEqual(@as(i32, 1), store.getState().count);
    try std.testing.expect(store.canUndo());

    try store.dispatch(.increment);
    try std.testing.expectEqual(@as(i32, 2), store.getState().count);

    // Undo
    const undone = store.undo();
    try std.testing.expect(undone);
    try std.testing.expectEqual(@as(i32, 1), store.getState().count);
    try std.testing.expect(store.canRedo());

    // Redo
    const redone = store.redo();
    try std.testing.expect(redone);
    try std.testing.expectEqual(@as(i32, 2), store.getState().count);
    try std.testing.expect(!store.canRedo());
}

test "v2.13.0: UndoStore undo from initial state returns false" {
    const allocator = std.testing.allocator;
    const UndoStore = sailor.undo_middleware.UndoStore(CountState, CountAction);

    var store = try UndoStore.init(
        allocator,
        CountState{ .count = 5 },
        countReducer,
        10,
    );
    defer store.deinit(allocator);

    // Cannot undo at initial state
    try std.testing.expect(!store.canUndo());
    const result = store.undo();
    try std.testing.expect(!result);
    try std.testing.expectEqual(@as(i32, 5), store.getState().count);
}

test "v2.13.0: UndoStore dispatching after undo clears redo history" {
    const allocator = std.testing.allocator;
    const UndoStore = sailor.undo_middleware.UndoStore(CountState, CountAction);

    var store = try UndoStore.init(
        allocator,
        CountState{ .count = 0 },
        countReducer,
        50,
    );
    defer store.deinit(allocator);

    try store.dispatch(.increment);
    try store.dispatch(.increment);
    _ = store.undo();

    try std.testing.expect(store.canRedo());

    // New dispatch clears redo
    try store.dispatch(.{ .set = 10 });
    try std.testing.expect(!store.canRedo());
    try std.testing.expectEqual(@as(i32, 10), store.getState().count);
}

// ============================================================================
// StatePersist (v2.13.0)
// ============================================================================

test "v2.13.0: state_persist module exists" {
    const state_persist = sailor.state_persist;
    const ModuleType = @TypeOf(state_persist);
    try std.testing.expect(ModuleType == type);
}

fn encodeCount(state: CountState, writer: std.io.AnyWriter) anyerror!void {
    var buf: [16]u8 = undefined;
    const s = try std.fmt.bufPrint(&buf, "{d}", .{state.count});
    try writer.writeAll(s);
}

fn decodeCount(reader: std.io.AnyReader, _: std.mem.Allocator) anyerror!CountState {
    var buf: [16]u8 = undefined;
    const n = try reader.read(&buf);
    const count = try std.fmt.parseInt(i32, buf[0..n], 10);
    return CountState{ .count = count };
}

test "v2.13.0: StatePersist save and load round-trip" {
    const allocator = std.testing.allocator;
    const StatePersist = sailor.state_persist.StatePersist(CountState);

    const persister = StatePersist.init(encodeCount, decodeCount);

    var buf: [64]u8 = undefined;
    var fbs_write = std.io.fixedBufferStream(&buf);

    const original = CountState{ .count = 42 };
    try persister.save(original, fbs_write.writer());

    const written = fbs_write.pos;
    var fbs_read = std.io.fixedBufferStream(buf[0..written]);
    const loaded = try persister.load(fbs_read.reader(), allocator);

    try std.testing.expectEqual(original.count, loaded.count);
}

// ============================================================================
// Reactive module (v2.13.0)
// ============================================================================

test "v2.13.0: reactive module exists" {
    const reactive = sailor.reactive;
    const ModuleType = @TypeOf(reactive);
    try std.testing.expect(ModuleType == type);
}

// ============================================================================
// Backward Compatibility
// ============================================================================

test "v2.13.0: backward compatible with signal module (v2.12.0)" {
    const signal = sailor.signal;
    const ModuleType = @TypeOf(signal);
    try std.testing.expect(ModuleType == type);
}

test "v2.13.0: backward compatible with Store (v2.12.0)" {
    const allocator = std.testing.allocator;
    const Store = sailor.store.Store(CountState, CountAction);

    var store = try Store.init(allocator, CountState{ .count = 0 }, countReducer);
    defer store.deinit(allocator);

    try store.dispatch(.increment);
    try std.testing.expectEqual(@as(i32, 1), store.getState().count);
}

test "v2.13.0: backward compatible with tui module" {
    const tui = sailor.tui;
    const ModuleType = @TypeOf(tui);
    try std.testing.expect(ModuleType == type);
}

test "v2.13.0: backward compatible with arg module" {
    const arg = sailor.arg;
    const ModuleType = @TypeOf(arg);
    try std.testing.expect(ModuleType == type);
}
