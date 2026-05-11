# Iteration 245 Implementation Guide — Keyspace Notifications

## Quick Reference

**Goal**: Integrate keyspace notification publishing into string and generic key command handlers.

**Foundation**: `src/storage/notifications.zig` provides all notification infrastructure (DONE ✓).

**Scope**: String commands (SET, APPEND, INCR, etc.) and generic key commands (DEL, EXPIRE, RENAME, etc.).

**Out of scope**: List/set/hash/zset/stream notifications, special events (`new`, `overwritten`), expiration events.

---

## Implementation Pattern

### Step 1: Import notifications module
```zig
const notifications_mod = @import("../storage/notifications.zig");
```

### Step 2: Check if notifications are enabled
```zig
const flags = storage.getNotificationFlags();
if (flags == 0) return; // Notifications disabled
```

### Step 3: Publish notification after successful modification
```zig
// Check if the specific event type is enabled
if (!notifications_mod.shouldNotify(flags, .string)) return; // For string commands
if (!notifications_mod.shouldNotify(flags, .generic)) return; // For generic commands

// Publish to __keyspace@0__:<key> and __keyevent@0__:<event>
try notifications_mod.publishNotification(
    allocator,
    pubsub_state,
    0, // DB index (always 0)
    key,
    event_name,
    flags,
);
```

---

## Example: cmdSet() in strings.zig

### Before (no notifications)
```zig
pub fn cmdSet(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub: *PubSub,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Parse arguments...
    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };
    const value = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid value"),
    };

    // Set the value
    try storage.set(key, value, expires_at);

    return w.writeSimpleString("OK");
}
```

### After (with notifications)
```zig
pub fn cmdSet(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub: *PubSub,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Parse arguments...
    const key = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };
    const value = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid value"),
    };

    // Set the value
    try storage.set(key, value, expires_at);

    // Publish "set" notification
    const flags = storage.getNotificationFlags();
    if (flags != 0 and notifications_mod.shouldNotify(flags, .string)) {
        try notifications_mod.publishNotification(
            allocator,
            pubsub,
            0, // DB index
            key,
            "set",
            flags,
        );

        // If EX/PX option was used, also publish "expire" event
        if (expires_at != null and notifications_mod.shouldNotify(flags, .generic)) {
            try notifications_mod.publishNotification(
                allocator,
                pubsub,
                0,
                key,
                "expire",
                flags,
            );
        }
    }

    return w.writeSimpleString("OK");
}
```

---

## Example: cmdDel() in keys.zig

### Before (no notifications)
```zig
pub fn cmdDel(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub: *PubSub,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'del' command");
    }

    // Collect keys
    var keys = std.ArrayList([]const u8){};
    defer keys.deinit(allocator);
    for (args[1..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    const count = storage.del(keys.items);
    return w.writeInteger(@intCast(count));
}
```

### After (with notifications)
```zig
pub fn cmdDel(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub: *PubSub,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 2) {
        return w.writeError("ERR wrong number of arguments for 'del' command");
    }

    // Collect keys
    var keys = std.ArrayList([]const u8){};
    defer keys.deinit(allocator);
    for (args[1..]) |arg| {
        const key = switch (arg) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        try keys.append(allocator, key);
    }

    // Check which keys exist BEFORE deletion (for notification)
    const flags = storage.getNotificationFlags();
    var keys_to_notify = std.ArrayList([]const u8){};
    defer keys_to_notify.deinit(allocator);

    if (flags != 0 and notifications_mod.shouldNotify(flags, .generic)) {
        for (keys.items) |key| {
            if (storage.exists(key)) {
                try keys_to_notify.append(allocator, key);
            }
        }
    }

    // Delete keys
    const count = storage.del(keys.items);

    // Publish "del" notification for each successfully deleted key
    if (flags != 0 and notifications_mod.shouldNotify(flags, .generic)) {
        for (keys_to_notify.items) |key| {
            try notifications_mod.publishNotification(
                allocator,
                pubsub,
                0,
                key,
                "del",
                flags,
            );
        }
    }

    return w.writeInteger(@intCast(count));
}
```

**Key insight**: We check which keys exist **before** deletion, then publish notifications only for those keys.

---

## Example: cmdRename() in keys.zig

### After (with notifications)
```zig
pub fn cmdRename(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub: *PubSub,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    // Parse arguments...
    const oldkey = switch (args[1]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };
    const newkey = switch (args[2]) {
        .bulk_string => |s| s,
        else => return w.writeError("ERR invalid key"),
    };

    // Check if destination existed (for "del" event)
    const flags = storage.getNotificationFlags();
    const newkey_existed = if (flags != 0) storage.exists(newkey) else false;

    // Perform rename
    try storage.rename(oldkey, newkey);

    // Publish notifications
    if (flags != 0 and notifications_mod.shouldNotify(flags, .generic)) {
        // 1. "rename_from" on source key
        try notifications_mod.publishNotification(
            allocator,
            pubsub,
            0,
            oldkey,
            "rename_from",
            flags,
        );

        // 2. "del" on destination key if it existed
        if (newkey_existed) {
            try notifications_mod.publishNotification(
                allocator,
                pubsub,
                0,
                newkey,
                "del",
                flags,
            );
        }

        // 3. "rename_to" on destination key
        try notifications_mod.publishNotification(
            allocator,
            pubsub,
            0,
            newkey,
            "rename_to",
            flags,
        );
    }

    return w.writeSimpleString("OK");
}
```

**Event order**: `rename_from` (source) → `del` (dest, if existed) → `rename_to` (dest).

---

## Example: cmdMset() in strings.zig

### After (with notifications)
```zig
pub fn cmdMset(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub: *PubSub,
    args: []const RespValue,
) ![]const u8 {
    var w = Writer.init(allocator);
    defer w.deinit();

    if (args.len < 3 or (args.len - 1) % 2 != 0) {
        return w.writeError("ERR wrong number of arguments for 'mset' command");
    }

    // Parse key-value pairs
    var i: usize = 1;
    var kv_pairs = std.ArrayList(struct { key: []const u8, value: []const u8 }){};
    defer kv_pairs.deinit(allocator);

    while (i < args.len) : (i += 2) {
        const key = switch (args[i]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid key"),
        };
        const value = switch (args[i + 1]) {
            .bulk_string => |s| s,
            else => return w.writeError("ERR invalid value"),
        };
        try kv_pairs.append(allocator, .{ .key = key, .value = value });
    }

    // Set all key-value pairs
    for (kv_pairs.items) |kv| {
        try storage.set(kv.key, kv.value, null);
    }

    // Publish "set" notification for each key
    const flags = storage.getNotificationFlags();
    if (flags != 0 and notifications_mod.shouldNotify(flags, .string)) {
        for (kv_pairs.items) |kv| {
            try notifications_mod.publishNotification(
                allocator,
                pubsub,
                0,
                kv.key,
                "set",
                flags,
            );
        }
    }

    return w.writeSimpleString("OK");
}
```

**Key insight**: MSET fires **one "set" event per key**, not a single event.

---

## Helper Functions (Optional)

To reduce code duplication, you can add helper functions:

### String notification helper
```zig
/// Publish keyspace notification for a string command
fn publishStringNotification(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub: *PubSub,
    key: []const u8,
    event: []const u8,
) !void {
    const flags = storage.getNotificationFlags();
    if (flags == 0) return;
    if (!notifications_mod.shouldNotify(flags, .string)) return;

    try notifications_mod.publishNotification(
        allocator,
        pubsub,
        0,
        key,
        event,
        flags,
    );
}
```

### Generic notification helper
```zig
/// Publish keyspace notification for a generic key command
fn publishGenericNotification(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub: *PubSub,
    key: []const u8,
    event: []const u8,
) !void {
    const flags = storage.getNotificationFlags();
    if (flags == 0) return;
    if (!notifications_mod.shouldNotify(flags, .generic)) return;

    try notifications_mod.publishNotification(
        allocator,
        pubsub,
        0,
        key,
        event,
        flags,
    );
}
```

### Usage with helpers
```zig
// In cmdSet()
try storage.set(key, value, null);
try publishStringNotification(allocator, storage, pubsub, key, "set");

// In cmdDel()
const count = storage.del(keys.items);
for (keys_to_notify.items) |key| {
    try publishGenericNotification(allocator, storage, pubsub, key, "del");
}
```

---

## Event Name Reference

### String Commands (flag: `$`)
| Command | Event Name | Notes |
|---------|------------|-------|
| SET, SETEX, PSETEX, SETNX, GETSET | `"set"` | |
| SETEX, PSETEX | `"expire"` | **Second** event (after `"set"`) |
| APPEND | `"append"` | |
| INCR, DECR, INCRBY, DECRBY | `"incrby"` | All use same event name |
| INCRBYFLOAT | `"incrbyfloat"` | |
| SETRANGE | `"setrange"` | |
| MSET, MSETNX | `"set"` | One event **per key** |
| GETEX (with EX/PX) | `"expire"` | |
| GETEX (with PERSIST) | `"persist"` | |
| GETDEL | `"del"` | Generic event, not string |

### Generic Key Commands (flag: `g`)
| Command | Event Name | Notes |
|---------|------------|-------|
| DEL, UNLINK | `"del"` | One event **per deleted key** |
| EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT | `"expire"` | Or `"del"` if TTL <= 0 |
| PERSIST | `"persist"` | |
| RENAME | `"rename_from"`, `"rename_to"` | Two events (order: from → to) |
| RENAMENX | `"rename_from"`, `"rename_to"` | Only if successful |
| COPY | `"copy_to"` | |
| SORT with STORE | `"sortstore"` | |

---

## Common Pitfalls

### 1. Publishing event before modification
**Wrong**:
```zig
try publishStringNotification(allocator, storage, pubsub, key, "set");
try storage.set(key, value, null); // Set AFTER notification
```

**Right**:
```zig
try storage.set(key, value, null); // Set FIRST
try publishStringNotification(allocator, storage, pubsub, key, "set"); // Then notify
```

### 2. Publishing event when command fails
**Wrong**:
```zig
const success = try storage.setnx(key, value);
try publishStringNotification(allocator, storage, pubsub, key, "set"); // Always fires
return if (success) w.writeInteger(1) else w.writeInteger(0);
```

**Right**:
```zig
const success = try storage.setnx(key, value);
if (success) {
    try publishStringNotification(allocator, storage, pubsub, key, "set"); // Only if successful
}
return if (success) w.writeInteger(1) else w.writeInteger(0);
```

### 3. Wrong flag check
**Wrong**:
```zig
// In cmdSet()
if (notifications_mod.shouldNotify(flags, .generic)) { // Wrong flag!
    try notifications_mod.publishNotification(..., "set", ...);
}
```

**Right**:
```zig
// In cmdSet()
if (notifications_mod.shouldNotify(flags, .string)) { // Correct flag
    try notifications_mod.publishNotification(..., "set", ...);
}
```

### 4. Wrong event name
**Wrong**:
```zig
// In cmdIncr()
try publishStringNotification(allocator, storage, pubsub, key, "incr"); // Wrong name
```

**Right**:
```zig
// In cmdIncr()
try publishStringNotification(allocator, storage, pubsub, key, "incrby"); // Correct name
```

Redis uses `"incrby"` for all increment commands (INCR, DECR, INCRBY, DECRBY).

### 5. Missing pubsub parameter
Some command functions don't have `pubsub: *PubSub` parameter yet. You'll need to add it:

**Before**:
```zig
pub fn cmdSet(
    allocator: std.mem.Allocator,
    storage: *Storage,
    args: []const RespValue,
) ![]const u8
```

**After**:
```zig
pub fn cmdSet(
    allocator: std.mem.Allocator,
    storage: *Storage,
    pubsub: *PubSub,
    args: []const RespValue,
) ![]const u8
```

Don't forget to update the function signature in the command dispatch table!

---

## Testing Strategy

### 1. Manual Testing with redis-cli
```bash
# Terminal 1: Start Zoltraak
zig build run

# Terminal 2: Enable notifications and subscribe
redis-cli
> CONFIG SET notify-keyspace-events KEA
> SUBSCRIBE __keyspace@0__:mykey __keyevent@0__:set

# Terminal 3: Execute command
redis-cli
> SET mykey value

# Terminal 2 should receive:
# 1) "message"
# 2) "__keyspace@0__:mykey"
# 3) "set"
#
# 1) "message"
# 2) "__keyevent@0__:set"
# 3) "mykey"
```

### 2. Integration Tests
See `iteration-245-keyspace-notifications-spec.md` Section 7 for full test suite.

### 3. Python Differential Testing
Compare Zoltraak's RESP output byte-by-byte with real Redis.

---

## Performance Notes

### Early Return on Disabled Notifications
```zig
const flags = storage.getNotificationFlags();
if (flags == 0) return; // Fast path: no notifications enabled
```

This avoids any overhead when notifications are disabled (default).

### Atomic Flag Access
`Storage.notification_flags` is atomic, so reads are lock-free:
```zig
notification_flags: std.atomic.Value(u16)
// ...
return self.notification_flags.load(.acquire); // Lock-free read
```

### Allocation Only When Needed
`publishNotification()` only allocates channel name strings when notifications are enabled and the specific flag (K or E) is set.

---

## Questions?

Refer to:
- **Full spec**: `.claude/iteration-245-keyspace-notifications-spec.md`
- **Redis docs**: https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/
- **Notifications module**: `src/storage/notifications.zig`
- **Config module**: `src/storage/config.zig`

---

**End of Implementation Guide**
