# JavaScript API Reference

This page documents the global objects available in the JavaScript environment shared by the `nodered_js` and `tag_processor` processors. The engine is goja (ES5.1 with some ES6 features) — no Node.js APIs are available.

## msg

The message object. Contains the payload and metadata of the current message.

```javascript
msg.payload    // The message content (any JSON type)
msg.meta       // Metadata key-value pairs (strings)
```

**Return behavior:**
- `return msg;` — pass the message through (modified or not)
- `return null;` or `return undefined;` — drop the message
- `return { payload: ..., meta: ... };` — create a new message

**Example:**
```javascript
msg.payload = msg.payload * 2;
msg.meta.processed = "true";
return msg;
```

## console

Logging functions that write to the Benthos logger.

```javascript
console.debug(...)  // DEBUG level
console.log(...)    // INFO level
console.info(...)   // INFO level
console.warn(...)   // WARN level
console.error(...)  // ERROR level
```

Accepts multiple arguments: `console.log("value is", msg.payload.value)`

## cache

Key-value store for maintaining state across messages. Supports any JSON-compatible type: strings, numbers, booleans, objects, arrays.

Two backends:

- `memory` (default) — in-process, lost on restart. No configuration needed.
- `persistent` — bbolt file on disk, survives restarts. Configure path + TTL.

```yaml
nodered_js:
  code: |
    ...
  cache:
    backend: persistent
    path: ./cache.db   # "~" expands to home; relative paths use working dir
    ttl: 1h            # entry lifetime; 0 disables expiration
```

```javascript
cache.set(key, value)            // Store a value under key (string)
cache.get(key)                   // Retrieve a value, logs error if key not found
cache.exists(key)                // Returns true if key exists, false otherwise
cache.delete(key)                // Remove a key
cache.update(key, fn)            // Atomic read-modify-write; see below
```

Always use `cache.exists(key)` before `cache.get(key)` to avoid error logs on missing keys.

```javascript
if (cache.exists("counter")) {
  var count = cache.get("counter");
} else {
  var count = 0;
}
```

### cache.update — atomic read-modify-write

The pattern `get` then `set` is not atomic across concurrent messages. If two messages run in parallel (Benthos defaults to `pipeline.threads = NumCPU`), both can read the same value, both increment, and one update is lost. Use `cache.update` when the new value depends on the old:

```javascript
cache.update("counter", function(old, exists) {
  return (exists ? old : 0) + 1;
});
```

`fn` receives:

- `old` — current value, or `null` if the key is missing
- `exists` — `true` when the key existed (and was not expired)

Whatever `fn` returns is stored under `key`. The whole sequence (read, run `fn`, write) happens inside one transaction. Concurrent `cache.update` calls on the same key serialize.

#### When to use which

| Pattern | Use |
|---|---|
| Overwriting independent of old value (`msg.payload`, latest reading) | `cache.set` |
| Reading a value (no write) | `cache.exists` + `cache.get` |
| New value depends on old (counter, append, toggle) | `cache.update` |
| Removing an entry | `cache.delete` |

#### Examples

Append to a bounded history list:

```javascript
cache.update("history", function(old, exists) {
  var arr = exists ? old : [];
  arr.push(msg.payload.value);
  if (arr.length > 10) arr.shift();
  return arr;
});
```

Toggle an alarm flag:

```javascript
cache.update("alarm", function(old, exists) {
  return !exists || !old;
});
```

Increment per-source counter:

```javascript
cache.update("count:" + msg.payload.source, function(old, exists) {
  return (exists ? old : 0) + 1;
});
```

### Counter

```javascript
var count = 0;
if (cache.exists("count")) { count = cache.get("count"); }
count++;
cache.set("count", count);
msg.payload = count;
return msg;
```

### Previous value comparison

```javascript
var prev = null;
if (cache.exists("last_value")) {
  prev = cache.get("last_value");
}
var delta = 0;
if (prev !== null) {
  delta = msg.payload.value - prev;
}
cache.set("last_value", msg.payload.value);
msg.payload.delta = delta;
return msg;
```

### History (last N values)

```javascript
var history = [];
if (cache.exists("history")) {
  history = cache.get("history");
}
history.push(msg.payload.value);
if (history.length > 10) history.shift();
cache.set("history", history);
return msg;
```

### Alarm state tracking

```javascript
var alarmed = false;
if (cache.exists("alarm_active")) {
  alarmed = cache.get("alarm_active");
}
if (msg.payload.value > 100 && !alarmed) {
  cache.set("alarm_active", true);
  msg.meta.alarm = "triggered";
  return msg;
}
if (msg.payload.value <= 100 && alarmed) {
  cache.set("alarm_active", false);
  msg.meta.alarm = "cleared";
  return msg;
}
return msg;
```

### Cycle time between events

```javascript
var lastMs = null;
if (cache.exists("last_event_ms")) {
  lastMs = cache.get("last_event_ms");
}
if (lastMs !== null) {
  msg.payload.cycle_time_ms = Date.now() - lastMs;
}
cache.set("last_event_ms", Date.now());
return msg;
```

### Concurrency

Benthos defaults to `pipeline.threads = NumCPU`, so multiple messages can run through one `nodered_js` processor in parallel.

- `cache.set` / `cache.get` / `cache.exists` / `cache.delete` are each atomic on their own.
- The `get` → modify → `set` pattern is **not** atomic across threads. Two messages can both read the same value and one overwrite the other. Use `cache.update(key, fn)` for any read-modify-write (counters, lists, toggles).
- Setting `pipeline.threads: 1` removes parallelism entirely and makes any pattern safe at the cost of throughput.

### Limitations

- **Cache scope** — each `nodered_js` processor block has its own cache. Two separate blocks in the same pipeline cannot share a cache; use the UNS to exchange data between flows.
- **`persistent` backend is single-writer** — bbolt acquires an exclusive file lock. Two processors pointing at the same file (in one process or across processes) fail at startup with a timeout.
- **No size limits** — the cache grows unboundedly if keys are never deleted. Use `cache.delete` to clean up unused keys, or rely on `ttl` for expiration. A hard cap is planned (ENG-4829).
- **Cache scope in `tag_processor`** — the cache is shared across all stages (`defaults`, `conditions`, `advancedProcessing`). A value set in `defaults` is visible in `advancedProcessing` within the same message.

### Metrics

Each processor exposes Benthos metrics for its cache (sampled every 30 s):

- `cache_keys` — number of entries currently stored
- `cache_disk_bytes` — file size on disk (`0` for the memory backend)
