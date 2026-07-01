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
    name: shared       # sharing identifier (default: "shared"); see "Sharing across processors" below
    path: ./cache.db   # "~" expands to home; relative paths resolve from the benthos process start directory (under UMH Core: the S6 service dir). Prefer absolute paths.
    ttl: 0s            # entry lifetime; 0 (default) = no expiration. Set e.g. "1h" to auto-expire.
```

### Sharing across processors

Two `nodered_js` processors with the same `backend` and `name` share one cache instance within the same benthos process. Keys written by one are visible to the others. The default `name` is `"shared"` — so two processors with no explicit cache config already share state out of the box.

```yaml
pipeline:
  processors:
    - nodered_js:
        code: |
          cache.update("count", function(old, exists) {
            return (exists ? old : 0) + 1;
          });
          return msg;
        # implicit: backend=memory, name=shared
    - nodered_js:
        code: |
          msg.payload.count = cache.get("count");
          return msg;
        # same defaults (name=shared) → same cache instance → sees "count" from above
```

For persistent caches, only the **first** processor needs to define `path`; later processors attaching to the same `name` may omit it:

```yaml
- nodered_js:
    cache: { backend: persistent, name: state, path: /var/cache/umh.db }
- nodered_js:
    cache: { backend: persistent, name: state }   # attaches to the same store
```

Isolate groups by giving them different names. For a per-processor cache, set `name: ""` (empty).

Cross-process sharing (two separate benthos PIDs on the same host) is **not** supported — bbolt's file lock blocks the second open. Use an external KV store (Redis, etc.) for that.

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

**`cache.set` on its own is thread-safe** — under the hood every write is serialized behind the store's mutex. The race isn't in `cache.set`; it's in the **three-step JavaScript pattern** users tend to write:

```javascript
var n = cache.get("count");   // step 1: read
n = n + 1;                    // step 2: compute in JS (no lock held)
cache.set("count", n);        // step 3: write
```

Between steps 1 and 3 another goroutine can run steps 1–3 too. Both read the same value, both compute the same next value, both write it. One increment is silently lost. Benthos defaults to `pipeline.threads = NumCPU`, so this happens on the very first burst of messages — reproducibly.

Use `cache.update` whenever the new value depends on the old:

```javascript
cache.update("counter", function(old, exists) {
  return (exists ? old : 0) + 1;
});
```

`fn` receives:

- `old` — current value, or `null` if the key is missing
- `exists` — `true` when the key existed (and was not expired)

Whatever `fn` returns is stored under `key`. The store holds its write lock for the whole read → run `fn` → write sequence, so concurrent `cache.update` calls on the same key serialize with no lost updates.

**Rule of thumb**: if your code has `cache.get` followed by any compute followed by `cache.set` on the same key, replace all three with one `cache.update`.

> **Warning: don't call cache methods inside `fn`.** The store's write lock is held for the duration of `fn`. Calling `cache.set`, `cache.get`, `cache.exists`, `cache.delete`, or another `cache.update` from inside the callback deadlocks the processor permanently — the goroutine waits for a lock it already holds, every other concurrent cache call blocks behind it, and the pipeline stalls until restart. Use the `old` and `exists` arguments instead; they already reflect the current state for that key.
>
> ```javascript
> // ❌ deadlocks
> cache.update("counter", function(old, exists) {
>   if (cache.exists("counter")) { ... }   // nested call — blocks forever
>   return (old || 0) + 1;
> });
>
> // ✅ use the arguments
> cache.update("counter", function(old, exists) {
>   return (exists ? old : 0) + 1;
> });
> ```

#### When to use which

| You want to… | Use |
|---|---|
| Store the newest value (overwrite, no dependency on the old value) | `cache.set` |
| Check if a key exists | `cache.exists` |
| Read a value (no write) | `cache.get` (guard with `cache.exists`) |
| Compute a new value from the old one and store it | `cache.update` |
| Remove a key | `cache.delete` |

##### cache.set is correct when the new value is independent of the old

```javascript
cache.set("latest_temperature", msg.payload.value);   // newest reading wins
cache.set("last_seen_at", Date.now());                // newest timestamp wins
cache.set("device_status", "online");                 // pure flag
cache.set("config_snapshot", msg.payload);            // full replacement
```

None of these read the current value. Two goroutines writing at the same instant both succeed; the one that lands second is the one you read afterwards. That's the intended semantic of "latest".

##### cache.set is wrong when you first read the old value

All of the following are races — replace them with `cache.update`:

```javascript
// counter — depends on old
cache.set("count", cache.get("count") + 1);

// append to a list — depends on old
var arr = cache.get("history"); arr.push(x); cache.set("history", arr);

// running maximum — depends on old
if (msg.payload.v > cache.get("max")) cache.set("max", msg.payload.v);

// toggle — depends on old
cache.set("flag", !cache.get("flag"));
```

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

- **Cache scope** — sharing is per benthos process. Two `nodered_js` processors within one process share a cache when their `backend` and `name` match (see [Sharing across processors](#sharing-across-processors)); across separate processes they cannot share. Use the UNS or an external KV to exchange data across processes or flows.
- **`persistent` backend is single-writer per file** — bbolt acquires an exclusive OS file lock. Multiple processors in one process sharing by `name` are safe (they attach to the same open handle via the internal registry). A second benthos process pointing at the same file fails at startup with a timeout.
- **No size limits** — the cache grows unboundedly if keys are never deleted. Use `cache.delete` to clean up unused keys, or rely on `ttl` for expiration. A hard cap is planned (ENG-4829).
- **Cache scope in `tag_processor`** — the cache is shared across all stages (`defaults`, `conditions`, `advancedProcessing`). A value set in `defaults` is visible in `advancedProcessing` within the same message.

### Metrics

Each processor exposes Benthos metrics for its cache (sampled every 30 s):

- `cache_keys` — number of entries currently stored
- `cache_disk_bytes` — file size on disk (`0` for the memory backend)
