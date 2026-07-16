# JavaScript API Reference

This page documents the global objects available in the JavaScript environment shared by the `nodered_js` and `tag_processor` processors. The engine is goja (ES5.1 with some ES6 features); no Node.js APIs are available.

## msg

The message object. Contains the payload and metadata of the current message.

```javascript
msg.payload    // The message content (any JSON type)
msg.meta       // Metadata key-value pairs (strings)
```

**Return behavior:**
- `return msg;` passes the message through (modified or not)
- `return null;` or `return undefined;` drops the message
- `return { payload: ..., meta: ... };` creates a new message

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

- `memory` (default): in-process, lost on restart. No configuration needed.
- `persistent`: bbolt file on disk, persists across restarts. Configure path + TTL.

```yaml
nodered_js:
  code: |
    ...
  cache:
    backend: persistent
    name: shared       # sharing identifier (default: "shared"); see "Sharing across processors" below
    path: ./cache.db   # "~" expands to home; relative paths resolve from benthos start directory. Prefer absolute paths.
    ttl: 0s            # entry lifetime; 0 (default) = no expiration. Set e.g. "1h" to auto-expire.
```

### Sharing across processors

Two `nodered_js` processors with the same `backend` and `name` share one cache instance within the same benthos process. Keys written by one are visible to the others. The default `name` is `"shared"`, so two processors with no explicit cache config already share state out of the box.

```yaml
pipeline:
  processors:
    - nodered_js:
        code: |
          var n = cache.exists("count") ? cache.get("count") : 0;
          n = n + 1;
          cache.set("count", n);
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

Cross-process sharing (two separate benthos PIDs on the same host) is **not** supported: bbolt's file lock blocks the second open. Use an external KV store (Redis, etc.) for that.

```javascript
cache.set(key, value)            // Store a value under key (string)
cache.get(key)                   // Retrieve a value, logs error if key not found
cache.exists(key)                // Returns true if key exists, false otherwise
cache.delete(key)                // Remove a key
```

Always use `cache.exists(key)` before `cache.get(key)` to avoid error logs on missing keys.

```javascript
if (cache.exists("counter")) {
  var count = cache.get("counter");
} else {
  var count = 0;
}
```

### Thread safety (auto-lock)

Every batch acquires a per-cache mutex for its duration. Same mutex is shared across processors with the same `backend` + `name`. Effect:

- `cache.get` → compute → `cache.set` in the same message is atomic: no other message can slip in between the get and the set
- Multiple cache operations in one message run as one uninterrupted block
- Cross-processor read-modify-write on a shared cache is also atomic

Plain `get` / `set` is safe for counters, buffers, alarms, and all stateful patterns below.

Trade-off: cache operations serialize on a shared mutex. For workloads under 100 msg/s, the cost is negligible.

The auto-lock does not guarantee message order. With `pipeline.threads > 1`, messages process out of order; message 5 may arrive before message 3. For strict message order, set `pipeline.threads: 1`.

Cache writes also stay correct when a message is redelivered. Inputs like UNS, Kafka, or MQTT can re-emit a message after a downstream failure; without protection, counters and appends would fire twice. The processor remembers the last 100 messages by their content. When a redelivered message arrives, the JS still runs, but `cache.set` and `cache.delete` are silently skipped for that message. `cache.get` and `cache.exists` continue to return the current value.

Watch out with polling inputs that repeat the same reading (S7, Modbus, EIP without a per-read timestamp in metadata): a second identical reading looks like a redelivery and its writes are skipped. Inputs that already stamp each read — OPC UA `SourceTimestamp`, Kafka offset, Sparkplug sequence — don't hit this.

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

### Limitations

- Caches don't cross benthos process boundaries; bbolt's file lock blocks a second opener. Use an external KV to share across processes. See [Sharing across processors](#sharing-across-processors).
- The cache has no size limit and grows unboundedly if keys are never deleted. Use `cache.delete` or rely on `ttl` for expiration. A hard cap is planned.
- In `tag_processor`, one cache is shared across all stages (`defaults`, `conditions`, `advancedProcessing`); a value set in `defaults` is visible in `advancedProcessing` within the same message.

### Metrics

Each processor reports these Benthos metrics for its cache (sampled every 30 s):

- `cache_keys`: number of entries currently stored
- `cache_disk_bytes`: file size on disk (`0` for the memory backend)

## protobuf

Decode and encode protobuf messages inline, against a schema passed as a base64-encoded `FileDescriptorSet` (no files on disk). Useful for reading data the standard inputs don't decode — for example the raw Sparkplug B metric bytes attached by the `sparkplug_b` input's `passthrough_raw_metric` flag, including proto2 extension fields.

```javascript
protobuf.decode(dataB64, descriptorSetB64, msgName)  // base64 proto bytes -> object
protobuf.encode(obj, descriptorSetB64, msgName)      // object -> base64 proto bytes
```

- `descriptorSetB64` — base64 of a self-contained `FileDescriptorSet`. Compile it once with `protoc --include_imports --descriptor_set_out=schema.pb your.proto`, then base64-encode `schema.pb` and paste the string into your script.
- `msgName` — fully-qualified message name, e.g. `com.example.Payload.Metric` (no leading dot).
- The decoded object follows protojson conventions: `int64`/`uint64` and `bytes` come back as strings, enums as their names, and **proto2 extensions appear as `[package.extension]` keys**. For `encode`, pass 64-bit integers as strings.
- Both functions throw on error (invalid base64, unknown message, malformed descriptor set); wrap calls in `try/catch` to handle failures in script.

```javascript
// Decode the raw Sparkplug metric attached by passthrough_raw_metric, reading an extension field.
var DESC = "CtIB..."; // base64 FileDescriptorSet, compiled once
var metric = protobuf.decode(msg.meta.spb_metric_raw, DESC, "com.example.Payload.Metric");
msg.payload = { value: metric.value, extra: metric["[com.example.my_extension]"] };
return msg;
```

Available in both `nodered_js` and `tag_processor` — they share the same JavaScript environment.
