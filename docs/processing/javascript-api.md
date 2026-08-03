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

### Idempotency and monotonicity under retries (`dedupKey`)

At-least-once inputs (Kafka, UNS input, MQTT QoS1+, AMQP, JetStream) redeliver the same message when a downstream ACK is lost. Without protection, every retry re-runs the JavaScript, so a counter kept in the cache double-counts, an alarm re-fires, and a monotonic max advances past values it has already recorded.

Both processors accept a `dedupKey` config field that names a **Benthos message metadata field** (set by the input plugin) whose value uniquely identifies the source event across retries. On the first sight of a value, cache writes run normally and the value is recorded in the cache under the reserved prefix `__dedup__:`. On later deliveries of the same value, `cache.set` and `cache.delete` are no-ops for that message, so cache state stays idempotent under retries.

The dedup value is pulled from message metadata via `msg.meta.<dedupKey>`. It must be attached by the input plugin (for example the Kafka input auto-tags every message with `kafka_offset`, `kafka_partition`, `kafka_topic`). If your input doesn't attach a unique identifier, compose one with an upstream `bloblang` or `mapping` processor before the JS processor runs.

```yaml
# nodered_js
nodered_js:
  cache:
    dedupKey: kafka_offset

# tag_processor
tag_processor:
  dedupKey: kafka_offset
```

**What it covers**

- `cache.set` — no-op on retry
- `cache.delete` — no-op on retry
- Read-modify-write patterns become idempotent: the JS still runs and computes locally, but the retried write does not commit
- Monotonic counters, high-water marks, alarm latches — all stay correct across retries when built on `cache.set`

**What it does NOT cover**

- Side effects outside the cache — HTTP calls, external DB writes, log lines, and metrics `Incr` still run on every retry
- Output-side deduplication — messages sent to Kafka/UNS/MQTT still leave the pipeline on every retry. If your output needs dedup, gate it separately (e.g. Kafka idempotent producer, `umh_topic`-keyed compaction, or a downstream dedup filter)
- Messages missing the `dedupKey` meta field — a one-time warning is logged and the message is processed as fresh
- Cross-process retries — dedup markers live in the same cache instance as your user keys. Two benthos processes must share the same persistent cache (`backend: persistent`, same `name`) to share dedup state, and bbolt's single-writer file lock still applies

**How to use**

1. Pick a metadata field whose value uniquely identifies the source event across retries. For Kafka this is the `topic:partition:offset` triple — `kafka_offset` alone repeats across partitions and topics.
2. If the input doesn't already attach such a field (or attaches only its parts), compose one earlier in the pipeline with a `mapping`/`bloblang` step and write it to a fresh meta key.
3. Set `dedupKey` to that meta key on the processor.
4. Bound the marker lifetime with `cache.ttl`. Markers live under `__dedup__:<value>` in the same cache; a TTL matching your worst-case retry window (minutes for Kafka, hours for slow-consumer scenarios) keeps memory bounded. Without a TTL, markers accumulate for the lifetime of the process (or the persistent file).

**Example — idempotent counter under Kafka at-least-once**

Benthos' Kafka input auto-tags every message with `kafka_offset`, `kafka_partition`, `kafka_topic`, etc., so a single-partition consumer can point `dedupKey` straight at `kafka_offset` with no upstream step:

```yaml
input:
  kafka:
    addresses: ["broker:9092"]
    topics: ["events"]   # single partition
pipeline:
  processors:
    - nodered_js:
        cache:
          backend: persistent
          path: /var/cache/umh.db
          ttl: 6h
          dedupKey: kafka_offset
        code: |
          var n = cache.exists("count") ? cache.get("count") : 0;
          cache.set("count", n + 1);
          msg.payload = { count: n + 1 };
          return msg;
```

For **multi-partition or multi-topic** consumers, `kafka_offset` alone collides (partition 0 offset 42 ≡ partition 1 offset 42). Compose a unique key first with a `mapping` step and point `dedupKey` at that:

```yaml
input:
  kafka:
    addresses: ["broker:9092"]
    topics: ["events"]
pipeline:
  processors:
    - mapping: |
        meta dedup_id = meta("kafka_topic") + ":" + meta("kafka_partition") + ":" + meta("kafka_offset")
    - nodered_js:
        cache:
          dedupKey: dedup_id
        code: |
          var n = cache.exists("count") ? cache.get("count") : 0;
          cache.set("count", n + 1);
          msg.payload = { count: n + 1 };
          return msg;
```

- First delivery of offset 42: `count` was 41. JS reads 41 and writes 42. Payload reports `42`.
- Redelivery of offset 42 (crash before ACK): JS reads 42 and computes 43, but the `cache.set("count", 43)` call is suppressed. The cache stays at 42. The retried message still leaves the pipeline with `43` in its payload; downstream systems must dedup output separately. Nothing kept in the cache is corrupted.

**Example — monotonic max under retries**

```javascript
var seen = cache.exists("max") ? cache.get("max") : null;
if (seen === null || msg.payload.value > seen) {
  cache.set("max", msg.payload.value);
  seen = msg.payload.value;
}
msg.payload.max_so_far = seen;
return msg;
```

Without `dedupKey`, a redelivered sample larger than the previous high is fine on its own (max is idempotent by construction). But a redelivered smaller value combined with any read-modify-write in the same block can still corrupt paired state (for example a running sum kept in the same cache). Setting `dedupKey` gates all cache writes for that source event, so the running sum, the max, and any other paired state stay consistent as one decision per source event.

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
- Keys with the prefix `__dedup__:` are reserved for the `dedupKey` retry-idempotency machinery. Don't write to them from your own code.

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
