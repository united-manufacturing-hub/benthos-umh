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

Timestamp-gated key-value store for state that must survive retries and out-of-order arrivals. Every write carries a `timestamp_ms` (unix milliseconds); the store keeps the entry only when the incoming timestamp is strictly newer than the stored one. Replays and out-of-order writes are dropped with a WARN log and never mutate the cache.

Both `nodered_js` and `tag_processor` expose the same `cache` object and take the same `cache:` config block.

Two backends:

- `memory` (default): in-process, lost on restart.
- `persistent`: bbolt file on disk, persists across restarts. Configure `path` and `ttl`.

```yaml
nodered_js:
  code: |
    ...
  cache:
    backend: persistent
    name: shared       # sharing identifier (default: "shared"); see "Sharing across processors" below
    path: ./cache.db   # "~" expands to home; relative paths resolve from the benthos start directory. Prefer absolute paths.
    ttl: 0s            # entry lifetime; 0 (default) = no expiration. Set e.g. "1h" to auto-expire.
```

The same block works on `tag_processor`:

```yaml
tag_processor:
  cache:
    backend: persistent
    path: /var/cache/umh.db
  defaults: |
    ...
```

### API

```javascript
cache.set(key, msg)   // Write msg.value only when msg's watermark field is strictly newer than the stored one.
                      // msg must be an object of shape { value: any, <watermark-field>: <int64> } where
                      // the watermark-field is one of 'watermark', 'timestamp_ms', or 'kafka_offset' (pick one).
cache.get(key)        // Return { value: any, watermark: int64 }. Logs error if key not found.
                      // The watermark is always returned under the key 'watermark', regardless of which name you wrote.
cache.exists(key)     // Return true if key exists, false otherwise.
cache.delete(key)     // Remove the key. Clears the watermark gate too; a following set at any watermark is accepted.
```

The `msg` argument to `cache.set` is exactly the shape `tag_processor` produces in `msg.payload`, so most callers pass `msg.payload` directly:

```javascript
cache.set("last_temperature", msg.payload);
```

Always use `cache.exists(key)` before `cache.get(key)` to avoid error logs on missing keys:

```javascript
if (cache.exists("last_temperature")) {
  var last = cache.get("last_temperature");   // { value: ..., watermark: ... }
  console.log("last was", last.value, "at watermark", last.watermark);
}
```

### Timestamp gating: replays and out-of-order writes

`cache.set` is the same idempotency primitive under two names:

- **Replay**: same source event redelivered by the input (Kafka after ACK loss, MQTT QoS1+ retry, UNS input restart). The redelivered message carries the same `timestamp_ms` it did the first time, so the second `set` is a no-op.
- **Out-of-order**: a newer message arrived first, then an older one shows up. The older `timestamp_ms` is not strictly greater than the stored one, so the write is dropped.

Both cases produce one WARN log line naming the key, the incoming watermark, and the last stored watermark, plus a hint on where to look:

```
cache.set: dropped write for key "k" (incoming watermark 1234 is older/equal to the last stored 2222). Incoming messages must arrive in monotonic order — either messages are being replayed or the watermark source (timestamp_ms, kafka_offset) is going backwards. Check the input plugin or upstream data.
```

Retries should be rare in a healthy pipeline. A sustained flood of these WARN lines means the upstream input is replaying more than expected, or the watermark source is broken (e.g. clock rewind, out-of-order Kafka partition consumption) — investigate there.

**What it covers**

- `cache.set` under retries and out-of-order arrivals — the store is the source of truth.
- Patterns built on `cache.set` stay consistent: counters, running totals, tracking the largest value seen so far, alarm state that flips on threshold, history buffers.

**What it does NOT cover**

- Side effects outside the cache — HTTP calls, external DB writes, `msg.payload` modifications, log lines. JS still runs on every retry; only the `cache.set` write is gated.
- Output-side deduplication — messages sent to Kafka/UNS/MQTT still leave the pipeline on every retry. Downstream systems must dedup separately (e.g. Kafka idempotent producer, `umh_topic`-keyed compaction).
- `msg.payload` shaped differently than `{value, timestamp_ms}` — the field names are hard-coded. If your payload uses different names, remap first: `cache.set("k", { value: msg.payload.reading, timestamp_ms: msg.payload.t })`.

**ACID within the cache, not across cache and output**

The cache itself is ACID within its own boundary:

- **Atomic** per batch: every `cache.set` and `cache.delete` in one `ProcessBatch` call runs inside a single bbolt write tx. All commit together or none do.
- **Consistent**: `cache.set` refuses any write whose watermark is not strictly newer than the stored one. Invariants across multiple keys are your problem — put related fields in one object value if they must move together.
- **Isolated**: batches on the same cache instance serialize on a per-cache mutex. Across processes, bbolt's file lock stops a second opener.
- **Durable**: `persistent` backend fsyncs on commit. `memory` backend loses everything on process exit.

Cache and Kafka output are not jointly atomic. Order in `ProcessBatch` is: cache tx commits, then benthos publishes to the output. A crash in between advances the cache but never emits the message; the input's replay then hits the watermark gate and is dropped, and the message is gone. Cover this at the output: Kafka idempotent producer, `umh_topic`-keyed compaction, or downstream dedup on `msg.payload.timestamp_ms` + tag identity.

### Sharing across processors

Two processors with the same `backend` and `name` share one cache instance within the same benthos process. Keys written by one are visible to the others. The default `name` is `"shared"`, so two processors with no explicit cache config already share state.

```yaml
pipeline:
  processors:
    - nodered_js:
        code: |
          var next = (cache.exists("count") ? cache.get("count").value : 0) + 1;
          cache.set("count", { value: next, timestamp_ms: msg.payload.timestamp_ms });
          return msg;
        # implicit: backend=memory, name=shared
    - nodered_js:
        code: |
          msg.payload.count = cache.get("count").value;
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

### Thread safety (auto-lock)

Every batch acquires a per-cache mutex for its duration. Same mutex is shared across processors with the same `backend` + `name`. Effect:

- `cache.get` → compute → `cache.set` in the same message is atomic: no other message can slip in between the get and the set
- Multiple cache operations in one message run as one uninterrupted block
- Cross-processor read-modify-write on a shared cache is also atomic

Trade-off: cache operations serialize on a shared mutex. For workloads under 100 msg/s, the cost is negligible.

The auto-lock does not guarantee message order. With `pipeline.threads > 1`, messages process out of order; message 5 may arrive before message 3. The timestamp gate on `cache.set` is what keeps state correct under reordering, not the mutex. For strict message order set `pipeline.threads: 1`.

### Examples (time-series payloads)

Every example assumes messages carry `msg.payload.timestamp_ms` — the shape `tag_processor` emits. When writing to the cache, pass a `{value, timestamp_ms}` object.

**Read-then-derive pattern**: any example that computes something from the previous stored value AND emits it downstream must gate the derivation on watermark comparison. Otherwise a retried message reads the previous value, computes derived output, `cache.set` is silently dropped by the store, but the pipeline still emits the derived output — computed against a stored value the retry did not advance. The pattern:

1. Pass the watermark to `cache.set` (via `timestamp_ms`, `kafka_offset`, or `watermark` — pick one per message).
2. `cache.get(k)` returns `{value, watermark}`; compare the returned `watermark` to the incoming `msg.payload.timestamp_ms` (or `msg.meta.kafka_offset` for relational).
3. Derive downstream output only when the incoming message is strictly newer.
4. `cache.set` unconditionally — the store drops the stale write for you.

#### Last value seen

```javascript
cache.set("last_temperature", msg.payload);
return msg;
```

`msg.payload` is already `{value, timestamp_ms}`. A retried or out-of-order message with an older `timestamp_ms` is dropped by the store.

#### Delta since last sample

```javascript
var prev = cache.exists("last") ? cache.get("last") : null;
if (prev !== null && msg.payload.timestamp_ms > prev.watermark) {
  msg.payload.delta = msg.payload.value - prev.value;
}
cache.set("last", msg.payload);
return msg;
```

`prev` is the previously stored entry as `{value, watermark}` — the watermark comes back whichever key you originally wrote it under (`timestamp_ms`, `kafka_offset`, `watermark`). On a replay `prev.watermark === msg.payload.timestamp_ms`, the delta guard is false, no `delta` is added to the emitted payload, and `cache.set` is dropped harmlessly. On a fresh message the guard passes, delta is computed against the correct previous value, and the store advances.

#### Running sum (monotonic counter)

```javascript
var prev = cache.exists("sum") ? cache.get("sum") : null;
var prevTotal = prev !== null ? prev.value : 0;
if (prev === null || msg.payload.timestamp_ms > prev.watermark) {
  var running = prevTotal + msg.payload.value;
  cache.set("sum", { value: running, timestamp_ms: msg.payload.timestamp_ms });
  msg.payload.total = running;
} else {
  msg.payload.total = prevTotal;
}
return msg;
```

On a replay the else-branch fires: the emitted `total` reflects the current stored sum, not a doubly-counted value. On a fresh message the total advances and the store updates.

#### Largest value seen so far

Track the biggest reading up to now — useful for peak temperature, worst-case latency, maximum flow rate.

```javascript
var prev = cache.exists("max") ? cache.get("max") : null;
var best = prev !== null ? prev.value : Number.NEGATIVE_INFINITY;
if ((prev === null || msg.payload.timestamp_ms > prev.watermark) && msg.payload.value > best) {
  cache.set("max", msg.payload);
  best = msg.payload.value;
}
msg.payload.max_so_far = best;
return msg;
```

`max` is idempotent by construction (max(a, a) = a) — but only when compared against the SAME reading. Under replay we still want to skip both the JS-side max update and the emitted payload change; the watermark guard does that.

#### Alarm latch

```javascript
var prev = cache.exists("alarm") ? cache.get("alarm") : null;
if (prev === null || msg.payload.timestamp_ms > prev.watermark) {
  var active = prev !== null ? prev.value : false;
  if (msg.payload.value > 100 && !active) {
    cache.set("alarm", { value: true, timestamp_ms: msg.payload.timestamp_ms });
    msg.meta.alarm = "triggered";
  } else if (msg.payload.value <= 100 && active) {
    cache.set("alarm", { value: false, timestamp_ms: msg.payload.timestamp_ms });
    msg.meta.alarm = "cleared";
  }
}
return msg;
```

The watermark guard prevents a replay from re-triggering the alarm meta.

#### Cycle time between events

```javascript
var prev = cache.exists("last_event") ? cache.get("last_event") : null;
if (prev !== null && msg.payload.timestamp_ms > prev.watermark) {
  msg.payload.cycle_time_ms = msg.payload.timestamp_ms - prev.value;
}
cache.set("last_event", { value: msg.payload.timestamp_ms, timestamp_ms: msg.payload.timestamp_ms });
return msg;
```

Under replay the guard is false, no `cycle_time_ms` is emitted, and the store drops the redundant write.

### Limitations

- **One cache key, one monotonic stream.** Only one `umh_topic` may write to a given key. Two topics with independent watermarks would silently drop each other's writes at the gate. Use a per-topic key when in doubt: `cache.set(msg.meta.umh_topic + ":last", msg.payload)`.
- Caches don't cross benthos process boundaries; bbolt's file lock blocks a second opener. Use an external KV to share across processes. See [Sharing across processors](#sharing-across-processors).
- The cache has no size limit and grows unboundedly if keys are never deleted. Use `cache.delete` or rely on `ttl` for expiration. A hard cap is planned.
- In `tag_processor`, one cache is shared across all stages (`defaults`, `conditions`, `advancedProcessing`); a value set in `defaults` is visible in `advancedProcessing` within the same message.
- `cache.set` requires `msg.value` and exactly one watermark field (`watermark`, `timestamp_ms`, or `kafka_offset`) — a missing field, multiple set, or a non-numeric watermark logs an error and drops the write.

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
