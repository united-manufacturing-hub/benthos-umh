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
    dedupKey: kafka_offset # metadata field identifying a message across retries. Without it, a
                       # redelivered message writes to the cache a second time — a count is
                       # raised twice for one part. See "Retries and duplicate messages".
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

Every batch takes a lock on the cache for its duration. The same lock is shared across processors with the same `backend` and `name`. What that gives you:

- `cache.get` → compute → `cache.set` in the same message runs as one unit: no other message can slip in between the get and the set
- The lock covers the **whole batch**, not one message, so the guarantee holds across every message in it
- The same holds when two processors share a cache: one processor's read-change-write finishes before the other's starts

This makes plain `get` / `set` safe against two **different** messages racing each other. It does not make it safe against the **same** message being handled twice — that is a separate problem, handled by [`dedupKey`](#retries-and-duplicate-messages-dedupkey).

Trade-off: the lock is held for the entire batch, JavaScript execution included, not just for the cache calls. Two processors sharing a cache therefore take turns, and raising `pipeline.threads` buys a flow that uses the cache almost nothing. At the rates a bridge or stream processor produces this costs nothing you would notice — but don't expect more threads to speed up a stateful flow.

The auto-lock does not guarantee message order. With `pipeline.threads > 1`, messages process out of order; message 5 may arrive before message 3. For strict message order, set `pipeline.threads: 1`.

### Retries and duplicate messages (`dedupKey`)

UMH delivers messages at least once. When the pipeline cannot confirm that a message was handled, it delivers the same message again and your JavaScript runs again — so a count kept in the cache counts the same part twice.

`dedupKey` is what stops that. Once you tell it how to recognize a message, **the cache is written on the first delivery and not again**, however many times that message arrives.

`dedupKey` names a **message metadata field** — the value you read in JavaScript as `msg.meta.<name>`, not a field of the payload.

The first time a value arrives, cache writes run normally and the value is remembered. When a message arrives carrying a value that was seen before, `cache.set` and `cache.delete` do nothing for that message, a WARN line is logged, and the `cache_dedup_suppressed` counter goes up. Retries are rare in a healthy pipeline, so a steady stream of those lines means something upstream is redelivering constantly.

`cache.get` and `cache.exists` still read normally — only writes are held back. The JavaScript still runs on the redelivered message, and the message still reaches the output. What `dedupKey` protects is the cache: a stored count is not raised twice, a running total does not take the same reading in twice, a key you deleted stays deleted.

**Two conditions on the field you pick**

Ask two things about each candidate: does the message carry a value that survives a retry, and is that value different for every message? Both have to be true.

- **The same on a retry as on the first delivery.** If it changes, the retry looks like a new message and nothing is suppressed.
- **Different for every distinct message.** If two real readings share a value, the second one's cache writes are dropped and that reading is lost from your state.

A composed string works as well as a number, as long as both conditions hold.

**Choosing a key**

| Field | Comes from | Verdict |
| --- | --- | --- |
| `kafka_offset` | `uns` input | Best choice, and nothing to compose. The UNS writes everything to `umh.messages`, which always has exactly one partition, so the offset on its own identifies a message. |
| `kafka_offset` together with `kafka_partition` | Benthos' own `kafka` and `redpanda` inputs | These can read many partitions, and an offset is unique only inside one — partition 0 offset 42 and partition 1 offset 42 are different messages. Combine the two into one value with a `mapping` step, as described under the table. |
| `opcua_source_timestamp` | `opcua` input | Works. Watch for a device whose clock ticks coarser than it publishes: two readings then share a timestamp, and the second one's writes are dropped. |
| `kafka_timestamp_ms` | `uns` input; Kafka inputs | Avoid. It survives a retry, but two messages written in the same millisecond share it and the second one is suppressed. |
| `timestamp_ms` | the payload of a tag message | Unusable as it stands: it sits in the payload, not in metadata, so `dedupKey` never finds it. In a bridge it is also stamped while the message is being processed, so copying it into metadata does not help — it changes on every retry. In a stream processor it does survive a retry and you *can* copy it into metadata with a `mapping` step, but two tags read in the same millisecond then share a value and the second one's writes are dropped. Prefer `kafka_offset`. |
| nothing usable (S7, Modbus bridges) | — | These carry no field that identifies a message across retries. Publish the raw value to the UNS and do the counting in a stream processor, where a Kafka offset exists. |

The `uns` input also attaches `kafka_topic`, `kafka_msg_key` and `umh_topic`, but none of them identifies a single message — every reading from one tag carries the same values.

If your input attaches the parts of a unique value but not the value itself, compose one with a `mapping` step ahead of the JS processor and write it to a fresh metadata key. Reading a multi-partition topic with Benthos' own `kafka` input, that is:

```yaml
pipeline:
  processors:
    - mapping: |
        meta dedup_id = meta("kafka_partition") + ":" + meta("kafka_offset")
    - nodered_js:
        cache:
          dedupKey: dedup_id
        code: ...
```

Inside UMH you do not need this: `umh.messages` has one partition, so `dedupKey: kafka_offset` is complete on its own.

**Configuration**

In `nodered_js` the field sits under `cache`; in `tag_processor` it is a top-level field. Note the camelCase spelling, unlike its all-lowercase siblings.

Leave it unset and nothing is suppressed: every delivery writes to the cache, and a warning is logged once at startup. The field will become required in a future release.

Setting it to a name your messages don't carry is the trap to watch for, because the config looks protective and isn't. Messages without the field are treated as new, and the warning is logged **once per processor** — not once per message. So a name that *no* message ever carries, which is what happens if you reach for a payload field such as `timestamp_ms`, buys you a single line at startup and no protection at all for the rest of the run. Check `cache_dedup_suppressed` after a deploy: if a pipeline that should be seeing retries never moves it off zero, suspect the field name before you suspect the retries.

Watch `cache_dedup_suppressed` (see [Metrics](#metrics)) to confirm it is working; in a healthy pipeline it stays at or near zero.

```yaml
input:
  uns:
    umh_topic: umh\.v1\.acme\.berlin\.assembly\..+
pipeline:
  processors:
    - nodered_js:
        cache:
          backend: persistent
          path: /var/cache/umh.db
          dedupKey: kafka_offset
        code: |
          var n = cache.exists("part_count") ? cache.get("part_count") : 0;
          n = n + 1;
          cache.set("part_count", n);
          msg.payload = { part_count: n };
          return msg;
```

In a bridge reading from OPC UA, where each message is one reading, `tag_processor` takes the field at the top level:

```yaml
- tag_processor:
    dedupKey: opcua_source_timestamp
    defaults: |
      msg.meta.location_path = "acme.berlin.assembly";
      msg.meta.data_contract = "_raw";
      msg.meta.tag_name = "temperature";
      return msg;
```

**What a retry looks like**

With the counter above, at `kafka_offset` `42`:

- First delivery. `part_count` was 41. The JavaScript reads 41, writes 42, and the message leaves with `part_count: 42`.
- Redelivery of offset `42` after a lost ACK. The JavaScript reads 42 and computes 43, but the `cache.set` is skipped, so the stored count stays at 42 — that is the guarantee. The redelivered message itself still leaves the pipeline, carrying the 43 it computed locally. **The stored value is the authoritative one.** Because delivery is at least once, a message may reach the output more than once, and a value computed on a redelivery may run ahead of what is stored.

**What `dedupKey` does not do**

- **Nothing outside the cache is held back.** HTTP calls, writes to an external database, log lines and your other metrics all happen again on the retry.
- **No value is compared against the ones before it.** Nothing checks whether an incoming reading is older or newer than what you have stored. A message that arrives late, after newer ones, counts as a new message and its writes run. If your logic needs messages in arrival order, set `pipeline.threads: 1` (see [Thread safety](#thread-safety-auto-lock)) — that orders what this processor sees. It does not order what your flow writes: the UNS output sends batches concurrently, so a flow reading them downstream can still see them out of order.
- **Dedup does not cross process boundaries.** The markers live in one cache instance, and a cache belongs to a single benthos process (see [Sharing across processors](#sharing-across-processors)). Each process decides on its own.

**Marker storage and lifetime**

In `nodered_js`, dedup markers share the cache with your own keys: **one marker per message**, counted in `cache_keys` and, on the persistent backend, in the file on disk. The only thing that removes them is `cache.ttl`, which defaults to `0s` — never expire — so a long-running flow grows by one key for every message that passes through. Setting a TTL to bound them cuts both ways: `ttl` applies to your own values too, so a TTL short enough to clear markers also expires the counter you are keeping. Pick the TTL for your state and let the markers follow.

`tag_processor` works differently: it keeps its own cache in memory, private to that processor, with no `cache` block and therefore no TTL. Markers there are never removed, nothing is written to disk, and the state they protect is lost on restart.

Never delete `__dedup__:` keys from JavaScript; that prefix is reserved.

### Counter

Counting is the pattern that most needs `dedupKey` — set it (see [Retries and duplicate messages](#retries-and-duplicate-messages-dedupkey)) before using this:

```javascript
var count = 0;
if (cache.exists("count")) { count = cache.get("count"); }
count++;
cache.set("count", count);
msg.payload = count;
return msg;
```

**Without `dedupKey` this over-counts.** A redelivered message reads the stored count and writes count+1 a second time, so one part is counted as two — and nothing warns you.

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

Nothing accumulates here, so a redelivery cannot inflate the result. But if the message carrying a difference fails to send, that difference is lost: the first attempt already moved `last_value`, so the retry compares the value against itself and reports 0. `dedupKey` does not change this. Where a missing interval matters, publish the raw value and compute the difference in a stream processor instead.

### History (last N values)

Set `dedupKey` (see [Retries and duplicate messages](#retries-and-duplicate-messages-dedupkey)) before using this:

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

**Without `dedupKey` a redelivered message is appended twice**, so the buffer holds the same reading in two slots and anything averaged over it is wrong.

### Alarm state tracking

Publish the alarm state on every message and let consumers notice when it changes. No cache needed:

```javascript
msg.meta.alarm_active = msg.payload.value > 100;
return msg;
```

**Don't latch the alarm in the cache to fire only on the transition.** That pattern looks tidier but loses alarms. It decides "did this just change?" by comparing against the stored state — and if the message carrying the "triggered" annotation fails to send, the retry finds the state already changed, reports no transition, and the alarm reaches nobody. The same happens to "cleared", leaving a consumer showing an alarm that never ends. `dedupKey` does not rescue the latching version: the branch conditions have already decided there is nothing to write, so there is no write left to suppress. Publishing the state on every message has no such gap, because the value is computed from the message alone and a retry produces exactly the same result.

### Cycle time between events

Measure from the timestamps carried by the messages, not from the clock:

```javascript
var lastMs = null;
if (cache.exists("last_event_ms")) {
  lastMs = cache.get("last_event_ms");
}
var thisMs = msg.payload.timestamp_ms;
if (lastMs !== null) {
  msg.payload.cycle_time_ms = thisMs - lastMs;
}
cache.set("last_event_ms", thisMs);
return msg;
```

**Don't reach for `Date.now()` here.** It measures when benthos handled the message, not when the event happened, so a flow that falls behind and then catches up reports cycle times that never occurred. It also makes the result unrepeatable: a redelivered message computes a different answer than the first attempt, and no `dedupKey` can fix that, because the answer depends on the clock rather than on the message. As with the previous-value pattern above, one interval is still lost if a send fails.

### Limitations

- Caches don't cross benthos process boundaries; bbolt's file lock blocks a second opener. Use an external KV to share across processes. See [Sharing across processors](#sharing-across-processors).
- The cache has no size limit and grows unboundedly if keys are never deleted. Use `cache.delete` for your own keys, or `ttl` for everything including dedup markers, which you must not delete yourself. A hard cap is planned.
- In `tag_processor`, one cache is shared across all stages (`defaults`, `conditions`, `advancedProcessing`); a value set in `defaults` is visible in `advancedProcessing` within the same message.
- Keys with the prefix `__dedup__:` are reserved for the `dedupKey` retry check. Don't write to them from your own code.

### Metrics

Each processor reports these Benthos metrics for its cache. The two size figures are sampled on a 30-second tick, so they appear only after the first one; the suppression counter updates immediately.

- `cache_keys`: number of entries currently stored
- `cache_disk_bytes`: file size on disk (`0` for the memory backend)
- `cache_dedup_suppressed`: total messages whose cache writes were suppressed because their `dedupKey` value was seen before (see [Retries and duplicate messages](#retries-and-duplicate-messages-dedupkey))

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
