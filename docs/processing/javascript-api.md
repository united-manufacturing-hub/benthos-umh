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

Key-value store for maintaining state across messages. Persists across all messages for the lifetime of the Benthos process. In-memory only, lost on restart. Supports any JSON-compatible type: strings, numbers, booleans, objects, arrays.

The cache is automatic and requires no configuration.

```javascript
cache.set(key, value)           // Store a value under key (string)
cache.get(key, default)         // Retrieve a value, returns default if not found
cache.delete(key)               // Remove a key
```

`cache.get` requires two arguments. The second argument is the default value returned when the key does not exist. This prevents silent `undefined` bugs in counters, alarm flags, and other state.

```javascript
cache.get("counter", 0)         // returns 0 if missing
cache.get("alarm_active", false)// returns false if missing
cache.get("counter")            // TypeError — must provide default
```

### Counter

```javascript
var count = cache.get("count", 0);
count++;
cache.set("count", count);
msg.payload = count;
return msg;
```

### Previous value comparison

```javascript
var prev = cache.get("last_value", null);
var delta = (prev !== null) ? msg.payload.value - prev : 0;
cache.set("last_value", msg.payload.value);
msg.payload.delta = delta;
return msg;
```

### History (last N values)

```javascript
var history = cache.get("history", []);
history.push(msg.payload.value);
if (history.length > 10) history.shift();
cache.set("history", history);
return msg;
```

### Alarm state tracking

```javascript
var alarmed = cache.get("alarm_active", false);
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
var lastMs = cache.get("last_event_ms", null);
if (lastMs !== null) {
  msg.payload.cycle_time_ms = Date.now() - lastMs;
}
cache.set("last_event_ms", Date.now());
return msg;
```

### Limitations

- **In-memory only** — state is lost when the Benthos process restarts. A persistent backend is planned.
- **No size limits** — the cache grows unboundedly if keys are never deleted. Use `cache.delete` to clean up unused keys. A memory safeguard (threshold, eviction) is planned.
- **Cache scope in `tag_processor`** — the cache is shared across all stages (`defaults`, `conditions`, `advancedProcessing`). A value set in `defaults` is visible in `advancedProcessing` within the same message.
