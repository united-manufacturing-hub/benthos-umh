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

Key-value store for maintaining state across messages. Values persist for the lifetime of the processor. Supports any JSON-compatible type: strings, numbers, booleans, objects, arrays.

```javascript
cache.set(key, value)    // Store a value under key (string)
cache.get(key)           // Retrieve a value, returns undefined if not found
cache.delete(key)        // Remove a key
```

### Counter

```javascript
var count = cache.get("counter") || 0;
count++;
cache.set("counter", count);
msg.payload = count;
return msg;
```

### Previous value comparison

```javascript
var prev = cache.get("last_value");
var delta = (typeof prev !== "undefined") ? msg.payload.value - prev : 0;
cache.set("last_value", msg.payload.value);
msg.payload.delta = delta;
return msg;
```

### History (last N values)

```javascript
var history = cache.get("history") || [];
history.push(msg.payload.value);
if (history.length > 10) history.shift();
cache.set("history", history);
```

### Alarm state tracking

```javascript
var alarmed = cache.get("alarm_active") || false;
if (msg.payload > 100 && !alarmed) {
  cache.set("alarm_active", true);
  msg.meta.alarm = "triggered";
  return msg;
}
if (msg.payload <= 100 && alarmed) {
  cache.set("alarm_active", false);
  msg.meta.alarm = "cleared";
  return msg;
}
return msg;
```

### Configuration

The cache backend and expiration can be configured in the processor YAML. By default, the cache uses the `memory` backend with no expiration.

```yaml
pipeline:
  processors:
    - nodered_js:
        code: |
          // your code
        cache:
          backend: memory
          expiration: 0s    # 0s = no expiration (default)
```

### Limitations

- **Memory backend only** — state is lost on process restart. A persistent backend is planned.
- **No size limits** — the number of keys is bounded by your code. Use `cache.delete` to clean up unused keys.
