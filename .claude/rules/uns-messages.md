---
paths:
  - "tag_processor_plugin/**"
  - "uns_plugin/**"
  - "nodered_js_plugin/**"
  - "topic_browser_plugin/**"
  - "pkg/umh/topic/**"
  - "docs/processing/**"
  - "docs/output/uns-output.md"
  - "docs/input/uns-input.md"
---

# UNS messages, topics and the tag processor

## Message shape

Processors use a Node-RED style message: `msg.payload` holds the data and `msg.meta` holds metadata.

```javascript
{
  "payload": { "value": 42, "timestamp_ms": 1730986400000 },
  "meta": {
    "location_path": "enterprise.site.area.line",
    "data_contract": "_raw",
    "tag_name": "temperature",
    "virtual_path": "motor.electrical",   // optional
    "umh_topic": "umh.v1.enterprise.site.area.line._raw.motor.electrical.temperature"
  }
}
```

`msg.meta` becomes Benthos metadata, not part of the payload. The UNS output writes Benthos metadata as Kafka headers.

## Value wrapping

`tag_processor` always builds the payload `{value, timestamp_ms}` (`constructFinalMessage`). Without a `datatype` metadata field, `autoConvertValue` decides the value:

```javascript
// Input:  msg.payload = 42
// Output: msg.payload = {value: 42, timestamp_ms: 1730986400000}

// Input:  msg.payload = {temperature: 42, pressure: 100}
// Output: msg.payload = {value: "{\"pressure\":100,\"temperature\":42}", timestamp_ms: …}  // objects and arrays are JSON-encoded into value
```

`timestamp_ms` comes from the `timestamp_ms` metadata field when it parses, otherwise from the current time.

## Topics

`tag_processor` builds `umh_topic` from `location_path`, `data_contract`, optional `virtual_path` and `tag_name` with the builder in `pkg/umh/topic` (`constructUMHTopic`). Never set `umh_topic` by hand.

```
umh.v1.<location_path>.<data_contract>[.<virtual_path>].<name>
```

| Part | Underscore rule | Example |
|---|---|---|
| `location_path` | must not start with `_` | `enterprise.site.area` |
| `data_contract` | must start with `_` | `_raw`, `_pump_v1` |
| `virtual_path` | may start with `_` | `motor.electrical`, `_hidden` |
| `name` | may start with `_` | `temperature`, `_internal` |

## One tag, one topic

Each sensor or tag publishes to its own topic. Do not combine several sensors into one payload under one `tag_name`. Emit one message per tag instead:

```javascript
// Wrong: msg.payload = {temperature: 42, pressure: 100}; msg.meta.tag_name = "machine_01";
// Right: one message per tag
msg.payload = {value: 42, timestamp_ms: 1730986400000}; msg.meta.tag_name = "temperature";
```

Time-series data (`{timestamp_ms, value}`) and relational data (any JSON, e.g. work orders) do not share a topic. Use different `data_contract` values.

## The typical bridge

```yaml
input:
  s7comm:
    tcpDevice: "{{ .IP }}"
    addresses: ["DB1.DW20", "DB3.I270"]
pipeline:
  processors:
    - tag_processor:
        defaults: |
          msg.meta.location_path = "{{ .location_path }}";
          msg.meta.data_contract = "_raw";
          msg.meta.tag_name = msg.meta.s7_address;
          return msg;
output:
  uns: {}
```

The `{{ .… }}` variables are expanded by umh-core before benthos starts.

## The UNS output and its single Kafka topic

The `uns` output writes every message to one Kafka topic, `umh.messages` (`defaultOutputTopic` in `uns_plugin/uns_output.go`). The Kafka key is the `umh_topic` value. Consumers filter by key.

The `uns` output only works inside umh-core, because it expects umh-core's embedded Redpanda broker. Outside umh-core, use the standard `kafka` output with `topic: "umh.messages"` and `key: "${! @umh_topic }"`.
