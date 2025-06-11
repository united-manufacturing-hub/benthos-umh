# Classic to Core Processor Plugin

The `classic_to_core` processor converts UMH Historian Data Contract format messages into Core format, following the "one tag, one message, one topic" principle.

## Overview

This processor helps migrate from UMH Historian Data Contract to Core format by:

- Taking single messages with multiple values/tag groups + timestamp
- Flattening nested tag groups using `.` (dot) separator for intuitive paths
- Creating separate messages for each tag
- Reconstructing topics according to Core conventions
- Preserving metadata while updating topic-related fields

## Configuration

```yaml
classic_to_core:
  timestamp_field: timestamp_ms # Field containing timestamp (default: timestamp_ms)
  target_data_contract: _raw # Target data contract. If empty, uses input's contract
  exclude_fields: [] # List of fields to exclude from conversion
  preserve_meta: true # Whether to preserve original metadata
  max_recursion_depth: 10 # Maximum recursion depth for flattening nested tag groups
  max_tags_per_message: 1000 # Maximum number of tags to extract from a single message
```

## Examples

### Basic Conversion

**Input (Historian Data Contract):**

- Topic: `umh.v1.acme._historian.weather`
- Payload:

```json
{
  "timestamp_ms": 1717083000000,
  "temperature": 23.4,
  "humidity": 42.1
}
```

**Output (Core Format):**

- Topic: `umh.v1.acme._raw.weather.temperature`
- Payload: `{"value": 23.4, "timestamp_ms": 1717083000000}`
- Metadata: `umh_topic: umh.v1.acme._raw.weather.temperature`

- Topic: `umh.v1.acme._raw.weather.humidity`
- Payload: `{"value": 42.1, "timestamp_ms": 1717083000000}`
- Metadata: `umh_topic: umh.v1.acme._raw.weather.humidity`

### Tag Groups (Nested Objects)

**Input (Historian Data Contract with Tag Groups):**

- Topic: `umh.v1.acme._historian.cnc-mill`
- Payload:

```json
{
  "timestamp_ms": 1670001234567,
  "pos": {
    "x": 12.5,
    "y": 7.3,
    "z": 3.2
  },
  "temperature": 50.0,
  "collision": false
}
```

**Output (Core Format with Flattened Tags):**

- Topic: `umh.v1.acme._raw.cnc-mill.pos.x`
- Payload: `{"value": 12.5, "timestamp_ms": 1670001234567}`

- Topic: `umh.v1.acme._raw.cnc-mill.pos.y`
- Payload: `{"value": 7.3, "timestamp_ms": 1670001234567}`

- Topic: `umh.v1.acme._raw.cnc-mill.pos.z`
- Payload: `{"value": 3.2, "timestamp_ms": 1670001234567}`

- Topic: `umh.v1.acme._raw.cnc-mill.temperature`
- Payload: `{"value": 50.0, "timestamp_ms": 1670001234567}`

- Topic: `umh.v1.acme._raw.cnc-mill.collision`
- Payload: `{"value": false, "timestamp_ms": 1670001234567}`

> **Note**: Tag groups are flattened using `.` (dot) separator, creating intuitive dot-notation paths for nested structures.

### Complex Location Paths

**Input:**

- Topic: `umh.v1.enterprise.plant1.machining.cnc-line.cnc5._historian.axis.position`
- Payload:

```json
{
  "timestamp_ms": 1717083000000,
  "x_position": 125.7,
  "y_position": 89.3
}
```

**Output:**

- Topic: `umh.v1.enterprise.plant1.machining.cnc-line.cnc5._raw.axis.position.x_position`
- Payload: `{"value": 125.7, "timestamp_ms": 1717083000000}`

- Topic: `umh.v1.enterprise.plant1.machining.cnc-line.cnc5._raw.axis.position.y_position`
- Payload: `{"value": 89.3, "timestamp_ms": 1717083000000}`

### Excluding Fields

```yaml
classic_to_core:
  timestamp_field: timestamp_ms
  target_data_contract: _raw
  exclude_fields:
    - quality_status
    - internal_id
```

**Input:**

```json
{
  "timestamp_ms": 1717083000000,
  "temperature": 23.4,
  "quality_status": "OK",
  "internal_id": "sensor_123"
}
```

**Output:** Only `temperature` will be processed; `quality_status` and `internal_id` are excluded.

### Using Input Data Contract

```yaml
classic_to_core:
  timestamp_field: timestamp_ms
  # target_data_contract not specified - uses input's _historian
```

**Input:**

- Topic: `umh.v1.acme._historian.weather`

```json
{
  "timestamp_ms": 1717083000000,
  "pressure": 1013.25
}
```

**Output:**

- Topic: `umh.v1.acme._historian.weather.pressure`
- Payload: `{"value": 1013.25, "timestamp_ms": 1717083000000}`
- Metadata: `umh_topic: umh.v1.acme._historian.weather.pressure`

### Custom Timestamp Field

```yaml
classic_to_core:
  timestamp_field: custom_timestamp
  target_data_contract: _processed
```

**Input:**

```json
{
  "custom_timestamp": "1717083000000",
  "pressure": 1013.25
}
```

**Output:**

- Topic: `umh.v1.acme._processed.weather.pressure`
- Payload: `{"value": 1013.25, "timestamp_ms": 1717083000000}`

## Topic Transformation

The processor parses Classic topics and reconstructs them for Core:

| Component     | Classic Example          | Core Example                     |
| ------------- | ------------------------ | -------------------------------- |
| Prefix        | `umh.v1`                 | `umh.v1`                         |
| Location Path | `enterprise.plant1.area` | `enterprise.plant1.area`         |
| Data Contract | `_historian`             | `_raw` (configurable)            |
| Context       | `weather`                | `weather` (becomes virtual_path) |
| Field Name    | N/A                      | `temperature`                    |

## Metadata Handling

The processor sets the following metadata fields:

- `topic`: The new Core topic
- `umh_topic`: Same as topic (enables direct use with `uns_output`)
- `location_path`: Extracted from original topic
- `data_contract`: The target data contract (or input's contract if not specified)
- `tag_name`: The field name
- `virtual_path`: Original context (if present)

When `preserve_meta: true` (default), original metadata is preserved alongside new fields.

## Performance & Reliability

The processor includes several safeguards for production use:

- **Recursion Limit**: `max_recursion_depth` prevents stack overflow from deeply nested tag groups
- **Message Size Limit**: `max_tags_per_message` prevents memory exhaustion from overly large messages
- **Topic Validation**: Strict UMH v1 topic format validation with proper error handling
- **Comprehensive Metrics**: Tracks processing counts, errors, and limit violations for monitoring

All limits are configurable with sensible defaults (10 levels, 1000 tags) suitable for most industrial use cases.

## Error Handling

The processor handles various error conditions gracefully:

- Missing timestamp field → message skipped, error logged
- Invalid JSON → message skipped, error logged
- Missing topic metadata → message skipped, error logged
- Invalid topic format → message skipped, error logged
- Unsupported timestamp format → message skipped, error logged

## Performance Considerations

- **Message Expansion**: Each input message creates N output messages (N = number of data fields)
- **Memory Usage**: Metadata is copied for each output message when `preserve_meta: true`
- **Topic Parsing**: Efficient string parsing with minimal allocations

## Metrics

The processor exposes the following metrics:

- `messages_processed`: Total input messages processed
- `messages_errored`: Messages that failed processing
- `messages_expanded`: Total output messages created
- `messages_dropped`: Messages dropped due to configuration

## Integration Example

Complete Benthos configuration for migration:

```yaml
input:
  kafka:
    addresses: ['kafka:9092']
    topics: ['umh.v1.+.+.+._historian.+']

pipeline:
  processors:
    - classic_to_core:
        timestamp_field: timestamp_ms
        # Uses input data contract (_historian) if not specified
        preserve_meta: true

output:
  # Use UNS output for seamless integration
  uns_output:
    kafka:
      addresses: ['kafka:9092']
      key: ${! meta("location_path") }-${! meta("tag_name") }
```

This configuration:

1. Consumes all Classic `_historian` topics
2. Converts them to Core format using the original `_historian` data contract
3. Publishes to individual Core topics via `uns_output`
4. Uses `umh_topic` metadata for automatic topic routing
5. Preserves all metadata

## Testing

Run tests with:

```bash
TEST_CLASSIC_TO_CORE=1 go test ./classic_to_core_plugin/...
```

## Migration Strategy

1. **Side-by-side deployment**: Run Classic and Core systems in parallel
2. **Gradual migration**: Convert topics one at a time
3. **Validation**: Compare output between systems
4. **Switch consumers**: Update consumers to use Core topics
5. **Decommission Classic**: Remove Classic systems once validated

## Troubleshooting

**No output messages:** Check that input has valid JSON with timestamp field and topic metadata.

**Missing fields:** Verify `exclude_fields` configuration and check logs for parsing errors.

**Wrong topics:** Ensure input topics follow Classic format: `umh.v1.<location>._historian.<context>`

**Timestamp errors:** Check that timestamp field contains numeric values (int, float, or string numbers).
