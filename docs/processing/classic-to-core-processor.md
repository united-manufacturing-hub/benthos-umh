# Classic to Core Processor

The `classic_to_core` processor converts UMH Historian Data Contract format messages into Core format, following the "one tag, one message, one topic" principle. This processor is essential for migrating from legacy historian data formats to modern Core data architecture.

## Overview

The Classic to Core Processor transforms single messages containing multiple values and timestamps into separate Core format messages, each containing a single value. It handles nested tag groups by flattening them into intuitive dot-notation paths and reconstructs topics according to Core conventions.

**Key Features:**

- Converts UMH Historian Data Contract to Core format
- Flattens nested tag groups using dot (`.`) separator
- Preserves metadata while updating topic-related fields
- Supports configurable field exclusion and data contracts
- Includes performance safeguards and comprehensive error handling
- Provides detailed metrics for monitoring

## When to Use

Use the `classic_to_core` processor when you need to:

- **Migrate from Historian to Core**: Converting legacy UMH Historian Data Contract messages to modern Core format
- **Normalize message structure**: Transform multi-value messages into single-value messages
- **Flatten nested data**: Convert complex tag groups into simple dot-notation paths
- **Maintain topic conventions**: Preserve UMH topic structure while converting data contracts

## Configuration

```yaml
pipeline:
  processors:
    - classic_to_core:
        timestamp_field: timestamp_ms # Field containing timestamp (default: timestamp_ms)
        target_data_contract: _raw # Target data contract. If empty, uses input's contract
        exclude_fields: [] # List of fields to exclude from conversion
        preserve_meta: true # Whether to preserve original metadata
        max_recursion_depth: 10 # Maximum recursion depth for flattening nested tag groups
        max_tags_per_message: 1000 # Maximum number of tags to extract from a single message
```

### Configuration Options

| Parameter              | Type     | Default        | Description                                                   |
| ---------------------- | -------- | -------------- | ------------------------------------------------------------- |
| `timestamp_field`      | string   | `timestamp_ms` | Field containing the timestamp value                          |
| `target_data_contract` | string   | `""`           | Target data contract. If empty, uses input's data contract    |
| `exclude_fields`       | []string | `[]`           | List of fields to exclude from conversion                     |
| `preserve_meta`        | boolean  | `true`         | Whether to preserve original metadata from source message     |
| `max_recursion_depth`  | int      | `10`           | Maximum recursion depth for flattening nested tag groups      |
| `max_tags_per_message` | int      | `1000`         | Maximum number of tags to extract from a single input message |

## Message Transformation

### Basic Conversion

The processor transforms single messages with multiple values into separate Core format messages:

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

Message 1:

- Topic: `umh.v1.acme._raw.weather.temperature`
- Payload: `{"value": 23.4, "timestamp_ms": 1717083000000}`
- Metadata: `umh_topic: umh.v1.acme._raw.weather.temperature`

Message 2:

- Topic: `umh.v1.acme._raw.weather.humidity`
- Payload: `{"value": 42.1, "timestamp_ms": 1717083000000}`
- Metadata: `umh_topic: umh.v1.acme._raw.weather.humidity`

### Tag Groups (Nested Objects)

The processor flattens nested tag groups using dot notation:

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

The processor creates 5 separate messages:

1. Topic: `umh.v1.acme._raw.cnc-mill.pos.x`

   - Payload: `{"value": 12.5, "timestamp_ms": 1670001234567}`

2. Topic: `umh.v1.acme._raw.cnc-mill.pos.y`

   - Payload: `{"value": 7.3, "timestamp_ms": 1670001234567}`

3. Topic: `umh.v1.acme._raw.cnc-mill.pos.z`

   - Payload: `{"value": 3.2, "timestamp_ms": 1670001234567}`

4. Topic: `umh.v1.acme._raw.cnc-mill.temperature`

   - Payload: `{"value": 50.0, "timestamp_ms": 1670001234567}`

5. Topic: `umh.v1.acme._raw.cnc-mill.collision`
   - Payload: `{"value": false, "timestamp_ms": 1670001234567}`

### Complex Location Paths

The processor handles complex hierarchical location paths:

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

## Advanced Configuration

### Field Exclusion

Exclude specific fields from processing:

```yaml
classic_to_core:
  timestamp_field: timestamp_ms
  target_data_contract: _raw
  exclude_fields:
    - quality_status
    - internal_id
    - _debug_info
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

**Output:** Only `temperature` will be processed; excluded fields are ignored.

### Using Input Data Contract

When `target_data_contract` is not specified, the processor uses the input's data contract:

```yaml
classic_to_core:
  timestamp_field: timestamp_ms
  # target_data_contract not specified - uses input's contract
```

**Input:**

- Topic: `umh.v1.acme._historian.weather`

**Output:**

- Topic: `umh.v1.acme._historian.weather.pressure`
- Maintains the original `_historian` data contract

### Custom Timestamp Field

Configure a different timestamp field:

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

The processor parses Classic topics and reconstructs them for Core format:

| Component     | Classic Example          | Core Example             | Description                    |
| ------------- | ------------------------ | ------------------------ | ------------------------------ |
| Prefix        | `umh.v1`                 | `umh.v1`                 | Unchanged                      |
| Location Path | `enterprise.plant1.area` | `enterprise.plant1.area` | Unchanged                      |
| Data Contract | `_historian`             | `_raw` (configurable)    | Updated based on configuration |
| Context       | `weather`                | `weather`                | Becomes virtual_path           |
| Field Name    | N/A                      | `temperature`            | Added for each field           |

**Example Transformation:**

- **Input Topic**: `umh.v1.enterprise.plant1.area._historian.weather`
- **Output Topic**: `umh.v1.enterprise.plant1.area._raw.weather.temperature`

## Metadata Handling

The processor sets the following metadata fields:

**Generated Metadata Fields:**

- `topic`: The new Core topic
- `umh_topic`: Same as topic (enables direct use with `uns_output`)
- `location_path`: Extracted from original topic
- `data_contract`: The target data contract (or input's contract if not specified)
- `tag_name`: The field name
- `virtual_path`: Original context (if present)

**Metadata Preservation:**
When `preserve_meta: true` (default), original metadata is preserved alongside new fields.

## Performance & Reliability

### Built-in Safeguards

The processor includes several safeguards for production use:

- **Recursion Limit**: `max_recursion_depth` prevents stack overflow from deeply nested tag groups
- **Message Size Limit**: `max_tags_per_message` prevents memory exhaustion from overly large messages
- **Topic Validation**: Strict UMH v1 topic format validation with proper error handling
- **Comprehensive Metrics**: Tracks processing counts, errors, and limit violations for monitoring

### Default Limits

All limits are configurable with sensible defaults suitable for most industrial use cases:

- **Max Recursion Depth**: 10 levels (handles complex nested structures)
- **Max Tags Per Message**: 1000 tags (suitable for large industrial datasets)

### Performance Considerations

- **Message Expansion**: Each input message creates N output messages (N = number of data fields)
- **Memory Usage**: Metadata is copied for each output message when `preserve_meta: true`
- **Processing Overhead**: Minimal - efficient string parsing with optimized allocations

## Error Handling

The processor handles various error conditions gracefully:

| Error Condition              | Behavior                      | Metrics               |
| ---------------------------- | ----------------------------- | --------------------- |
| Missing timestamp field      | Message skipped, error logged | `messages_errored`    |
| Invalid JSON                 | Message skipped, error logged | `messages_errored`    |
| Missing topic metadata       | Message skipped, error logged | `messages_errored`    |
| Invalid topic format         | Message skipped, error logged | `messages_errored`    |
| Unsupported timestamp format | Message skipped, error logged | `messages_errored`    |
| Recursion depth exceeded     | Flattening stopped at limit   | `recursion_limit_hit` |
| Too many tags                | Processing stopped at limit   | `tag_limit_exceeded`  |

## Metrics

The processor exposes comprehensive metrics for monitoring:

| Metric                | Type    | Description                             |
| --------------------- | ------- | --------------------------------------- |
| `messages_processed`  | Counter | Total input messages processed          |
| `messages_errored`    | Counter | Messages that failed processing         |
| `messages_expanded`   | Counter | Total output messages created           |
| `messages_dropped`    | Counter | Messages dropped due to configuration   |
| `recursion_limit_hit` | Counter | Times recursion depth limit was reached |
| `tag_limit_exceeded`  | Counter | Times tag limit was exceeded            |

## Complete Integration Example

Here's a complete Benthos configuration for migrating from Historian to Core format:

```yaml
input:
  kafka:
    addresses: ['kafka:9092']
    topics: ['umh.v1.+.+.+._historian.+']
    consumer_group: historian-to-core-migration

pipeline:
  processors:
    - classic_to_core:
        timestamp_field: timestamp_ms
        # Uses input data contract (_historian) if not specified
        preserve_meta: true
        exclude_fields:
          - _quality
          - _debug_info
        max_recursion_depth: 10
        max_tags_per_message: 1000

output:
  # Use UNS output for seamless integration
  uns_output:
    kafka:
      addresses: ['kafka:9092']
      key: ${! meta("location_path") }-${! meta("tag_name") }
      compression: snappy
      max_message_bytes: 1MB
```

**This configuration:**

1. Consumes all Classic `_historian` topics
2. Converts them to Core format using the original `_historian` data contract
3. Excludes quality and debug fields
4. Publishes to individual Core topics via `uns_output`
5. Uses `umh_topic` metadata for automatic topic routing
6. Preserves all metadata for downstream processing

## Migration Strategy

### Recommended Migration Approach

1. **Side-by-side deployment**: Run Classic and Core systems in parallel
2. **Gradual migration**: Convert topics one at a time
3. **Validation**: Compare output between systems
4. **Switch consumers**: Update consumers to use Core topics
5. **Decommission Classic**: Remove Classic systems once validated

### Testing Your Migration

Before deploying to production:

```bash
# Run unit tests
TEST_CLASSIC_TO_CORE=1 go test ./classic_to_core_plugin/...

# Test with sample data
benthos -c migration-config.yaml --log.level=debug
```

## Troubleshooting

### Common Issues

**No output messages**

- Check that input has valid JSON with timestamp field
- Verify topic metadata is present
- Ensure input topics follow Classic format: `umh.v1.<location>._historian.<context>`

**Missing fields**

- Verify `exclude_fields` configuration
- Check logs for parsing errors
- Ensure fields are not nested beyond `max_recursion_depth`

**Wrong topics**

- Validate input topic format
- Check `target_data_contract` configuration
- Verify UMH v1 topic structure

**Timestamp errors**

- Check that timestamp field contains numeric values (int, float, or string numbers)
- Verify timestamp field name matches configuration
- Ensure timestamp values are valid Unix timestamps

### Debug Configuration

For troubleshooting, enable debug logging:

```yaml
logger:
  level: DEBUG
  format: json

input:
  # ... your input config

pipeline:
  processors:
    - classic_to_core:
        # ... your processor config

output:
  # For debugging, you can output to stdout first
  stdout:
    codec: lines
```

This will show detailed processing information including:

- Topic parsing results
- Field extraction details
- Metadata transformations
- Error conditions and reasons
