# Classic to Core Processor

The `classic_to_core` processor converts UMH Historian schema format messages into Core format, following the "one tag, one message, one topic" principle. This processor is essential for migrating from Classic historian schemas to Core data architecture.

## Overview

The Classic to Core Processor transforms single messages containing multiple values and timestamps into separate Core format messages, each containing a single value. It handles nested tag groups by flattening them into intuitive dot-notation paths and reconstructs topics according to Core conventions.

## When to Use

Use the `classic_to_core` processor when you need to:

- **Process Classic Data in Core**: Convert Classic UMH Historian schema messages to Core format for processing in Core-based systems
- **Use Core Processors**: Enable the use of processors that rely on the Core data model (like downsampler)

The processor maintains compatibility with the `_historian` schema, ensuring that the output can be consumed by systems expecting Classic format messages.

## Configuration

```yaml
pipeline:
  processors:
    - classic_to_core: {}
```

### Configuration Options

| Parameter              | Type   | Default | Description                                         |
| ---------------------- | ------ | ------- | --------------------------------------------------- |
| `target_data_contract` | string | `""`    | Target data contract. If empty, uses input's schema |

## Message Transformation

### Basic Conversion

The processor transforms single messages with multiple values into separate Core format messages:

**Input (Historian Schema):**

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

**Input (Historian Schema with Tag Groups):**

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

## Advanced Configuration

### Using Input Schema

When `target_data_contract` is not specified, the processor uses the input's schema:

```yaml
classic_to_core:
  # target_data_contract not specified - uses input's schema
```

**Input:**

- Topic: `umh.v1.acme._historian.weather`

**Output:**

- Topic: `umh.v1.acme._historian.weather.pressure`
- Maintains the original `_historian` schema

## Topic Transformation

The processor parses Classic topics and reconstructs them for Core format:

| Component     | Classic Example          | Core Example             | Description                    |
| ------------- | ------------------------ | ------------------------ | ------------------------------ |
| Prefix        | `umh.v1`                 | `umh.v1`                 | Unchanged                      |
| Location Path | `enterprise.plant1.area` | `enterprise.plant1.area` | Unchanged                      |
| Schema        | `_historian`             | `_raw` (configurable)    | Updated based on configuration |
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
- `data_contract`: The target data contract (or input's schema if not specified)
- `tag_name`: The field name
- `virtual_path`: Original context (if present)

**Metadata Preservation:**
Original metadata is always preserved alongside new fields.

## Performance & Reliability

### Built-in Safeguards

The processor includes several safeguards for production use:

- **Recursion Limit**: Maximum recursion depth of 10 levels for flattening nested tag groups
- **Message Size Limit**: Maximum of 1000 tags per message to prevent memory exhaustion
- **Topic Validation**: Strict UMH v1 topic format validation with proper error handling
- **Comprehensive Metrics**: Tracks processing counts, errors, and limit violations for monitoring

### Performance Considerations

- **Message Expansion**: Each input message creates N output messages (N = number of data fields)
- **Memory Usage**: Metadata is copied for each output message
- **Processing Overhead**: Minimal - efficient string parsing with optimized allocations

## Error Handling

The processor includes comprehensive error handling and logging for non-standard messages. Each error condition is logged with detailed information to help diagnose issues:

| Error Condition          | Log Message Example                                          | Behavior                      | Metrics               |
| ------------------------ | ------------------------------------------------------------ | ----------------------------- | --------------------- |
| Invalid JSON             | "failed to parse as structured data: ..."                    | Message skipped, error logged | `messages_errored`    |
| Missing timestamp_ms     | "timestamp field 'timestamp_ms' not found in payload"        | Message skipped, error logged | `messages_errored`    |
| Invalid timestamp format | "failed to parse timestamp: ..."                             | Message skipped, error logged | `messages_errored`    |
| Missing topic metadata   | "no topic found in message metadata"                         | Message skipped, error logged | `messages_errored`    |
| Invalid topic format     | "invalid topic structure, expected at least 4 parts: ..."    | Message skipped, error logged | `messages_errored`    |
| Invalid UMH prefix       | "invalid UMH topic prefix, expected 'umh.v1': ..."           | Message skipped, error logged | `messages_errored`    |
| Missing data contract    | "no data contract found in topic: ..."                       | Message skipped, error logged | `messages_errored`    |
| Missing location path    | "missing location path in topic: ..."                        | Message skipped, error logged | `messages_errored`    |
| Recursion depth exceeded | "Maximum recursion depth of 10 reached, stopping flattening" | Flattening stopped at limit   | `recursion_limit_hit` |
| Too many tags            | "Message exceeds maximum tag limit of 1000, truncating"      | Processing stopped at limit   | `tag_limit_exceeded`  |

All error messages include the original message content and specific details about what went wrong, making it easy to identify and fix issues with non-standard messages.

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

These metrics can be used to monitor the health of the processor and identify patterns of non-standard messages that need attention.

## Complete Integration Example

Here's a complete Benthos configuration for migrating from Classic to Core format:

```yaml
input:
  kafka:
    addresses: ['kafka:9092']
    topics: ['umh.v1.+.+.+._historian.+']
    consumer_group: historian-to-core-migration

pipeline:
  processors:
    - classic_to_core: {}

output:
  # Use UNS output for seamless integration
  uns_output: {}
```

**This configuration:**

1. Consumes all Classic `_historian` topics
2. Converts them to Core format using the `_raw` data contract
3. Publishes to individual Core topics via `uns_output`
4. Uses `umh_topic` metadata for automatic topic routing
5. Preserves all metadata for downstream processing

## Troubleshooting

### Common Issues

**No output messages**

- Check that input has valid JSON with timestamp_ms field
- Verify topic metadata is present
- Ensure input topics follow Classic format: `umh.v1.<location>._historian.<context>`

**Missing fields**

- Check logs for parsing errors
- Ensure fields are not nested beyond maximum recursion depth (10 levels)

**Wrong topics**

- Validate input topic format
- Check `target_data_contract` configuration
- Verify UMH v1 topic structure

**Timestamp errors**

- Check that timestamp_ms field contains numeric values (int, float, or string numbers)
- Ensure timestamp values are valid Unix timestamps
