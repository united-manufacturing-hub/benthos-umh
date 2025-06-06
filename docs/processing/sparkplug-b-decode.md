# Sparkplug B Decode Processor

The Sparkplug B decode processor is designed to decode MQTT messages containing Sparkplug B protobuf payloads and resolve metric aliases using BIRTH packets. This processor is essential for working with Sparkplug B data in the UMH data flow, enabling the transformation of compact, alias-based DATA messages into fully enriched JSON messages with metric names.

## Overview

Sparkplug B is an open standard for MQTT-based industrial IoT communication that uses protobuf encoding and alias-based metric naming to minimize bandwidth usage. The protocol defines several message types:

- **NBIRTH/DBIRTH**: Node/Device birth messages that establish metric definitions and aliases
- **NDATA/DDATA**: Node/Device data messages that use aliases to reference metrics
- **NDEATH/DDEATH**: Node/Device death messages indicating disconnection
- **NCMD/DCMD**: Node/Device command messages for control operations

This processor maintains an in-memory cache of metric name aliases from BIRTH messages and uses these to resolve aliases in DATA messages, enriching them with human-readable metric names.

## Key Features

- **Thread-safe alias caching**: Maintains metric name mappings per device
- **Automatic alias resolution**: Enriches DATA messages with resolved metric names
- **Graceful error handling**: Drops malformed payloads without stopping the pipeline
- **Configurable behavior**: Options for dropping BIRTH messages and strict topic validation
- **JSON output**: Converts protobuf payloads to clean JSON format
- **Metadata enrichment**: Adds Sparkplug-specific metadata to processed messages

## Configuration

```yaml
pipeline:
  processors:
    - sparkplug_b_decode:
        drop_birth_messages: false
        strict_topic_validation: false
        cache_ttl: ""
```

### Configuration Fields

#### Auto-Features (Enhanced)
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auto_split_metrics` | `bool` | `true` | Whether to automatically split multi-metric messages into individual metric messages. When `true`, each metric becomes a separate message with enriched metadata. When `false`, the original message structure is preserved. |
| `data_messages_only` | `bool` | `true` | Whether to only process DATA messages (NDATA/DDATA) and drop other message types. When `true`, only DATA messages are processed for easier UNS integration. When `false`, all message types are processed according to other configuration options. |
| `auto_extract_values` | `bool` | `true` | Whether to automatically extract Sparkplug values and set them as the message payload. When `true`, the actual metric value is extracted and set as payload.value. When `false`, the full metric object is preserved in the payload. |

#### Traditional Options
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `drop_birth_messages` | `bool` | `false` | Whether to drop BIRTH messages after processing them for alias extraction. When `false`, BIRTH messages are passed through as JSON. When `true`, only their alias information is cached and the messages are dropped. |
| `strict_topic_validation` | `bool` | `false` | Whether to strictly validate Sparkplug topic format. When `true`, messages with invalid topic formats are dropped. When `false`, messages with invalid topics are passed through unchanged. |
| `cache_ttl` | `string` | `""` | Time-to-live for alias cache entries (e.g., '1h', '30m'). Empty string means no expiration. This helps prevent memory leaks when devices restart with new aliases. |

## Message Flow

### 1. BIRTH Messages (NBIRTH/DBIRTH)
- Extract alias → metric name mappings
- Store mappings in thread-safe cache per device
- Pass through as JSON (unless `drop_birth_messages` is `true`)

### 2. DATA Messages (NDATA/DDATA)
- Look up cached aliases for the device
- Replace alias numbers with human-readable metric names
- Output enriched JSON payload

### 3. Other Message Types
- Pass through as JSON without alias processing

## Topic Structure

The processor expects MQTT topics following the Sparkplug B specification:

```
spBv1.0/<Group>/<MsgType>/<EdgeNode>[/<Device>]
```

Where:
- `spBv1.0`: Sparkplug B namespace
- `<Group>`: Logical grouping of edge nodes
- `<MsgType>`: Message type (NBIRTH, NDATA, DBIRTH, DDATA, etc.)
- `<EdgeNode>`: Edge node identifier
- `<Device>`: Optional device identifier

## Examples

### Basic Usage

```yaml
input:
  mqtt:
    urls: ["tcp://broker:1883"]
    topics: ["spBv1.0/Factory/#"]
    client_id: benthos-sparkplug

pipeline:
  processors:
    - sparkplug_b_decode: {}

output:
  stdout: {}
```

### With BIRTH Message Filtering

If you only want to process DATA messages and cache aliases without outputting BIRTH messages:

```yaml
pipeline:
  processors:
    - sparkplug_b_decode:
        drop_birth_messages: true
    - bloblang: |
        # Only DATA messages will reach this point
        root = this.metrics
```

### Legacy Mode (Preserve Original Behavior)

To maintain the original behavior without auto-features:

```yaml
pipeline:
  processors:
    - sparkplug_b_decode:
        auto_split_metrics: false      # Return full payload with metrics array
        data_messages_only: false      # Process all message types  
        auto_extract_values: false     # Keep full metric objects
        strict_topic_validation: true
    - log:
        level: INFO
        message: "Processed Sparkplug message: ${! meta(\"sparkplug_msg_type\") }"
```

### Recommended Usage: Simplified Sparkplug B to UNS Integration

With the enhanced auto-features, integrating Sparkplug B data with UNS becomes dramatically simpler:

```yaml
input:
  mqtt:
    urls: ["tcp://localhost:1883"]
    topics: 
      - "spBv1.0/+/+/+"     # Node-level topics
      - "spBv1.0/+/+/+/+"   # Device-level topics
    client_id: benthos-sparkplug-to-uns
    qos: 1

pipeline:
  processors:
    # Step 1: Decode Sparkplug B with auto-features (handles everything automatically!)
    - sparkplug_b_decode: {}  # All auto-features enabled by default
    
    # Step 2: Transform to UMH format (much simpler with auto-extracted metadata!)
    - tag_processor:
        defaults: |
          # tag_name, device_id, group_id, edge_node_id already set by sparkplug_b_decode
          msg.meta.data_contract = "_historian";
          
          # Build location path from auto-extracted metadata
          msg.meta.location_path = msg.meta.group_id + "." + msg.meta.edge_node_id;
          if msg.meta.device_id != "" {
            msg.meta.location_path = msg.meta.location_path + "." + msg.meta.device_id;
          }
          
          # Set default virtual path
          msg.meta.virtual_path = "sensors.generic";
          return msg;
        
        conditions:
          # Simple categorization using auto-extracted tag_name
          - if: msg.meta.tag_name && msg.meta.tag_name.includes("temp")
            then: |
              msg.meta.virtual_path = "sensors.temperature";
              return msg;
          
          - if: msg.meta.tag_name && msg.meta.tag_name.includes("press")
            then: |
              msg.meta.virtual_path = "sensors.pressure";
              return msg;
          
          - if: msg.meta.tag_name && msg.meta.tag_name.includes("speed")
            then: |
              msg.meta.virtual_path = "actuators.motor";
              return msg;

output:
  uns: {}
```

**What happens automatically:**
1. **Auto-splits** multi-metric messages into individual metric messages
2. **Auto-filters** to only process DATA messages (drops BIRTH/DEATH/CMD)
3. **Auto-extracts** values with quality metadata
4. **Auto-sets** metadata: `tag_name`, `group_id`, `edge_node_id`, `device_id`
5. **Resolves** aliases to human-readable metric names
6. **Publishes** to UNS with proper UMH topic structure

**Comparison: 70%+ simpler configuration**
- **Before**: 6 processors + 85+ lines of complex logic
- **After**: 2 processors + 25 lines of simple configuration

**Resulting UMH Topics:**
- `umh.v1.[group].[edge_node].[device]._historian.sensors.temperature.[tag_name]`
- `umh.v1.[group].[edge_node].[device]._historian.actuators.motor.[tag_name]`

## Input/Output Format

### Input
- **Message Body**: Raw Sparkplug B protobuf payload
- **Metadata**: Must include `mqtt_topic` with valid Sparkplug topic format

### Output
- **Message Body**: JSON representation of the Sparkplug payload with resolved aliases
- **Metadata**: Original metadata plus:
  - `sparkplug_msg_type`: The message type (NBIRTH, NDATA, etc.)
  - `sparkplug_device_key`: Device key used for alias caching

### Example Input/Output

**NBIRTH Input** (protobuf):
```
timestamp: 1640995200000
metrics: [
  {
    name: "Temperature"
    alias: 1
    datatype: 9  // Float
    float_value: 23.5
  },
  {
    name: "Pressure" 
    alias: 2
    datatype: 9  // Float
    float_value: 1013.25
  }
]
```

**NBIRTH Output** (JSON):
```json
{
  "timestamp": "1640995200000",
  "metrics": [
    {
      "name": "Temperature",
      "alias": "1", 
      "datatype": 9,
      "floatValue": 23.5
    },
    {
      "name": "Pressure",
      "alias": "2",
      "datatype": 9, 
      "floatValue": 1013.25
    }
  ]
}
```

**NDATA Input** (protobuf):
```
timestamp: 1640995260000
metrics: [
  {
    alias: 1  // References "Temperature"
    datatype: 9
    float_value: 24.1
  },
  {
    alias: 2  // References "Pressure"
    datatype: 9
    float_value: 1012.8
  }
]
```

**NDATA Output** (JSON with resolved aliases):
```json
{
  "timestamp": "1640995260000", 
  "metrics": [
    {
      "name": "Temperature",
      "alias": "1",
      "datatype": 9,
      "floatValue": 24.1
    },
    {
      "name": "Pressure", 
      "alias": "2",
      "datatype": 9,
      "floatValue": 1012.8
    }
  ]
}
```

## Performance Considerations

- **Memory Usage**: The processor maintains an in-memory cache of aliases per device. Memory usage scales with the number of devices and metrics per device.
- **Thread Safety**: All cache operations are thread-safe using read-write mutexes.
- **Cache Eviction**: Consider setting `cache_ttl` in environments with many devices to prevent memory leaks.

## Error Handling

The processor handles various error conditions gracefully:

- **Missing MQTT Topic**: Messages without `mqtt_topic` metadata are passed through unchanged (unless `strict_topic_validation` is enabled)
- **Invalid Topic Format**: Invalid Sparkplug topics are passed through unchanged or dropped based on configuration
- **Malformed Protobuf**: Invalid protobuf payloads are dropped to prevent pipeline backup
- **Unknown Aliases**: DATA messages with unknown aliases leave the alias numeric value unchanged

## Monitoring

The processor exposes several metrics for monitoring:

- `messages_processed`: Total messages successfully processed
- `messages_dropped`: Total messages dropped due to errors or configuration
- `messages_errored`: Total messages that encountered errors
- `birth_messages_cached`: Total BIRTH messages that contributed to alias cache
- `alias_resolutions`: Total alias resolutions performed
- `topic_parse_errors`: Total topic parsing errors

## Use Cases

### Industrial IoT Data Collection
Process Sparkplug B data from industrial devices and PLCs, converting compact alias-based messages to human-readable JSON for downstream analytics.

### MQTT to Time Series Database
Decode Sparkplug payloads and transform them for ingestion into time series databases like InfluxDB or TimescaleDB.

### Real-time Monitoring Dashboards
Convert Sparkplug data to JSON format suitable for real-time visualization in dashboards and monitoring systems.

### Data Lake Ingestion
Process and enrich Sparkplug data before storing in data lakes for historical analysis and machine learning.

## Troubleshooting

### Common Issues

**Aliases not resolving in DATA messages**
- Ensure BIRTH messages are processed before DATA messages
- Check that the device key (derived from topic) matches between BIRTH and DATA messages
- Verify that BIRTH messages contain both `name` and `alias` fields

**Messages being dropped unexpectedly**
- Check if `strict_topic_validation` is enabled and topics are valid
- Verify protobuf payloads are valid Sparkplug B format
- Monitor error logs for specific error messages

**Memory usage growing over time**
- Consider setting `cache_ttl` to expire old aliases
- Monitor the number of unique device keys in the cache
- Implement cache size limits if needed

### Debug Configuration

Enable debug logging to troubleshoot issues:

```yaml
pipeline:
  processors:
    - sparkplug_b_decode:
        strict_topic_validation: true
    - log:
        level: DEBUG
        message: |
          Sparkplug message processed:
          Type: ${! meta("sparkplug_msg_type") }
          Device: ${! meta("sparkplug_device_key") }
          Payload: ${! content() }
```

## Related Processors

- **[Tag Processor](tag-processor.md)**: For converting processed Sparkplug data to UMH data model
- **[Node-RED JavaScript Processor](node-red-javascript-processor.md)**: For custom JavaScript-based transformations
- **MQTT Input**: For receiving Sparkplug B messages from MQTT brokers 