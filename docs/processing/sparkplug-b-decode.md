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

### Strict Topic Validation

For production environments where you want to ensure only valid Sparkplug topics are processed:

```yaml
pipeline:
  processors:
    - sparkplug_b_decode:
        strict_topic_validation: true
    - log:
        level: INFO
        message: "Processed Sparkplug message: ${! meta(\"sparkplug_msg_type\") }"
```

### Recommended Usage: Sparkplug B to UNS Integration

The recommended approach for using the Sparkplug B processor is to integrate it with UMH's tag_processor and UNS output for complete data ingestion:

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
    # Step 1: Decode Sparkplug B protobuf payloads
    - sparkplug_b_decode:
        drop_birth_messages: false
        strict_topic_validation: true
        cache_ttl: "2h"

    # Step 2: Process only DATA messages for UNS
    - bloblang: |
        root = if meta("sparkplug_msg_type").contains("DATA") {
          this
        } else {
          deleted()
        }

    # Step 3: Split multi-metric messages into individual metrics
    - bloblang: |
        root = this.metrics.map_each(metric -> {
          meta("mqtt_topic"): meta("mqtt_topic"),
          meta("sparkplug_msg_type"): meta("sparkplug_msg_type"),
          meta("sparkplug_device_key"): meta("sparkplug_device_key"),
          payload: metric
        })
    
    - split: {}

    # Step 4: Transform to UMH format using tag_processor
    - tag_processor:
        defaults: |
          // Extract Sparkplug topic information
          let topic = msg.meta.mqtt_topic;
          let parts = topic.split("/");
          let group = parts[1];
          let edge_node = parts[3];
          let device = parts.length > 4 ? parts[4] : "";
          
          // Map Sparkplug groups to UMH location hierarchy
          if (group == "Factory1") {
            msg.meta.location_path = "enterprise.factory1.production.line1." + edge_node.toLowerCase();
          } else if (group == "Warehouse") {
            msg.meta.location_path = "enterprise.warehouse.logistics.zone1." + edge_node.toLowerCase();
          } else {
            msg.meta.location_path = "enterprise.unknown." + group.toLowerCase() + ".area1." + edge_node.toLowerCase();
          }
          
          // Add device if present
          if (device != "") {
            msg.meta.location_path += "." + device.toLowerCase();
          }
          
          // Set UMH metadata
          msg.meta.data_contract = "_historian";
          msg.meta.tag_name = msg.payload.name || "unknown_metric";
          
          return msg;
        
        conditions:
          # Categorize metrics by type using virtual paths
          - if: msg.payload.name && msg.payload.name.includes("Temperature")
            then: |
              msg.meta.virtual_path = "sensors.temperature";
              msg.meta.tag_name = msg.payload.name.toLowerCase().replace(/[^a-z0-9]/g, "_");
              return msg;
          
          - if: msg.payload.name && msg.payload.name.includes("Pressure")
            then: |
              msg.meta.virtual_path = "sensors.pressure";
              msg.meta.tag_name = msg.payload.name.toLowerCase().replace(/[^a-z0-9]/g, "_");
              return msg;
          
          - if: msg.payload.name && msg.payload.name.includes("Speed")
            then: |
              msg.meta.virtual_path = "actuators.motor";
              msg.meta.tag_name = msg.payload.name.toLowerCase().replace(/[^a-z0-9]/g, "_");
              return msg;
        
        advancedProcessing: |
          // Transform Sparkplug metric to UMH value format
          let metric = msg.payload;
          let value = null;
          let quality = "GOOD";
          
          // Extract value based on Sparkplug B datatype
          if (metric.is_null) {
            value = null;
            quality = "BAD";
          } else if (metric.floatValue) {
            value = metric.floatValue;
          } else if (metric.intValue) {
            value = metric.intValue;
          } else if (metric.doubleValue) {
            value = metric.doubleValue;
          } else if (metric.booleanValue) {
            value = metric.booleanValue;
          } else if (metric.stringValue) {
            value = metric.stringValue;
          } else {
            value = metric;
            quality = "UNCERTAIN";
          }
          
          // Create UMH-compatible payload
          msg.payload = {
            "value": value,
            "quality": quality,
            "timestamp_ms": Date.now()
          };
          
          return msg;

# Output to UNS (Unified Namespace)
output:
  uns: {}
```

This configuration:
1. **Receives** Sparkplug B messages from MQTT brokers
2. **Decodes** protobuf payloads and resolves metric aliases  
3. **Filters** to process only DATA messages (drops BIRTH/DEATH/CMD)
4. **Splits** multi-metric messages into individual metric messages
5. **Maps** Sparkplug topic structure to UMH location hierarchy
6. **Categorizes** metrics using virtual paths (sensors, actuators, etc.)
7. **Transforms** to UMH data format with value, quality, and timestamp
8. **Publishes** to the Unified Namespace with proper UMH topics

**Resulting UMH Topics:**
- `umh.v1.enterprise.factory1.production.line1.plc001._historian.sensors.temperature.temperature_sensor_1`
- `umh.v1.enterprise.warehouse.logistics.zone1.gateway01._historian.actuators.motor.motor_speed_rpm`

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