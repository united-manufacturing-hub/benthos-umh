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

- **Auto-Split Metrics**: Automatically splits multi-metric messages into individual metric messages
- **Data Messages Only**: Filters to only process DATA messages by default for easier UNS integration  
- **Auto-Extract Values**: Extracts clean values with quality metadata automatically
- **Auto-Set Metadata**: Enriches messages with `tag_name`, `group_id`, `edge_node_id`, `device_id` for easy tag_processor usage
- **Thread-safe alias caching**: Maintains metric name mappings per device
- **Graceful error handling**: Drops malformed payloads without stopping the pipeline

## Configuration

```yaml
pipeline:
  processors:
    - sparkplug_b_decode: {}  # All auto-features enabled by default
```

### Configuration Fields

#### Auto-Features (Enhanced)
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auto_split_metrics` | `bool` | `true` | Whether to automatically split multi-metric messages into individual metric messages. When `true`, each metric becomes a separate message with enriched metadata. |
| `data_messages_only` | `bool` | `true` | Whether to only process DATA messages (NDATA/DDATA) and drop other message types for easier UNS integration. |
| `auto_extract_values` | `bool` | `true` | Whether to automatically extract Sparkplug values and set them as the message payload with quality metadata. |

#### Traditional Options
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `drop_birth_messages` | `bool` | `false` | Whether to drop BIRTH messages after processing them for alias extraction. |
| `strict_topic_validation` | `bool` | `false` | Whether to strictly validate Sparkplug topic format. |
| `cache_ttl` | `string` | `""` | Time-to-live for alias cache entries (e.g., '1h', '30m'). Empty string means no expiration. |

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

## Sparkplug B to UNS Integration

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

## What Happens Automatically

1. **Auto-splits** multi-metric messages into individual metric messages
2. **Auto-filters** to only process DATA messages (drops BIRTH/DEATH/CMD)
3. **Auto-extracts** values with quality metadata
4. **Auto-sets** metadata: `tag_name`, `group_id`, `edge_node_id`, `device_id`
5. **Resolves** aliases to human-readable metric names
6. **Publishes** to UNS with proper UMH topic structure

**Configuration Simplification:**
- **Before**: 6 processors + 85+ lines of complex logic
- **After**: 2 processors + 25 lines of simple configuration
- **Result**: 70%+ reduction in complexity

**Resulting UMH Topics:**
- `umh.v1.[group].[edge_node].[device]._historian.sensors.temperature.[tag_name]`
- `umh.v1.[group].[edge_node].[device]._historian.actuators.motor.[tag_name]`

## Performance Considerations

- **Memory Usage**: The processor maintains an in-memory cache of aliases per device
- **Thread Safety**: All cache operations are thread-safe using read-write mutexes
- **Cache Eviction**: Consider setting `cache_ttl` in environments with many devices to prevent memory leaks

 