# Sparkplug B (Input)

The Sparkplug B input plugin acts as a **Primary SCADA Host** in the Sparkplug B ecosystem, providing comprehensive MQTT-based industrial IoT data collection with session lifecycle management, rebirth requests, and death certificate handling.

Sparkplug B is an open standard for MQTT-based industrial IoT communication that uses protobuf encoding and hierarchical topic structures to organize edge nodes and devices. This plugin implements the Primary SCADA Host role, which is responsible for:

- Subscribing to all Sparkplug B message types from edge nodes
- Managing MQTT session lifecycle and tracking edge node states
- Automatically requesting rebirth certificates when detecting sequence gaps
- Handling death certificates and session state transitions
- Providing comprehensive metadata for downstream processing

## Sparkplug B Protocol Overview

Sparkplug B defines a hierarchical topic structure and message types:

**Topic Structure:**
```
spBv1.0/<Group>/<MsgType>/<EdgeNode>[/<Device>]
```

**Message Types:**
- **NBIRTH/DBIRTH**: Node/Device birth certificates establishing metric definitions
- **NDATA/DDATA**: Node/Device data messages with current values
- **NDEATH/DDEATH**: Node/Device death certificates indicating disconnection
- **NCMD/DCMD**: Node/Device command messages for control operations
- **STATE**: Host state messages for session management

## Key Features

- **Primary SCADA Host Role**: Complete implementation of Sparkplug B host responsibilities
- **Session Lifecycle Management**: Tracks edge node states and handles reconnections
- **Automatic Rebirth Requests**: Detects sequence number gaps and requests rebirth certificates
- **Death Certificate Handling**: Processes node/device death messages and state transitions
- **Comprehensive Metadata**: Extracts and provides rich metadata for downstream processing
- **Flexible Filtering**: Configurable message type filtering for specific use cases
- **Sequence Number Validation**: Tracks and validates message sequence numbers
- **Industrial-Grade Reliability**: Handles edge cases and connection failures gracefully

## Metadata Outputs

The plugin provides comprehensive metadata for each message that can be used for downstream processing:

| Metadata | Description |
|----------|-------------|
| `sparkplug_msg_type` | The Sparkplug B message type (NBIRTH, NDATA, DBIRTH, DDATA, NDEATH, DDEATH, NCMD, DCMD) |
| `sparkplug_device_key` | Unique device identifier (Group/EdgeNode[/Device]) |
| `group_id` | Sparkplug B Group ID |
| `edge_node_id` | Edge Node ID within the group |
| `device_id` | Device ID under the edge node (only for device-level messages) |
| `mqtt_topic` | Original MQTT topic |
| `sequence_number` | Message sequence number (for NDATA/DDATA messages) |
| `session_established` | Whether the session is established for this edge node |
| `rebirth_requested` | Whether a rebirth request was sent for this message |

## Configuration Options

```yaml
input:
  sparkplug_input:
    broker_urls: ["tcp://localhost:1883"]
    client_id: "benthos-sparkplug-host"
    group_id: "MyGroup"
    edge_node_id: "MyHost"
    username: "admin"          # optional
    password: "password"       # optional
    qos: 1                     # optional (default: 1)
    retain: true              # optional (default: true)
    clean_session: true       # optional (default: true)
    keep_alive: 60            # optional (default: 60)
    connect_timeout: "10s"    # optional (default: "10s")
    message_types:            # optional (default: all types)
      - "NBIRTH"
      - "NDATA"
      - "DBIRTH"
      - "DDATA"
      - "NDEATH"
      - "DDEATH"
    rebirth_request_timeout: "30s"  # optional (default: "30s")
    session_timeout: "60s"          # optional (default: "60s")
    max_sequence_gap: 5             # optional (default: 5)
```

## Required Configuration

### Basic MQTT Connection

```yaml
input:
  sparkplug_input:
    broker_urls: ["tcp://localhost:1883"]
    client_id: "benthos-sparkplug-host"
    group_id: "MyGroup"
    edge_node_id: "MyHost"
```

### Authentication

If your MQTT broker requires authentication:

```yaml
input:
  sparkplug_input:
    broker_urls: ["tcp://localhost:1883"]
    client_id: "benthos-sparkplug-host"
    group_id: "MyGroup"
    edge_node_id: "MyHost"
    username: "admin"
    password: "password"
```

### Message Type Filtering

To subscribe only to specific message types:

```yaml
input:
  sparkplug_input:
    broker_urls: ["tcp://localhost:1883"]
    client_id: "benthos-sparkplug-host"
    group_id: "MyGroup"
    edge_node_id: "MyHost"
    message_types:
      - "NDATA"
      - "DDATA"
```

## Complete Example: Sparkplug B to UNS Integration

Here's a complete example showing how to use the Sparkplug B input with the processor and UNS output:

```yaml
input:
  sparkplug_input:
    broker_urls: ["tcp://localhost:1883"]
    client_id: "benthos-sparkplug-host"
    group_id: "Factory"
    edge_node_id: "SCADA-Host-01"
    username: "admin"
    password: "admin123"
    qos: 1
    retain: true
    message_types:
      - "NBIRTH"
      - "NDATA"
      - "DBIRTH"
      - "DDATA"
      - "NDEATH"
      - "DDEATH"

pipeline:
  processors:
    # Step 1: Decode Sparkplug B payloads with auto-features
    - sparkplug_b_decode:
        auto_split_metrics: true
        data_messages_only: true
        auto_extract_values: true
    
    # Step 2: Transform to UMH format
    - tag_processor:
        defaults: |
          msg.meta.data_contract = "_raw_";
          msg.meta.location_path = msg.meta.group_id + "." + msg.meta.edge_node_id;
          if msg.meta.device_id != "" {
            msg.meta.location_path = msg.meta.location_path + "." + msg.meta.device_id;
          }
          msg.meta.virtual_path = "sensors.generic";
          return msg;
        
        conditions:
          - if: msg.meta.tag_name && msg.meta.tag_name.includes("temp")
            then: |
              msg.meta.virtual_path = "sensors.temperature";
              return msg;
          
          - if: msg.meta.tag_name && msg.meta.tag_name.includes("press")
            then: |
              msg.meta.virtual_path = "sensors.pressure";
              return msg;

output:
  uns: {}
```

## Session Management

The Sparkplug B input implements comprehensive session management:

### Edge Node State Tracking

The input tracks the state of each edge node:
- **OFFLINE**: Initial state or after death certificate
- **ONLINE**: After receiving valid NBIRTH message
- **STALE**: After detecting sequence gaps or timeouts

### Rebirth Request Logic

When sequence number gaps are detected:
1. Send NCMD message to request rebirth certificate
2. Mark edge node as requiring rebirth
3. Wait for NBIRTH message to re-establish session
4. Resume normal processing after session re-establishment

### Death Certificate Processing

When NDEATH/DDEATH messages are received:
1. Mark edge node/device as offline
2. Clear cached sequence numbers
3. Expect NBIRTH/DBIRTH for reconnection

## Configuration Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `broker_urls` | `[]string` | **required** | List of MQTT broker URLs |
| `client_id` | `string` | **required** | MQTT client identifier |
| `group_id` | `string` | **required** | Sparkplug B Group ID for this host |
| `edge_node_id` | `string` | **required** | Edge Node ID for this host |
| `username` | `string` | `""` | MQTT username |
| `password` | `string` | `""` | MQTT password |
| `qos` | `int` | `1` | MQTT QoS level |
| `retain` | `bool` | `true` | Whether to retain STATE messages |
| `clean_session` | `bool` | `true` | MQTT clean session flag |
| `keep_alive` | `int` | `60` | MQTT keep alive interval (seconds) |
| `connect_timeout` | `string` | `"10s"` | Connection timeout |
| `message_types` | `[]string` | all types | List of message types to subscribe to |
| `rebirth_request_timeout` | `string` | `"30s"` | Timeout for rebirth requests |
| `session_timeout` | `string` | `"60s"` | Session timeout for edge nodes |
| `max_sequence_gap` | `int` | `5` | Maximum allowed sequence number gap |

## Troubleshooting

### Common Issues

**Connection Failures:**
- Verify broker URLs and authentication credentials
- Check network connectivity and firewall settings
- Ensure MQTT broker supports Sparkplug B QoS requirements

**Missing Messages:**
- Check message type filtering configuration
- Verify edge nodes are publishing to expected topics
- Monitor sequence number gaps and rebirth requests

**Session Management Issues:**
- Review rebirth request timeout settings
- Check edge node death certificate handling
- Verify STATE message publishing

### Monitoring

The plugin provides comprehensive logging and metrics:
- Connection status and failures
- Message processing statistics
- Session state transitions
- Rebirth request activity
- Sequence number validation results

## Security Considerations

### MQTT Security

- Use TLS encryption for broker connections
- Implement proper authentication and authorization
- Consider using client certificates for enhanced security

### Sparkplug B Security

- Validate message signatures if required
- Implement proper access controls for command messages
- Monitor for unexpected message patterns or sequence anomalies

## Integration with Other UMH Components

The Sparkplug B input is designed to work seamlessly with other UMH components:

- **Sparkplug B Processor**: Automatically decodes and enriches messages
- **Tag Processor**: Transforms metadata for UNS integration
- **UNS Output**: Publishes to the Unified Namespace
- **Historian**: Stores historical data with proper metadata

This creates a complete industrial IoT data pipeline from Sparkplug B edge devices to the Unified Namespace and beyond. 