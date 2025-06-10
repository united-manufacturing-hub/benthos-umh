# Sparkplug B (Input)

The Sparkplug B input plugin provides comprehensive MQTT-based industrial IoT data collection with multiple role support, session lifecycle management, rebirth requests, and death certificate handling.

Sparkplug B is an open standard for MQTT-based industrial IoT communication that uses protobuf encoding and hierarchical topic structures to organize edge nodes and devices. This plugin supports multiple roles:

**Roles:**
- **primary_host**: Acts as SCADA/Primary Application, subscribes to all groups (`spBv1.0/+/#`) or specific groups with filtering
- **edge_node**: Acts as Edge Node, subscribes only to its own group (`spBv1.0/{group}/#`)  
- **hybrid**: Combines both behaviors (rare, but useful for gateways), supports group filtering like primary_host

**Key Responsibilities:**
- Role-based subscription behavior (all groups vs single group)
- Managing MQTT session lifecycle and tracking edge node states
- Automatically requesting rebirth certificates when detecting sequence gaps
- Handling death certificates and session state transitions
- Automatic message processing with alias resolution and metric splitting
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

- **Multiple Role Support**: Support for primary_host, edge_node, and hybrid deployment patterns
- **Idiomatic Configuration**: Clean organization with mqtt/identity/role/behaviour sections
- **Integrated Processing**: Built-in alias resolution, metric splitting, and value extraction
- **Session Lifecycle Management**: Tracks edge node states and handles reconnections
- **Automatic Rebirth Requests**: Detects sequence number gaps and requests rebirth certificates
- **Death Certificate Handling**: Processes node/device death messages and state transitions
- **Comprehensive Metadata**: Extracts and provides rich metadata for downstream processing
- **Flexible Behavior**: Configurable message processing, filtering, and transformation
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
| `tag_name` | Metric name (when auto_split_metrics is enabled) |
| `mqtt_topic` | Original MQTT topic |
| `sequence_number` | Message sequence number (for NDATA/DDATA messages) |
| `session_established` | Whether the session is established for this edge node |
| `rebirth_requested` | Whether a rebirth request was sent for this message |

## Configuration Options

```yaml
input:
  sparkplug_b:
    # MQTT Transport Configuration
    mqtt:
      urls: ["tcp://localhost:1883"]
      client_id: "benthos-sparkplug"
      credentials:                    # optional
        username: "admin"
        password: "password"
      qos: 1                          # optional (default: 1)
      keep_alive: "60s"               # optional (default: "60s")
      connect_timeout: "30s"          # optional (default: "30s")
      clean_session: true             # optional (default: true)
    
    # Sparkplug Identity Configuration
    identity:
      group_id: "SCADA"
      edge_node_id: "Primary-Host-01"
      device_id: ""                   # optional (empty for node-level identity)
    
    # Role Configuration
    role: "primary_host"              # primary_host, edge_node, or hybrid
    
    # Subscription Configuration (optional for primary_host and hybrid roles)
    subscription:                     # optional
      groups: ["benthos", "factory1"] # specific groups to subscribe to (empty = all groups)
    
    # Processing Behavior Configuration
    behaviour:                        # optional
      auto_split_metrics: true        # optional (default: true)
      data_messages_only: false       # optional (default: false)
      enable_rebirth_req: true        # optional (default: true)
      drop_birth_messages: false      # optional (default: false)
      strict_topic_validation: false  # optional (default: false)
      auto_extract_values: true       # optional (default: true)
```

## Required Configuration

### Basic Configuration (Primary Host)

```yaml
input:
  sparkplug_b:
    mqtt:
      urls: ["tcp://localhost:1883"]
      client_id: "scada-host-01"
    identity:
      group_id: "SCADA"
      edge_node_id: "Primary-Host-01"
    role: "primary_host"
```

### Edge Node Configuration

```yaml
input:
  sparkplug_b:
    mqtt:
      urls: ["tcp://localhost:1883"]
      client_id: "edge-listener"
    identity:
      group_id: "Factory1"
      edge_node_id: "Line1-Gateway"
    role: "edge_node"
```

### Authentication

If your MQTT broker requires authentication:

```yaml
input:
  sparkplug_b:
    mqtt:
      urls: ["tcp://localhost:1883"]
      client_id: "benthos-sparkplug"
      credentials:
        username: "admin"
        password: "password"
    identity:
      group_id: "SCADA"
      edge_node_id: "Primary-Host-01"
    role: "primary_host"
```

### Subscription Filtering

For primary_host and hybrid roles, you can filter subscriptions to specific groups instead of monitoring all groups:

```yaml
input:
  sparkplug_b:
    mqtt:
      urls: ["tcp://localhost:1883"]
      client_id: "department-scada"
    identity:
      group_id: "SCADA"
      edge_node_id: "Department-Host"
    role: "primary_host"
    subscription:
      groups: ["factory_a", "warehouse", "quality_lab"]
```

**Group Filtering Examples:**

```yaml
# Monitor only test environments
subscription:
  groups: ["test", "development", "staging"]

# Monitor specific factory departments  
subscription:
  groups: ["assembly_line", "packaging", "shipping"]

# Monitor security zones
subscription:
  groups: ["secure_zone_1", "critical_systems"]

# Monitor everything (default behavior)
subscription:
  groups: []  # Empty list = all groups (spBv1.0/+/#)
```

**Use Cases:**
- **Production Segregation**: Different SCADA systems for different departments
- **Security Zones**: Isolate monitoring by security classification
- **Testing Environments**: Separate test/development from production
- **Departmental Monitoring**: Factory floor vs office systems
- **Performance Optimization**: Reduce message volume by filtering relevant groups

### Processing Behavior

To configure message processing behavior:

```yaml
input:
  sparkplug_b:
    mqtt:
      urls: ["tcp://localhost:1883"]
      client_id: "benthos-sparkplug"
    identity:
      group_id: "SCADA"
      edge_node_id: "Primary-Host-01"
    role: "primary_host"
    behaviour:
      auto_split_metrics: true        # Split multi-metric messages
      data_messages_only: true        # Only process DATA messages
      enable_rebirth_req: true        # Send rebirth requests on gaps
      auto_extract_values: true       # Extract metric values
```

## Complete Example: Sparkplug B to UNS Integration

Here's a complete example showing how to use the Sparkplug B input with integrated processing and UNS output:

```yaml
input:
  sparkplug_b:
    # MQTT Configuration
    mqtt:
      urls: ["tcp://localhost:1883"]
      client_id: "benthos-sparkplug-scada"
      credentials:
        username: "admin"
        password: "admin123"
      qos: 1
      keep_alive: "60s"
      connect_timeout: "30s"
      clean_session: true
    
    # Sparkplug Identity
    identity:
      group_id: "Factory"
      edge_node_id: "SCADA-Host-01"
    
    # Primary Host Role
    role: "primary_host"
    
    # Subscribe only to Factory group (optional)
    subscription:
      groups: ["Factory"]             # Filter to Factory group only
    
    # Processing Behavior
    behaviour:
      auto_split_metrics: true        # Split metrics for individual processing
      data_messages_only: true        # Focus on DATA messages for UNS
      enable_rebirth_req: true        # Handle sequence gaps
      auto_extract_values: true       # Extract clean values
      drop_birth_messages: false      # Keep BIRTH for alias resolution

pipeline:
  processors:
    # Transform to UMH format using built-in metadata
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

### MQTT Section
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mqtt.urls` | `[]string` | **required** | List of MQTT broker URLs |
| `mqtt.client_id` | `string` | `"benthos-sparkplug"` | MQTT client identifier |
| `mqtt.credentials.username` | `string` | `""` | MQTT username |
| `mqtt.credentials.password` | `string` | `""` | MQTT password |
| `mqtt.qos` | `int` | `1` | MQTT QoS level |
| `mqtt.keep_alive` | `duration` | `"60s"` | MQTT keep alive interval |
| `mqtt.connect_timeout` | `duration` | `"30s"` | Connection timeout |
| `mqtt.clean_session` | `bool` | `true` | MQTT clean session flag |

### Identity Section
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `identity.group_id` | `string` | **required** | Sparkplug B Group ID |
| `identity.edge_node_id` | `string` | **required** | Edge Node ID for this application |
| `identity.device_id` | `string` | `""` | Device ID (empty for node-level identity) |

### Role Configuration
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `role` | `string` | `"primary_host"` | Role: `primary_host`, `edge_node`, or `hybrid` |

### Subscription Section
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `subscription.groups` | `[]string` | `[]` | Specific groups to subscribe to for primary_host/hybrid roles. Empty = all groups |

### Behaviour Section
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `behaviour.auto_split_metrics` | `bool` | `true` | Split multi-metric messages into individual messages |
| `behaviour.data_messages_only` | `bool` | `false` | Only process DATA messages (drop others after processing) |
| `behaviour.enable_rebirth_req` | `bool` | `true` | Send rebirth requests on sequence gaps |
| `behaviour.drop_birth_messages` | `bool` | `false` | Drop BIRTH messages after alias extraction |
| `behaviour.strict_topic_validation` | `bool` | `false` | Strictly validate Sparkplug topic format |
| `behaviour.auto_extract_values` | `bool` | `true` | Extract metric values as message payload |

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

- **Built-in Processing**: Automatically decodes, enriches, and transforms messages
- **Tag Processor**: Further transforms metadata for UNS integration
- **UNS Output**: Publishes to the Unified Namespace
- **Historian**: Stores historical data with proper metadata

This creates a complete industrial IoT data pipeline from Sparkplug B edge devices to the Unified Namespace and beyond. The integrated processing capabilities eliminate the need for separate decoding processors while providing comprehensive metadata extraction and transformation features. 