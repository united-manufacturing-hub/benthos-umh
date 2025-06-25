# Sparkplug B (Output)

The Sparkplug B output plugin acts as an **Edge Node** or **Hybrid** node in the Sparkplug B ecosystem, publishing industrial IoT data using the standardized MQTT-based Sparkplug B protocol with protobuf encoding and alias management.

Sparkplug B is an open standard for MQTT-based industrial IoT communication that minimizes bandwidth usage through metric aliases and efficient protobuf encoding. This plugin supports multiple roles:

**Roles:**
- **edge_node**: Acts as Edge Node publishing data to SCADA/Primary Applications
- **hybrid**: Combines edge publishing with primary host capabilities (rare, for gateways)

**Key Responsibilities:**
- Publishing BIRTH certificates to establish metric definitions and aliases
- Sending DATA messages with current sensor/actuator values
- Managing session lifecycle with death certificates
- Handling command messages from SCADA hosts
- Optimizing bandwidth through alias-based metric references

> **Data Transformations**  
> It is recommended to perform field-to-Sparkplug transformations _before_ this plugin (e.g., via Node-RED JavaScript or [Bloblang](https://www.benthos.dev/docs/guides/bloblang/about)). This allows you to map your data fields directly to Sparkplug metrics without complex logic in the output configuration.

## Sparkplug B Protocol Overview

This plugin implements **Device-Level PARRIS** architecture for industry-aligned Sparkplug B compliance:

**Architecture Benefits:**
- **Static Edge Node ID**: Ensures Sparkplug B session consistency
- **Dynamic Device ID**: Enables flexible device identification via UMH location hierarchy
- **Industry Aligned**: Matches patterns used by Ignition MQTT Transmission
- **Specification Compliant**: Proper NBIRTH → DBIRTH → DDATA message flow

Sparkplug B defines a hierarchical topic structure and message lifecycle:

**Topic Structure:**
```
spBv1.0/<Group>/<MsgType>/<EdgeNode>[/<Device>]
```

**Message Lifecycle:**
1. **NBIRTH/DBIRTH**: Establish metrics and aliases
2. **NDATA/DDATA**: Send current values using aliases
3. **NDEATH/DDEATH**: Signal disconnection
4. **NCMD/DCMD**: Receive control commands

## Basic Configuration

```yaml
output:
  sparkplug_b:
    # MQTT Transport Configuration
    mqtt:
      urls: ["tcp://localhost:1883"]
      client_id: "benthos-edge-node"
      credentials:
        username: "edge-node"
        password: "secure-password"
      qos: 1
      keep_alive: "60s"
      connect_timeout: "30s"
      clean_session: true
    
    # Sparkplug Identity Configuration
    identity:
      group_id: "Factory"
      edge_node_id: "Line1-PLC"       # required: static Edge Node ID
      location_path: ""               # populated from message metadata  
      device_id: ""                   # optional: if empty, auto-generated from location_path
    
    # Role Configuration
    role: "edge_node"
    
    # Behavior Configuration
    behaviour:
      auto_extract_tag_name: true     # Extract tag_name from message metadata
      retain_last_values: true        # Retain last values for comparison
    
    # Define metrics with aliases for bandwidth optimization
    metrics:
      - name: "Temperature"
        alias: 1
        type: "float"
        value_from: "temperature"
      - name: "Pressure"
        alias: 2
        type: "double"
        value_from: "pressure"
      - name: "Motor_Speed"
        alias: 3
        type: "uint32"
        value_from: "motor_rpm"
      - name: "System_Status"
        alias: 4
        type: "string"
        value_from: "status"
```

## Supported Data Types

The Sparkplug B output plugin supports all standard Sparkplug B data types with automatic conversion:

**Numeric Types:**
- `Int8`, `Int16`, `Int32`, `Int64`: Signed integers
- `UInt8`, `UInt16`, `UInt32`, `UInt64`: Unsigned integers  
- `Float`: 32-bit floating-point
- `Double`: 64-bit floating-point

**Other Types:**
- `Boolean`: True/false values
- `String`: UTF-8 text strings
- `DateTime`: Timestamp values
- `Text`: Localized text with language code

**Note:** Data type conversion is handled automatically. If your input data doesn't match the specified type, the plugin will attempt conversion or mark the value as null with appropriate quality indicators.

## Configuration Fields

### MQTT Section
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mqtt.urls` | `[]string` | **required** | List of MQTT broker URLs |
| `mqtt.client_id` | `string` | `"benthos-sparkplug"` | MQTT client identifier |
| `mqtt.credentials.username` | `string` | `""` | MQTT username |
| `mqtt.credentials.password` | `string` | `""` | MQTT password |
| `mqtt.qos` | `int` | `1` | MQTT QoS level |
| `mqtt.keep_alive` | `duration` | `"30s"` | MQTT keep alive interval |
| `mqtt.connect_timeout` | `duration` | `"10s"` | Connection timeout |
| `mqtt.clean_session` | `bool` | `true` | MQTT clean session flag |

### Identity Section
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `identity.group_id` | `string` | **required** | Sparkplug B Group ID |
| `identity.edge_node_id` | `string` | **required** | Static Edge Node ID for Sparkplug B compliance (must be consistent throughout session) |
| `identity.device_id` | `string` | `""` | Device ID (empty for node-level messages) |

#### Device-Level PARRIS Method

The plugin implements **Device-Level PARRIS** for industry-aligned Sparkplug B architecture that maintains specification compliance while enabling dynamic device identification.

**Architecture:**
- **Static Edge Node ID**: Required field that remains consistent throughout the session (Sparkplug B compliance)
- **Dynamic Device ID**: Generated from UMH `location_path` metadata using PARRIS Method conversion
- **Topic Structure**: `spBv1.0/<Group>/DDATA/<StaticEdgeNode>/<DynamicDevice>`

**PARRIS Method Conversion:**
- Converts UMH dot notation to Sparkplug colon notation for Device ID
- Example: `"enterprise.factory.line1.station1"` → `"enterprise:factory:line1:station1"`

**Configuration Example:**
```yaml
identity:
  group_id: "FactoryA"           # Required: Stable business grouping  
  edge_node_id: "StaticNode01"   # Required: Static Edge Node ID
  location_path: ""              # Optional: Will be populated from message metadata
  device_id: ""                  # Optional: If empty, auto-generated from location_path

# Pipeline provides metadata:
# msg.meta.location_path = "enterprise.plant1.line3.station5"
# Results in: spBv1.0/FactoryA/DDATA/StaticNode01/enterprise:plant1:line3:station5
```

**Message Flow:**
1. **NBIRTH**: Node-level birth with Edge Node ID: `spBv1.0/FactoryA/NBIRTH/StaticNode01`
2. **DBIRTH**: Device-level birth for new devices: `spBv1.0/FactoryA/DBIRTH/StaticNode01/enterprise:plant1:line3:station5`
3. **DDATA**: Device-level data messages: `spBv1.0/FactoryA/DDATA/StaticNode01/enterprise:plant1:line3:station5`

**Metadata Requirements:**
- **Required**: `location_path` - Hierarchical location in dot notation (for dynamic device ID)
- **Optional**: `tag_name` - Used for metric name generation

**Benefits:**
- **Sparkplug B Compliant**: Static Edge Node ID ensures session consistency
- **Industry Aligned**: Matches Ignition MQTT Transmission patterns
- **UMH Integration**: Seamless conversion from UMH location hierarchy

### Role Configuration
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `role` | `string` | `"edge_node"` | Role: `edge_node` or `hybrid` |

### Behaviour Section
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `behaviour.auto_extract_tag_name` | `bool` | `true` | Extract tag_name from message metadata |
| `behaviour.retain_last_values` | `bool` | `true` | Retain last values for comparison |

### Output Configuration
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `metrics` | `[]object` | **required** | List of metric definitions |

### Metric Definition

Each metric in the `metrics` array supports:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| **name** | `string` | **yes** | Human-readable metric name |
| **alias** | `uint64` | **yes** | Unique numeric alias (1-65535) |
| **type** | `string` | **yes** | Sparkplug B data type |
| **value_from** | `string` | **yes** | JSON field name containing the value |
| **units** | `string` | no | Engineering units (e.g., "°C", "bar") |
| **is_historical** | `bool` | no | Whether this is historical data |
| **metadata** | `object` | no | Additional key-value metadata |

## Usage Examples

### Basic Temperature Sensor

**Incoming Message:**
```json
{
  "sensor_id": "TMP001",
  "temperature": 23.5,
  "humidity": 65.2,
  "timestamp": "2024-01-15T10:30:00Z"
}
```

**Configuration:**
```yaml
output:
  sparkplug_b:
    mqtt:
      urls: ["tcp://mqtt.factory.com:1883"]
      client_id: "temp-sensor-001"
    identity:
      group_id: "Factory"
      edge_node_id: "Building-A"      # required: static Edge Node ID
      location_path: ""               # auto-populated from message metadata
      device_id: ""                   # auto-generated from location_path
    role: "edge_node"
    
    behaviour:
      auto_extract_tag_name: true
      retain_last_values: true
    
    metrics:
      - name: "Temperature"
        alias: 1
        type: "float"
        value_from: "temperature"
        units: "°C"
      - name: "Humidity"
        alias: 2
        type: "float"
        value_from: "humidity"
        units: "%RH"
```

### Industrial Motor Controller

**Incoming Message:**
```json
{
  "motor_rpm": 1450,
  "motor_current": 12.5,
  "motor_voltage": 415.2,
  "motor_enabled": true,
  "fault_code": 0,
  "status": "RUNNING"
}
```

**Configuration:**
```yaml
output:
  sparkplug_b:
    mqtt:
      urls: ["tcp://localhost:1883"]
      client_id: "motor-controller-01"
    identity:
      group_id: "Production"
      edge_node_id: "Line-1"          # required: static Edge Node ID
      location_path: ""               # auto-populated from message metadata
      device_id: ""                   # auto-generated from location_path
    role: "edge_node"
    
    behaviour:
      auto_extract_tag_name: true
      retain_last_values: true
    
    metrics:
      - name: "RPM"
        alias: 10
        data_type: "UInt32"
        value_from: "motor_rpm"
        units: "rpm"
      - name: "Current"
        alias: 11
        data_type: "Float"
        value_from: "motor_current"
        units: "A"
      - name: "Voltage"
        alias: 12
        data_type: "Float"
        value_from: "motor_voltage"
        units: "V"
      - name: "Enabled"
        alias: 13
        data_type: "Boolean"
        value_from: "motor_enabled"
      - name: "Fault_Code"
        alias: 14
        data_type: "UInt16"
        value_from: "fault_code"
      - name: "Status"
        alias: 15
        data_type: "String"
        value_from: "status"
```

### Multi-Device Edge Node

For publishing data from multiple devices through a single edge node:

```yaml
# First device output
output:
  sparkplug_b:
    mqtt:
      urls: ["tcp://localhost:1883"]
      client_id: "plc-gateway"
    identity:
      group_id: "Factory"
      edge_node_id: "PLC-Gateway-01"  # required: static Edge Node ID
      location_path: ""               # auto-populated from message metadata
      device_id: ""                   # auto-generated from location_path
    role: "edge_node"
    
    behaviour:
      auto_extract_tag_name: true
      retain_last_values: true
    
    metrics:
      - name: "Belt_Speed"
        alias: 1
        data_type: "Float"
        value_from: "speed"
        units: "m/min"
      - name: "Motor_Load"
        alias: 2
        data_type: "Float"
        value_from: "load_percent"
        units: "%"

# Multiple devices automatically get different device IDs from their location_path metadata
# This creates the proper Sparkplug B hierarchy with device-level PARRIS
```

## Message Flow and Lifecycle

### BIRTH Messages (NBIRTH/DBIRTH)

Sent automatically when:
- Initial connection to MQTT broker
- Reconnection after network failure
- Upon receiving rebirth request from host

Contains:
- Complete metric definitions with names and aliases
- Data types and engineering units
- Current values for all metrics
- Device capabilities and properties

### DATA Messages (NDATA/DDATA)

Sent for each incoming message containing:
- Only changed metric values (by alias)
- Sequence numbers for ordering
- Timestamps for each metric
- Quality indicators for each value

### DEATH Messages (NDEATH/DDEATH)

Sent automatically when:
- Clean shutdown of the plugin
- MQTT connection loss (via Will message)
- Graceful device disconnect

## Complete Example: UNS to Sparkplug B

Here's a complete pipeline showing UNS data transformation to Sparkplug B:

```yaml
input:
  uns: {}

pipeline:
  processors:
    # Transform UNS data to fields expected by Sparkplug output
    - bloblang: |
        root = this
        
        # Extract values from UNS message
        root.temperature = this.payload.value
        root.quality = this.payload.quality
        root.timestamp = this.timestamp
        
        # Extract location info from UNS topic metadata
        root.location = this.meta.location
        root.asset = this.meta.asset

output:
  sparkplug_b:
    mqtt:
      urls: ["tcp://sparkplug-broker:1883"]
      client_id: "uns-to-sparkplug-bridge"
    identity:
      group_id: "UNS-Bridge"
      edge_node_id: "Factory-Gateway"  # required: static Edge Node ID
      location_path: ""                # auto-populated from message metadata
      device_id: ""                    # auto-generated from location_path
    role: "edge_node"
    
    behaviour:
      auto_extract_tag_name: true
      retain_last_values: true
    
    metrics:
      - name: "Temperature"
        alias: 1
        data_type: "Float"
        value_from: "temperature"
        units: "°C"
```

## Advanced Features

### Alias Management

The plugin automatically manages aliases for bandwidth optimization:
- BIRTH messages include full metric definitions
- DATA messages use only aliases to reference metrics
- Aliases are cached and reused across sessions
- Automatic validation prevents alias conflicts

### Sequence Numbers

Each DATA message includes sequence numbers for:
- Message ordering at the receiver
- Gap detection and rebirth requests
- Session continuity validation
- Out-of-order message handling

### Quality Indicators

Supports Sparkplug B quality indicators:
- **GOOD**: Valid, current data
- **BAD**: Invalid or failed sensor reading
- **UNCERTAIN**: Questionable data quality
- **STALE**: Data older than expected update interval

### Command Handling

The plugin can receive and process command messages:
- Automatic subscription to NCMD/DCMD topics
- Command parsing and validation
- Optional command acknowledgment
- Integration with downstream processors for command execution

## Monitoring and Troubleshooting

### Metrics and Logging

The plugin provides comprehensive observability:
- Connection status and retry attempts
- Message publishing statistics
- Alias resolution and validation
- Data type conversion results
- Quality indicator statistics

### Common Issues

**Connection Problems:**
- Verify MQTT broker URL and credentials
- Check network connectivity and firewall rules
- Ensure broker supports required QoS levels

**Message Publishing Failures:**
- Validate metric definitions and aliases
- Check data type conversions
- Verify input message field mappings
- Monitor sequence number continuity

**Performance Issues:**
- Optimize alias assignments for frequently changing metrics
- Tune QoS settings based on reliability requirements
- Consider message batching for high-frequency data

## Security Considerations

### MQTT Security
- Use TLS encryption for broker connections: `tls://broker:8883`
- Implement client certificate authentication
- Configure proper topic-level access controls
- Use strong passwords and rotate credentials regularly

### Sparkplug B Security
- Validate incoming command messages
- Implement proper alias range management
- Monitor for unexpected message patterns
- Use signed/encrypted payloads for sensitive data

## Integration Patterns

### Edge-to-Cloud Pipeline

```yaml
# Local edge processing
input:
  modbus: {}  # or other industrial protocol

pipeline:
  processors:
    - tag_processor: {}     # Local enrichment
    - sparkplug_b_decode: {} # Local UNS

output:
  sparkplug_b: {}    # Publish to cloud SCADA
```

### Multi-Protocol Bridge

```yaml
# Bridge multiple protocols to Sparkplug B
input:
  opcua: {}

pipeline:
  processors:
    - bloblang: |
        # Transform OPC UA to Sparkplug format
        root.value = this.payload
        root.quality = if this.meta.opcua_status == "Good" { "GOOD" } else { "BAD" }

output:
  sparkplug_b: {}
```

This creates a robust, standards-compliant Sparkplug B edge node implementation that integrates seamlessly with the broader UMH ecosystem. 