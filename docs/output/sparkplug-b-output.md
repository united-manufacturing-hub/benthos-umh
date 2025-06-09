# Sparkplug B (Output)

The Sparkplug B output plugin acts as an **Edge Node** in the Sparkplug B ecosystem, publishing industrial IoT data using the standardized MQTT-based Sparkplug B protocol with protobuf encoding and alias management.

Sparkplug B is an open standard for MQTT-based industrial IoT communication that minimizes bandwidth usage through metric aliases and efficient protobuf encoding. This plugin implements the Edge Node role, which is responsible for:

- Publishing BIRTH certificates to establish metric definitions and aliases
- Sending DATA messages with current sensor/actuator values
- Managing session lifecycle with death certificates
- Handling command messages from SCADA hosts
- Optimizing bandwidth through alias-based metric references

> **Data Transformations**  
> It is recommended to perform field-to-Sparkplug transformations _before_ this plugin (e.g., via Node-RED JavaScript or [Bloblang](https://www.benthos.dev/docs/guides/bloblang/about)). This allows you to map your data fields directly to Sparkplug metrics without complex logic in the output configuration.

## Sparkplug B Protocol Overview

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
  sparkplug_output:
    broker_urls: ["tcp://localhost:1883"]
    client_id: "benthos-edge-node"
    group_id: "Factory"
    edge_node_id: "Line1-PLC"
    device_id: "Sensor-Array-01"    # optional for device-level messages
    
    # Define metrics with aliases for bandwidth optimization
    metrics:
      - name: "Temperature"
        alias: 1
        data_type: "Float"
        value_from: "temp_celsius"
      - name: "Pressure"
        alias: 2
        data_type: "Double"
        value_from: "pressure_bar"
      - name: "Motor_Speed"
        alias: 3
        data_type: "UInt32"
        value_from: "rpm"
      - name: "System_Status"
        alias: 4
        data_type: "String"
        value_from: "status"
    
    # Optional MQTT settings
    username: "edge-node"
    password: "secure-password"
    qos: 1
    retain: false
    keep_alive: 60
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

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| **broker_urls** | `[]string` | **required** | List of MQTT broker URLs |
| **client_id** | `string` | **required** | MQTT client identifier |
| **group_id** | `string` | **required** | Sparkplug B Group ID |
| **edge_node_id** | `string` | **required** | Edge Node ID within the group |
| **device_id** | `string` | `""` | Optional Device ID for device-level messages |
| **metrics** | `[]object` | **required** | List of metric definitions |
| **username** | `string` | `""` | MQTT username |
| **password** | `string` | `""` | MQTT password |
| **qos** | `int` | `1` | MQTT QoS level |
| **retain** | `bool` | `false` | Whether to retain messages |
| **keep_alive** | `int` | `60` | MQTT keep alive interval (seconds) |
| **birth_on_connect** | `bool` | `true` | Send BIRTH message on connection |
| **death_on_disconnect** | `bool` | `true` | Send DEATH message on disconnection |

### Metric Definition

Each metric in the `metrics` array supports:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| **name** | `string` | **yes** | Human-readable metric name |
| **alias** | `uint64` | **yes** | Unique numeric alias (1-65535) |
| **data_type** | `string` | **yes** | Sparkplug B data type |
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
  sparkplug_output:
    broker_urls: ["tcp://mqtt.factory.com:1883"]
    client_id: "temp-sensor-001"
    group_id: "Factory"
    edge_node_id: "Building-A"
    device_id: "TMP001"
    
    metrics:
      - name: "Temperature"
        alias: 1
        data_type: "Float"
        value_from: "temperature"
        units: "°C"
      - name: "Humidity"
        alias: 2
        data_type: "Float"
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
  sparkplug_output:
    broker_urls: ["tcp://localhost:1883"]
    client_id: "motor-controller-01"
    group_id: "Production"
    edge_node_id: "Line-1"
    device_id: "Motor-01"
    
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
  sparkplug_output:
    broker_urls: ["tcp://localhost:1883"]
    client_id: "plc-gateway"
    group_id: "Factory"
    edge_node_id: "PLC-Gateway-01"
    device_id: "Conveyor-Belt"
    
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

# Second device would use same edge_node_id but different device_id
# This creates the proper Sparkplug B hierarchy
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
  sparkplug_output:
    broker_urls: ["tcp://sparkplug-broker:1883"]
    client_id: "uns-to-sparkplug-bridge"
    group_id: "UNS-Bridge"
    edge_node_id: "Factory-Gateway"
    
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
  sparkplug_output: {}    # Publish to cloud SCADA
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
  sparkplug_output: {}
```

This creates a robust, standards-compliant Sparkplug B edge node implementation that integrates seamlessly with the broader UMH ecosystem. 