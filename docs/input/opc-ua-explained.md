# OPC UA Benthos Plugin - How It Works

This document provides a comprehensive explanation of the OPC UA Benthos plugin, covering its architecture, data flow, configuration, and typical use cases.

## Overview

The OPC UA plugin is an industrial protocol bridge that enables benthos-umh to communicate with OPC UA servers. It supports both **reading data** (input) from OPC UA servers and **writing data** (output) back to them. The plugin automatically detects server capabilities and optimizes performance accordingly.

## Data Flow Architecture

### Input (Reading Data)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          OPC UA Input Data Flow                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. CONNECT                                                                  │
│     ┌──────────┐      ┌───────────────┐      ┌──────────────────┐           │
│     │  Config  │ ───► │  Endpoint     │ ───► │  Authenticate    │           │
│     │  YAML    │      │  Discovery    │      │  (Auto/User/Cert)│           │
│     └──────────┘      └───────────────┘      └──────────────────┘           │
│                                                      │                       │
│  2. DETECT PROFILE                                   ▼                       │
│     ┌──────────────────────────────────────────────────────────┐            │
│     │  Query ServerInfo → Match Manufacturer → Apply Profile    │            │
│     │  (Siemens S7-1500 → MaxBatch=500, MaxWorkers=20)         │            │
│     └──────────────────────────────────────────────────────────┘            │
│                                                      │                       │
│  3. BROWSE NODES                                     ▼                       │
│     ┌──────────────────────────────────────────────────────────┐            │
│     │  Global Worker Pool (5-60 concurrent workers)            │            │
│     │  ┌────┐ ┌────┐ ┌────┐                                    │            │
│     │  │ W1 │ │ W2 │ │ W3 │ ... Recursive browse from NodeIDs  │            │
│     │  └────┘ └────┘ └────┘                                    │            │
│     │  Discovers all child nodes (up to 25 levels deep)        │            │
│     └──────────────────────────────────────────────────────────┘            │
│                                                      │                       │
│  4. SUBSCRIBE/POLL                                   ▼                       │
│     ┌─────────────────────┐    ┌─────────────────────┐                      │
│     │   Subscribe Mode    │ OR │     Pull Mode       │                      │
│     │   (on-change)       │    │   (poll every Nms)  │                      │
│     └─────────────────────┘    └─────────────────────┘                      │
│                                                      │                       │
│  5. OUTPUT MESSAGES                                  ▼                       │
│     ┌──────────────────────────────────────────────────────────┐            │
│     │  Benthos Message                                          │            │
│     │  ├── payload: <value>                                     │            │
│     │  └── metadata:                                            │            │
│     │      ├── opcua_tag_name: "Temperature"                    │            │
│     │      ├── opcua_tag_path: "Machine.Motor1"                 │            │
│     │      ├── opcua_source_timestamp: "2026-01-31T10:30:45Z"   │            │
│     │      └── opcua_attr_nodeid: "ns=2;s=Temperature"          │            │
│     └──────────────────────────────────────────────────────────┘            │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Output (Writing Data)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          OPC UA Output Data Flow                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. RECEIVE MESSAGE                                                          │
│     ┌─────────────────────────────────────────────────────────┐             │
│     │  Benthos Message with values to write                    │             │
│     │  { "setpoint_value": 42.5 }                              │             │
│     └─────────────────────────────────────────────────────────┘             │
│                                          │                                   │
│  2. MAP TO NODES                         ▼                                   │
│     ┌─────────────────────────────────────────────────────────┐             │
│     │  nodeMappings: "setpoint_value" → "ns=2;s=SetPoint"      │             │
│     └─────────────────────────────────────────────────────────┘             │
│                                          │                                   │
│  3. WRITE                                ▼                                   │
│     ┌─────────────────────────────────────────────────────────┐             │
│     │  OPC UA Write Request to Server                          │             │
│     └─────────────────────────────────────────────────────────┘             │
│                                          │                                   │
│  4. HANDSHAKE (Optional)                 ▼                                   │
│     ┌─────────────────────────────────────────────────────────┐             │
│     │  Read-back verification: Did the write succeed?          │             │
│     │  Retry up to maxWriteAttempts if mismatch                │             │
│     └─────────────────────────────────────────────────────────┘             │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Two Operational Modes

### Pull Mode (Default)

```yaml
input:
  opcua:
    endpoint: "opc.tcp://plc:4840"
    nodeIDs: ["ns=2;s=Sensors"]
    subscribeEnabled: false  # Default
    pollRate: 1000           # Poll every 1 second
```

**How it works**: Polls ALL nodes at the configured rate (default: 1000ms), regardless of whether values changed.

**Advantages**:
- Real-time visibility into all data
- Easy to differentiate "no data" vs "value unchanged"
- Better for dashboards and MQTT Explorer

**Disadvantages**:
- Higher data throughput (sends data even when unchanged)

### Subscribe Mode

```yaml
input:
  opcua:
    endpoint: "opc.tcp://plc:4840"
    nodeIDs: ["ns=2;s=Sensors"]
    subscribeEnabled: true
```

**How it works**: Server pushes data only when values change (OPC UA subscriptions).

**Advantages**:
- Lower bandwidth usage
- Only sends meaningful changes

**Disadvantages**:
- Harder to tell if connection is alive without heartbeat
- Less visibility into real-time status

## Server Profile Auto-Detection

The plugin automatically detects OPC UA server type and optimizes performance:

```
┌──────────────────────────────────────────────────────────────┐
│                    Server Profile Detection                   │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  1. Query ServerInfo nodes:                                   │
│     - ManufacturerName                                        │
│     - ProductName                                             │
│     - SoftwareVersion                                         │
│                                                               │
│  2. Pattern match against known vendors:                      │
│     "Siemens" + "S7-1200" → siemens-s7-1200 profile          │
│     "Inductive Automation" → ignition profile                 │
│     "Kepware" → kepware profile                               │
│                                                               │
│  3. Apply tuned parameters:                                   │
│     - MaxWorkers (browse phase concurrency)                   │
│     - MaxBatchSize (subscription batching)                    │
│     - DataChangeFilter support                                │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

### Available Profiles

| Profile | Server Type | MaxWorkers | MaxBatchSize | Max Monitored Items |
|---------|-------------|------------|--------------|---------------------|
| `auto` | Unknown | 5 | 50 | Unlimited |
| `siemens-s7-1200` | Siemens S7-1200 PLC | 10 | 100 | 1,000 |
| `siemens-s7-1500` | Siemens S7-1500 PLC | 20 | 500 | 10,000 |
| `kepware` | PTC Kepware | 40 | 1,000 | Unlimited |
| `ignition` | Inductive Automation | 20 | 100 | Unlimited |
| `prosys` | Prosys Simulation | 60 | 800 | Unlimited |
| `high-performance` | Manual override | 50 | 1,000 | Unlimited |

**Why profiles matter**: A Siemens S7-1200 reports it can handle 1000 monitored items per call, but values >200 cause 50× performance degradation. The profile uses safe limits from real-world testing.

## Configuration Reference

### Basic Input Configuration

```yaml
input:
  opcua:
    # Required
    endpoint: "opc.tcp://192.168.1.100:4840"
    nodeIDs: ["ns=2;s=Machines", "ns=2;s=Sensors"]

    # Authentication (optional)
    username: "user"
    password: "pass"

    # Security (optional, auto-negotiated if omitted)
    securityMode: "SignAndEncrypt"
    securityPolicy: "Basic256Sha256"

    # Operational mode
    subscribeEnabled: true   # false = pull mode, true = subscribe mode
    pollRate: 1000           # Poll interval in ms (pull mode only)

    # Connection resilience
    autoReconnect: true
    reconnectIntervalInSeconds: 5
    useHeartbeat: true       # Monitor connection via server time

    # Performance tuning (advanced)
    profile: ""              # Auto-detect, or "siemens-s7-1500", etc.
    queueSize: 10            # Subscription notification buffer
    samplingInterval: 0.0    # Server sampling rate (0 = fastest)
```

### Basic Output Configuration

```yaml
output:
  opcua:
    endpoint: "opc.tcp://192.168.1.100:4840"

    # Map message fields to OPC UA nodes
    nodeMappings:
      - nodeId: "ns=2;s=SetPoint"
        valueFrom: "setpoint_value"
        dataType: "Float"
      - nodeId: "ns=2;s=Enable"
        valueFrom: "enable_flag"
        dataType: "Boolean"

    # Write verification
    handshake:
      enabled: true
      readbackTimeoutMs: 2000
      maxWriteAttempts: 3
      timeBetweenRetriesMs: 1000
```

## Message Metadata

Every message from the OPC UA input includes rich metadata:

| Metadata Field | Description | Example |
|----------------|-------------|---------|
| `opcua_tag_name` | Sanitized node name (unique) | `Temperature` |
| `opcua_tag_path` | Dot-separated path to tag | `Machine.Motor1` |
| `opcua_tag_group` | Alias for tag_path | `Machine.Motor1` |
| `opcua_tag_type` | Simplified type | `number`, `string`, `bool` |
| `opcua_source_timestamp` | When value was sampled | `2026-01-31T10:30:45.123Z` |
| `opcua_server_timestamp` | When server processed | `2026-01-31T10:30:45.456Z` |
| `opcua_attr_nodeid` | Full OPC UA NodeID | `ns=2;s=Temperature` |
| `opcua_attr_datatype` | OPC UA data type | `Double` |
| `opcua_attr_statuscode` | Quality indicator | `Good`, `BadQualityLost` |

### Path Construction Example

Given this OPC UA structure:
```
Root
└── ns=2;s=FolderNode
    ├── ns=2;s=Tag1
    └── ns=2;s=SubFolder
        └── ns=2;s=Tag2
```

Subscribing to `ns=2;s=FolderNode` produces:

| Tag | `opcua_tag_name` | `opcua_tag_path` |
|-----|------------------|------------------|
| Tag1 | `Tag1` | `FolderNode` |
| Tag2 | `Tag2` | `FolderNode.SubFolder` |

## Typical Use Cases

### Use Case 1: Manufacturing Equipment Monitoring

Read sensor data from a Siemens PLC and stream to the Unified Namespace:

```yaml
input:
  opcua:
    endpoint: "opc.tcp://siemens-plc:4840"
    nodeIDs: ["ns=2;s=ProductionLine"]
    subscribeEnabled: true
    useHeartbeat: true
    autoReconnect: true

pipeline:
  processors:
    - tag_processor:
        defaults: |
          msg.meta.location_path = "enterprise.site.area.line01";
          msg.meta.data_contract = "_raw";
          msg.meta.tag_name = msg.meta.opcua_tag_name;
          return msg;

output:
  uns: {}
```

**Flow**: PLC → OPC UA → Benthos → Tag Processor → UNS (Kafka)

### Use Case 2: High-Frequency Data Collection (Ignition)

Collect high-frequency data from an Ignition gateway:

```yaml
input:
  opcua:
    endpoint: "opc.tcp://ignition-server:4840"
    nodeIDs: ["ns=1;s=HighSpeedSensors"]
    subscribeEnabled: true
    samplingInterval: 100    # 100ms sampling
    queueSize: 100           # Larger buffer for bursts
    profile: "ignition"      # Optional: skip auto-detection
```

### Use Case 3: Writing Setpoints to PLCs

Write control values back to a PLC:

```yaml
input:
  kafka:
    addresses: ["localhost:9092"]
    topics: ["control-commands"]

output:
  opcua:
    endpoint: "opc.tcp://plc:4840"
    nodeMappings:
      - nodeId: "ns=2;s=TemperatureSetpoint"
        valueFrom: "temp_setpoint"
        dataType: "Float"
      - nodeId: "ns=2;s=PumpEnable"
        valueFrom: "pump_enable"
        dataType: "Boolean"
    handshake:
      enabled: true
      maxWriteAttempts: 3
```

### Use Case 4: Secure Connection with Certificate

Connect securely with explicit security settings:

```yaml
input:
  opcua:
    endpoint: "opc.tcp://secure-server:4840"
    nodeIDs: ["ns=2;s=SecureData"]

    # Explicit security configuration
    securityMode: "SignAndEncrypt"
    securityPolicy: "Basic256Sha256"
    serverCertificateFingerprint: "abc123..."  # SHA3-512 hash
    clientCertificate: "base64-encoded-pem..."  # Optional, auto-generated if omitted

    # User authentication
    username: "operator"
    password: "secure-password"
```

### Use Case 5: Polling Mode for Dashboard Visibility

Use pull mode when you need constant data updates for dashboards:

```yaml
input:
  opcua:
    endpoint: "opc.tcp://plc:4840"
    nodeIDs: ["ns=2;s=Dashboard"]
    subscribeEnabled: false  # Pull mode
    pollRate: 500            # Poll every 500ms for responsive UI
    useHeartbeat: true
```

## Key Design Principles

1. **Automatic Optimization**: Server profiles auto-tune performance without manual configuration
2. **Safe Defaults**: Uses conservative limits that work across all servers
3. **Global Resource Management**: Worker pools prevent server overload when browsing multiple NodeIDs
4. **Graceful Degradation**: Falls back to simpler methods if advanced features (like DataChangeFilters) aren't supported
5. **Rich Metadata**: Every message includes comprehensive context for downstream processing

## Supported Data Types

The plugin handles all common OPC UA data types:
- Numeric: `Int16`, `Int32`, `Int64`, `UInt16`, `UInt32`, `UInt64`, `Float`, `Double`
- Text: `String`, `LocalizedText`, `XmlElement`
- Boolean: `Boolean`
- Time: `DateTime`, `UtcTime`, `Duration`
- Special: `Guid`, `NodeId`, `StatusCode`, `ByteString`, `ByteArray`

**Not supported**: Two-dimensional arrays, UA Extension Objects, Variant arrays (mixed types)

## Troubleshooting

### Connection Issues
- Verify endpoint URL and port
- Check firewall rules
- Confirm authentication credentials
- Review security mode/policy compatibility

### Performance Issues
- Check server profile detection in logs
- Consider explicit profile override for better batching
- Reduce NodeID scope if browsing takes too long

### Missing Data
- Verify NodeIDs exist and are readable
- Check `opcua_attr_statuscode` for quality issues
- Enable `useHeartbeat` to detect stalled connections
- Confirm subscription queue isn't overflowing

For more details, see:
- [OPC UA Input Reference](./opc-ua-input.md)
- [Server Profiles Implementation](../../opcua_plugin/server_profiles.go)
