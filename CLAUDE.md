# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the benthos-umh repository.

## Repository Purpose

benthos-umh is a specialized extension of Benthos (Redpanda Connect) tailored for the manufacturing industry. It serves as the stream processing engine for the United Manufacturing Hub, bridging industrial protocols with the UNS (Unified Namespace) through YAML-defined data pipelines.

## Documentation Structure

- `docs/input/` - Input connectors documentation
  - OPC UA, Modbus, S7, Sparkplug B, Ethernet/IP, sensorconnect
- `docs/output/` - Output connectors documentation
  - UNS, OPC UA, Sparkplug B
- `docs/processing/` - Processing pipelines documentation
  - Tag processor, downsampler, stream processor, topic browser
- `docs/libraries/` - Shared libraries documentation
  - UMH topic parser
- `docs/testing/` - Testing approach and examples
- Full documentation: [docs.umh.app/benthos-umh](https://docs.umh.app/benthos-umh)

## Plugin Architecture

benthos-umh extends Benthos with 13 specialized plugins organized by function:

### Input Plugins
Industrial protocol connectors that read data from field devices:
- `opcua_plugin/` - OPC UA protocol input/output (Server/Client)
- `modbus_plugin/` - Modbus TCP/RTU protocol (Master/Slave)
- `s7comm_plugin/` - Siemens S7 protocol (S7-300/400/1200/1500)
- `sparkplug_plugin/` - Sparkplug B MQTT protocol (Edge/Host)
- `eip_plugin/` - Ethernet/IP protocol (Allen-Bradley)
- `sensorconnect_plugin/` - ifm IO-Link Master integration

**Integration**: Used in Bridge "Read" flows, orchestrated by UMH Core FSM

### Processing Plugins
Data transformation and enrichment processors:
- `tag_processor_plugin/` - Individual sensor data processing (metadata injection, value wrapping)
- `stream_processor_plugin/` - Multi-source data aggregation with state management
- `downsampler_plugin/` - Data reduction and aggregation (time-based sampling)
- `topic_browser_plugin/` - Topic discovery and metadata extraction
- `classic_to_core_plugin/` - UMH Classic to Core migration transformer
- `nodered_js_plugin/` - Node-RED JavaScript execution engine (goja-based)

**Integration**: Used in `pipeline.processors[]` section of DFC YAML

### Output Plugin
Unified Namespace writer with validation:
- `uns_plugin/` - Unified Namespace output with topic validation and Kafka batching

**Integration**: Only works within UMH Core (requires embedded Redpanda)

### Plugin Registration

All plugins auto-register during `init()`:
- Input plugins: `input.RegisterPlugin()`
- Processor plugins: `processor.RegisterPlugin()`
- Output plugins: `output.RegisterPlugin()`

**Discovery**: Run `benthos list inputs`, `benthos list processors`, `benthos list outputs`

## Key Concepts

- **Data Flow Component (DFC)**: YAML-defined data pipeline configuration
- **Protocol Converter**: Bridge between industrial protocols and UNS
- **Stream Processor**: Aggregates multiple data sources into unified streams
- **Tag Processor**: Handles individual sensor/tag data points
- **UNS (Unified Namespace)**: Central data structure following ISA-95 standard
- **Topic Browser**: Discovers and catalogs available data topics
- **Downsampler**: Reduces data volume while preserving important changes

## Integration with UMH Core

benthos-umh is primarily designed to run within UMH Core, which orchestrates benthos instances through Finite State Machines (FSM).

### Bridge Architecture

**Bridges** are a UMH Core UI/configuration concept that combines:
- **Connection Monitoring**: `nmap` probe to verify device availability
- **Read Flow**: Data Flow Component with `input` → `pipeline` → `output`
- **Write Flow**: (Not yet implemented) Future feature for bidirectional communication

In UMH Core's `config.yaml`:
```yaml
protocolConverter:
  - name: my-plc-bridge
    desiredState: active
    protocolConverterServiceConfig:
      location:
        2: "production-line"  # Agent location (0,1) + Bridge location (2,3,4)
        3: "plc-01"
      config:
        connection:
          nmap:
            target: "{{ .IP }}"    # Template variable injection
            port: "{{ .PORT }}"
        dataflowcomponent_read:
          benthos:               # This is benthos-umh configuration
            input:
              s7comm:
                tcpDevice: "{{ .IP }}"
                addresses: ["DB1.DW20"]
            pipeline:
              processors:
                - tag_processor:
                    defaults: |
                      msg.meta.location_path = "{{ .location_path }}";
                      msg.meta.data_contract = "_raw";
                      return msg;
            output:
              uns: {}
      variables:
        IP: "192.168.1.100"
        PORT: "102"
```

### Template Variable System

UMH Core injects variables into benthos configurations:
- `{{ .IP }}` - From `variables.IP` (flattened from nested structure)
- `{{ .PORT }}` - From `variables.PORT`
- `{{ .location_path }}` - Computed from agent location + bridge location
  - Example: Agent location `{0: "enterprise", 1: "site"}` + Bridge location `{2: "area", 3: "line"}` = `"enterprise.site.area.line"`

**Critical**: Variables are Go template strings, expanded before benthos sees the configuration.

### FSM Orchestration

UMH Core manages benthos instances through state machines:
- **Bridge States**: `stopped` → `starting_connection` → `starting_redpanda` → `starting_dfc` → `idle`/`active`
- **DFC States**: State machine tracks benthos process lifecycle
- **Degraded Handling**: Connection failures, protocol errors trigger state transitions

When debugging benthos-umh in production, always check:
1. UMH Core FSM state (from agent logs)
2. benthos process health (S6 supervision)
3. Configuration after variable substitution

## Message Architecture

benthos-umh uses **Node-RED style message format** with separate payload and metadata:

```javascript
{
  "payload": {
    "value": 42,
    "timestamp_ms": 1730986400000
  },
  "meta": {
    "location_path": "enterprise.site.area.line",
    "data_contract": "_raw",
    "tag_name": "temperature",
    "virtual_path": "motor.electrical",  // Optional
    "umh_topic": "umh.v1.enterprise.site.area.line._raw.motor.electrical.temperature"  // Auto-generated
  }
}
```

### Metadata Flow

**Critical**: `msg.meta` becomes Benthos metadata, **NOT** part of payload:
1. **Tag Processor**: Sets `msg.meta` fields via JavaScript
2. **Benthos Metadata**: `msg.meta` → Benthos internal metadata
3. **Kafka Headers**: Benthos metadata → Kafka message headers
4. **UNS Output**: Reconstructs topic from `umh_topic` metadata field

**Common mistake**: Trying to access `msg.meta` in payload - it's not there!

### Value Wrapping

Tag processor automatically wraps primitive values:
```javascript
// Input: msg.payload = 42
// Output: msg.payload = {value: 42, timestamp_ms: 1730986400000}

// Input: msg.payload = {temperature: 42, pressure: 100}
// Output: Unchanged (already an object)
```

### Topic Generation

The `umh_topic` field is **auto-generated** from metadata:
```javascript
// tag_processor automatically creates:
msg.meta.umh_topic = `umh.v1.${msg.meta.location_path}.${msg.meta.data_contract}.${msg.meta.virtual_path || ''}.${msg.meta.tag_name}`;
```

**Never set `umh_topic` manually** - let tag_processor generate it to ensure validation.

## Topic Naming Convention

UMH topics follow strict validation rules defined in `pkg/umhtopic/`:

### Pattern Structure

```
umh.v1.<location_path>.<data_contract>[.<virtual_path>].<name>
       └─────────────┘  └─────────────┘  └────────────┘  └────┘
       WHERE data is    WHAT data is     HOW organized   Sensor
       (ISA-95 levels)  (validation)     (optional)      name
```

### Validation Rules

| Component | Underscore Rule | Example | Validation |
|-----------|----------------|---------|------------|
| `location_path` | **MUST NOT** start with `_` | `enterprise.site.area` | ✅ Path segments |
| `data_contract` | **MUST** start with `_` | `_raw`, `_pump_v1` | ✅ Required underscore |
| `virtual_path` | **CAN** start with `_` | `motor.electrical`, `_hidden` | ✅ Optional organization |
| `name` | **CAN** start with `_` | `temperature`, `_internal` | ✅ Any valid identifier |

## Data Flow Patterns

### Tag Processor → UNS Output Flow

The most common pattern in bridges:

```yaml
input:
  s7comm:
    tcpDevice: "{{ .IP }}"
    addresses: ["DB1.DW20", "DB3.I270"]
pipeline:
  processors:
    - tag_processor:
        defaults: |
          msg.meta.location_path = "{{ .location_path }}";
          msg.meta.data_contract = "_raw";
          msg.meta.tag_name = msg.meta.s7_address;  // Use S7 address as tag name
          return msg;
output:
  uns: {}
```

**Flow**: S7 input → tag_processor (sets metadata) → UNS output (writes to Kafka)

### One Tag, One Topic Principle

**Design rule**: Each sensor/tag publishes to its own topic.

**Why**:
- **Zero merge logic**: No need to combine multiple sensors into one payload
- **Simple subscriptions**: Subscribe to exact data you need
- **Clear debugging**: One topic = one data source
- **Schema simplicity**: Each topic has single value type

**Wrong approach**:
```javascript
// ❌ DON'T: Multiple sensors in one payload
msg.payload = {
  temperature: 42,
  pressure: 100,
  flow: 75
};
msg.meta.tag_name = "machine_01";  // Ambiguous!
```

**Correct approach**:
```javascript
// ✅ DO: One message per sensor
// Message 1:
msg.payload = {value: 42, timestamp_ms: 1730986400000};
msg.meta.tag_name = "temperature";

// Message 2:
msg.payload = {value: 100, timestamp_ms: 1730986400000};
msg.meta.tag_name = "pressure";
```

### Processor Behaviors

**Dropping**: Return `null` or `undefined` to drop message
```javascript
if (msg.payload.value < threshold) {
  return null;  // Message dropped
}
return msg;
```

**Duplicating**: Return array of messages
```javascript
return [
  {payload: {value: msg.payload.temp}, meta: {tag_name: "temperature"}},
  {payload: {value: msg.payload.temp * 1.8 + 32}, meta: {tag_name: "temperature_f"}}
];
```

### Single Kafka Topic Architecture

**Critical**: UNS output writes ALL data to ONE Kafka topic: `umh.messages`

**Routing**:
- Topic name: Always `umh.messages` (created with `compact,delete` policy)
- Kafka key: `umh_topic` metadata field (the full topic string)
- Kafka headers: All Benthos metadata fields

**Why single topic**:
- Simplifies Kafka topic management (no dynamic topic creation)
- Enables compaction with meaningful keys
- Centralizes all UNS data in one place

**Consumer pattern**:
```go
// Consumers filter by Kafka key (umh_topic)
kafkaConsumer.Subscribe("umh.messages")
for msg := range kafkaConsumer.Messages() {
  topicKey := msg.Key  // e.g., "umh.v1.enterprise.site._raw.temperature"
  if strings.HasPrefix(topicKey, "umh.v1.enterprise.site") {
    // Process this location's data
  }
}
```

## Non-Intuitive Patterns

### UNS Output Requires UMH Core

The `uns` output plugin **ONLY works within UMH Core environment**:
- Requires embedded Redpanda (Kafka-compatible broker)
- Expects `umh.messages` topic to exist
- Uses UMH Core's Kafka configuration

**Cannot use standalone**: If running benthos-umh outside UMH Core, use standard Benthos outputs:
```yaml
output:
  kafka:
    addresses: ["localhost:9092"]
    topic: "umh.messages"
    key: "${! @umh_topic }"  # Use metadata as key
```

### JavaScript Engine: goja

All JavaScript execution uses **goja engine** (pure Go):
- **Security**: Sandbox isolated from host system
- **Performance**: Slower than V8 but no CGO dependency
- **Compatibility**: ES5.1 with some ES6 features
- **No Node.js APIs**: No `require()`, `fs`, `http`, etc.

**Available in**:
- `tag_processor` - Per-message transformation
- `nodered_js` - Node-RED style flows
- `stream_processor` - Multi-source aggregation with state

**Limitations**:
```javascript
// ✅ Works:
const result = msg.payload.value * 2;
JSON.stringify(msg);
Date.now();

// ❌ Doesn't work:
require('fs');           // No Node.js modules
fetch('https://...');    // No external HTTP
setTimeout(() => {}, 1000);  // No async timers
```

### OPC UA Browsing Behavior

OPC UA browse operations **only follow `Organizes` references**:
- Ignores `HasComponent`, `HasProperty`, `HasTypeDefinition`
- This is intentional to limit browse depth
- To access non-organized nodes, specify NodeID directly in configuration

### Stream Processor State Management

Stream processors maintain **in-memory state** across messages:
```javascript
// State persists across messages
if (!state.counter) state.counter = 0;
state.counter++;

// Access multiple sources
const temp = sources.temperature[0].payload.value;
const pressure = sources.pressure[0].payload.value;
```

**State lifecycle**: State lost on restart unless persisted to external storage.

### Template Variables in Nested Structures

Variables flatten during injection:
```yaml
variables:
  connection:
    ip: "192.168.1.100"
    port: 102
```

**Becomes**: `{{ .connection.ip }}` and `{{ .connection.port }}` (nested path preserved)

But in most configs, variables are flat:
```yaml
variables:
  IP: "192.168.1.100"  # Access as {{ .IP }}
  PORT: 102            # Access as {{ .PORT }}
```

### Payload Format Implications

**Time-series** (`{timestamp_ms, value}`): Use for sensor data
- Single value per timestamp
- Efficient for time-range queries
- Max 1MiB per message

**Relational** (any JSON): Use for business events
- Work orders, batch reports, alerts
- No timestamp requirement
- Can include arrays and nested objects

**Mixing formats**: Don't mix in same topic - use different `data_contract` values.

## Development Guidelines

### General Preferences
- Be terse and code-focused - provide answers immediately, explanations after
- Treat developer as expert - no basic explanations needed
- Give actual code, not high-level descriptions
- Respect existing code comments unless clearly irrelevant
- No unnecessary preambles, disclaimers, or AI identification
- Value arguments over authorities - source doesn't matter, correctness does
- Consider new technologies and contrarian approaches

### Golang Specific
- **Testing Framework**: Ginkgo v2 with Gomega matchers
- **Go Version Features**: Use newer Go methods when applicable
  - go1.21+: min/max/clear builtins, slices/maps packages
  - go1.22+: range over integers, math/rand/v2
  - go1.23+: iterator functions, unique package
- **Testing Rules**:
  - Do not remove focused tests (`FIt`, `FDescribe`)
  - Maintain existing documentation
  - Tests use `_test.go` and `_suite_test.go` pattern

## Build & Test Commands

### Core Commands
```bash
make all          # Clean and build
make target       # Build benthos binary
make clean        # Remove build artifacts
make test         # Run all tests with Ginkgo
```

### Plugin-Specific Tests
```bash
make test-opcua              # OPC UA plugin tests
make test-modbus             # Modbus plugin tests
make test-s7                 # S7 protocol tests
make test-sparkplug          # Sparkplug B tests
make test-eip                # Ethernet/IP tests
make test-sensorconnect      # Sensorconnect tests
make test-stream-processor   # Stream processor tests
make test-tag-processor      # Tag processor tests
make test-topic-browser      # Topic browser tests
make test-downsampler        # Downsampler tests
make test-classic-to-core    # Migration plugin tests
make test-uns                # UNS output tests
```

### Utility Commands
```bash
make license-check    # Verify Apache 2.0 headers
make license-fix      # Add missing license headers
make setup-test-deps  # Install test dependencies
make serve-pprof      # Run profiling server
```

## Testing Approach

- **Unit Tests**: Per-plugin using Ginkgo v2
  - Focus on business logic validation
  - Mock external dependencies
- **Integration Tests**: Using Docker containers
  - Test real protocol connections
  - Validate end-to-end data flow
- **YAML Validation**: Schema-based config testing
- **Benchmarks**: Performance testing for critical paths
  - `make bench-stream-processor`
  - `make bench-pkg-umh-topic`

### Test Patterns
```go
var _ = Describe("PluginName", func() {
    Context("when processing data", func() {
        It("should handle valid input", func() {
            // Test implementation
            Expect(result).To(Equal(expected))
        })
    })
})
```

## Common Tasks

### Adding a New Protocol
1. Create new plugin directory: `protocol_plugin/`
2. Implement input/output interfaces
3. Add documentation in `docs/input/` or `docs/output/`
4. Create tests with `_suite_test.go` and `_test.go`
5. Add Make target for testing
6. Update this CLAUDE.md

### Debugging Data Flows
1. Enable debug logging in configuration
2. Use `make serve-pprof` for performance profiling
3. Check plugin-specific logs for protocol details
4. Test with minimal config first
5. Use example configurations from `config/` directory

## Project Structure

```
benthos-umh/
├── cmd/benthos/          # Main application entry
├── config/               # Configuration examples
├── docs/                 # Documentation (GitBook format)
├── pkg/                  # Shared packages
│   └── umhtopic/         # UMH topic parsing
├── *_plugin/             # Protocol/processor plugins
├── Makefile              # Build automation
└── Makefile.Common       # Shared Make configuration
```

## Important Notes

- Default branch is `master` (not `main` or `staging`)
- Apache 2.0 license headers required on all source files
- Ginkgo runs tests in parallel with randomization
- Plugins are loaded dynamically at runtime
- Configuration uses standard Benthos YAML format
- Each plugin maintains its own documentation

## UX Standards

See `UX_STANDARDS.md` for UI/UX principles when building management interfaces or user-facing components.
