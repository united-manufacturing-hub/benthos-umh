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

benthos-umh is **orchestrated by UMH Core** - it does not run standalone. UMH Core manages benthos instances through FSM state machines and S6 process supervision.

**High-level flow**:
1. User deploys bridge in ManagementConsole
2. umh-core Agent generates benthos config from templates
3. umh-core launches benthos process via S6
4. benthos process runs and sends data to Kafka

**For orchestration details** (FSM, S6, template expansion, logging, debugging in production), see `umh-core/CLAUDE.md`.

## Debugging benthos

**Config validation errors**:
```bash
# Test config without running
benthos lint -c config.yaml

# Check for template expansion issues
grep "{{" config.yaml  # Should be empty if variables expanded
```

**Plugin errors**:
```bash
# List available plugins
benthos list inputs
benthos list processors
benthos list outputs

# Run with debug logging
benthos run -c config.yaml --log.level DEBUG
```

**Connection issues**:
- Check protocol-specific logs for timeout/refused/unreachable errors
- Verify input plugin addresses, ports, credentials
- Test connectivity outside benthos (ping, telnet, protocol-specific tools)

**Data processing issues**:
- Enable debug logging to see message flow
- Check processor JavaScript for syntax errors (goja engine)
- Verify metadata fields are set correctly (msg.meta.*)
- Test with minimal config to isolate the problem

**For production debugging in UMH Core environment**, see `umh-core/CLAUDE.md`.

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

UMH topics follow strict validation rules defined in `pkg/umh/topic/`:

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

## Implementation Details

### OPC UA Server Profiles

Auto-optimizes Browse workers and Subscribe batch size based on detected server vendor.

**User docs**: See [docs/input/opc-ua-input.md](docs/input/opc-ua-input.md#server-profiles-and-performance-tuning)

**Implementation files**:
- `opcua_plugin/server_profiles.go` - Profile definitions and auto-detection logic
- `opcua_plugin/core_browse_workers.go` - Worker pool (Browse phase)
- `opcua_plugin/read_discover.go` - Batch operations (Subscribe phase)

**Key insight**: Profile values are production-safe limits from vendor docs, not server-reported theoretical maximums (e.g., S7-1200 uses 100 not 1000).

**Adding new profiles**: Define in `server_profiles.go`, add detection logic in `DetectServerProfile()`, validate in `init()`.

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

### Plugin Field Classification

Plugin fields follow a required/optional rule for consistent UI generation in the Management Console.

#### Core Rule

| Field Type | Modifiers | Shows in UI |
|------------|-----------|-------------|
| **Basic** | No `.Default()`, no `.Advanced()` | Required (asterisk) |
| **Advanced** | `.Default().Advanced()` | Optional (collapsed) |

**Key insight:** `.Default()` alone makes a field not required. `.Optional()` is rarely needed.

#### Examples

```go
// Basic field - required, user MUST configure
Field(service.NewStringField("endpoint").
    Description("OPC UA endpoint URL"))

// Advanced field - optional with sensible default
Field(service.NewIntField("sessionTimeout").
    Description("Session timeout in milliseconds").
    Default(10000).
    Advanced())
```

#### Field Examples Best Practices

- **Always list all valid options** in `.Examples()` for enum-like fields
- **Boolean fields**: Include both `true` and `false`
- **Numeric fields**: Only include the default value unless other values represent meaningful thresholds
- **String lists**: Show common patterns (single item, multiple items)

```go
// Good: enum-like field with all options
Field(service.NewStringField("securityMode").
    Examples("", "None", "Sign", "SignAndEncrypt"))

// Good: boolean with both values
Field(service.NewBoolField("subscribeEnabled").
    Examples(true, false))

// Good: numeric with just default (no arbitrary values)
Field(service.NewIntField("queueSize").
    Default(10).
    Examples(10))
```

#### When to Use Basic vs Advanced

**Basic**: Required for plugin to function (endpoint, device address, credentials)
**Advanced**: Optimization parameters with sensible defaults (timeout, buffer size, retry count)

## Build & Test Commands

### Core Commands
```bash
make all          # Clean and build
make build        # Build benthos binary (outputs to tmp/bin/benthos)
make clean        # Remove build artifacts
make test         # Run all tests with Ginkgo
make run CONFIG=./config/opcua-hex-test.yaml LOG_LEVEL=DEBUG  # Run locally with a config
```

### Plugin-Specific Tests
```bash
make test-unit-opc           # OPC UA unit tests (sets TEST_OPCUA_UNIT=true)
make test-integration-opc    # OPC UA integration tests (needs hardware env vars, see `make env`)
make test-modbus             # Modbus plugin tests
make test-s7comm             # S7 protocol tests
make test-sparkplug          # Sparkplug B tests
make test-eip                # Ethernet/IP tests
make test-sensorconnect      # Sensorconnect tests (runs serially)
make test-stream-processor   # Stream processor tests (with -race)
make test-tag-processor      # Tag processor tests (sets TEST_TAG_PROCESSOR=true)
make test-noderedjs          # Node-RED JS tests (sets TEST_NODERED_JS=true)
make test-topic-browser      # Topic browser tests (sets TEST_TOPIC_BROWSER=1)
make test-downsampler        # Downsampler tests
make test-classic-to-core    # Migration plugin tests (sets TEST_CLASSIC_TO_CORE=1)
make test-pkg-umh-topic      # UMH topic library tests
make test-uns                # UNS tests (excludes Redpanda-dependent tests)
make test-uns-redpanda       # Full UNS tests (requires Docker for testcontainers)
```

### Lint & Format
```bash
make lint              # Run golangci-lint
make lint-fix          # Auto-fix lint issues
make fmt               # Format with gofumpt + gci (import ordering)
```

### Code Generation
```bash
make proto                              # Regenerate protobuf Go files
make generate-schema VERSION=0.11.7     # Generate plugin schemas for ManagementConsole
```

### Fuzz & Benchmarks
```bash
make fuzz-stream-processor   # Run fuzz tests (press Ctrl+C to stop)
make bench-stream-processor  # Benchmark stream processor
make bench-pkg-umh-topic     # Benchmark topic parsing
```

### Utility Commands
```bash
make license-check    # Verify Apache 2.0 headers
make license-fix      # Add missing license headers
make setup-test-deps  # Install test dependencies
make install          # Install all dev tools (lint, fmt, license, protoc, etc.)
make serve-pprof      # Run profiling server
make env              # Show all test environment variables
```

### Running a Single Test
```bash
# Run a single test by name pattern
go run github.com/onsi/ginkgo/v2/ginkgo --focus "test name pattern" ./plugin_dir/...

# Or use FIt/FDescribe in code to focus tests (don't commit these)
FIt("should handle valid input", func() { ... })
```

### Test Environment Variables

Many tests are gated by environment variables and silently skip without them. The `make test-*` targets set these automatically, but they matter when running tests directly with `go test` or Ginkgo.

| Variable | Used by | Set by make target |
|---|---|---|
| `TEST_TAG_PROCESSOR=true` | tag_processor_plugin tests | `test-tag-processor` |
| `TEST_OPCUA_UNIT=true` | opcua_plugin unit tests | `test-unit-opc` |
| `TEST_NODERED_JS=true` | nodered_js_plugin tests | `test-noderedjs` |
| `TEST_CLASSIC_TO_CORE=1` | classic_to_core_plugin tests | `test-classic-to-core` |
| `TEST_TOPIC_BROWSER=1` | topic_browser_plugin tests | `test-topic-browser` |

Run `make env` for the full list including hardware integration test variables.

## Testing Approach

- **Unit Tests**: Per-plugin using Ginkgo v2
  - Focus on business logic validation
  - Mock external dependencies
- **Integration Tests**: Using Docker containers
  - Test real protocol connections
  - Validate end-to-end data flow
- **YAML Validation**: Schema-based config testing
- **Benchmarks**: Performance testing for critical paths (see Fuzz & Benchmarks section above)

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
│   └── umh/topic/        # UMH topic parsing
├── *_plugin/             # Protocol/processor plugins
├── Makefile              # Build automation
└── Makefile.Common       # Shared Make configuration
```

## Important Notes

- Default branch is `master` (not `main` or `staging`)
- Go version: 1.25.5
- GODEBUG: `x509negativeserial=1` (for Kepware OPC UA certificate compatibility)
- Import ordering (enforced by `make fmt`): standard → third-party → `github.com/united-manufacturing-hub/benthos-umh`
- Apache 2.0 license headers required on all source files
- Ginkgo runs tests in parallel with randomization
- Plugins are loaded dynamically at runtime
- Configuration uses standard Benthos YAML format
- Each plugin maintains its own documentation

### External Plugins

The bundle (`cmd/benthos/bundle/package.go`) also imports 3 external community plugins:
- `github.com/RuneRoven/benthosADS` - Beckhoff ADS protocol
- `github.com/RuneRoven/benthosAlarm` - Alarm plugin
- `github.com/RuneRoven/benthosSMTP` - SMTP output

## UX Standards

See `UX_STANDARDS.md` for UI/UX principles when building management interfaces or user-facing components.
