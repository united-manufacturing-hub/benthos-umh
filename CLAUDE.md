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

benthos-umh is **primarily designed to run within UMH Core**, which orchestrates benthos instances through Finite State Machines (FSM) and S6 supervision. **benthos-umh does NOT run standalone** - it requires UMH Core for lifecycle management.

### Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                    umh-core                         │
├─────────────────────────────────────────────────────┤
│  Agent (Go):                                        │
│  - Reads config.yaml                                │
│  - Expands template variables                       │
│  - Generates benthos configs from templates         │
│                                                      │
│  FSM Controllers:                                   │
│  - BenthosFSM: Manages lifecycle per bridge/DFC    │
│  - Creates S6 service directories                   │
│  - Monitors process health                          │
│                                                      │
│  S6 Supervision:                                    │
│  - Launches benthos-umh processes                   │
│  - Restarts on crash                                │
│  - Manages logs with tai64n timestamps             │
└─────────────────────────────────────────────────────┘
                     │ Orchestrates
                     ↓
┌─────────────────────────────────────────────────────┐
│              benthos-umh processes                  │
│  - Each bridge = separate process                   │
│  - Each DFC = separate process                      │
│  - Process name: benthos-{bridge-id}                │
│  - Config path: /data/services/benthos-{id}/data/  │
└─────────────────────────────────────────────────────┘
```

### Lifecycle Management

#### 1. User Action in ManagementConsole

User clicks "Deploy Bridge" → Frontend sends action to backend → Redis queue → umh-core Puller retrieves action

#### 2. umh-core Agent Processes Action

```go
// umh-core/pkg/agent/agent.go
func (a *Agent) handleDeployProtocolConverter(payload interface{}) {
    // 1. Update config.yaml with bridge configuration
    a.configManager.AddProtocolConverter(bridgeConfig)

    // 2. Trigger FSM reconciliation
    a.benthosFSM.SetDesiredState(bridgeID, "active")
}
```

#### 3. FSM Generates benthos Config

```go
// umh-core/pkg/fsm/benthos/reconcile.go
func (b *BenthosFSM) Reconcile(ctx context.Context) {
    // Read config.yaml
    config := b.configManager.GetConfig()

    // Find bridge configuration
    bridge := config.ProtocolConverters[bridgeID]

    // Expand template variables
    benthosConfig := b.expandTemplate(bridge.TemplateRef, bridge.Variables)

    // Write to /data/services/benthos-{id}/data/config.yaml
    b.writeConfig(bridgeID, benthosConfig)

    // Trigger S6 to start process
    b.s6.Enable(fmt.Sprintf("benthos-%s", bridgeID))
}
```

#### 4. S6 Launches benthos Process

```bash
# S6 creates service directory structure:
/data/services/benthos-bridge-123/
├── run                          # S6 run script
│   └── exec benthos run -c data/config.yaml
├── finish                       # S6 finish script (handles cleanup)
├── supervise/                   # S6 internal state
│   ├── status                   # Process state
│   ├── lock                     # Supervision lock
│   └── control                  # Control socket
└── data/
    └── config.yaml              # Generated benthos config (expanded)
```

#### 5. benthos Process Runs

```bash
# Process launched by S6:
/usr/local/bin/benthos run -c /data/services/benthos-bridge-123/data/config.yaml

# Logs written to:
/data/logs/benthos-bridge-123/current
```

### Template Variable System

**CRITICAL**: ManagementConsole NEVER writes benthos config. It only updates config.yaml, and umh-core generates benthos configs from templates.

#### Variable Sources

1. **Connection variables** (from `protocolConverter.connection.variables`):
   ```yaml
   connection:
     variables:
       IP: "192.168.1.100"
       PORT: 502
   ```

2. **Agent location** (from `agent.location`):
   ```yaml
   agent:
     location:
       - enterprise: "ACME"
       - site: "Factory-1"
   ```

3. **Bridge location** (from `protocolConverter.location`):
   ```yaml
   protocolConverter:
     location:
       - line: "Line-A"
       - cell: "Cell-5"
   ```

#### Variable Flattening

**IMPORTANT**: Nested variables become top-level in templates:

```yaml
# In config.yaml:
connection:
  variables:
    IP: "192.168.1.100"
    PORT: 502

# In benthos template (FLAT ACCESS):
{{ .IP }}     # NOT {{ .variables.IP }}
{{ .PORT }}   # NOT {{ .variables.PORT }}
```

#### Location Path Computation

Agent location + bridge location = `{{ .location_path }}`:

```yaml
# Agent: enterprise.site
# Bridge: line.cell
# Result: {{ .location_path }} = "enterprise.site.line.cell"
```

#### Template Expansion Example

```yaml
# Template in config.yaml:
templates:
  - id: "modbus-tcp"
    config: |
      input:
        label: "modbus_{{ .name }}"
        modbus_tcp:
          address: "{{ .IP }}:{{ .PORT }}"
      pipeline:
        processors:
          - tag_processor:
              defaults: |
                msg.meta.location_path = "{{ .location_path }}";
                msg.meta.data_contract = "_raw";
                return msg;
      output:
        uns: {}

# Bridge config:
protocolConverters:
  - id: "bridge-123"
    name: "PLC-Bridge"
    templateRef: "modbus-tcp"
    connection:
      variables:
        IP: "192.168.1.100"
        PORT: 502
    location:
      - line: "Line-A"

# Generated benthos config (/data/services/benthos-bridge-123/data/config.yaml):
input:
  label: "modbus_PLC-Bridge"
  modbus_tcp:
    address: "192.168.1.100:502"
pipeline:
  processors:
    - tag_processor:
        defaults: |
          msg.meta.location_path = "ACME.Factory-1.Line-A";
          msg.meta.data_contract = "_raw";
          return msg;
output:
  uns: {}
```

### FSM Orchestration

UMH Core manages benthos instances through state machines:

#### Bridge FSM States

```
stopped
  ↓ (User clicks "Deploy")
starting_connection (verifying device availability)
  ↓ (nmap probe succeeds)
starting_redpanda (Kafka broker startup)
  ↓ (Redpanda ready)
starting_dfc (launching benthos process)
  ↓ (benthos process running)
idle (benthos running, no data flowing)
  ↓ (Data detected)
active (benthos running, data flowing)
```

**Degraded states**:
- `starting_failed_connection` - Device unreachable
- `starting_failed_redpanda` - Kafka broker issue
- `starting_failed_dfc` - benthos config/process error

#### State Transition Triggers

```go
// FSM checks conditions every reconciliation cycle (default: 1s)

// idle → active:
if throughput > 0 {
    fsm.Transition("active")
}

// active → idle:
if throughput == 0 && idleFor > 30s {
    fsm.Transition("idle")
}

// * → starting_failed_dfc:
if benthosProcess.Exited() && attempts < maxRetries {
    fsm.Transition("starting_failed_dfc")
    scheduleRetry()
}
```

### S6 Supervision Details

S6 is the process supervisor for all benthos instances:

#### S6 Service States

**Check service state**:
```bash
# List all benthos services
ls -la /data/services/benthos-*/supervise/status

# Check specific service
s6-svstat /data/services/benthos-bridge-123/

# Example output:
# up (pid 1234) 300 seconds
# down 45 seconds, normally up, want up, ready 0 seconds
```

**S6 State Meanings**:
- `up (pid XXXX)` - Process running
- `down` - Process not running
- `normally up` - Process should be running (will restart)
- `want down` - Process should be stopped (won't restart)
- `ready N seconds` - Process finished starting up N seconds ago

#### S6 Log Management

**Log rotation**:
```bash
# Logs are in /data/logs/benthos-{id}/
current         # Current log (active writing)
@*.s            # Rotated logs (clean rotation)
@*.u            # Unfinished logs (container killed mid-write)

# Read with tai64n timestamp decoding
tai64nlocal < /data/logs/benthos-bridge-123/current | tail -100

# Search across all log files
grep "error" /data/logs/benthos-bridge-123/@* | tai64nlocal
```

**Log file naming**:
- `@400000006572a5b80f1e3c2c.s` - Timestamp in TAI64N format + `.s` (clean)
- `@400000006572a5b80f1e3c2c.u` - `.u` means unfinished (container killed)

### Debugging benthos in Production

#### 1. Check FSM State

```bash
# Check umh-core logs for FSM transitions
grep "bridge-123.*FSM" /data/logs/umh-core/current | tai64nlocal | tail -50

# Look for:
# - Current state
# - Desired state
# - Transition attempts
# - Error messages
```

#### 2. Check S6 Process Health

```bash
# Is process running?
s6-svstat /data/services/benthos-bridge-123/

# If down, check why:
# - down file present? (manual stop)
ls -la /data/services/benthos-bridge-123/down

# - Crashed and not restarting?
tail -100 /data/logs/benthos-bridge-123/current | tai64nlocal
```

#### 3. Verify Config After Variable Expansion

```bash
# Check generated benthos config
cat /data/services/benthos-bridge-123/data/config.yaml

# Compare with template in config.yaml
grep -A 50 "templates:" /data/config.yaml | grep -A 30 "modbus-tcp"

# Verify variables were expanded (no {{ }} left)
grep "{{" /data/services/benthos-bridge-123/data/config.yaml
# Should return empty if expansion succeeded
```

#### 4. Check benthos Logs

```bash
# Recent errors
tai64nlocal < /data/logs/benthos-bridge-123/current | grep -i "error" | tail -20

# Protocol-specific errors
tai64nlocal < /data/logs/benthos-bridge-123/current | grep -i "s7comm\|modbus\|opcua"

# Connection issues
tai64nlocal < /data/logs/benthos-bridge-123/current | grep -i "connection\|timeout\|refused"
```

### Common Cross-Repository Issues

#### Issue: Bridge shows "starting" but benthos is actually running and processing data

**Root Cause**: umh-core Communicator Pusher channel overflow prevents status updates from reaching ManagementConsole

**Investigation**:
```bash
# Check umh-core for channel overflow
grep "Outbound message channel is full" /data/logs/umh-core/current

# Verify benthos is processing (check Kafka)
rpk topic consume umh.messages --num 10

# Check benthos logs show normal operation
tail -50 /data/logs/benthos-bridge-123/current | tai64nlocal | grep "INFO"
```

**Resolution**: This is a display bug. Data is flowing correctly. umh-core will eventually catch up on status updates.

---

#### Issue: Template variables not expanding, benthos fails with "unexpected '{{'"

**Root Cause**: Template expansion failed or didn't happen

**Investigation**:
```bash
# Check for {{ }} in generated config
grep "{{" /data/services/benthos-bridge-123/data/config.yaml

# Check umh-core logs for template expansion errors
grep "template.*bridge-123" /data/logs/umh-core/current | tai64nlocal

# Verify variables exist in config.yaml
grep -A 10 "bridge-123" /data/config.yaml
```

**Resolution**: Fix variable definitions in config.yaml or template reference.

---

#### Issue: benthos process crashes immediately on start

**Root Cause**: Config validation failure or plugin error

**Investigation**:
```bash
# Check benthos startup logs
tail -100 /data/logs/benthos-bridge-123/current | tai64nlocal

# Look for config validation errors
grep "failed to parse\|invalid configuration" /data/logs/benthos-bridge-123/current

# Check S6 state
s6-svstat /data/services/benthos-bridge-123/
# If "ready 0 seconds" keeps resetting, process is crash-looping
```

**Resolution**: Fix benthos config syntax or plugin configuration.

### Development Workflow for Cross-Repo Changes

When adding new benthos features that integrate with umh-core:

1. **Develop benthos plugin** in benthos-umh repo
2. **Test standalone** with example config in `config/` directory
3. **Create template** in umh-core's `config.yaml`
4. **Test in umh-core** with `make test-no-copy`
5. **Add frontend UI** in ManagementConsole (if needed)
6. **End-to-end test**:
   - Deploy local umh-core instance
   - Use ManagementConsole to deploy bridge
   - Monitor FSM transitions in umh-core logs
   - Monitor benthos process in S6 logs
   - Verify data flows to Kafka

**Debugging checklist**:
- [ ] benthos plugin works standalone (test with `benthos run -c config.yaml`)
- [ ] Template variables expand correctly in umh-core
- [ ] S6 launches process successfully
- [ ] FSM transitions to correct states
- [ ] Data appears in Kafka (use `rpk topic consume`)
- [ ] Status updates flow back to ManagementConsole

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
