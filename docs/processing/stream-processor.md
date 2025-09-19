# Stream Processor

The Stream Processor is a specialized Benthos processor designed to collect timeseries data from multiple UNS (Unified Namespace) sources, maintain state for variable mappings, and generate transformed messages using JavaScript expressions. It operates exclusively with UNS input/output and implements sophisticated dependency-based evaluation logic.

## When to Use This Processor

Use the Stream Processor when you need to:
- Combine data from multiple UNS timeseries sources into calculated outputs
- Maintain state between messages for complex calculations
- Transform data using JavaScript expressions with variable dependencies
- Generate new UMH topics based on a model/version data contract
- Perform real-time data aggregation and transformation at the edge

**Note:** This processor requires UMH timeseries format input (`value` + `timestamp_ms`) and is designed specifically for UNS/UMH ecosystems.

## Key Features

### Dependency-Based Evaluation

The processor implements an intelligent evaluation system:

- **Static Mappings**: Expressions with no variable dependencies are evaluated on every incoming message
- **Dynamic Mappings**: Expressions that reference variables are only evaluated when their dependent variables are received
- **Partial Dependencies**: Mappings with multiple dependencies wait until all required variables are available

This approach optimizes performance and ensures calculations only occur when meaningful.

### JavaScript Expression Engine

The Stream Processor uses the same underlying [goja](https://github.com/dop251/goja) JavaScript engine as the [Node-RED JavaScript Processor](node-red-javascript-processor.md), but with different configurations optimized for different use cases:

**Stream Processor JavaScript Features:**
- **Expression-based**: Evaluates single JavaScript expressions for each mapping
- **Security sandbox**: Prevents dangerous operations (eval, require, process access)
- **Pre-compiled expressions**: All expressions are compiled at startup for optimal performance
- **Dependency tracking**: Automatically detects which variables each expression uses
- **Optimized for streaming**: Designed for high-throughput data transformation

**Comparison with Node-RED JavaScript Processor:**
- Node-RED JS runs full JavaScript functions with `return` statements
- Stream Processor evaluates expressions directly (no function wrapper needed)
- Both use security sandboxing to prevent dangerous operations
- Node-RED JS allows console.log() for debugging, Stream Processor blocks it
- Stream Processor adds expression pre-compilation and dependency analysis

**Security Features (shared with Node-RED JS):**
- Maximum call stack size limits to prevent stack overflow
- Blocked dangerous globals (eval, Function, require, process, setTimeout)
- Safe built-in objects available (Math, JSON, Date, String, Number)
- Execution within a sandboxed environment

### State Management

- Maintains current values of all configured source variables
- State persists across messages within the same processor instance
- Enables calculations that depend on multiple asynchronous data sources

## Configuration

```yaml
processors:
  - stream_processor:
      mode: "timeseries"              # Processing mode (currently only timeseries supported)
      output_topic: "umh.v1.enterprise.site.area"  # Base output topic
      model:
        name: "pump"                  # Model name for data contract
        version: "v1"                 # Model version
      sources:                        # Map variable names to UNS topics
        press: "umh.v1.enterprise.site.area._raw.pressure"
        temp: "umh.v1.enterprise.site.area._raw.temperature"
        run: "umh.v1.enterprise.site.area._raw.running"
      mapping:                        # JavaScript transformations
        # Static mappings (no dependencies)
        serialNumber: '"SN-P42-008"'
        deviceType: '"pump"'
        # Dynamic mappings (with dependencies)
        pressure: "press + 4.00001"              # Depends on 'press'
        temperatureC: "(temp - 32) * 5/9"        # Depends on 'temp'
        motor:
          rpm: "press / 4"                       # Nested, depends on 'press'
        status: 'run ? "active" : "inactive"'    # Depends on 'run'
        efficiency: "(press / temp) * 100"       # Depends on both 'press' and 'temp'
```

### Configuration Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `mode` | string | Yes | Processing mode. Currently only `"timeseries"` is supported |
| `output_topic` | string | Yes | Base UMH topic for output messages |
| `model.name` | string | Yes | Model name used in data contract generation |
| `model.version` | string | Yes | Model version used in data contract generation |
| `sources` | map[string]string | Yes | Maps variable names to their source UNS topics |
| `mapping` | object | No | JavaScript expressions for field transformations |

## Message Processing Workflow

### Input Requirements

Messages must meet these requirements:
1. **UMH Timeseries Format**: JSON payload with `value` and `timestamp_ms` fields
2. **Metadata**: Must have `umh_topic` metadata field
3. **Topic Match**: The `umh_topic` must match one of the configured sources

Example input message:
```json
{
  "value": 25.5,
  "timestamp_ms": 1647890123456
}
```
With metadata: `umh_topic: "umh.v1.enterprise.site.area._raw.pressure"`

### Processing Steps

1. **Topic Resolution**: Incoming message's `umh_topic` is matched against configured sources
2. **State Update**: The variable value is stored in the processor's state
3. **Static Evaluation**: All static mappings (no dependencies) are evaluated
4. **Dynamic Evaluation**: Dynamic mappings that depend on the updated variable are evaluated if all their dependencies are satisfied
5. **Output Generation**: New messages are created with transformed values

### Output Topic Construction

Output topics follow the UMH convention:
```
<output_topic>.<data_contract>[.<virtual_path>].<name>
```

Where:
- `output_topic`: The configured base topic
- `data_contract`: `_<model_name>_<model_version>` (e.g., `_pump_v1`)
- `virtual_path`: Optional path segments for nested mappings
- `name`: The mapping field name

Examples:
- Simple: `umh.v1.enterprise.site.area._pump_v1.pressure`
- Nested: `umh.v1.enterprise.site.area._pump_v1.motor.rpm`

## JavaScript Expressions

### Allowed Operations

The processor supports standard JavaScript operations within a secure sandbox:

```javascript
// Math operations
"Math.PI * 2"
"Math.sqrt(press * press + temp * temp)"

// Arithmetic
"press * 2 + 1"
"(temp - 32) * 5/9"

// Conditionals
"press > 100 ? 'high' : 'normal'"
"run ? 'active' : 'inactive'"

// String operations
'"Pump-" + press'
'`Temperature: ${temp}°C`'

// JSON operations
"JSON.stringify({pressure: press, temp: temp})"

// Date operations
"Date.now()"
```

### Blocked Operations

For security, these operations are blocked:
- `eval()` - No dynamic code evaluation
- `Function()` - No function constructors
- `require()` - No module imports
- `process` - No process access
- `setTimeout/setInterval` - No async operations
- `console` - No console access

## Examples

### Basic Sensor Processing

Transform raw sensor values with calibration offsets:

```yaml
stream_processor:
  mode: "timeseries"
  output_topic: "umh.v1.plant1.line5.cell3"
  model:
    name: "sensor_calibrated"
    version: "v1"
  sources:
    raw_temp: "umh.v1.plant1.line5.cell3._raw.temperature"
    raw_press: "umh.v1.plant1.line5.cell3._raw.pressure"
  mapping:
    temperature: "raw_temp + 0.5"  # Calibration offset
    pressure: "raw_press * 1.02"   # Calibration factor
    timestamp: "Date.now()"        # Current time (static)
```

### Conditional Status Generation

Generate status based on multiple inputs:

```yaml
stream_processor:
  mode: "timeseries"
  output_topic: "umh.v1.factory.assembly"
  model:
    name: "machine_status"
    version: "v2"
  sources:
    speed: "umh.v1.factory.assembly._raw.conveyor_speed"
    temp: "umh.v1.factory.assembly._raw.motor_temp"
    running: "umh.v1.factory.assembly._raw.is_running"
  mapping:
    status: |
      running ? (temp > 80 ? "warning" : "running") : "stopped"
    alert: "temp > 90"
    efficiency: "running ? (speed / 100) * 100 : 0"
```

### Complex Calculations with Dependencies

Perform calculations that require multiple variables:

```yaml
stream_processor:
  mode: "timeseries"
  output_topic: "umh.v1.site.boiler"
  model:
    name: "efficiency_calc"
    version: "v1"
  sources:
    flow_in: "umh.v1.site.boiler._raw.input_flow"
    flow_out: "umh.v1.site.boiler._raw.output_flow"
    temp_in: "umh.v1.site.boiler._raw.input_temp"
    temp_out: "umh.v1.site.boiler._raw.output_temp"
  mapping:
    # Static mapping - always evaluated
    unit: '"BTU/hr"'

    # Single dependency
    flow_diff: "flow_out - flow_in"
    temp_diff: "temp_out - temp_in"

    # Multiple dependencies - only evaluates when all are available
    thermal_efficiency: |
      (flow_out * temp_out - flow_in * temp_in) / (flow_in * temp_in) * 100
    heat_transfer: "(flow_out - flow_in) * (temp_out - temp_in) * 500"
```

### Nested Object Structure

Create hierarchical output structures:

```yaml
stream_processor:
  mode: "timeseries"
  output_topic: "umh.v1.plant.packaging"
  model:
    name: "machine_metrics"
    version: "v3"
  sources:
    speed: "umh.v1.plant.packaging._raw.belt_speed"
    count: "umh.v1.plant.packaging._raw.package_count"
    weight: "umh.v1.plant.packaging._raw.total_weight"
  mapping:
    performance:
      speed: "speed"
      throughput: "count / 60"  # packages per minute
    quality:
      avg_weight: "weight / count"
      deviation: "Math.abs((weight / count) - 1.5)"  # from 1.5kg target
    metadata:
      timestamp: "Date.now()"
      shift: "new Date().getHours() < 12 ? 'morning' : 'afternoon'"
```

Output topics for nested example:
- `umh.v1.plant.packaging._machine_metrics_v3.performance.speed`
- `umh.v1.plant.packaging._machine_metrics_v3.performance.throughput`
- `umh.v1.plant.packaging._machine_metrics_v3.quality.avg_weight`
- `umh.v1.plant.packaging._machine_metrics_v3.quality.deviation`
- `umh.v1.plant.packaging._machine_metrics_v3.metadata.timestamp`
- `umh.v1.plant.packaging._machine_metrics_v3.metadata.shift`

## Processing Behavior

### Static vs Dynamic Evaluation

Understanding when mappings are evaluated is crucial:

**Static Mappings** (evaluated on every message):
- Constant values: `"SN-12345"`
- Time functions: `Date.now()`
- Math constants: `Math.PI`

**Dynamic Mappings** (evaluated only when dependencies update):
- Single dependency: `press * 2`
- Multiple dependencies: `press / temp`
- Conditional with dependency: `run ? "active" : "stopped"`

### Metadata Preservation

All metadata from input messages is preserved in output messages, except:
- `umh_topic`: Replaced with the new output topic
- All other metadata fields are passed through unchanged

### Error Handling

The processor handles errors gracefully:
- **Invalid JSON**: Message skipped, logged, processing continues
- **Missing Dependencies**: Mapping skipped until all dependencies available
- **JavaScript Errors**: Failed mapping skipped, other mappings continue
- **Missing umh_topic**: Message skipped entirely
