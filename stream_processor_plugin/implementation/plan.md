# Stream Processor Plugin Plan

## Overview

The `stream_processor` is a specialized Benthos processor designed to collect timeseries data from multiple UNS sources, maintain state for variable mappings, and generate transformed messages using JavaScript expressions. It operates exclusively with UNS input/output and maintains per-model state for variable resolution.

## Architecture

### Core Components

1. **Config Parser** (`config.go`)
   - Parse mode, model, output_topic, sources, and mapping structure
   - Validate JavaScript expressions and topic patterns
   - Handle nested mapping fields for virtual paths (e.g., motor.rpm)
   - Validate that mapping expressions only reference source variables
   - Identify static mappings (expressions that don't reference any source variables)

2. **State Manager** (`state.go`)
   - Variable storage with thread-safe access
   - Source-to-variable mapping resolution
   - Simple key-value store for variable values

3. **JavaScript Engine** (`js_engine.go`)
   - Goja runtime integration for mapping expressions
   - Variable context injection for dynamic mappings
   - Static expression evaluation (no variables needed)
   - Expression validation and execution

4. **Message Processor** (`processor.go`)
   - BatchProcessor interface implementation
   - UNS message parsing and validation
   - Output message generation with topic construction

5. **Metrics & Monitoring** (`metrics.go`)
   - Processing counters and timers
   - Error tracking per source/virtual path
   - Variable resolution success rates

6. **Optimizations & Benchmarks**
   - Create benchmarks, to validate how many messages can be proceessed per second.
      - Goal: >100k msg/sec
   - Consider optimizations to reduce memory/cpu usage or to speed up processing

## Configuration Structure

```yaml
stream_processor:
  mode: timeseries
  model:
    name: pump
    version: v1
  output_topic: umh.v1.corpA.plant-A.aawd  # base topic for outputs
  sources:               # alias → raw topic
    press: umh.v1.corpA.plant-A.aawd._raw.press
    tF:    umh.v1.corpA.plant-A.aawd._raw.tempF
    r:     umh.v1.corpA.plant-A.aawd._raw.run
  mapping:  # field → JS / constant / alias
    pressure: press+4.00001
    temperature: tF*69/31
    motor: 
      rpm: press/4
    serialNumber: '"SN-P42-008"'
```

## Implementation Details

### Message Processing Flow

1. **Input Message Validation**
   - Verify umh_topic metadata exists
   - Parse UMH topic structure
   - Validate timeseries format: `{"value": X, "timestamp_ms": Y}`

2. **Variable Resolution**
   - Match incoming umh_topic against configured sources
   - Extract variable name from source mapping
   - Store value with timestamp in variable state

3. **Mapping Execution**
   - For static mappings (identified at startup):
     - Always execute and generate output messages
   - For dynamic mappings that reference the received variable:
     - Check if all referenced variables are available
     - Execute JavaScript expression with variable context
     - Generate output message with calculated value

4. **Output Generation**
   - Create new message with transformed value
   - Construct output topic from output_topic, data contract, and virtual path
   - Set umh_topic metadata to constructed topic
   - Preserve original timestamp_ms
   - Format as UMH-core timeseries structure

### Processing Flow Diagram

See processing_flow.svg

**Key Behavior**: The processor implements dependency-based evaluation with special handling for static mappings:

1. **Static Mappings**: Identified at startup and evaluated on every incoming message
2. **Dynamic Mappings**: Only evaluated when their dependent variables are received

Example with variables `a`, `b` available and mappings:
- `x: a+b` (dynamic - depends on `a`, `b`)
- `y: c+b` (dynamic - depends on `c`, `b`)
- `z: a+c` (dynamic - depends on `a`, `c`)
- `static: '"constant"'` (static - no dependencies)

When variable `c` is received:
- `y` and `z` are evaluated (they depend on `c`)
- `static` is also evaluated (always emitted)
- `x` is not evaluated (doesn't depend on `c`)

**Static Mapping Examples**:
- `serialNumber: '"SN-P42-008"'` (string constant)
- `deviceType: '"pump"'` (string constant) 
- `version: 42` (numeric constant)
- `timestamp: Date.now()` (dynamic constant - evaluated each time)

**Important Constraint**: Mappings can only reference variables from the sources configuration, not other computed mappings. For example, `pressure` cannot reference `temperature` from the mapping - it can only reference source variables like `press`, `tF`, `r`.

### Output Topic Construction

Output topics are constructed from the configured output_topic, data contract, and virtual path:

```
<output_topic>.<data_contract>.<virtual_path>
```

Where:
- **data_contract**: `_<model_name>_<model_version>` (e.g., `_pump_v1`)
- **virtual_path**: `<field_path>` (e.g., `pressure`, `motor.rpm`)

Examples:
- `pressure` → `umh.v1.corpA.plant-A.aawd._pump_v1.pressure`
- `motor.rpm` → `umh.v1.corpA.plant-A.aawd._pump_v1.motor.rpm`
- `serialNumber` → `umh.v1.corpA.plant-A.aawd._pump_v1.serialNumber`

The data contract is generated from the model configuration, and the virtual path is generated from the mapping structure.

### Static Mapping Identification

At processor initialization, mappings are analyzed to determine their dependencies:

```go
type MappingType int

const (
    StaticMapping  MappingType = iota  // No source variable dependencies
    DynamicMapping                     // References one or more source variables
)

type MappingInfo struct {
    VirtualPath  string
    Expression   string
    Type         MappingType
    Dependencies []string  // Source variables this mapping depends on
}

type Config struct {
    Mode            string
    Model           ModelConfig
    OutputTopic     string
    Sources         map[string]string  // alias -> topic
    Mapping         map[string]interface{}  // raw mapping config
    StaticMappings  map[string]MappingInfo  // pre-analyzed static mappings
    DynamicMappings map[string]MappingInfo  // pre-analyzed dynamic mappings
}
```

**Static Detection Algorithm**:
1. Parse JavaScript expression using AST analysis
2. Extract all variable references
3. Check if any references match configured source variable names
4. If no matches found → Static mapping
5. If matches found → Dynamic mapping with dependency list

### State Management

```go
type ProcessorState struct {
    Variables map[string]*VariableValue
    mutex     sync.RWMutex
}

type VariableValue struct {
    Value     interface{}
    Timestamp time.Time
    Source    string
}
```

### JavaScript Integration

- Use goja runtime for expression evaluation
- Inject source variables as JavaScript global context
- Support for mathematical operations and string manipulation
- Error handling with fallback to original message
- **Constraint**: Only source variables (from sources config) are available in JS context, not other computed mappings

### Thread Safety

- Per-model mutex protection for variable access
- Atomic operations for metrics
- Read-write locks for state access patterns

## Testing Strategy

### Unit Tests (`stream_processor_plugin_test.go`)
- Config parsing validation
- JavaScript expression evaluation
- Variable state management
- Message transformation logic

### Integration Tests (`integration_test.go`)
- End-to-end message processing
- UNS input/output compatibility
- Nested virtual path scenarios
- Error handling edge cases

### Suite Tests (`stream_processor_plugin_suite_test.go`)
- Ginkgo v2 test suite setup
- Shared test utilities
- Performance benchmarks

## Error Handling

### Fail-Open Policy
- Continue processing on expression errors
- Log errors but don't block message flow
- Provide fallback values where possible

### Error Categories
1. **Configuration Errors**: Invalid JS expressions, malformed topics, mappings referencing other mappings
2. **Runtime Errors**: JS execution failures, type conversion issues
3. **State Errors**: Variable resolution failures, memory limits
4. **Message Errors**: Invalid UMH format, missing metadata

## Performance Considerations

### Memory Management
- Simple variable storage without TTL
- Variables overwritten on new values
- Memory usage scales with number of unique sources

### Processing Efficiency
- Compiled JS expressions (where possible)
- Efficient variable lookup structures
- Batch processing optimizations

## Documentation Requirements

### Processor Documentation (`docs/processing/stream-processor.md`)
- Configuration reference
- Usage examples
- JavaScript expression guide
- Common patterns and troubleshooting

### Example Configurations
- Basic processor setup
- Complex JavaScript expressions
- Nested virtual path structures
- Integration with UNS input/output

## File Structure

```
stream_processor_plugin/
├── config.go                    # Configuration parsing and validation
├── static_detection.go          # Static mapping analysis using AST parsing
├── state.go                     # Variable state management
├── js_engine.go                 # JavaScript runtime integration
├── processor.go                 # Main BatchProcessor implementation
├── metrics.go                   # Metrics collection
├── stream_processor_plugin.go   # Plugin registration and init
├── stream_processor_plugin_test.go       # Unit tests
├── stream_processor_plugin_suite_test.go # Test suite
├── integration_test.go          # Integration tests
├── example_config.yaml          # Example configuration
└── README.md                    # Plugin-specific documentation
```

## Implementation Phases

### Phase 1: Core Infrastructure
- [ ] Config parsing and validation
- [ ] Static mapping identification and AST analysis
- [ ] Basic state management
- [ ] Plugin registration
- [ ] Unit test framework

### Phase 2: JavaScript Integration
- [ ] Goja runtime setup
- [ ] Expression compilation and execution
- [ ] Variable context injection
- [ ] JS error handling

### Phase 3: Message Processing
- [ ] UNS message parsing
- [ ] Variable resolution logic
- [ ] Output message generation
- [ ] Integration tests

### Phase 4: Production Readiness
- [ ] Performance optimization
- [ ] Memory management
- [ ] Comprehensive error handling
- [ ] Documentation and examples

## Dependencies

- `github.com/dop251/goja` - JavaScript runtime and AST parser
- `github.com/redpanda-data/benthos/v4/public/service` - Benthos service
- Standard Go sync, time, and testing packages

## Security Considerations

- Sandbox JavaScript execution
- Validate all user-provided expressions
- Limit JavaScript execution time
- Prevent access to system resources from JS

## Monitoring and Observability

- Metrics for processing rates, errors, and latency
- Variable resolution success rates
- Per-source and per-virtual path performance tracking
- JavaScript execution timing and errors 